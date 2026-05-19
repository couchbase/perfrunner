"""Builders for `mcstat timings` histograms, shared by the histogram and collector tests.

Kept out of both test modules because `cbagent.histogram` and `McstatHistogramStats` are
tested against the same fixture shapes, and the shapes are the part that has to stay faithful
to what kv-engine emits.
"""

import math
from collections.abc import Sequence

MAX_TRACKABLE = 1024


def mcstat_hist(
    buckets: Sequence[tuple[int, int]],
    overflowed: int = 0,
    overflowed_sum: int = 0,
    max_trackable: int = MAX_TRACKABLE,
    low: int = 0,
) -> dict:
    """Build a histogram in the shape `mcstat timings` emits per bucket, per metric.

    ``buckets`` is a list of ``(upper_bound, count)`` pairs, ordered and contiguous: a bucket's
    lower bound is the previous bucket's upper bound, and the first one's is ``bucketsLow``.
    Each row carries a third element - the percentile of values at or below the upper bound -
    which `hist_diff` unpacks positionally, so keep it. Format per kv_engine's own reader,
    `utilities/timing_histogram_printer.cc`.

    Bins are emitted by an HdrHistogram percentile iterator, one row per percentile tick, so
    bin widths track the distribution rather than being fixed. That is why two snapshots of
    the same metric share few boundaries, in *both* directions, and why `hist_diff` has to
    re-bin at all. See `percentile_iterator_hist` for the faithful version.
    """
    data = []
    running = 0
    grand_total = sum(count for _, count in buckets) + overflowed
    for hi, count in buckets:
        running += count
        data.append([hi, count, round(100 * running / grand_total, 4) if grand_total else 0.0])
    return {
        "bucketsLow": low,
        "data": data,
        "total": sum(count for _, count in buckets),
        "overflowed": overflowed,
        "overflowed_sum": overflowed_sum,
        "max_trackable": max_trackable,
    }


def percentile_iterator_hist(
    bucket_counts: Sequence[int], bounds: Sequence[int], ticks_per_half_distance: int = 5
) -> dict:
    """Emit ``bucket_counts`` the way kv-engine does: one row per HdrHistogram percentile tick.

    Reproduces the emitter's two artefacts, which a hand-written histogram misses: zero-count
    rows, and consecutive rows sharing an upper bound (two ticks landing in one bucket). Tick
    sequence is 0, 10, .., 50, 55, .., 75, 77.5, .., 87.5, 88.75, .., matching the samples
    collected off real clusters.
    """
    total = sum(bucket_counts)
    if not total:
        return mcstat_hist([], max_trackable=bounds[-1])
    rows, cumulative, since_last, tick = [], 0, 0, 0.0
    for bound, count in zip(bounds, bucket_counts):
        cumulative += count
        since_last += count
        while tick <= 100.0 and cumulative >= math.ceil(tick * total / 100.0):
            rows.append([bound, since_last, tick])
            since_last = 0
            half_distance = 2 ** (math.floor(math.log2(100.0 / max(100.0 - tick, 1e-12))) + 1)
            tick += 100.0 / (ticks_per_half_distance * half_distance)
    hist = mcstat_hist([], max_trackable=bounds[-1])
    hist["data"], hist["total"] = rows, total
    return hist


LATENCY_BOUNDS = sorted({int(64 * 1.08**k) for k in range(120)})


def latency_peak(centre: int, spread: float, scale: int = 10**4) -> list[int]:
    """Per-bucket counts for a Gaussian latency peak, as a real metric produces."""
    return [
        max(0, int(scale * math.exp(-((i - centre) ** 2) / spread)))
        for i in range(len(LATENCY_BOUNDS))
    ]


def drifting_pair() -> tuple[dict, dict]:
    """Two snapshots of a latency peak that drifted slower, via the real emitter."""
    before, added = latency_peak(40, 60), latency_peak(55, 30)
    return (
        percentile_iterator_hist(before, LATENCY_BOUNDS),
        percentile_iterator_hist([b + a for b, a in zip(before, added)], LATENCY_BOUNDS),
    )


def counts(diff_hist: dict) -> list[tuple[int, int]]:
    """Return a diff histogram's ``data`` as ``(upper_bound, count)`` pairs."""
    return [(hi, count) for hi, count, *_ in diff_hist["data"]]

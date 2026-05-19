from bisect import bisect_left
from typing import Optional, Sequence


class HistogramInvariantError(ValueError):
    """Raised when hist2 cannot be a later sample of the histogram hist1 came from."""


class TruncatedHistogramError(HistogramInvariantError):
    """Raised when hist2 lost the top of its distribution instead of having been reset.

    kv-engine renders "data" with an HdrHistogram percentile iterator whose walk stops as
    soon as its running count reaches the "total_count" it copied when the walk began - see
    `hdr_iter_init` and `has_next` in hdr_histogram.c, and `HdrHistogram::to_json` in
    couchbase/platform. Recording is not excluded while that walk runs: `addValueAndCount`
    takes the same *shared* lock the iterator holds and increments the counters
    non-atomically. So an observation recorded into a bucket the walk has not reached yet
    makes the running count hit that copy early, and the walk stops below the highest
    recorded bucket. Everything above the stopping point is missing from "data" although
    nothing was reset.

    Such a pair still has no interval to report, but it says nothing about the cluster and
    clears itself on the next sample, unlike counters that really did go backwards.
    """


def _cumulative_at_bounds(data: list) -> tuple[dict, int]:
    """Cumulative count at each bound, plus the grand total.

    Summed from "data" rather than read from "total": the two are not two views of one
    moment - "total" is the accumulating counter copied when the emitter's walk began, and
    "data" is what that walk went on to render - and only "data" carries the bounds these
    counts are being compared against.
    """
    cumulative, running = {}, 0
    for row in data:
        running += row[1]
        cumulative[row[0]] = running
    return cumulative, running


def hist_invariant_violation(hist1: dict, hist2: dict) -> Optional[HistogramInvariantError]:
    """Return the error saying why hist2 cannot be a later sample of hist1, or None if it can.

    Covers only what the two samples make directly comparable: the scalar counters, and
    ranges delimited by a boundary they share. A range they subdivide differently is left to
    `hist_diff`, which raises on it itself - so neither check is redundant.

    A shortfall confined *above* the highest bound the two share comes back as
    `TruncatedHistogramError` when "total" did not fall with it. "total" is the raw
    accumulating counter rather than anything the walk rendered, so it holding or growing
    rules out the reset that the same shortfall would mean lower down, and leaves the newer
    sample simply missing its tail.
    """
    if hist1["bucketsLow"] != hist2["bucketsLow"]:
        return HistogramInvariantError(
            f"bucketsLow changed from {hist1['bucketsLow']} to {hist2['bucketsLow']}"
        )

    for key, what in (("overflowed", "count"), ("overflowed_sum", "sum")):
        if hist2[key] < hist1[key]:
            return HistogramInvariantError(
                f"untrackable observation {what} fell from {hist1[key]} to {hist2[key]}"
            )

    cum1, total1 = _cumulative_at_bounds(hist1["data"])
    cum2, total2 = _cumulative_at_bounds(hist2["data"])
    below1 = below2 = 0
    block_low = hist1["bucketsLow"]

    # The range above the highest shared boundary is the whole histogram when they share none,
    # which is the only comparison available for a pair whose bins never line up.
    for bound in sorted(cum1.keys() & cum2.keys()):
        block1, block2 = cum1[bound] - below1, cum2[bound] - below2
        if block2 < block1:
            # A range both samples delimit, so a truncated walk cannot be what emptied it:
            # the walk only ever stops short of the *top*, leaving no shared bound above it.
            return HistogramInvariantError(
                f"count over ({block_low}, {bound}] fell from {block1} to {block2}"
            )
        below1, below2, block_low = cum1[bound], cum2[bound], bound

    above1, above2 = total1 - below1, total2 - below2
    if above2 < above1:
        reason = f"count above {block_low} fell from {above1} to {above2}"
        if hist2["total"] >= hist1["total"]:
            return TruncatedHistogramError(reason)
        return HistogramInvariantError(reason)

    return None


def hist_diff(hist1: dict, hist2: dict) -> dict:
    """Compute the delta from hist1 -> hist2, i.e. one sampling interval's observations.

    Both arguments are consecutive samples of the same mcstat histogram, in the shape
    kv-engine emits it (see kv_engine's utilities/timing_histogram_printer.cc): "data" is a
    contiguous partition of (bucketsLow, last bound], one row per HdrHistogram percentile
    tick, each row being
        [inclusive upper bound, count in (previous bound, this bound], percentile <= bound].
    A row may be empty, and consecutive rows may repeat a bound when two ticks land in the
    same underlying bucket. Observations too large to bucket are counted in "overflowed"
    instead, and are not in "data".

    Bins are placed per sample, so the two share only some of their boundaries and differ in
    both directions. Only the boundaries move - within a row the count is exact for its span
    - so hist1 is re-binned onto hist2's boundaries and subtracted. Where a hist1 row
    straddles a hist2 boundary its count has to be split, and the split deliberately
    overstates the interval's slower observations rather than its faster ones; where the two
    samples share a boundary the count below it is exact and no split is made. Reversing
    either of those changes what every run reports.

    Raises `HistogramInvariantError` for a pair with no interval to report, or its
    `TruncatedHistogramError` subclass where the newer sample only lost its tail:
    `hist_invariant_violation` rejects what the two samples make directly comparable, the
    guard in the loop the rest.
    """
    if violation := hist_invariant_violation(hist1, hist2):
        raise violation

    # `remaining` is the part of the current hist1 row not yet charged to a hist2 bucket.
    rows = hist1["data"]
    num_rows = len(rows)
    idx = 0
    remaining = rows[0][1] if rows else 0

    diff_hist_data = []
    for row in hist2["data"]:
        hist2_hi, hist2_count = row[0], row[1]

        # A row ending at or below the bound lies wholly inside this hist2 bucket, so its
        # whole count is charged here. Once hist1 is exhausted the loop runs zero times,
        # which is what leaves the rest of hist2 attributed entirely to the interval.
        hist1_count = 0
        ended_on_bound = False
        while idx < num_rows and rows[idx][0] <= hist2_hi:
            hist1_count += remaining
            ended_on_bound = rows[idx][0] == hist2_hi
            idx += 1
            remaining = rows[idx][1] if idx < num_rows else 0

        if idx < num_rows and not ended_on_bound:
            # The row we stopped on straddles the bound, so neither side's share of its
            # count is knowable: fill this bucket as far as it will go, pushing the
            # interval's observations up into the slower bucket. Whatever does not fit stays
            # on the row for the next hist2 bucket - deferred, not dropped - and the headroom
            # is only negative if hist1 holds observations hist2 does not, which the guard
            # below reports.
            #
            # Not reached when the loop ended exactly on the bound: both samples delimit that
            # value, so hist1's count below it is already exact and every row above it begins
            # there. Borrowing anyway would overstate the tail with nothing to justify it.
            borrow = min(max(0, hist2_count - hist1_count), remaining)
            remaining -= borrow
            hist1_count += borrow

        if hist1_count > hist2_count:
            # The attribution above caps this bucket's share at hist2_count, so getting here
            # means hist1 holds observations that hist2 no longer has - the two are not
            # successive samples of one accumulating histogram after all. The precondition
            # check cannot see this: it compares counts only over ranges both samples
            # delimit, and this surplus lives inside one of those ranges.
            raise HistogramInvariantError(
                f"re-binned hist1 count {hist1_count} exceeds hist2's {hist2_count} for "
                f"bucket <= {hist2_hi}"
            )
        diff_hist_data.append([hist2_hi, hist2_count - hist1_count])

    return {
        "bucketsLow": hist1["bucketsLow"],
        "max_trackable": hist2["max_trackable"],
        "data": diff_hist_data,
        "total": sum(count for _, count in diff_hist_data),
        "overflowed": hist2["overflowed"] - hist1["overflowed"],
        "overflowed_sum": hist2["overflowed_sum"] - hist1["overflowed_sum"],
    }


def hist_percentiles(hist: dict, percentiles: Sequence[float]) -> dict[float, int]:
    """Return percentile values from histogram, linearly interpolating within buckets.

    Keyed by the requested percentile. An interval that recorded nothing returns no results
    at all rather than zeroes, and a percentile falling in the untrackable tail comes back as
    "max_trackable".
    """
    total = hist["total"] + hist["overflowed"]
    if not percentiles or total <= 0:
        return {}

    max_percentile = max(percentiles)
    data = hist["data"]
    reached, running = [0.0], 0
    for row in data:
        running += row[1]
        reached.append(percentile_here := running / total * 100)
        if percentile_here >= max_percentile:
            break

    results = {}
    for percentile in sorted(percentiles):
        edge = bisect_left(reached, percentile)
        if not 0 < edge < len(reached):
            results[percentile] = hist["max_trackable"]  # in the untrackable tail
            continue
        # `reached` has a leading 0.0 for bucketsLow, so it sits one ahead of `data`.
        hi = data[edge - 1][0]
        lo = data[edge - 2][0] if edge > 1 else hist["bucketsLow"]
        span = reached[edge] - reached[edge - 1]
        results[percentile] = int(lo + (hi - lo) * ((percentile - reached[edge - 1]) / span))
    return results

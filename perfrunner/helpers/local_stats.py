"""Parse the local stat files that spring workers and JTS write on disk.

Single source of truth for turning raw files into normalised samples. Both the collectors and
``MetricHelper`` parse through here, so they cannot drift on units, columns, or consolidation.
"""

import csv
import glob
from typing import Iterator, NamedTuple, Optional


class LatencySample(NamedTuple):
    """One normalised spring-latency measurement (ms/epoch-ms)."""

    operation: str
    timestamp_ms: int
    latency_ms: float
    latency_total_ms: Optional[float]
    target: str


def parse_spring_latency_file(path: str) -> Iterator[LatencySample]:
    """Yield normalised samples from one spring worker reservoir dump.

    Rows are ``(operation, timestamp_ns, latency_single_s, latency_total_s, target)``;
    timestamps are converted ns->ms and latencies s->ms to match what the store receives.
    ``latency_total_ms`` is ``None`` when the row carries no total latency.
    """
    with open(path) as fh:
        for row in csv.reader(fh):
            if len(row) < 5:
                continue
            operation, timestamp, latency_single, latency_total, target = row
            yield LatencySample(
                operation=operation,
                timestamp_ms=int(timestamp) // 1_000_000,
                latency_ms=float(latency_single) * 1000,
                latency_total_ms=float(latency_total) * 1000 if latency_total else None,
                target=target,
            )


def consolidate_jts_log(jts_logs_dir: str, filename: str, is_latency: bool) -> dict[int, float]:
    """Consolidate JTS ``<time_bucket_index>:<value>`` samples across worker logs.

    Sums the value per time-bucket index across every matching worker log under
    ``<jts_logs_dir>/*/<filename>``; latency is additionally averaged per index
    (throughput is left summed). Returns ``{index: value}``, the collector needs
    the index to derive per-sample timestamps.
    """
    per_index: dict[int, list[float]] = {}
    for path in glob.glob(f"{jts_logs_dir}/*/{filename}"):
        with open(path) as fh:
            for line in fh:
                kv = line.split(":")
                if not kv[0].strip():
                    continue
                index = int(kv[0])
                value = float(kv[1].rstrip("\n")) if len(kv) > 1 else 0
                per_index.setdefault(index, []).append(value)

    consolidated = {}
    for index, samples in per_index.items():
        total = sum(samples)
        if is_latency:
            total /= len(samples)
        consolidated[index] = total
    return consolidated

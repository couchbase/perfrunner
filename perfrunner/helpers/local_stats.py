"""Parse the local stat files that spring workers and JTS write on disk.

Single source of truth for turning raw files into normalised samples. Both the collectors and
``MetricHelper`` parse through here, so they cannot drift on units, columns, or consolidation.
"""

import csv
import glob
from enum import Enum
from typing import Iterator, NamedTuple, Optional

# Where spring workers dump their latency data files, on both local and remote workers.
SPRING_LATENCY_LIVE_DIR = "spring_latency"
# Where latency collectors archive those data files (one subdir per cbmonitor snapshot)
# once they have been processed, so a new workload starts from an empty data dir.
SPRING_LATENCY_SNAPSHOT_DIR = "spring_latency_snapshots"

KV_WORKER_PATTERN = "*kv-worker-*"
QUERY_WORKER_PATTERN = "query-worker-*"


def spring_latency_live_dir(master_node: str) -> str:
    """Return the dir where spring workers dump their latency data files."""
    return f"{SPRING_LATENCY_LIVE_DIR}/master_{master_node}"


def spring_latency_target_groups(collection_map: Optional[dict]) -> dict[str, dict[str, str]]:
    """Map each bucket's ``scope:collection`` target to its configured stat group.

    Spring records the target on each sample when per-collection latency is enabled, and
    both the collector (which labels the perfdb bucket) and `MetricHelper` (which splits
    KPIs per stat group) have to turn that target back into a group the same way, so the
    key format lives here. Collections with no ``stat_group`` map to ``""``, matching the
    default that callers use for an unknown target.
    """
    return {
        bucket: {
            f"{scope}:{collection}": options.get("stat_group", "")
            for scope, collections in scopes.items()
            for collection, options in collections.items()
        }
        for bucket, scopes in (collection_map or {}).items()
    }


def spring_latency_snapshot_dir(label: str, master_node: str) -> str:
    """Return the dir where one stats phase's latency data files are archived.

    ``label`` must be unique per stats phase, otherwise two phases archive into the same
    dir and their data is indistinguishable afterwards. The relative path is valid both
    locally and on a remote worker: local work runs from the repo root, remote work runs
    under ``<worker_home>/perfrunner``.
    """
    return f"{SPRING_LATENCY_SNAPSHOT_DIR}/{label}/master_{master_node}"


def spring_latency_key(master_node: str, pattern: str) -> tuple[str, str]:
    """Return the key under which one workload's archive dir is recorded on the test.

    Deliberately built from master node and worker pattern only. Both identify *data*
    and are stable for the whole test, so `MetricHelper` can rebuild the key at report
    time. Anything phase-derived (the cbmonitor cluster id, the Prometheus phase label)
    is re-minted on every ``with_stats`` phase, so a KPI reported after a later phase
    could never look it back up - see `KVLatency.move_local_stat_files`.
    """
    return master_node, pattern


# Where `resolve_spring_latency_files` found the data files it returned.
class SpringLatencySource(str, Enum):
    ARCHIVE = "archive"  # this workload's own per-phase archive dir
    LIVE = "live"  # the shared live dump dir, may mix phases
    NOWHERE = "none"  # no data files at all


class SpringLatencyFiles(NamedTuple):
    """Data files backing one spring latency KPI, and where they came from."""

    paths: list[str]
    source: SpringLatencySource


def resolve_spring_latency_files(
    pattern: str, archive_dir: Optional[str], live_dir: str
) -> SpringLatencyFiles:
    """Find the data files backing one spring latency KPI, preferring the phase archive.

    ``archive_dir`` is the dir a latency collector archived this workload's files into,
    or None when no stats phase reported archiving any. It is preferred because it is
    scoped to a single phase; ``live_dir`` is shared by every phase, so falling back to
    it can mix data from workloads the KPI is not about.

    Callers are expected to warn on any source other than the archive - the two failure
    modes are distinguishable by whether ``archive_dir`` was None.
    """
    if archive_dir and (paths := sorted(glob.glob(f"{archive_dir}/{pattern}"))):
        return SpringLatencyFiles(paths, SpringLatencySource.ARCHIVE)

    if paths := sorted(glob.glob(f"{live_dir}/{pattern}")):
        return SpringLatencyFiles(paths, SpringLatencySource.LIVE)

    return SpringLatencyFiles([], SpringLatencySource.NOWHERE)


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

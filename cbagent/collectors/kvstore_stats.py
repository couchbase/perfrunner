import json
import re
from collections import defaultdict
from typing import Callable, Iterable, Optional

from cbagent.collectors.collector import CouchbaseCollector
from cbagent.histogram import (
    HistogramInvariantError,
    TruncatedHistogramError,
    hist_diff,
    hist_percentiles,
)
from cbagent.settings import CbAgentSettings
from logger import logger
from perfrunner.helpers.local import extract_cb_any, run_mcstat
from perfrunner.tests import PerfTest

BUCKET_SEP = re.compile(r"^\*+\n", flags=re.MULTILINE)


class KVStoreStats(CouchbaseCollector):
    COLLECTOR = "kvstore_stats"
    COLLECTOR_FLAG = "kvstore"
    SKIP_ON_DYNAMIC = True
    REQUIRES_NON_CYGWIN = True

    # mcstat-based stat collection requires server build >= 8.0 on Capella.
    MIN_CAPELLA_BUILD = (8, 0, 0, 0)

    @classmethod
    def is_activated(cls, test, collector_flags):
        if not super().is_activated(test, collector_flags):
            return False
        if test.capella_infra and test.server_info.build_tuple < cls.MIN_CAPELLA_BUILD:
            return False
        return True

    MC_STATS_PORT = 11209
    MC_STATS_PORT_TLS = 11207
    METRIC_CAP = 50

    FUSION_METRICS_AVERAGE = ("LogStoreWriteAmp", "LogStoreFragmentationRatio")
    FUSION_METRICS = (
        "NumLogStoreCachedReads",
        "NumReads",
        "NumSyncs",
        "NumBytesSynced",
        "NumBytesIncoming",
        "NumLogCleanBytesRead",
        "NumLogCleanReads",
        "ExtentMergerBytesRead",
        "ExtentMergerReads",
        "NumLogMergerBytesRead",
        "NumLogsMerged",
        "NumLogStoreRemotePuts",
        "NumLogStoreRemoteGets",
        "NumLogStoreRemoteLists",
        "NumLogStoreRemoteDeletes",
        "NumBytesMigrated",
        "NumLogStoreCachedReadsPerSec",
        "NumReadsPerSec",
        "NumSyncsPerSec",
        "NumBytesSyncedPerSec",
        "NumBytesIncomingPerSec",
        "NumLogCleanBytesReadPerSec",
        "NumLogCleanReadsPerSec",
        "ExtentMergerBytesReadPerSec",
        "ExtentMergerReadsPerSec",
        "NumLogMergerBytesReadPerSec",
        "NumLogsMergedPerSec",
        "NumLogStoreRemotePutsPerSec",
        "NumLogStoreRemoteGetsPerSec",
        "NumLogStoreRemoteListsPerSec",
        "NumLogStoreRemoteDeletesPerSec",
        "NumBytesMigratedPerSec",
        "LogStoreDataSize",
        "NumLogSegments",
        "NumFiles",
        "NumFileExtents",
        "LogStoreGarbageSize",
        "LogStoreSummarySectionSize",
        "FileMapMemUsed",
        *FUSION_METRICS_AVERAGE,
    )

    METRICS_AVERAGE_PER_SHARD = (
        "ReadAmp",
        "ReadAmpGet",
        "ReadIOAmp",
        "WriteAmp",
        "TxnSizeEstimate",
        "RecentWriteAmp",
        "RecentReadAmp",
        "RecentReadAmpGet",
        "RecentReadIOAmp",
        "RecentBytesPerRead",
        "FlushQueueSize",
        "CompactQueueSize",
        "BloomFilterFPR",
        "RecentBlockCacheHitRatio",
        "BlockCacheHitRatio",
        "ReadIOAmpSet",
        "RecentReadIOAmpSet",
        "CheckpointOverheadPeriod",
        "CheckpointOverheadRatio",
        *FUSION_METRICS_AVERAGE,
    )

    NO_CAP = (
        "TxnSizeEstimate",
        "RecentBytesPerRead",
        "FlushQueueSize",
        "CompactQueueSize",
        "BloomFilterFPR",
        "RecentBlockCacheHitRatio",
        "BlockCacheHitRatio",
        "ReadIOAmpSet",
        "RecentReadIOAmpSet",
        "CheckpointOverheadPeriod",
        "CheckpointOverheadRatio",
        "CheckpointOverhead",
        "ActiveDataSize",
        "ActiveDiskUsage",
        *FUSION_METRICS_AVERAGE,
    )

    NESTED_METRICS = {
        # metric name: nested object which contains it
        "TxnSizeEstimate": "walStats",
    }

    # All the metrics. If they aren't also in any of the above lists then they will be
    # summed across all shards without averaging or capping
    METRICS_ACROSS_SHARDS = (
        "BlockCacheQuota",
        "WriteCacheQuota",
        "BlockCacheMemUsed",
        "BlockCacheHits",
        "BlockCacheMisses",
        "BytesIncoming",
        "BytesOutgoing",
        "BytesPerRead",
        "FSReadBytes",
        "FSWriteBytes",
        "MemoryQuota",
        "NCommitBatches",
        "NDeletes",
        "NGets",
        "NInserts",
        "NReadBytes",
        "NReadBytesCompact",
        "NReadBytesGet",
        "NReadIOs",
        "NReadIOsGet",
        "NSets",
        "NSyncs",
        "NTablesCreated",
        "NTablesDeleted",
        "NTableFiles",
        "NFileCountCompacts",
        "TableMetaMemUsed",
        "TotalBloomFilterMemUsed",
        "NWriteBytes",
        "NWriteBytesCompact",
        "NWriteIOs",
        "BufferMemUsed",
        "WALMemUsed",
        "WriteCacheMemUsed",
        "NCompacts",
        "NFlushes",
        "NGetsPerSec",
        "NSetsPerSec",
        "NDeletesPerSec",
        "NCommitBatchesPerSec",
        "NFlushesPerSec",
        "NCompactsPerSec",
        "NSyncsPerSec",
        "NReadBytesPerSec",
        "NReadBytesGetPerSec",
        "NReadBytesCompactPerSec",
        "BytesOutgoingPerSec",
        "NReadIOsPerSec",
        "NReadIOsGetPerSec",
        "BytesIncomingPerSec",
        "NWriteBytesPerSec",
        "NWriteIOsPerSec",
        "NWriteBytesCompactPerSec",
        "NGetStatsPerSec",
        "NGetStatsComputedPerSec",
        "NBloomFilterHits",
        "NBloomFilterMisses",
        "NumNormalFlushes",
        "NumPersistentFlushes",
        "NumSyncFlushes",
        "WALBufferMemUsed",
        "TreeSnapshotMemoryUsed",
        "ReadAheadBufferMemUsed",
        "TableObjectMemUsed",
        "BlockCacheHitsPerSec",
        "BlockCacheMissesPerSec",
        "NBloomFilterMissesPerSec",
        "NBloomFilterHitsPerSec",
        "NReadBytesSet",
        "NReadIOsSet",
        "NReadBytesSetPerSec",
        "NReadIOsSetPerSec",
        "ActiveIndexBlocksSize",
        "NBlocksCached",
        "NBlocksDropped",
        "BlockCacheBlockSize",
        "NMemoryOptimisedCommitBatches",
        "TotalDiskUsage",
        "HistoryDiskUsage",
        "HistoryDataSize",
        "NonResidentBloomFilterSize",
        *(set(FUSION_METRICS) | set(METRICS_AVERAGE_PER_SHARD) | set(NO_CAP) | set(NESTED_METRICS)),
    )

    def __init__(self, settings: CbAgentSettings, test: PerfTest):
        super().__init__(settings, test)
        extract_cb_any(filename="couchbase")
        self.collect_per_server_stats = test.test_config.magma_settings.collect_per_server_stats
        self.cluster_spec = test.cluster_spec

        # Track previously reported node problems to avoid noisy logging
        self._prev_unreachable = set()
        self._prev_without_stats = set()

        # Create REST helper for Capella to get active nodes
        self.rest = test.rest if self.capella_infra else None

        self.use_tls = self.capella_infra or self.n2n_enabled
        self.mcstat_port = self.MC_STATS_PORT_TLS if self.use_tls else self.MC_STATS_PORT

    @staticmethod
    def _split_stat_output_by_bucket(stdout: str) -> dict[str, dict]:
        stats_per_bucket = {}
        split_by_bucket = re.split(BUCKET_SEP, stdout.strip("*"))
        for section in split_by_bucket:
            bucket, *stats = section.split(maxsplit=1)
            if stats:
                stats_per_bucket[bucket] = json.loads(stats[0])
        return stats_per_bucket

    def _get_raw_stats(self, server: str, statkey: str) -> Optional[dict[str, dict]]:
        """Fetch `statkey` from `server`, or None if the node could not be asked for it.

        None and an empty dict are different answers: None means the node did not return
        usable output, an empty dict that it did and had nothing to report. Only the first is
        a node problem, so the callers keep them apart rather than treating both as a node
        that has dropped out.
        """
        uname, pwd = self.auth
        stdout, stderr, returncode = run_mcstat(
            server,
            self.mcstat_port,
            statkey,
            uname,
            pwd,
            bucket=None,
            tls=self.use_tls,
            quiet=True,
        )

        if returncode != 0:
            if stderr and "config-only bucket" in stderr:
                # Expected during rebalance on newly added nodes. The node is up and will
                # report once the bucket is, so this is nothing to report rather than a node
                # that could not be asked.
                return {}

            logger.warning(
                f"{self.__class__.__name__}: failed to get {statkey} stats from {server}. "
                f"Stderr: {stderr}"
            )
            return None

        if not stdout or not stdout.strip():
            logger.warning(
                f"{self.__class__.__name__}: got empty output from {server} for {statkey} stats. "
                f"Stderr: {stderr}"
            )
            return None

        return self._split_stat_output_by_bucket(stdout)

    def _get_bucket_stats(self, server: str, statkey: str, extract: Callable) -> Optional[dict]:
        """Fetch `statkey` from `server` and apply `extract` per bucket; never raise.

        Owns the boundary around one server, so a node whose output cannot be parsed is
        skipped for this interval instead of losing the whole sample.

        Returns None when the node could not be asked, and an empty dict when it answered
        with nothing `extract` recognises - a metric no operation has produced yet, say,
        which is not the node's fault and not something to report as one.
        """
        try:
            if (raw_stats := self._get_raw_stats(server, statkey)) is None:
                return None

            return {
                bucket: extracted
                for bucket, bucket_stats in raw_stats.items()
                if (extracted := extract(bucket_stats))
            }
        except Exception as e:
            logger.error(
                f"{self.__class__.__name__}: unexpected error for server {server}: {e}",
                exc_info=True,
            )
            return None

    def _magma_metrics(self, bucket_stats: dict) -> dict[str, float]:
        totals = defaultdict(int)
        for shard, metrics in bucket_stats.items():
            if not shard.endswith(":magma"):
                continue

            for metric in self.METRICS_ACROSS_SHARDS:
                if nested := self.NESTED_METRICS.get(metric):
                    value = metrics.get(nested, {}).get(metric)
                elif metric in self.FUSION_METRICS:
                    # Fusion metrics MUST come from FusionFSStats to avoid duplication
                    value = metrics.get("FusionFSStats", {}).get(metric)
                else:
                    value = metrics.get(metric)

                if value is not None:
                    totals[metric] += value
        return dict(totals)

    def _get_magma_stats(self, server: str) -> Optional[dict[str, dict[str, float]]]:
        return self._get_bucket_stats(server, "kvstore", self._magma_metrics)

    def _log_set_change(
        self, prev: set, current: set, problem: str, recovered: str, level: str = "warning"
    ) -> set:
        """Log `problem` when the affected set changes, `recovered` once it empties again."""
        if current != prev:
            if current:
                getattr(logger, level)(f"{self.__class__.__name__}: {problem}")
            elif prev:
                logger.info(f"{self.__class__.__name__}: {recovered}")
        return current

    def _log_unreachable_nodes(self, unreachable: set):
        self._prev_unreachable = self._log_set_change(
            self._prev_unreachable,
            unreachable,
            f"skipped {len(unreachable)} unreachable node(s): " + ", ".join(sorted(unreachable)),
            "all nodes are now reachable again",
        )

    def _log_nodes_without_stats(self, without_stats: set, what: str):
        """Note nodes that answered but had no `what` for us, which is not a node problem.

        Expected while no operation has produced the metric yet, so this is a note rather
        than a warning - but a node that never leaves the set is reporting nothing all run,
        which is worth being able to see.
        """
        self._prev_without_stats = self._log_set_change(
            self._prev_without_stats,
            without_stats,
            f"{len(without_stats)} node(s) reporting no {what} yet: "
            + ", ".join(sorted(without_stats)),
            f"all nodes are now reporting {what}",
            level="info",
        )

    def _collect_per_node(self, fetch: Callable[[str], Optional[dict]], what: str) -> dict:
        """Ask every node for `what`, and return it keyed by the nodes that had some.

        Owns the three-way split the callers would otherwise each repeat: a node that could
        not be asked, one that answered with nothing, and one with stats to report. The
        first two are logged here - only the first as a node problem - so a sample() below
        deals only in nodes that returned something.
        """
        collected, unreachable, without_stats = {}, set(), set()

        for node in self.nodes:
            if (stats := fetch(node)) is None:
                unreachable.add(node)
            elif not stats:
                without_stats.add(node)
            else:
                collected[node] = stats

        self._log_unreachable_nodes(unreachable)
        self._log_nodes_without_stats(without_stats, what)
        return collected

    def _get_num_shards(self, server: str, buckets: Iterable) -> dict[str, int]:
        shards_per_bucket = {}
        statkey = "workload"
        stat = "ep_workload:num_shards"
        data = self._get_raw_stats(server, statkey) or {}

        for bucket in buckets:
            if not (bucket_stats := data.get(bucket)):
                shards = 1
                logger.warning(
                    f"{self.__class__.__name__}: failed to get {statkey} stats for "
                    f"{bucket} on {server}. Using fallback shard count of {shards}."
                )
            elif not (shards := bucket_stats.get(stat)):
                shards = 1
                logger.warning(
                    f"{self.__class__.__name__}: didn't find {stat} stat for {bucket} on "
                    f"{server}. Using fallback shard count of {shards}."
                )
            shards_per_bucket[bucket] = shards
        return shards_per_bucket

    def _get_node_list(self) -> list[str]:
        # Refresh node list to handle nodes removed during rebalance
        # For Capella, use get_all_cluster_nodes() to get actual active nodes
        # This prevents attempting to collect from removed nodes and reduces noise
        if self.capella_infra:
            try:
                cluster_nodes = self.rest.get_all_cluster_nodes()
                # Extract hostnames from "hostname:services" format
                return [node.split(":")[0] for _, nodes in cluster_nodes.items() for node in nodes]
            except Exception as e:
                # Fall back to get_nodes() if get_all_cluster_nodes() fails
                logger.debug(f"Failed to get active nodes from Capella API: {e}, using fallback")
                return list(self.get_nodes())

        return list(self.get_nodes())

    def _calculate_average_stats(self, stats: dict[str, float], shards: int):
        for metric in self.METRICS_AVERAGE_PER_SHARD:
            if metric in stats:
                avg_value = stats[metric] / shards
                if metric in self.NO_CAP:
                    stats[metric] = avg_value
                else:
                    stats[metric] = min(avg_value, self.METRIC_CAP)

    def sample(self):
        self.nodes = self._get_node_list()

        shards_per_bucket_per_node = {}
        stats_per_bucket_per_node = {}

        per_node_stats = self._collect_per_node(self._get_magma_stats, "magma kvstore stats")

        for node, magma_stats in per_node_stats.items():
            # get shard counts for buckets we got stats for
            shards_per_bucket = self._get_num_shards(node, magma_stats.keys())

            for bucket, stats in magma_stats.items():
                stats_per_bucket_per_node.setdefault(bucket, {})[node] = stats
                shards_per_bucket_per_node.setdefault(bucket, {})[node] = shards_per_bucket[bucket]

        for bucket, raw_per_node_stats in stats_per_bucket_per_node.items():
            shards_per_node = shards_per_bucket_per_node[bucket]
            total_bucket_shards = sum(shards_per_node.values())
            cluster_stats = defaultdict(float)

            for node, raw_node_stats in raw_per_node_stats.items():
                for metric, value in raw_node_stats.items():
                    cluster_stats[metric] += value

                if self.collect_per_server_stats:
                    node_stats = dict(raw_node_stats)
                    self._calculate_average_stats(node_stats, shards_per_node[node])
                    self.update_metric_metadata(node_stats.keys(), server=node, bucket=bucket)
                    self.store.append(
                        node_stats,
                        cluster=self.cluster,
                        bucket=bucket,
                        server=node,
                        collector=self.COLLECTOR,
                    )

            self._calculate_average_stats(cluster_stats, total_bucket_shards)
            if cluster_stats:
                self.update_metric_metadata(cluster_stats.keys(), bucket=bucket)
                self.store.append(
                    cluster_stats,
                    cluster=self.cluster,
                    bucket=bucket,
                    collector=self.COLLECTOR,
                )

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for node in self.nodes:
            self.mc.add_server(node)


class McstatHistogramStats(KVStoreStats):
    COLLECTOR = "mcstat_histogram_stats"
    COLLECTOR_FLAG = "mcstat_histogram"

    HISTOGRAM_METRICS = ("bg_load",)

    PERCENTILES = (50, 90, 95, 99, 99.9)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # per-bucket, per-node, per-metric histograms
        self.prev_histograms = {}

        # Track previously reported rejected pairs to avoid noisy logging
        self._prev_violations = set()
        self._prev_truncated = set()
        self._prev_malformed = set()

        # Histograms whose baseline the last sample held back, so it is held back once only
        self._retained = set()

    @staticmethod
    def _describe_rejected(rejected: dict[tuple[str, str, str], str]) -> str:
        """Name every histogram a sample had to reject, and why."""
        return "; ".join(
            f"{bucket}/{node}/{metric}: {reason}"
            for (bucket, node, metric), reason in sorted(rejected.items())
        )

    def _histograms(self, bucket_stats: dict) -> dict[str, dict]:
        return {
            metric: hist
            for metric in self.HISTOGRAM_METRICS
            if (hist := bucket_stats.get(metric)) is not None
        }

    def _get_timing_stats(self, server: str) -> Optional[dict[str, dict[str, dict]]]:
        return self._get_bucket_stats(server, "timings", self._histograms)

    def _retain_baselines(self, prev_histograms: dict, unusable: set):
        """Undo the re-baseline for histograms this sample rendered too badly to diff against.

        A reset leaves a sound sample that is merely discontinuous, so that one keeps the new
        baseline. A torn or unparseable render does not: the observations it failed to show
        are still in the counter, and baselining on it hands them to the next interval as if
        they had just happened, which lands on p99.9 hardest because they are the slowest
        ones there are.

        Held back for one sample only. A histogram that keeps tearing is unreportable
        whichever baseline it is measured against, so refusing to advance would only widen
        the window the eventual good sample covers, without saving it from anything.
        """
        retained = unusable - self._retained
        for bucket, node, metric in retained:
            self.prev_histograms[bucket][node][metric] = prev_histograms[bucket][node][metric]
        self._retained = retained

    def sample(self):
        self.nodes = self._get_node_list()

        current_histograms = {}
        stats_per_bucket_per_node = {}

        # A node with no histograms has simply not produced any of HISTOGRAM_METRICS yet.
        per_node_stats = self._collect_per_node(self._get_timing_stats, "timing histograms")

        for node, timing_stats in per_node_stats.items():
            for bucket, stats in timing_stats.items():
                current_histograms.setdefault(bucket, {})[node] = stats

        # Re-baseline before diffing, so a pair we cannot diff costs one interval rather than
        # every interval after it. The loop below must read the saved previous samples, and
        # the block after it undoes this for samples that turn out not to be worth baselining.
        prev_histograms, self.prev_histograms = self.prev_histograms, current_histograms

        violations, truncated, malformed = {}, {}, {}

        for bucket, per_node_hists in current_histograms.items():
            for node, hists in per_node_hists.items():
                stats = {}
                for metric, curr_hist in hists.items():
                    if not (prev_hist := prev_histograms.get(bucket, {}).get(node, {}).get(metric)):
                        continue

                    try:
                        diff = hist_diff(prev_hist, curr_hist)
                        percentiles = hist_percentiles(diff, self.PERCENTILES)
                    except TruncatedHistogramError as e:
                        truncated[(bucket, node, metric)] = str(e)
                        continue
                    except HistogramInvariantError as e:
                        violations[(bucket, node, metric)] = str(e)
                        continue
                    except (KeyError, IndexError, TypeError) as e:
                        malformed[(bucket, node, metric)] = f"{type(e).__name__}: {e}"
                        continue

                    stats |= {f"{metric}_p{p}": p_value for p, p_value in percentiles.items()}

                if stats:
                    stats_per_bucket_per_node.setdefault(bucket, {})[node] = stats

        self._retain_baselines(prev_histograms, set(truncated) | set(malformed))

        self._prev_violations = self._log_set_change(
            self._prev_violations,
            set(violations),
            f"skipped {len(violations)} histogram(s) whose counters went backwards "
            f"(kv stats reset mid-phase, node restarted, or bucket recreated?): "
            + self._describe_rejected(violations),
            "histogram counters are consistent again",
        )

        # A torn read is expected under load and typically clears itself on the next sample, so it
        # is worth a note rather than a warning about a cluster that is behaving perfectly well.
        self._prev_truncated = self._log_set_change(
            self._prev_truncated,
            set(truncated),
            f"skipped {len(truncated)} histogram(s) the emitter rendered without their tail "
            f"(its read raced with kv recording; the counters did not go backwards): "
            + self._describe_rejected(truncated),
            "histograms are being rendered whole again",
            level="info",
        )

        self._prev_malformed = self._log_set_change(
            self._prev_malformed,
            set(malformed),
            f"skipped {len(malformed)} histogram(s) that are not shaped as expected "
            f"(has the server's timings format changed?): " + self._describe_rejected(malformed),
            "histograms are parseable again",
        )

        for bucket, raw_per_node_stats in stats_per_bucket_per_node.items():
            for node, raw_node_stats in raw_per_node_stats.items():
                self.update_metric_metadata(raw_node_stats.keys(), server=node, bucket=bucket)
                self.store.append(
                    raw_node_stats,
                    cluster=self.cluster,
                    bucket=bucket,
                    server=node,
                    collector=self.COLLECTOR,
                )

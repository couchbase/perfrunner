import json
import re
from collections import defaultdict
from typing import Iterable

from cbagent.collectors.collector import CouchbaseCollector
from cbagent.settings import CbAgentSettings
from logger import logger
from perfrunner.helpers.local import extract_cb_any, run_mcstat
from perfrunner.helpers.rest import RestHelper
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

        # Track previous skipped nodes to avoid noisy logging
        self._prev_skipped = set()

        # Create REST helper for Capella to get active nodes
        self.rest = RestHelper(self.cluster_spec, True) if self.capella_infra else None

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

    def _get_raw_stats(self, server: str, statkey: str) -> dict[str, dict]:
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
                # This is expected to happen during rebalance on newly added nodes.
                pass
            else:
                logger.warning(
                    f"KVStoreStats: failed to get {statkey} stats from {server}. Stderr: {stderr}"
                )
            return {}

        if not stdout or not stdout.strip():
            logger.warning(
                f"KVStoreStats: received empty output from {server} for {statkey} stats. "
                f"Stderr: {stderr}"
            )
            return {}

        return self._split_stat_output_by_bucket(stdout)

    def _get_magma_stats(self, server: str) -> dict[str, dict[str, float]]:
        stats = {}
        try:
            data = self._get_raw_stats(server, "kvstore")

            for bucket, bucket_stats in data.items():
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
                            if bucket not in stats:
                                stats[bucket] = {}

                            if metric not in stats[bucket]:
                                stats[bucket][metric] = 0

                            stats[bucket][metric] += value
        except Exception as e:
            logger.error(
                f"KVStoreStats: unexpected error for server {server}: {e}",
                exc_info=True,
            )

        return stats

    def _get_num_shards(self, server: str, buckets: Iterable) -> dict[str, int]:
        shards_per_bucket = {}
        statkey = "workload"
        stat = "ep_workload:num_shards"
        data = self._get_raw_stats(server, statkey)

        for bucket in buckets:
            if not (bucket_stats := data.get(bucket)):
                shards = 1
                logger.warning(
                    f"KVStoreStats: failed to get {statkey} stats for {bucket} on {server}. "
                    f"Using fallback shard count of {shards}."
                )
            elif not (shards := bucket_stats.get(stat)):
                shards = 1
                logger.warning(
                    f"KVStoreStats: didn't find {stat} stat for {bucket} on {server}. "
                    f"Using fallback shard count of {shards}."
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

        shards_per_bucket_per_node = defaultdict(dict)
        stats_per_bucket_per_node = defaultdict(dict)
        skipped_nodes = set()

        for node in self.nodes:
            if not (magma_stats := self._get_magma_stats(node)):
                skipped_nodes.add(node)
                continue

            # get shard counts for buckets we got stats for
            shards_per_bucket = self._get_num_shards(node, magma_stats.keys())

            for bucket, stats in magma_stats.items():
                stats_per_bucket_per_node[bucket][node] = stats
                shards_per_bucket_per_node[bucket][node] = shards_per_bucket[bucket]

        # Log summary of skipped nodes only when the set changes (to avoid noise)
        if skipped_nodes != self._prev_skipped:
            if skipped_nodes:
                logger.warning(
                    f"KVStoreStats: skipped {len(skipped_nodes)} unreachable node(s): "
                    + ", ".join(sorted(skipped_nodes))
                )
            elif self._prev_skipped:
                # Nodes recovered
                logger.info("KVStoreStats: all nodes are now reachable again")
            self._prev_skipped = skipped_nodes

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

import json

from cbagent.collectors.collector import Collector
from logger import logger
from perfrunner.helpers.local import extract_cb_any, run_cbstats


class KVStoreStats(Collector):
    COLLECTOR = "kvstore_stats"
    CB_STATS_PORT = 11209
    METRICS_ACROSS_SHARDS = (
        "BlockCacheQuota",
        "WriteCacheQuota",
        "BlockCacheMemUsed",
        "BlockCacheHits",
        "BlockCacheMisses",
        "BloomFilterMemUsed",
        "BytesIncoming",
        "BytesOutgoing",
        "BytesPerRead",
        "FSReadBytes",
        "FSWriteBytes",
        "IndexBlocksSize",
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
        "ReadAmp",
        "ReadAmpGet",
        "ReadIOAmp",
        "WriteAmp",
        "TxnSizeEstimate",
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
        "RecentWriteAmp",
        "RecentReadAmp",
        "RecentReadAmpGet",
        "RecentReadIOAmp",
        "RecentBytesPerRead",
        "NGetStatsPerSec",
        "NGetStatsComputedPerSec",
        "FlushQueueSize",
        "CompactQueueSize",
        "NBloomFilterHits",
        "NBloomFilterMisses",
        "BloomFilterFPR",
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
        "RecentBlockCacheHitRatio",
        "BlockCacheHitRatio",
        "ReadIOAmpSet",
        "RecentReadIOAmpSet",
        "NReadBytesSet",
        "NReadIOsSet",
        "NReadBytesSetPerSec",
        "NReadIOsSetPerSec",
        "ActiveIndexBlocksSize",
        "NBlocksCached",
        "CheckpointOverheadPeriod",
        "CheckpointOverheadRatio",
        "NBlocksDropped",
        "BlockCacheBlockSize",
        "NMemoryOptimisedCommitBatches",
        "TotalDiskUsage",
        "HistoryDiskUsage",
        "HistoryDataSize",
        "NonResidentBloomFilterSize",
        "RecentBloomFilterCacheHitRatio",
        "CheckpointOverhead",
        "ActiveDataSize",
        "ActiveDiskUsage",
        "CheckpointOverheadKeyIndex",
        "CheckpointOverheadSeqIndex"
    )
    METRICS_AVERAGE_PER_NODE_PER_SHARD = (
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
        "RecentBloomFilterCacheHitRatio"
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
        "RecentBloomFilterCacheHitRatio",
        "CheckpointOverhead",
        "ActiveDataSize",
        "ActiveDiskUsage"
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        extract_cb_any(filename='couchbase')
        self.collect_per_server_stats = test.collect_per_server_stats
        self.cluster_spec = test.cluster_spec

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            uname, pwd = self.cluster_spec.rest_credentials
            stdout, returncode = run_cbstats("kvstore", server, self.CB_STATS_PORT, uname, pwd,
                                             bucket)
            if returncode != 0:
                logger.warning("KVStoreStats failed to get kvstore stats from server: {}"
                               .format(server))
                return stats

            data = json.loads(stdout)
            for shard, metrics in data.items():
                if not shard.endswith(":magma"):
                    continue

                # The :magma stats are an escaped JSON string so we need to parse it again
                metrics = json.loads(metrics)

                for metric in self.METRICS_ACROSS_SHARDS:
                    if metric in metrics:
                        value = metrics[metric]

                    if metric == "TxnSizeEstimate" and metric in metrics.get("walStats", {}):
                        value = metrics["walStats"][metric]
                    elif metric == "CheckpointOverheadKeyIndex" and \
                            metric in metrics.get("KeyStats", {}):
                        value = metrics["KeyStats"][metric]
                    elif metric == "CheckpointOverheadSeqIndex" and \
                            metric in metrics.get("SeqStats", {}):
                        value = metrics["SeqStats"][metric]

                    if metric in stats:
                        stats[metric] += value
                    else:
                        stats[metric] = value
        except Exception:
            pass

        return stats

    def _get_kvstore_stats(self, bucket: str, server: str):
        node_stats = self._get_stats_from_server(bucket, server=server)
        return node_stats

    def _get_num_shards(self, bucket: str, server: str):
        uname, pwd = self.cluster_spec.rest_credentials
        stdout, returncode = run_cbstats("workload", server, self.CB_STATS_PORT, uname, pwd, bucket)
        if returncode != 0:
            logger.warning("KVStoreStats failed to get workload stats from server: {}"
                           .format(server))
            return 1

        data = json.loads(stdout)
        return data["ep_workload:num_shards"]

    def sample(self):
        if self.collect_per_server_stats:
            for node in self.nodes:
                for bucket in self.get_buckets():
                    num_shards = self._get_num_shards(bucket, self.master_node)
                    stats = self._get_kvstore_stats(bucket, node)
                    for metric in self.METRICS_AVERAGE_PER_NODE_PER_SHARD:
                        if metric in stats:
                            if stats[metric] / num_shards >= 50 and metric not in self.NO_CAP:
                                stats[metric] = 50
                            else:
                                stats[metric] /= num_shards

                    if stats:
                        self.update_metric_metadata(stats.keys(), server=node, bucket=bucket)
                        self.append_to_store(stats, cluster=self.cluster,
                                             bucket=bucket, server=node,
                                             collector=self.COLLECTOR)

        for bucket in self.get_buckets():
            stats = {}
            num_shards = self._get_num_shards(bucket, self.master_node)
            num_nodes = len(self.nodes)
            for node in self.nodes:
                temp_stats = self._get_kvstore_stats(bucket, node)
                for st in temp_stats:
                    if st in stats:
                        stats[st] += temp_stats[st]
                    else:
                        stats[st] = temp_stats[st]

            for metric in self.METRICS_AVERAGE_PER_NODE_PER_SHARD:
                if metric in stats:
                    if stats[metric]/(num_shards * num_nodes) >= 50 and metric not in self.NO_CAP:
                        stats[metric] = 50
                    else:
                        stats[metric] /= (num_shards * num_nodes)

            if stats:
                self.update_metric_metadata(stats.keys(), bucket=bucket)
                self.append_to_store(stats, cluster=self.cluster,
                                     bucket=bucket,
                                     collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for node in self.nodes:
            self.mc.add_server(node)

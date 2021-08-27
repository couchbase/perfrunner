import json

from cbagent.collectors.collector import Collector
from perfrunner.helpers.local import extract_cb_any, get_cbstats


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
        "ActiveBloomFilterMemUsed",
        "TotalBloomFilterMemUsed",
        "NWriteBytes",
        "NWriteBytesCompact",
        "NWriteIOs",
        "TotalMemUsed",
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
        "HistogramMemUsed",
        "WALBufferMemUsed",
        "TreeSnapshotMemoryUsed",
        "LSMTreeObjectMemUsed",
        "ReadAheadBufferMemUsed",
        "TableObjectMemUsed",
        "BlockCacheHitsPerSec",
        "BlockCacheMissesPerSec",
        "NBloomFilterMissesPerSec",
        "NBloomFilterHitsPerSec"
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
        "BloomFilterFPR"
    )
    NO_CAP = (
        "TxnSizeEstimate",
        "RecentBytesPerRead",
        "FlushQueueSize",
        "CompactQueueSize",
        "BloomFilterFPR"
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        extract_cb_any(filename='couchbase')
        self.collect_per_server_stats = test.collect_per_server_stats
        self.cluster_spec = test.cluster_spec

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            result = get_cbstats(server, self.CB_STATS_PORT, "kvstore", self.cluster_spec)
            buckets_data = list(filter(lambda a: a != "", result.split("*")))
            for data in buckets_data:
                data = data.strip()
                if data.startswith(bucket):
                    data = data.split("\n", 1)[1]
                    data = data.replace("\"{", "{")
                    data = data.replace("}\"", "}")
                    data = data.replace("\\", "")
                    data = json.loads(data)
                    for (shard, metrics) in data.items():
                        if not shard.endswith(":magma"):
                            continue
                        for metric in self.METRICS_ACROSS_SHARDS:
                            if metric in metrics.keys():
                                if metric in stats:
                                    stats[metric] += metrics[metric]
                                else:
                                    stats[metric] = metrics[metric]
                            if metric == "TxnSizeEstimate" and "walStats" in metrics.keys():
                                if metric in stats:
                                    stats[metric] += metrics["walStats"][metric]
                                else:
                                    stats[metric] = metrics["walStats"][metric]
                    break
        except Exception:
            pass

        return stats

    def _get_kvstore_stats(self, bucket: str, server: str):
        node_stats = self._get_stats_from_server(bucket, server=server)
        return node_stats

    def _get_num_shards(self, bucket: str, server: str):
        result = get_cbstats(server, self.CB_STATS_PORT, "workload", self.cluster_spec)
        buckets_data = list(filter(lambda a: a != "", result.split("*")))
        for data in buckets_data:
            data = data.strip()
            if data.startswith(bucket):
                data = data.split("\n", 1)[1]
                data = data.replace("\"{", "{")
                data = data.replace("}\"", "}")
                data = data.replace("\\", "")
                data = json.loads(data)
                return data["ep_workload:num_shards"]
        return 1

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
                        self.store.append(stats, cluster=self.cluster,
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
                self.store.append(stats, cluster=self.cluster,
                                  bucket=bucket,
                                  collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for node in self.nodes:
            self.mc.add_server(node)

import json

from fabric.api import local

from cbagent.collectors import Collector
from perfrunner.helpers.local import extract_cb


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
        "NReadBytes",
        "NReadBytesCompact",
        "NReadBytesGet",
        "NReadIOs",
        "NReadIOsGet",
        "NSets",
        "NSyncs",
        "NTablesCreated",
        "NTablesDeleted",
        "NWriteBytes",
        "NWriteBytesCompact",
        "NWriteIOs",
        "TotalMemUsed",
        "WriteCacheMemUsed"
    )
    METRICS_ONLY_ONCE = (
        "ReadAmp",
        "ReadAmpGet",
        "ReadIOAmp",
        "WriteAmp"
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        extract_cb(filename='couchbase.rpm')
        self.collect_per_server_stats = test.collect_per_server_stats
        print("collect_per_server_stats: {}".format(self.collect_per_server_stats))

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            cmd = "./opt/couchbase/bin/cbstats -a {}:{} -u Administrator -p password kvstore -j"\
                .format(server, self.CB_STATS_PORT)
            result = local(cmd, capture=True)
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
                        for metric in self.METRICS_ACROSS_SHARDS:
                            if metric in metrics.keys():
                                if metric in stats:
                                    stats[metric] += metrics[metric]
                                else:
                                    stats[metric] = metrics[metric]
                        for metric in self.METRICS_ONLY_ONCE:
                            if metric in metrics.keys():
                                stats[metric] = metrics[metric]
                    break
        except Exception:
            pass

        return stats

    def _get_kvstore_stats(self, bucket: str, server: str):
        node_stats = self._get_stats_from_server(bucket, server=server)
        return node_stats

    def sample(self):
        if self.collect_per_server_stats:
            for node in self.nodes:
                for bucket in self.get_buckets():
                    stats = self._get_kvstore_stats(bucket, node)
                    if stats:
                        self.update_metric_metadata(stats.keys(), server=node, bucket=bucket)
                        self.store.append(stats, cluster=self.cluster,
                                          bucket=bucket, server=node,
                                          collector=self.COLLECTOR)

        for bucket in self.get_buckets():
            stats = {}
            for node in self.nodes:
                temp_stats = self._get_kvstore_stats(bucket, node)
                for st in temp_stats:
                    if st in stats:
                        stats[st] += temp_stats[st]
                    else:
                        stats[st] = temp_stats[st]

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

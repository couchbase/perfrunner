import json

from cbagent.collectors.collector import Collector
from perfrunner.helpers.local import extract_cb_any, get_cbstats


class CBStatsMemory(Collector):
    COLLECTOR = "cbstats_memory"
    CB_STATS_PORT = 11209
    METRICS = (
        "ep_mem_used_secondary",
        "ep_mem_used_primary"
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        extract_cb_any(filename='couchbase')
        self.cluster_spec = test.cluster_spec

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            result = get_cbstats(server, self.CB_STATS_PORT, "memory", self.cluster_spec)
            buckets_data = list(filter(lambda a: a != "", result.split("*")))
            for data in buckets_data:
                data = data.strip()
                if data.startswith(bucket):
                    data = data.split("\n", 1)[1]
                    data = data.replace("\"{", "{")
                    data = data.replace("}\"", "}")
                    data = data.replace("\\", "")
                    data = json.loads(data)
                    for (metric, number) in data.items():
                        if metric in self.METRICS:
                            if metric in stats:
                                stats[metric] += number
                            else:
                                stats[metric] = number
                    break
        except Exception:
            pass

        return stats

    def _get_memory_stats(self, bucket: str, server: str):
        node_stats = self._get_stats_from_server(bucket, server=server)
        return node_stats

    def sample(self):
        for bucket in self.get_buckets():
            stats = {}
            for node in self.nodes:
                temp_stats = self._get_memory_stats(bucket, node)
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

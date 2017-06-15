from cbagent.collectors import Collector


class SecondaryStorageStats(Collector):

    COLLECTOR = "secondary_storage_stats"

    METRICS = "memory_size", "num_cached_pages", "num_pages", "num_pages_swapout", "num_pages_swapin", \
              "bytes_incoming", "bytes_written", "write_amp", "lss_fragmentation", "cache_hits", "cache_misses", \
              "cache_hit_ratio", "resident_ratio", "allocated", "freed", "reclaimed", "reclaim_pending", \
              "rcache_hits", "rcache_misses", "rcache_hit_ratio"

    def __init__(self, settings):
        super().__init__(settings)
        self.index_node = settings.index_node

    def get_all_indexes(self):
        for index in self.indexes:
            yield index, self.buckets[0]

    def _get_secondary_storage_stats(self):
        server = self.index_node
        port = '9102'
        uri = "/stats/storage"
        samples = self.get_http(path=uri, server=server, port=port)
        index_stats = dict()
        for sample in samples:
            stats = dict()
            index = sample["Index"].split(":")[1]

            for store in sample["Stats"]:
                for metric, value in sample["Stats"][store].items():
                    if metric in self.METRICS:
                        key = store + "_" + metric
                        stats[key] = value
            index_stats[index] = stats
        return index_stats

    def sample(self):
        index_stats = self._get_secondary_storage_stats()
        if index_stats:
            for index, bucket in self.get_all_indexes():
                if index in index_stats and index_stats[index]:
                    stats = index_stats[index]
                    index1 = "{}.{}".format(bucket, index)
                    self.update_metric_metadata(stats.keys(), index=index1)
                    self.store.append(stats, cluster=self.cluster,
                                      index=index1, collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
        for index, bucket in self.get_all_indexes():
            self.mc.add_index("{}.{}".format(bucket, index))

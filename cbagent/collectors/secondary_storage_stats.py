from cbagent.collectors.collector import Collector


class SecondaryStorageStats(Collector):

    COLLECTOR = "secondary_storage_stats"

    METRICS = (
        "cache_hit_ratio",
        "compress_cache_hit_ratio",
        "compressed_percent",
        "lss_stats/lss_fragmentation",
        "memory_size",
        "rcache_hit_ratio",
        "rcompress_cache_hit_ratio",
        "reclaim_pending",
        "resident_ratio",
        "write_amp",
        "mvcc_purge_ratio",
        "memory_quota",
        "lss_blk_read_bs",
        "lss_stats/lss_blk_gc_reads_bs",
        "lss_blk_rdr_reads_bs",
        "rlss_num_reads",
        "avg_throttle_dur_reader",
        "avg_throttle_dur_writer",
        "avg_throttle_dur_other",
        "inserts",
        "burst_eviction_rate",
        "periodic_eviction_rate",
        "eviction_rate_reader",
        "eviction_rate_writer",
        "eviction_rate_swapper",
        "eviction_rate_other",
    )

    def __init__(self, settings):
        super().__init__(settings)
        self.index_node = settings.index_node

    def _get_secondary_storage_stats(self):
        server = self.index_node
        port = '9102'
        uri = "/stats/storage"
        samples = self.get_http(path=uri, server=server, port=port)
        index_stats = dict()
        for sample in samples:
            stats = dict()
            if "Index" not in sample:
                continue
            index = sample["Index"].split(":", 1)[1]

            for store in sample["Stats"]:
                for metric, value in sample["Stats"][store].items():
                    if metric == "lss_stats":
                        for lss_key, lss_value in sample["Stats"][store]["lss_stats"].items():
                            if "lss_stats/" + lss_key in self.METRICS:
                                key = store + "_" + lss_key
                                stats[key] = lss_value
                    elif metric in self.METRICS:
                        key = store + "_" + metric
                        stats[key] = value
            index_stats[index] = stats
        return index_stats

    def sample(self):
        index_stats = self._get_secondary_storage_stats()
        if index_stats:
            for index, bucket, scope, collection in self.get_all_indexes():
                index_name = index
                if scope and collection and scope != "_default" and collection != "_default":
                    index = "{}:{}:{}".format(scope, collection, index)
                if index in index_stats and index_stats[index]:
                    stats = index_stats[index]
                    index1 = "{}.{}".format(bucket, index_name)
                    self.update_metric_metadata(stats.keys(), index=index1)
                    self.store.append(stats, cluster=self.cluster,
                                      index=index1, collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
        for index, bucket, scope, collection in self.get_all_indexes():
            self.mc.add_index("{}.{}".format(bucket, index))

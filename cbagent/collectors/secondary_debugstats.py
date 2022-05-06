from cbagent.collectors.collector import Collector


class SecondaryDebugStats(Collector):

    COLLECTOR = "secondary_debugstats"

    METRICS = (
        "memory_used",
        "memory_used_queue",
        "memory_used_storage",
        "num_connections",
    )

    PORT = 9102

    def __init__(self, settings):
        super().__init__(settings)
        self.index_node = settings.index_node

    def parse_timings(self, value: str) -> float:
        # timings attribute has 3 space separated values - count, sum, sum of
        # square.
        stats = [int(x) for x in value.split(" ")]
        if len(stats) == 3 and stats[0] > 0:
            return stats[1] / stats[0]  # Compute average value as sum/count.
        return 0

    def get_stats(self) -> dict:
        return self.get_http(path='/stats',
                             server=self.index_node,
                             port=self.PORT)

    def _get_secondary_debugstats(self, bucket=None, index=None) -> dict:
        stats = self.get_stats()

        samples = dict()
        for metric in self.METRICS:
            _metric = bucket and "{}:{}".format(bucket, metric) or metric
            _metric = index and "{}:{}:{}".format(bucket, index, metric) or _metric

            if _metric in stats:
                value = stats[_metric]
                if metric.startswith("timings/"):
                    value = self.parse_timings(value)
                samples[metric.replace('/', '_')] = value
        return samples

    def sample(self):
        stats = self._get_secondary_debugstats()
        if stats:
            self.update_metric_metadata(self.METRICS)
            self.store.append(stats, cluster=self.cluster,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()


class SecondaryDebugStatsBucket(SecondaryDebugStats):

    COLLECTOR = "secondary_debugstats_bucket"

    METRICS = (
        "mutation_queue_size",
        "num_nonalign_ts",
        "ts_queue_size",
    )

    def sample(self):
        for bucket in self.get_buckets():
            stats = self._get_secondary_debugstats(bucket=bucket)
            if stats:
                self.update_metric_metadata(self.METRICS, bucket=bucket)
                self.store.append(stats, cluster=self.cluster, bucket=bucket,
                                  collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)


class SecondaryDebugStatsIndex(SecondaryDebugStats):

    COLLECTOR = "secondary_debugstats_index"

    METRICS = (
        "avg_scan_latency",
        "avg_scan_wait_latency",
        "avg_ts_interval",
        "avg_ts_items_count",
        "disk_store_duration",
        "flush_queue_size",
        "num_compactions",
        "num_completed_requests",
        "num_docs_indexed",
        "num_rows_returned",
        "num_rows_scanned_aggr",
        "scan_cache_hit_aggr",
        "timings/dcp_getseqs",
        "timings/storage_commit",
        "timings/storage_del",
        "timings/storage_get",
        "timings/storage_set",
        "timings/storage_snapshot_create",
    )

    def sample(self):
        for index, bucket, scope, collection in self.get_all_indexes():
            if scope and collection and \
                            scope != "_default" and collection != "_default":
                full_index_name = "{}:{}:{}".format(scope, collection, index)
                stats = self._get_secondary_debugstats(bucket=bucket, index=full_index_name)
            else:
                stats = self._get_secondary_debugstats(bucket=bucket, index=index)
            if stats:
                _index = "{}.{}".format(bucket, index)
                self.update_metric_metadata(self.METRICS, index=_index)
                self.store.append(stats,
                                  cluster=self.cluster,
                                  index=_index,
                                  collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
        for index, bucket, scope, collection in self.get_all_indexes():
            self.mc.add_index("{}.{}".format(bucket, index))

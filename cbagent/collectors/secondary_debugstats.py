from cbagent.collectors import Collector


class SecondaryDebugStats(Collector):

    COLLECTOR = "secondary_debugstats"

    METRICS = "num_connections", "memory_used_storage", "memory_used_queue"

    def __init__(self, settings):
        super(SecondaryDebugStats, self).__init__(settings)
        self.index_node = settings.index_node

    def _get_secondary_debugstats(self, bucket=None, index=None):
        server = self.index_node
        port = '9102'
        uri = "/stats"
        samples = self.get_http(path=uri, server=server, port=port)
        stats = dict()
        for metric in self.METRICS:
            metric1 = "{}:{}".format(bucket, metric) if bucket else metric
            metric1 = "{}:{}:{}".format(bucket, index, metric) if index else metric1
            value = 0
            if metric1 in samples:
                value = samples[metric1]
                # timings attribute has 3 space separated values-count, sum, sum of square
                # Compute average for timing metrics(CBPS-135) by sum/count.
                if metric.startswith("timings/"):
                    values = [int(x) for x in value.split(" ")]
                    value = values[1] / values[0] if len(values) == 3 and values[0] is not 0 else 0
            stats[metric.replace('/', '_')] = value
        return stats

    def sample(self):
        stats = self._get_secondary_debugstats()
        if stats:
            self.update_metric_metadata(self.METRICS)
            self.store.append(stats, cluster=self.cluster,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

    def get_all_indexes(self):
        for index in self.indexes:
            yield index, self.buckets[0]


class SecondaryDebugStatsBucket(SecondaryDebugStats):

    COLLECTOR = "secondary_debugstats_bucket"

    METRICS = "mutation_queue_size", "num_nonalign_ts", "ts_queue_size"

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
        "avg_ts_interval",
        "num_completed_requests",
        "avg_ts_items_count",
        "num_compactions",
        "num_rows_returned",
        "flush_queue_size",
        "avg_scan_wait_latency",
        "timings/storage_commit",
        "timings/storage_del",
        "timings/storage_get",
        "timings/storage_set",
        "timings/storage_snapshot_create",
        "timings/dcp_getseqs",
    )

    def sample(self):
        for index, bucket in self.get_all_indexes():
            stats = self._get_secondary_debugstats(bucket=bucket, index=index)
            if stats:
                index1 = "{}.{}".format(bucket, index)
                self.update_metric_metadata(self.METRICS, index=index1)
                self.store.append(stats, cluster=self.cluster,
                                  index=index1, collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
        for index, bucket in self.get_all_indexes():
            self.mc.add_index("{}.{}".format(bucket, index))

from cbagent.collectors import Collector


class SecondaryDebugStats(Collector):
    COLLECTOR = "secondary_debugstats"

    METRICS = ("num_connections", "memory_used_storage", "memory_used_queue")

    def _get_secondary_debugstats(self, bucket=None):
        server = self.index_node
        port = '9102'
        uri = "/stats"
        samples = self.get_http(path=uri, server=server, port=port)
        stats = dict()
        for metric in self.METRICS:
            metric1 = "{}:{}".format(bucket, metric) if bucket else metric
            value = samples[metric1]
            stats[metric] = value
        return stats

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

    METRICS = ("mutation_queue_size", "num_nonalign_ts", "ts_queue_size")

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

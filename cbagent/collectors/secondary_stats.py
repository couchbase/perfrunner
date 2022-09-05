from cbagent.collectors.collector import Collector


class SecondaryStats(Collector):

    COLLECTOR = "secondary_stats"

    def _get_secondary_stats(self, bucket):
        uri = "/pools/default/buckets/@index-{}/stats".format(bucket)
        samples = self.get_http(path=uri)
        stats = dict()
        for metric, values in samples['op']['samples'].items():
            metric = metric.replace('/', '_')
            stats[metric] = values[-1]  # only the most recent sample
        return stats

    def sample(self):
        for bucket in self.get_buckets():
            stats = self._get_secondary_stats(bucket)
            if stats:
                self.update_metric_metadata(stats.keys(), bucket=bucket)
                self.append_to_store(stats, cluster=self.cluster,
                                     bucket=bucket, collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)

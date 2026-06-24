from cbagent.collectors.collector import CouchbaseCollector


class SecondaryStats(CouchbaseCollector):

    COLLECTOR = "secondary_stats"
    COLLECTOR_FLAG = "secondary_stats"
    SKIP_ON_DYNAMIC = True

    def _get_secondary_stats(self, bucket):
        uri = "/pools/default/buckets/@index-{}/stats".format(bucket)
        samples = self.get_http(path=uri)
        stats = dict()
        for metric, values in samples['op']['samples'].items():
            if not values:
                # The index-stats endpoint can return a metric whose sample series
                # is still empty (early in a phase, or right after the index service
                # starts). Skip it rather than indexing into an empty list — it will
                # be picked up on a later sample once data is available.
                continue
            metric = metric.replace('/', '_')
            stats[metric] = values[-1]  # only the most recent sample
        return stats

    def sample(self):
        for bucket in self.get_buckets():
            stats = self._get_secondary_stats(bucket)
            if stats:
                self.update_metric_metadata(stats.keys(), bucket=bucket)
                self.store.append(
                    stats, cluster=self.cluster, bucket=bucket, collector=self.COLLECTOR
                )

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)

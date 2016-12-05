from cbagent.collectors import Collector


class NSServer(Collector):

    COLLECTOR = "ns_server"

    def _get_stats_uri(self):
        for bucket, stats in self.get_buckets(with_stats=True):
            uri = stats["uri"]
            yield uri, bucket  # cluster wide

    def _get_stats(self, uri):
        samples = self.get_http(path=uri)  # get last minute samples
        stats = dict()

        if samples["op"]["lastTStamp"] == 0:
            # Index and N1QL nodes don't have stats in ns_server
            return None

        for metric, values in samples['op']['samples'].iteritems():
            metric = metric.replace('/', '_')
            stats[metric] = values[-1]  # only the most recent sample
        return stats

    def sample(self):
        for uri, bucket in self._get_stats_uri():
            stats = self._get_stats(uri)
            if not stats:
                continue
            self.update_metric_metadata(stats.keys(), bucket)
            self.store.append(stats, cluster=self.cluster, bucket=bucket,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)

        for node in self.get_nodes():
            self.mc.add_server(node)


class XdcrStats(Collector):

    COLLECTOR = "xdcr_stats"

    def _get_stats_uri(self):
        for bucket in self.get_buckets():
            uri = '/_uistats?bucket={}&zoom=minute'.format(bucket)
            yield bucket, uri

    def _get_stats(self, bucket, uri):
        samples = self.get_http(path=uri)

        stats = dict()
        for metric, values in samples['stats']['@xdcr-{}'.format(bucket)].items():
            if 'replications' in metric:
                metric = metric.split('/')[-1]
                stats[metric] = values[-1]
        return stats

    def sample(self):
        for bucket, uri in self._get_stats_uri():
            stats = self._get_stats(bucket, uri)
            if not stats:
                continue
            self.update_metric_metadata(stats.keys(), bucket)
            self.store.append(stats, cluster=self.cluster, bucket=bucket,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)

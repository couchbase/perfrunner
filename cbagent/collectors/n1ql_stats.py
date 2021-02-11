from cbagent.collectors.collector import Collector


class N1QLStats(Collector):

    COLLECTOR = "n1ql_stats"

    def _get_n1ql_stats(self):
        samples = self.get_http(path="/pools/default/buckets/@query/stats")
        stats = dict()
        for metric, values in samples['op']['samples'].items():
            metric = metric.replace('/', '_')
            stats[metric] = values[-1]  # only the most recent sample
        return stats

    def sample(self):
        stats = self._get_n1ql_stats()
        if stats:
            self.update_metric_metadata(stats.keys())
            self.store.append(stats,
                              cluster=self.cluster,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

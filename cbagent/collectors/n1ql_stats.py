from cbagent.collectors import Collector

class N1QLStats(Collector):

    COLLECTOR = "n1ql_stats"

    def _get_n1ql_stats(self):
        server = self.master_node
        server = server.split(':')[0]
        qsamples = self.get_http(path="/pools/default/buckets/@query/stats",
                                 server=server)
        qstats = dict()
        for metric, values in qsamples['op']['samples'].iteritems():
            metric = metric.replace('/', '_')
            qstats[metric] = values[-1]  # only the most recent sample
        return qstats

    def sample(self):
        stats = self._get_n1ql_stats()
        if stats:
            self.update_metric_metadata(stats.keys())
            self.store.append(stats,
                              cluster=self.cluster,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

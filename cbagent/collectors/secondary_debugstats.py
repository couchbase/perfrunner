from cbagent.collectors import Collector
from logger import logger

class SecondaryDebugStats(Collector):

    COLLECTOR = "secondary_debugstats"

    def _get_secondary_debugstats(self):
        server = self.index_node
        port = '9102'
        uri = "/stats"
        samples = self.get_http(path=uri, server=server, port=port)
        stats = dict()
        for metric, values in samples.iteritems():
            metric = metric.replace('/', '_')
            stats[metric] = values  # only the most recent sample
        return stats

    def sample(self):
         stats = self._get_secondary_debugstats()
         if stats:
             self.update_metric_metadata(stats.keys())
             self.store.append(stats,cluster=self.cluster,
                                 collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

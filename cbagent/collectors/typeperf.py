from cbagent.collectors.libstats.typeperfstats import TPStats
from cbagent.collectors import Collector


class TypePerf(Collector):

    COLLECTOR = "atop"  # Legacy

    KNOWN_PROCESSES = ("beam.smp","memcached")

    def __init__(self, settings):
        super(TypePerf, self).__init__(settings)
        self.nodes = settings.hostnames or list(self.get_nodes())
        if hasattr(settings, "sync_gateway_nodes") and settings.sync_gateway_nodes:
            self.nodes += settings.sync_gateway_nodes
        self.tp = TPStats(hosts=self.nodes,
                          user=self.ssh_username,
                          password=self.ssh_password)

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes:
            self.mc.add_server(node)

    def sample(self):
        for process in self.KNOWN_PROCESSES:
            for node, stats in self.tp.get_samples(process).items():
                if stats:
                    self.update_metric_metadata(stats.keys(), server=node)
                    self.store.append(stats,
                                      cluster=self.cluster, server=node,
                                      collector=self.COLLECTOR)

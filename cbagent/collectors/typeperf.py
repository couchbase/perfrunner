from cbagent.collectors import Collector
from cbagent.collectors.libstats.typeperfstats import TPStats


class TypePerf(Collector):

    COLLECTOR = "atop"  # Legacy / compatibility

    TRACKED_PROCESSES = "beam.smp", "memcached"

    def __init__(self, settings):
        super().__init__(settings)
        self.nodes = settings.hostnames or list(self.get_nodes())
        self.tp = TPStats(hosts=self.nodes,
                          user=self.ssh_username,
                          password=self.ssh_password)
        self.monitored_processes = settings.monitored_processes

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes:
            self.mc.add_server(node)

    def sample(self):
        for process in self.monitored_processes:
            for node, stats in self.tp.get_samples(process).items():
                if stats:
                    self.update_metric_metadata(stats.keys(), server=node)
                    self.store.append(stats,
                                      cluster=self.cluster, server=node,
                                      collector=self.COLLECTOR)

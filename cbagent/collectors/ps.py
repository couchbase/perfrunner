from cbagent.collectors.libstats.psstats import PSStats
from cbagent.collectors import Collector


class PS(Collector):

    COLLECTOR = "atop"  # Legacy

    KNOWN_PROCESSES = ("beam.smp", "memcached", "indexer", "projector",
                       "cbq-engine", "sync_gateway")

    def __init__(self, settings):
        super(PS, self).__init__(settings)
        self.nodes = settings.hostnames or list(self.get_nodes())
        if hasattr(settings, "sync_gateway_nodes") and settings.sync_gateway_nodes:
            self.nodes += settings.sync_gateway_nodes
        if hasattr(settings, "monitor_clients") and settings.monitor_clients\
                and settings.master_node in settings.monitor_clients:
            self.nodes = settings.monitor_clients
            self.KNOWN_PROCESSES = ("backup", "cbbackupwrapper", )
        if hasattr(settings, "fts_server"):
            self.KNOWN_PROCESSES = ("beam.smp", "memcached", "cbft",)
        self.ps = PSStats(hosts=self.nodes,
                          user=self.ssh_username,
                          password=self.ssh_password)

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes:
            self.mc.add_server(node)

    def sample(self):
        for process in self.KNOWN_PROCESSES:
            for node, stats in self.ps.get_samples(process).items():
                if stats:
                    self.update_metric_metadata(stats.keys(), server=node)
                    self.store.append(stats,
                                      cluster=self.cluster, server=node,
                                      collector=self.COLLECTOR)

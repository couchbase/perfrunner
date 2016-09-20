from cbagent.collectors import Collector
from cbagent.collectors.libstats.iostat import IOstat


class IO(Collector):

    COLLECTOR = "iostat"

    def get_nodes(self):
        return self.settings.hostnames or super(IO, self).get_nodes()

    def __init__(self, settings):
        self.settings = settings
        self.partitions = settings.partitions

        super(IO, self).__init__(settings)

        self.io = IOstat(hosts=self.nodes,
                         user=self.ssh_username,
                         password=self.ssh_password)

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes:
            self.mc.add_server(node)

    def sample(self):
        for node, stats in self.io.get_samples(self.partitions).items():
            if stats:
                self.update_metric_metadata(stats.keys(), server=node)
                self.store.append(stats,
                                  cluster=self.cluster, server=node,
                                  collector=self.COLLECTOR)

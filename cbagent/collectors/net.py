from cbagent.collectors.libstats.net import NetStat
from cbagent.collectors import Collector


class Net(Collector):

    COLLECTOR = "net"

    def __init__(self, settings):
        super(Net, self).__init__(settings)
        self.nodes = settings.hostnames or list(self.get_nodes())
        self.net = NetStat(hosts=self.nodes,
                           user=self.ssh_username,
                           password=self.ssh_password)

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes:
            self.mc.add_server(node)

    def sample(self):
        for node, stats in self.net.get_samples().items():
            self.update_metric_metadata(stats.keys(), server=node)
            self.store.append(stats,
                              cluster=self.cluster, server=node,
                              collector=self.COLLECTOR)

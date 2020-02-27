from cbagent.collectors import Collector
from cbagent.collectors.libstats.iostat import DiskStats, IOStat
from cbagent.collectors.libstats.meminfo import MemInfo
from cbagent.collectors.libstats.net import NetStat
from cbagent.collectors.libstats.pcstat import PCStat
from cbagent.collectors.libstats.psstats import PSStats
from cbagent.collectors.libstats.sysdig import SysdigStat
from cbagent.collectors.libstats.typeperfstats import TPStats
from cbagent.collectors.libstats.vmstat import VMStat


class System(Collector):

    def get_nodes(self):
        return self.settings.hostnames or super().get_nodes()

    def update_metadata(self):
        self.mc.add_cluster()
        for node in self.nodes + self.workers:
            self.mc.add_server(node)

    def add_stats(self, node, stats):
        if stats:
            self.update_metric_metadata(stats.keys(), server=node)
            self.store.append(stats,
                              cluster=self.cluster, server=node,
                              collector=self.COLLECTOR)

    def sample(self):
        raise NotImplementedError

    def __init__(self, settings):
        self.settings = settings

        super().__init__(settings)


class PS(System):

    COLLECTOR = "atop"  # Legacy / compatibility

    def __init__(self, settings):
        super().__init__(settings)

        self.sampler = PSStats(hosts=self.nodes,
                               workers=self.workers,
                               user=self.ssh_username,
                               password=self.ssh_password,
                               interval=self.interval)

    def sample(self):
        for process in self.settings.server_processes:
            for node, stats in self.sampler.get_server_samples(process).items():
                self.add_stats(node, stats)

        for process in self.settings.client_processes:
            for node, stats in self.sampler.get_client_samples(process).items():
                self.add_stats(node, stats)


class IO(System):

    COLLECTOR = "iostat"

    def __init__(self, settings):
        super().__init__(settings)

        self.partitions = settings.partitions

        self.sampler = IOStat(hosts=self.nodes,
                              workers=self.workers,
                              user=self.ssh_username,
                              password=self.ssh_password)

    def sample(self):
        for node, stats in self.sampler.get_server_samples(self.partitions).items():
            self.add_stats(node, stats)

        for node, stats in self.sampler.get_client_samples(self.partitions).items():
            self.add_stats(node, stats)


class Disk(System):

    COLLECTOR = "disk"

    def __init__(self, settings):
        super().__init__(settings)

        self.partitions = settings.partitions

        self.sampler = DiskStats(hosts=self.nodes,
                                 workers=self.workers,
                                 user=self.ssh_username,
                                 password=self.ssh_password)

        self.initial_stats = {}

    def sample(self):
        for node, stats in self.sampler.get_server_samples(self.partitions).items():
            if not self.initial_stats.get(node):
                self.initial_stats[node] = stats.copy()
            for metric, value in stats.items():
                stats[metric] -= self.initial_stats[node][metric]
            self.add_stats(node, stats)


class PageCache(System):

    COLLECTOR = "pcstat"

    def __init__(self, settings):
        super().__init__(settings)

        self.partitions = settings.partitions['server']

        self.sampler = PCStat(hosts=self.nodes,
                              workers=self.workers,
                              user=self.ssh_username,
                              password=self.ssh_password)

    def sample(self):
        for node, stats in self.sampler.get_samples(self.partitions).items():
            self.add_stats(node, stats)


class Net(System):

    COLLECTOR = "net"

    def __init__(self, settings):
        super().__init__(settings)

        self.sampler = NetStat(hosts=self.nodes,
                               workers=self.workers,
                               user=self.ssh_username,
                               password=self.ssh_password)

    def sample(self):
        for node, stats in self.sampler.get_samples().items():
            self.add_stats(node, stats)


class TypePerf(PS):

    def __init__(self, settings):
        super().__init__(settings)

        self.sampler = TPStats(hosts=self.nodes,
                               workers=self.workers,
                               user=self.ssh_username,
                               password=self.ssh_password)


class Sysdig(System):

    COLLECTOR = "sysdig"

    def __init__(self, settings):
        super().__init__(settings)

        self.sampler = SysdigStat(hosts=self.nodes,
                                  workers=self.workers,
                                  user=self.ssh_username,
                                  password=self.ssh_password)

    def sample(self):
        processes = self.settings.traced_processes
        for node, stats in self.sampler.get_samples(processes).items():
            self.add_stats(node, stats)


class Memory(System):

    COLLECTOR = 'meminfo'

    def __init__(self, settings):
        super().__init__(settings)

        self.sampler = MemInfo(hosts=self.nodes,
                               workers=self.workers,
                               user=self.ssh_username,
                               password=self.ssh_password)

    def sample(self):
        for node, stats in self.sampler.get_samples().items():
            self.add_stats(node, stats)


class VMSTAT(System):

    COLLECTOR = 'vmstat'

    def __init__(self, settings):
        super().__init__(settings)

        self.sampler = VMStat(hosts=self.nodes,
                              workers=self.workers,
                              user=self.ssh_username,
                              password=self.ssh_password)

    def sample(self):
        for node, stats in self.sampler.get_samples().items():
            self.add_stats(node, stats)

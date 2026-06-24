from typing import Optional

from cbagent.collectors.collector import CouchbaseCollector
from cbagent.collectors.libstats.iostat import DiskStats, IOStat
from cbagent.collectors.libstats.meminfo import MemInfo
from cbagent.collectors.libstats.net import NetStat
from cbagent.collectors.libstats.pcstat import PCStat
from cbagent.collectors.libstats.psstats import PSStats
from cbagent.collectors.libstats.sysdig import SysdigStat
from cbagent.collectors.libstats.typeperfstats import TPStats
from cbagent.collectors.libstats.vmstat import VMStat
from cbagent.settings import CbAgentSettings
from perfrunner.tests import PerfTest


class System(CouchbaseCollector):
    ABSTRACT = True

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

    def __init__(self, settings, test: Optional[PerfTest] = None):
        self.settings = settings

        super().__init__(settings)


class IOSystem(System):
    """Base for collectors that need IO partitions (IO, Disk, PageCache)."""

    ABSTRACT = True

    @classmethod
    def _build_partitions(cls, test: PerfTest):
        partitions = {
            "client": {},
            "server": {"data": test.cluster_spec.data_path},
        }
        if test.test_config.showfast.component == "views":
            partitions["server"]["index"] = test.cluster_spec.index_path
        elif test.test_config.showfast.component == "tools":
            partitions["client"]["tools"] = test.cluster_spec.backup
        elif test.test_config.showfast.component == "analytics":
            for i, path in enumerate(test.cluster_spec.analytics_paths):
                partitions["server"][f"analytics{i}"] = path
        return partitions

    @classmethod
    def create_instances(cls, test: PerfTest, cluster_map: dict):
        partitions = cls._build_partitions(test)
        instances = []
        for cluster_id, master_node in cluster_map.items():
            settings = CbAgentSettings(test)
            settings.cluster = cluster_id
            settings.master_node = master_node
            settings.partitions = partitions
            instances.append(cls(settings, test))
        return instances


class PS(System):
    COLLECTOR = "atop"  # Legacy / compatibility
    ALWAYS_ON = True
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
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


class IO(IOSystem):

    COLLECTOR = "iostat"
    COLLECTOR_FLAG = "iostat"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True
    REQUIRES_NON_CLOUD = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
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


class Disk(IOSystem):

    COLLECTOR = "disk"
    COLLECTOR_FLAG = "disk"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True
    REQUIRES_NON_CLOUD = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
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


class PageCache(IOSystem):

    COLLECTOR = "pcstat"
    COLLECTOR_FLAG = "page_cache"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
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
    COLLECTOR_FLAG = "net"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
        super().__init__(settings)

        self.sampler = NetStat(hosts=self.nodes,
                               workers=self.workers,
                               user=self.ssh_username,
                               password=self.ssh_password)

    def sample(self):
        for node, stats in self.sampler.get_samples().items():
            self.add_stats(node, stats)


class TypePerf(PS):
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = False
    REQUIRES_CYGWIN = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
        super().__init__(settings)

        self.sampler = TPStats(hosts=self.nodes,
                               workers=self.workers,
                               user=self.ssh_username,
                               password=self.ssh_password)


class Sysdig(System):
    COLLECTOR = "sysdig"
    ALWAYS_ON = True
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
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
    COLLECTOR_FLAG = "memory"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
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
    COLLECTOR_FLAG = "vmstat"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True

    def __init__(self, settings, test: Optional[PerfTest] = None):
        super().__init__(settings)

        self.sampler = VMStat(hosts=self.nodes,
                              workers=self.workers,
                              user=self.ssh_username,
                              password=self.ssh_password)

    def sample(self):
        for node, stats in self.sampler.get_samples().items():
            self.add_stats(node, stats)

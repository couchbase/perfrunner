import time
from optparse import OptionParser

from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.misc import server_group
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.tests import TargetIterator


class ClusterManager(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec)
        self.monitor = Monitor(cluster_spec)
        self.memcached = MemcachedHelper(cluster_spec)

        self.clusters = cluster_spec.yield_clusters()
        self.servers = cluster_spec.yield_servers
        self.masters = cluster_spec.yield_masters
        self.hostnames = cluster_spec.yield_hostnames

        self.initial_nodes = test_config.initial_nodes
        self.mem_quota = test_config.mem_quota
        self.group_number = test_config.group_number or 1

    def set_data_path(self):
        data_path, index_path = self.cluster_spec.paths
        for server in self.servers():
            self.rest.set_data_path(server, data_path, index_path)

    def set_auth(self):
        for server in self.servers():
            self.rest.set_auth(server)

    def set_mem_quota(self):
        for server in self.servers():
            self.rest.set_mem_quota(server, self.mem_quota)

    def create_server_groups(self):
        for master in self.masters():
            for i in range(1, self.group_number):
                name = 'Group {}'.format(i + 1)
                self.rest.create_server_group(master, name=name)

    def add_nodes(self):
        for (_, servers), initial_nodes in zip(self.clusters,
                                               self.initial_nodes):
            if initial_nodes < 2:  # Single-node cluster
                continue

            # Adding initial nodes
            master = servers[0]
            if self.group_number > 1:
                groups = self.rest.get_server_groups(master)
            else:
                groups = {}
            for i, host_port in enumerate(servers[1:initial_nodes],
                                          start=1):
                host = host_port.split(':')[0]
                uri = groups.get(server_group(servers[:initial_nodes],
                                              self.group_number, i))
                self.rest.add_node(master, host, uri)

            # Rebalance
            master = servers[0]
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)

    def create_buckets(self):
        threads_number = self.test_config.mrw_threads_number
        replica_number = self.test_config.replica_number
        if replica_number is None:
            replica_number = 1

        num_buckets = self.test_config.num_buckets
        ram_quota = self.mem_quota / num_buckets
        buckets = ['bucket-{}'.format(i + 1) for i in xrange(num_buckets)]

        for master in self.masters():
            for name in buckets:
                self.rest.create_bucket(master, name, ram_quota,
                                        replica_number=replica_number,
                                        threads_number=threads_number)

    def configure_auto_compaction(self):
        compaction_settings = self.test_config.compaction_settings
        for master in self.masters():
            self.rest.configure_auto_compaction(master, compaction_settings)

    def configure_internal_settings(self):
        internal_settings = self.test_config.internal_settings
        for master in self.masters():
            for parameter, value in internal_settings.items():
                self.rest.set_internal_settings(master,
                                                {parameter: int(value)})

    def clean_memory(self):
        self.remote.reset_swap()
        self.remote.drop_caches()
        self.remote.set_swappiness()

    def restart_with_alternative_swt(self):
        swt = self.test_config.swt
        if swt is not None:
            self.remote.restart_with_alternative_swt(swt)

    def restart_with_alternative_num_vbuckets(self):
        num_vbuckets = self.test_config.num_vbuckets
        if num_vbuckets is not None:
            self.remote.restart_with_alternative_num_vbuckets(num_vbuckets)

    def enable_auto_failover(self):
        for master in self.masters():
            self.rest.enable_auto_failover(master)

    def wait_until_warmed_up(self):
        target_iterator = TargetIterator(self.cluster_spec, self.test_config)
        for target in target_iterator:
            host = target.node.split(':')[0]
            self.monitor.monitor_warmup(self.memcached, host, target.bucket)

    def change_watermarks(self):
        watermark_settings = self.test_config.watermark_settings
        for hostname, initial_nodes in zip(self.hostnames(),
                                           self.initial_nodes):
            for bucket in self.test_config.buckets:
                for key, val in watermark_settings.items():
                    val = self.memcached.calc_watermark(val, self.mem_quota)
                    self.memcached.set_flusher_param(hostname, bucket, key, val)


def get_options():
    usage = '%prog -c cluster -t test_config'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-t', dest='test_config_fname',
                      help='path to test configuration file',
                      metavar='my_test.test')

    options, args = parser.parse_args()
    if not options.cluster_spec_fname or not options.test_config_fname:
        parser.error('Missing mandatory parameter')

    return options, args


def main():
    options, args = get_options()
    override = args and (arg.split('.') for arg in ' '.join(args).split(','))

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname)
    test_config = TestConfig()
    test_config.parse(options.test_config_fname, override)

    cm = ClusterManager(cluster_spec, test_config)

    # Individual nodes
    cm.restart_with_alternative_swt()
    cm.restart_with_alternative_num_vbuckets()
    cm.configure_internal_settings()
    cm.set_data_path()
    cm.set_auth()
    cm.set_mem_quota()

    time.sleep(5)

    # Cluster
    if cm.group_number > 1:
        cm.create_server_groups()
    cm.add_nodes()
    cm.create_buckets()
    cm.wait_until_warmed_up()
    cm.configure_auto_compaction()
    cm.enable_auto_failover()
    cm.change_watermarks()
    cm.clean_memory()

if __name__ == '__main__':
    main()

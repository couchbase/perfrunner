import time
from optparse import OptionParser

from logger import logger

from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.misc import server_group
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.tests import TargetIterator


class ClusterManager(object):

    def __init__(self, cluster_spec, test_config, verbose):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)
        self.monitor = Monitor(cluster_spec)
        self.memcached = MemcachedHelper(test_config)

        self.clusters = cluster_spec.yield_clusters
        self.servers = cluster_spec.yield_servers
        self.masters = cluster_spec.yield_masters

        self.initial_nodes = test_config.cluster.initial_nodes
        self.mem_quota = test_config.cluster.mem_quota
        self.index_mem_quota = test_config.cluster.index_mem_quota
        self.group_number = test_config.cluster.group_number or 1
        self.roles = cluster_spec.roles

    def set_data_path(self):
        if self.cluster_spec.paths:
            data_path, index_path = self.cluster_spec.paths
            for server in self.servers():
                self.rest.set_data_path(server, data_path, index_path)

    def set_auth(self):
        for server in self.servers():
            self.rest.set_auth(server)

    def set_mem_quota(self):
        for server in self.servers():
            self.rest.set_mem_quota(server, self.mem_quota)

    def set_index_mem_quota(self):
        for server in self.servers():
            self.rest.set_index_mem_quota(server, self.index_mem_quota)

    def set_query_settings(self):
        settings = self.test_config.n1ql_settings.settings
        for _, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            for server in servers:
                self.rest.set_query_settings(server, settings)

    def set_index_settings(self):
        settings = self.test_config.secondaryindex_settings.settings
        for _, servers in self.cluster_spec.yield_servers_by_role('index'):
            for server in servers:
                self.rest.set_index_settings(server, settings)
        self.remote.restart()
        time.sleep(60)

    def set_services(self):
        for (_, servers), initial_nodes in zip(self.clusters(),
                                               self.initial_nodes):
            master = servers[0]
            self.rest.set_services(master, self.roles[master])

    def disable_moxi(self):
        if self.test_config.cluster.disable_moxi is not None:
            self.remote.disable_moxi()

    def create_server_groups(self):
        for master in self.masters():
            for i in range(1, self.group_number):
                name = 'Group {}'.format(i + 1)
                self.rest.create_server_group(master, name=name)

    def add_nodes(self):
        for (_, servers), initial_nodes in zip(self.clusters(),
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
                uri = groups.get(server_group(servers[:initial_nodes],
                                              self.group_number, i))
                self.rest.add_node(master, host_port, self.roles[host_port],
                                   uri)

            # Rebalance
            master = servers[0]
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)

    def create_buckets(self):
        ram_quota = self.mem_quota / self.test_config.cluster.num_buckets
        replica_number = self.test_config.bucket.replica_number
        replica_index = self.test_config.bucket.replica_index
        eviction_policy = self.test_config.bucket.eviction_policy
        threads_number = self.test_config.bucket.threads_number
        password = self.test_config.bucket.password

        for master in self.masters():
            for bucket_name in self.test_config.buckets:
                self.rest.create_bucket(host_port=master,
                                        name=bucket_name,
                                        ram_quota=ram_quota,
                                        replica_number=replica_number,
                                        replica_index=replica_index,
                                        eviction_policy=eviction_policy,
                                        threads_number=threads_number,
                                        password=password)

    def configure_auto_compaction(self):
        compaction_settings = self.test_config.compaction
        for master in self.masters():
            self.rest.configure_auto_compaction(master, compaction_settings)

    def configure_internal_settings(self):
        internal_settings = self.test_config.internal_settings
        for master in self.masters():
            for parameter, value in internal_settings.items():
                self.rest.set_internal_settings(master,
                                                {parameter: int(value)})

    def tweak_memory(self):
        self.remote.reset_swap()
        self.remote.drop_caches()
        self.remote.set_swappiness()
        self.remote.disable_thp()

    def restart_with_alternative_num_vbuckets(self):
        num_vbuckets = self.test_config.cluster.num_vbuckets
        if num_vbuckets is not None:
            self.remote.restart_with_alternative_num_vbuckets(num_vbuckets)

    def restart_with_alternative_bucket_options(self):
        cmd = 'ns_bucket:update_bucket_props("{}", ' \
              '[{{extra_config_string, "{}={}"}}]).'

        for option in ('defragmenter_enabled',
                       'exp_pager_stime',
                       'ht_locks',
                       'max_num_shards',
                       'max_threads',
                       'warmup_min_memory_threshold',
                       'bfilter_enabled'):
            value = getattr(self.test_config.bucket, option)
            if value != -1 and value is not None:
                logger.info('Changing {} to {}'.format(option, value))
                for master in self.masters():
                    for bucket in self.test_config.buckets:
                        diag_eval = cmd.format(bucket, option, value)
                        self.rest.run_diag_eval(master, diag_eval)
                self.remote.restart()

    def tune_logging(self):
        self.remote.tune_log_rotation()
        self.remote.restart()

    def restart_with_alternative_num_cpus(self):
        num_cpus = self.test_config.cluster.num_cpus
        if num_cpus:
            self.remote.restart_with_alternative_num_cpus(num_cpus)

    def restart_with_tcmalloc_aggressive_decommit(self):
        if self.test_config.cluster.tcmalloc_aggressive_decommit:
            self.remote.restart_with_tcmalloc_aggressive_decommit()

    def restart_with_sfwi(self):
        if self.test_config.cluster.sfwi:
            self.remote.restart_with_sfwi()

    def enable_auto_failover(self):
        for master in self.masters():
            self.rest.enable_auto_failover(master)

    def wait_until_warmed_up(self):
        target_iterator = TargetIterator(self.cluster_spec, self.test_config)
        for target in target_iterator:
            self.monitor.monitor_warmup(self.memcached, target.node,
                                        target.bucket)

    def wait_until_healthy(self):
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_node_health(master)

    def change_watermarks(self):
        watermark_settings = self.test_config.watermark_settings
        for host_port, initial_nodes in zip(self.servers(),
                                            self.initial_nodes):
            host = host_port.split(':')[0]
            memcached_port = self.rest.get_memcached_port(host_port)
            for bucket in self.test_config.buckets:
                for key, val in watermark_settings.items():
                    val = self.memcached.calc_watermark(val, self.mem_quota)
                    self.memcached.set_flusher_param(host, memcached_port,
                                                     bucket, key, val)

    def start_cbq_engine(self):
        if self.test_config.cluster.run_cbq:
            self.remote.start_cbq()


def get_options():
    usage = '%prog -c cluster -t test_config'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-t', dest='test_config_fname',
                      help='path to test configuration file',
                      metavar='my_test.test')
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')

    options, args = parser.parse_args()
    if not options.cluster_spec_fname or not options.test_config_fname:
        parser.error('Missing mandatory parameter')

    return options, args


def main():
    options, args = get_options()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, args)
    test_config = TestConfig()
    test_config.parse(options.test_config_fname, args)

    cm = ClusterManager(cluster_spec, test_config, options.verbose)

    # Individual nodes
    if cm.remote:
        cm.tune_logging()
        cm.restart_with_sfwi()
        cm.restart_with_alternative_num_vbuckets()
        cm.restart_with_alternative_num_cpus()
        cm.restart_with_tcmalloc_aggressive_decommit()
        cm.disable_moxi()
    cm.configure_internal_settings()
    cm.set_data_path()
    cm.set_services()
    cm.set_mem_quota()
    cm.set_index_mem_quota()
    cm.set_auth()

    time.sleep(30)  # crutch

    # Cluster
    if cm.group_number > 1:
        cm.create_server_groups()
    cm.add_nodes()
    if cm.test_config.cluster.num_buckets:
        cm.create_buckets()
    if cm.remote:
        cm.restart_with_alternative_bucket_options()
    cm.wait_until_warmed_up()
    cm.wait_until_healthy()
    cm.configure_auto_compaction()
    cm.enable_auto_failover()
    cm.change_watermarks()
    if cm.remote:
        cm.set_index_settings()
        time.sleep(60)
        cm.set_query_settings()
        cm.tweak_memory()
        cm.remote.disable_wan()
        cm.start_cbq_engine()

if __name__ == '__main__':
    main()

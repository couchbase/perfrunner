import time

from logger import logger

from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.misc import pretty_dict, server_group
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper


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
        self.fts_index_mem_quota = test_config.cluster.fts_index_mem_quota
        self.group_number = test_config.cluster.group_number or 1
        self.roles = cluster_spec.roles

    def set_data_path(self):
        if self.cluster_spec.paths:
            data_path, index_path = self.cluster_spec.paths
            for server in self.servers():
                self.rest.set_data_path(server, data_path, index_path)

    def rename(self):
        for server in self.servers():
            self.rest.rename(server)

    def set_auth(self):
        for server in self.servers():
            self.rest.set_auth(server)

    def set_mem_quota(self):
        for server in self.servers():
            self.rest.set_mem_quota(server, self.mem_quota)

    def set_index_mem_quota(self):
        for server in self.servers():
            self.rest.set_index_mem_quota(server, self.index_mem_quota)

    def set_fts_index_mem_quota(self):
        master_node = self.masters().next()
        version = self.rest.get_version(master_node)
        if version >= '4.5.0':  # FTS was introduced in 4.5.0
            for server in self.servers():
                self.rest.set_fts_index_mem_quota(server,
                                                  self.fts_index_mem_quota)

    def set_query_settings(self):
        settings = self.test_config.n1ql_settings.settings
        for _, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            for server in servers:
                self.rest.set_query_settings(server, settings)

    def set_index_settings(self):
        settings = self.test_config.secondaryindex_settings.settings
        for _, servers in self.cluster_spec.yield_servers_by_role('index'):
            for server in servers:
                if settings:
                    self.rest.set_index_settings(server, settings)
                curr_settings = self.rest.get_index_settings(server)
                curr_settings = pretty_dict(curr_settings)
                logger.info("Index settings: {}".format(curr_settings))

    def set_services(self):
        for (_, servers), initial_nodes in zip(self.clusters(),
                                               self.initial_nodes):
            master = servers[0]
            self.rest.set_services(master, self.roles[master])

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
        self.wait_until_healthy()

    def create_buckets(self):
        ram_quota = self.mem_quota / self.test_config.cluster.num_buckets

        for master in self.masters():
            for bucket_name in self.test_config.buckets:
                self.rest.create_bucket(
                    host_port=master,
                    name=bucket_name,
                    ram_quota=ram_quota,
                    password=self.test_config.bucket.password,
                    replica_number=self.test_config.bucket.replica_number,
                    replica_index=self.test_config.bucket.replica_index,
                    eviction_policy=self.test_config.bucket.eviction_policy,
                    time_synchronization=self.test_config.bucket.time_synchronization,
                )

    def configure_auto_compaction(self):
        compaction_settings = self.test_config.compaction
        for master in self.masters():
            self.rest.configure_auto_compaction(master, compaction_settings)
            settings = self.rest.get_auto_compaction_settings(master)
            logger.info('Auto-compaction settings: {}'
                        .format(pretty_dict(settings)))

    def configure_internal_settings(self):
        internal_settings = self.test_config.internal_settings
        for master in self.masters():
            for parameter, value in internal_settings.items():
                self.rest.set_internal_settings(master,
                                                {parameter: int(value)})

    def configure_xdcr_settings(self):
        xdcr_cluster_settings = self.test_config.xdcr_cluster_settings
        for master in self.masters():
            for parameter, value in xdcr_cluster_settings.items():
                self.rest.set_xdcr_cluster_settings(master,
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
        """Apply custom buckets settings (e.g., max_num_shards or max_num_auxio)
        using "/diag/eval" and restart the entire cluster."""
        cmd = 'ns_bucket:update_bucket_props("{}", ' \
              '[{{extra_config_string, "{}={}"}}]).'

        for option, value in self.test_config.bucket_extras.items():
            logger.info('Changing {} to {}'.format(option, value))
            for master in self.masters():
                for bucket in self.test_config.buckets:
                    diag_eval = cmd.format(bucket, option, value)
                    self.rest.run_diag_eval(master, diag_eval)
        if self.test_config.bucket_extras:
            self.remote.restart()
            self.wait_until_healthy()

    def tune_logging(self):
        self.remote.tune_log_rotation()
        self.remote.restart()

    def enable_auto_failover(self):
        for master in self.masters():
            self.rest.enable_auto_failover(master)

    def wait_until_warmed_up(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def wait_until_healthy(self):
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_node_health(master)

    def change_dcp_io_threads(self):
        if self.test_config.secondaryindex_settings.db == 'moi':
            cmd = 'ns_bucket:update_bucket_props("{}", ' \
                  '[{{extra_config_string, "max_num_auxio=16"}}]).'
            for master in self.masters():
                for bucket in self.test_config.buckets:
                    diag_eval = cmd.format(bucket)
                    self.rest.run_diag_eval(master, diag_eval)
            time.sleep(30)
            self.remote.restart()
            self.wait_until_healthy()

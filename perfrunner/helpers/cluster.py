from typing import List

from logger import logger

from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.misc import pretty_dict, server_group
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper


class ClusterManager:

    def __init__(self, cluster_spec, test_config, verbose=False):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)
        self.monitor = Monitor(cluster_spec, test_config, verbose)
        self.memcached = MemcachedHelper(test_config)

        self.master_node = next(self.cluster_spec.masters)

        self.initial_nodes = test_config.cluster.initial_nodes
        self.mem_quota = test_config.cluster.mem_quota
        self.index_mem_quota = test_config.cluster.index_mem_quota
        self.fts_mem_quota = test_config.cluster.fts_index_mem_quota
        self.group_number = test_config.cluster.group_number or 1
        self.roles = cluster_spec.roles

    def is_compatible(self, min_release):
        for master in self.cluster_spec.masters:
            version = self.rest.get_version(master)
            return version >= min_release

    def set_data_path(self):
        if self.cluster_spec.paths:
            data_path, index_path = self.cluster_spec.paths
            for server in self.cluster_spec.servers:
                self.rest.set_data_path(server, data_path, index_path)

    def rename(self):
        for server in self.cluster_spec.servers:
            self.rest.rename(server)

    def set_auth(self):
        for server in self.cluster_spec.servers:
            self.rest.set_auth(server)

    def set_mem_quota(self):
        for master in self.cluster_spec.masters:
            self.rest.set_mem_quota(master, self.mem_quota)
            self.rest.set_index_mem_quota(master, self.index_mem_quota)
            if self.is_compatible(min_release='4.5.0'):
                self.rest.set_fts_index_mem_quota(master, self.fts_mem_quota)

    def set_query_settings(self):
        settings = self.test_config.n1ql_settings.settings
        for servers in self.cluster_spec.servers_by_role('n1ql'):
            for server in servers:
                if settings:
                    self.rest.set_query_settings(server, settings)

    def set_index_settings(self):
        settings = self.test_config.gsi_settings.settings
        for servers in self.cluster_spec.servers_by_role('index'):
            for server in servers:
                if settings:
                    self.rest.set_index_settings(server, settings)
                curr_settings = self.rest.get_index_settings(server)
                curr_settings = pretty_dict(curr_settings)
                logger.info("Index settings: {}".format(curr_settings))

    def set_services(self):
        if not self.is_compatible(min_release='4.0.0'):
            return

        for (_, servers), initial_nodes in zip(self.cluster_spec.clusters,
                                               self.initial_nodes):
            master = servers[0]
            self.rest.set_services(master, self.roles[master])

    def create_server_groups(self):
        for master in self.cluster_spec.masters:
            for i in range(1, self.group_number):
                name = 'Group {}'.format(i + 1)
                self.rest.create_server_group(master, name=name)

    def add_nodes(self):
        for (_, servers), initial_nodes in zip(self.cluster_spec.clusters,
                                               self.initial_nodes):

            if initial_nodes < 2:  # Single-node cluster
                continue

            master = servers[0]
            if self.group_number > 1:
                groups = self.rest.get_server_groups(master)
            else:
                groups = {}
            for i, node in enumerate(servers[1:initial_nodes], start=1):
                uri = groups.get(server_group(servers[:initial_nodes],
                                              self.group_number, i))
                self.rest.add_node(master, node, self.roles[node], uri)

    def rebalance(self):
        for (_, servers), initial_nodes in zip(self.cluster_spec.clusters,
                                               self.initial_nodes):
            master = servers[0]
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)
        self.wait_until_healthy()

    def create_buckets(self):
        ram_quota = self.mem_quota // self.test_config.cluster.num_buckets

        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.buckets:
                self.rest.create_bucket(
                    host=master,
                    name=bucket_name,
                    ram_quota=ram_quota,
                    password=self.test_config.bucket.password,
                    replica_number=self.test_config.bucket.replica_number,
                    replica_index=self.test_config.bucket.replica_index,
                    eviction_policy=self.test_config.bucket.eviction_policy,
                    bucket_type=self.test_config.bucket.bucket_type,
                    conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                )

    def configure_auto_compaction(self):
        compaction_settings = self.test_config.compaction
        for master in self.cluster_spec.masters:
            self.rest.configure_auto_compaction(master, compaction_settings)
            settings = self.rest.get_auto_compaction_settings(master)
            logger.info('Auto-compaction settings: {}'
                        .format(pretty_dict(settings)))

    def configure_internal_settings(self):
        internal_settings = self.test_config.internal_settings
        for master in self.cluster_spec.masters:
            for parameter, value in internal_settings.items():
                self.rest.set_internal_settings(master,
                                                {parameter: int(value)})

    def configure_xdcr_settings(self):
        xdcr_cluster_settings = self.test_config.xdcr_cluster_settings
        for master in self.cluster_spec.masters:
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
            for master in self.cluster_spec.masters:
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
        for master in self.cluster_spec.masters:
            self.rest.enable_auto_failover(master)

    def wait_until_warmed_up(self):
        if self.test_config.bucket.bucket_type == 'ephemeral':
            return

        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def wait_until_healthy(self):
        for master in self.cluster_spec.masters:
            self.monitor.monitor_node_health(master)

    def enable_secrets(self):
        if not self.is_compatible(min_release='4.6.0') or \
                self.remote.os == 'Cygwin' or \
                self.rest.is_community(self.master_node):
            return

        for server in self.cluster_spec.servers:
            self.rest.set_master_password(server)
        self.remote.set_master_password()

    def enable_audit(self):
        if not self.is_compatible(min_release='4.0.0') or \
                self.rest.is_community(self.master_node):
            return

        for master in self.cluster_spec.masters:
            self.rest.enable_audit(master)

    def generate_ce_roles(self) -> List[str]:
        return ['admin']

    def generate_ee_roles(self) -> List[str]:
        existing_roles = {r['role']
                          for r in self.rest.get_rbac_roles(self.master_node)}

        roles = []
        for role in (
                'bucket_admin',
                'data_dcp_reader',
                'data_monitoring',
                'data_reader_writer',
                'data_reader',
                'data_writer',
                'fts_admin',
                'fts_searcher',
                'query_delete',
                'query_insert',
                'query_select',
                'query_update',
        ):
            if role in existing_roles:
                roles.append(role + '[{bucket}]')

        return roles

    def add_rbac_users(self):
        if not self.is_compatible(min_release='5.0'):
            return

        if self.rest.is_community(self.master_node):
            roles = self.generate_ce_roles()
        else:
            roles = self.generate_ee_roles()

        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                bucket_roles = [role.format(bucket=bucket) for role in roles]
                self.rest.add_rbac_user(
                    host=master,
                    bucket=bucket,
                    password=self.test_config.bucket.password,
                    roles=bucket_roles,
                )

    def throttle_cpu(self):
        if self.remote.os == 'Cygwin':
            return

        self.remote.enable_cpu()

        if self.test_config.cluster.throttle_cpu:
            self.remote.disable_cpu()

    def indexer_restrict_memory(self):
        if self.test_config.gsi_settings.restrict_kernel_memory:
            self.remote.restrict_memory_kernel_parameter(
                self.test_config.gsi_settings.restrict_kernel_memory)
            self.remote.reboot()
            self.monitor.wait_for_indexer()

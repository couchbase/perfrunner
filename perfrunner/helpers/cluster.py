import os
import random
import re
import time
from typing import Iterable, List, Optional
from uuid import uuid4

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.config_files import (
    CAOCouchbaseBucketFile,
    CAOCouchbaseClusterFile,
    CAOSyncgatewayDeploymentFile,
)
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.misc import (
    SGPortRange,
    create_build_tuple,
    maybe_atoi,
    pretty_dict,
    run_aws_cli_command,
    run_local_shell_command,
    set_azure_capella_subscription,
    set_azure_perf_subscription,
)
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig


class ClusterManager:
    def __new__(cls, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool = False):
        if cluster_spec.dynamic_infrastructure:
            return KubernetesClusterManager(cluster_spec, test_config)
        elif cluster_spec.capella_infrastructure:
            return CapellaClusterManager(cluster_spec, test_config, verbose)
        else:
            return DefaultClusterManager(cluster_spec, test_config, verbose)


class ClusterManagerBase:
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool = False):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.capella_infra = self.cluster_spec.capella_infrastructure
        self.rest = RestHelper(cluster_spec, test_config)
        self.remote = RemoteHelper(cluster_spec, verbose)
        self.memcached = MemcachedHelper(cluster_spec, test_config)
        self.master_node = next(self.cluster_spec.masters)
        self.initial_nodes = [
            num_nodes
            for i, num_nodes in enumerate(test_config.cluster.initial_nodes)
            if i not in self.cluster_spec.inactive_cluster_idxs
        ]
        self.build = self.rest.get_version(self.master_node)
        self.build_tuple = create_build_tuple(self.build)
        self.is_columnar = self.rest.is_columnar(self.master_node)
        self.monitor = Monitor(cluster_spec, test_config, self.rest, self.remote, self.build)

    def is_compatible(self, min_release: str) -> bool:
        for master in self.cluster_spec.masters:
            return (self.rest.get_version(master) >= min_release) or self.rest.is_columnar(master)

    def set_analytics_settings(self):
        replica_analytics = self.test_config.analytics_settings.replica_analytics
        if replica_analytics:
            self.rest.set_analytics_replica(self.master_node, replica_analytics)
            self.rebalance()
            check_replica = self.rest.get_analytics_settings(self.master_node)
            logger.info("Analytics replica setting: {}".format(check_replica))

        if not (analytics_nodes := self.cluster_spec.servers_by_role("cbas")):
            return

        analytics_node = analytics_nodes[0]
        if not (config_settings := self.test_config.analytics_settings.config_settings):
            return

        service_settings = self.rest.get_analytics_config(analytics_node, "service")
        node_settings = self.rest.get_analytics_config(analytics_node, "node")

        new_service_settings, new_node_settings = {}, {}
        for setting, value in config_settings.items():
            if setting in node_settings:
                new_node_settings[setting] = value
            elif setting in service_settings:
                new_service_settings[setting] = value
            else:
                logger.warn(f"Unrecognised analytics config setting: {setting}")

        if not (new_service_settings or new_node_settings):
            return

        if new_service_settings:
            self.rest.set_analytics_config_settings(analytics_node, "service", new_service_settings)

        if new_node_settings:
            for node in analytics_nodes:
                self.rest.set_analytics_config_settings(node, "node", new_node_settings)

        self.rest.restart_analytics_cluster(analytics_node)

        if new_service_settings:
            self.rest.validate_analytics_settings(analytics_node, "service", new_service_settings)

        if new_node_settings:
            for node in analytics_nodes:
                self.rest.validate_analytics_settings(node, "node", new_node_settings)

    def flush_buckets(self):
        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.buckets:
                self.rest.flush_bucket(host=master, bucket=bucket_name)

    def delete_buckets(self):
        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.buckets:
                self.rest.delete_bucket(host=master, name=bucket_name)

    def create_collections(self):
        collection_map = self.test_config.collection.collection_map
        for master in self.cluster_spec.masters:
            if collection_map is not None:
                if self.test_config.collection.use_bulk_api:
                    for bucket, scopes in collection_map.items():
                        create_scopes = []
                        for scope, collections in scopes.items():
                            create_collections = []
                            for collection, options in collections.items():
                                create_collection = {"name": collection}
                                if "history" in options:
                                    create_collection["history"] = bool(options["history"])
                                create_collections.append(create_collection)
                            create_scope = {"name": scope, "collections": create_collections}
                            create_scopes.append(create_scope)

                        self.rest.set_collection_map(master, bucket, {"scopes": create_scopes})
                else:
                    for bucket, scopes in collection_map.items():
                        # If transactions are enabled, we need to keep the default collection
                        # Otherwise, we can delete it if it is not specified in the collection map
                        delete_default = (
                            not self.test_config.access_settings.transactionsenabled
                        ) and "_default" not in scopes.get("_default", {})

                        if delete_default:
                            self.rest.delete_collection(master, bucket, "_default", "_default")

                        for scope, collections in scopes.items():
                            if scope != "_default":
                                self.rest.create_scope(master, bucket, scope)
                            for collection, options in collections.items():
                                if collection != "_default":
                                    history = (
                                        bool(options["history"]) if "history" in options else None
                                    )
                                    self.rest.create_collection(
                                        master, bucket, scope, collection, history
                                    )

    def serverless_throttle(self):
        if not all(value == 0 for value in self.test_config.cluster.serverless_throttle.values()):
            self.rest.set_serverless_throttle(
                self.master_node, self.test_config.cluster.serverless_throttle
            )

    def wait_until_warmed_up(self):
        if self.test_config.bucket.bucket_type in ("ephemeral", "memcached"):
            return

        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def wait_until_healthy(self, polling_interval_secs: int = 0, max_retries: int = 0):
        """Wait for all nodes to be healthy and for the analytics service to be active."""
        for master in self.cluster_spec.masters:
            self.monitor.monitor_node_health(master, polling_interval_secs, max_retries)

            for analytics_node in self.rest.get_active_nodes_by_role(master, "cbas"):
                self.monitor.monitor_analytics_node_active(
                    analytics_node, polling_interval_secs, max_retries
                )

    def _gen_disabled_audit_events(self, master: str) -> List[str]:
        curr_settings = self.rest.get_audit_settings(master)
        curr_disabled = {str(event) for event in curr_settings["disabled"]}
        disabled = curr_disabled - self.test_config.audit_settings.extra_events
        return list(disabled)

    def enable_audit(self):
        if not self.is_compatible(min_release="4.0.0") or self.rest.is_community(self.master_node):
            return

        if not self.test_config.audit_settings.enabled:
            return

        for master in self.cluster_spec.masters:
            disabled = []
            if self.test_config.audit_settings.extra_events:
                disabled = self._gen_disabled_audit_events(master)
            self.rest.enable_audit(master, disabled)

    def enable_developer_preview(self):
        if (
            self.build_tuple > (7, 0, 0, 4698)
            or self.build_tuple < (1, 0, 0, 0)
            or self.is_columnar
        ):
            self.remote.enable_developer_preview()

    def set_magma_min_quota(self):
        if self.test_config.magma_settings.magma_min_memory_quota:
            magma_min_quota = self.test_config.magma_settings.magma_min_memory_quota
            self.remote.set_magma_min_memory_quota(magma_min_quota)


class DefaultClusterManager(ClusterManagerBase):
    """Cluster Manager for all non-Kubernetes self-managed clusters."""

    def set_data_path(self):
        for server in self.cluster_spec.servers:
            self.remote.change_owner(server, self.cluster_spec.data_path)
            self.rest.set_data_path(server, self.cluster_spec.data_path)

    def set_index_path(self):
        for server in self.cluster_spec.servers:
            self.remote.change_owner(server, self.cluster_spec.index_path)
            self.rest.set_index_path(server, self.cluster_spec.index_path)

    def set_analytics_path(self):
        paths = []
        for path in self.cluster_spec.analytics_paths:
            for i in range(self.test_config.analytics_settings.num_io_devices):
                io_device = '{}/dev{}'.format(path, i)
                paths.append(io_device)
        for server in self.cluster_spec.servers_by_role('cbas'):
            for path in self.cluster_spec.analytics_paths:
                self.remote.change_owner(server, path)
            self.rest.set_analytics_paths(server, paths)

    def rename(self):
        if self.cluster_spec.using_private_cluster_ips:
            for public_ip, private_ip in self.cluster_spec.servers_public_to_private_ip.items():
                if private_ip:
                    self.rest.rename(public_ip, private_ip)
        else:
            for server in self.cluster_spec.servers:
                self.rest.rename(server)

    def set_auth(self):
        for server in self.cluster_spec.servers:
            self.rest.set_auth(server)

    def set_mem_quotas(self):
        for master in self.cluster_spec.masters:
            self.rest.set_kv_mem_quota(master, self.test_config.cluster.mem_quota)
            self.rest.set_index_mem_quota(master, self.test_config.cluster.index_mem_quota)
            if self.test_config.cluster.fts_index_mem_quota:
                self.rest.set_fts_index_mem_quota(
                    master, self.test_config.cluster.fts_index_mem_quota
                )
            if self.test_config.cluster.analytics_mem_quota:
                self.rest.set_analytics_mem_quota(
                    master, self.test_config.cluster.analytics_mem_quota
                )
            if self.test_config.cluster.eventing_mem_quota:
                self.rest.set_eventing_mem_quota(
                    master, self.test_config.cluster.eventing_mem_quota
                )

    def set_query_settings(self):
        logger.info("Setting query settings")
        query_nodes = self.cluster_spec.servers_by_role('n1ql')
        if query_nodes:
            settings = self.test_config.n1ql_settings.cbq_settings
            if settings:
                self.rest.set_query_settings(query_nodes[0], settings)
            settings = self.rest.get_query_settings(query_nodes[0])
            settings = pretty_dict(settings)
            logger.info('Query settings: {}'.format(settings))

    def set_index_settings(self):
        logger.info('Setting index settings')
        index_nodes = self.cluster_spec.servers_by_role('index')
        if index_nodes:
            settings = self.test_config.gsi_settings.settings
            if settings:
                for cluster_index_servers in self.cluster_spec.servers_by_cluster_and_role("index"):
                    index_node = cluster_index_servers[0]
                    self.rest.set_index_settings(index_node,
                                                    self.test_config.gsi_settings.settings)
                    cluster_settings = self.rest.get_index_settings(index_node)
                    cluster_settings = pretty_dict(self.rest.get_index_settings(index_node))
                    logger.info('Index settings: {}'.format(cluster_settings))

    def set_services(self):
        if not self.is_compatible(min_release="4.0.0"):
            return

        for master in self.cluster_spec.masters:
            roles = self.cluster_spec.roles[master]
            self.rest.set_services(master, roles)

    def add_nodes(self):
        for (cluster, servers), initial_nodes in zip(
            self.cluster_spec.clusters, self.initial_nodes
        ):
            if initial_nodes < 2:  # Single-node cluster
                continue
            if using_private_ips := self.cluster_spec.using_private_cluster_ips:
                private_ips = dict(self.cluster_spec.clusters_private)[cluster]
            master = servers[0]
            for i, node in enumerate(servers[1:initial_nodes]):
                roles = self.cluster_spec.roles[node]
                new_host = private_ips[1:initial_nodes][i] if using_private_ips else node
                self.rest.add_node(master, new_host, roles)

    def rebalance(self):
        for (_, servers), initial_nodes in zip(self.cluster_spec.clusters, self.initial_nodes):
            master = servers[0]
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)
        self.wait_until_healthy()

    def increase_bucket_limit(self, num_buckets: int):
        for master in self.cluster_spec.masters:
            self.rest.increase_bucket_limit(master, num_buckets)

    def create_buckets(self):
        mem_quota = self.test_config.cluster.mem_quota

        if self.test_config.cluster.num_buckets > 7:
            self.increase_bucket_limit(self.test_config.cluster.num_buckets + 3)

        if self.test_config.cluster.eventing_metadata_bucket_mem_quota:
            mem_quota -= (self.test_config.cluster.eventing_metadata_bucket_mem_quota +
                          self.test_config.cluster.eventing_bucket_mem_quota)

        per_bucket_quota = mem_quota // self.test_config.cluster.num_buckets

        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.buckets:
                self.rest.create_bucket(
                    host=master,
                    name=bucket_name,
                    ram_quota=per_bucket_quota,
                    replica_number=self.test_config.bucket.replica_number,
                    replica_index=self.test_config.bucket.replica_index,
                    eviction_policy=self.test_config.bucket.eviction_policy,
                    bucket_type=self.test_config.bucket.bucket_type,
                    magma_seq_tree_data_block_size=self.test_config.bucket.magma_seq_tree_data_block_size,
                    backend_storage=self.test_config.bucket.backend_storage,
                    conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                    compression_mode=self.test_config.bucket.compression_mode,
                    history_seconds=self.test_config.bucket.history_seconds,
                    history_bytes=self.test_config.bucket.history_bytes,
                    max_ttl=self.test_config.bucket.max_ttl,
                )

    def create_eventing_buckets(self):
        if not self.test_config.cluster.eventing_bucket_mem_quota:
            return

        per_bucket_quota = \
            self.test_config.cluster.eventing_bucket_mem_quota \
            // self.test_config.cluster.eventing_buckets

        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.eventing_buckets:
                self.rest.create_bucket(
                    host=master,
                    name=bucket_name,
                    ram_quota=per_bucket_quota,
                    replica_number=self.test_config.bucket.replica_number,
                    replica_index=self.test_config.bucket.replica_index,
                    eviction_policy=self.test_config.bucket.eviction_policy,
                    bucket_type=self.test_config.bucket.bucket_type,
                    backend_storage=self.test_config.bucket.backend_storage,
                    conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                    compression_mode=self.test_config.bucket.compression_mode,
                )

    def create_eventing_metadata_bucket(self):
        if not self.test_config.cluster.eventing_metadata_bucket_mem_quota:
            return

        for master in self.cluster_spec.masters:
            self.rest.create_bucket(
                host=master,
                name=self.test_config.cluster.EVENTING_METADATA_BUCKET_NAME,
                ram_quota=self.test_config.cluster.eventing_metadata_bucket_mem_quota,
                replica_number=self.test_config.bucket.replica_number,
                replica_index=self.test_config.bucket.replica_index,
                eviction_policy=self.test_config.bucket.eviction_policy,
                bucket_type=self.test_config.bucket.bucket_type,
                backend_storage=self.test_config.bucket.backend_storage,
                conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                compression_mode=self.test_config.bucket.compression_mode,
            )

    def configure_auto_compaction(self):
        compaction_settings = self.test_config.compaction

        for master in self.cluster_spec.masters:
            self.rest.configure_auto_compaction(master, compaction_settings)
            settings = self.rest.get_auto_compaction_settings(master)
            logger.info('Auto-compaction settings: {}'.format(pretty_dict(settings)))

    def configure_internal_settings(self):
        internal_settings = self.test_config.internal_settings
        for master in self.cluster_spec.masters:
            for parameter, value in internal_settings.items():
                self.rest.set_internal_settings(master, {parameter: maybe_atoi(value)})

    def configure_xdcr_settings(self):
        xdcr_cluster_settings = self.test_config.xdcr_cluster_settings
        for master in self.cluster_spec.masters:
            for parameter, value in xdcr_cluster_settings.items():
                self.rest.set_xdcr_cluster_settings(
                    master,
                    {parameter: maybe_atoi(value)})

    def tweak_memory(self):
        self.remote.reset_swap()
        self.remote.drop_caches()
        self.remote.set_swappiness()
        self.remote.disable_thp()

    def enable_n2n_encryption(self):
        if self.test_config.cluster.enable_n2n_encryption:
            for master in self.cluster_spec.masters:
                self.remote.enable_n2n_encryption(
                    master,
                    self.test_config.cluster.enable_n2n_encryption)

    def disable_ui_http(self):
        if self.test_config.cluster.ui_http == 'disabled':
            self.remote.ui_http_off(self.cluster_spec.servers[0])

    def restart_with_alternative_num_vbuckets(self):
        num_vbuckets = self.test_config.cluster.num_vbuckets
        if num_vbuckets is not None:
            self.remote.restart_with_alternative_num_vbuckets(num_vbuckets)

    def restart_with_alternative_bucket_options(self):
        """Apply custom buckets settings.

        Tune bucket settings (e.g., max_num_shards or max_num_auxio) using
        "/diag/eval" and restart the entire cluster.
        """
        if self.test_config.bucket_extras:
            self.remote.enable_nonlocal_diag_eval()

        cmd = 'ns_bucket:update_bucket_props("{}", ' \
              '[{{extra_config_string, "{}"}}]).'

        params = ''
        for option, value in self.test_config.bucket_extras.items():
            if re.search("^num_.*_threads$", option):
                self.rest.set_num_threads(self.master_node, option, value)
            else:
                params = params + option+"="+value+";"

        if params:
            for master in self.cluster_spec.masters:
                for bucket in (self.test_config.buckets + self.test_config.eventing_buckets +
                               self.test_config.eventing_metadata_bucket):
                    logger.info('Changing {} to {}'.format(bucket, params))
                    diag_eval = cmd.format(bucket, params[:len(params) - 1])
                    self.rest.run_diag_eval(master, diag_eval)

        if self.test_config.bucket_extras:
            self._restart_clusters()

        if self.test_config.magma_settings.storage_quota_percentage:
            magma_quota = self.test_config.magma_settings.storage_quota_percentage
            for bucket in self.test_config.buckets:
                self.remote.set_magma_quota(bucket, magma_quota)

    def _restart_clusters(self):
        self.disable_auto_failover()
        self.remote.restart()
        self.wait_until_healthy()
        self.enable_auto_failover()

    def configure_ns_server(self):
        """Configure ns_server using diag/eval.

        Tune ns_server settings with specified Erlang code using `diag/eval`
        and restart the cluster after a specified delay. If no `diag/eval` to do, just
        run `enable_auto_failover()`.
        """
        diag_eval_settings = self.test_config.diag_eval

        if not diag_eval_settings.payloads:
            # Nothing to configure, just enable autofailover and return
            self.enable_auto_failover()
            return

        self.remote.enable_nonlocal_diag_eval()

        for master in self.cluster_spec.masters:
            for payload in diag_eval_settings.payloads:
                payload = payload.strip('\'')
                logger.info("Running diag/eval: '{}' on {}".format(payload, master))
                self.rest.run_diag_eval(master, '{}'.format(payload))

        # Some config may be replicated to other nodes asynchronously.
        # Allow configurable delay before restart
        time.sleep(diag_eval_settings.restart_delay)
        self._restart_clusters()  # Restart and enable auto-failover after

    def tune_logging(self):
        self.remote.tune_log_rotation()
        self.remote.restart()

    def enable_auto_failover(self):
        enabled = self.test_config.bucket.autofailover_enabled
        failover_timeouts = self.test_config.bucket.failover_timeouts
        disk_failover_timeout = self.test_config.bucket.disk_failover_timeout

        for master in self.cluster_spec.masters:
            self.rest.set_auto_failover(master, enabled, failover_timeouts,
                                        disk_failover_timeout)

    def disable_auto_failover(self):
        enabled = 'false'
        failover_timeouts = self.test_config.bucket.failover_timeouts
        disk_failover_timeout = self.test_config.bucket.disk_failover_timeout

        for master in self.cluster_spec.masters:
            self.rest.set_auto_failover(master, enabled, failover_timeouts,
                                        disk_failover_timeout)

    def add_server_groups(self):
        logger.info("Server group map: {}".format(self.cluster_spec.server_group_map))
        if self.cluster_spec.server_group_map:
            server_group_info = self.rest.get_server_group_info(self.master_node)["groups"]
            existing_server_groups = [group_info["name"] for group_info in server_group_info]
            groups = set(self.cluster_spec.server_group_map.values())
            for group in groups:
                if group not in existing_server_groups:
                    self.rest.create_server_group(self.master_node, group)

    def change_group_membership(self):
        if self.cluster_spec.server_group_map:
            server_group_info = self.rest.get_server_group_info(self.master_node)
            server_groups = set(self.cluster_spec.server_group_map.values())

            node_group_json = {
                "groups": []
            }

            for i, group_info in enumerate(server_group_info["groups"]):
                node_group_json["groups"].append(dict((k, group_info[k])
                                                      for k in ["name", "uri"]))
                node_group_json["groups"][i]["nodes"] = []
            nodes_initialised = 1
            for server, group in self.cluster_spec.server_group_map.items():
                for server_info in node_group_json["groups"]:
                    if server_info["name"] == group and nodes_initialised <= self.initial_nodes[0]:
                        server_info["nodes"].append({"otpNode": "ns_1@{}".format(server)})
                        nodes_initialised += 1
                        break

            logger.info("node json {}".format(node_group_json))
            self.rest.change_group_membership(self.master_node,
                                              server_group_info["uri"],
                                              node_group_json)
            logger.info("group membership updated")
            for server_grp in server_group_info["groups"]:
                if server_grp["name"] not in server_groups:
                    self.delete_server_group(server_grp["name"])

    def delete_server_group(self, server_group):
        logger.info("Deleting Server Group {}".format(server_group))
        server_group_info = self.rest.get_server_group_info(self.master_node)["groups"]
        for server_grp in server_group_info:
            if server_grp["name"] == server_group:
                uri = server_grp["uri"]
                break
        self.rest.delete_server_group(self.master_node, uri)

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
                'views_admin',
        ):
            if role in existing_roles:
                roles.append(role + '[{bucket}]')

        return roles

    def delete_rbac_users(self):
        if not self.is_compatible(min_release='5.0'):
            return

        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.rest.delete_rbac_user(
                    host=master,
                    bucket=bucket
                )

    def add_rbac_users(self):
        if not self.rest.supports_rbac(self.master_node):
            logger.info('RBAC not supported - skipping adding RBAC users')
            return

        if self.rest.is_community(self.master_node):
            roles = self.generate_ce_roles()
        else:
            roles = self.generate_ee_roles()

        for master in self.cluster_spec.masters:
            admin_user, admin_password = self.cluster_spec.rest_credentials
            self.rest.add_rbac_user(
                host=master,
                user=admin_user,
                password=admin_password,
                roles=['admin'],
            )

            buckets = self.test_config.buckets + self.test_config.eventing_buckets

            for bucket in buckets:
                bucket_roles = [role.format(bucket=bucket) for role in roles]
                bucket_roles.append("admin")
                self.rest.add_rbac_user(
                    host=master,
                    user=bucket,  # Backward compatibility
                    password=self.test_config.bucket.password,
                    roles=bucket_roles,
                )

    def add_extra_rbac_users(self, num_users):
        if not self.rest.supports_rbac(self.master_node):
            logger.info('RBAC not supported - skipping adding RBAC users')
            return

        if self.rest.is_community(self.master_node):
            roles = self.generate_ce_roles()
        else:
            roles = self.generate_ee_roles()

        for master in self.cluster_spec.masters:
            admin_user, admin_password = self.cluster_spec.rest_credentials
            self.rest.add_rbac_user(
                host=master,
                user=admin_user,
                password=admin_password,
                roles=['admin'],
            )

            for bucket in self.test_config.buckets:
                bucket_roles = [role.format(bucket=bucket) for role in roles]
                bucket_roles.append("admin")
                for i in range(1, num_users+1):
                    user = 'user{user_number}'.format(user_number=str(i))
                    self.rest.add_rbac_user(
                        host=master,
                        user=user,
                        password=self.test_config.bucket.password,
                        roles=bucket_roles,
                    )

    def throttle_cpu(self):
        if self.remote.PLATFORM == 'cygwin':
            return

        if self.test_config.cluster.enable_cpu_cores:
            self.remote.enable_cpu()

        if self.test_config.cluster.online_cores:
            self.remote.disable_cpu(self.test_config.cluster.online_cores)

        if self.test_config.cluster.sgw_online_cores:
            self.remote.disable_cpu_sgw(self.test_config.cluster.sgw_online_cores)

    def tune_memory_settings(self):
        kernel_memory = self.test_config.cluster.kernel_mem_limit
        kv_kernel_memory = self.test_config.cluster.kv_kernel_mem_limit
        if kernel_memory or kv_kernel_memory:
            for service in self.test_config.cluster.kernel_mem_limit_services:
                for server in self.cluster_spec.servers_by_role(service):
                    if service == 'kv' and kv_kernel_memory:
                        self.remote.tune_memory_settings(host_string=server,
                                                            size=kv_kernel_memory)
                    else:
                        self.remote.tune_memory_settings(host_string=server,
                                                            size=kernel_memory)
            self.monitor.wait_for_servers()

    def reset_memory_settings(self):
        for service in self.test_config.cluster.kernel_mem_limit_services:
            for server in self.cluster_spec.servers_by_role(service):
                self.remote.reset_memory_settings(host_string=server)
        self.monitor.wait_for_servers()

    def flush_iptables(self):
        self.remote.flush_iptables()

    def clear_login_history(self):
        self.remote.clear_wtmp()

    def disable_wan(self):
        self.remote.disable_wan()

    def enable_ipv6(self):
        if self.test_config.cluster.ipv6:
            if self.build_tuple < (6, 5, 0, 0) and not self.is_columnar:
                self.remote.update_ip_family_rest()
            else:
                self.remote.update_ip_family_cli()
            self.remote.enable_ipv6()

    def set_x509_certificates(self):
        if self.test_config.access_settings.ssl_mode == "auth":
            local.create_x509_certificates(self.cluster_spec.servers)
            self.remote.allow_non_local_ca_upload()
            self.remote.setup_x509()
            self.rest.upload_cluster_certificate(self.cluster_spec.servers[0])
            for i in range(self.initial_nodes[0]):
                self.rest.reload_cluster_certificate(self.cluster_spec.servers[i])
                self.rest.enable_certificate_auth(self.cluster_spec.servers[i])

    def set_cipher_suite(self):
        if self.test_config.access_settings.cipher_list:
            check_cipher_suit = self.rest.get_cipher_suite(self.master_node)
            logger.info('current cipher suit: {}'.format(check_cipher_suit))
            self.rest.set_cipher_suite(
                self.master_node, self.test_config.access_settings.cipher_list)
            check_cipher_suit = self.rest.get_cipher_suite(self.master_node)
            logger.info('new cipher suit: {}'.format(check_cipher_suit))

    def set_min_tls_version(self):
        if self.test_config.access_settings.min_tls_version or \
           self.test_config.backup_settings.min_tls_version or \
           self.test_config.restore_settings.min_tls_version:
            check_tls_version = self.rest.get_minimum_tls_version(self.master_node)
            logger.info('current tls version: {}'.format(check_tls_version))
            self.rest.set_minimum_tls_version(
                self.master_node,
                self.test_config.access_settings.min_tls_version
            )
            check_tls_version = self.rest.get_minimum_tls_version(self.master_node)
            logger.info('new tls version: {}'.format(check_tls_version))

    def deploy_couchbase_with_cgroups_for_index_nodes(self):
        if self.test_config.cluster.enable_cgroups:
            logger.info("starting server in cgroup")
            self.remote.add_system_limit_config()
            self.remote.restart()
            time.sleep(200)

    def clear_system_limit_config(self):
        self.remote.clear_system_limit_config()
        self.remote.restart()

    def set_goldfish_s3_bucket(self):
        region = os.environ.get("AWS_REGION", "us-east-1")
        s3_bucket_name = self.cluster_spec.backup.split("://")[1]
        self.remote.configure_analytics_s3_bucket(region=region, s3_bucket=s3_bucket_name)

    def add_aws_credential(self):
        access_key_id, secret_access_key = local.get_aws_credential(
            self.test_config.backup_settings.aws_credential_path, False
        )
        self.remote.add_aws_credential(access_key_id, secret_access_key)

    def set_goldfish_storage_partitions(self):
        storage_partitions = self.test_config.analytics_settings.goldfish_storage_partitions

        if ((8, 0, 0, 0) < self.build_tuple < (8, 0, 0, 1547)) and storage_partitions:
            logger.warning(
                "Cannot set storage partitions for Goldfish. "
                "Couchbase Server 8.0.0-1547 or later required."
            )
            return

        if storage_partitions := self.test_config.analytics_settings.goldfish_storage_partitions:
            self.remote.set_goldfish_storage_partitions(storage_partitions)
        else:
            logger.info("Keeping default number of storage partitions for Goldfish.")

    def configure_kafka(self):
        self.remote.configure_kafka_brokers(
            self.test_config.columnar_kafka_links_settings.partitions_per_topic
        )

    def start_kafka(self):
        self.remote.start_kafka_zookeepers()
        self.remote.start_kafka_brokers()

    def get_msk_connect_custom_plugin_arn(self, name: str) -> Optional[str]:
        command_template = (
            "kafkaconnect list-custom-plugins --region $AWS_REGION "
            "--query 'customPlugins[?name==`{}`].customPluginArn' "
            "--output text"
        )

        if arn := run_aws_cli_command(command_template, name):
            logger.info('Found ARN for MSK Connect custom plugin "{}": {}'.format(name, arn))

        return arn

    def get_msk_connect_worker_configuration_arn(self, name: str) -> Optional[str]:
        command_template = (
            "kafkaconnect list-worker-configurations --region $AWS_REGION "
            "--query 'workerConfigurations[?name==`{}`].workerConfigurationArn' "
            "--output text"
        )

        if arn := run_aws_cli_command(command_template, name):
            logger.info('Found ARN for MSK Connect worker configuration "{}": {}'.format(name, arn))

        return arn

    def get_msk_connect_service_execution_role_arn(self, name: str) -> Optional[str]:
        command_template = (
            "iam list-roles --region $AWS_REGION "
            "--query 'Roles[?RoleName==`{}`].Arn' "
            "--output text"
        )

        if arn := run_aws_cli_command(command_template, name):
            logger.info(
                'Found ARN for MSK Connect service execution role "{}": {}'.format(name, arn)
            )

        return arn

    def get_msk_connect_connector_arns(self) -> List[str]:
        command_template = (
            "kafkaconnect list-connectors --region $AWS_REGION "
            "--query "
            "'connectors[?kafkaCluster.apacheKafkaCluster.bootstrapServers==`{}`].connectorArn' "
            "--output text"
        )
        bootstrap_servers = ",".join(["{}:9092".format(k) for k in self.cluster_spec.kafka_brokers])

        if arns := run_aws_cli_command(command_template, bootstrap_servers):
            logger.info("Found ARNs for MSK Connect connectors: {}".format(arns))

        return arns

    def set_kafka_links_settings(self):
        kafka_links_settings = self.test_config.columnar_kafka_links_settings
        settings = {
            # Generic settings
            "kafkaConnectVersion": "2.7.1",
            "vendor": "AWS_KAFKA",
            # Settings that MSK Connect needs for creating connectors
            "brokersUrl": ",".join([f"{k}:9092" for k in self.cluster_spec.kafka_brokers]),
            "subnets": ",".join(self.cluster_spec.kafka_broker_subnet_ids),
            "region": os.environ.get("AWS_REGION", "us-east-1"),
            # Settings for CBAS sink connector
            "couchbaseSeedNodes": ",".join(self.cluster_spec.servers_by_role("cbas")),
            "couchbaseUsername": self.cluster_spec.rest_credentials[0],
            "couchbasePassword": self.cluster_spec.rest_credentials[1],
            "batchSize": kafka_links_settings.batch_size_bytes,
            "maxTasksPerWorker": kafka_links_settings.max_tasks_per_worker,
            "logBucket": kafka_links_settings.connector_log_bucket,
            # General connector settings
            "minimumWorkerCount": kafka_links_settings.min_worker_count,
            "maximumWorkerCount": kafka_links_settings.max_worker_count,
            "minCpuPerWorker": kafka_links_settings.min_cpu_per_worker,
        }

        worker_config_arn = self.get_msk_connect_worker_configuration_arn(
            self.test_config.columnar_kafka_links_settings.msk_connect_worker_config
        )
        if not worker_config_arn:
            raise Exception("Could not find specified MSK Connect worker configuration")

        service_exec_role_arn = self.get_msk_connect_service_execution_role_arn(
            self.test_config.columnar_kafka_links_settings.msk_connect_service_exec_role
        )
        if not service_exec_role_arn:
            raise Exception("Could not find specified MSK Connect service execution role")

        mongodb_plugin_arn = self.get_msk_connect_custom_plugin_arn(
            self.test_config.columnar_kafka_links_settings.mongodb_connector_custom_plugin
        )
        if not mongodb_plugin_arn:
            raise Exception("Could not find specified MongoDB connector custom plugin")

        cbas_plugin_arn = self.get_msk_connect_custom_plugin_arn(
            self.test_config.columnar_kafka_links_settings.cbas_connector_custom_plugin
        )
        if not cbas_plugin_arn:
            raise Exception("Could not find specified CBAS connector custom plugin")

        settings.update(
            {
                "workerConfigurationArn": worker_config_arn,
                "serviceExecutionRoleArn": service_exec_role_arn,
                "MONGODB": mongodb_plugin_arn,
                "COUCHBASE_ANALYTICS": cbas_plugin_arn,
            }
        )

        logger.info(f"Setting Kafka Links settings: {pretty_dict(settings)}")
        if (8, 0, 0, 0) < self.build_tuple < (8, 0, 0, 1428):
            # Set settings using environment variables
            self.remote.set_kafka_links_env_vars(settings)
            self.remote.restart()
        else:
            # Set settings within metakv
            self.remote.set_kafka_links_metakv_settings(settings)


class CapellaClusterManager(ClusterManagerBase):
    def create_buckets(self):
        mem_quota = self.test_config.cluster.mem_quota

        logger.info("Getting free memory available for buckets on Capella cluster.")
        mem_info = self.rest.get_bucket_mem_available(next(self.cluster_spec.masters))
        mem_quota = mem_info["free"]
        logger.info("Free memory for buckets (per node): {}MB".format(mem_quota))

        if self.test_config.cluster.eventing_metadata_bucket_mem_quota:
            mem_quota -= (
                self.test_config.cluster.eventing_metadata_bucket_mem_quota
                + self.test_config.cluster.eventing_bucket_mem_quota
            )

        per_bucket_quota = mem_quota // self.test_config.cluster.num_buckets

        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.buckets:
                self.rest.create_bucket(
                    host=master,
                    name=bucket_name,
                    ram_quota=per_bucket_quota,
                    replica_number=self.test_config.bucket.replica_number,
                    eviction_policy=self.test_config.bucket.eviction_policy,
                    backend_storage=self.test_config.bucket.backend_storage,
                    conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                    flush=self.test_config.bucket.flush,
                    durability=self.test_config.bucket.min_durability,
                    ttl_value=self.test_config.bucket.doc_ttl_value,
                    ttl_unit=self.test_config.bucket.doc_ttl_unit,
                )

    def create_eventing_buckets(self):
        if not self.test_config.cluster.eventing_bucket_mem_quota:
            return

        per_bucket_quota = (
            self.test_config.cluster.eventing_bucket_mem_quota
            // self.test_config.cluster.eventing_buckets
        )

        for master in self.cluster_spec.masters:
            for bucket_name in self.test_config.eventing_buckets:
                self.rest.create_bucket(
                    host=master,
                    name=bucket_name,
                    ram_quota=per_bucket_quota,
                    replica_number=self.test_config.bucket.replica_number,
                    eviction_policy=self.test_config.bucket.eviction_policy,
                    backend_storage=self.test_config.bucket.backend_storage,
                    conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                    flush=self.test_config.bucket.flush,
                    durability=self.test_config.bucket.min_durability,
                )

    def create_eventing_metadata_bucket(self):
        if not self.test_config.cluster.eventing_metadata_bucket_mem_quota:
            return

        for master in self.cluster_spec.masters:
            self.rest.create_bucket(
                host=master,
                name=self.test_config.cluster.EVENTING_METADATA_BUCKET_NAME,
                ram_quota=self.test_config.cluster.eventing_metadata_bucket_mem_quota,
                replica_number=self.test_config.bucket.replica_number,
                eviction_policy=self.test_config.bucket.eviction_policy,
                backend_storage=self.test_config.bucket.backend_storage,
                conflict_resolution_type=self.test_config.bucket.conflict_resolution_type,
                flush=self.test_config.bucket.flush,
                durability=self.test_config.bucket.min_durability,
            )

    def capella_allow_client_ips(self):
        if self.cluster_spec.infrastructure_settings.get("peering_connection", None) is None:
            client_ips = self.cluster_spec.clients
            if self.cluster_spec.capella_backend == "aws":
                client_ips = [
                    dns.split(".")[0].removeprefix("ec2-").replace("-", ".") for dns in client_ips
                ]
            self.rest.add_allowed_ips_all_clusters(client_ips)

    def allow_ips_for_serverless_dbs(self):
        for db_id in self.test_config.buckets:
            self.rest.allow_my_ip(db_id)
            client_ips = self.cluster_spec.clients
            if self.cluster_spec.capella_backend == "aws":
                client_ips = [
                    dns.split(".")[0].removeprefix("ec2-").replace("-", ".") for dns in client_ips
                ]
            self.rest.add_allowed_ips(db_id, client_ips)

    def bypass_nebula_for_clients(self):
        client_ips = self.cluster_spec.clients
        if self.cluster_spec.capella_backend == "aws":
            client_ips = [
                dns.split(".")[0].removeprefix("ec2-").replace("-", ".") for dns in client_ips
            ]
        for ip in client_ips:
            self.rest.bypass_nebula(ip)

    def provision_serverless_db_keys(self):
        dbs = self.test_config.serverless_db.db_map
        for db_id in dbs.keys():
            resp = self.rest.get_db_api_key(db_id)
            dbs[db_id]["access"] = resp.json()["access"]
            dbs[db_id]["secret"] = resp.json()["secret"]

        self.test_config.serverless_db.update_db_map(dbs)

    def init_nebula_ssh(self):
        if (
            self.cluster_spec.serverless_infrastructure
            and self.cluster_spec.capella_backend == "aws"
        ):
            self.cluster_spec.set_nebula_instance_ids()
            self.remote.nebula_init_ssh()

    def open_capella_cluster_ports(self, port_ranges: Iterable[SGPortRange]):
        if self.cluster_spec.capella_infrastructure:
            logger.info(
                "Opening port ranges for Capella cluster:\n{}".format(
                    "\n".join(str(pr) for pr in port_ranges)
                )
            )

            if self.cluster_spec.capella_backend == "aws":
                self._open_capella_aws_cluster_ports(port_ranges)
            elif self.cluster_spec.capella_backend == "azure":
                self._open_capella_azure_cluster_ports(port_ranges)

    def _open_capella_aws_cluster_ports(self, port_ranges: Iterable[SGPortRange]):
        command_template = (
            "env/bin/aws ec2 authorize-security-group-ingress --region $AWS_REGION "
            "--group-id {} "
            "--ip-permissions {}"
        )

        for cluster_id, hostname in zip(self.rest.cluster_ids, self.cluster_spec.masters):
            if not (vpc_id := self.get_capella_aws_cluster_vpc_id(hostname)):
                logger.error(
                    "Failed to get Capella cluster VPC ID in order to get Security Group ID. "
                    "Cannot open desired ports for cluster {}.".format(cluster_id)
                )
                continue

            if not (sg_id := self.get_capella_aws_cluster_security_group_id(vpc_id)):
                logger.error(
                    "Failed to get Security Group ID for Capella cluster VPC. "
                    "Cannot open desired ports for cluster {}.".format(cluster_id)
                )
                continue

            ip_perms_template = (
                "IpProtocol={},FromPort={},ToPort={},IpRanges=[{{CidrIp=0.0.0.0/0}}]"
            )
            ip_perms_list = [
                ip_perms_template.format(pr.protocol, pr.min_port, pr.max_port)
                for pr in port_ranges
            ]

            command = command_template.format(sg_id, " ".join(ip_perms_list))

            run_local_shell_command(
                command=command,
                success_msg="Successfully opened ports for Capella cluster {}.".format(cluster_id),
                err_msg="Failed to open ports for Capella cluster {}.".format(cluster_id),
            )

    def _open_capella_azure_cluster_ports(self, port_ranges: Iterable[SGPortRange]):
        command_template = (
            "az network nsg rule create "
            "--resource-group rg-{0} "
            "--nsg-name cc-{0} "
            "--name {1} "
            "--priority {2} "
            "--destination-address-prefixes 'VirtualNetwork' "
            "--destination-port-ranges {3} "
            "--access Allow "
            "--direction Inbound"
        )

        # NSG rule priorities need to be unique within an NSG, so choose one at random from a
        # large enough range to minimize collisions
        rule_priority = random.randint(1000, 2000)
        rule_name = "AllowPerfrunnerInbound-{}".format(uuid4().hex[:6])

        err = set_azure_capella_subscription(
            self.cluster_spec.controlplane_settings.get("env", "sandbox")
        )
        if not err:
            for cluster_id in self.rest.cluster_ids:
                command = command_template.format(
                    cluster_id,
                    rule_name,
                    rule_priority,
                    " ".join(pr.port_range_str() for pr in port_ranges),
                )

                run_local_shell_command(
                    command=command,
                    success_msg="Successfully opened ports for Capella cluster {}.".format(
                        cluster_id
                    ),
                    err_msg="Failed to create NSG rule for cluster {}".format(cluster_id),
                )

            set_azure_perf_subscription()

    def get_capella_aws_cluster_vpc_id(self, cluster_node_hostname):
        command_template = (
            "ec2 describe-instances --region $AWS_REGION "
            "--filter Name=ip-address,Values=$(dig +short {}) "
            '--query "Reservations[].Instances[].VpcId" '
            "--output text"
        )

        vpc_id = run_aws_cli_command(command_template, cluster_node_hostname)

        if vpc_id is not None:
            logger.info("Found VPC ID: {}".format(vpc_id))

        return vpc_id

    def get_capella_aws_cluster_security_group_id(self, vpc_id):
        command_template = (
            "ec2 describe-security-groups --region $AWS_REGION "
            "--filters Name=vpc-id,Values={} Name=group-name,Values=default "
            '--query "SecurityGroups[*].GroupId" '
            "--output text"
        )

        sg_id = run_aws_cli_command(command_template, vpc_id)

        if sg_id is not None:
            logger.info("Found Security Group ID: {}".format(sg_id))

        return sg_id

    def set_nebula_log_levels(self):
        if dn_log_level := self.test_config.direct_nebula.log_level:
            self.remote.set_dn_log_level(dn_log_level)

        if dapi_log_level := self.test_config.data_api.log_level:
            self.remote.set_dapi_log_level(dapi_log_level)


class KubernetesClusterManager:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.cao_version = ''
        self.remote = RemoteHelper(cluster_spec)


    def configure_cluster(self):
        logger.info('Preparing cluster configuration file')
        with CAOCouchbaseClusterFile(self.cao_version, self.cluster_spec, self.test_config) as cm:
            cm.reload_from_dest()  # Installer will have set image tags in the dest file
            cm.set_server_count()
            cm.set_memory_quota()
            cm.set_services()
            cm.set_memory_settings()
            cm.set_cpu_settings()
            cm.set_index_settings()
            cm.set_auto_failover()
            cm.configure_auto_compaction()
            cm.configure_upgrade()
            self.cluster_file = cm.dest_file
            logger.info(f"Cluster config stored at {self.cluster_file}")

    def configure_autoscaling(self):
        autoscaling_settings = self.test_config.autoscaling_setting
        if not autoscaling_settings.enabled:
            return
        logger.info("Configuring auto-scaling")
        with CAOCouchbaseClusterFile(self.cao_version, self.cluster_spec, self.test_config) as cm:
            cm.reload_from_dest()  # Full cluster config already exists
            cm.configure_autoscaling(autoscaling_settings.server_group)
            self.remote.create_horizontal_pod_autoscaler(
                cm.get_cluster_name(),
                autoscaling_settings.server_group,
                int(autoscaling_settings.min_nodes),
                int(autoscaling_settings.max_nodes),
                autoscaling_settings.target_metric,
                autoscaling_settings.target_type,
                autoscaling_settings.target_value,
            )

    def deploy_couchbase_cluster(self):
        logger.info('Creating couchbase cluster')
        self.remote.create_from_file(self.cluster_file)
        logger.info('Waiting for cluster')
        self.remote.wait_for_cluster_ready()

    def create_buckets(self):
        bucket_quota = self.test_config.cluster.mem_quota // self.test_config.cluster.num_buckets
        self.remote.delete_all_buckets()
        for bucket_name in self.test_config.buckets:
            dest_file = None
            with CAOCouchbaseBucketFile(bucket_name) as bucket:
                bucket.set_bucket_settings(bucket_quota, self.test_config.bucket)
                dest_file = bucket.dest_file

            self.remote.create_from_file(dest_file)

        self.remote.wait_for_cluster_ready(timeout=60)

    def add_rbac_users(self):
        self.remote.create_from_file('cloud/operator/user-password-secret.yaml')
        self.remote.create_from_file('cloud/operator/bucket-user.yaml')
        self.remote.create_from_file('cloud/operator/rbac-admin-group.yaml')
        self.remote.create_from_file('cloud/operator/rbac-admin-role-binding.yaml')

    def install_syncgateway(self):
        if not self.cluster_spec.infrastructure_syncgateways:
            return

        logger.info("Deploying syncgateway")
        sgw_deployment = ""
        desired_size = len(self.cluster_spec.sgw_servers)
        # Currently we dont have a convenient way to pass sgw version as we dont run `sgw_install`.
        # As a workaround for these tests, we pass the sgw image as its own override section.
        # Example: sgw.image."ghcr.io/cb-vanilla/sync-gateway:3.2.0-319-enterprise"
        sgw_image = self.cluster_spec.infrastructure_section("sgw").get(
            "image", "couchbase/sync-gateway:latest"
        )
        with CAOSyncgatewayDeploymentFile(sgw_image, desired_size) as sgw_config:
            sgw_config.configure_sgw()
            sgw_deployment = sgw_config.dest_file

        self.remote.create_from_file(sgw_deployment)
        logger.info("Waiting for syncgateway and its service...")
        self.remote.wait_for_pods_ready("sync-gateway", desired_size)
        self.remote.wait_for_svc_deployment("syncgateway-service")

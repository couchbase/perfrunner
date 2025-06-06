from argparse import ArgumentParser
from multiprocessing import set_start_method

from perfrunner.helpers.cluster import (
    CapellaClusterManager,
    DefaultClusterManager,
    KubernetesClusterManager,
)
from perfrunner.helpers.config_files import record_time
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig

set_start_method("fork")


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster', dest='cluster_spec_fname',
                        required=True,
                        help='path to the cluster specification file')
    parser.add_argument('-t', '--test', dest='test_config_fname',
                        required=True,
                        help='path to test test configuration file')
    parser.add_argument('--cluster-name', dest='cluster_name',
                        help='if there are multiple clusters in the cluster spec, this lets you '
                             'name just one of them to set up (default: all clusters)')
    parser.add_argument('--verbose', dest='verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('override',
                        nargs='*',
                        help='custom cluster settings')

    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname, override=args.override)
    if args.cluster_name:
        cluster_spec.set_active_clusters_by_name([args.cluster_name])
    test_config = TestConfig()
    test_config.parse(args.test_config_fname, override=args.override)

    if cluster_spec.dynamic_infrastructure:
        cm = KubernetesClusterManager(cluster_spec, test_config)
        cm.configure_cluster()
        cm.configure_autoscaling()
        cm.deploy_couchbase_cluster()
        cm.add_rbac_users()
        cm.create_buckets()
        cm.install_syncgateway()
        return

    if cluster_spec.capella_infrastructure:
        # To get admin creds we need to use CP AWS account creds
        cluster_spec.set_capella_admin_credentials()

        if (prov_cluster := cluster_spec.prov_cluster_in_columnar_test) and (
            not args.cluster_name or args.cluster_name == prov_cluster
        ):
            cluster_spec.set_active_clusters_by_name([prov_cluster])
            cluster_spec.config["infrastructure"].pop("service")

        if (
            not cluster_spec.serverless_infrastructure and not cluster_spec.columnar_infrastructure
        ) or cluster_spec.prov_cluster_in_columnar_test:
            # If on provisioned Capella, we need to do two things before anything else:
            # 1. Create some DB credentials
            # 2. Whitelist the local IP
            rest = RestHelper(cluster_spec, bool(test_config.cluster.enable_n2n_encryption))
            rest.allow_my_ip_all_clusters()
            rest.create_db_user_all_clusters(*cluster_spec.rest_credentials)

        cm = CapellaClusterManager(cluster_spec, test_config, args.verbose)

        if cm.cluster_spec.serverless_infrastructure:
            cm.allow_ips_for_serverless_dbs()
            cm.provision_serverless_db_keys()
            cm.bypass_nebula_for_clients()
            cm.serverless_throttle()
            cm.init_nebula_ssh()
            cm.set_nebula_log_levels()
        elif (
            not cm.cluster_spec.columnar_infrastructure
            or cm.cluster_spec.prov_cluster_in_columnar_test
        ):
            with record_time("bucket"):
                cm.create_buckets()
                cm.create_eventing_buckets()
                cm.create_eventing_metadata_bucket()
            cm.capella_allow_client_ips()
        else:
            cm.set_analytics_settings()

        if (
            not cm.cluster_spec.columnar_infrastructure
            or cm.cluster_spec.prov_cluster_in_columnar_test
        ):
            if cm.test_config.collection.collection_map:
                cm.create_collections()

        return

    cm = DefaultClusterManager(cluster_spec, test_config, args.verbose)
    if cm.cluster_spec.infrastructure_kafka_clusters:
        cm.configure_kafka()
        cm.start_kafka()

    # Individual nodes
    if cm.cluster_spec.columnar_infrastructure:
        if cm.cluster_spec.infrastructure_kafka_clusters:
            cm.set_kafka_links_settings()
        cm.set_columnar_cloud_storage()
        cm.add_columnar_cloud_storage_creds()
        cm.set_columnar_storage_partitions()

    cm.disable_wan()
    cm.clear_login_history()
    cm.tune_memory_settings()
    cm.throttle_cpu()
    cm.enable_ipv6()
    cm.tune_logging()
    cm.restart_with_alternative_num_vbuckets()
    cm.flush_iptables()

    cm.set_systemd_resource_limits()

    cm.configure_internal_settings()
    cm.set_data_path()
    cm.set_index_path()
    cm.set_analytics_path()
    cm.set_mem_quotas()
    cm.set_services()
    cm.rename()
    cm.set_auth()
    cm.configure_xdcr_settings()

    # Cluster
    cm.add_server_groups()
    cm.add_nodes()
    cm.change_group_membership()
    cm.rebalance()
    # Configure NS Server and enable auto-failover after the new configuration are applied
    cm.configure_ns_server()
    cm.configure_auto_compaction()
    cm.enable_audit()
    cm.set_magma_min_quota()
    cm.serverless_throttle()

    if need_buckets := (
        not cm.cluster_spec.columnar_infrastructure and cm.test_config.cluster.num_buckets
    ):
        cm.create_buckets()
        cm.create_eventing_buckets()
        cm.create_eventing_metadata_bucket()
        cm.create_conflict_logging_buckets()
        cm.add_rbac_users()

    cm.restart_with_alternative_bucket_options()
    cm.set_index_settings()
    cm.set_query_settings()
    cm.set_analytics_settings()
    cm.set_x509_certificates()
    cm.set_min_tls_version()
    cm.set_cipher_suite()
    cm.wait_until_healthy()
    if need_buckets:
        cm.wait_until_warmed_up()
    cm.disable_ui_http()

    if need_buckets and cm.test_config.collection.collection_map:
        cm.create_collections()

    if cm.test_config.cluster.enable_query_awr:
        cm.create_query_awr_collection()

    cm.tweak_memory()
    cm.enable_n2n_encryption()
    cm.set_indexer_systemd_mem_limits()
    cm.enable_app_telemetry()


if __name__ == '__main__':
    main()

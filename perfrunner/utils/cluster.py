from argparse import ArgumentParser
from multiprocessing import set_start_method

from perfrunner.helpers.cluster import ClusterManager
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
    test_config = TestConfig()
    test_config.parse(args.test_config_fname, override=args.override)

    # If on provisioned Capella, we need to do two things before anything else:
    # 1. Create some DB credentials
    # 2. Whitelist the local IP
    if cluster_spec.capella_infrastructure and not cluster_spec.serverless_infrastructure:
        rest = RestHelper(cluster_spec, test_config)
        rest.allow_my_ip_all_clusters()
        rest.create_db_user_all_clusters(*cluster_spec.rest_credentials)

    cm = ClusterManager(cluster_spec, test_config, args.verbose)

    if cluster_spec.capella_infrastructure:
        if cluster_spec.serverless_infrastructure:
            cm.allow_ips_for_serverless_dbs()
            cm.provision_serverless_db_keys()
            cm.bypass_nebula_for_clients()
            cm.serverless_throttle()
        else:
            cm.get_capella_cluster_admin_creds()
            cm.create_buckets()
            cm.create_eventing_buckets()
            cm.create_eventing_metadata_bucket()
            cm.capella_allow_client_ips()
        if cm.test_config.collection.collection_map:
            cm.create_collections()
        cm.init_capella_ssh()
        return
    elif cluster_spec.dynamic_infrastructure:
        cm.set_mem_quotas()
        cm.set_services()
        cm.tune_memory_settings()
        cm.throttle_cpu()
        cm.enable_auto_failover()
        cm.configure_auto_compaction()
        cm.configure_autoscaling()
    else:
        # Individual nodes
        cm.serverless_mode()
        cm.disable_wan()
        cm.clear_login_history()
        cm.tune_memory_settings()
        cm.throttle_cpu()
        cm.enable_ipv6()
        cm.tune_logging()
        cm.restart_with_alternative_num_vbuckets()
        cm.flush_iptables()

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
        cm.enable_auto_failover()
        cm.configure_auto_compaction()
        cm.enable_audit()
        cm.set_magma_min_quota()
        cm.serverless_throttle()

    if cm.test_config.cluster.num_buckets:
        cm.create_buckets()
        cm.create_eventing_buckets()
        cm.create_eventing_metadata_bucket()
        cm.add_rbac_users()

    cm.restart_with_alternative_bucket_options()
    cm.set_index_settings()
    cm.set_query_settings()
    cm.set_analytics_settings()
    cm.set_x509_certificates()
    cm.set_min_tls_version()
    cm.set_cipher_suite()
    cm.wait_until_healthy()
    cm.wait_until_warmed_up()
    cm.disable_ui_http()

    if cm.test_config.collection.collection_map:
        cm.create_collections()

    cm.tweak_memory()
    cm.enable_n2n_encryption()

    if cm.test_config.profiling_settings.linux_perf_profile_flag:
        cm.install_cb_debug_rpm()


if __name__ == '__main__':
    main()

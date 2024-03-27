from argparse import ArgumentParser
from multiprocessing import set_start_method
from time import time

from logger import logger
from perfrunner.helpers.cluster import DefaultClusterManager, KubernetesClusterManager
from perfrunner.helpers.config_files import TimeTrackingFile
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
        return

    if cluster_spec.capella_infrastructure:
        cluster_spec.set_capella_admin_credentials()
        if not cluster_spec.serverless_infrastructure and not cluster_spec.goldfish_infrastructure:
            # If on provisioned Capella, we need to do two things before anything else:
            # 1. Create some DB credentials
            # 2. Whitelist the local IP
            rest = RestHelper(cluster_spec, test_config)
            rest.allow_my_ip_all_clusters()
            rest.create_db_user_all_clusters(*cluster_spec.rest_credentials)

    cm = DefaultClusterManager(cluster_spec, test_config, args.verbose)

    if cm.cluster_spec.capella_infrastructure:
        if cm.cluster_spec.serverless_infrastructure:
            cm.allow_ips_for_serverless_dbs()
            cm.provision_serverless_db_keys()
            cm.bypass_nebula_for_clients()
            cm.serverless_throttle()
            cm.init_nebula_ssh()
            cm.set_nebula_log_levels()
        elif not cm.cluster_spec.goldfish_infrastructure:
            if cm.test_config.deployment.monitor_deployment_time:
                logger.info("Start timing the bucket creation")
                t0 = time()
            cm.create_buckets()
            cm.create_eventing_buckets()
            cm.create_eventing_metadata_bucket()
            if cm.test_config.deployment.monitor_deployment_time:
                deployment_time = time() - t0
                logger.info("The total bucket creation time is: {}".format(deployment_time))
                with TimeTrackingFile() as t:
                    t.config['bucket'] = deployment_time
            cm.capella_allow_client_ips()

        if cm.test_config.collection.collection_map:
            cm.create_collections()

        return
    else:
        if cm.cluster_spec.infrastructure_kafka_clusters:
            cm.configure_kafka()
            cm.start_kafka()

        # Individual nodes
        if cm.cluster_spec.goldfish_infrastructure:
            if cm.cluster_spec.infrastructure_kafka_clusters:
                cm.set_kafka_links_settings()
            cm.set_goldfish_s3_bucket()
            cm.add_aws_credential()
            cm.set_goldfish_storage_partitions()

        cm.disable_wan()
        cm.clear_login_history()
        cm.tune_memory_settings()
        cm.throttle_cpu()
        cm.enable_ipv6()
        cm.tune_logging()
        cm.restart_with_alternative_num_vbuckets()
        cm.flush_iptables()
        cm.clear_system_limit_config()

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
    cm.deploy_couchbase_with_cgroups_for_index_nodes()


if __name__ == '__main__':
    main()

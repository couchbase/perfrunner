from argparse import ArgumentParser

from perfrunner.helpers.cluster import ClusterManager
from perfrunner.settings import ClusterSpec, TestConfig


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

    cm = ClusterManager(cluster_spec, test_config, args.verbose)

    # Individual nodes
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
    cm.add_nodes()
    cm.rebalance()
    cm.enable_auto_failover()
    cm.configure_auto_compaction()
    cm.enable_audit()

    if cm.test_config.cluster.num_buckets:
        cm.create_buckets()
        cm.create_eventing_buckets()
        cm.create_eventing_metadata_bucket()
        cm.add_rbac_users()

    cm.restart_with_alternative_bucket_options()
    cm.set_index_settings()
    cm.set_query_settings()
    cm.set_x509_certificates()
    cm.set_cipher_suite()
    cm.set_min_tls_version()
    cm.wait_until_healthy()
    cm.wait_until_warmed_up()

    if cm.test_config.collection.config:
        cm.create_collections()

    cm.tweak_memory()
    cm.enable_n2n_encryption()


if __name__ == '__main__':
    main()

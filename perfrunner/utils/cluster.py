from optparse import OptionParser

from perfrunner.helpers.cluster import ClusterManager
from perfrunner.settings import ClusterSpec, TestConfig


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
    cm.throttle_cpu()
    cm.remote.disable_wan()
    cm.enable_secrets()
    cm.tune_logging()
    cm.restart_with_alternative_num_vbuckets()

    cm.configure_internal_settings()
    cm.set_data_path()
    cm.set_services()
    cm.rename()
    cm.set_mem_quota()
    cm.set_auth()
    cm.configure_xdcr_settings()

    # Cluster
    if cm.group_number > 1:
        cm.create_server_groups()
    cm.add_nodes()
    cm.rebalance()
    cm.enable_auto_failover()
    cm.configure_auto_compaction()
    cm.enable_audit()

    if cm.test_config.cluster.num_buckets:
        cm.create_buckets()
        cm.add_rbac_users()

    cm.restart_with_alternative_bucket_options()
    cm.set_index_settings()
    cm.set_query_settings()

    cm.wait_until_warmed_up()
    cm.wait_until_healthy()

    cm.tweak_memory()


if __name__ == '__main__':
    main()

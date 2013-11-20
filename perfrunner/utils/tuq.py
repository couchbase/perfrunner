from logger import logger
from optparse import OptionParser
from fabric.operations import local

from perfrunner.settings import ClusterSpec, TestConfig

def get_options():
    usage = '%prog  -c cluster -t test_config'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-t', dest='test_config_fname',
                      help='path to test configuration file',
                      metavar='my_test.test')

    options, _ = parser.parse_args()
    if not options.cluster_spec_fname or not options.test_config_fname:
        parser.error('Missing mandatory parameter')

    return options

def main():
    options = get_options()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname)
    clusters = cluster_spec.get_clusters().values()
    if not clusters:
        logger.interrupt("invalid cluster spec")
    cred = cluster_spec.get_rest_credentials()

    test_config = TestConfig()
    test_config.parse(options.test_config_fname)
    tuq = test_config.get_tuq_settings()

    logger.info('Start tuq server locally')
    local('true || killall -q -9 cbq-engine')
    for cluster in clusters:
        local('%s -couchbase http://%s:%s@%s/ &' %
              (tuq.server_exe, cred[0], cred[1], cluster[0]))

if __name__ == "__main__":
    main()
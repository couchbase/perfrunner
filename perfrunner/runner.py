from optparse import OptionParser

from perfrunner import settings

from perfrunner.tests.kv import KVTest


def get_options():
    usage = '%prog -c cluster -t test_config test_class'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-t', dest='test_config',
                      help='path to test configuration file',
                      metavar='my_test.test')

    options, args = parser.parse_args()
    if not options.cluster or not options.test_config:
        parser.error('Missing mandatory parameter')

    return options, args[0]


def main():
    options, test_class = get_options()

    cluster_spec = settings.ClusterSpec()
    cluster_spec.parse(options.cluster)

    test_config = settings.TestConfig()
    test_config.parse(options.test_config)

    test = eval('{0}(cluster_spec, test_config)'.format(test_class))
    test.run()

if __name__ == '__main__':
    main()

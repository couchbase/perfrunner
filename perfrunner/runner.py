from optparse import OptionParser

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

    options, _ = parser.parse_args()
    if not options.cluster_spec_fname or not options.test_config_fname:
        parser.error('Missing mandatory parameter')

    return options


def main():
    options = get_options()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname)
    test_config = TestConfig()
    test_config.parse(options.test_config_fname)

    test_module = test_config.get_test_module()
    test_class = test_config.get_test_class()
    exec('from {0} import {1}'.format(test_module, test_class))

    with eval(test_class)(cluster_spec, test_config) as test:
        test.run()

if __name__ == '__main__':
    main()

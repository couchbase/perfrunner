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
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')
    parser.add_option('--remote', dest='remote', action='store_true',
                      help='use remote workers as workload generators')
    parser.add_option('--debug', dest='debug', action='store_true',
                      help='enable debug phase')

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

    test_module = test_config.test_case.test_module
    test_class = test_config.test_case.test_class
    exec('from {} import {}'.format(test_module, test_class))

    with eval(test_class)(cluster_spec, test_config, options.verbose) as test:
        test.run()

if __name__ == '__main__':
    main()

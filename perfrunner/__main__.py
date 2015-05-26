from optparse import OptionParser

from perfrunner.settings import ClusterSpec, TestConfig, Experiment


def get_options():
    usage = '%prog -c cluster -t test_config'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-t', dest='test_config_fname',
                      help='path to test configuration file',
                      metavar='my_test.test')
    parser.add_option('-e', dest='exp_fname',
                      help='path to experiment template',
                      metavar='experiment.json')
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')
    parser.add_option('--local', dest='local', action='store_true',
                      help='use localhost as workload generator')
    parser.add_option('--nodebug', dest='nodebug', action='store_true',
                      help='disable debug phase')

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
    experiment = options.exp_fname and Experiment(options.exp_fname)

    test_module = test_config.test_case.test_module
    test_class = test_config.test_case.test_class
    exec('from {} import {}'.format(test_module, test_class))

    with eval(test_class)(cluster_spec,
                          test_config,
                          options.verbose,
                          experiment) as test:
        test.run()

if __name__ == '__main__':
    main()

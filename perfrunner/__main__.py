from argparse import ArgumentParser

from perfrunner.settings import ClusterSpec, TestConfig


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster', dest='cluster_spec_fname',
                        required=True,
                        help='path to the cluster specification file')
    parser.add_argument('-t', '--test', dest='test_config_fname',
                        required=True,
                        help='path to test test configuration file')
    parser.add_argument('--verbose', dest='verbose', action='store_true',
                        help='enable verbose logging')
    parser.add_argument('--remote', dest='remote', action='store_true',
                        help='use remote workers as workload generators')
    parser.add_argument('--remote-copy', dest='remote_copy', action='store_true',
                        help='save a remote copy of a package')
    parser.add_argument('override',
                        nargs='*',
                        help='custom cluster and/or test settings')

    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname, args.override)
    test_config = TestConfig()
    test_config.parse(args.test_config_fname, args.override)

    test_module = test_config.test_case.test_module
    test_class = test_config.test_case.test_class
    exec('from {} import {}'.format(test_module, test_class))

    with eval(test_class)(cluster_spec, test_config, args.verbose) as test:
        test.run()


if __name__ == '__main__':
    main()

from optparse import OptionParser

from perfrunner.tests.compaction import DbCompactionTest
from perfrunner.tests.views import IndexTest
from perfrunner.tests.kv import KVTest
from perfrunner.tests.xdcr import XDCRTest


def get_options():
    usage = '%prog -c cluster -t test_config test_class'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-t', dest='test_config_fname',
                      help='path to test configuration file',
                      metavar='my_test.test')

    options, args = parser.parse_args()
    if not options.cluster_spec_fname or not options.test_config_fname:
        parser.error('Missing mandatory parameter')

    return options, args[0]


def main():
    options, test_class = get_options()

    test = eval(
        '{0}(options.cluster_spec_fname, options.test_config_fname)'.format(
            test_class)
    )
    test.run()

if __name__ == '__main__':
    main()

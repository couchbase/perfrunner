from optparse import OptionParser

from perfrunner.helpers import Helper
from perfrunner.helpers.rest import RestHelper


class ClusterManager(Helper):

    def __init__(self, cluster_spec, test_config):
        super(ClusterManager, self).__init__(cluster_spec, test_config)

        self.rest_helper = RestHelper(cluster_spec)

    def set_data_path(self):
        for cluster in self.clusters:
            for host_port in cluster:
                self.rest_helper.set_data_path(host_port)

    def set_auth(self):
        for cluster in self.clusters:
            for host_port in cluster:
                self.rest_helper.set_auth(host_port)

    def set_mem_quota(self):
        for cluster in self.clusters:
            for host_port in cluster:
                self.rest_helper.set_mem_quota(host_port, self.mem_quota)


def get_options():
    usage = '%prog -c cluster -t test_config'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-t', dest='test_config',
                      help='path to test configuration file',
                      metavar='my_test.test')

    options, _ = parser.parse_args()
    if not options.cluster or not options.test_config:
        parser.error('Missing mandatory parameter')

    return options


def main():
    options = get_options()

    cm = ClusterManager(options.cluster, options.test_config)
    cm.set_data_path()
    cm.set_auth()
    cm.set_mem_quota()

if __name__ == '__main__':
    main()

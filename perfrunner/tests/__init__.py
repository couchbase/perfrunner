from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig, TargetSettings


class PerfTest(object):

    def __init__(self, cluster_spec_fname, test_config_fname):
        self.cluster_spec = ClusterSpec()
        self.cluster_spec.parse(cluster_spec_fname)
        self.test_config = TestConfig()
        self.test_config.parse(test_config_fname)

        self.monitor = Monitor(cluster_spec_fname, test_config_fname)
        self.rest_helper = RestHelper(cluster_spec_fname)
        self.reporter = Reporter()


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        for cluster in self.cluster_spec.get_clusters():
            master = cluster[0]
            for bucket in self.test_config.get_buckets():
                yield TargetSettings(master, bucket, username, password)

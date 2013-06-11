from hashlib import md5

from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import TargetSettings


def target_hash(*args):
    int_hash = hash(args)
    str_hash = md5(hex(int_hash)).hexdigest()
    return str_hash[:6]


class PerfTest(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.monitor = Monitor(cluster_spec, test_config)
        self.rest = RestHelper(cluster_spec)
        self.reporter = Reporter()

        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config)


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        for cluster in self.cluster_spec.get_clusters():
            master = cluster[0]
            for bucket in self.test_config.get_buckets():
                prefix = target_hash(master, bucket)
                yield TargetSettings(master, bucket, username, password, prefix)

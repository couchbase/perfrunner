from perfrunner.settings import TargetSettings


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        for cluster in self.cluster_spec.get_clusters():
            master = cluster[0]
            for i in xrange(self.test_config.get_num_buckets()):
                bucket = 'bucket-{0}'.format(i + 1)
                yield TargetSettings(master, bucket, username, password)

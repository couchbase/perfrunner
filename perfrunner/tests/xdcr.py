from hashlib import md5

from perfrunner.helpers.rest import RestHelper
from perfrunner.tests import TargetIterator
from perfrunner.tests.kv import KVTest


class XDCRTest(KVTest):

    def __init__(self, cluster_spec, test_config):
        super(XDCRTest, self).__init__(cluster_spec, test_config)
        self.rest_helper = RestHelper(cluster_spec)

    def _start_replication(self, c1, c2):
        for bucket in self.test_config.get_buckets():
            stream_hash = hash((c1, c2, bucket))
            name = md5(hex(stream_hash)).hexdigest()

            self.rest_helper.add_remote_cluster(c1, c2, name)
            self.rest_helper.start_replication(c1, bucket, bucket, name)

    def _init_xdcr(self):
        xdcr_settings = self.test_config.get_xdcr_settings()

        c1, c2 = self.cluster_spec.get_clusters()

        if xdcr_settings.replication_type == 'unidir':
            self._start_replication(c1, c2)
        if xdcr_settings.replication_type == 'bidir':
            self._start_replication(c1, c2)
            self._start_replication(c2, c1)

        for target_settings in TargetIterator(self.cluster_spec,
                                              self.test_config):
            self.monitor.monitor_xdcr_replication(target_settings)

    def run(self):
        self._run_load_phase()
        self._init_xdcr()

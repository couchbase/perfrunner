from hashlib import md5

from perfrunner.tests import TargetIterator
from perfrunner.tests.kv import KVTest


class XDCRTest(KVTest):

    def _start_replication(self, m1, m2):
        for bucket in self.test_config.get_buckets():
            stream_hash = hash((m1, m2, bucket))
            name = md5(hex(stream_hash)).hexdigest()

            self.rest.add_remote_cluster(m1, m2, name)
            self.rest.start_replication(m1, bucket, bucket, name)

    def _init_xdcr(self):
        xdcr_settings = self.test_config.get_xdcr_settings()

        c1, c2 = self.cluster_spec.get_clusters()
        m1, m2 = c1[0], c2[0]

        if xdcr_settings.replication_type == 'unidir':
            self._start_replication(m1, m2)
        if xdcr_settings.replication_type == 'bidir':
            self._start_replication(m1, m2)
            self._start_replication(m1, m2)

        for target_settings in TargetIterator(self.cluster_spec,
                                              self.test_config):
            self.monitor.monitor_xdcr_replication(target_settings)

    def run(self):
        self._run_load_phase()
        self._init_xdcr()

from perfrunner.tests import TargetIterator, target_hash
from perfrunner.tests.kv import KVTest


class XDCRTest(KVTest):

    def _start_replication(self, m1, m2):
        name = target_hash(m1, m2)
        self.rest.add_remote_cluster(m1, m2, name)

        for bucket in self.test_config.get_buckets():
            self.rest.start_replication(m1, bucket, bucket, name)

    def _init_xdcr(self):
        xdcr_settings = self.test_config.get_xdcr_settings()

        c1, c2 = self.cluster_spec.get_clusters()
        m1, m2 = c1[0], c2[0]

        if xdcr_settings.replication_type == 'unidir':
            self._start_replication(m1, m2)
        if xdcr_settings.replication_type == 'bidir':
            self._start_replication(m1, m2)
            self._start_replication(m2, m1)

        for target_settings in TargetIterator(self.cluster_spec,
                                              self.test_config):
            self.monitor.monitor_xdcr_replication(target_settings)

    def run(self):
        self._run_load_phase()

        self.reporter.start()
        self._init_xdcr()
        self.reporter.finish('Initial replication')

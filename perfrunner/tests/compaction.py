from perfrunner.tests import TargetIterator
from perfrunner.tests.kv import KVTest


class DbCompactionTest(KVTest):

    def _compact(self):
        self.reporter.start()
        for target_settings in TargetIterator(self.cluster_spec,
                                              self.test_config):
            self.rest.trigger_bucket_compaction(target_settings.node,
                                                target_settings.bucket)
            self.monitor.monitor_bucket_fragmentation(target_settings)
        self.reporter.finish('Bucket compaction')

    def run(self):
        self._run_load_phase()  # initial load
        self._run_load_phase()  # extra mutations for fragmentation
        self._compact()

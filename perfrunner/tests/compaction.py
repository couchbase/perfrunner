from perfrunner.tests.kv import KVTest


class DbCompactionTest(KVTest):

    def _compact(self):
        for target in self.target_iterator:
            self.rest.trigger_bucket_compaction(target.node,
                                                target.bucket)
            self.monitor.monitor_bucket_fragmentation(target)

    def run(self):
        self._run_load_phase()  # initial load
        self._run_load_phase()  # extra mutations for fragmentation

        self.reporter.start()
        self._compact()
        self.reporter.finish('Bucket compaction')

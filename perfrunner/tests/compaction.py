from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


class BucketCompactionTest(PerfTest):

    def run(self):
        self._run_load_phase()  # initial load
        self._run_load_phase()  # extra mutations for bucket fragmentation

        self.reporter.start()
        self._compact_bucket()
        value = self.reporter.finish('Bucket compaction')
        metric = self.cluster_spec.fname + self.test_config.fname
        self.reporter.post(self, metric, value)


class IndexCompactionTest(IndexTest):

    def run(self):
        self._run_load_phase()
        self._compact_bucket()

        self._define_ddocs()
        self._build_index()

        self._run_load_phase()  # extra mutations for index fragmentation
        self._compact_bucket()
        self._build_index()

        self.reporter.start()
        self._compact_index()
        self.reporter.finish('Index compaction')

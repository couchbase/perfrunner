from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


class BucketCompactionTest(PerfTest):

    def run(self):
        self._run_load_phase()  # initial load
        self._run_load_phase()  # extra mutations for bucket fragmentation

        self.reporter.start()
        self._compact_bucket()
        value = self.reporter.finish('Bucket compaction')
        self.reporter.post_to_sf(self, value)


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
        value = self.reporter.finish('Index compaction')
        self.reporter.post_to_sf(self, value)

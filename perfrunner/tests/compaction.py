from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


class BucketCompactionTest(PerfTest):

    def run(self):
        self.run_load_phase()  # initial load
        self.wait_for_persistence()
        self.run_load_phase()  # extra mutations for bucket fragmentation
        self.wait_for_persistence()

        self.reporter.start()
        self.compact_bucket()
        value = self.reporter.finish('Bucket compaction')
        self.reporter.post_to_sf(value)


class IndexCompactionTest(IndexTest):

    def run(self):
        self.run_load_phase()
        self.wait_for_persistence()

        self.define_ddocs()
        self.build_index()

        self.run_load_phase()  # extra mutations for index fragmentation
        self.wait_for_persistence()
        self.build_index()

        self.reporter.start()
        self.compact_index()
        value = self.reporter.finish('Index compaction')
        self.reporter.post_to_sf(value)

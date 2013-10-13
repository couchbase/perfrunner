from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


class BucketCompactionTest(PerfTest):

    @with_stats()
    def compact_bucket(self):
        super(BucketCompactionTest, self).compact_bucket()

    def run(self):
        self.load()  # initial load
        self.wait_for_persistence()
        self.load()  # extra mutations for bucket fragmentation
        self.wait_for_persistence()

        self.reporter.start()
        self.compact_bucket()
        value = self.reporter.finish('Bucket compaction')
        self.reporter.post_to_sf(value)


class IndexCompactionTest(IndexTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.define_ddocs()
        self.build_index()

        self.load()  # extra mutations for index fragmentation
        self.wait_for_persistence()
        self.build_index()

        self.reporter.start()
        self.compact_index()
        value = self.reporter.finish('Index compaction')
        self.reporter.post_to_sf(value)

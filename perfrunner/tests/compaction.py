from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


class BucketCompactionTest(PerfTest):

    @with_stats
    def compact_bucket(self):
        super(BucketCompactionTest, self).compact_bucket()

    def run(self):
        self.load()  # initial load
        self.wait_for_persistence()
        self.load()  # extra mutations for bucket fragmentation
        self.wait_for_persistence()

        from_ts, to_ts = self.compact_bucket()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Bucket compaction', time_elapsed)
        self.reporter.post_to_sf(time_elapsed)


class IndexCompactionTest(IndexTest):

    def access(self):
        super(IndexCompactionTest, self).timer()

    @with_stats
    def compact_index(self):
        super(IndexCompactionTest, self).compact_index()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg_with_ddocs()
        self.access()
        self.wait_for_persistence()

        self.reporter.start()
        from_ts, to_ts = self.compact_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Index compaction', time_elapsed)
        self.reporter.post_to_sf(time_elapsed)

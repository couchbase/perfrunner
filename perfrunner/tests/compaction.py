from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


class BucketCompactionTest(PerfTest):

    ALL_BUCKETS = True

    @with_stats
    def compact_bucket(self):
        super(BucketCompactionTest, self).compact_bucket()

    def run(self):
        self.load()  # initial load
        self.wait_for_persistence()
        self.load()  # extra mutations for fragmentation
        self.wait_for_persistence()

        from_ts, to_ts = self.compact_bucket()
        time_elapsed = (to_ts - from_ts) / 1000.0

        self.reporter.finish('Bucket compaction', time_elapsed)
        compaction_speed = \
            self.metric_helper.calc_compaction_speed(time_elapsed, bucket=True)
        self.reporter.post_to_sf(compaction_speed)


class IndexCompactionTest(IndexTest):

    ALL_BUCKETS = True

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

        self.load()  # extra mutations for fragmentation
        self.wait_for_persistence()
        self.build_index()

        self.reporter.start()
        from_ts, to_ts = self.compact_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        self.reporter.finish('Index compaction', time_elapsed)
        compaction_speed = \
            self.metric_helper.calc_compaction_speed(time_elapsed, bucket=False)
        self.reporter.post_to_sf(compaction_speed)

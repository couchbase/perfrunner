from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats


class KVTest(PerfTest):

    @with_stats()
    def timer(self):
        super(KVTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.access_bg()
        self.timer()
        self.shutdown_event.set()


class BgFetcherTest(KVTest):

    def run(self):
        super(BgFetcherTest, self).timer()
        self.reporter.post_to_sf(self.metric_helper.calc_avg_ep_bg_fetched())


class FlusherTest(KVTest):

    def run(self):
        super(FlusherTest, self).timer()
        self.reporter.post_to_sf(self.metric_helper.calc_avg_drain_rate())

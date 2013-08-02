import time

from logger import logger

from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats


class KVTest(PerfTest):

    @with_stats(latency=True)
    def access(self):
        super(KVTest, self).access()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.access()


class BgFetcherTest(PerfTest):

    @with_stats()
    def timer(self):
        access_settings = self.test_config.get_access_settings()
        logger.info('Running phase for {0} seconds'.format(access_settings.time))
        time.sleep(access_settings.time)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.access_bg()
        self.timer()
        self.shutdown_event.set()

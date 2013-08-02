import time

from logger import logger
from mc_bin_client.mc_bin_client import MemcachedClient

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


class FlusherTest(PerfTest):

    def stop_persistence(self):
        for target in self.target_iterator:
            mc = MemcachedClient(host=target.node, port=11210)
            mc.sasl_auth_plain(target.bucket, '')
            mc.stop_persistence()

    @with_stats()
    def drain(self):
        for target in self.target_iterator:
            mc = MemcachedClient(host=target.node, port=11210)
            mc.sasl_auth_plain(target.bucket, '')
            mc.start_persistence()
        for target in self.target_iterator:
            self.monitor.monitor_disk_queue(target)

    def run(self):
        self.stop_persistence()
        self.load()
        self.drain()

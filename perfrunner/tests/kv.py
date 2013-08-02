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


class McIterator(object):

    def __init__(self, test):
        self.test = test

    def __iter__(self):
        initial_nodes = self.test.test_config.get_initial_nodes()
        for servers in self.test.cluster_spec.get_clusters().values():
            for host_port in servers[:initial_nodes]:
                host = host_port.split(':')[0]
                for bucket in self.test.test_config.get_buckets():
                    mc = MemcachedClient(host=host, port=11210)
                    mc.sasl_auth_plain(bucket, '')
                    yield mc


class FlusherTest(PerfTest):

    def stop_persistence(self):
        for mc in McIterator(self.target_iterator):
            mc.stop_persistence()
            mc.close()

    @with_stats()
    def drain(self):
        for mc in McIterator(self.target_iterator):
            mc.start_persistence()
            mc.close()
        for target in self.target_iterator:
            self.monitor.monitor_disk_queue(target)

    def run(self):
        self.stop_persistence()
        self.load()
        self.drain()

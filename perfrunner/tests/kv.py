from mc_bin_client.mc_bin_client import MemcachedClient

from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats


class KVTest(PerfTest):

    @with_stats()
    def access(self):
        super(KVTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.workload = self.test_config.get_access_settings()
        self.access_bg()
        self.access()
        self.shutdown_event.set()


class LatencyTest(KVTest):

    @with_stats(latency=True)
    def access(self):
        super(LatencyTest, self).timer()

    def run(self):
        super(LatencyTest, self).run()
        for operation in ('get', 'set'):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_kv_latency(operation=operation,
                                                    percentile=0.9)
            )


class BgFetcherTest(KVTest):

    def run(self):
        super(BgFetcherTest, self).run()
        self.reporter.post_to_sf(self.metric_helper.calc_avg_ep_bg_fetched())


class FlusherTest(KVTest):

    def stop_persistence(self):
        for target in self.target_iterator:
            host = target.node.split(':')[0]
            mc = MemcachedClient(host=host, port=11210)
            mc.sasl_auth_plain(user=target.bucket, password=target.password)
            mc.stop_persistence()

    @with_stats()
    def drain(self):
        for target in self.target_iterator:
            host = target.node.split(':')[0]
            mc = MemcachedClient(host=host, port=11210)
            mc.sasl_auth_plain(user=target.bucket, password=target.password)
            mc.start_persistence()
        for target in self.target_iterator:
            self.monitor.monitor_disk_queue(target)

    def run(self):
        self.stop_persistence()
        self.load()
        self.drain()
        self.reporter.post_to_sf(self.metric_helper.calc_avg_drain_rate())

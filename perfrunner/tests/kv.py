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

    def mc_iterator(self):
        _, password = self.cluster_spec.get_rest_credentials()
        for bucket in self.test_config.get_buckets():
            for host in self.cluster_spec.get_all_hosts():
                mc = MemcachedClient(host=host, port=11210)
                mc.sasl_auth_plain(user=bucket, password=password)
                yield mc

    def stop_persistence(self):
        for mc in self.mc_iterator():
            mc.stop_persistence()

    def start_persistence(self):
        for mc in self.mc_iterator():
            mc.start_persistence()

    @with_stats()
    def drain(self):
        for target in self.target_iterator:
            self.monitor.monitor_disk_queue(target)

    def run(self):
        self.stop_persistence()
        self.load()

        self.access_bg()
        self.start_persistence()

        self.reporter.start()
        self.drain()
        time_elapsed = self.reporter.finish('Drain')

        self.shutdown_event.set()

        self.reporter.post_to_sf(
            self.metric_helper.calc_avg_drain_rate(time_elapsed)
        )


class BeamRssTest(KVTest):

    def run(self):
        super(BeamRssTest, self).run()
        if self.remote.os != 'Cygwin':
            self.reporter.post_to_sf(*self.metric_helper.calc_max_beam_rss())


class WarmupTest(PerfTest):

    def access(self):
        super(WarmupTest, self).timer()

    def warmup(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for target in self.target_iterator:
            warmup_time = self.monitor.monitor_warmup(target)
            return float(warmup_time) / 10 ** 6 / 60  # min

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.workload = self.test_config.get_access_settings()
        self.access_bg()
        self.access()
        self.wait_for_persistence()
        self.compact_bucket()
        self.shutdown_event.set()

        warmup_time = self.warmup()

        self.reporter.post_to_sf(warmup_time)

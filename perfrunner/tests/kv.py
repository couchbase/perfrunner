from mc_bin_client.mc_bin_client import MemcachedClient

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class KVTest(PerfTest):

    @with_stats
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


class ObserveLatencyTest(KVTest):

    COLLECTORS = {'observe_latency': True}

    def run(self):
        super(ObserveLatencyTest, self).run()

        latency = self.reporter.post_to_sf(
            *self.metric_helper.calc_observe_latency(percentile=95)
        )
        if hasattr(self, 'experiment'):
            self.experiment.post_results(latency)


class MixedLatencyTest(KVTest):

    COLLECTORS = {'latency': True}

    def run(self):
        super(MixedLatencyTest, self).run()
        for operation in ('get', 'set'):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_kv_latency(operation=operation,
                                                    percentile=95)
            )


class ReadLatencyTest(MixedLatencyTest):

    COLLECTORS = {'latency': True}

    def run(self):
        super(MixedLatencyTest, self).run()
        self.reporter.post_to_sf(
            *self.metric_helper.calc_kv_latency(operation='get',
                                                percentile=95)
        )


class BgFetcherTest(KVTest):

    COLLECTORS = {'latency': True}

    def run(self):
        super(BgFetcherTest, self).run()
        self.reporter.post_to_sf(self.metric_helper.calc_avg_ep_bg_fetched())


class FlusherTest(KVTest):

    def mc_iterator(self):
        _, password = self.cluster_spec.get_rest_credentials()
        for bucket in self.test_config.get_buckets():
            for hostname in self.cluster_spec.yield_hostnames():
                mc = MemcachedClient(host=hostname, port=11210)
                mc.sasl_auth_plain(user=bucket, password=password)
                yield mc

    def stop_persistence(self):
        for mc in self.mc_iterator():
            mc.stop_persistence()

    def start_persistence(self):
        for mc in self.mc_iterator():
            mc.start_persistence()

    @with_stats
    def drain(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.get_buckets():
                self.monitor.monitor_disk_queue(master, bucket)

    def run(self):
        self.stop_persistence()
        self.load()

        self.access_bg()
        self.start_persistence()
        from_ts, to_ts = self.drain()
        time_elapsed = (to_ts - from_ts) / 1000.0

        self.reporter.finish('Drain', time_elapsed)
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
        warmup_time = 0
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.get_buckets():
                host = master.node.split(':')[0]
                warmup_time += self.monitor.monitor_warmup(self.memcached,
                                                           host, bucket)
        return round(float(warmup_time) / 10 ** 6 / 60, 1)  # min

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

        warmup_time = self.warmup()

        self.reporter.post_to_sf(warmup_time)

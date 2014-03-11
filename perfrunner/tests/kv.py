from logger import logger
from mc_bin_client.mc_bin_client import MemcachedClient, MemcachedError
from tap import TAP
from upr import UprClient
from upr.constants import CMD_STREAM_REQ

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.workloads.tcmalloc import WorkloadGen


class KVTest(PerfTest):

    @with_stats
    def access(self):
        super(KVTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()


class ObserveLatencyTest(KVTest):

    COLLECTORS = {'observe_latency': True}

    ALL_BUCKETS = True

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

    ALL_BUCKETS = True

    def run(self):
        super(BgFetcherTest, self).run()
        self.reporter.post_to_sf(self.metric_helper.calc_avg_ep_bg_fetched())


class DrainTest(KVTest):

    ALL_BUCKETS = True

    def run(self):
        super(DrainTest, self).run()
        drain_rate = self.reporter.post_to_sf(
            self.metric_helper.calc_avg_drain_rate()
        )
        if hasattr(self, 'experiment'):
            self.experiment.post_results(drain_rate)


class FlusherTest(KVTest):

    def mc_iterator(self):
        _, password = self.cluster_spec.rest_credentials
        for hostname in self.cluster_spec.yield_hostnames():
            for bucket in self.test_config.buckets:
                mc = MemcachedClient(host=hostname, port=11210)
                try:
                    mc.sasl_auth_plain(user=bucket, password=password)
                    yield mc
                except MemcachedError:
                    logger.warn('Auth failure')

    def stop_persistence(self):
        for mc in self.mc_iterator():
            mc.stop_persistence()

    def start_persistence(self):
        for mc in self.mc_iterator():
            mc.start_persistence()

    @with_stats
    def drain(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
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
            self.metric_helper.calc_max_drain_rate(time_elapsed)
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
            for bucket in self.test_config.buckets:
                host = master.split(':')[0]
                warmup_time += self.monitor.monitor_warmup(self.memcached,
                                                           host, bucket)
        return round(warmup_time / 10 ** 6 / 60, 1)  # min

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()

        self.wait_for_persistence()
        self.compact_bucket()

        warmup_time = self.warmup()

        self.reporter.post_to_sf(warmup_time)


class TapTest(PerfTest):

    def consume(self):
        logger.info('Reading data via TAP')
        _, password = self.cluster_spec.rest_credentials
        for master in self.cluster_spec.yield_masters():
            host = master.split(':')[0]
            for bucket in self.test_config.buckets:
                tap = TAP(host=host, bucket=bucket, password=password)
                while True:
                    status, batch = tap.provide_batch()
                    if not batch or status:
                        break

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.reporter.start()
        self.consume()
        self.reporter.finish('Backfilling')


class UprTest(TapTest):

    MAX_SEQNO = 0xFFFFFFFFFFFFFFFF

    def consume(self):
        _, password = self.cluster_spec.rest_credentials
        for master in self.cluster_spec.yield_masters():
            host = master.split(':')[0]
            for bucket in self.test_config.buckets:
                logger.info(
                    'Reading data via UPR from {}/{}'.format(host, bucket)
                )
                upr_client = UprClient(host=host, port=11210)
                upr_client.sasl_auth_plain(username=bucket, password=password)
                for vbucket in range(1024):
                    logger.info('Reading vbucket {}'.format(vbucket))
                    op = upr_client.stream_req(vb=vbucket,
                                               flags=0,
                                               start_seqno=0,
                                               end_seqno=self.MAX_SEQNO,
                                               vb_uuid=0,
                                               high_seqno=0)
                    while op.has_response():
                        response = op.next_response()
                        if response['opcode'] != CMD_STREAM_REQ:
                            break
                    upr_client.close_stream(vbucket=vbucket)
                upr_client.shutdown()


class FragmentationTest(PerfTest):

    @with_stats
    def load_and_append(self):
        _, password = self.cluster_spec.rest_credentials
        WorkloadGen(self.test_config.load_settings.items,
                    self.master_node, self.test_config.buckets[0],
                    password).run()

    def calc_fragmentation_ratio(self):
        ratios = []
        _, password = self.cluster_spec.rest_credentials
        for target in self.target_iterator:
            host = target.node.split(':')[0]
            stats = self.memcached.get_stats(host, target.bucket,
                                             stats='memory')
            ratio = float(stats['mem_used']) / float(stats['total_heap_bytes'])
            ratios.append(ratio)
        ratio = 100 * (1 - sum(ratios) / len(ratios))
        ratio = round(ratio, 1)
        logger.info('Fragmentation: {}'.format(ratio))
        return ratio

    def run(self):
        self.load_and_append()
        fragmentation_ratio = self.calc_fragmentation_ratio()
        self.reporter.post_to_sf(fragmentation_ratio)


class FragmentationLargeTest(FragmentationTest):

    @with_stats
    def load_and_append(self):
        _, password = self.cluster_spec.rest_credentials
        WorkloadGen(self.test_config.load_settings.items,
                    self.master_node, self.test_config.buckets[0], password,
                    small=False).run()

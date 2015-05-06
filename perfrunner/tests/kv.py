import random
from threading import Thread
from time import time, sleep

import numpy as np
from couchbase import Couchbase
from couchbase.user_constants import OBS_NOTFOUND
from logger import logger
from mc_bin_client.mc_bin_client import MemcachedClient, MemcachedError
from tap import TAP

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import pretty_dict, uhex, log_phase
from perfrunner.helpers.worker import run_pillowfight_via_celery
from perfrunner.tests import PerfTest
from perfrunner.workloads.revAB.__main__ import produce_AB
from perfrunner.workloads.revAB.graph import generate_graph, PersonIterator
from perfrunner.workloads.tcmalloc import WorkloadGen
from perfrunner.workloads.pathoGen import PathoGen
from perfrunner.workloads.pillowfight import Pillowfight


class KVTest(PerfTest):

    """
    The most basic KV workflow:
        Initial data load ->
            Persistence and intra-cluster replication (for consistency) ->
                Data compaction (for consistency) ->
                    "Hot" load or working set warm up ->
                        "access" phase or active workload
    """

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


class PersistLatencyTest(KVTest):

    """
    The same as base test but persistence latency is measured.
    """

    COLLECTORS = {'persist_latency': True}

    ALL_BUCKETS = True

    def run(self):
        super(PersistLatencyTest, self).run()

        if self.test_config.stats_settings.enabled:
            latency = self.reporter.post_to_sf(
                *self.metric_helper.calc_observe_latency(percentile=95)
            )
            if hasattr(self, 'experiment'):
                self.experiment.post_results(latency)


class ReplicateLatencyTest(PersistLatencyTest):

    """
    The same as base test but intra-cluster replication latency is measured.
    """

    COLLECTORS = {'replicate_latency': True}


class MixedLatencyTest(KVTest):

    """
    Enables reporting of GET and SET latency.
    """

    COLLECTORS = {'latency': True}

    def run(self):
        super(MixedLatencyTest, self).run()
        if self.test_config.stats_settings.enabled:
            for operation in ('get', 'set'):
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_kv_latency(operation=operation,
                                                        percentile=95)
                )


class ReadLatencyTest(MixedLatencyTest):

    """
    Enables reporting of GET latency.
    """

    COLLECTORS = {'latency': True}

    def run(self):
        super(MixedLatencyTest, self).run()
        if self.test_config.stats_settings.enabled:
            latency_get = self.reporter.post_to_sf(
                *self.metric_helper.calc_kv_latency(operation='get',
                                                    percentile=95)
            )
            if hasattr(self, 'experiment'):
                self.experiment.post_results(latency_get)


class BgFetcherTest(KVTest):

    """
    Enables reporting of average BgFetcher wait time (disk fetches).
    """

    COLLECTORS = {'latency': True}

    ALL_BUCKETS = True

    def run(self):
        super(BgFetcherTest, self).run()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(self.metric_helper.calc_avg_bg_wait_time())


class DrainTest(KVTest):

    """
    Enables reporting of average disk write queue size.
    """

    ALL_BUCKETS = True

    def run(self):
        super(DrainTest, self).run()
        if self.test_config.stats_settings.enabled:
            drain_rate = self.reporter.post_to_sf(
                self.metric_helper.calc_avg_disk_write_queue()
            )
            if hasattr(self, 'experiment'):
                self.experiment.post_results(drain_rate)


class FlusherTest(KVTest):

    """
    The maximum drain rate benchmark. Data set is loaded while persistence is
    disabled. After that persistence is enabled and ep-engine commits data with
    maximum possible speed (using very large batches).

    Please notice that measured drain rate has nothing to do with typical
    production characteristics. It only used as baseline / reference.
    """

    def mc_iterator(self):
        password = self.test_config.bucket.password
        for host_port in self.cluster_spec.yield_servers():
            host = host_port.split(':')[0]
            memcached_port = self.rest.get_memcached_port(host_port)
            for bucket in self.test_config.buckets:
                mc = MemcachedClient(host=host, port=memcached_port)
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
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                self.metric_helper.calc_max_drain_rate(time_elapsed)
            )


class BeamRssTest(KVTest):

    """
    Enables reporting of Erlang (beam.smp process) memory usage.
    """

    def run(self):
        super(BeamRssTest, self).run()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_max_beam_rss()
            )


class WarmupTest(PerfTest):

    """
    After typical workload we restart all nodes and measure time it takes
    to perform cluster warm up (internal technology, not to be confused with
    "hot" load phase).
    """

    def access(self):
        super(WarmupTest, self).timer()

    def warmup(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        warmup_time = 0
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                warmup_time += self.monitor.monitor_warmup(self.memcached,
                                                           master, bucket)
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

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(warmup_time)


class TapTest(PerfTest):

    """
    Load data and then read entire data set via TAP protocol (single-threaded
    consumer).
    """

    def consume(self):
        logger.info('Reading data via TAP')
        password = self.test_config.bucket.password
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


class FragmentationTest(PerfTest):

    """
    This test implements the append-only scenario:
    1. Single node.
    2. Load X items, 700-1400 bytes, average 1KB (11-22 fields).
    3. Append data
        3.1. Mark first 80% of items as working set.
        3.2. Randomly update 75% of items in working set by adding 1 field at a time (62 bytes).
        3.3. Mark first 40% of items as working set.
        3.4. Randomly update 75% of items in working set by adding 1 field at a time (62 bytes).
        3.5. Mark first 20% of items as working set.
        3.6. Randomly update 75% of items in working set by adding 1 field at a time (62 bytes).
    4. Repeat step #3 5 times.

    See workloads/tcmalloc.py for details.

    Scenario described above allows to spot issues with memory/allocator
    fragmentation.
    """

    @with_stats
    def load_and_append(self):
        password = self.test_config.bucket.password
        WorkloadGen(self.test_config.load_settings.items,
                    self.master_node, self.test_config.buckets[0],
                    password).run()

    def calc_fragmentation_ratio(self):
        ratios = []
        for target in self.target_iterator:
            host = target.node.split(':')[0]
            port = self.rest.get_memcached_port(target.node)
            stats = self.memcached.get_stats(host, port, target.bucket,
                                             stats='memory')
            ratio = float(stats['mem_used']) / float(stats['total_heap_bytes'])
            ratios.append(ratio)
        ratio = 100 * (1 - sum(ratios) / len(ratios))
        ratio = round(ratio, 1)
        logger.info('Fragmentation: {}'.format(ratio))
        return ratio

    def run(self):
        self.load_and_append()
        if self.test_config.stats_settings.enabled:
            fragmentation_ratio = self.calc_fragmentation_ratio()
            self.reporter.post_to_sf(fragmentation_ratio)


class FragmentationLargeTest(FragmentationTest):

    """
    The same as base test but large documents are used.
    """

    @with_stats
    def load_and_append(self):
        password = self.test_config.bucket.password
        WorkloadGen(self.test_config.load_settings.items,
                    self.master_node, self.test_config.buckets[0], password,
                    small=False).run()


class ReplicationTest(PerfTest):

    """
    Quick replication test. Single documents are sequentially inserted and
    replication latency is measured after each insert.
    """

    NUM_SAMPLES = 5000

    def measure_latency(self):
        logger.info('Measuring replication latency')
        timings = []
        found = lambda cb: [
            v for v in cb.observe(item).value if v.flags != OBS_NOTFOUND
        ]
        password = self.test_config.bucket.password
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                host, port = master.split(':')
                cb = Couchbase.connect(host=host, port=port,
                                       bucket=bucket, password=password)
                for _ in range(self.NUM_SAMPLES):
                    item = uhex()
                    cb.set(item, item)
                    t0 = time()
                    while len(found(cb)) != 2:
                        sleep(0.001)
                    latency = 1000 * (time() - t0)  # s -> ms
                    logger.info(latency)
                    timings.append(latency)

        summary = {
            'min': round(min(timings), 1),
            'max': round(max(timings), 1),
            'mean': round(np.mean(timings), 1),
            '80th': round(np.percentile(timings, 80), 1),
            '90th': round(np.percentile(timings, 90), 1),
            '95th': round(np.percentile(timings, 95), 1),
            '99th': round(np.percentile(timings, 99), 1),
        }
        logger.info(pretty_dict(summary))

        if hasattr(self, 'experiment'):
            self.experiment.post_results(summary['95th'])

    def run(self):
        self.measure_latency()


class RevABTest(FragmentationTest):

    def generate_graph(self):
        random.seed(0)
        self.graph = generate_graph(self.test_config.load_settings.items)
        self.graph_keys = self.graph.nodes()
        random.shuffle(self.graph_keys)

    @with_stats
    def load(self):
        for target in self.target_iterator:
            host, port = target.node.split(':')
            conn = {'host': host, 'port': port, 'bucket': target.bucket}

            threads = list()
            for seqid in range(self.test_config.load_settings.workers):
                iterator = PersonIterator(
                    self.graph,
                    self.graph_keys,
                    seqid,
                    self.test_config.load_settings.workers,
                )
                t = Thread(
                    target=produce_AB,
                    args=(iterator,
                          self.test_config.load_settings.iterations,
                          conn),
                )
                threads.append(t)
                t.start()
            for t in threads:
                t.join()

    def run(self):
        self.generate_graph()
        self.load()
        if self.test_config.stats_settings.enabled:
            fragmentation_ratio = self.calc_fragmentation_ratio()
            self.reporter.post_to_sf(fragmentation_ratio)
            self.reporter.post_to_sf(
                *self.metric_helper.calc_max_memcached_rss()
            )


class MemUsedTest(KVTest):

    ALL_BUCKETS = True

    def run(self):
        super(MemUsedTest, self).run()
        if self.test_config.stats_settings.enabled:
            for metric in ('max', 'min'):
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_mem_used(metric)
                )
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_mem_used(metric)
                )


class PathoGenTest(FragmentationTest):
    """Pathologically bad malloc test. See pathoGen.py for full details."""

    @with_stats
    def access(self):
        for target in self.target_iterator:
            host, port = target.node.split(':')
            PathoGen(num_items=self.test_config.load_settings.items,
                     num_workers=self.test_config.load_settings.workers,
                     num_iterations=self.test_config.load_settings.iterations,
                     host=host, port=port, bucket=target.bucket).run()

    def run(self):
        self.access()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_avg_memcached_rss()
            )
            self.reporter.post_to_sf(
                *self.metric_helper.calc_max_memcached_rss()
            )


class PathoGenFrozenTest(PathoGenTest):
    """Pathologically bad mmalloc test, Frozen mode. See pathoGen.py for full details."""

    @with_stats
    def access(self):
        for target in self.target_iterator:
            host, port = target.node.split(':')
            PathoGen(num_items=self.test_config.load_settings.items,
                     num_workers=self.test_config.load_settings.workers,
                     num_iterations=self.test_config.load_settings.iterations,
                     frozen_mode=True,
                     host=host, port=port, bucket=target.bucket).run()


class PillowfightTest(PerfTest):
    """Uses pillowfight from libcouchbase to drive cluster."""

    @with_stats
    def access(self):
        settings = self.test_config.access_settings
        if self.test_config.test_case.use_workers:
            log_phase('access phase', settings)
            self.worker_manager.run_workload(settings,
                                             self.target_iterator,
                                             run_workload=run_pillowfight_via_celery)
            self.worker_manager.wait_for_workers()
        else:
            for target in self.target_iterator:
                host, port = target.node.split(':')
                Pillowfight(host=host, port=port, bucket=target.bucket,
                            password=self.test_config.bucket.password,
                            num_items=settings.items,
                            num_threads=settings.workers,
                            writes=settings.updates,
                            size=settings.size).run()

    def run(self):
        from_ts, to_ts = self.access()
        time_elapsed = (to_ts - from_ts) / 1000.0

        self.reporter.finish('Pillowfight', time_elapsed)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                self.metric_helper.calc_avg_ops()
            )

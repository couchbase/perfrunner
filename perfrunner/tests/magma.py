import copy
import json
import time
from typing import Callable

from decorator import decorator

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import (
    pillowfight_data_load_task,
    pillowfight_task,
    ycsb_data_load_task,
    ycsb_task,
)
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import RebalanceKVTest
from perfrunner.tests.tools import BackupTest
from perfrunner.tests.ycsb import YCSBThroughputTest


@decorator
def with_console_stats(method: Callable, *args, **kwargs):
    """Execute the decorated function to print disk amplification stats and kvstore stats."""
    helper = args[0]
    helper.reset_kv_stats()
    helper.save_stats()
    method(*args, **kwargs)
    helper.print_amplifications(doc_size=helper.test_config.access_settings.size)
    helper.print_kvstore_stats()


class MagmaBenchmarkTest(PerfTest):

    def __init__(self, *args):
        super().__init__(*args)

        self.settings = self.test_config.magma_benchmark_settings
        self.stats_file = "stats.json"

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tear_down()

        if exc_type == KeyboardInterrupt:
            logger.warn('The test was interrupted')
            return True

    def create_command(self, write_multiplier: int = 1):

        cmd = "ulimit -n 1000000;/opt/couchbase/bin/priv/magma_bench {DATA_DIR}/{ENGINE} " \
              "--kvstore {NUM_KVSTORES} --ndocs {NUM_DOCS} " \
              "--batch-size {WRITE_BATCHSIZE} --keylen {KEY_LEN} --vallen {DOC_SIZE} " \
              "--nwrites {NUM_WRITES} --nreads {NUM_READS} --nreaders {NUM_READERS} " \
              "--memquota {MEM_QUOTA} --fs-cache-size {FS_CACHE_SIZE} --active-stats " \
              "--engine {ENGINE} --engine-config {ENGINE_CONFIG} --stats {STATS_FILE}"\
            .format(NUM_KVSTORES=self.settings.num_kvstores, NUM_DOCS=self.settings.num_docs,
                    WRITE_BATCHSIZE=self.settings.write_batchsize, KEY_LEN=self.settings.key_len,
                    DOC_SIZE=self.settings.doc_size,
                    NUM_WRITES=(self.settings.num_writes * write_multiplier),
                    NUM_READS=self.settings.num_reads, NUM_READERS=self.settings.num_readers,
                    MEM_QUOTA=self.settings.memquota,
                    FS_CACHE_SIZE=self.settings.fs_cache_size, DATA_DIR=self.settings.data_dir,
                    ENGINE=self.settings.engine, ENGINE_CONFIG=self.settings.engine_config,
                    STATS_FILE=self.stats_file)
        return cmd

    def run_and_get_stats(self, cmd: str) -> dict:
        self.remote.run_magma_benchmark(cmd, self.stats_file)
        data = read_json(self.stats_file)
        logger.info("\nStats: {}".format(pretty_dict(data)))
        return data

    def create(self):
        cmd = self.create_command()
        cmd += " --benchmark writeSequential --clear-existing"
        stats = self.run_and_get_stats(cmd)
        return \
            stats["writer"]["Throughput"], \
            stats["WriteAmp"], stats["SpaceAmp"], \
            stats["writer"]["Latency"]["p99.99"]

    def read(self):
        cmd = self.create_command()
        cmd += " --benchmark readRandom"
        stats = self.run_and_get_stats(cmd)
        return \
            stats["reader"]["Throughput"], \
            stats["ReadIOAmp"], \
            stats["BytesPerRead"], \
            stats["reader"]["Latency"]["p99.99"]

    def update(self):
        cmd = self.create_command(write_multiplier=self.settings.write_multiplier)
        cmd += " --benchmark writeRandom"
        stats = self.run_and_get_stats(cmd)
        return \
            stats["writer"]["Throughput"], \
            stats["WriteAmp"], stats["SpaceAmp"], \
            stats["writer"]["Latency"]["p99.99"]

    def delete(self):
        cmd = self.create_command()
        cmd += " --benchmark deleteRandom"
        stats = self.run_and_get_stats(cmd)
        return \
            stats["writer"]["Throughput"], \
            stats["DiskUsed"], \
            stats["writer"]["Latency"]["p99.99"]

    def _report_kpi(self, create_metrics, read_metrics, write_metrics, delete_metrics):
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=create_metrics[0],
                                                  precision=0,
                                                  benchmark="Throughput, Write sequential")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=create_metrics[1],
                                                  precision=2,
                                                  benchmark="Write amp, Write sequential")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=create_metrics[2],
                                                  precision=2,
                                                  benchmark="Space amp, Write sequential")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=create_metrics[3],
                                                  precision=0,
                                                  benchmark="P9999, Write sequential")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=read_metrics[0],
                                                  precision=0,
                                                  benchmark="Throughput, Read random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=read_metrics[1],
                                                  precision=2,
                                                  benchmark="Read IO amp, Read random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=read_metrics[2],
                                                  precision=1,
                                                  benchmark="Bytes per read, Read random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=read_metrics[3],
                                                  precision=0,
                                                  benchmark="P9999, Read random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=write_metrics[0],
                                                  precision=0,
                                                  benchmark="Throughput, Write random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=write_metrics[1],
                                                  precision=2,
                                                  benchmark="Write amp, Write random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=write_metrics[2],
                                                  precision=2,
                                                  benchmark="Space amp, Write random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=write_metrics[3],
                                                  precision=0,
                                                  benchmark="P9999, Write random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=delete_metrics[0],
                                                  precision=0,
                                                  benchmark="Throughput, Delete random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=delete_metrics[1],
                                                  precision=0,
                                                  benchmark="DiskUsed, Delete random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=delete_metrics[2],
                                                  precision=0,
                                                  benchmark="P9999, Delete random")
        )

    def run(self):
        self.remote.stop_server()

        create_metrics = self.create()

        read_metrics = self.read()

        write_metrics = self.update()

        delete_metrics = self.delete()

        self.report_kpi(create_metrics, read_metrics, write_metrics, delete_metrics)


class KVTest(PerfTest):
    COLLECTORS = {'disk': True, 'latency': True, 'net': False, 'kvstore': True, 'vmstat': True}
    CB_STATS_PORT = 11209

    def __init__(self, *args):
        super().__init__(*args)
        local.extract_cb_any(filename='couchbase')
        self.collect_per_server_stats = self.test_config.magma_settings.collect_per_server_stats
        self.disk_stats = {}
        self.memcached_stats = {}
        self.disk_ops = {}

    def print_kvstore_stats(self):
        try:
            result = local.get_cbstats(self.master_node, self.CB_STATS_PORT, "kvstore",
                                       self.cluster_spec)
            buckets_data = list(filter(lambda a: a != "", result.split("*")))
            for data in buckets_data:
                data = data.strip()
                if data.startswith(self.test_config.buckets[0]):
                    data = data.split("\n", 1)[1]
                    data = data.replace("\"{", "{")
                    data = data.replace("}\"", "}")
                    data = data.replace("\\", "")
                    data = json.loads(data)
                    stats = {}
                    for key, value in data.items():
                        if key.startswith(("rw_0:", "rw_1:", "rw_2:", "rw_3:")):
                            stats[key] = value
                    logger.info("kvstore stats for first 4 shards: {}".format(pretty_dict(stats)))
                    break
        except Exception:
            pass

    def get_disk_stats(self):
        stats = {}
        for server in self.cluster_spec.servers_by_role("kv"):
            result = self.remote.get_disk_stats(server=server)
            device = self.remote.get_device(server=server)
            sector_size = int(self.remote.get_device_sector_size(server=server, device=device))
            logger.info(result)
            logger.info("Device:" + device)
            device = device.split("/")[-1]

            for device_data in result.split("\n"):
                if device == device_data.split()[2]:
                    values = device_data.split()
                    stats[server] = {}
                    stats[server]["nr"] = int(values[3])
                    stats[server]["nrb"] = int(values[5]) * sector_size
                    stats[server]["nw"] = int(values[7])
                    stats[server]["nwb"] = int(values[9]) * sector_size
                    break
            else:
                logger.info("Failed to get disk stats for {}".format(server))
        return stats

    def get_memcached_stats(self):
        stats = dict()
        for server in self.rest.get_active_nodes_by_role(self.master_node, "kv"):
            stats[server] = dict()
            temp_stats = dict()
            cmd_op = self.remote.get_memcached_io_stats(server=server)
            for line in cmd_op.split("\n"):
                values = line.split(":")
                temp_stats[values[0]] = int(values[1].strip())
            stats[server]["nw"] = temp_stats["syscw"]
            stats[server]["nwb"] = temp_stats["wchar"]
            stats[server]["nr"] = temp_stats["syscr"]
            stats[server]["nrb"] = temp_stats["rchar"]

        return stats

    def save_stats(self):
        self.disk_stats = self.get_disk_stats()
        self.disk_ops = self._measure_disk_ops()
        self.memcached_stats = self.get_memcached_stats()

    def _print_amplifications(self, old_stats, now_stats, now_ops, doc_size, stat_type):
        ampl_stats = dict()
        for server in self.rest.get_active_nodes_by_role(self.master_node, "kv"):
            if (server not in now_stats.keys()) or (server not in old_stats.keys()):
                logger.info("{} stats for {} not found!".format(stat_type, server))
                continue
            get_ops = now_ops[server]["get_ops"] - self.disk_ops[server]["get_ops"]
            set_ops = now_ops[server]["set_ops"] - self.disk_ops[server]["set_ops"]
            if set_ops:
                ampl_stats["write_amp"] = \
                    (now_stats[server]["nwb"] - old_stats[server]["nwb"]) / \
                    (set_ops * doc_size)
                ampl_stats["write_io_per_set"] = \
                    (now_stats[server]["nw"] - old_stats[server]["nw"]) / set_ops
                ampl_stats["read_bytes_per_set"] = \
                    (now_stats[server]["nrb"] - old_stats[server]["nrb"]) / set_ops
                ampl_stats["read_io_per_set"] = \
                    (now_stats[server]["nr"] - old_stats[server]["nr"]) / set_ops
            if get_ops:
                ampl_stats["read_amp"] = \
                    (now_stats[server]["nr"] - old_stats[server]["nr"]) / get_ops
                ampl_stats["read_bytes_per_get"] = \
                    (now_stats[server]["nrb"] - old_stats[server]["nrb"]) / get_ops

            logger.info("{} Amplification stats for {}: {}".format(stat_type, server,
                                                                   pretty_dict(ampl_stats)))
            logger.info("Note: read_bytes_per_set and read_io_per_set are "
                        "valid for set only workload.")

    def print_amplifications(self, doc_size: int):
        now_ops = self._measure_disk_ops()
        logger.info("Saved ops: {}\nCurrent ops: {}".
                    format(pretty_dict(self.disk_ops), pretty_dict(now_ops)))

        now_disk_stats = self.get_disk_stats()
        logger.info("Saved disk stats: {}\nCurrent disk stats: {}".
                    format(pretty_dict(self.disk_stats), pretty_dict(now_disk_stats)))

        self._print_amplifications(old_stats=self.disk_stats, now_stats=now_disk_stats,
                                   now_ops=now_ops, doc_size=doc_size, stat_type="Actual")

        now_memcached_ops = self.get_memcached_stats()
        logger.info("Saved memcached stats: {}\nCurrent memcached stats: {}".
                    format(pretty_dict(self.memcached_stats), pretty_dict(now_memcached_ops)))

        self._print_amplifications(old_stats=self.memcached_stats, now_stats=now_memcached_ops,
                                   now_ops=now_ops, doc_size=doc_size, stat_type="Virtual")

    def wait_for_fragmentation(self):
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.wait_for_fragmentation_stable(master, bucket)
        time.sleep(300)

    @with_console_stats
    @with_stats
    def access(self, *args):
        super().access(*args)

    @with_stats
    def extra_access(self, access_settings):
        logger.info("Starting first access phase")
        PerfTest.access(self, settings=access_settings)

    def run_extra_access(self):
        pass

    @with_stats
    def custom_load(self, *args):
        super().load(*args)

    @with_console_stats
    def load(self, *args):
        self.COLLECTORS["latency"] = False
        self.custom_load()
        self.wait_for_persistence()
        self.COLLECTORS["latency"] = True

    def run(self):
        self.load()

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        self.hot_load()
        self.reset_kv_stats()

        self.access()

        self.report_kpi()


class StabilityBootstrap(KVTest):
    @with_console_stats
    def run_extra_access(self):
        self.COLLECTORS["latency"] = False
        self.extra_access(access_settings=self.test_config.extra_access_settings)
        self.COLLECTORS["latency"] = True


class ReadLatencyDGMTest(StabilityBootstrap):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.kv_latency(operation='get')
        )


class CompactionMagmaTest(StabilityBootstrap):

    @with_stats
    @timeit
    def compact(self):
        self.compact_bucket()

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.elapsed_time(time_elapsed)
        )

    def run(self):
        self.load()

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        self.hot_load()
        self.reset_kv_stats()

        self.access_bg()

        time_elapsed = self.compact()

        self.report_kpi(time_elapsed)


class CompressionMagmaTest(CompactionMagmaTest):

    def _report_kpi(self):
        total_size = 0
        for bucket in self.test_config.buckets:
            bucket_stats = self.rest.get_bucket_stats(self.master_node, bucket)
            disk_size = bucket_stats['op']['samples'].get("couch_docs_actual_disk_size")[-1]
            total_size += disk_size

        self.reporter.post(
            *self.metrics.disk_size(total_size)
        )

    def run(self):
        self.load()

        self.reset_kv_stats()

        self.compact()

        self.report_kpi()


class ThroughputDGMMagmaTest(StabilityBootstrap):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_ops()
        )


class LoadThroughputDGMMagmaTest(ThroughputDGMMagmaTest):

    def run(self):
        self.load()
        self.report_kpi()


class SingleNodeThroughputDGMMagmaTest(ThroughputDGMMagmaTest):

    def restart(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def run(self):
        self.load()

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        self.COLLECTORS["kvstore"] = False
        self.COLLECTORS["disk"] = False
        self.COLLECTORS["latency"] = False
        self.COLLECTORS["vmstat"] = False
        self.restart()
        self.COLLECTORS["kvstore"] = True
        self.COLLECTORS["disk"] = True
        self.COLLECTORS["latency"] = True
        self.COLLECTORS["vmstat"] = True

        local.cbepctl(
            master_node=self.master_node,
            cluster_spec=self.cluster_spec,
            bucket=self.test_config.buckets[0],
            option="mem_low_wat",
            value=self.test_config.load_settings.mem_low_wat
        )

        local.cbepctl(
            master_node=self.master_node,
            cluster_spec=self.cluster_spec,
            bucket=self.test_config.buckets[0],
            option="mem_high_wat",
            value=self.test_config.load_settings.mem_high_wat
        )

        self.hot_load()
        self.reset_kv_stats()

        self.access()

        self.report_kpi()


class SingleNodeMixedLatencyDGMTest(SingleNodeThroughputDGMMagmaTest):

    def _report_kpi(self):
        for operation in ('get', 'set'):
            self.reporter.post(
                *self.metrics.kv_latency(operation=operation)
            )

    def run(self):
        self.load()

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        self.COLLECTORS["kvstore"] = False
        self.COLLECTORS["disk"] = False
        self.COLLECTORS["latency"] = False
        self.COLLECTORS["vmstat"] = False
        self.restart()
        self.COLLECTORS["kvstore"] = True
        self.COLLECTORS["disk"] = True
        self.COLLECTORS["latency"] = True
        self.COLLECTORS["vmstat"] = True

        self.hot_load()
        self.reset_kv_stats()

        self.access()

        self.report_kpi()


class MixedLatencyDGMTest(StabilityBootstrap):

    def _report_kpi(self):
        for operation in ('get', 'set'):
            self.reporter.post(
                *self.metrics.kv_latency(operation=operation)
            )


class WriteLatencyDGMTest(StabilityBootstrap):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.kv_latency(operation='set')
        )


class EnhancedDurabilityLatencyDGMTest(StabilityBootstrap):

    def _report_kpi(self):
        for percentile in 50.00, 99.9:
            self.reporter.post(
                *self.metrics.kv_latency(operation='set', percentile=percentile)
            )


class PillowFightDGMTest(StabilityBootstrap):

    """Use cbc-pillowfight from libcouchbase to drive cluster."""

    ALL_BUCKETS = True

    def load(self, *args):
        PerfTest.load(self, task=pillowfight_data_load_task)

    @with_stats
    def access(self, *args):
        self.download_certificate()

        PerfTest.access(self, task=pillowfight_task)

    @with_stats
    def run_extra_access(self):
        logger.info("Starting first access phase")
        PerfTest.access(self, settings=self.test_config.extra_access_settings,
                        task=pillowfight_task)

    def _report_kpi(self, *args):
        self.reporter.post(
            *self.metrics.max_ops()
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        if self.test_config.users.num_users_per_bucket > 0:
            self.cluster.add_extra_rbac_users(self.test_config.users.num_users_per_bucket)

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        self.reset_kv_stats()

        if self.test_config.access_settings.user_mod_workers:
            access_settings = copy.deepcopy(self.test_config.access_settings)
            access_settings.workers = 0
            access_settings.n1ql_workers = 0
            access_settings.query_workers = 0
            self.access_bg(settings=access_settings)

        self.access()

        self.report_kpi()


class WarmupDGMTest(StabilityBootstrap):

    @with_stats
    def warmup(self):
        self.remote.stop_server()
        self.remote.drop_caches()

        return self._warmup()

    @timeit
    def _warmup(self):
        self.remote.start_server()
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.elapsed_time(time_elapsed)
        )

    def run(self):
        self.load()

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        self.reset_kv_stats()

        self.access()
        self.wait_for_persistence()

        self.COLLECTORS["kvstore"] = False
        self.COLLECTORS["disk"] = False
        self.COLLECTORS["latency"] = False
        self.COLLECTORS["vmstat"] = False
        time_elapsed = self.warmup()

        self.report_kpi(time_elapsed)


class YCSBThroughputHIDDTest(YCSBThroughputTest, KVTest):

    COLLECTORS = {'disk': True, 'net': True, 'kvstore': True, 'vmstat': True}

    def __init__(self, *args):
        KVTest.__init__(self, *args)

    @with_stats
    def custom_load(self):
        KVTest.save_stats(self)
        YCSBThroughputTest.load(self)
        self.wait_for_persistence()
        self.check_num_items()
        self.print_amplifications(doc_size=self.test_config.access_settings.size)
        KVTest.print_kvstore_stats(self)

    @with_stats
    def run_extra_access(self):
        self.reset_kv_stats()
        KVTest.save_stats(self)
        logger.info("Starting first access phase")
        PerfTest.access(self, task=ycsb_task, settings=self.test_config.extra_access_settings)
        self.print_amplifications(doc_size=self.test_config.extra_access_settings.size)
        KVTest.print_kvstore_stats(self)

    def run(self):
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.custom_load()

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        self.reset_kv_stats()
        KVTest.save_stats(self)
        if self.test_config.access_settings.cbcollect:
            YCSBThroughputTest.access_bg(self)
            self.collect_cb()
        else:
            YCSBThroughputTest.access(self)

        self.print_amplifications(doc_size=self.test_config.access_settings.size)
        KVTest.print_kvstore_stats(self)

        self.report_kpi()


class YCSBLoadThroughputHIDDTest(YCSBThroughputHIDDTest):

    def _report_kpi(self):
        self.collect_export_files()

        self.reporter.post(
            *self.metrics.ycsb_throughput(operation="load")
        )

    def run(self):
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.custom_load()

        self.report_kpi()


class YCSBThroughputLatencyHIDDPhaseTest(YCSBThroughputHIDDTest):

    def _report_kpi(self, phase: int, workload: str, operation: str = "access"):
        self.collect_export_files()

        self.reporter.post(
            *self.metrics.ycsb_throughput_phase(phase, workload, operation)
        )

        for percentile in self.test_config.ycsb_settings.latency_percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile, operation=operation)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency_phase(key, latency_dic[key], phase, workload)
                    )

        if self.test_config.ycsb_settings.average_latency == 1:
            latency_dic = self.metrics.ycsb_get_latency(
                percentile=99, operation=operation)

            for key, value in latency_dic.items():
                if "Average" in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency_phase(key, latency_dic[key], phase, workload)
                    )

    @with_stats
    def custom_load(self, phase):
        KVTest.save_stats(self)
        loading_settings = self.test_config.load_settings
        loading_settings.insertstart = loading_settings.items * phase
        PerfTest.load(self, task=ycsb_data_load_task, settings=loading_settings)
        self.wait_for_persistence()
        self.print_amplifications(doc_size=self.test_config.access_settings.size)
        KVTest.print_kvstore_stats(self)

    @with_stats
    @with_profiles
    def access(self, settings):
        self.reset_kv_stats()
        KVTest.save_stats(self)
        PerfTest.access(self, task=ycsb_task, settings=settings)
        self.print_amplifications(doc_size=self.test_config.access_settings.size)
        KVTest.print_kvstore_stats(self)

    def run(self):
        self.download_ycsb()

        access_settings = self.test_config.access_settings

        for phase in range(self.test_config.load_settings.phase):

            self.custom_load(phase=phase)
            self.report_kpi(phase=(phase+1), workload="Load", operation="load")

            access_settings.workload_path = "workloads/workloadc"
            access_settings.items = self.test_config.load_settings.items * (phase + 1)
            logger.info("Starting Phase {} Workload C".format((phase + 1)))
            self.access(settings=access_settings)
            self.report_kpi(phase=(phase+1), workload="Workload C")

            access_settings.workload_path = "workloads/workloada"
            logger.info("Starting Phase {} Workload A".format((phase + 1)))
            self.access(settings=access_settings)
            self.report_kpi(phase=(phase+1), workload="Workload A")


class YCSBLatencyHiDDTest(YCSBThroughputHIDDTest):

    def _report_kpi(self):
        self.collect_export_files()

        for percentile in self.test_config.ycsb_settings.latency_percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency(key, latency_dic[key])
                    )

        if self.test_config.ycsb_settings.average_latency == 1:
            latency_dic = self.metrics.ycsb_get_latency(
                percentile=99)

            for key, value in latency_dic.items():
                if "Average" in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency(key, latency_dic[key])
                    )


class YCSBDurabilityThroughputHiDDTest(YCSBThroughputHIDDTest):

    def log_latency_percentiles(self, type: str, percentiles):
        for percentile in percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and type in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    logger.info("{}: {}".format(key, latency_dic[key]))

    def log_percentiles(self):
        logger.info("------------------")
        logger.info("Latency Percentiles")
        logger.info("-------READ-------")
        self.log_latency_percentiles("READ", [95, 96, 97, 98, 99])
        logger.info("------UPDATE------")
        self.log_latency_percentiles("UPDATE", [95, 96, 97, 98, 99])
        logger.info("------------------")

    def _report_kpi(self):
        self.collect_export_files()

        self.log_percentiles()

        self.reporter.post(
            *self.metrics.ycsb_durability_throughput()
        )

        for percentile in self.test_config.ycsb_settings.latency_percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_slo_latency(key, latency_dic[key])
                    )

        for key, value in self.metrics.ycsb_get_max_latency().items():
            self.reporter.post(
                *self.metrics.ycsb_slo_max_latency(key, value)
            )

        for key, value in self.metrics.ycsb_get_failed_ops().items():
            self.reporter.post(
                *self.metrics.ycsb_failed_ops(key, value)
            )

        self.reporter.post(
            *self.metrics.ycsb_gcs()
        )


class JavaDCPThroughputDGMTest(KVTest):

    def _report_kpi(self, time_elapsed: float, clients: int, stream: str):
        self.reporter.post(
            *self.metrics.dcp_throughput(time_elapsed, clients, stream)
        )

    def init_java_dcp_client(self):
        local.clone_git_repo(repo=self.test_config.java_dcp_settings.repo,
                             branch=self.test_config.java_dcp_settings.branch)
        local.build_java_dcp_client()

    @with_stats
    @timeit
    def access(self, *args):
        for target in self.target_iterator:
            local.run_java_dcp_client(
                connection_string=target.connection_string,
                messages=self.test_config.load_settings.items,
                config_file=self.test_config.java_dcp_settings.config,
            )

    def run(self):
        self.init_java_dcp_client()

        self.load()
        self.wait_for_persistence()

        time_elapsed = self.access()

        self.report_kpi(time_elapsed,
                        int(self.test_config.java_dcp_settings.clients),
                        self.test_config.java_dcp_settings.stream)


class RebalanceKVDGMTest(RebalanceKVTest, StabilityBootstrap):

    COLLECTORS = {'disk': True, 'latency': True, 'net': False, 'kvstore': True, 'vmstat': True}

    def __init__(self, *args):
        RebalanceKVTest.__init__(self, *args)

    def run(self):
        StabilityBootstrap.load(self)

        if self.test_config.extra_access_settings.run_extra_access:
            StabilityBootstrap.run_extra_access(self)
            self.wait_for_persistence()
            StabilityBootstrap.wait_for_fragmentation()

        StabilityBootstrap.hot_load(self)

        StabilityBootstrap.reset_kv_stats(self)

        self.access_bg()
        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class BackupTestDGM(BackupTest):

    @with_stats
    def run_extra_access(self):
        self.COLLECTORS["latency"] = False
        logger.info("Starting first access phase")
        PerfTest.access(self, settings=self.test_config.extra_access_settings)
        self.COLLECTORS["latency"] = True

    def wait_for_fragmentation(self):
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.wait_for_fragmentation_stable(master, bucket)
        time.sleep(300)

    def run(self):
        self.extract_tools()

        self.load()
        self.wait_for_persistence()

        if self.test_config.extra_access_settings.run_extra_access:
            self.run_extra_access()
            self.wait_for_persistence()
            self.wait_for_fragmentation()

        time_elapsed = self.backup()

        self.report_kpi(time_elapsed)

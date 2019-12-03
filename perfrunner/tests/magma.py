import json
from typing import Callable

from decorator import decorator

from logger import logger
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.local import extract_cb_deb, get_cbstats
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.tests import PerfTest
from perfrunner.tests.ycsb import YCSBThroughputTest


@decorator
def with_console_stats(method: Callable, *args, **kwargs):
    """Execute the decorated function to print disk amplification stats and kvstore stats."""
    helper = args[0]
    helper.reset_kv_stats()
    helper.save_disk_stats()
    method(*args, **kwargs)
    helper.print_amplifications(ops=helper._measure_disk_ops(),
                                doc_size=helper.test_config.access_settings.size)
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

        cmd = "ulimit -n 1000000;/opt/couchbase/bin/magma_bench {DATA_DIR}/{ENGINE} " \
              "--kvstore {NUM_KVSTORES} --ndocs {NUM_DOCS} " \
              "--batch-size {WRITE_BATCHSIZE} --keylen {KEY_LEN} --vallen {DOC_SIZE} " \
              "--nwrites {NUM_WRITES} --nreads {NUM_READS} --nreaders {NUM_READERS} " \
              "--memquota {MEM_QUOTA} --fs-cache-size {FS_CACHE_SIZE} " \
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
        return stats["writer"]["Throughput"], stats["WriteAmp"], stats["SpaceAmp"]

    def read(self):
        cmd = self.create_command()
        cmd += " --benchmark readRandom"
        stats = self.run_and_get_stats(cmd)
        return stats["reader"]["Throughput"], stats["ReadIOAmp"], stats["BytesPerRead"]

    def update(self):
        cmd = self.create_command(write_multiplier=self.settings.write_multiplier)
        cmd += " --benchmark writeRandom"
        stats = self.run_and_get_stats(cmd)
        return stats["writer"]["Throughput"], stats["WriteAmp"], stats["SpaceAmp"]

    def _report_kpi(self, create_metrics, read_metrics, write_metrics):
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=create_metrics[0],
                                                  precision=0,
                                                  benchmark="Throughput, Write sequential")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=create_metrics[1],
                                                  precision=2,
                                                  benchmark="Write amplification, "
                                                            "Write sequential")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=create_metrics[2],
                                                  precision=2,
                                                  benchmark="Space amplification, "
                                                            "Write sequential")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=read_metrics[0],
                                                  precision=0,
                                                  benchmark="Throughput, Read random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=read_metrics[1],
                                                  precision=2,
                                                  benchmark="Read IO amplification, Read random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=read_metrics[2],
                                                  precision=1,
                                                  benchmark="Bytes per read, Read random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=write_metrics[0],
                                                  precision=0,
                                                  benchmark="Throughput, Write random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=write_metrics[1],
                                                  precision=2,
                                                  benchmark="Write amplification, Write random")
        )
        self.reporter.post(
            *self.metrics.magma_benchmark_metrics(throughput=write_metrics[2],
                                                  precision=2,
                                                  benchmark="Space amplification, Write random")
        )

    def run(self):
        self.remote.stop_server()

        create_metrics = self.create()

        read_metrics = self.read()

        write_metrics = self.update()

        self.report_kpi(create_metrics, read_metrics, write_metrics)


class KVTest(PerfTest):
    COLLECTORS = {'disk': True, 'latency': True, 'net': False, 'kvstore': True}
    CB_STATS_PORT = 11209

    def __init__(self, *args):
        super().__init__(*args)
        extract_cb_deb(filename='couchbase.deb')
        self.collect_per_server_stats = self.test_config.magma_settings.collect_per_server_stats
        self.disk_stats = {}

    def print_kvstore_stats(self):
        try:
            result = get_cbstats(self.master_node, self.CB_STATS_PORT, "kvstore", self.cluster_spec)
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

    def save_disk_stats(self):
        self.disk_stats = self.get_disk_stats()

    def print_amplifications(self, ops: dict, doc_size: int):
        now_stats = self.get_disk_stats()
        logger.info("Ops: {}".format(pretty_dict(ops)))
        logger.info("Saved stats: {}\nCurrent stats: {}".
                    format(pretty_dict(self.disk_stats), pretty_dict(now_stats)))
        if not bool(self.disk_stats) or not bool(now_stats):
            logger.info("Either saved stats or current stats not present!")
            return
        ampl_stats = dict()
        for server in self.rest.get_active_nodes_by_role(self.master_node, "kv"):
            if (server not in now_stats.keys()) or (server not in self.disk_stats.keys()):
                logger.info("Stats for {} not found!".format(server))
                continue
            get_ops, set_ops = ops[server]["get_ops"], ops[server]["set_ops"]
            if set_ops:
                ampl_stats["write_amp"] = \
                    (now_stats[server]["nwb"] - self.disk_stats[server]["nwb"]) / \
                    (set_ops * doc_size)
                ampl_stats["write_io_per_set"] = \
                    (now_stats[server]["nw"] - self.disk_stats[server]["nw"]) / set_ops
                ampl_stats["read_bytes_per_set"] = \
                    (now_stats[server]["nrb"] - self.disk_stats[server]["nrb"]) / set_ops
                ampl_stats["read_io_per_set"] = \
                    (now_stats[server]["nr"] - self.disk_stats[server]["nr"]) / set_ops
            if get_ops:
                ampl_stats["read_amp"] = \
                    (now_stats[server]["nr"] - self.disk_stats[server]["nr"]) / get_ops
                ampl_stats["read_bytes_per_get"] = \
                    (now_stats[server]["nrb"] - self.disk_stats[server]["nrb"]) / get_ops

            logger.info("Amplification stats for {}: {}".format(server, pretty_dict(ampl_stats)))
            logger.info("Note: read_bytes_per_set and read_io_per_set are "
                        "valid for set only workload.")
        self.disk_stats = {}

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

        self.run_extra_access()

        self.hot_load()
        self.reset_kv_stats()

        self.access()

        self.report_kpi()


class S0Test(KVTest):
    @with_console_stats
    def run_extra_access(self):
        access_settings = self.test_config.access_settings
        access_settings.updates = 100
        access_settings.creates = 0
        access_settings.deletes = 0
        access_settings.reads = 0
        access_settings.ops = int(access_settings.items * 1.5)
        access_settings.time = 3600 * 24
        access_settings.throughput = float('inf')
        self.COLLECTORS["latency"] = False
        self.extra_access(access_settings=access_settings)
        self.COLLECTORS["latency"] = True


class ReadLatencyDGMTest(S0Test):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.kv_latency(operation='get')
        )


class ThroughputDGMMagmaTest(S0Test):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_ops()
        )


class MixedLatencyDGMTest(S0Test):

    def _report_kpi(self):
        for operation in ('get', 'set'):
            self.reporter.post(
                *self.metrics.kv_latency(operation=operation)
            )


class WriteLatencyDGMTest(S0Test):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.kv_latency(operation='set')
        )


class ReadLatencyS1DGMTest(ReadLatencyDGMTest):
    @with_console_stats
    def run_extra_access(self):
        access_settings = self.test_config.access_settings
        access_settings.updates = 100
        access_settings.creates = 0
        access_settings.deletes = 0
        access_settings.reads = 0
        access_settings.workers = 100
        access_settings.ops = access_settings.items
        access_settings.time = 3600 * 24
        access_settings.throughput = float('inf')
        self.COLLECTORS["latency"] = False
        self.extra_access(access_settings=access_settings)
        self.COLLECTORS["latency"] = True


class YCSBThroughputHIDDTest(YCSBThroughputTest, KVTest):

    COLLECTORS = {'disk': True, 'net': True, 'kvstore': True}

    def __init__(self, *args):
        KVTest.__init__(self, *args)

    @with_stats
    def custom_load(self):
        KVTest.save_disk_stats(self)
        YCSBThroughputTest.load(self)
        self.wait_for_persistence()
        self.check_num_items()
        self.print_amplifications(ops=self._measure_disk_ops(),
                                  doc_size=self.test_config.access_settings.size)
        KVTest.print_kvstore_stats(self)

    def run(self):
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.custom_load()

        self.reset_kv_stats()
        KVTest.save_disk_stats(self)
        if self.test_config.access_settings.cbcollect:
            YCSBThroughputTest.access_bg(self)
            self.collect_cb()
        else:
            YCSBThroughputTest.access(self)

        self.print_amplifications(ops=self._measure_disk_ops(),
                                  doc_size=self.test_config.access_settings.size)
        KVTest.print_kvstore_stats(self)

        self.report_kpi()

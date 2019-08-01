import csv
import json
import subprocess
import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.local import (
    get_indexer_heap_profile,
    kill_process,
    run_cbindexperf,
)
from perfrunner.tests import PerfTest, TargetIterator
from perfrunner.tests.rebalance import RebalanceTest
from spring.docgen import decimal_fmtr


class SecondaryIndexTest(PerfTest):

    """Measure time it takes to build secondary index.

    This is just a base class, actual measurements happen in initial and
    incremental secondary indexing tests. It benchmarks dumb/bulk indexing.
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_debugstats': True,
                  'secondary_debugstats_bucket': True, 'secondary_debugstats_index': True}

    SECONDARY_STATS_FILE = '/root/statsfile'

    def __init__(self, *args):
        super().__init__(*args)

        self.configfile = self.test_config.gsi_settings.cbindexperf_configfile
        self.configfiles = self.test_config.gsi_settings.cbindexperf_configfiles.split(",")
        self.run_recovery_test = self.test_config.gsi_settings.run_recovery_test
        self.incremental_only = self.test_config.gsi_settings.incremental_only
        self.incremental_load_iterations = self.test_config.gsi_settings.incremental_load_iterations
        self.scan_time = self.test_config.gsi_settings.scan_time
        self.report_initial_build_time = self.test_config.gsi_settings.report_initial_build_time

        self.storage = self.test_config.gsi_settings.storage
        self.indexes = self.test_config.gsi_settings.indexes

        self.bucket = self.test_config.buckets[0]
        self.target_iterator = TargetIterator(self.cluster_spec, self.test_config, "gsi")

        if self.storage == "plasma":
            self.COLLECTORS["secondary_storage_stats"] = True
            self.COLLECTORS["secondary_storage_stats_mm"] = True

    def remove_statsfile(self):
        rmfile = "rm -f {}".format(self.SECONDARY_STATS_FILE)
        status = subprocess.call(rmfile, shell=True)
        if status != 0:
            raise Exception('existing 2i latency stats file could not be removed')
        else:
            logger.info('Existing 2i latency stats file removed')

    @with_stats
    def build_secondaryindex(self):
        return self._build_secondaryindex()

    def _build_secondaryindex(self):
        """Call cbindex create command."""
        logger.info('building secondary index..')

        self.remote.build_secondary_index(self.index_nodes, self.bucket,
                                          self.indexes, self.storage)
        time_elapsed = self.monitor.wait_for_secindex_init_build(
            self.index_nodes[0],
            list(self.indexes.keys()),
        )
        return time_elapsed

    @staticmethod
    def get_data_from_config_json(config_file_name):
        with open(config_file_name) as fh:
            return json.load(fh)

    def validate_num_connections(self):
        config_data = self.get_data_from_config_json(self.configfile)
        # Expecting connections = Number of GSi clients * concurrency in config file
        ret = self.metrics.verify_series_in_limits(config_data["Concurrency"] *
                                                   config_data["Clients"])
        if not ret:
            raise Exception('Validation for num_connections failed')

    @with_stats
    def apply_scanworkload(self, path_to_tool="/opt/couchbase/bin/cbindexperf"):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        with open(self.configfile, 'r') as fp:
            config_file_content = fp.read()
        logger.info("cbindexperf config file: \n" + config_file_content)
        status = run_cbindexperf(path_to_tool, self.index_nodes[0],
                                 rest_username, rest_password, self.configfile)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')

    def print_index_disk_usage(self, text=""):
        if text:
            logger.info("{}".format(text))

        disk_usage = self.remote.get_disk_usage(self.index_nodes[0],
                                                self.cluster_spec.index_path)
        logger.info("Disk usage:\n{}".format(disk_usage))

        storage_stats = self.rest.get_index_storage_stats(self.index_nodes[0])
        logger.info("Index storage stats:\n{}".format(storage_stats))

        heap_profile = get_indexer_heap_profile(self.index_nodes[0],
                                                self.rest.rest_username,
                                                self.rest.rest_password)
        logger.info("Indexer heap profile:\n{}".format(heap_profile))

        if self.storage == 'plasma':
            stats = self.rest.get_index_storage_stats_mm(self.index_nodes[0])
            logger.info("Index storage stats mm:\n{}".format(stats))

        return self.remote.get_disk_usage(self.index_nodes[0],
                                          self.cluster_spec.index_path,
                                          human_readable=False)

    def change_scan_range(self, iteration):
        working_set = self.test_config.access_settings.working_set / 100
        num_hot_items = int(self.test_config.access_settings.items * working_set)

        with open(self.configfile, "r") as jsonFile:
            data = json.load(jsonFile)

        if iteration == 0:
            old_start = num_hot_items
        else:
            old_start = data["ScanSpecs"][0]["Low"][0].split("-")
            old_start = old_start[0] if len(old_start) == 1 else old_start[1]

        # predict next hot_load_start
        start = (int(old_start) + self.test_config.access_settings.working_set_moving_docs) % \
                (self.test_config.access_settings.items - num_hot_items)
        end = start + num_hot_items

        data["ScanSpecs"][0]["Low"][0] = decimal_fmtr(start, prefix='')
        data["ScanSpecs"][0]["High"][0] = decimal_fmtr(end, prefix='')

        with open(self.configfile, "w") as jsonFile:
            jsonFile.write(json.dumps(data))

    def read_scanresults(self):
        with open('{}'.format(self.configfile)) as config_file:
            configdata = json.load(config_file)
        numscans = 0
        for scanspec in configdata['ScanSpecs']:
            numscans += scanspec['Repeat']

        with open('result.json') as result_file:
            resdata = json.load(result_file)
        duration_s = (resdata['Duration'])
        num_rows = resdata['Rows']
        """scans and rows per sec"""
        scansps = numscans / duration_s
        rowps = num_rows / duration_s
        return scansps, rowps


class InitialandIncrementalSecondaryIndexTest(SecondaryIndexTest):

    """Measures time it takes to build index (both initial and incremental).

    There is no disabling of index updates in incremental building, index
    updating is conurrent to KV incremental load.
    """

    def _report_kpi(self, time_elapsed, index_type, unit="min"):
        self.reporter.post(
            *self.metrics.get_indexing_meta(value=time_elapsed,
                                            index_type=index_type,
                                            unit=unit)
        )

    def build_initindex(self):
        self.build_secondaryindex()

    @with_stats
    @timeit
    def build_incrindex(self):
        self.access()

        numitems = self.test_config.load_settings.items + \
            self.test_config.access_settings.items
        self.monitor.wait_for_secindex_incr_build(self.index_nodes,
                                                  self.bucket,
                                                  list(self.indexes.keys()),
                                                  numitems)

    def run_recovery_scenario(self):
        if self.run_recovery_test:
            # Measure recovery time for index
            self.remote.kill_process_on_index_node("indexer")
            recovery_time = self.monitor.wait_for_recovery(self.index_nodes,
                                                           self.bucket,
                                                           list(self.indexes.keys())[0])
            if recovery_time == -1:
                raise Exception('Indexer failed to recover...!!!')
            # Convert recovery time to second
            recovery_time /= 1000
            self.report_kpi(recovery_time, 'Recovery')

    def load_and_build_initial_index(self):
        self.load()
        self.wait_for_persistence()

        time_elapsed = self.build_secondaryindex()
        self.print_index_disk_usage()
        if not self.incremental_only:
            self.report_kpi(time_elapsed, 'Initial')

    def run(self):
        self.load_and_build_initial_index()

        time_elapsed = self.build_incrindex()
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Incremental')

        self.run_recovery_scenario()


class InitialandIncrementalDGMSecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):

    """Run InitialandIncrementalSecondaryIndexTest in DGM mode."""

    def run(self):
        self.load_and_build_initial_index()

        self.build_incrindex()
        self.print_index_disk_usage()

        time_elapsed = self.build_incrindex()
        self.print_index_disk_usage()

        self.report_kpi(time_elapsed, 'Incremental')
        self.run_recovery_scenario()


class InitialandSingleIncrementalDGMSecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):

    """Run InitialandIncrementalSecondaryIndexTest in DGM mode."""

    @with_stats
    def after_test(self):
        time.sleep(900)

    def run(self):
        self.load_and_build_initial_index()

        time_elapsed = self.build_incrindex()
        self.print_index_disk_usage()

        self.after_test()
        self.print_index_disk_usage()

        self.report_kpi(time_elapsed, 'Incremental')
        self.run_recovery_scenario()


class MultipleIncrementalSecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):

    def __init__(self, *args):
        super().__init__(*args)
        self.memory_usage = dict()
        self.disk_usage = dict()

    def _report_kpi(self, usage_diff, memory_type, unit="GB"):
        usage_diff = float(usage_diff) / 2 ** 30
        usage_diff = round(usage_diff, 2)

        self.reporter.post(
            *self.metrics.get_memory_meta(value=usage_diff,
                                          memory_type=memory_type)
        )

    @with_stats
    def build_incrindex_multiple_times(self, num_times):
        numitems = self.test_config.load_settings.items + \
            self.test_config.access_settings.items

        for i in range(1, num_times + 1):
            self.access()

            self.monitor.wait_for_secindex_incr_build(self.index_nodes,
                                                      self.bucket,
                                                      list(self.indexes.keys()),
                                                      numitems)
            self.disk_usage[i] = \
                self.print_index_disk_usage(
                    text="After running incremental load for {} iteration/s =>\n".format(i))
            self.memory_usage[i] = self.remote.get_indexer_rss(self.index_nodes[0])
            time.sleep(30)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self._build_secondaryindex()

        self.build_incrindex_multiple_times(self.incremental_load_iterations)

        # report memory usage diff
        self.report_kpi(self.memory_usage[6] - self.memory_usage[4], 'Memory usage difference')

        # report disk usage diff
        self.report_kpi(self.disk_usage[6] - self.disk_usage[4], 'Disk usage difference')

        self.run_recovery_scenario()


class InitialandIncrementalSecondaryIndexRebalanceTest(InitialandIncrementalSecondaryIndexTest):

    def rebalance(self, initial_nodes, nodes_after):
        for _, servers in self.cluster_spec.clusters:
            master = servers[0]
            ejected_nodes = []
            new_nodes = enumerate(
                servers[initial_nodes:nodes_after],
                start=initial_nodes
            )
            known_nodes = servers[:nodes_after]
            for i, node in new_nodes:
                self.rest.add_node(master, node)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

    def run(self):
        self.load()
        self.wait_for_persistence()

        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.rebalance(initial_nodes[0], nodes_after[0])
        time_elapsed = self.build_secondaryindex()
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Initial')

        for master in self.cluster_spec.masters:
            self.monitor.monitor_rebalance(master)
        initial_nodes[0] += 1
        nodes_after[0] += 1
        self.rebalance(initial_nodes[0], nodes_after[0])
        time_elapsed = self.build_incrindex()
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Incremental')


class InitialSecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):

    """Measures time it takes to build index for the first time."""

    def run(self):
        self.load_and_build_initial_index()
        self.print_index_disk_usage()

        # Adding sleep to get log cleaner catch up
        time.sleep(600)
        self.run_recovery_scenario()


class SecondaryIndexingScanTest(SecondaryIndexTest):

    """Apply moving scan workload and measure scan latency and average scan throughput."""

    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def __init__(self, *args):
        super().__init__(*args)
        self.scan_thr = []

    def _report_kpi(self,
                    scan_thr: float = 0,
                    time_elapsed: float = 0):

        if time_elapsed != 0:
            if self.report_initial_build_time:
                title = str(self.test_config.showfast.title).split(",", 1)[1].strip()
                self.reporter.post(
                    *self.metrics.get_indexing_meta(value=time_elapsed,
                                                    index_type="Initial",
                                                    unit="min",
                                                    name=title)
                )
        else:
            self.reporter.post(
                *self.metrics.scan_throughput(scan_thr, metric_id_append_str="thr")
            )
            title = str(self.test_config.showfast.title).split(",", 1)[1].strip()
            self.reporter.post(
                *self.metrics.secondary_scan_latency(percentile=90, title=title)
            )
            self.reporter.post(
                *self.metrics.secondary_scan_latency(percentile=95, title=title)
            )

    def run(self):
        self.load()
        self.wait_for_persistence()

        initial_index_time = self.build_secondaryindex()
        self.report_kpi(0, initial_index_time)
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(scan_thr, 0)
        self.validate_num_connections()


class SecondaryIndexingThroughputTest(SecondaryIndexTest):

    """Apply scan workload and measure the average scan throughput."""

    def _report_kpi(self, scan_thr):
        self.reporter.post(
            *self.metrics.scan_throughput(scan_thr)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(scan_thr)
        self.validate_num_connections()


class SecondaryIndexingThroughputRebalanceTest(SecondaryIndexingThroughputTest):

    """Measure the average scan throughput during rebalance."""

    def rebalance(self, initial_nodes, nodes_after):
        for _, servers in self.cluster_spec.clusters:
            master = servers[0]
            ejected_nodes = []
            new_nodes = enumerate(
                servers[initial_nodes:nodes_after],
                start=initial_nodes
            )
            known_nodes = servers[:nodes_after]
            for i, node in new_nodes:
                self.rest.add_node(master, node)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()
        self.access_bg()
        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.rebalance(initial_nodes[0], nodes_after[0])
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(scan_thr)
        self.validate_num_connections()


class InitialIncrementalScanThroughputTest(InitialandIncrementalDGMSecondaryIndexTest):

    def __init__(self, *args):
        super().__init__(*args)
        self.run_recovery_test_local = self.run_recovery_test
        self.run_recovery_test = 0

    def _report_kpi(self, *args):
        if len(args) == 1:
            self.report_throughput_kpi(*args)

    def report_throughput_kpi(self, scan_thr):
        self.reporter.post(
            *self.metrics.scan_throughput(scan_thr)
        )

    def run(self):
        self.remove_statsfile()
        super().run()
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(scan_thr)
        self.validate_num_connections()

        self.run_recovery_test = self.run_recovery_test_local
        self.run_recovery_scenario()


class InitialIncrementalMovingScanThroughputTest(InitialIncrementalScanThroughputTest):

    """Apply moving scan workload and measure the scan throughput."""

    def __init__(self, *args):
        super().__init__(*args)
        self.scan_thr = []

    def get_config(self):
        with open('{}'.format(self.configfile)) as config_file:
            config_data = json.load(config_file)
        return config_data['ScanSpecs'][0]['NInterval'], config_data['Concurrency']

    def get_throughput(self) -> float:
        """Parse statsfile created by cbindexperf and calculate the throughput.

        interval - number of scans after which entry made in statsfile
        concurrency - number of concurrent routines making scans

        Format for statsfile is-
        id:1, rows:0, duration:8632734534, Nth-latency:16686556
        id:1, rows:0, duration:3403693509, Nth-latency:6859285
        """
        duration = 0
        lines = 0
        with open(self.SECONDARY_STATS_FILE, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',')
            for row in csv_reader:
                duration += int(row[2].split(":")[1])
                lines += 1
        interval, concurrency = self.get_config()
        return lines * interval / (duration / 1000000000 / concurrency)

    def calc_throughput(self) -> float:
        """Calculate average throughput from list of throughput's."""
        logger.info('Throughputs collected over hot workloads: {}'.format(self.scan_thr))
        return sum(self.scan_thr) / len(self.scan_thr)

    @with_stats
    def apply_scanworkload(self, path_to_tool="/opt/couchbase/bin/cbindexperf"):
        """Apply moving scan workload and collect throughput for each load."""
        rest_username, rest_password = self.cluster_spec.rest_credentials

        t = 0
        while t < self.scan_time:
            self.change_scan_range(t)
            run_cbindexperf(path_to_tool, self.index_nodes[0], rest_username,
                            rest_password, self.configfile, run_in_background=True)
            time.sleep(self.test_config.access_settings.working_set_move_time)
            kill_process("cbindexperf")
            self.scan_thr.append(self.get_throughput())
            t += self.test_config.access_settings.working_set_move_time

    def run(self):
        self.remove_statsfile()
        InitialandIncrementalDGMSecondaryIndexTest.run(self)
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        scan_throughput = self.calc_throughput()
        self.print_index_disk_usage()
        self.report_kpi(scan_throughput)
        self.validate_num_connections()

        self.run_recovery_test = self.run_recovery_test_local
        self.run_recovery_scenario()


class SecondaryIndexingScanLatencyTest(SecondaryIndexTest):

    """Apply scan workload and measure the scan latency."""

    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def _report_kpi(self, *args):
        self.reporter.post(
            *self.metrics.secondary_scan_latency(percentile=90)
        )
        self.reporter.post(
            *self.metrics.secondary_scan_latency(percentile=95)
        )

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        self.print_index_disk_usage()
        self.report_kpi()
        self.validate_num_connections()


class SecondaryIndexingScanLatencyLongevityTest(SecondaryIndexingScanLatencyTest):

    @with_stats
    def apply_scanworkload(self, path_to_tool="/opt/couchbase/bin/cbindexperf"):
        rest_username, rest_password = self.cluster_spec.rest_credentials

        t = 0
        while t < self.scan_time:
            run_cbindexperf(path_to_tool, self.index_nodes[0], rest_username,
                            rest_password, self.configfile, run_in_background=True)
            time.sleep(3600)
            kill_process("cbindexperf")
            t += 3600

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        self.print_index_disk_usage()


class SecondaryIndexingScanLatencyRebalanceTest(SecondaryIndexingScanLatencyTest):

    """Apply scan workload and measure the scan latency during rebalance."""

    def rebalance(self, initial_nodes, nodes_after):
        for _, servers in self.cluster_spec.clusters:
            master = servers[0]
            ejected_nodes = []
            new_nodes = enumerate(
                servers[initial_nodes:nodes_after],
                start=initial_nodes
            )
            known_nodes = servers[:nodes_after]
            for i, node in new_nodes:
                self.rest.add_node(master, node)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.build_secondaryindex()
        self.access_bg()
        self.rebalance(initial_nodes[0], nodes_after[0])
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        self.print_index_disk_usage()
        self.report_kpi()
        self.validate_num_connections()


class InitialIncrementalScanLatencyTest(InitialandIncrementalDGMSecondaryIndexTest):

    """Apply scan workload and measure the scan latency.

    The test perform initial index build, then incremental index build and then
    applies scan workload against the 2i server and measures scan latency for
    moving workload.
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def __init__(self, *args):
        super().__init__(*args)
        self.run_recovery_test_local = self.run_recovery_test
        self.run_recovery_test = 0

    def _report_kpi(self, *args):
        if len(args):
            super()._report_kpi(*args)
        else:
            self.report_latency_kpi()

    def report_latency_kpi(self):
        self.reporter.post(
            *self.metrics.secondary_scan_latency(percentile=90)
        )
        self.reporter.post(
            *self.metrics.secondary_scan_latency(percentile=95)
        )

    def run(self):
        self.remove_statsfile()
        super().run()
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        self.print_index_disk_usage()
        self.report_kpi()
        self.validate_num_connections()

        self.run_recovery_test = self.run_recovery_test_local
        self.run_recovery_scenario()


class InitialIncrementalScanTest(InitialandIncrementalDGMSecondaryIndexTest):

    """Apply scan workload and measure the scan latency.

    The test perform initial index build, then incremental index build and then
    applies scan workload against the 2i server and measures scan latency for
    moving workload.
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def __init__(self, *args):
        super().__init__(*args)
        self.run_recovery_test_local = self.run_recovery_test
        self.run_recovery_test = 0

    def _report_kpi(self, *args):
        if len(args) == 1:
            self.report_scan_kpi(*args)
        else:
            super()._report_kpi(*args)

    def report_throughput_kpi(self, scan_thr):
        self.reporter.post(
            *self.metrics.scan_throughput(scan_thr)
        )

    def report_scan_kpi(self, scan_thr):
        self.reporter.post(
            *self.metrics.secondary_scan_latency(percentile=90)
        )
        self.reporter.post(
            *self.metrics.secondary_scan_latency(percentile=95)
        )
        self.reporter.post(
            *self.metrics.scan_throughput(scan_thr)
        )

    def run(self):
        self.remove_statsfile()
        super().run()
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.report_kpi(scan_thr)
        self.validate_num_connections()

        self.run_recovery_test = self.run_recovery_test_local
        self.run_recovery_scenario()


class InitialIncrementalMovingScanLatencyTest(InitialIncrementalScanLatencyTest):

    @with_stats
    def apply_scanworkload(self, path_to_tool="/opt/couchbase/bin/cbindexperf"):
        rest_username, rest_password = self.cluster_spec.rest_credentials

        t = 0
        while t < self.scan_time:
            self.change_scan_range(t)
            run_cbindexperf(path_to_tool, self.index_nodes[0], rest_username,
                            rest_password, self.configfile, run_in_background=True)
            time.sleep(self.test_config.access_settings.working_set_move_time)
            kill_process("cbindexperf")
            t += self.test_config.access_settings.working_set_move_time


class SecondaryIndexingDocIndexingLatencyTest(SecondaryIndexingScanLatencyTest):

    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True, "secondary_index_latency": True}

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.observe_latency(percentile=90)
        )

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        self._build_secondaryindex()
        self.access_bg()
        self.apply_scanworkload()
        self.report_kpi()


class SecondaryIndexingMultiScanTest(SecondaryIndexingScanLatencyTest):

    COLLECTORS = {'secondary_stats': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def _report_kpi(self, multifilter_time, independent_time):
        time_diff = independent_time - multifilter_time
        self.reporter.post(
            *self.metrics.multi_scan_diff(time_diff)
        )

    @staticmethod
    def read_duration_from_results():
        with open('result.json') as result_file:
            resdata = json.load(result_file)
        return resdata['Duration']

    def apply_scanworkload_configfile(self, configfile):
        self.configfile = configfile
        return self.apply_scanworkload(path_to_tool="./cbindexperf")

    def apply_scanworkloads(self):
        total_time = 0
        for configfile in self.configfiles:
            self.apply_scanworkload_configfile(configfile)
            diff = self.read_duration_from_results()
            logger.info("Config File: {} - Time taken: {}".format(configfile, diff))
            total_time += diff
        return total_time

    def apply_scan_multifilter_workload(self):
        self.apply_scanworkload_configfile(self.configfile)
        return self.read_duration_from_results()

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()

        self.access_bg()

        multifilter_time = self.apply_scan_multifilter_workload()
        logger.info("Multifilter time taken: {}".format(multifilter_time))

        independent_time = self.apply_scanworkloads()
        logger.info("Independent filters time taken: {}".format(independent_time))

        self.report_kpi(multifilter_time, independent_time)


class SecondaryRebalanceTest(SecondaryIndexTest, RebalanceTest):

    """Measure swap rebalance time for indexer."""

    COLLECTORS = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings

    def run(self):
        self.load()
        self.wait_for_persistence()

        self._build_secondaryindex()
        self.access_bg()
        self.rebalance_indexer()
        self.report_kpi()

    def rebalance_indexer(self):
        self.rebalance(services="index")

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.rebalance_time(self.rebalance_time)
        )

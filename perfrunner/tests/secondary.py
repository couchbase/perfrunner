import csv
import json
import subprocess
import time

import numpy as np
from logger import logger

from cbagent.stores import SerieslyStore
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.local import kill_process, run_cbindexperf
from perfrunner.tests import PerfTest, TargetIterator
from perfrunner.tests.rebalance import RebalanceTest


class SecondaryIndexTest(PerfTest):

    """
    The test measures time it takes to build secondary index. This is just a
    base class, actual measurements happen in initial and incremental secondary
    indexing tests. It benchmarks dumb/bulk indexing.
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_debugstats': True,
                  'secondary_debugstats_bucket': True, 'secondary_debugstats_index': True}

    def __init__(self, *args):
        super().__init__(*args)

        self.configfile = self.test_config.gsi_settings.cbindexperf_configfile
        self.configfiles = self.test_config.gsi_settings.cbindexperf_configfiles.split(",")
        self.init_num_connections = self.test_config.gsi_settings.init_num_connections
        self.step_num_connections = self.test_config.gsi_settings.step_num_connections
        self.max_num_connections = self.test_config.gsi_settings.max_num_connections
        self.run_recovery_test = self.test_config.gsi_settings.run_recovery_test
        self.incremental_only = self.test_config.gsi_settings.incremental_only
        self.block_memory = self.test_config.gsi_settings.block_memory
        self.incremental_load_iterations = self.test_config.gsi_settings.incremental_load_iterations
        self.scan_time = self.test_config.gsi_settings.scan_time

        self.storage = self.test_config.gsi_settings.storage
        self.indexes = self.test_config.gsi_settings.indexes

        self.secondary_statsfile = self.test_config.stats_settings.secondary_statsfile

        # Get first cluster, its index nodes, and first bucket
        (cluster_name, servers) = \
            next(self.cluster_spec.yield_servers_by_role('index'))
        if not servers:
            raise RuntimeError(
                "No index nodes specified for cluster {}".format(cluster_name))
        self.index_nodes = servers

        self.bucket = self.test_config.buckets[0]
        self._block_memory()
        self.target_iterator = TargetIterator(self.cluster_spec, self.test_config, "gsi")

        if self.storage == "plasma":
            self.COLLECTORS["secondary_storage_stats"] = True
            self.COLLECTORS["secondary_storage_stats_mm"] = True

    def _block_memory(self):
        if self.block_memory > 0:
            self.remote.block_memory(self.block_memory)

    def check_memory_blocker(self):
        if self.block_memory > 0:
            if not self.remote.check_process_running("memblock"):
                raise Exception('memblock is not running, might have been killed!!!')

    def remove_statsfile(self):
        rmfile = "rm -f {}".format(self.test_config.stats_settings.secondary_statsfile)
        status = subprocess.call(rmfile, shell=True)
        if status != 0:
            raise Exception('existing 2i latency stats file could not be removed')
        else:
            logger.info('Existing 2i latency stats file removed')

    @with_stats
    def build_secondaryindex(self):
        return self._build_secondaryindex()

    def _build_secondaryindex(self):
        """call cbindex create command"""
        logger.info('building secondary index..')

        self.remote.build_secondary_index(self.index_nodes, self.bucket,
                                          self.indexes, self.storage)
        time_elapsed = self.monitor.wait_for_secindex_init_build(
            self.index_nodes[0].split(':')[0],
            list(self.indexes.keys()),
        )
        return time_elapsed

    def run_access_for_2i(self, run_in_background=False):
        if run_in_background:
            self.access_bg()
        else:
            self.access()

    @staticmethod
    def get_data_from_config_json(config_file_name):
        with open(config_file_name) as fh:
            return json.load(fh)

    def validate_num_connections(self):
        db = SerieslyStore.build_dbname(self.cbagent.cluster_ids[0], None, None, None, "secondary_debugstats")
        config_data = self.get_data_from_config_json(self.configfile)
        # Expecting few extra connections(Number of GSi clients) than concurrency in config file
        ret = self.metric_helper.verify_series_in_limits(db, config_data["Concurrency"] + 5, "num_connections")
        if not ret:
            raise Exception('Validation for num_connections failed')

    @with_stats
    def apply_scanworkload(self, path_to_tool="/opt/couchbase/bin/cbindexperf"):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        status = run_cbindexperf(path_to_tool, self.index_nodes[0], rest_username, rest_password, self.configfile)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')

    def print_index_disk_usage(self, text=""):
        (data, index) = self.cluster_spec.paths
        logger.info("{}".format(text))
        logger.info("Disk usage:\n{}".format(self.remote.get_disk_usage(index)))
        logger.info("Index storage stats:\n{}".format(
            self.rest.get_index_storage_stats(self.index_nodes[0].split(':')[0])))
        logger.info("Indexer heap profile:\n{}".format(
            self.remote.get_indexer_heap_profile(self.index_nodes[0].split(':')[0])))
        if self.storage == 'plasma':
            logger.info("Index storage stats mm:\n{}".format(
                self.rest.get_index_storage_stats_mm(self.index_nodes[0].split(':')[0])))

        return self.remote.get_disk_usage(index, human_readable=False)

    def change_scan_range(self, iteration):
        num_hot_items = \
            int(self.test_config.access_settings.items * self.test_config.access_settings.working_set / 100.0)

        with open(self.configfile, "r") as jsonFile:
            data = json.load(jsonFile)

        if iteration == 0:
            old_start = num_hot_items
        else:
            old_start = data["ScanSpecs"][0]["Low"][0].split("-")
            old_start = old_start[0] if len(old_start) == 1 else old_start[1]

        # predict next hot_load_start
        start = (int(old_start) * 2.5) % (self.test_config.access_settings.items - num_hot_items)
        end = start + num_hot_items

        data["ScanSpecs"][0]["Low"][0] = '%012d' % start
        data["ScanSpecs"][0]["High"][0] = '%012d' % end

        with open(self.configfile, "w") as jsonFile:
            jsonFile.write(json.dumps(data))

    def read_scanresults(self):
        with open('{}'.format(self.configfile)) as config_file:
            configdata = json.load(config_file)
        numscans = configdata['ScanSpecs'][0]['Repeat']

        with open('result.json') as result_file:
            resdata = json.load(result_file)
        duration_s = (resdata['Duration'])
        num_rows = resdata['Rows']
        """scans and rows per sec"""
        scansps = numscans / duration_s
        rowps = num_rows / duration_s
        return scansps, rowps

    def __exit__(self, *args, **kwargs):
        super().__exit__(*args, **kwargs)
        if self.block_memory > 0:
            self.remote.kill_process_on_index_node("memblock")
        if self.test_config.gsi_settings.restrict_kernel_memory:
            self.remote.reset_memory_kernel_parameter()
            self.remote.reboot()
            self.monitor.wait_for_indexer()


class InitialandIncrementalSecondaryIndexTest(SecondaryIndexTest):
    """
    The test measures time it takes to build index for the first time as well as
    incremental build. There is no disabling of index updates in incremental building,
    index updating is conurrent to KV incremental load.
    """

    def _report_kpi(self, time_elapsed, index_type, unit="min"):

        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type=index_type,
                                                  unit=unit)
        )

    def build_initindex(self):
        self.build_secondaryindex()

    @with_stats
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
            self.report_kpi(recovery_time, 'Recovery', "ms")

    def load_and_build_initial_index(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        from_ts, to_ts = self.build_secondaryindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)
        self.print_index_disk_usage()
        if not self.incremental_only:
            self.report_kpi(time_elapsed, 'Initial')

    def run(self):
        self.load_and_build_initial_index()

        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Incremental secondary index', time_elapsed)
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Incremental')

        self.run_recovery_scenario()
        self.check_memory_blocker()


class InitialandIncrementalDGMSecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):
    """
    The test measures time it takes to build index for the first time as well as
    incremental build. There is no disabling of index updates in incremental building,
    index updating is conurrent to KV incremental load.
    """
    def run(self):
        self.load_and_build_initial_index()

        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        self.reporter.finish('First incremental secondary index', time_elapsed)
        self.print_index_disk_usage()

        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Second incremental secondary index', time_elapsed)
        self.print_index_disk_usage()

        self.report_kpi(time_elapsed, 'Incremental')
        self.run_recovery_scenario()
        self.check_memory_blocker()


class MultipleIncrementalSecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):

    def __init__(self, *args):
        super().__init__(*args)
        self.memory_usage = dict()
        self.disk_usage = dict()

    def _report_kpi(self, usage_diff, memory_type, unit="GB"):
        usage_diff = float(usage_diff) / 2 ** 30
        usage_diff = round(usage_diff, 2)

        self.reporter.post_to_sf(
            *self.metric_helper.get_memory_meta(value=usage_diff,
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
                self.print_index_disk_usage(text="After running incremental load for {} iteration/s =>\n".format(i))
            self.memory_usage[i] = self.remote.get_indexer_process_memory()
            time.sleep(30)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self._build_secondaryindex()

        self.build_incrindex_multiple_times(self.incremental_load_iterations)

        # report memory usage diff
        self.report_kpi(self.memory_usage[6] - self.memory_usage[4], 'Memory usage difference')

        # report disk usage diff
        self.report_kpi(self.disk_usage[6] - self.disk_usage[4], 'Disk usage difference')

        self.run_recovery_scenario()
        self.check_memory_blocker()


class InitialandIncrementalSecondaryIndexRebalanceTest(InitialandIncrementalSecondaryIndexTest):

    def rebalance(self, initial_nodes, nodes_after):
        clusters = self.cluster_spec.yield_clusters()
        for _, servers in clusters:
            master = servers[0]
            ejected_nodes = []
            new_nodes = enumerate(
                servers[initial_nodes:nodes_after],
                start=initial_nodes
            )
            known_nodes = servers[:nodes_after]
            for i, host_port in new_nodes:
                self.rest.add_node(master, host_port)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.rebalance(initial_nodes[0], nodes_after[0])
        from_ts, to_ts = self.build_secondaryindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Initial')

        master = []
        for _, servers in self.cluster_spec.yield_clusters():
            master = servers[0]
        self.monitor.monitor_rebalance(master)
        initial_nodes[0] += 1
        nodes_after[0] += 1
        self.rebalance(initial_nodes[0], nodes_after[0])
        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Incremental secondary index', time_elapsed)
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Incremental')


class InitialSecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):
    """
    The test measures time it takes to build index for the first time
    """

    def run(self):
        self.load_and_build_initial_index()

        self.run_recovery_scenario()
        self.check_memory_blocker()


class SecondaryIndexingThroughputTest(SecondaryIndexTest):
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """

    def _report_kpi(self, scan_thr):
        self.reporter.post_to_sf(
            round(scan_thr, 1)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(scan_thr)
        self.validate_num_connections()


class SecondaryIndexingThroughputRebalanceTest(SecondaryIndexingThroughputTest):
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput"""

    def rebalance(self, initial_nodes, nodes_after):
        clusters = self.cluster_spec.yield_clusters()
        for _, servers in clusters:
            master = servers[0]
            ejected_nodes = []
            new_nodes = enumerate(
                servers[initial_nodes:nodes_after],
                start=initial_nodes
            )
            known_nodes = servers[:nodes_after]
            for i, host_port in new_nodes:
                self.rest.add_node(master, host_port)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.rebalance(initial_nodes[0], nodes_after[0])
        self.apply_scanworkload()
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(scan_thr)
        self.validate_num_connections()


class InitialIncrementalScanThroughputTest(InitialandIncrementalDGMSecondaryIndexTest):

    def _report_kpi(self, *args):
        if len(args) == 1:
            self.report_throughput_kpi(*args)

    def report_throughput_kpi(self, scan_thr):
        self.reporter.post_to_sf(
            round(scan_thr, 1)
        )

    def run(self):
        self.remove_statsfile()
        super().run()
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(scan_thr)
        self.validate_num_connections()


class InitialIncrementalMovingScanThroughputTest(InitialIncrementalScanThroughputTest):
    """
    The test applies scan workload against the 2i server and measures
    scan throughput for moving workload
    """
    def __init__(self, *args):
        super().__init__(*args)
        self.scan_thr = []

    def get_config(self):
        with open('{}'.format(self.configfile)) as config_file:
            config_data = json.load(config_file)
        return config_data['ScanSpecs'][0]['NInterval'], config_data['Concurrency']

    """
    get_throughput function parses statsfile created by cbindexperf, and
    calculates throughput.
    interval- number of scans after which entry made in statsfile
    concurrency- number of concurrent routines making scans
    Format for statsfile is-
    id:1, rows:0, duration:8632734534, Nth-latency:16686556
    id:1, rows:0, duration:3403693509, Nth-latency:6859285
    """
    def get_throughput(self) -> float:
        duration = 0
        lines = 0
        with open(self.secondary_statsfile, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',')
            for row in csv_reader:
                duration += int(row[2].split(":")[1])
                lines += 1
        interval, concurrency = self.get_config()
        return lines * interval / (duration / 1000000000 / concurrency)

    """
    Calculating average throughput from list of throughputs
    """
    def calc_throughput(self) -> float:
        logger.info('Throughputs collected over hot workloads: {}'.format(self.scan_thr))
        return sum(self.scan_thr) / len(self.scan_thr)

    """
    Apply moving scan workload and collect throughput for each load
    """
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
            self.scan_thr.append(self.get_throughput())
            t += self.test_config.access_settings.working_set_move_time

    def run(self):
        self.remove_statsfile()
        InitialandIncrementalSecondaryIndexTest.run(self)
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
        scan_throughput = self.calc_throughput()
        self.print_index_disk_usage()
        self.report_kpi(scan_throughput)
        self.validate_num_connections()


class SecondaryIndexingScanLatencyTest(SecondaryIndexTest):
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """
    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def _report_kpi(self, *args):
        self.reporter.post_to_sf(
            *self.metric_helper.calc_secondary_scan_latency(percentile=90)
        )

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
        self.print_index_disk_usage()
        self.report_kpi()
        self.validate_num_connections()


class SecondaryIndexingScanLatencyRebalanceTest(SecondaryIndexingScanLatencyTest):
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """

    def rebalance(self, initial_nodes, nodes_after):
        clusters = self.cluster_spec.yield_clusters()
        for _, servers in clusters:
            master = servers[0]
            ejected_nodes = []
            new_nodes = enumerate(
                servers[initial_nodes:nodes_after],
                start=initial_nodes
            )
            known_nodes = servers[:nodes_after]
            for i, host_port in new_nodes:
                self.rest.add_node(master, host_port)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.rebalance(initial_nodes[0], nodes_after[0])
        self.apply_scanworkload()
        self.print_index_disk_usage()
        self.report_kpi()
        self.validate_num_connections()


class InitialIncrementalScanLatencyTest(InitialandIncrementalDGMSecondaryIndexTest):
    """
    The test perform initial index build, then incremental index build and
    then applies scan workload against the 2i server and measures
    scan latency for moving workload
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
        self.reporter.post_to_sf(
            *self.metric_helper.calc_secondary_scan_latency(percentile=90)
        )

    def run(self):
        self.remove_statsfile()
        super().run()
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
        self.print_index_disk_usage()
        self.report_kpi()
        self.validate_num_connections()

        self.run_recovery_test = self.run_recovery_test_local
        self.run_recovery_scenario()
        self.check_memory_blocker()


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
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan latency end to end, meaning since doc is added till it gets reflected in query
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True, "secondary_index_latency": True}

    def _report_kpi(self):
        self.reporter.post_to_sf(
            *self.metric_helper.calc_observe_latency(percentile=90)
        )

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self._build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
        self.report_kpi()


class SecondaryIndexingLatencyTest(SecondaryIndexTest):
    """
    This test applies scan workload against a 2i server and measures
    the indexing latency
    """

    @with_stats
    def apply_scanworkload(self, *args):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        logger.info('Initiating the scan workload')
        cmdstr = "/opt/couchbase/bin/cbindexperf -cluster {} -auth=\"{}:{}\" " \
                 "-configfile scripts/config_indexinglatency.json " \
                 "-resultfile result.json".format(self.index_nodes[0], rest_username, rest_password)
        status = subprocess.call(cmdstr, shell=True)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')
        return status

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.hot_load()
        self.build_secondaryindex()
        num_samples = 100
        samples = []

        while num_samples != 0:
            self.access()

            time_before = time.time()
            status = self.apply_scanworkload()
            time_after = time.time()
            if status == 0:
                num_samples -= 1
                time_elapsed = (time_after - time_before) / 1000000.0
                samples.append(time_elapsed)

        temp = np.array(samples)
        indexing_latency_percentile_80 = np.percentile(temp, 80)

        logger.info('Indexing latency (80th percentile): {} ms.'.format(indexing_latency_percentile_80))

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(indexing_latency_percentile_80)


class SecondaryNumConnectionsTest(SecondaryIndexTest):
    """
    This test applies scan workload against a 2i server and measures
    the number of connections it can create.
    """

    def __init__(self, *args):
        super().__init__(*args)
        self.curr_connections = self.init_num_connections
        self.config_data = self.get_data_from_config_json('tests/gsi/config_template.json')

    def _report_kpi(self, connections):
        self.reporter.post_to_sf(value=connections)

    @with_stats
    def apply_scanworkload_steps(self):
        logger.info('Initiating scan workload with stats output')
        while self.curr_connections <= self.max_num_connections:
            try:
                self.curr_connections = (self.curr_connections / len(self.remote.kv_hosts)) * len(self.remote.kv_hosts)
                logger.info("try for num-connections: {}".format(self.curr_connections))
                self.remote.run_cbindexperf(self.index_nodes[0], self.config_data, self.curr_connections)
                status = self.monitor.wait_for_num_connections(self.index_nodes[0].split(':')[0], self.curr_connections)
            except Exception as e:
                logger.info("Got error {}".format(e))
                status = False
                break
            finally:
                self.remote.kill_process_on_kv_nodes("cbindexperf")
            time.sleep(5)
            self.curr_connections += self.step_num_connections
            if not status:
                break
        ret_val = self.curr_connections - self.step_num_connections
        if not status:
            ret_val -= self.step_num_connections
        return ret_val if ret_val > self.init_num_connections else 0

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.build_secondaryindex()

        connections = self.apply_scanworkload_steps()
        logger.info('Connections: {}'.format(connections))

        self.report_kpi(connections)


class SecondaryIndexingMultiScanTest(SecondaryIndexingScanLatencyTest):
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """
    COLLECTORS = {'secondary_stats': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def _report_kpi(self, multifilter_time, independent_time):
        time_diff = independent_time - multifilter_time
        self.reporter.post_to_sf(round(time_diff, 2))

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
        self.compact_bucket()
        self.build_secondaryindex()

        self.run_access_for_2i(run_in_background=True)

        multifilter_time = self.apply_scan_multifilter_workload()
        logger.info("Multifilter time taken: {}".format(multifilter_time))

        independent_time = self.apply_scanworkloads()
        logger.info("Independent filters time taken: {}".format(independent_time))

        self.report_kpi(multifilter_time, independent_time)


class SecondaryRebalanceTest(SecondaryIndexTest, RebalanceTest):
    """
    Measure swap rebalance time for indexer.
    """

    COLLECTORS = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self._build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.rebalance_indexer()
        self.report_kpi(self.rebalance_time)

    def rebalance_indexer(self):
        self._rebalance(services="index")

    def _report_kpi(self, rebalance_time):
        self.reporter.post_to_sf(rebalance_time)

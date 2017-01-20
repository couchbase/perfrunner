import json
import subprocess
import time

import numpy as np
from logger import logger

from cbagent.stores import SerieslyStore
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.local import run_cbindexperf
from perfrunner.tests import PerfTest


class SecondaryIndexTest(PerfTest):

    """
    The test measures time it takes to build secondary index. This is just a
    base class, actual measurements happen in initial and incremental secondary
    indexing tests. It benchmarks dumb/bulk indexing.
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_debugstats': True,
                  'secondary_debugstats_bucket': True, 'secondary_debugstats_index': True}

    def __init__(self, *args):
        super(SecondaryIndexTest, self).__init__(*args)

        self.configfile = self.test_config.gsi_settings.cbindexperf_configfile
        self.configfiles = self.test_config.gsi_settings.cbindexperf_configfiles.split(",")
        self.init_num_connections = self.test_config.gsi_settings.init_num_connections
        self.step_num_connections = self.test_config.gsi_settings.step_num_connections
        self.max_num_connections = self.test_config.gsi_settings.max_num_connections
        self.run_recovery_test = self.test_config.gsi_settings.run_recovery_test
        self.block_memory = self.test_config.gsi_settings.block_memory

        self.storage = self.test_config.gsi_settings.storage
        self.indexes = self.test_config.gsi_settings.indexes

        # Get first cluster, its index nodes, and first bucket
        (cluster_name, servers) = \
            self.cluster_spec.yield_servers_by_role('index').next()
        if not servers:
            raise RuntimeError(
                "No index nodes specified for cluster {}".format(cluster_name))
        self.index_nodes = servers

        self.bucket = self.test_config.buckets[0]
        self._block_memory()

    def __del__(self):
        self.remote.kill_process_on_index_node("memblock")

    def _block_memory(self):
        if self.block_memory > 0:
            self.remote.block_memory(self.block_memory)

    @with_stats
    def build_secondaryindex(self):
        return self._build_secondaryindex()

    def _build_secondaryindex(self):
        """call cbindex create command"""
        logger.info('building secondary index..')

        self.remote.build_secondary_index(self.index_nodes, self.bucket,
                                          self.indexes, self.storage)
        time_elapsed = self.monitor.wait_for_secindex_init_build(
            self.index_nodes[0].split(':')[0], self.indexes.keys())
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
        # Expecting one extra connection than concurrency in config file, MB-21584
        ret = self.metric_helper.verify_series_in_limits(db, config_data["Concurrency"] + 1, "num_connections")
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

    def print_index_disk_usage(self):
        (data, index) = self.cluster_spec.paths
        logger.info("Disk usage:\n{}".format(self.remote.get_disk_usage(index)))
        logger.info("Index storage stats:\n{}".format(
            self.rest.get_index_storage_stats(self.index_nodes[0].split(':')[0])))


class InitialandIncrementalSecondaryIndexTest(SecondaryIndexTest):
    """
    The test measures time it takes to build index for the first time as well as
    incremental build. There is no disabling of index updates in incremental building,
    index updating is conurrent to KV incremental load.
    """

    def _report_kpi(self, time_elapsed, index_type, unit="min"):
        storage = self.storage == 'memdb' and 'moi' or 'fdb'
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type=index_type,
                                                  storage=storage,
                                                  unit=unit)
        )

    def build_initindex(self):
        self.build_secondaryindex()

    @with_stats
    def build_incrindex(self):
        access_settings = self.test_config.access_settings
        load_settings = self.test_config.load_settings
        self.worker_manager.run_workload(access_settings, self.target_iterator)
        self.worker_manager.wait_for_workers()
        numitems = load_settings.items + access_settings.items
        self.monitor.wait_for_secindex_incr_build(self.index_nodes, self.bucket,
                                                  self.indexes.keys(), numitems)

    def run_recovery_scenario(self):
        if self.run_recovery_test:
            # Measure recovery time for index
            self.remote.kill_process_on_index_node("indexer")
            recovery_time = self.monitor.wait_for_recovery(self.index_nodes, self.bucket, self.indexes.keys()[0])
            if recovery_time == -1:
                raise Exception('Indexer failed to recover...!!!')
            self.report_kpi(recovery_time, 'Recovery', "ms")

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        from_ts, to_ts = self.build_secondaryindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Initial')

        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Incremental secondary index', time_elapsed)
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Incremental')

        self.run_recovery_scenario()


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


class SecondaryIndexingThroughputTest(SecondaryIndexTest):
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """

    def _report_kpi(self, scan_thr):
        self.reporter.post_to_sf(
            round(scan_thr, 1)
        )

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


class SecondaryIndexingScanLatencyTest(SecondaryIndexTest):
    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """
    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def _report_kpi(self):
        self.reporter.post_to_sf(
            *self.metric_helper.calc_secondary_scan_latency(percentile=80)
        )

    def remove_statsfile(self):
        rmfile = "rm -f {}".format(self.test_config.stats_settings.secondary_statsfile)
        status = subprocess.call(rmfile, shell=True)
        if status != 0:
            raise Exception('existing 2i latency stats file could not be removed')
        else:
            logger.info('Existing 2i latency stats file removed')

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
            *self.metric_helper.calc_observe_latency(percentile=80)
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
    def apply_scanworkload(self):
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
            access_settings = self.test_config.access_settings
            self.worker_manager.run_workload(access_settings, self.target_iterator)
            self.worker_manager.wait_for_workers()
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
        super(SecondaryNumConnectionsTest, self).__init__(*args)
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
                logger.info("Got error {}".format(e.message))
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

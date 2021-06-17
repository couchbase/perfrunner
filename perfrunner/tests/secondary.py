import copy
import csv
import json
import subprocess
import time

import numpy

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.local import (
    extract_cb_deb,
    get_indexer_heap_profile,
    kill_process,
    run_cbindex,
    run_cbindexperf,
)
from perfrunner.helpers.profiler import with_profiles
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
        extract_cb_deb(filename='couchbase.deb')

        self.cbindexperf_concurrency = self.test_config.gsi_settings.cbindexperf_concurrency
        self.cbindexperf_repeat = self.test_config.gsi_settings.cbindexperf_repeat
        self.local_path_to_cbindex = './opt/couchbase/bin/cbindex'

        if self.configfile:
            with open(self.configfile, 'r') as f:
                cbindexperf_contents = json.load(f)
                if self.cbindexperf_concurrency:
                    cbindexperf_contents["Concurrency"] = self.cbindexperf_concurrency
                if self.cbindexperf_repeat:
                    for scan_spec in cbindexperf_contents["ScanSpecs"]:
                        scan_spec["Repeat"] = self.cbindexperf_repeat

            with open(self.configfile, 'w') as f:
                json.dump(cbindexperf_contents, f, indent=4)

        if self.storage == "plasma":
            self.COLLECTORS["secondary_storage_stats"] = True
            self.COLLECTORS["secondary_storage_stats_mm"] = True

        if self.test_config.gsi_settings.disable_perindex_stats:
            self.COLLECTORS["secondary_debugstats_index"] = False
            self.COLLECTORS["secondary_storage_stats"] = False

        self.build = self.rest.get_version(self.master_node)

    def remove_statsfile(self):
        rmfile = "rm -f {}".format(self.SECONDARY_STATS_FILE)
        status = subprocess.call(rmfile, shell=True)
        if status != 0:
            raise Exception('existing 2i latency stats file could not be removed')
        else:
            logger.info('Existing 2i latency stats file removed')

    def batch_create_index_collection_options(self, indexes, storage):
        indexes_copy = copy.deepcopy(indexes)
        all_options = ""
        for bucket_name, scope_map in indexes_copy.items():
            for scope_name, collection_map in scope_map.items():
                for collection_name, index_map in collection_map.items():
                    for index, index_config in index_map.items():
                        configs = ''
                        partition_keys = ''
                        if type(index_config) is dict:
                            index_def = index_config.pop("field")
                            for config, value in index_config.items():
                                configs = configs + '"{}":{},'.format(config, value)

                            if index_config["num_partition"] > 1:
                                partition_keys = " --scheme KEY" \
                                                 " --partitionKeys `{}` ".format(index_def)
                        else:
                            index_def = index_config

                        where = None
                        if ':' in index_def:
                            fields, where = index_def.split(":")
                        else:
                            fields = index_def
                        fields_list = fields.split(",")

                        options = "-type create " \
                                  "-bucket {bucket} " \
                                  "-scope {scope} " \
                                  "-collection {collection} ". \
                            format(bucket=bucket_name,
                                   scope=scope_name,
                                   collection=collection_name)

                        options += "-fields "
                        for field in fields_list:
                            options += "`{}`,".format(field)

                        options = options.rstrip(",")
                        options = options + " "

                        if where is not None:
                            options = '{options} -where "{where_clause}"' \
                                .format(options=options, where_clause=where)

                        if storage == 'memdb' or storage == 'plasma':
                            options = '{options} -using {db}'.format(options=options,
                                                                     db=storage)

                        if partition_keys:
                            options = "{options} {partition_keys}"\
                                .format(options=options, partition_keys=partition_keys)

                        options = "{options} -index {index} " \
                            .format(options=options, index=index)

                        options = options + '-with {{{}"defer_build":true}} \n'.format(configs)
                        options = options.rstrip(',')
                        all_options = all_options + options

        return all_options

    def batch_build_index_collection_options(self, indexes):
        all_options = ""
        for bucket_name, scope_map in indexes.items():
            for scope_name, collection_map in scope_map.items():
                for collection_name, index_map in collection_map.items():
                    build_indexes = ",".join(["{}:{}:{}:{}".format(
                        bucket_name,
                        scope_name,
                        collection_name,
                        index_name)
                        for index_name in index_map.keys()])
                    options = "-type build " \
                              "-indexes {build_indexes} \n" \
                        .format(build_indexes=build_indexes)
                    all_options = all_options + options
        return all_options

    @with_stats
    @with_profiles
    def build_secondaryindex(self):
        return self._build_secondaryindex()

    def _build_secondaryindex(self):
        """Call cbindex create command."""
        logger.info('building secondary index..')
        if self.test_config.collection.collection_map:
            create_options = self.batch_create_index_collection_options(self.indexes, self.storage)
            self.remote.create_secondary_index_collections(
                self.index_nodes, create_options)

            build_options = self.batch_build_index_collection_options(self.indexes)

            build_start = time.time()
            self.remote.build_secondary_index_collections(
                self.index_nodes, build_options)
            self.monitor.wait_for_secindex_init_build_collections(
                self.index_nodes[0],
                self.indexes)

            time_elapsed = time.time() - build_start
        else:
            self.remote.build_secondary_index(
                self.index_nodes,
                self.bucket,
                self.indexes,
                self.storage)

            time_elapsed = self.monitor.wait_for_secindex_init_build(
                self.index_nodes[0],
                list(self.indexes.keys()))
        return time_elapsed

    @staticmethod
    def get_data_from_config_json(config_file_name):
        with open(config_file_name) as fh:
            return json.load(fh)

    def validate_num_connections(self):
        config_data = self.get_data_from_config_json(self.configfile)
        # Expecting connections = Number of GSi clients * concurrency in config file
        error = 0
        # Added error to forestdb as buffer for corner cases
        if self.storage == 'forestdb':
            error = 20
        ret = self.metrics.verify_series_in_limits(config_data["Concurrency"] *
                                                   config_data["Clients"] + error)
        if not ret:
            raise Exception('Validation for num_connections failed')

    @with_stats
    @with_profiles
    def apply_scanworkload(self, path_to_tool="./opt/couchbase/bin/cbindexperf"):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        with open(self.configfile, 'r') as fp:
            config_file_content = fp.read()

        if not self.test_config.gsi_settings.disable_perindex_stats:
            logger.info("cbindexperf config file: \n" + config_file_content)

        status = run_cbindexperf(path_to_tool, self.index_nodes[0],
                                 rest_username, rest_password, self.configfile)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')

    def calc_avg_rr(self, storage_stats):
        total_num_rec_allocs, total_num_rec_frees, total_num_rec_swapout, total_num_rec_swapin =\
            0, 0, 0, 0

        for index in storage_stats:
            total_num_rec_allocs += index["Stats"]["MainStore"]["num_rec_allocs"] + \
                index["Stats"]["BackStore"]["num_rec_allocs"]
            total_num_rec_frees += index["Stats"]["MainStore"]["num_rec_frees"] + \
                index["Stats"]["BackStore"]["num_rec_frees"]
            total_num_rec_swapout += index["Stats"]["MainStore"]["num_rec_swapout"] + \
                index["Stats"]["BackStore"]["num_rec_swapout"]
            total_num_rec_swapin += index["Stats"]["MainStore"]["num_rec_swapin"] + \
                index["Stats"]["BackStore"]["num_rec_swapin"]

        total_recs_in_mem = total_num_rec_allocs - total_num_rec_frees
        total_recs_on_disk = total_num_rec_swapout - total_num_rec_swapin
        logger.info("Total Recs in Mem {}".format(total_recs_in_mem))
        logger.info("Total Recs in Disk {}".format(total_recs_on_disk))
        avg_rr = total_recs_in_mem / (total_recs_on_disk + total_recs_in_mem)
        return avg_rr

    def print_average_rr(self):
        if self.storage == 'plasma':
            version, build_number = self.build.split('-')
            build = tuple(map(int, version.split('.'))) + (int(build_number),)
            # MB - 43098 Caused missing stats from indexer - Hence this fails
            # before build 7.0.0-3951
            if build > (7, 0, 0, 3951):
                storage_stats = self.rest.get_index_storage_stats(self.index_nodes[0])
                avg_rr = self.calc_avg_rr(storage_stats.json())
                logger.info("Average RR over all Indexes  : {}".format(avg_rr))

    def print_index_disk_usage(self, text=""):
        self.print_average_rr()
        if self.test_config.gsi_settings.disable_perindex_stats:
            return

        if text:
            logger.info("{}".format(text))

        disk_usage = self.remote.get_disk_usage(self.index_nodes[0],
                                                self.cluster_spec.index_path)
        logger.info("Disk usage:\n{}".format(disk_usage))

        storage_stats = self.rest.get_index_storage_stats(self.index_nodes[0])
        logger.info("Index storage stats:\n{}".format(storage_stats.text))

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

    def load_and_build_initial_index(self):
        self.load()
        self.wait_for_persistence()
        time_elapsed = self.build_secondaryindex()
        self.print_index_disk_usage()
        if not self.incremental_only:
            self.report_kpi(time_elapsed, 'Initial')

    @with_stats
    @timeit
    @with_profiles
    def build_incrindex(self):
        if self.test_config.collection.collection_map is not None:
            coll_map = self.test_config.collection.collection_map
            num_access_collections = 0
            for bucket in coll_map.keys():
                for scope in coll_map[bucket].keys():
                    for collection in coll_map[bucket][scope].keys():
                        if coll_map[bucket][scope][collection]['load'] == 1 \
                                and coll_map[bucket][scope][collection]['access'] == 1:
                            num_access_collections += 1

            expected_docs = \
                (self.test_config.load_settings.items + self.test_config.access_settings.items) \
                // num_access_collections

            self.access()
            self.monitor.wait_for_secindex_incr_build_collections(
                self.index_nodes,
                self.indexes,
                expected_docs)
        else:
            self.access()
            numitems = self.test_config.load_settings.items + self.test_config.access_settings.items
            self.monitor.wait_for_secindex_incr_build(
                self.index_nodes,
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

    def run(self):
        self.load_and_build_initial_index()

        time_elapsed = self.build_incrindex()
        self.print_index_disk_usage()
        self.report_kpi(time_elapsed, 'Incremental')

        self.run_recovery_scenario()


class InitialandIncrementalandRecoverySecondaryIndexTest(InitialandIncrementalSecondaryIndexTest):

    def run_recovery_scenario(self):
        if self.run_recovery_test:
            # Measure recovery time for index
            self.remote.kill_process_on_index_node("indexer")
            start_time = time.time()
            self.monitor.wait_for_secindex_init_build_collections(
                self.index_nodes[0], self.indexes, recovery=True)
            recovery_time = time.time() - start_time
            self.report_kpi(recovery_time, 'Recovery')


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

    COLLECTORS = {'secondary_stats': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True,
                  'secondary_debugstats_index': True}

    def __init__(self, *args):
        super().__init__(*args)
        self.scan_thr = []

    def _report_kpi(self,
                    percentile_latencies,
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
            title = "Secondary Scan Throughput (scanps) {}" \
                .format(str(self.test_config.showfast.title).strip())
            self.reporter.post(
                *self.metrics.scan_throughput(scan_thr,
                                              metric_id_append_str="thr",
                                              title=title)
            )
            title = str(self.test_config.showfast.title).strip()
            self.reporter.post(
                *self.metrics.secondary_scan_latency_value(percentile_latencies[90],
                                                           percentile=90,
                                                           title=title))
            self.reporter.post(
                *self.metrics.secondary_scan_latency_value(percentile_latencies[95],
                                                           percentile=95,
                                                           title=title))

    def calculate_scan_latencies(self):

        scan_latencies = []
        percentile_latencies = []

        with open(self.SECONDARY_STATS_FILE, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                nth_lat_str = row[-1]
                nth_lat_val = nth_lat_str.split(":")[-1]
                val = nth_lat_val.strip()
                scan_latencies.append(float(val))

        for percentile in range(100):
            percentile_latencies.append(numpy.percentile(scan_latencies, percentile))

        return percentile_latencies

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        initial_index_time = self.build_secondaryindex()
        self.report_kpi(0, 0, initial_index_time)
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        percentile_latencies = self.calculate_scan_latencies()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(percentile_latencies, scan_thr, 0)
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
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
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
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
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
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
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
    def apply_scanworkload(self, path_to_tool="./opt/couchbase/bin/cbindexperf"):
        """Apply moving scan workload and collect throughput for each load."""
        rest_username, rest_password = self.cluster_spec.rest_credentials

        t = 0
        while t < self.scan_time:
            self.change_scan_range(t)
            run_cbindexperf(path_to_tool, self.index_nodes[0], rest_username,
                            rest_password, self.configfile, run_in_background=True,
                            collect_profile=False)
            time.sleep(self.test_config.access_settings.working_set_move_time)
            kill_process("cbindexperf")
            self.scan_thr.append(self.get_throughput())
            t += self.test_config.access_settings.working_set_move_time

    def run(self):
        self.remove_statsfile()
        InitialandIncrementalDGMSecondaryIndexTest.run(self)
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
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
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
        self.print_index_disk_usage()
        self.report_kpi()
        self.validate_num_connections()


class ScanOverlapWorkloadTest(SecondaryIndexingScanTest):

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()
        self.build_secondaryindex()
        self.print_index_disk_usage()

        access_settings = copy.deepcopy(self.test_config.access_settings)
        access_settings.collections = access_settings.split_workload
        access_settings.throughput = int(access_settings.split_workload_throughput)
        access_settings.workers = int(access_settings.split_workload_workers)

        self.access_bg(settings=access_settings)
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        percentile_latencies = self.calculate_scan_latencies()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.print_index_disk_usage()
        self.report_kpi(percentile_latencies, scan_thr, 0)
        self.validate_num_connections()


class SecondaryIndexingScanLatencyLongevityTest(SecondaryIndexingScanLatencyTest):

    @with_stats
    def apply_scanworkload(self, path_to_tool="./opt/couchbase/bin/cbindexperf"):
        rest_username, rest_password = self.cluster_spec.rest_credentials

        t = 0
        while t < self.scan_time:
            run_cbindexperf(path_to_tool, self.index_nodes[0], rest_username,
                            rest_password, self.configfile, run_in_background=True,
                            collect_profile=False)
            time.sleep(3600)
            kill_process("cbindexperf")
            t += 3600

    def run(self):
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()
        self.access_bg()
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
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
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
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
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
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
        self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
        self.report_kpi(scan_thr)
        self.validate_num_connections()

        self.run_recovery_test = self.run_recovery_test_local
        self.run_recovery_scenario()


class InitialIncrementalMovingScanLatencyTest(InitialIncrementalScanLatencyTest):

    @with_stats
    def apply_scanworkload(self, path_to_tool="./opt/couchbase/bin/cbindexperf"):
        rest_username, rest_password = self.cluster_spec.rest_credentials

        t = 0
        while t < self.scan_time:
            self.change_scan_range(t)
            run_cbindexperf(path_to_tool, self.index_nodes[0], rest_username,
                            rest_password, self.configfile, run_in_background=True,
                            collect_profile=False)
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
        return self.apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf")

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


class CreateBackupandRestoreIndexTest(SecondaryIndexTest):

    """Measure time to create indexes, to backup and to restore indexes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.index_defns = {}

    def _report_kpi(self, time_elapsed, index_type, unit="min"):
        self.reporter.post(
            *self.metrics.get_ddl_time(value=time_elapsed,
                                       index_type=index_type,
                                       unit=unit)
        )

    @with_stats
    @timeit
    def run_create_index(self, create_options):
        status, error = run_cbindex(self.local_path_to_cbindex, create_options, self.index_nodes)
        if status != 0:
            raise Exception('Cbindex command failed with {}'.format(error))
        self.monitor.wait_for_secindex_init_build_collections(self.index_nodes[0],
                                                              self.indexes,
                                                              recovery=False,
                                                              created=True)

    @with_stats
    @timeit
    def backup(self):
        self.index_defns = self.rest.backup_index(self.index_nodes[0], self.bucket)

    @with_stats
    @timeit
    def restore(self):
        self.rest.restore_index(self.index_nodes[0], self.bucket, self.index_defns,
                                self.bucket, self.bucket)
        self.monitor.wait_for_secindex_init_build_collections(self.index_nodes[0],
                                                              self.indexes,
                                                              recovery=False,
                                                              created=True)

    def drop_all_indexes(self):
        logger.info('Dropping all indexes')
        self.rest.delete_bucket(self.master_node, self.bucket)
        self.monitor.wait_for_all_indexes_dropped(self.index_nodes)
        logger.info('Recreate buckets and collections')
        self.cluster.create_buckets()
        self.cluster.create_collections()

    def run(self):
        logger.info('Creating Secondary Indexes')
        if self.test_config.collection.collection_map:
            create_options = self.batch_create_index_collection_options(self.indexes, self.storage)
            time_elapsed = self.run_create_index(create_options)

        self.report_kpi(time_elapsed, "Create")

        for server in self.cluster_spec.servers_by_role('index'):
            logger.info("{} : {} Indexes".format(server, self.rest.indexes_per_node(server)))

        time_to_backup = self.backup()
        self.drop_all_indexes()
        time_to_restore = self.restore()

        self.report_kpi(time_to_backup, "Backup", "seconds")
        self.report_kpi(time_to_restore, "Restore", "seconds")


class CreateBackupandRestoreIndexTestWithRebalance(CreateBackupandRestoreIndexTest):

    """Measure time to create indexes, to backup and to restore indexes with rebalance."""

    @with_stats
    @with_profiles
    def rebalance_indexer(self):
        self.rebalance(services="index")

    def run(self):
        create_options = self.batch_create_index_collection_options(self.indexes, self.storage)
        self.run_create_index(create_options)

        time_to_backup = self.backup()
        self.drop_all_indexes()
        self.rebalance_indexer()
        time_to_restore = self.restore()

        self.report_kpi(time_to_backup, "Backup", "seconds")
        self.report_kpi(time_to_restore, "Restore", "seconds")

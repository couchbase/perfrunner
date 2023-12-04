import copy
import csv
import json
import subprocess
import threading
import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.local import (
    get_indexer_heap_profile,
    read_aws_credential,
    run_cbindexperf,
)
from perfrunner.helpers.misc import SGPortRange, pretty_dict
from perfrunner.helpers.profiler import with_profiles
from perfrunner.tests import PerfTest, TargetIterator
from perfrunner.tests.integration import EndToEndLatencyTest
from perfrunner.tests.rebalance import CapellaRebalanceTest
from perfrunner.tests.secondary import (
    SecondaryIndexingScanTest,
    SecondaryRebalanceTest,
)
from spring.docgen import decimal_fmtr


class SecondaryIndexCloudTest(PerfTest):

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
        self.remote.extract_cb_any('couchbase', worker_home=self.worker_manager.WORKER_HOME)

        self.cbindexperf_concurrency = self.test_config.gsi_settings.cbindexperf_concurrency
        self.cbindexperf_gcpercent = self.test_config.gsi_settings.cbindexperf_gcpercent
        self.cbindexperf_repeat = self.test_config.gsi_settings.cbindexperf_repeat
        self.cbindexperf_clients = self.test_config.gsi_settings.cbindexperf_clients
        self.cbindexperf_limit = self.test_config.gsi_settings.cbindexperf_limit
        self.local_path_to_cbindex = './opt/couchbase/bin/cbindex'
        self.is_ssl = False
        if self.test_config.cluster.enable_n2n_encryption:
            self.is_ssl = True

        if self.configfile:
            with open(self.configfile, 'r') as f:
                cbindexperf_contents = json.load(f)
                if self.cbindexperf_concurrency:
                    cbindexperf_contents["Concurrency"] = self.cbindexperf_concurrency
                if self.cbindexperf_repeat:
                    for scan_spec in cbindexperf_contents["ScanSpecs"]:
                        scan_spec["Repeat"] = self.cbindexperf_repeat
                if self.cbindexperf_limit:
                    for scan_spec in cbindexperf_contents["ScanSpecs"]:
                        scan_spec["Limit"] = self.cbindexperf_limit
                if self.cbindexperf_clients:
                    cbindexperf_contents["Clients"] = self.cbindexperf_clients

            with open(self.configfile, 'w') as f:
                json.dump(cbindexperf_contents, f, indent=4)

        if self.storage == "plasma":
            self.COLLECTORS["secondary_storage_stats"] = True
            self.COLLECTORS["secondary_storage_stats_mm"] = True

        if self.test_config.gsi_settings.disable_perindex_stats:
            self.COLLECTORS["secondary_debugstats_index"] = False
            self.COLLECTORS["secondary_storage_stats"] = False

        if self.test_config.gsi_settings.aws_credential_path:
            credential = read_aws_credential(self.test_config.gsi_settings.aws_credential_path)
            self.remote.create_aws_config_gsi(credential)
            self.remote.client_drop_caches()

        self.build = self.rest.get_version(self.master_node)

        self.download_certificate()
        self.remote.cloud_put_certificate(self.ROOT_CERTIFICATE, self.worker_manager.WORKER_HOME)

        if self.cluster_spec.has_any_capella:
            # Open ports for cbindex and cbindexperf
            self.cluster.open_capella_cluster_ports([SGPortRange(9100, 9105), SGPortRange(9999)])

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
                        if isinstance(index_config, dict):
                            index_def = index_config.pop("field")
                            for config, value in index_config.items():
                                configs = configs + '"{}":{},'.format(config, value)

                            if index_config.get("num_partition", 0) > 1:
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
                self.index_nodes, create_options, self.is_ssl)

            build_options = self.batch_build_index_collection_options(self.indexes)

            build_start = time.time()
            self.remote.build_secondary_index_collections(
                self.index_nodes, build_options, self.is_ssl)
            self.monitor.wait_for_secindex_init_build_collections(
                self.index_nodes[0],
                self.indexes)

            time_elapsed = time.time() - build_start
        else:
            self.remote.build_secondary_index(
                self.index_nodes,
                self.bucket,
                self.indexes,
                self.storage,
                self.is_ssl)

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
    def apply_scanworkload(self, path_to_tool="./opt/couchbase/bin/cbindexperf",
                           run_in_background=False, is_ssl=False):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        with open(self.configfile, 'r') as fp:
            config_file_content = fp.read()

        if not self.test_config.gsi_settings.disable_perindex_stats:
            logger.info("cbindexperf config file: \n" + config_file_content)

        status = run_cbindexperf(path_to_tool, self.index_nodes[0],
                                 rest_username, rest_password, self.configfile,
                                 run_in_background, is_ssl=is_ssl,
                                 gcpercent=self.cbindexperf_gcpercent)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')

    @with_stats
    @with_profiles
    def cloud_apply_scanworkload(self, path_to_tool="./opt/couchbase/bin/cbindexperf",
                                 run_in_background=False, is_ssl=False):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        with open(self.configfile, 'r') as fp:
            config_file_content = fp.read()

        if not self.test_config.gsi_settings.disable_perindex_stats:
            logger.info("cbindexperf config file: \n" + config_file_content)
        self.remote.cloud_put_scanfile(self.configfile, self.worker_manager.WORKER_HOME)
        status = str(self.remote.run_cbindexperf(path_to_tool, self.index_nodes[0],
                                                 rest_username, rest_password,
                                                 self.configfile,
                                                 self.worker_manager.WORKER_HOME,
                                                 run_in_background, is_ssl=is_ssl))

        logger.info("scan status {}".format(status))
        if "Log Level = error" not in status:
            raise Exception('Scan workload could not be applied: ' + str(status))
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

    def calc_avg_rr_compression(self, storage_stats):
        total_num_rec_allocs, total_num_rec_frees, total_num_rec_swapout, total_num_rec_swapin,\
            total_num_rec_compressed = 0, 0, 0, 0, 0

        for index in storage_stats:
            total_num_rec_compressed += index["Stats"]["MainStore"]["num_rec_compressed"] + \
                                        index["Stats"]["BackStore"]["num_rec_compressed"]
            total_num_rec_allocs += index["Stats"]["MainStore"]["num_rec_allocs"] + \
                index["Stats"]["BackStore"]["num_rec_allocs"]
            total_num_rec_frees += index["Stats"]["MainStore"]["num_rec_frees"] + \
                index["Stats"]["BackStore"]["num_rec_frees"]
            total_num_rec_swapout += index["Stats"]["MainStore"]["num_rec_swapout"] + \
                index["Stats"]["BackStore"]["num_rec_swapout"]
            total_num_rec_swapin += index["Stats"]["MainStore"]["num_rec_swapin"] + \
                index["Stats"]["BackStore"]["num_rec_swapin"]

        total_recs_in_mem = total_num_rec_allocs - total_num_rec_frees + total_num_rec_compressed
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
            if build > (7, 0, 0, 3951) and build < (7, 1, 0, 1887):
                storage_stats = self.rest.get_index_storage_stats(self.index_nodes[0])
                avg_rr = self.calc_avg_rr(storage_stats.json())
                logger.info("Average RR over all Indexes  : {}".format(avg_rr))

            if build >= (7, 1, 0, 1887):
                storage_stats = self.rest.get_index_storage_stats(self.index_nodes[0])
                avg_rr = self.calc_avg_rr_compression(storage_stats.json())
                logger.info("Average RR over all Indexes with compression  : {}".format(avg_rr))

    def print_index_disk_usage(self, text="", heap_profile=True):
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
        if heap_profile:
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

        with open(self.configfile, "r") as json_file:
            data = json.load(json_file)

        if iteration == 0:
            old_start = num_hot_items
        else:
            old_start = data["ScanSpecs"][0]["Low"][0].split("-")
            old_start = old_start[0] if len(old_start) == 1 else old_start[1]

        # predict next hot_load_start
        start = (int(old_start) + self.test_config.access_settings.working_set_moving_docs) % \
                (self.test_config.access_settings.items - num_hot_items)
        end = start + num_hot_items

        logger.info("new start {} new end {}".format(start, end))
        data["ScanSpecs"][0]["Low"][0] = decimal_fmtr(start, prefix='')
        data["ScanSpecs"][0]["High"][0] = decimal_fmtr(end, prefix='')

        with open(self.configfile, "w") as json_file:
            json_file.write(json.dumps(data))

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


class CapellaSecondaryRebalanceOnlyTest(CapellaRebalanceTest):

    COLLECTORS = {'secondary_stats': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings

    @timeit
    def _rebalance(self, services):
        master = self.master_node
        rebalance_config = self.rebalance_settings.rebalance_config

        new_cluster_config = {
            'specs': json.load(rebalance_config)
        }

        self.rest.update_cluster_configuration(master, new_cluster_config)
        self.monitor.wait_for_rebalance_to_begin(master)
        self.monitor_progress(master)

    def create_indexes(self):
        logger.info('Creating and building indexes')
        create_statements = []
        build_statements = []

        for statement in self.test_config.index_settings.statements:
            check_stmt = statement.replace(" ", "").upper()
            if 'CREATEINDEX' in check_stmt \
                    or 'CREATEPRIMARYINDEX' in check_stmt:
                create_statements.append(statement)
            elif 'BUILDINDEX' in check_stmt:
                build_statements.append(statement)

        queries = []
        for statement in create_statements:
            logger.info('Creating index: ' + statement)
            queries.append(threading.Thread(target=self.execute_index, args=(statement,)))

        for query in queries:
            query.start()

        for query in queries:
            query.join()

        queries = []
        for statement in build_statements:
            logger.info('Building index: ' + statement)
            queries.append(threading.Thread(target=self.execute_index, args=(statement,)))

        for query in queries:
            query.start()

        for query in queries:
            query.join()

        logger.info('Index Create and Build Complete')

    def execute_index(self, statement):
        self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
        cont = False
        while not cont:
            building = 0
            index_status = self.rest.get_index_status(self.index_nodes[0])
            index_list = index_status['status']
            for index in index_list:
                if index['status'] != "Ready" and index['status'] != "Created":
                    building += 1
            if building < 10:
                cont = True
            else:
                time.sleep(10)

    @with_stats
    @with_profiles
    def rebalance_indexer(self, services="index"):
        self.rebalance(services)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.create_indexes()
        self.wait_for_indexing()
        self.rebalance_indexer(services="index,n1ql")
        logger.info("Rebalance time: {}".format(self.rebalance_time))
        self.report_kpi(rebalance_time=True)


class CloudSecondaryInitalBuildTest(CapellaSecondaryRebalanceOnlyTest):

    @timeit
    @with_stats
    @with_profiles
    def build_index(self):
        self.create_indexes()
        self.wait_for_indexing()

    def _report_kpi(self, time_elapsed, index_type, unit="min"):
        self.reporter.post(
            *self.metrics.get_indexing_meta(value=time_elapsed,
                                            index_type=index_type,
                                            unit=unit,
                                            update_category=False)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        build_time = self.build_index()
        logger.info("index build time: {}".format(build_time))
        self.report_kpi(build_time, 'Initial')


class CloudSecondaryRebalanceTest(SecondaryIndexingScanTest, CapellaRebalanceTest):

    ALL_HOSTNAMES = True

    """Measure rebalance time for indexer with scan and access workload."""

    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.remote.extract_cb_any('couchbase', worker_home=self.worker_manager.WORKER_HOME)

    def get_config(self):
        with open('{}'.format(self.configfile)) as config_file:
            config_data = json.load(config_file)
        return config_data['ScanSpecs'][0]['NInterval'], config_data['Concurrency']

    def get_throughput(self) -> float:
        duration = 0
        lines = 0
        with open(self.SECONDARY_STATS_FILE, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',')
            for row in csv_reader:
                duration += int(row[2].split(":")[1])
                lines += 1
        interval, concurrency = self.get_config()
        logger.info("interval: {}, concurrency: {}, duration: {}".format(interval, concurrency,
                                                                         duration))
        return lines * interval / (duration / 1000000000 / concurrency)

    def run(self):
        self.download_certificate()
        self.remote.cloud_put_certificate(self.ROOT_CERTIFICATE, self.worker_manager.WORKER_HOME)
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()
        for server in self.index_nodes:
            logger.info("{} : {} Indexes".format(server,
                                                 self.rest.indexes_instances_per_node(server)))
        self.print_average_rr()
        self.access_bg()
        self.cloud_apply_scanworkload(path_to_tool="./opt/couchbase/bin/cbindexperf",
                                      run_in_background=True, is_ssl=self.is_ssl)
        if self.test_config.gsi_settings.excludeNode:
            planner_settings = {"excludeNode": self.test_config.gsi_settings.excludeNode}
            nodes = self.rest.get_active_nodes_by_role(self.master_node, 'index')
            for node in nodes:
                logger.info(f"setting planner settings {planner_settings}")
                self.rest.set_planner_settings(node, planner_settings)
                meta = self.rest.get_index_metadata(node)
                logger.info('Index Metadata: {}'.format(pretty_dict(meta['localSettings'])))

        self.rebalance_indexer()
        logger.info("Indexes after rebalance")
        for server in self.index_nodes:
            logger.info("{} : {} Indexes".format(server,
                                                 self.rest.indexes_instances_per_node(server)))
        self.report_kpi(rebalance_time=True)
        self.remote.kill_client_process("cbindexperf")
        self.remote.get_gsi_measurements(self.worker_manager.WORKER_HOME)
        scan_thr = self.get_throughput()
        percentile_latencies = self.calculate_scan_latencies()
        logger.info('Scan throughput: {}'.format(scan_thr))
        self.print_average_rr()
        self.report_kpi(percentile_latencies, scan_thr)

    @with_stats
    def rebalance_indexer(self, services="index"):
        self.rebalance(services)

    def _report_kpi(self,
                    percentile_latencies=0,
                    scan_thr: float = 0,
                    rebalance_time: bool = False):

        if rebalance_time:
            self.reporter.post(
                *self.metrics.rebalance_time(self.rebalance_time)
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


class CloudSecondaryRebalanceTestWithoutScan(CloudSecondaryRebalanceTest):

    def run(self):
        self.download_certificate()
        self.remote.cloud_put_certificate(self.ROOT_CERTIFICATE, self.worker_manager.WORKER_HOME)
        self.remove_statsfile()
        self.load()
        self.wait_for_persistence()

        self.build_secondaryindex()
        for server in self.index_nodes:
            logger.info("{} : {} Indexes".format(server,
                                                 self.rest.indexes_instances_per_node(server)))
        self.print_average_rr()
        self.access_bg()
        if self.test_config.gsi_settings.excludeNode:
            planner_settings = {"excludeNode": self.test_config.gsi_settings.excludeNode}
            nodes = self.rest.get_active_nodes_by_role(self.master_node, 'index')
            for node in nodes:
                logger.info(f"setting planner settings {planner_settings}")
                self.rest.set_planner_settings(node, planner_settings)
                meta = self.rest.get_index_metadata(node)
                logger.info('Index Metadata: {}'.format(pretty_dict(meta['localSettings'])))

        self.rebalance_indexer()
        logger.info("Indexes after rebalance")
        for server in self.index_nodes:
            logger.info("{} : {} Indexes".format(server,
                                                 self.rest.indexes_instances_per_node(server)))
        self.print_average_rr()
        self.report_kpi(rebalance_time=True)


class CloudSecondaryRebalanceOnlyTest(EndToEndLatencyTest, SecondaryRebalanceTest):
    COLLECTORS = {'secondary_stats': True,
                  'secondary_debugstats': True, 'secondary_debugstats_bucket': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings

    @with_stats
    @timeit
    def create_indexes_with_stats(self):
        self.create_indexes_with_statement_only()
        self.wait_for_indexing()

    def run(self):
        self.load()
        self.wait_for_persistence()

        build_time = self.create_indexes_with_stats()
        logger.info("index build completed in {} sec".format(build_time))
        index_meta = {"time": build_time, "type": "initial", "unit": "min"}
        self.report_index_kpi(index_meta)

        for server in self.index_nodes:
            logger.info("{} : {} Indexes".format(server,
                                                 self.rest.indexes_instances_per_node(server)))

        self.rebalance_indexer()
        logger.info("Indexes after rebalance")
        logger.info("Rebalance time: {}".format(self.rebalance_time))
        for server in self.index_nodes:
            logger.info("{} : {} Indexes".format(server,
                                                 self.rest.indexes_instances_per_node(server)))
        self.print_index_disk_usage(heap_profile=False)
        self.report_kpi(rebalance_time=True)

import json
import subprocess
import time

import numpy as np
from logger import logger

from cbagent.stores import SerieslyStore
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class SecondaryIndexTest(PerfTest):
    """
    The test measures time it takes to build secondary index. This is just a base
    class, actual measurements happen in initial and incremental secondary indexing tests.
    It benchmarks dumb/bulk indexing.

    Sample test spec snippet:

    [secondary]
    name = myindex1,myindex2
    field = email,city
    index_myindex1_partitions={"email":["5fffff", "7fffff"]}
    index_myindex2_partitions={"city":["5fffff", "7fffff"]}

    NOTE: two partition pivots above imply that we need 3 indexer nodes in the
    cluster spec.
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_debugstats': True,
                  'secondary_debugstats_bucket': True, 'secondary_debugstats_index': True}

    def __init__(self, *args):
        super(SecondaryIndexTest, self).__init__(*args)

        """self.test_config.secondaryindex_settings"""
        self.secondaryindex_settings = None
        self.index_nodes = None
        self.index_fields = None
        self.bucket = None
        self.indexes = []
        self.secondaryDB = None
        self.configfile = self.test_config.secondaryindex_settings.cbindexperf_configfile
        self.init_num_connections = self.test_config.secondaryindex_settings.init_num_connections
        self.step_num_connections = self.test_config.secondaryindex_settings.step_num_connections
        self.max_num_connections = self.test_config.secondaryindex_settings.max_num_connections

        if self.test_config.secondaryindex_settings.db == 'moi':
            self.secondaryDB = 'memdb'
        logger.info('secondary storage DB..{}'.format(self.secondaryDB))

        self.indexes = self.test_config.secondaryindex_settings.name.split(',')
        self.index_fields = self.test_config.secondaryindex_settings.field.split(",")

        # Get first cluster, its index nodes, and first bucket
        (cluster_name, servers) = \
            self.cluster_spec.yield_servers_by_role('index').next()
        if not servers:
            raise RuntimeError(
                "No index nodes specified for cluster {}".format(cluster_name))
        self.index_nodes = servers

        self.bucket = self.test_config.buckets[0]

        # Generate active index names that are used if there are partitions.
        # Else active_indexes is the same as indexes specified in test config
        self.active_indexes = self.indexes
        num_partitions = None
        for index, field_where in self._get_where_map().items():
            where_list = field_where.values().next()
            num_partitions = len(where_list)
            break
        if num_partitions:
            # overwrite with indexname_0, indexname_1 ... names for each partition
            self.active_indexes = []
            for index in self.indexes:
                for i in range(num_partitions):
                    index_i = index + "_{}".format(i)
                    self.active_indexes.append(index_i)

    def _get_where_map(self):
        """
        Given the following in test config:

        [secondary]
        name = myindex1,myindex2
        field = email,city
        index_myindex1_partitions={"email":["5fffff", "afffff"]}
        index_myindex2_partitions={"city":["5fffff", "afffff"]}

        returns something like the following, details omitted by "...":

        {
            "myindex1":
                {"email": [ 'email < "5fffff"', ... ] },
            "myindex2":
                {"city": [ ... , 'city >= "5fffff" and city < "afffff"', city >= "afffff" ]},
        }
        """
        result = {}
        # For each index/field, get the partition pivots in a friendly format.
        # Start with the (index_name, field) pair, find each field's
        # corresponding partition pivots. From the pivots, generate the (low,
        # high) endpoints that define a partition. Use None to represent
        # unbounded.
        for index_name, field in zip(self.indexes, self.index_fields):
            index_partition_name = "index_{}_partitions".format(index_name)
            # check that secondaryindex_settings.index_blah_partitions exists.
            if not hasattr(self.test_config.secondaryindex_settings,
                           index_partition_name):
                continue
            # Value of index_{}_partitions should be a string that resembles a
            # Python dict instead of a JSON due to the support for tuple as
            # keys. However, at the moment the same string can be interpretted
            # as either JSON or Python Dict.
            field_pivots = eval(getattr(self.test_config.secondaryindex_settings,
                                        index_partition_name))
            for field, pivots in field_pivots.items():
                pivots = [None] + pivots + [None]
                partitions = []
                for i in range(len(pivots) - 1):
                    partitions.append((pivots[i], pivots[i + 1]))
                if len(partitions) != len(self.index_nodes):
                    raise RuntimeError(
                        "Number of pivots in partitions should be one less" +
                        " than number of index nodes")

            # construct where clause
            where_list = []
            for (left, right) in partitions:
                where = None
                if left and right:
                    where = '\\\"{}\\\" <= {} and {} < \\\"{}\\\"'.format(
                        left, field, field, right)
                elif left:
                    where = '{} >= \\\"{}\\\"'.format(field, left)
                elif right:
                    where = '{} < \\\"{}\\\"'.format(field, right)
                where_list.append(where)

            if index_name not in result:
                result[index_name] = {}
            result[index_name][field] = where_list
        return result

    @with_stats
    def build_secondaryindex(self):
        return self._build_secondaryindex()

    def _build_secondaryindex(self):
        """call cbindex create command"""
        logger.info('building secondary index..')

        where_map = self._get_where_map()

        self.remote.build_secondary_index(
            self.index_nodes, self.bucket, self.indexes, self.index_fields,
            self.secondaryDB, where_map)
        time_elapsed = self.monitor.wait_for_secindex_init_build(self.index_nodes[0].split(':')[0],
                                                                 self.active_indexes)
        return time_elapsed

    def run_load_for_2i(self):
        if self.secondaryDB == 'memdb':
            load_settings = self.test_config.load_settings
            self.remote.run_spring_on_kv(ls=load_settings)
        else:
            self.load()

    def run_access_for_2i(self, run_in_background=False):
        if self.secondaryDB == 'memdb':
            access_settings = self.test_config.access_settings
            self.remote.run_spring_on_kv(ls=access_settings, silent=run_in_background)
        else:
            if run_in_background:
                self.access_bg()
            else:
                self.access()

    @staticmethod
    def get_data_from_config_json(config_file_name):
        with open(config_file_name) as fh:
            return json.load(fh)

    def validate_num_connections(self):
        db = SerieslyStore.build_dbname(self.metric_helper.cluster_names[0], None, None, None, "secondary_debugstats")
        config_data = self.get_data_from_config_json(self.configfile)
        # Expecting one extra connection than concurrency in config file, MB-21584
        ret = self.metric_helper.verify_series_in_limits(db, config_data["Concurrency"] + 1, "num_connections")
        if not ret:
            raise Exception('Validation for num_connections failed')

    @with_stats
    def apply_scanworkload(self):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        logger.info('Initiating scan workload')

        cmdstr = "/opt/couchbase/bin/cbindexperf -cluster {} -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
                 "-statsfile /root/statsfile" \
            .format(self.index_nodes[0], rest_username, rest_password, self.configfile)
        logger.info('To be applied: {}'.format(cmdstr))
        status = subprocess.call(cmdstr, shell=True)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')


class InitialandIncrementalSecondaryIndexTest(SecondaryIndexTest):
    """
    The test measures time it takes to build index for the first time as well as
    incremental build. There is no disabling of index updates in incremental building,
    index updating is conurrent to KV incremental load.
    """

    def _report_kpi(self, time_elapsed, index_type):
        storage = self.secondaryDB and 'moi' or 'fdb'
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type=index_type,
                                                  storage=storage)
        )

    def build_initindex(self):
        self.build_secondaryindex()

    @with_stats
    def build_incrindex(self):
        access_settings = self.test_config.access_settings
        load_settings = self.test_config.load_settings
        if self.secondaryDB == 'memdb':
            self.remote.run_spring_on_kv(ls=access_settings)
        else:
            self.worker_manager.run_workload(access_settings, self.target_iterator)
            self.worker_manager.wait_for_workers()
        numitems = load_settings.items + access_settings.items
        self.monitor.wait_for_secindex_incr_build(self.index_nodes, self.bucket,
                                                  self.active_indexes, numitems)

    def run(self):
        self.run_load_for_2i()
        self.wait_for_persistence()
        self.compact_bucket()
        from_ts, to_ts = self.build_secondaryindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)
        self.report_kpi(time_elapsed, 'Initial')

        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Incremental secondary index', time_elapsed)
        self.report_kpi(time_elapsed, 'Incremental')


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
        self.run_load_for_2i()
        self.wait_for_persistence()
        self.compact_bucket()
        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.rebalance(initial_nodes[0], nodes_after[0])
        from_ts, to_ts = self.build_secondaryindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)
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
        self.run_load_for_2i()
        self.wait_for_persistence()
        self.compact_bucket()
        self.build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
        scan_thr, row_thr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scan_thr))
        logger.info('Rows throughput: {}'.format(row_thr))
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
        self.run_load_for_2i()
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
        self.run_load_for_2i()
        self.wait_for_persistence()
        self.compact_bucket()
        self.build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.apply_scanworkload()
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
        self.run_load_for_2i()
        self.wait_for_persistence()
        self.compact_bucket()
        nodes_after = [0]
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after[0] = initial_nodes[0] + 1
        self.build_secondaryindex()
        self.run_access_for_2i(run_in_background=True)
        self.rebalance(initial_nodes[0], nodes_after[0])
        self.apply_scanworkload()
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
        self.run_load_for_2i()
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
        self.run_load_for_2i()
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
        self.run_load_for_2i()
        self.wait_for_persistence()
        self.compact_bucket()
        self.build_secondaryindex()

        connections = self.apply_scanworkload_steps()
        logger.info('Connections: {}'.format(connections))

        self.report_kpi(connections)

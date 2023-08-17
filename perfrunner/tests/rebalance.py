import time

import dateutil.parser

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.local import build_sdk_benchmark
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import (
    pillowfight_data_load_task,
    sdks_benchmark_task,
)
from perfrunner.tests import PerfTest
from perfrunner.tests.fts import FTSTest
from perfrunner.tests.views import QueryTest
from perfrunner.tests.xdcr import DestTargetIterator, UniDirXdcrInitTest
from perfrunner.utils.terraform import CapellaTerraform
from perfrunner.workloads.sdks_bench import get_sdk_build_command


class RebalanceTest(PerfTest):

    """Implement methods required for rebalance management.

    See child classes for workflow details.

    Here is rebalance phase timeline:

    start stats collection ->
        sleep X minutes (observe pre-rebalance characteristics) ->
            trigger rebalance stopwatch ->
                start rebalance -> wait for rebalance to finish ->
            trigger rebalance stopwatch ->
        sleep X minutes (observe post-rebalance characteristics) ->
    stop stats collection.

    The timeline is implemented via a long chain of decorators.

    Actual rebalance steps depend on test scenario (e.g., basic rebalance or
    rebalance after graceful failover, and etc.).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0

    def is_balanced(self):
        for master in self.cluster_spec.masters:
            if self.rest.is_not_balanced(master):
                return False
        return True

    def monitor_progress(self, master):
        self.monitor.monitor_rebalance(master)

    def _report_kpi(self, *args):
        self.reporter.post(
            *self.metrics.rebalance_time(self.rebalance_time)
        )

    @timeit
    def _rebalance(self, services):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after = self.rebalance_settings.nodes_after
        swap = self.rebalance_settings.swap
        public_to_private_ip = self.cluster_spec.servers_public_to_private_ip

        for (_, servers), initial_nodes, nodes_after in zip(clusters,
                                                            initial_nodes,
                                                            nodes_after):
            master = servers[0]

            new_nodes = []
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []

            if nodes_after > initial_nodes:  # rebalance-in
                new_nodes = servers[initial_nodes:nodes_after]
                known_nodes = servers[:nodes_after]
            elif nodes_after < initial_nodes:  # rebalance-out
                ejected_nodes = servers[nodes_after:initial_nodes]
            elif swap:
                new_nodes = servers[initial_nodes:initial_nodes + swap]
                known_nodes = servers[:initial_nodes + swap]
                ejected_nodes = servers[initial_nodes - swap:initial_nodes]
            else:
                continue

            if self.cluster_spec.using_private_cluster_ips:
                new_nodes = [public_to_private_ip[node] for node in new_nodes]
            group_map = self.cluster_spec.server_group_map
            for node in new_nodes:
                if group_map:
                    self.rest.add_node_to_group(master, node, services=services,
                                                group=group_map[node])
                else:
                    self.rest.add_node(master, node, services=services)

            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor_progress(master)

    def pre_rebalance(self):
        """Execute additional steps before rebalance."""
        logger.info('Sleeping for {} seconds before taking actions'
                    .format(self.rebalance_settings.start_after))
        time.sleep(self.rebalance_settings.start_after)

    def post_rebalance(self):
        """Execute additional steps after rebalance."""
        logger.info('Sleeping for {} seconds before finishing'
                    .format(self.rebalance_settings.stop_after))
        time.sleep(self.rebalance_settings.stop_after)

    @with_stats
    @with_profiles
    def rebalance(self, services=None):
        self.pre_rebalance()
        self.rebalance_time = self._rebalance(services)
        self.post_rebalance()


class CapellaRebalanceTest(RebalanceTest):

    def update_cluster_configs(self) -> list[str]:
        new_clusters = self.rest.get_all_cluster_nodes()

        logger.info('Cluster nodes: {}'.format(pretty_dict(new_clusters)))

        for cluster_name, new_nodes in new_clusters.items():
            self.cluster_spec.config.set('clusters', cluster_name, '\n' + '\n'.join(new_nodes))

        self.cluster_spec.update_spec_file()

    def post_rebalance(self):
        super().post_rebalance()
        self.update_cluster_configs()

    @timeit
    def _rebalance(self, services):
        masters = self.cluster_spec.masters
        clusters_schemas = self.cluster_spec.clusters_schemas
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after = self.rebalance_settings.nodes_after
        swap = self.rebalance_settings.swap

        if swap:
            logger.info('Swap rebalance not available for Capella tests. Ignoring.')

        for master, (_, schemas), initial_nodes, nodes_after in zip(masters, clusters_schemas,
                                                                    initial_nodes, nodes_after):
            if initial_nodes != nodes_after:
                nodes_after_rebalance = schemas[:nodes_after]

                new_cluster_config = {
                    'specs': CapellaTerraform.construct_capella_server_groups(
                        self.cluster_spec, nodes_after_rebalance
                    )[0]
                }

                self.rest.update_cluster_configuration(master, new_cluster_config)
                self.monitor.wait_for_rebalance_to_begin(master)
                self.monitor_progress(master)


class RebalanceKVTest(RebalanceTest):

    ALL_HOSTNAMES = True

    COLLECTORS = {'latency': True}

    def post_rebalance(self):
        super().post_rebalance()
        self.worker_manager.abort_all_tasks()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.reset_kv_stats()

        self.access_bg()
        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class CapellaRebalanceKVTest(RebalanceKVTest, CapellaRebalanceTest):

    def post_rebalance(self):
        super().post_rebalance()
        self.worker_manager.abort_all_tasks()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class RebalanceKVCompactionTest(RebalanceKVTest):
    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.reset_kv_stats()

        self.access_bg()
        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class RebalanceDurabilityTest(RebalanceTest):

    ALL_HOSTNAMES = True

    COLLECTORS = {'latency': True}

    @with_stats
    @with_profiles
    def rebalance(self, services=None):
        self.access_bg()
        self.rebalance_time = self._rebalance(services)
        self.worker_manager.abort_all_tasks()

    def _report_kpi(self, *args):
        for operation in ('set', 'durable_set'):
            for metric in self.metrics.kv_latency(operation=operation, percentiles=[50.0, 99.9]):
                self.reporter.post(*metric)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.hot_load()
        self.reset_kv_stats()
        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class RebalanceForFTS(RebalanceTest, FTSTest):

    ALL_HOSTNAMES = True
    COLLECTORS = {'fts_stats': True, 'jts_stats': True}

    def build_indexes(self):
        elapsed_time = self.create_fts_indexes()
        return elapsed_time

    def run(self):
        # self.cleanup_and_restore()
        self.load()
        self.wait_for_persistence()
        fts_nodes_before = self.add_extra_fts_parameters()
        self.create_fts_index_definitions()
        logger.info("Sleeping for 10s before the index creation")
        time.sleep(10)
        total_index_time = self.build_indexes()
        logger.info("Total index build time: {} seconds".format(total_index_time))

        self.wait_for_index_persistence(fts_nodes=fts_nodes_before)

        total_index_size_bytes = self.calculate_index_size()
        logger.info("Total index size: {} MB".format(int(total_index_size_bytes / (1024 ** 2))))

        self.rebalance(services="fts")
        logger.info("Total rebalance time: {} seconds".format(self.rebalance_time))

        if self.is_balanced():
            self.report_kpi()


class RecoveryTest(RebalanceTest):

    def failover(self):
        self.pre_failover()
        self._failover()

    def _failover(self):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            master = servers[0]

            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            for node in failed:
                if self.rebalance_settings.failover == 'hard':
                    self.rest.fail_over(master, node)
                else:
                    self.rest.graceful_fail_over(master, node)
                    self.monitor_progress(master)
                self.rest.add_back(master, node)

            if self.rebalance_settings.delta_recovery:
                for node in failed:
                    self.rest.set_delta_recovery_type(master, node)

    def pre_failover(self):
        logger.info('Sleeping {} seconds before triggering failover'
                    .format(self.rebalance_settings.delay_before_failover))
        time.sleep(self.rebalance_settings.delay_before_failover)

    @timeit
    def _rebalance(self, *args):
        """Recover cluster after failover."""
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes

        for (_, servers), initial_nodes in zip(clusters, initial_nodes):
            master = servers[0]

            self.rest.rebalance(master,
                                known_nodes=servers[:initial_nodes],
                                ejected_nodes=[])
            self.monitor_progress(master)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.access_bg()

        self.failover()

        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class FailoverTest(RebalanceTest):

    @staticmethod
    def convert_time(time_str):
        t = dateutil.parser.parse(time_str, ignoretz=True)
        return float(t.strftime('%s.%f'))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Use selected ochestrator as the master node.
        self.master_node = self.rest.get_orchestrator_node(self.master_node)

    def _failover(self):
        pass

    @with_stats
    def failover(self):
        self.pre_rebalance()
        self._failover()
        self.post_rebalance()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.access_bg()
        self.failover()

        if self.is_balanced():
            self.report_kpi()


class HardFailoverTest(FailoverTest):

    def _report_kpi(self, *args):
        t_start = self.remote.detect_hard_failover_start(self.master_node)
        t_end = self.remote.detect_failover_end(self.master_node)

        if t_end and t_start:
            t_start = self.convert_time(t_start)
            t_end = self.convert_time(t_end)
            delta = int(1000 * (t_end - t_start))  # s -> ms
            self.reporter.post(
                *self.metrics.failover_time(delta)
            )

    def _failover(self, *args):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            master = servers[0]

            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            for node in failed:
                self.rest.fail_over(master, node)


class GracefulFailoverTest(FailoverTest):

    def _report_kpi(self, *args):
        t_start = self.remote.detect_graceful_failover_start(self.master_node)
        t_end = self.remote.detect_failover_end(self.master_node)

        if t_end and t_start:
            t_start = self.convert_time(t_start)
            t_end = self.convert_time(t_end)
            delta = int(1000 * (t_end - t_start))  # s -> ms
            self.reporter.post(
                *self.metrics.failover_time(delta)
            )

    def _failover(self, *args):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            master = servers[0]

            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            for node in failed:
                self.rest.graceful_fail_over(master, node)
                self.monitor_progress(master)


class AutoFailoverTest(FailoverTest):

    def _report_kpi(self, *args):
        t_start = self.remote.detect_hard_failover_start(self.master_node)
        t_end = self.remote.detect_failover_end(self.master_node)

        if t_end and t_start:
            t_start = self.convert_time(t_start)
            t_end = self.convert_time(t_end)
            delta = int(1000 * (t_end - t_start))  # s -> ms
            self.reporter.post(
                *self.metrics.failover_time(delta)
            )

    def _failover(self, *args):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            for node in failed:
                self.remote.shutdown(node)


class FailureDetectionTest(FailoverTest):

    def _report_kpi(self, *args):
        t_failover = self.remote.detect_auto_failover(self.master_node)

        if t_failover:
            t_failover = self.convert_time(t_failover)
            delta = round(t_failover - self.t_failure, 1)
            self.reporter.post(
                *self.metrics.failover_time(delta)
            )

    def _failover(self, *args):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            self.t_failure = time.time()
            for node in failed:
                self.remote.shutdown(node)


class AutoFailoverAndFailureDetectionTest(FailoverTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.t_failure = None

    def _report_kpi(self, *args):
        # Failover time
        time_map = self.get_rebalance_timings()
        failover_time = time_map.get('failover')  # Make sure the report measured failover
        if failover_time and (total_time := time_map.get('total_time')):
            self.reporter.post(
                *self.metrics.autofailover_time(total_time * 1000)  # ms
            )

        # Failure detection time
        t_failure_detection = self.remote.detect_auto_failover(self.master_node)
        if t_failure_detection and self.t_failure:
            t_failure_detection = self.convert_time(t_failure_detection)
            failure_detection_delta = round(t_failure_detection - self.t_failure, 1)  # sec
            self.reporter.post(
                *self.metrics.failure_detection_time(failure_detection_delta)
            )


class DiskFailureDetectionTest(FailureDetectionTest):

    def _failover(self, *args):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            self.t_failure = time.time()
            for node in failed:
                self.remote.set_write_permissions("0444", node, self.cluster_spec.data_path)

    def restore(self, *args):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            for node in servers:
                self.remote.set_write_permissions("0755", node, self.cluster_spec.data_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.restore()
        super().__exit__(exc_type, exc_val, exc_tb)


class MemcachedFailureDetectionTest(AutoFailoverAndFailureDetectionTest):

    def _failover(self, *args):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            self.t_failure = time.time()
            for node in failed:
                self.remote.kill_memcached(node)


class RebalanceWithQueriesTest(RebalanceTest, QueryTest):

    def access_bg(self, *args):
        settings = self.test_config.access_settings
        settings.ddocs = self.ddocs

        super().access_bg(settings=settings)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.define_ddocs()
        self.build_index()

        self.access_bg()
        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class RebalanceWithXdcrInitTest(RebalanceTest, UniDirXdcrInitTest):

    def load_dest(self):
        if self.test_config.cluster.initial_nodes[1] == \
                self.rebalance_settings.nodes_after[1]:
            return

        dest_target_iterator = DestTargetIterator(self.cluster_spec,
                                                  self.test_config)
        PerfTest.load(self, target_iterator=dest_target_iterator)

    def rebalance(self, *args):
        self._rebalance(services=None)

    def check_rebalance(self):
        pass

    def monitor_progress(self, *args):
        pass

    def _report_kpi(self, time_elapsed):
        UniDirXdcrInitTest._report_kpi(self, time_elapsed)

    def run(self):
        self.load()
        self.load_dest()

        self.wait_for_persistence()

        self.rebalance()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class RebalanceLoadOnlyTest(RebalanceTest):

    ALL_HOSTNAMES = True

    def run(self):
        self.load()

        self.wait_for_persistence()

        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class RebalanceMultiBucketKVTest(RebalanceKVTest):

    ALL_BUCKETS = True

    COLLECTORS = {
        'iostat': True,
        'memory': True,
        'ns_server_system': True,
        'latency': True}


class OnlineMigrationWithRebalanceTest(RebalanceKVTest):

    """Do only a single node migration to monitor isolated migration Metrics."""

    def pre_rebalance(self):
        # Update storage backend for each bucket to magma or couchstore based
        # on the initial set storage backend
        current_storage_backend = self.test_config.bucket.backend_storage
        new_storage_backend = 'magma'
        if current_storage_backend and current_storage_backend == 'magma':
            new_storage_backend = 'couchstore'
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                logger.info('Storage backend before migration: {}'.format(
                    self.rest.get_bucket_storage_backend_info(master, bucket)))
                self.rest.update_bucket_storage_backend(master, bucket, new_storage_backend)
        return super().pre_rebalance()

    def post_rebalance(self):
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                logger.info('Storage backend after rebalance: {}'.format(
                    self.rest.get_bucket_storage_backend_info(master, bucket)))
        return super().post_rebalance()

    @with_stats
    @with_profiles
    def rebalance(self, services=None):
        self.pre_rebalance()
        self.rebalance_time = self._rebalance(services)
        self.post_rebalance()

    def _report_kpi(self, *args):
        self.reporter.post(
            *self.metrics.elapsed_time(self.rebalance_time)
        )


class OnlineMigrationDurabilityTest(OnlineMigrationWithRebalanceTest):

    ALL_HOSTNAMES = True

    COLLECTORS = {'latency': True}

    def _report_kpi(self, *args):
        for operation in ('set', 'durable_set'):
            for metric in self.metrics.kv_latency(operation=operation, percentiles=[50.0, 99.9]):
                self.reporter.post(*metric)

    @with_stats
    @with_profiles
    def rebalance(self, services=None):
        self.access_bg()
        self.pre_rebalance()
        self.rebalance_time = self._rebalance(services)
        self.post_rebalance()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.hot_load()
        self.reset_kv_stats()
        self.rebalance()

        if self.is_balanced():
            self.report_kpi()


class FailoverSDKConfigPushTest(FailoverTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sdk_type = self.test_config.sdktesting_settings.sdk_type[-1]
        self.benchmark_name = self.test_config.sdktesting_settings.benchmark_name
        self.sdk_version = self.test_config.client_settings.sdk_version

    def get_keys_from_node_to_fail(self) -> str:
        """Return a string of keys separated by comma from the nodes to be failed."""
        keys = []
        key_count = self.test_config.access_settings.items // len(self.nodes_to_fail)
        for node in self.nodes_to_fail:
            node_keys = self.rest.get_random_local_key(node, self.test_config.buckets[0],
                                                       keys_count=key_count)
            logger.info('Node: {} keys: "{}"'.format(node, node_keys))
            keys.extend(node_keys)
        return ','.join(keys)

    def setup_benchmark(self):
        """Perform any necessary build of the benchmarks as needed."""
        build_cmd = get_sdk_build_command(self.benchmark_name, self.sdk_type, self.sdk_version)
        build_sdk_benchmark(self.benchmark_name, self.sdk_type, build_cmd)

    def _failover(self, *args):
        for node in self.nodes_to_fail:
            logger.info('Failing node: {}'.format(node))
            self.remote.shutdown(node)
        # Fof this usecase start the failure time after the shutdown command is sent.
        # Works for single-node failure tests, need improvements for multi-node failure tests.
        # Also keep a record of epoch and ns times, the later used by Python SDK runs
        self.t_failure = (time.time(), time.perf_counter_ns())

    def _get_nodes_to_fail(self):
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes
        self.nodes_to_fail = []

        for (_, servers), initial_nodes in zip(clusters, initial_nodes):
            self.nodes_to_fail.extend(servers[initial_nodes - failed_nodes:initial_nodes])

    def _report_kpi(self, *args):
        # Failover time
        failover_time = None
        time_map = self.get_rebalance_timings()
        if time_map.get('failover'):  # Make sure the report measured failover
            failover_time = round(time_map.get('total_time'), 2)  # sec

        # Failure detection time
        failure_detection_time = None
        t_failure_detection = self.remote.detect_auto_failover(self.master_node)
        if t_failure_detection:
            t_failure_detection = self.convert_time(t_failure_detection)
            failure_detection_time = round(t_failure_detection - self.t_failure[0], 2)  # sec

        logger.info('Failover time: {}s Failure detection time: {}s'.format(
            failover_time, failure_detection_time))

        # SDK write unavailable time
        self.reporter.post(
            *self.metrics.sdk_bench_config_push_time(self.t_failure, self.benchmark_name,
                                                     self.convert_time)
        )

    def access_bg(self):
        # Add custom configuration from different settings
        workload_setting = self.test_config.access_settings
        workload_setting.benchmark_name = self.benchmark_name
        workload_setting.sdk_type = self.sdk_type
        workload_setting.sdk_timeout = self.test_config.sdktesting_settings.sdk_timeout
        workload_setting.config_poll_interval = \
            self.test_config.sdktesting_settings.config_poll_interval
        workload_setting.keys = self.get_keys_from_node_to_fail()
        workload_setting.time = self.rebalance_settings.start_after \
            + self.rebalance_settings.stop_after
        return super().access_bg(task=sdks_benchmark_task, settings=workload_setting)

    def run(self):
        if self.sdk_type == 'python':
            self.load()  # We dont want to install pillowfight, so use spring to load data
        else:
            self.load(task=pillowfight_data_load_task)
        self.wait_for_persistence()

        self.setup_benchmark()
        # For this test we need to know the node we will be failing before hand
        # so we can access local vBuckets and documents
        self._get_nodes_to_fail()
        self.access_bg()
        self.failover()

        if self.is_balanced():
            self.report_kpi()

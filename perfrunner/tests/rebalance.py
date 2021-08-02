import time

import dateutil.parser

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.profiler import with_profiles
from perfrunner.tests import PerfTest
from perfrunner.tests.fts import FTSTest
from perfrunner.tests.views import QueryTest
from perfrunner.tests.xdcr import DestTargetIterator, UniDirXdcrInitTest


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

            for node in new_nodes:
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


class RebalanceKVTest(RebalanceTest):

    ALL_HOSTNAMES = True

    COLLECTORS = {'latency': True}

    def post_rebalance(self):
        super().post_rebalance()
        self.worker_manager.abort()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.reset_kv_stats()

        self.access_bg()
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
        self.worker_manager.abort()

    def _report_kpi(self, *args):
        for percentile in 50.00, 99.9:
            self.reporter.post(
                *self.metrics.kv_latency(operation='set', percentile=percentile)
            )

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

    def _failover(self):
        pass

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

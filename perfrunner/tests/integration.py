import time
from typing import Any, Dict

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.profiler import with_profiles
from perfrunner.tests import PerfTest
from perfrunner.tests.fts import FTSLatencyLoadTest
from perfrunner.tests.n1ql import N1QLElixirThroughputTest
from perfrunner.tests.rebalance import CapellaRebalanceTest
from perfrunner.tests.xdcr import CapellaXdcrTest, SrcTargetIterator


class EndToEndLatencyTest(N1QLElixirThroughputTest):
    ALL_BUCKETS = False

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'ns_server_system': True,
        'latency': True
    }

    def create_indexes_with_statement_only(self):
        logger.info('Creating and building indexes uing N1ql statements only')

        create_statements = []
        build_statements = []
        for statement in self.test_config.index_settings.statements:
            if statement.split()[0].upper() == 'CREATE':
                create_statements.append(statement)
            elif statement.split()[0].upper() == 'BUILD':
                build_statements.append(statement)
            else:
                logger.info("Something is wrong with {}".format(statement))

        for statement in create_statements:
            logger.info('Creating index: ' + statement)
            self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            cont = False
            while not cont:
                building = 0
                index_status = self.rest.get_index_status(self.index_node)
                index_list = index_status['status']
                for index in index_list:
                    if index['status'] != "Ready" and index['status'] != "Created":
                        building += 1
                if building < 10:
                    cont = True
                else:
                    time.sleep(10)

        for statement in build_statements:
            logger.info('Building index: ' + statement)
            self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            cont = False
            while not cont:
                building = 0
                index_status = self.rest.get_index_status(self.index_node)
                index_list = index_status['status']
                for index in index_list:
                    if index['status'] != "Ready" and index['status'] != "Created":
                        building += 1
                if building < 10:
                    cont = True
                else:
                    time.sleep(10)

        logger.info('Index Create and Build Complete')

    def report_kv_kpi(self):
        for operation in ('get', 'set'):
            for percentile in self.test_config.access_settings.latency_percentiles:
                self.reporter.post(
                    *self.metrics.kv_latency(operation=operation, percentile=percentile)
                )

    def report_n1ql_kpi(self):
        for percentile in self.test_config.access_settings.latency_percentiles:
            self.reporter.post(*self.metrics.query_latency(percentile=percentile))

    def report_index_kpi(self, index: Dict[str, Any]):
        self.reporter.post(
            *self.metrics.get_indexing_meta(value=index["time"],
                                            index_type=index["type"],
                                            unit=index["unit"])
        )

    def enable_stats(self, n1ql: bool = False, xdcr: bool = False):
        if self.index_nodes:
            if not hasattr(self, 'ALL_BUCKETS'):
                self.COLLECTORS.update({
                    'secondary_debugstats': True,
                    'secondary_debugstats_index': True,
                    'secondary_stats': True
                })
        if n1ql:
            self.COLLECTORS.update({
                'n1ql_latency': True,
                'n1ql_stats': True
            })
        if xdcr:
            self.COLLECTORS.update({
                'xdcr_lag': True,
                'xdcr_stats': True
            })

    @with_stats
    def load_with_stats(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

    @with_stats
    @timeit
    def create_indexes_with_stats(self):
        self.create_indexes()
        self.wait_for_indexing()

    @with_stats
    def access(self, kv: bool, n1ql: bool):
        if kv:
            # If N1QL is not enabled, start KV in foreground, otherwise start in background
            kv_access_method = PerfTest.access_bg if n1ql else PerfTest.access
            kv_settings = self.test_config.access_settings
            kv_settings.n1ql_workers = 0
            kv_access_method(self, settings=kv_settings)

        if n1ql:
            # Start N1QL in foreground
            n1ql_settings = self.test_config.access_settings
            n1ql_settings.workers = 0
            PerfTest.access(self, settings=n1ql_settings)

    def run(self):
        access_settings = self.test_config.access_settings
        kv = bool(access_settings.workers)
        n1ql = bool(access_settings.n1ql_workers)

        self.load_with_stats()

        if n1ql:
            index_build_time = self.create_indexes_with_stats()
            logger.info("index build completed in {} sec".format(index_build_time))
            index_meta = {"time": index_build_time, "type": "initial", "unit": "min"}
            self.report_index_kpi(index_meta)
            self.store_plans()

        self.enable_stats(n1ql=n1ql)

        self.access(kv, n1ql)

        if kv:
            self.report_kv_kpi()
        if n1ql:
            self.report_n1ql_kpi()


class EndToEndThroughputTest(EndToEndLatencyTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'ns_server_system': True,
    }

    def report_kv_kpi(self):
        if self.test_config.access_settings.throughput < float('inf'):
            logger.warn('KV throughput was throttled. Not reporting KV throughput.')
            return

        self.reporter.post(*self.metrics.avg_ops())

    def report_n1ql_kpi(self):
        if self.test_config.access_settings.n1ql_throughput < float('inf'):
            logger.warn('N1QL throughput was throttled. Not reporting N1QL throughput.')
            return

        self.reporter.post(*self.metrics.avg_n1ql_throughput(self.master_node))


class EndToEndRebalanceLatencyTest(EndToEndLatencyTest, CapellaRebalanceTest):

    @with_stats
    @with_profiles
    def rebalance(self, services=None):
        self.pre_rebalance()
        self._rebalance(services)
        self.post_rebalance()
        self.rebalance_timings = self.get_rebalance_timings()
        logger.info('Rebalance completed in {} secs'.format(self.rebalance_timings['total_time']))

    def access_bg(self, kv: bool, n1ql: bool):
        if kv:
            # Start KV in background
            kv_settings = self.test_config.access_settings
            kv_settings.n1ql_workers = 0
            PerfTest.access_bg(self, settings=kv_settings)

        if n1ql:
            # Start N1QL in background
            n1ql_settings = self.test_config.access_settings
            n1ql_settings.workers = 0
            PerfTest.access_bg(self, settings=n1ql_settings)

    def report_rebalance_kpi(self):
        self.reporter.post(
            *self.metrics.rebalance_time(self.rebalance_timings['total_time'],
                                         update_category=True)
        )

    def run(self):
        access_settings = self.test_config.access_settings
        kv = bool(access_settings.workers)
        n1ql = bool(access_settings.n1ql_workers)

        self.load_with_stats()

        if n1ql:
            build_time = self.create_indexes_with_stats()
            logger.info("index build completed in {} sec".format(build_time))
            index_meta = {"time": build_time, "type": "initial", "unit": "min"}
            self.report_index_kpi(index_meta)
            self.store_plans()

        self.enable_stats(n1ql=n1ql)

        self.access_bg(kv, n1ql)
        self.rebalance(services=self.rebalance_settings.services)

        if kv:
            self.report_kv_kpi()
        if n1ql:
            self.report_n1ql_kpi()

        self.report_rebalance_kpi()


class EndToEndRebalanceThroughputTest(EndToEndThroughputTest, EndToEndRebalanceLatencyTest):

    def run(self):
        access_settings = self.test_config.access_settings
        kv = bool(access_settings.workers)
        n1ql = bool(access_settings.n1ql_workers)

        self.load_with_stats()

        if n1ql:
            build_time = self.create_indexes_with_stats()
            logger.info("index build completed in {} sec".format(build_time))
            index_meta = {"time": build_time, "type": "initial", "unit": "min"}
            self.report_index_kpi(index_meta)
            self.store_plans()

        self.enable_stats(n1ql=n1ql)

        self.access_bg(kv, n1ql)
        self.rebalance(services=self.rebalance_settings.services)

        if kv:
            self.report_kv_kpi()
        if n1ql:
            self.report_n1ql_kpi()

        self.report_rebalance_kpi()


class EndToEndLatencyWithXDCRTest(EndToEndLatencyTest, CapellaXdcrTest):

    @with_stats
    def create_indexes_with_stats(self) -> float:
        """Create indexes on all clusters, but only time index build on the first cluster."""
        query_nodes_per_cluster = self.cluster_spec.servers_by_cluster_and_role('n1ql')
        index_nodes_per_cluster = self.cluster_spec.servers_by_cluster_and_role('index')

        t0 = time.time()
        for cluster_query_nodes in query_nodes_per_cluster:
            self.create_indexes(query_node=cluster_query_nodes[0])

        # Wait for index build to complete on first cluster, and record time
        logger.info('Waiting for index build on primary cluster')
        self.wait_for_indexing(index_nodes=index_nodes_per_cluster[0])
        index_build_time = time.time() - t0
        logger.info("Index build completed in {} sec".format(index_build_time))

        # Wait for index build to complete on remaining clusters
        logger.info('Waiting for index build to complete on remaining clusters')
        remaining_index_nodes = [node for nodes in index_nodes_per_cluster[1:] for node in nodes]
        self.wait_for_indexing(index_nodes=remaining_index_nodes)

        return index_build_time

    def load(self):
        PerfTest.load(self, target_iterator=self.load_target_iterator)

    @with_stats
    def access(self, kv: bool, n1ql: bool):
        if kv:
            # If N1QL is not enabled, start KV in foreground, otherwise start in background
            kv_access_method = PerfTest.access_bg if n1ql else PerfTest.access
            kv_settings = self.test_config.access_settings
            kv_settings.n1ql_workers = 0
            kv_access_method(self, settings=kv_settings, target_iterator=self.load_target_iterator)

        if n1ql:
            # Start N1QL in foreground
            n1ql_settings = self.test_config.access_settings
            n1ql_settings.workers = 0
            PerfTest.access(self, settings=n1ql_settings, target_iterator=self.load_target_iterator)

    def report_kv_kpi(self):
        for operation in ('get', 'set'):
            for percentile in self.test_config.access_settings.latency_percentiles:
                self.reporter.post(
                    *self.metrics.kv_latency(operation=operation, percentile=percentile)
                )

    def report_n1ql_kpi(self):
        for percentile in self.test_config.access_settings.latency_percentiles:
            self.reporter.post(
                *self.metrics.query_latency(percentile=percentile)
            )

    def report_xdcr_kpi(self):
        self.reporter.post(*self.metrics.xdcr_lag())

    def run(self):
        access_settings = self.test_config.access_settings
        kv = bool(access_settings.workers)
        n1ql = bool(access_settings.n1ql_workers)

        self.load_target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config,
                                                      prefix='symmetric')

        self.create_replications(
            num_xdcr_links=self.test_config.xdcr_settings.num_xdcr_links,
            xdcr_links_priority=self.test_config.xdcr_settings.xdcr_links_priority,
            xdcr_link_directions=self.test_config.xdcr_settings.xdcr_link_directions,
            xdcr_link_network_limits=self.test_config.xdcr_settings.xdcr_link_network_limits
        )

        self.load_with_stats()

        if n1ql:
            index_build_time = self.create_indexes_with_stats()
            index_meta = {"time": index_build_time, "type": "initial", "unit": "min"}
            self.report_index_kpi(index_meta)
            self.store_plans()

        self.monitor_replication()
        self.wait_for_persistence()

        self.enable_stats(n1ql=n1ql, xdcr=True)

        self.access(kv, n1ql)

        if kv:
            self.report_kv_kpi()
        if n1ql:
            self.report_n1ql_kpi()

        self.report_xdcr_kpi()


class EndToEndThroughputWithXDCRTest(EndToEndLatencyWithXDCRTest):

    def report_kv_kpi(self):
        for i, _ in enumerate(self.cluster_spec.masters):
            self.reporter.post(
                *self.metrics.avg_ops(cluster_idx=i)
            )

    def report_n1ql_kpi(self):
        for master in self.cluster_spec.masters:
            self.reporter.post(
                *self.metrics.avg_n1ql_throughput(master_node=master)
            )


class EndToEndRebalanceLatencyWithXDCRTest(EndToEndLatencyWithXDCRTest,
                                           EndToEndRebalanceLatencyTest):

    def run(self):
        access_settings = self.test_config.access_settings
        kv = bool(access_settings.workers)
        n1ql = bool(access_settings.n1ql_workers)

        self.load_target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config,
                                                      prefix='symmetric')

        self.create_replications(
            num_xdcr_links=self.test_config.xdcr_settings.num_xdcr_links,
            xdcr_links_priority=self.test_config.xdcr_settings.xdcr_links_priority,
            xdcr_link_directions=self.test_config.xdcr_settings.xdcr_link_directions,
            xdcr_link_network_limits=self.test_config.xdcr_settings.xdcr_link_network_limits
        )

        self.load_with_stats()

        if n1ql:
            index_build_time = self.create_indexes_with_stats()
            index_meta = {"time": index_build_time, "type": "initial", "unit": "min"}
            self.report_index_kpi(index_meta)
            self.store_plans()

        self.monitor_replication()
        self.wait_for_persistence()

        self.enable_stats(n1ql=n1ql, xdcr=True)

        self.access_bg(kv, n1ql)
        self.rebalance(services=self.rebalance_settings.services)

        if kv:
            self.report_kv_kpi()
        if n1ql:
            self.report_n1ql_kpi()

        self.report_rebalance_kpi()
        self.report_xdcr_kpi()


class EndToEndFTSLatencyTest(EndToEndLatencyTest, FTSLatencyLoadTest):
    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'ns_server_system': True,
        'latency': True,
        'jts_stats': True,
        'fts_stats': True
    }

    def _report_kpi(self, index=None, n1ql=False):
        if index:
            self.reporter.post(
                *self.metrics.get_indexing_meta(value=index["time"],
                                                index_type=index["type"],
                                                unit=index["unit"])
            )
        if n1ql:
            for percentile in self.test_config.access_settings.latency_percentiles:
                self.reporter.post(
                    *self.metrics.query_latency(percentile=percentile)
                )

    def report_kv_kpi(self):
        for operation in ('get', 'set'):
            for percentile in self.test_config.access_settings.latency_percentiles:
                self.reporter.post(
                    *self.metrics.kv_latency(operation=operation, percentile=percentile)
                )

    def enable_stats(self):
        if self.index_nodes:
            if not hasattr(self, 'ALL_BUCKETS'):
                self.COLLECTORS['secondary_debugstats'] = True
                self.COLLECTORS['secondary_debugstats_index'] = True
                self.COLLECTORS['secondary_stats'] = True
        if "latency" in self.__class__.__name__.lower():
            self.COLLECTORS['n1ql_latency'] = True

    @with_stats
    def load_with_stats(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

    @with_stats
    @timeit
    def create_indexes_with_stats(self):
        self.create_indexes()
        self.wait_for_indexing()

    def access_n1ql_bg(self, *args):
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        PerfTest.access_bg(self, settings=access_settings)

    def report_fts_kpi(self):
        self.reporter.post(*self.metrics.jts_latency(percentile=80))
        self.reporter.post(*self.metrics.jts_latency(percentile=95))

    def run(self):
        self.load_with_stats()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        build_time = self.create_indexes_with_stats()
        logger.info("index build completed in {} sec".format(build_time))
        index_meta = {"time": build_time, "type": "initial", "unit": "min"}
        self._report_kpi(index_meta)
        self.enable_stats()
        self.store_plans()
        self.reset_kv_stats()
        self.warmup()
        self.access_bg()
        self.access_n1ql_bg()
        self.run_test()
        logger.info("Sleeping for 300 seconds to finish all tasks")
        time.sleep(300)
        self._report_kpi(n1ql=True)
        # self.report_kv_kpi()
        self.report_fts_kpi()


class EndToEndRebalanceLatencyTestWithStatementsOnly(EndToEndRebalanceLatencyTest):

    @with_stats
    @timeit
    def create_indexes_with_stats(self):
        self.create_indexes_with_statement_only()
        self.wait_for_indexing()

    def run(self):
        access_settings = self.test_config.access_settings
        kv = bool(access_settings.workers)
        n1ql = bool(access_settings.n1ql_workers)

        self.load_with_stats()

        if n1ql:
            build_time = self.create_indexes_with_stats()
            logger.info("index build completed in {} sec".format(build_time))
            index_meta = {"time": build_time, "type": "initial", "unit": "min"}
            self.report_index_kpi(index_meta)

        self.enable_stats(n1ql=n1ql)

        self.access_bg(kv, n1ql)
        self.rebalance(services=self.rebalance_settings.services)

        if kv:
            self.report_kv_kpi()
        if n1ql:
            self.report_n1ql_kpi()

        self.report_rebalance_kpi()

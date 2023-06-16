import re
import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.profiler import with_profiles
from perfrunner.tests import PerfTest
from perfrunner.tests.fts import FTSLatencyLoadTest
from perfrunner.tests.n1ql import N1QLElixirThroughputTest
from perfrunner.tests.rebalance import CapellaRebalanceTest
from perfrunner.utils.terraform import CapellaTerraform


class EndToEndLatencyTest(N1QLElixirThroughputTest):
    ALL_BUCKETS = False

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': True,
        'n1ql_stats': True,
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

    def access_bg_with_stats(self):
        access_settings = self.test_config.access_settings
        access_settings.n1ql_workers = 0
        access_settings.time = 86400
        PerfTest.access_bg(self, settings=access_settings)

    def run(self):
        self.load_with_stats()

        build_time = self.create_indexes_with_stats()
        logger.info("index build completed in {} sec".format(build_time))
        index_meta = {"time": build_time, "type": "initial", "unit": "min"}
        self.report_kpi(index_meta)
        self.enable_stats()
        self.store_plans()
        # self.reset_kv_stats()
        self.access_bg_with_stats()
        self.access()

        self.report_kpi(n1ql=True)
        # self.report_kv_kpi()


class EndToEndThroughTest(EndToEndLatencyTest):
    def _report_kpi(self, index=None, n1ql=False):
        if index:
            self.reporter.post(
                *self.metrics.get_indexing_meta(value=index["time"],
                                                index_type=index["type"],
                                                unit=index["unit"])
            )
        if n1ql:
            self.reporter.post(
                *self.metrics.avg_n1ql_throughput(self.master_node)
            )


class EndToEndRebalanceLatencyTest(EndToEndLatencyTest, CapellaRebalanceTest):

    def access(self, *args):
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        PerfTest.access_bg(self, settings=access_settings)

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

    def store_plans(self):
        logger.info('Storing query plans')
        for i, query in enumerate(self.test_config.access_settings.n1ql_queries):
            query_statement = query['statement']
            replace_target = "RAW_QUERY "
            query_statement = query_statement.replace(replace_target, "")
            query_context = None
            if self.test_config.collection.collection_map:
                for bucket in self.test_config.buckets:
                    if bucket in query_statement:
                        bucket_replaced = False
                        bucket_scopes = self.test_config.collection.collection_map[bucket]
                        for scope in bucket_scopes.keys():
                            if scope in query_statement:
                                for collection in bucket_scopes[scope].keys():
                                    if collection in query_statement:
                                        query_context = "default:`{}`.`{}`".format(bucket, scope)
                                        bucket_replaced = True
                                        break
                                if bucket_replaced:
                                    break
                        if bucket_replaced:
                            break
                        else:
                            raise Exception('No access target for bucket: {}'.format(bucket))
                logger.info("Grabbing plan for query: {}".format(query_statement))
                plan = self.rest.explain_n1ql_statement(self.query_nodes[0], query_statement,
                                                        query_context)
            else:
                if self.test_config.cluster.serverless_mode == 'enabled':
                    bucket = re.search(r' FROM ([^\s]+)', query['statement']).group(1)
                    query_context = 'default:{}.`_default`'.format(bucket)
                else:
                    query_context = None
                plan = self.rest.explain_n1ql_statement(self.query_nodes[0], query_statement,
                                                        query_context)
            with open('query_plan_{}.json'.format(i), 'w') as fh:
                fh.write(pretty_dict(plan))

    def rebalance(self, services=None):
        self.pre_rebalance()
        self.rebalance_time = self._rebalance(services)

    @with_stats
    @with_profiles
    def rebalance_with_bg_task_and_stats(self):
        self.access_bg_with_stats()
        self.rebalance(services=self.rebalance_settings.services)
        self.access()
        self.monitor_progress(self.cluster_spec.servers[0])
        self.post_rebalance()
        logger.info("All the worker task are {}".format(self.worker_manager.async_results))
        self.worker_manager.wait_for_workers()

    def run(self):
        self.load_with_stats()

        build_time = self.create_indexes_with_stats()
        logger.info("index build completed in {} sec".format(build_time))
        index_meta = {"time": build_time, "type": "initial", "unit": "min"}
        self.report_kpi(index_meta)
        self.enable_stats()
        self.store_plans()
        # self.reset_kv_stats()
        self.rebalance_with_bg_task_and_stats()
        rebalance_time = self.get_rebalance_timings()
        logger.info("rebalance completed in {} sec".format(rebalance_time["total_time"]))
        self.report_kpi(n1ql=True)
        self.reporter.post(
            *self.metrics.rebalance_time(rebalance_time["total_time"], update_category=True)
        )
        # self.report_kv_kpi()


class EndToEndRebalanceThroughputTest(EndToEndLatencyTest, CapellaRebalanceTest):

    def access(self, *args):
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        PerfTest.access_bg(self, settings=access_settings)

    def _report_kpi(self, index=None, n1ql=False):
        if index:
            self.reporter.post(
                *self.metrics.get_indexing_meta(value=index["time"],
                                                index_type=index["type"],
                                                unit=index["unit"])
            )
        if n1ql:
            self.reporter.post(
                *self.metrics.avg_n1ql_throughput(self.master_node)
            )

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

    def run(self):
        self.load_with_stats()

        build_time = self.create_indexes_with_stats()
        logger.info("index build completed in {} sec".format(build_time))
        index_meta = {"time": build_time, "type": "initial", "unit": "min"}
        self.report_kpi(index_meta)
        self.enable_stats()
        self.store_plans()

        self.access_bg_with_stats()
        self.rebalance(services=self.rebalance_settings.services)
        self.access()
        self.monitor_progress(self.cluster_spec.servers[0])
        rebalance_time = self.get_rebalance_timings()
        logger.info("rebalance completed in {} sec".format(rebalance_time["total_time"]))
        self.report_kpi(n1ql=True)
        self.reporter.post(
            *self.metrics.rebalance_time(rebalance_time["total_time"], update_category=True)
        )
        self.reporter.post(
            *self.metrics.avg_ops()
        )
        # self.report_kv_kpi()


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

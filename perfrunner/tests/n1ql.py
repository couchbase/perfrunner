import datetime
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import numpy as np

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import create_build_tuple, pretty_dict, run_aws_cli_command
from perfrunner.helpers.profiler import with_profiles
from perfrunner.settings import AccessSettings
from perfrunner.tests import PerfTest, TargetIterator
from perfrunner.tests.rebalance import (
    CapellaRebalanceTest,
    DynamicServiceRebalanceTest,
    RebalanceTest,
)
from perfrunner.tests.tools import CapellaSnapshotBackupRestoreTest
from perfrunner.utils.terraform import CapellaProvisionedDeployer


class N1QLTest(PerfTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'ns_server_system': True
    }

    def load(self, *args):
        """Create two data sets with different key prefixes.

        In order to run the N1QL tests we need to satisfy two contradicting
        requirements:
        * Fields should be changed so that the secondary indexes are being
        updated.
        * Fields remain the same (based on a deterministic random algorithm) so
        that we can query them.

        The following workaround was introduced:
        * 50% of documents are being randomly mutated. These documents are not
        used for queries.
        * 50% of documents remain unchanged. Only these documents are used for
        queries.
        """
        load_settings = self.test_config.load_settings
        load_settings.items //= 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        super().load(settings=load_settings, target_iterator=iterator)
        super().load(settings=load_settings)

    def access_bg(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.items //= 2
        access_settings.n1ql_workers = 0

        super().access_bg(settings=access_settings)

    @with_stats
    @with_profiles
    def access(self, *args):
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.items //= 2
        access_settings.workers = 0

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')

        super().access(settings=access_settings, target_iterator=iterator)

    def access_n1ql_bg(self, *args):
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.items //= 2
        access_settings.workers = 0

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')

        super().access_bg(settings=access_settings, target_iterator=iterator)

    def store_plans(self):
        logger.info('Storing query plans')
        for i, query in enumerate(self.test_config.access_settings.n1ql_queries):
            if self.test_config.collection.collection_map:
                query_statement = query['statement']
                if "TARGET_BUCKET" in query_statement:
                    for bucket in self.test_config.buckets:
                        bucket_replaced = False
                        bucket_scopes = self.test_config.collection.collection_map[bucket]
                        for scope in bucket_scopes.keys():
                            for collection in bucket_scopes[scope].keys():
                                if bucket_scopes[scope][collection]["access"] == 1:
                                    query_context = "default:`{}`.`{}`".format(bucket, scope)
                                    query_target = "{}.`{}`".format(query_context, collection)
                                    replace_target = "`TARGET_BUCKET`"
                                    query_statement = query_statement.\
                                        replace(replace_target, query_target)

                                    target_scope = "`{}`.`{}`".format(bucket, scope)
                                    replace_target = "`TARGET_SCOPE`"
                                    query_statement = query_statement.\
                                        replace(replace_target, target_scope)

                                    bucket_replaced = True
                                    break
                            if bucket_replaced:
                                break
                        if bucket_replaced:
                            break
                        else:
                            raise Exception('No access target for bucket: {}'
                                            .format(bucket))
                else:
                    for bucket in self.test_config.buckets:
                        if bucket in query_statement:
                            bucket_replaced = False
                            bucket_scopes = self.test_config.collection.collection_map[bucket]
                            for scope in bucket_scopes.keys():
                                for collection in bucket_scopes[scope].keys():
                                    if bucket_scopes[scope][collection]["access"] == 1:
                                        query_context = "default:`{}`.`{}`".format(bucket, scope)
                                        query_target = "{}.`{}`".format(query_context, collection)
                                        replace_target = "`{}`".format(bucket)
                                        query_statement = query_statement.\
                                            replace(replace_target, query_target)
                                        bucket_replaced = True
                                        break
                                if bucket_replaced:
                                    break
                            if bucket_replaced:
                                break
                            else:
                                raise Exception('No access target for bucket: {}'
                                                .format(bucket))
                logger.info("Grabbing plan for query: {}".format(query_statement))
                plan = self.rest.explain_n1ql_statement(self.query_nodes[0], query_statement,
                                                        query_context)
            else:
                # If we aren't using collections, we could be using couchbase <7 in which case
                # we can't use query context. Therefore, we will only use query context if we
                # really should, which is for a serverless test
                if self.test_config.cluster.serverless_mode == 'enabled':
                    bucket = re.search(r' FROM ([^\s]+)', query['statement']).group(1)
                    query_context = 'default:{}.`_default`'.format(bucket)
                else:
                    query_context = None
                plan = self.rest.explain_n1ql_statement(self.query_nodes[0], query['statement'],
                                                        query_context)
            with open('query_plan_{}.json'.format(i), 'w') as fh:
                fh.write(pretty_dict(plan))

    def enable_stats(self):
        if self.index_nodes:
            if not hasattr(self, 'ALL_BUCKETS'):
                self.COLLECTORS['secondary_debugstats'] = True
                self.COLLECTORS['secondary_debugstats_index'] = True
                self.COLLECTORS['secondary_stats'] = True
        if "latency" in self.__class__.__name__.lower():
            self.COLLECTORS['n1ql_latency'] = True

    def enable_query_awr(self):
        if self.test_config.cluster.enable_query_awr:
            bucket = self.test_config.cluster.query_awr_bucket
            scope = self.test_config.cluster.query_awr_scope
            collection = self.test_config.cluster.query_awr_collection
            location = f"{bucket}.{scope}.{collection}"
            statement = (f'update system:awr set location="{location}", '
                         f'interval="1m", threshold=0, enabled=true;')
            logger.info(f"Enabling query AWR: {statement}")
            self.rest.exec_n1ql_statement(self.query_nodes[0], statement)

    @with_stats
    def generate_query_awr_report(self, start_time: str, end_time: str):
        bucket = self.test_config.cluster.query_awr_bucket
        scope = self.test_config.cluster.query_awr_scope
        collection = self.test_config.cluster.query_awr_collection
        keyspace = f"{bucket}.{scope}.{collection}"
        self.remote.run_cbqueryreportgen_command(keyspace, start_time, end_time)

    def run(self):
        self.enable_stats()
        self.enable_query_awr()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        if self.test_config.users.num_users_per_bucket > 0:
            self.cluster.add_extra_rbac_users(self.test_config.users.num_users_per_bucket)

        query_awr_start_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        self.access_bg()
        self.access()
        query_awr_end_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        if self.test_config.cluster.enable_query_awr:
            time.sleep(60)
            logger.info("sleep 60 seconds before generating report")
            self.generate_query_awr_report(query_awr_start_time, query_awr_end_time)

        self.report_kpi()

    def get_n1ql_request_count_until_now(self) -> int:
        total_requests = 0
        for query_node in self.query_nodes:
            vitals = self.rest.get_query_stats(query_node)
            total_requests += vitals['requests.count']
        return total_requests


class N1QLLatencyTest(N1QLTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.query_latency(percentile=90)
        )


class N1QLLatencyRawStatementTest(N1QLLatencyTest):

    def _report_kpi(self):
        for percentile in self.test_config.access_settings.latency_percentiles:
            self.reporter.post(
                *self.metrics.query_latency(percentile=percentile)
            )

    def load(self):
        PerfTest.load(self)

    def create_indexes(self, query_node: Optional[str] = None, index_node: Optional[str] = None,
                       statements: Optional[list[str]] = None):


        query_node = query_node or self.query_nodes[0]
        index_node = index_node or self.index_nodes[0]

        create_statements = []
        build_statements = []


        for statement in statements or self.test_config.index_settings.statements:
            check_stmt = statement.replace(" ", "").upper()
            if 'CREATEINDEX' in check_stmt \
                    or 'CREATEPRIMARYINDEX' in check_stmt:
                create_statements.append(statement)
            elif 'CREATEVECTORINDEX' in check_stmt:
                create_statements.append(statement)
            elif 'BUILDINDEX' in check_stmt:
                build_statements.append(statement)

        queries = []
        for statement in create_statements:
            logger.info(f"Creating index: {statement}")
            queries.append(threading.Thread(target=self.execute_index,
                                            args=(statement, query_node, index_node)))

        for query in queries:
            query.start()

        for query in queries:
            query.join()

        queries = []
        for statement in build_statements:
            logger.info(f"Building index:{statement}")
            queries.append(threading.Thread(target=self.execute_index,
                                            args=(statement, query_node, index_node)))

        for query in queries:
            query.start()

        for query in queries:
            query.join()

        logger.info('Index Create and Build Complete')

    def execute_index(self, statement: str, query_node: Optional[str] = None,
                      index_node: Optional[str] = None):
        query_node = query_node or self.query_nodes[0]
        index_node = index_node or self.index_nodes[0]

        self.rest.exec_n1ql_statement(query_node, statement)
        cont = False
        while not cont:
            building = 0
            index_status = self.rest.get_index_status(index_node)
            index_list = index_status['status']
            for index in index_list:
                if index['status'] != "Ready" and index['status'] != "Created":
                    building += 1
            if building < 10:
                cont = True
            else:
                time.sleep(10)

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

    def access_bg(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.n1ql_workers = 0
        PerfTest.access_bg(self, settings=access_settings)

    @with_stats
    @with_profiles
    def access(self, *args):
        self.download_certificate()
        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        PerfTest.access(self, settings=access_settings)

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLLatencyRebalanceTest(N1QLLatencyTest):

    def is_balanced(self):
        for master in self.cluster_spec.masters:
            if not self.rest.is_balanced(master):
                return False
        return True

    def monitor_progress(self, master):
        self.monitor.monitor_rebalance(master)

    @timeit
    def _rebalance(self, initial_nodes):
        for _, servers in self.cluster_spec.clusters:
            master = servers[0]

            new_nodes = servers[initial_nodes:initial_nodes + 1]
            known_nodes = servers[:initial_nodes + 1]
            ejected_nodes = servers[1:2]

            for node in new_nodes:
                self.rest.add_node(master, node)
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor_progress(master)

    @with_stats
    @with_profiles
    def rebalance(self, initial_nodes):
        self.access_n1ql_bg()

        logger.info('Sleeping for 30 seconds before taking actions')
        time.sleep(30)

        self.rebalance_time = self._rebalance(initial_nodes)

        logger.info('Sleeping for 30 seconds before finishing')
        time.sleep(30)

        self.worker_manager.abort_all_tasks()

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        initial_nodes = self.test_config.cluster.initial_nodes
        self.rebalance(initial_nodes[0])

        if self.is_balanced():
            self.report_kpi()


class N1QLThroughputTest(N1QLTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'ns_server_system': True
    }

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput(self.master_node)
        )


class N1QLElixirThroughputTest(N1QLThroughputTest):

    ALL_BUCKETS = True

    def __init__(self, *args):
        super().__init__(*args)
        self.index_node = self.index_nodes[0]

    def load(self):
        PerfTest.load(self)

    def access_bg(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.n1ql_workers = 0
        PerfTest.access_bg(self, settings=access_settings)

    @with_stats
    @with_profiles
    def access(self, *args):
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        PerfTest.access(self, settings=access_settings)

    def create_indexes(self, query_node: Optional[str] = None, index_node: Optional[str] = None):
        logger.info('Creating and building indexes')

        query_node = query_node or self.query_nodes[0]
        index_node = index_node or self.index_node

        index_replicas = str(self.test_config.index_settings.replicas)
        bmap = {}
        mlen = 0
        for bucket in self.test_config.buckets:
            create_statements = []
            build_statements = []
            query_contexts = []
            build_query_contexts = []
            bucket_scopes = self.test_config.collection.collection_map[bucket]
            for scope in bucket_scopes.keys():
                for collection in bucket_scopes[scope].keys():
                    if bucket_scopes[scope][collection]["access"] == 1:
                        query_context = "default:`{}`.`{}`".format(bucket, scope)
                        index_target = "{}.`{}`".format(query_context, collection)
                        replace_target = "`TARGET_BUCKET`"
                        bindexes = ""
                        for statement in self.test_config.index_settings.statements:
                            index_name = statement.split()[2]
                            create_statement = statement.replace(replace_target, index_target)
                            create_statement = create_statement.replace('index_replicas',
                                                                        index_replicas)
                            query_contexts.append(query_context)
                            create_statements.append(create_statement)
                            if bindexes != "":
                                bindexes = bindexes + ", "
                            bindexes += "`" + index_name + "`"
                        build_statement = "BUILD INDEX ON default:`{}`.`{}`.`{}`({})".\
                            format(bucket, scope, collection, bindexes)
                        build_query_contexts.append(query_context)
                        build_statements.append(build_statement)

            bmap[bucket] = {
                "cs": create_statements,
                "csq": query_contexts,
                "bs": build_statements,
                "bsq": build_query_contexts
            }

            if mlen < len(create_statements):
                mlen = len(create_statements)

        create_statements = []
        build_statements = []
        query_contexts = []
        build_query_contexts = []
        for i in range(0, mlen):
            for bucket in bmap.keys():
                if i < len(bmap[bucket]["cs"]):
                    create_statements.append(bmap[bucket]["cs"][i])
                    query_contexts.append(bmap[bucket]["csq"][i])
                if i < len(bmap[bucket]["bs"]):
                    build_statements.append(bmap[bucket]["bs"][i])
                    build_query_contexts.append(bmap[bucket]["bsq"][i])

        for statement, query_context in zip(create_statements, query_contexts):
            logger.info('Creating index: ' + statement)
            self.rest.exec_n1ql_statement(query_node, statement, query_context)
            cont = False
            while not cont:
                building = 0
                index_status = self.rest.get_index_status(index_node)
                index_list = index_status['status']
                for index in index_list:
                    if index['status'] != "Ready" and index['status'] != "Created":
                        building += 1
                if building < 10:
                    cont = True
                else:
                    time.sleep(10)

        for statement, query_context in zip(build_statements, build_query_contexts):
            logger.info('Building index: ' + statement)
            self.rest.exec_n1ql_statement(query_node, statement, query_context)
            cont = False
            while not cont:
                building = 0
                index_status = self.rest.get_index_status(index_node)
                index_list = index_status['status']
                for index in index_list:
                    if index['status'] != "Ready" and index['status'] != "Created":
                        building += 1
                if building < 10:
                    cont = True
                else:
                    time.sleep(10)

        logger.info('Index Create and Build Complete')

    def create_udf(self):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        count = 0
        for bucket in self.test_config.buckets:
            bucket_scopes = self.test_config.collection.collection_map[bucket]
            statement = "CREATE OR REPLACE FUNCTION `TARGET_SCOPE`.udf(id) LANGUAGE " \
                        "JAVASCRIPT AS 'function udf(id) { return id}';"
            for scope in bucket_scopes.keys():
                for collection in bucket_scopes[scope].keys():
                    if bucket_scopes[scope][collection]["access"] == 1:
                        target_scope = "default:`{}`.`{}`".format(bucket, scope)
                        replace_target = "`TARGET_SCOPE`"
                        statement = statement.replace(replace_target, target_scope)
                        logger.info('Creating UDF FUNCTION: ' + statement)
                        self.rest.exec_n1ql_statement(self.query_nodes[0], statement, target_scope)
                        break
            count += 1

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_indexes()
        self.wait_for_indexing()
        self.create_udf()

        self.store_plans()

        if self.test_config.users.num_users_per_bucket > 0:
            self.cluster.add_extra_rbac_users(self.test_config.users.num_users_per_bucket)

        if self.test_config.access_settings.reset_throttle_limit:
            self.rest.reset_serverless_throttle(self.master_node)

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLElixirLatencyTest(N1QLElixirThroughputTest):

    def _report_kpi(self):
        for percentile in self.test_config.access_settings.latency_percentiles:
            self.reporter.post(
                *self.metrics.query_latency(percentile=percentile)
            )


class N1QLThroughputRebalanceTest(N1QLThroughputTest):

    def _report_kpi(self, rebalance_time, total_requests):
        self.reporter.post(
            *self.metrics.avg_n1ql_rebalance_throughput(rebalance_time, total_requests)
        )

    def is_balanced(self):
        for master in self.cluster_spec.masters:
            if not self.rest.is_balanced(master):
                return False
        return True

    def monitor_progress(self, master):
        self.monitor.monitor_rebalance(master)

    @timeit
    def _rebalance(self, initial_nodes):
        for _, servers in self.cluster_spec.clusters:
            master = servers[0]

            new_nodes = servers[initial_nodes:initial_nodes + 1]
            known_nodes = servers[:initial_nodes + 1]
            ejected_nodes = servers[1:2]

            for node in new_nodes:
                self.rest.add_node(master, node)
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor_progress(master)

    @with_stats
    @with_profiles
    def rebalance(self, initial_nodes):
        self.access_n1ql_bg()

        logger.info('Sleeping for {} seconds before taking actions'
                    .format(self.test_config.rebalance_settings.start_after))
        time.sleep(self.test_config.rebalance_settings.start_after)
        total_requests_before = self.get_n1ql_request_count_until_now()
        rebalance_time = self._rebalance(initial_nodes)
        total_requests_after = self.get_n1ql_request_count_until_now()
        logger.info('Sleeping for {} seconds before finishing'
                    .format(self.test_config.rebalance_settings.stop_after))
        time.sleep(self.test_config.rebalance_settings.stop_after)

        total_requests = total_requests_after - total_requests_before

        self.worker_manager.abort_all_tasks()

        return rebalance_time, total_requests

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        initial_nodes = self.test_config.cluster.initial_nodes
        rebalance_time, total_requests = self.rebalance(initial_nodes[0])

        if self.is_balanced():
            self.report_kpi(rebalance_time, total_requests)


class N1QLJoinTest(N1QLTest):

    ALL_BUCKETS = True

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'ns_server_system': True
    }

    def load_regular(self, load_settings, target):
        load_settings.items //= 2
        super(N1QLTest, self).load(settings=load_settings,
                                   target_iterator=(target, ))
        target.prefix = 'n1ql'
        super(N1QLTest, self).load(settings=load_settings,
                                   target_iterator=(target, ))
        self.bucket_items[target.bucket] = load_settings.items*2

    def load_categories(self, load_settings, target):
        load_settings.items = load_settings.num_categories
        target.prefix = 'n1ql'
        super(N1QLTest, self).load(settings=load_settings,
                                   target_iterator=(target, ))
        self.bucket_items[target.bucket] = load_settings.items

    def load(self, *args):
        doc_gens = self.test_config.load_settings.doc_gen.split(',')
        for doc_gen, target in zip(doc_gens, self.target_iterator):
            load_settings = self.test_config.load_settings
            load_settings.doc_gen = doc_gen

            if doc_gen == 'ref':
                self.load_categories(load_settings, target)
            else:
                self.load_regular(load_settings, target)

    def access_bg(self, *args):
        doc_gens = self.test_config.load_settings.doc_gen.split(',')
        for doc_gen, target in zip(doc_gens, self.target_iterator):
            if doc_gen == 'ref':
                continue

            access_settings = self.test_config.access_settings
            access_settings.doc_gen = doc_gen
            access_settings.items //= 2
            access_settings.n1ql_workers = 0

            super(N1QLTest, self).access_bg(settings=access_settings,
                                            target_iterator=(target, ))

    @with_profiles
    @with_stats
    def access(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.items //= 2
        access_settings.workers = 0
        access_settings.buckets = self.test_config.buckets
        access_settings.doc_gen = self.test_config.access_settings.n1ql_gen

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')

        super(N1QLTest, self).access(settings=access_settings,
                                     target_iterator=iterator)

    def run(self):
        self.bucket_items = {}
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items(bucket_items=self.bucket_items)
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLJoinThroughputTest(N1QLJoinTest, N1QLThroughputTest):

    pass


class N1QLJoinLatencyTest(N1QLJoinTest, N1QLLatencyTest):

    pass


class N1QLBulkTest(N1QLTest):

    @with_stats
    @timeit
    def access(self, *args):
        statement = self.test_config.access_settings.n1ql_queries[0]['statement']
        statement_list = []
        if self.test_config.collection.collection_map:
            for bucket in self.test_config.buckets:
                if bucket in statement:
                    bucket_scopes = self.test_config.collection.collection_map[bucket]
                    for scope in bucket_scopes.keys():
                        for collection in bucket_scopes[scope].keys():
                            if bucket_scopes[scope][collection]["access"] == 1 \
                                    and bucket_scopes[scope][collection]["load"] == 1:
                                replace_target = "default:`{}`.`{}`.`{}`"\
                                    .format(bucket, scope, collection)
                                statement_with_coll = statement.\
                                    replace("`{}`".format(bucket), replace_target)
                                statement_list.append(statement_with_coll)
        else:
            statement_list.append(statement)
        if not statement_list:
            raise Exception('No statements to execute')
        for statement in statement_list:
            print("executing {}".format(statement))
            self.rest.exec_n1ql_statement(self.query_nodes[0], statement)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.bulk_n1ql_throughput(time_elapsed)
        )

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        time_elapsed = self.access()

        self.report_kpi(time_elapsed)


class N1QLDGMTest(PerfTest):

    COLLECTORS = {
        'n1ql_latency': True,
        'n1ql_stats': True,
        'net': False,
        'secondary_stats': True,
        'secondary_storage_stats': True,
    }

    def load(self, *args):
        PerfTest.load(self, *args)

    def access_bg(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.n1ql_workers = 0

        PerfTest.access_bg(self, settings=access_settings)

    @with_stats
    @with_profiles
    def access(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.workers = 0

        PerfTest.access(self, settings=access_settings)

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        if self.test_config.users.num_users_per_bucket > 0:
            self.cluster.add_extra_rbac_users(self.test_config.users.num_users_per_bucket)

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLDGMThroughputTest(N1QLDGMTest, N1QLThroughputTest):

    pass


class N1QLDGMLatencyTest(N1QLDGMTest, N1QLLatencyTest):

    pass


class N1QLXattrThroughputTest(N1QLThroughputTest):

    def xattr_load(self, *args, **kwargs):
        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        super().xattr_load()
        super().xattr_load(target_iterator=iterator)

    def run(self):
        self.enable_stats()
        self.load()
        self.xattr_load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLXattrThroughputRebalanceTest(N1QLXattrThroughputTest):

    def is_balanced(self):
        for master in self.cluster_spec.masters:
            if not self.rest.is_balanced(master):
                return False
        return True

    def monitor_progress(self, master):
        self.monitor.monitor_rebalance(master)

    @timeit
    def _rebalance(self, initial_nodes):
        for _, servers in self.cluster_spec.clusters:
            master = servers[0]

            new_nodes = servers[initial_nodes:initial_nodes + 1]
            known_nodes = servers[:initial_nodes + 1]
            ejected_nodes = servers[1:2]

            for node in new_nodes:
                self.rest.add_node(master, node)
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor_progress(master)

    @with_stats
    @with_profiles
    def rebalance(self, initial_nodes):
        self.access_n1ql_bg()

        logger.info('Sleeping for 30 seconds before taking actions')
        time.sleep(30)

        self.rebalance_time = self._rebalance(initial_nodes)

        logger.info('Sleeping for 30 seconds before finishing')
        time.sleep(30)

        self.worker_manager.abort_all_tasks()

    def run(self):
        self.enable_stats()
        self.load()
        self.xattr_load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        initial_nodes = self.test_config.cluster.initial_nodes
        self.rebalance(initial_nodes[0])

        if self.is_balanced():
            self.report_kpi()


class TpcDsTest(N1QLTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'net': False,
        'secondary_debugstats_index': False,
    }

    def restore_remote(self):
        self.remote.extract_cb_any(
            filename="couchbase", worker_home=self.worker_manager.WORKER_HOME
        )
        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)

        credential = local.read_aws_credential(self.test_config.backup_settings.aws_credential_path)
        self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()

        archive = self.test_config.restore_settings.backup_storage
        if self.test_config.restore_settings.modify_storage_dir_name:
            suffix_repo = "aws"
            if self.cluster_spec.capella_infrastructure:
                suffix_repo = self.cluster_spec.capella_backend
            archive += f"/{suffix_repo}"

        self.remote.restore(
            cluster_spec=self.cluster_spec,
            master_node=self.master_node,
            threads=self.test_config.restore_settings.threads,
            worker_home=self.worker_manager.WORKER_HOME,
            archive=archive,
            repo=self.test_config.restore_settings.backup_repo,
            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
            obj_region=self.test_config.backup_settings.obj_region,
            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
            use_tls=self.test_config.restore_settings.use_tls,
            map_data=self.test_config.restore_settings.map_data,
            encrypted=self.test_config.restore_settings.encrypted,
            passphrase=self.test_config.restore_settings.passphrase,
            env_vars=self.test_config.restore_settings.env_vars,
        )

    def run(self):
        self.enable_stats()
        self.enable_query_awr()
        if self.cluster_spec.cloud_infrastructure:
            self.restore_remote()
        else:
            self.load_tpcds_json_data()
        self.wait_for_persistence()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access()

        self.report_kpi()


class TpcDsLatencyTest(TpcDsTest, N1QLLatencyTest):

    pass


class TpcDsThroughputTest(TpcDsTest, N1QLThroughputTest):

    pass


class TpcDsIndexTest(TpcDsTest):

    COLLECTORS = {
        'memory': False,
        'net': False,
        'secondary_debugstats_index': False,
    }

    @with_stats
    @with_profiles
    @timeit
    def create_indexes(self):
        super().create_indexes()
        self.wait_for_indexing()

    def _report_kpi(self, indexing_time: float):
        self.reporter.post(
            *self.metrics.indexing_time(indexing_time)
        )

    def run(self):
        self.enable_stats()
        self.load_tpcds_json_data()
        self.wait_for_persistence()
        self.compact_bucket()

        time_elapsed = self.create_indexes()

        self.report_kpi(time_elapsed)


class BigFUNLatencyTest(N1QLLatencyTest):

    COLLECTORS = {
        'n1ql_latency': False,
        'n1ql_stats': True,
        'net': False,
        'secondary_debugstats_index': False,
    }

    def run(self):
        self.enable_stats()
        self.restore()
        self.wait_for_persistence()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.access()

        self.report_kpi()


class N1QLFunctionTest(N1QLTest):

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_functions()
        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLFunctionLatencyTest(N1QLFunctionTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.query_latency(percentile=90)
        )


class N1QLFunctionThroughputTest(N1QLFunctionTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput(self.master_node)
        )


class PytpccBenchmarkTest(N1QLTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'secondary_stats': True,
        'ns_server_system': True,
    }

    def download_pytpcc(self):
        branch = self.test_config.pytpcc_settings.pytpcc_branch
        repo = self.test_config.pytpcc_settings.pytpcc_repo
        local.download_pytppc(branch=branch, repo=repo)

    def pytpcc_create_collections(self):
        collection_config = self.test_config.pytpcc_settings.collection_config
        master_node = self.cluster.master_node

        local.pytpcc_create_collections(collection_config=collection_config,
                                        master_node=master_node)

    def pytpcc_create_indexes(self):
        master_node = self.cluster.master_node
        run_sql_shell = self.test_config.pytpcc_settings.run_sql_shell
        cbrindex_sql = self.test_config.pytpcc_settings.cbrindex_sql
        port = self.test_config.pytpcc_settings.query_port
        index_replica = self.test_config.pytpcc_settings.index_replicas

        local.pytpcc_create_indexes(master_node=master_node,
                                    run_sql_shell=run_sql_shell,
                                    cbrindex_sql=cbrindex_sql,
                                    port=port, index_replica=index_replica)

    def pytpcc_create_functions(self):
        master_node = self.cluster.master_node
        run_function_shell = self.test_config.pytpcc_settings.run_function_shell
        cbrfunction_sql = self.test_config.pytpcc_settings.cbrfunction_sql
        port = self.test_config.pytpcc_settings.query_port
        index_replica = self.test_config.pytpcc_settings.index_replicas

        local.pytpcc_create_functions(master_node=master_node,
                                      run_function_shell=run_function_shell,
                                      cbrfunction_sql=cbrfunction_sql,
                                      port=port, index_replica=index_replica)

    def load_tpcc(self):
        master_node = self.cluster.master_node
        warehouses = self.test_config.pytpcc_settings.warehouse
        client_threads = self.test_config.pytpcc_settings.client_threads
        query_port = self.test_config.pytpcc_settings.query_port
        multi_query_node = self.test_config.pytpcc_settings.multi_query_node
        driver = self.test_config.pytpcc_settings.driver
        nodes = self.cluster_spec.servers[:self.test_config.cluster.initial_nodes[0]]

        local.pytpcc_load_data(
            master_node=master_node,
            warehouses=warehouses,
            client_threads=client_threads,
            port=query_port,
            cluster_spec=self.cluster_spec,
            multi_query_node=multi_query_node,
            driver=driver,
            nodes=nodes,
        )

    @with_profiles
    @with_stats
    def run_tpcc(self):
        warehouses = self.test_config.pytpcc_settings.warehouse
        duration = self.test_config.pytpcc_settings.duration
        client_threads = self.test_config.pytpcc_settings.client_threads
        query_port = self.test_config.pytpcc_settings.query_port
        driver = self.test_config.pytpcc_settings.driver
        master_node = self.cluster.master_node
        multi_query_node = self.test_config.pytpcc_settings.multi_query_node
        nodes = self.cluster_spec.servers[:self.test_config.cluster.initial_nodes[0]]
        durability_level = self.test_config.pytpcc_settings.durability_level
        scan_consistency = self.test_config.pytpcc_settings.scan_consistency
        txtimeout = self.test_config.pytpcc_settings.txtimeout

        local.pytpcc_run_task(
            warehouses=warehouses,
            duration=duration,
            client_threads=client_threads,
            port=query_port,
            driver=driver,
            master_node=master_node,
            multi_query_node=multi_query_node,
            cluster_spec=self.cluster_spec,
            nodes=nodes,
            durability_level=durability_level,
            scan_consistency=scan_consistency,
            txtimeout=txtimeout,
        )

    def restore_pytpcc(self):
        master_node = self.cluster.master_node
        local.restore(master_node=master_node, cluster_spec=self.cluster_spec, threads=8)

    def _report_kpi(self):

        self.reporter.post(
            *self.metrics.pytpcc_tpmc_throughput(self.test_config.pytpcc_settings.duration)
        )

    def copy_pytpcc_run_output(self):
        local.copy_pytpcc_run_output()

    def run(self):

        self.download_pytpcc()

        self.pytpcc_create_collections()
        time.sleep(60)

        if self.test_config.pytpcc_settings.use_pytpcc_backup:
            self.restore_pytpcc()
            self.pytpcc_create_indexes()
            self.create_indexes()
            self.wait_for_indexing()
            self.pytpcc_create_functions()
        else:
            self.pytpcc_create_indexes()
            self.create_indexes()
            self.wait_for_indexing()
            self.pytpcc_create_functions()
            self.load_tpcc()
            self.wait_for_persistence()

        for service in self.test_config.profiling_settings.services:
            if service == 'n1ql':
                self.remote.kill_process_on_query_node("cbq-engine")
                logger.info('cbq-engine service restarting after loading docs')
                break

        if self.test_config.pytpcc_settings.txt_cleanup_window:
            cleanup_interval = self.test_config.pytpcc_settings.txt_cleanup_window
            self.remote.txn_query_cleanup(timeout=cleanup_interval)

        if self.test_config.pytpcc_settings.txt_cleanup_window:
            self.remote.txn_query_cleanup(
                timeout=self.test_config.pytpcc_settings.txt_cleanup_window)

        self.run_tpcc()
        self.copy_pytpcc_run_output()
        self._report_kpi()


class N1QLShutdownTest(N1QLTest):

    def hard_failover(self, *args):
        wait_time = self.test_config.access_settings.time // 4
        time.sleep(wait_time)
        master = self.cluster_spec.servers[0]
        failover_n1ql_node = self.cluster_spec.servers_by_role("n1ql")[-1]
        self.rest.fail_over(master, failover_n1ql_node)
        self.monitor.monitor_rebalance(master)
        initial_nodes = self.test_config.cluster.initial_nodes[0]
        known_nodes = self.cluster_spec.servers[:initial_nodes]
        self.rest.rebalance(master, known_nodes, [failover_n1ql_node])
        self.monitor.monitor_rebalance(master)
        self.worker_manager.wait_for_bg_tasks()

    def graceful_failover(self, *args):
        wait_time = self.test_config.access_settings.time // 4
        time.sleep(wait_time)
        master = self.cluster_spec.servers[0]
        failover_n1ql_node = self.cluster_spec.servers_by_role("n1ql")[-1]
        self.rest.graceful_fail_over(master, failover_n1ql_node)
        self.monitor.monitor_rebalance(master)
        initial_nodes = self.test_config.cluster.initial_nodes[0]
        known_nodes = self.cluster_spec.servers[:initial_nodes]
        self.rest.rebalance(master, known_nodes, [failover_n1ql_node])
        self.monitor.monitor_rebalance(master)
        self.worker_manager.wait_for_bg_tasks()

    def rebalance_out(self, *args):
        wait_time = self.test_config.access_settings.time // 4
        time.sleep(wait_time)
        master = self.cluster_spec.servers[0]
        initial_nodes = self.test_config.cluster.initial_nodes[0]
        known_nodes = self.cluster_spec.servers[:initial_nodes]
        eject_n1ql_node = self.cluster_spec.servers_by_role("n1ql")[-1]
        self.rest.rebalance(master, known_nodes, [eject_n1ql_node])
        self.monitor.monitor_rebalance(master)
        self.worker_manager.wait_for_bg_tasks()

    @with_stats
    @with_profiles
    def shutdown_n1ql(self):
        shutdown_type = self.test_config.access_settings.n1ql_shutdown_type
        if shutdown_type == "hard_failover":
            self.hard_failover()
        elif shutdown_type == "graceful_failover":
            self.graceful_failover()
        elif shutdown_type == "rebalance_out":
            self.rebalance_out()

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        if self.test_config.users.num_users_per_bucket > 0:
            self.cluster.add_extra_rbac_users(self.test_config.users.num_users_per_bucket)

        self.access_bg()
        self.access_n1ql_bg()
        self.shutdown_n1ql()

        self.report_kpi()


class N1QLLatencyShutdownTest(N1QLShutdownTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.query_latency(percentile=90)
        )


class N1QLThroughputShutdownTest(N1QLShutdownTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'ns_server_system': True
    }

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput(self.master_node)
        )


class TpcDsShutdownTest(N1QLShutdownTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'net': False,
        'secondary_debugstats_index': False,
    }

    def run(self):
        self.enable_stats()
        self.load_tpcds_json_data()
        self.wait_for_persistence()
        self.compact_bucket()
        self.create_indexes()
        self.wait_for_indexing()
        self.store_plans()
        self.access_bg()
        self.access_n1ql_bg()
        self.shutdown_n1ql()
        self.report_kpi()


class TpcDsLatencyShutdownTest(TpcDsShutdownTest, N1QLLatencyShutdownTest):

    pass


class TpcDsThroughputShutdownTest(TpcDsShutdownTest, N1QLThroughputShutdownTest):

    pass


class N1QLTimeSeriesThroughputTest(N1QLThroughputTest):

    def load(self):
        self.iterator = TargetIterator(self.cluster_spec, self.test_config,
                                       self.test_config.load_settings.key_prefix)
        load_settings = self.test_config.load_settings
        PerfTest.load(self, settings=load_settings, target_iterator=self.iterator)

    @with_stats
    @with_profiles
    def access(self, *args):
        self.iterator = TargetIterator(self.cluster_spec, self.test_config,
                                       self.test_config.load_settings.key_prefix)
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        PerfTest.access(self, settings=access_settings, target_iterator=self.iterator)

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access()

        self.report_kpi()


class N1QLTimeSeriesThroughputLoadTest(N1QLTimeSeriesThroughputTest):

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()


class N1QLTimeSeriesThroughputAccessTest(N1QLTimeSeriesThroughputTest):

    def run(self):
        self.enable_stats()
        self.access()

        self.report_kpi()


class N1QLLatencyRebalanceRawStatementTest(N1QLLatencyRawStatementTest, CapellaRebalanceTest):

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
                    "specs": CapellaProvisionedDeployer.construct_capella_server_groups(
                        self.cluster_spec, nodes_after_rebalance
                    )[0]
                }

                self.rest.update_cluster_configuration(master, new_cluster_config)
                self.monitor.wait_for_rebalance_to_begin(master)

    def run(self):
        self.enable_stats()
        self.load()
        self.wait_for_persistence()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access_bg()
        self.rebalance(services="index,n1ql")
        self.access()

        self.report_kpi()


class N1qlVectorSearchTest(N1QLLatencyRawStatementTest):
    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.index_type = str(self.test_config.gsi_settings.vector_index_type).lower()

    def cloud_restore(self):
        self.remote.extract_cb_any(filename='couchbase',
                                   worker_home=self.worker_manager.WORKER_HOME)
        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)

        credential = local.read_aws_credential(
            self.test_config.backup_settings.aws_credential_path, self.cloud_infra)
        self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()
        self.remote.delete_existing_staging_dir(self.test_config.backup_settings.obj_staging_dir)
        collection_map = self.test_config.collection.collection_map
        restore_mapping = self.test_config.restore_settings.map_data
        if restore_mapping is None and collection_map:
            for target in self.target_iterator:
                if not collection_map.get(
                        target.bucket, {}).get("_default", {}).get("_default", {}).get('load', 0):
                    restore_mapping = \
                        "{0}._default._default={0}.scope-1.collection-1"\
                        .format(target.bucket)
        archive = self.test_config.restore_settings.backup_storage
        if self.test_config.restore_settings.modify_storage_dir_name:
            suffix_repo = "aws"
            if self.cluster_spec.capella_infrastructure:
                suffix_repo = self.cluster_spec.capella_backend
            archive = archive + "/" + suffix_repo

        self.remote.restore(
            cluster_spec=self.cluster_spec,
            master_node=self.master_node,
            threads=self.test_config.restore_settings.threads,
            worker_home=self.worker_manager.WORKER_HOME,
            archive=archive,
            repo=self.test_config.restore_settings.backup_repo,
            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
            obj_region=self.test_config.backup_settings.obj_region,
            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
            use_tls=self.test_config.restore_settings.use_tls,
            map_data=restore_mapping,
            encrypted=self.test_config.restore_settings.encrypted,
            passphrase=self.test_config.restore_settings.passphrase,
            env_vars=self.test_config.restore_settings.env_vars,
        )
        self.wait_for_persistence()

    def vector_recall_and_accuracy_check(self):
        """
        Evaluate vector search recall and accuracy.

        Calculate recall and accuracy metrics for different probe values
        against ground‑truth data.
        """
        # Setup: Get configuration and test parameters
        query_node = self.query_nodes[0]
        gsi_settings = self.test_config.gsi_settings
        index_settings = self.test_config.index_settings
        query_map = index_settings.vector_query_map
        indexes = gsi_settings.indexes
        probes = gsi_settings.vector_scan_probes.split(",")

        # Load ground truth data: expected results for each query
        ground_truth = [x.split() for x in
                        open(self.test_config.index_settings.ground_truth_file_name,
                             'r').readlines()]

        # Initialize result containers for each probe value
        recalls = []
        accuracies = []

        # Additional configuration
        load_settings = self.test_config.load_settings
        vector_filter_percentage = gsi_settings.vector_filter_percentage
        k = int(index_settings.top_k_results)  # Number of top results to retrieve
        threshold = (
                        int(vector_filter_percentage) * load_settings.items / 100
                    )
        bucket, scope = next(iter(indexes.items()))
        scope, collections = next(iter(scope.items()))
        collection, _ = next(iter(collections.items()))
        similarity = f"'{gsi_settings.vector_similarity}'"
        index_def_prefix = gsi_settings.include_columns or gsi_settings.index_def_prefix
        def process_query(vector, truth, probe, is_first_query):
            """
            Process a single vector query and calculate recall/accuracy metrics.

            Args:
                vector: Input vector query string
                truth: Expected ground truth results
                probe: Number of probes to use for the search
                is_first_query: Whether this is the first query (for logging)

            Returns:
                tuple: (recall_score, accuracy_score)
            """
            # Extract index configuration from the nested structure
            # Parse the vector query (skip first 2 elements, get numeric values)
            # Build the N1QL query statement based on filter configuration
            query = [float(x) for x in vector.split()[2:]]

            if index_def_prefix == "id":
                # Use numeric threshold filter
                query_statement = (
                    f"SELECT meta().id FROM `{bucket}`.`{scope}`.`{collection}` "
                    f"WHERE {index_def_prefix} < {threshold} "
                    f"ORDER BY ANN_DISTANCE({gsi_settings.vector_def_prefix}, {query}, "
                    f"{similarity}, {probe}) LIMIT {k}"
                )
            elif not index_def_prefix:
                # No filter, pure vector search
                query_statement = (
                    f"SELECT meta().id FROM `{bucket}`.`{scope}`.`{collection}` "
                    f"ORDER BY ANN_DISTANCE({gsi_settings.vector_def_prefix}, {query}, "
                    f"{similarity}, {probe}) LIMIT {k}"
                )
            else:
                # Use categorical filter
                query_statement = (
                    f"SELECT meta().id FROM `{bucket}`.`{scope}`.`{collection}` "
                    f"WHERE {index_def_prefix} = 'eligible' "
                    f"ORDER BY ANN_DISTANCE({gsi_settings.vector_def_prefix}, {query}, "
                    f"{similarity}, {probe}) LIMIT {k}"
                )

            # Apply index-specific modifications
            if "bhive" == self.index_type:
                # Add reranking parameter for bhive index type
                query_statement = query_statement.replace(") LIMIT",
                    f", {str(gsi_settings.vector_reranking).lower()}) LIMIT")
            if gsi_settings.partition_by_clause == "brand":
                # Special case for brand-based partitioning
                query_statement = query_statement.replace('eligible', 'q')

            # Log the first query for debugging
            if is_first_query:
                logger.info(f"query_statements {query_statement}")

            # Execute the query and extract document IDs
            result = self.rest.exec_n1ql_statement(query_node, query_statement)
            ids = [result['id'] for result in result['results']]

            # Handle special ID format conversion (test-XXXX -> numeric)
            if 'test' in ids[0]:
                for index in range(len(ids)):
                    ids[index] = str(int(ids[index].split('-')[1].lstrip('0')) - 1)

            # Calculate metrics: recall and accuracy
            kk = min([k, len(ids), len(truth)]) # Use minimum available results
            common_ids = sum(x in ids[:kk] for x in truth[:kk]) / float(kk) # Recall: overlap ratio
            acc = int(ids[0] == truth[0]) # Accuracy: exact match of top result
            return common_ids, acc

        # Main evaluation loop: test each probe value
        for probe in probes:
            recall = []
            accuracy = []

            # Process all queries if query map is available
            if not query_map:
                break
            is_first_query = True

            # Use thread pool for parallel query execution
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = []

                # Submit all queries for parallel processing
                for vector, truth in zip(query_map, ground_truth):
                    futures.append(executor.submit(process_query, vector, truth, probe,
                                                    is_first_query))
                    is_first_query = False

                # Collect results as they complete
                for future in as_completed(futures):
                    r, a = future.result()
                    recall.append(r)
                    accuracy.append(a)

            # Calculate and log average metrics for this probe value
            accuracy_percentage = np.mean(accuracy) * 100 if accuracy else 0
            logger.info(f"accuracy percentage for probe {probe}: {accuracy_percentage}")
            recall_percentage = np.mean(recall) * 100 if recall else 0
            logger.info(f"recall percentage for probe {probe}: {recall_percentage}")

            # Store results for this probe
            recalls.append(recall_percentage)
            accuracies.append(accuracy_percentage)

        # Return all results: probe values and their corresponding metrics
        return probes, recalls, accuracies

    def downloads_ground_truth_file(self):
        ground_truth_s3_path = self.test_config.index_settings.ground_truth_s3_path
        ground_truth_file_name = self.test_config.index_settings.ground_truth_file_name
        run_aws_cli_command(
        f"s3 cp {ground_truth_s3_path+ground_truth_file_name} {ground_truth_file_name}",
        profile = "default")

    def report_kpi(self, probes, recalls, accuracies):
        k = int(self.test_config.index_settings.top_k_results)
        for probe, avg_recall, avg_accuracy in zip(probes, recalls, accuracies):
            self.reporter.post(
                *self.metrics.n1ql_vector_recall_and_accuracy(k, probe, avg_recall, "Recall",
                                                              self.index_type)
                            )
            self.reporter.post(
                *self.metrics.n1ql_vector_recall_and_accuracy(k, probe, avg_accuracy, "Accuracy",
                                                              self.index_type)
                            )

    def create_statements(self):
        gsi_settings = self.test_config.gsi_settings
        indexes = gsi_settings.indexes
        statements = []
        build_statements = []

        vector_keyword = "VECTOR " if "bhive" == self.index_type else ""
        include_clause = f" INCLUDE ({gsi_settings.include_columns})" \
            if gsi_settings.include_columns else ""

        base_with_clause = {
            'dimension': int(gsi_settings.vector_dimension),
            'similarity': f"'{gsi_settings.vector_similarity}'",
            'defer_build': 'true'
        }

        for bucket, scopes in indexes.items():
            for scope, collections in scopes.items():
                for collection, vectors in collections.items():
                    for vector_idx, details in vectors.items():
                        field_def = gsi_settings.override_index_def or details['field']

                        new_statement = (f"CREATE {vector_keyword}INDEX `{vector_idx}` on "
                                        f"`{bucket}`.`{scope}`.`{collection}` ({field_def})"
                                        f"{include_clause}")

                        with_clause_dict = base_with_clause.copy()
                        with_clause_dict.update({
                            'num_replica': int(gsi_settings.num_index_replica or \
                                               details['num_replica']),
                            'description': str(gsi_settings.vector_description or \
                                               details['description'])
                        })

                        if details.get('num_partition'):
                            num_partition = gsi_settings.num_partition or details['num_partition']
                            with_clause_dict['num_partition'] = int(num_partition)
                            partition_by_clause = gsi_settings.partition_by_clause or "META().id"
                            partition_by = f" PARTITION BY HASH({partition_by_clause})"
                        else:
                            partition_by = ""
                        if gsi_settings.vector_train_list is not None:
                            with_clause_dict['train_list'] = int(gsi_settings.vector_train_list)

                        with_items = []
                        for key, value in with_clause_dict.items():
                            with_items.append(f"'{key}':{value}")

                        with_clause = " WITH {" + ", ".join(with_items) + "}"

                        new_statement += partition_by + with_clause
                        statements.append(new_statement)

                        build_statement = f"BUILD INDEX ON `{bucket}`.`{scope}`.`{collection}` "\
                                          f"('{vector_idx}')"
                        build_statements.append(build_statement)

        return statements + build_statements

    def run(self):
        self.cloud_restore()
        start_time = time.time()
        statements = self.create_statements()
        self.create_indexes(statements=statements)

        self.wait_for_indexing()
        index_time = time.time()-start_time
        logger.info(f"index time {index_time}")
        self.downloads_ground_truth_file()
        probes, recalls, accuracies = self.vector_recall_and_accuracy_check()
        self.report_kpi(probes, recalls, accuracies)


class N1qlVectorSearchWithFilterTest(N1qlVectorSearchTest):

    def access(self):
        access_settings = self.test_config.access_settings
        access_settings.filtering_percentage = \
            int(self.test_config.gsi_settings.vector_filter_percentage)
        PerfTest.access(self, settings=access_settings)

    def cloud_restore(self):
        super().cloud_restore()
        self.wait_for_persistence()
        self.access()

class N1qlVectorLatencyThroughputPreparedStatementTest(N1qlVectorSearchTest):

    @with_stats
    @with_profiles
    def access(self):
        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        access_settings = self.set_custom_query_settings(access_settings)
        PerfTest.access(self, settings=access_settings)

    def set_custom_query_settings(self, access_settings: AccessSettings):
        access_settings.n1ql_queries[0]['statement'] = access_settings.n1ql_queries[0][
            'statement'].replace("NPROBES", self.test_config.gsi_settings.vector_scan_probes)
        access_settings.n1ql_queries[0]['statement'] = access_settings.n1ql_queries[0][
            'statement'].replace("top_k_results", self.test_config.index_settings.top_k_results)
        access_settings.n1ql_queries[0]['statement'] = access_settings.n1ql_queries[0][
            'statement'].replace("SIMILARITY", self.test_config.gsi_settings.vector_similarity)
        access_settings.n1ql_queries[0]['statement'] = access_settings.n1ql_queries[0][
            'statement'].replace("RERANKING",
                                 str(self.test_config.gsi_settings.vector_reranking).lower())
        access_settings.vector_query_map = self.test_config.index_settings.vector_query_map
        return access_settings

    def report_recall_and_accuracy(self, probes, recalls, accuracies):
        k = int(self.test_config.index_settings.top_k_results)
        for probe, avg_recall, avg_accuracy in zip(probes, recalls, accuracies):
            self.reporter.post(
                *self.metrics.n1ql_vector_recall_and_accuracy(k, probe, avg_recall, "Recall",
                                                              self.index_type)
                            )
            self.reporter.post(
                *self.metrics.n1ql_vector_recall_and_accuracy(k, probe, avg_accuracy, "Accuracy",
                                                              self.index_type)
                            )

    def report_kpi(self, probes, recalls, accuracies, initial_throughput):
        self.report_recall_and_accuracy(probes, recalls, accuracies)
        for percentile in self.test_config.access_settings.latency_percentiles:
            self.reporter.post(
                *self.metrics.query_latency(percentile=percentile,
                                            custom_title_postfix=self.index_type,
                                            update_subcategory=True)
            )
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput(self.master_node,
                                              initial_throughput=initial_throughput,
                                              custom_title_postfix=self.index_type,
                                              update_subcategory=True)
        )

    def run(self):
        self.cloud_restore()
        statements = self.create_statements()
        self.create_indexes(statements=statements)
        self.wait_for_indexing(statements=statements)
        self.downloads_ground_truth_file()
        probes, recalls, accuracies = self.vector_recall_and_accuracy_check()
        self.enable_stats()
        initial_throughput = self.metrics._avg_n1ql_throughput(self.master_node)
        self.access()
        self.report_kpi(probes, recalls, accuracies, initial_throughput)


class CapellaSnapshotBackupWithN1QLTest(CapellaSnapshotBackupRestoreTest, N1QLTest):
    # To avoid circular import, this tools test resides here
    COLLECTORS = {
        "iostat": False,
        "memory": False,
        "n1ql_latency": False,
        "n1ql_stats": True,
        "ns_server_system": True,
    }

    @timeit
    def warmup(self):
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.wait_for_indexing()

    @with_stats
    def access(self) -> dict:
        self.access_n1ql_bg()
        time.sleep(self.test_config.access_settings.time)
        self.worker_manager.abort_all_tasks()
        latencies = {}
        for percentile in self.test_config.access_settings.latency_percentiles:
            latencies[f"{percentile}"] = self.metrics._query_latency(percentile)
        return latencies

    def run(self):
        self.enable_stats()
        PerfTest.load(self)
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        self.create_indexes()
        self.wait_for_indexing()
        self.store_plans()

        self.latencies_before = self.access()
        self.run_backup_restore()
        self.latencies_after = self.access()

        self.report_kpi()
        logger.info(f"\n{self.latencies_before=}\n{self.latencies_after=}")


class JoinEnumTest(N1QLTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.base_path = "file:///data/join_enum/RSTU/"

    def import_tables(self):
        for table in ["R", "S", "T", "U"]:
            import_file = f'{self.base_path}{table}.tbl'
            local.cbimport(
                master_node=self.master_node,
                cluster_spec=self.cluster_spec,
                bucket=table,
                data_type='csv',
                data_format='',
                import_file=import_file,
                scope_collection_exp='',
                generate_key='key::%rand%',
                threads=16,
                field_separator='"|"',
                infer_types=True
            )

    def _report_kpi(self, time_taken, suite):
       self.reporter.post(
           *self.metrics.query_suite_runtime(time_taken, suite)
       )

    def run_cbq_script(self, script: str):
        script_file = f'{self.base_path.replace("file://", "")}{script}.sql'
        local.cbq(
            node=self.query_nodes[0],
            cluster_spec=self.cluster_spec,
            script=script_file,
            port=8093
        )

    def wait_for_indexing(self):
        for server in self.index_nodes:
            self.monitor.monitor_indexing(server)

    @with_stats
    @timeit
    def run_query_suite(self, suite: str):
       suite_file = f'{self.base_path.replace("file://", "")}{suite}.sql'
       logger.info("Executing {}...".format(suite_file))
       local.cbq(
           node=self.query_nodes[0],
           cluster_spec=self.cluster_spec,
           script=suite_file,
           port=8093
       )

    def run_all_query_suites(self):
        for suite in ["60Joins", "Focus", "RST.HashJoins", "RST.NLJoins", "RSTU.HashJoins"]:
            time_taken = self.run_query_suite(suite)
            self.report_kpi(time_taken=time_taken, suite=suite)

    def run(self):
        self.restore_local()
        self.import_tables()

        # create indexes
        self.run_cbq_script("cr_ind_n_upd")

        # build indexes
        self.run_cbq_script("build_index")
        self.wait_for_indexing()

        # update index stats
        self.run_cbq_script("indexstats")

        self.run_all_query_suites()


class N1qlVectorLatencyThroughputPreparedStatementFilterTest(
    N1qlVectorLatencyThroughputPreparedStatementTest):
    def cloud_restore(self):
        super().cloud_restore()
        access_settings = self.test_config.access_settings
        access_settings.n1ql_workers = 0
        access_settings.filtering_percentage = \
            int(self.test_config.gsi_settings.vector_filter_percentage)
        access_settings.vector_query_map = self.test_config.index_settings.vector_query_map
        if access_settings.workers > 0:
            PerfTest.access(self, settings=access_settings)


class N1QLDynamicServiceRebalanceTest(N1QLThroughputRebalanceTest, DynamicServiceRebalanceTest):
    def _rebalance(self, *args, **kwargs) -> float:
        return DynamicServiceRebalanceTest._rebalance(self, *args, **kwargs)

    def _report_kpi(self, *args):
        super()._report_kpi(*args)

        # Report rebalance time
        value, snapshots, metric_info = self.metrics.elapsed_time(self.rebalance_time)
        metric_info["title"] = metric_info.get("title", "").replace(
            "Dynamic Service Rebalance", "Dynamic Service Rebalance (min)"
        )
        self.reporter.post(value, snapshots, metric_info)


class N1qlVectorLatencyThroughputTestWithBgOps(
    N1qlVectorLatencyThroughputPreparedStatementTest):

    def access(self):
        access_settings = self.test_config.access_settings
        access_settings.n1ql_workers = 0
        PerfTest.access_bg(self, settings=access_settings)
        time.sleep(60) # to start background workload
        super().access()


class N1qlVectorLatencyRebalanceTest(N1qlVectorLatencyThroughputPreparedStatementTest,
                                     RebalanceTest):

    def pre_rebalance(self):
        super().pre_rebalance()
        if self.shard_affinity:
            if create_build_tuple(self.build) > (7, 6, 0, 2037):
                self.monitor.wait_for_snapshot_persistence(self.index_nodes)

    def post_rebalance(self):
        self.worker_manager.abort_all_tasks()
        super().post_rebalance()

    def _rebalance(self, services):
        return super()._rebalance(services=services)

    @with_stats
    @with_profiles
    def rebalance_indexer(self):
        self.shard_affinity = self.test_config.gsi_settings.settings.get(
                              "indexer.settings.enable_shard_affinity", False)
        self.access_n1ql_bg()
        self.pre_rebalance()
        total_requests_before = self.get_n1ql_request_count_until_now()
        self.rebalance_time = self._rebalance(services="index")
        total_requests_after = self.get_n1ql_request_count_until_now()
        self.post_rebalance()

        total_requests_during_rebalance = total_requests_after - total_requests_before
        logger.info(f"Queries before rebalance: {total_requests_before}, "
                    f"after rebalance: {total_requests_after}, "
                    f"during rebalance: {total_requests_during_rebalance}")
        return total_requests_during_rebalance

    def recall_pre_rebalance(self):
        probes, recalls, accuracies = self.vector_recall_and_accuracy_check()
        logger.info("Recall and accuracy check before rebalance completed.")
        logger.info(f"Probes: {probes}, Recalls: {recalls}, Accuracies: {accuracies}")

    def access_n1ql_bg(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.workers = 0
        access_settings = self.set_custom_query_settings(access_settings)

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        PerfTest.access_bg(self, settings=access_settings, target_iterator=iterator)

    def _report_kpi(self, probes, recalls, accuracies, total_requests):
        self.report_recall_and_accuracy(probes, recalls, accuracies)
        self.reporter.post(*self.metrics.rebalance_time(self.rebalance_time))
        self.reporter.post(
            *self.metrics.avg_n1ql_rebalance_throughput(self.rebalance_time, total_requests)
        )

    def run(self):
        self.cloud_restore()
        statements = self.create_statements()
        self.create_indexes(statements=statements)
        self.wait_for_indexing(statements=statements)
        self.downloads_ground_truth_file()
        self.recall_pre_rebalance()
        self.access_bg()
        total_requests_during_rebalance = self.rebalance_indexer()
        probes, recalls, accuracies = self.vector_recall_and_accuracy_check()
        self._report_kpi(probes, recalls, accuracies, total_requests_during_rebalance)

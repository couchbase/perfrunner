import re
import time

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.profiler import with_profiles
from perfrunner.tests import PerfTest, TargetIterator


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
                        if not bucket_replaced:
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
                            if not bucket_replaced:
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
        self.access()

        self.report_kpi()


class N1QLLatencyTest(N1QLTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.query_latency(percentile=90)
        )


class N1QLLatencyRebalanceTest(N1QLLatencyTest):

    def is_balanced(self):
        for master in self.cluster_spec.masters:
            if self.rest.is_not_balanced(master):
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

        self.worker_manager.abort()

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

    def create_indexes(self):
        logger.info('Creating and building indexes')
        create_statements = []
        build_statements = []
        query_contexts = []
        for statement in self.test_config.index_settings.statements:
            index_name = statement.split()[2]
            index_replicas = str(self.test_config.index_settings.replicas)
            for bucket in self.test_config.buckets:
                bucket_scopes = self.test_config.collection.collection_map[bucket]
                for scope in bucket_scopes.keys():
                    for collection in bucket_scopes[scope].keys():
                        if bucket_scopes[scope][collection]["access"] == 1:
                            query_context = "default:`{}`.`{}`".format(bucket, scope)
                            index_target = "{}.`{}`".format(query_context, collection)
                            replace_target = "`TARGET_BUCKET`"
                            create_statement = statement.replace(replace_target, index_target)
                            create_statement = create_statement.replace('index_replicas',
                                                                        index_replicas)
                            build_statement = "BUILD INDEX ON default:`{}`.`{}`.`{}`('{}')".\
                                format(bucket, scope, collection, index_name)
                            query_contexts.append(query_context)
                            create_statements.append(create_statement)
                            build_statements.append(build_statement)

        for statement, query_context in zip(create_statements, query_contexts):
            logger.info('Creating index: ' + statement)
            self.rest.exec_n1ql_statement(self.query_nodes[0], statement, query_context)
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

        for statement, query_context in zip(build_statements, query_contexts):
            logger.info('Building index: ' + statement)
            self.rest.exec_n1ql_statement(self.query_nodes[0], statement, query_context)
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

        logger.info('Index Create and Build Complete')

    def create_udf(self):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        count = 0
        for bucket in self.test_config.buckets:
            udf = 'udflib' + str(count)
            if self.test_config.cluster.enable_n2n_encryption:
                local.create_javascript_udf(self.query_nodes[0], udf,
                                            rest_username, rest_password, True)
            else:
                local.create_javascript_udf(self.query_nodes[0], udf,
                                            rest_username, rest_password, False)
            bucket_scopes = self.test_config.collection.collection_map[bucket]
            statement = "CREATE OR REPLACE FUNCTION `TARGET_SCOPE`.udf(id) LANGUAGE " \
                        "JAVASCRIPT AS 'udf' AT '{}';".format(udf)
            for scope in bucket_scopes.keys():
                for collection in bucket_scopes[scope].keys():
                    if bucket_scopes[scope][collection]["access"] == 1:
                        target_scope = "`{}`.`{}`".format(bucket, scope)
                        query_context = "default:{}".format(target_scope)
                        replace_target = "`TARGET_SCOPE`"
                        statement = statement.replace(replace_target, target_scope)
                        logger.info('Creating UDF FUNCTION: ' + statement)
                        self.rest.exec_n1ql_statement(self.query_nodes[0], statement, query_context)
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

        self.rest.reset_serverless_throttle(self.master_node)

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLElixirLatencyTest(N1QLElixirThroughputTest):
    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.query_latency(percentile=90)
        )


class N1QLThroughputRebalanceTest(N1QLThroughputTest):

    def _report_kpi(self, rebalance_time, total_requests):
        self.reporter.post(
            *self.metrics.avg_n1ql_rebalance_throughput(rebalance_time, total_requests)
        )

    def is_balanced(self):
        for master in self.cluster_spec.masters:
            if self.rest.is_not_balanced(master):
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

        query_node = self.cluster_spec.servers_by_role('n1ql')[0]
        vitals = self.rest.get_query_stats(query_node)
        total_requests_before = vitals['requests.count']

        rebalance_time = self._rebalance(initial_nodes)

        vitals = self.rest.get_query_stats(query_node)
        total_requests_after = vitals['requests.count']

        logger.info('Sleeping for {} seconds before finishing'
                    .format(self.test_config.rebalance_settings.stop_after))
        time.sleep(self.test_config.rebalance_settings.stop_after)

        total_requests = total_requests_after - total_requests_before

        self.worker_manager.abort()

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
            if self.rest.is_not_balanced(master):
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

        self.worker_manager.abort()

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

    def run(self):
        self.enable_stats()
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
        warehouse = self.test_config.pytpcc_settings.warehouse
        client_threads = self.test_config.pytpcc_settings.client_threads
        query_port = self.test_config.pytpcc_settings.query_port
        multi_query_node = self.test_config.pytpcc_settings.multi_query_node
        driver = self.test_config.pytpcc_settings.driver
        nodes = self.cluster_spec.servers[:self.test_config.cluster.initial_nodes[0]]

        local.pytpcc_load_data(master_node=master_node, warehouse=warehouse,
                               client_threads=client_threads, port=query_port,
                               cluster_spec=self.cluster_spec,
                               multi_query_node=multi_query_node,
                               driver=driver,
                               nodes=nodes)

    @with_profiles
    @with_stats
    def run_tpcc(self):
        warehouse = self.test_config.pytpcc_settings.warehouse
        duration = self.test_config.pytpcc_settings.duration
        client_threads = self.test_config.pytpcc_settings.client_threads
        query_port = self.test_config.pytpcc_settings.query_port
        driver = self.test_config.pytpcc_settings.driver
        master_node = self.cluster.master_node
        multi_query_node = self.test_config.pytpcc_settings.multi_query_node
        nodes = self.cluster_spec.servers[:self.test_config.cluster.initial_nodes[0]]
        durability_level = self.test_config.pytpcc_settings.durability_level
        scan_consistency = self.test_config.pytpcc_settings.scan_consistency
        txtimetout = self.test_config.pytpcc_settings.txtimeout

        local.pytpcc_run_task(warehouse=warehouse, duration=duration,
                              client_threads=client_threads, port=query_port,
                              driver=driver, master_node=master_node,
                              multi_query_node=multi_query_node,
                              cluster_spec=self.cluster_spec,
                              nodes=nodes,
                              durability_level=durability_level,
                              scan_consistency=scan_consistency,
                              txtimeout=txtimetout)

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
        self.worker_manager.wait_for_workers()

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
        self.worker_manager.wait_for_workers()

    def rebalance_out(self, *args):
        wait_time = self.test_config.access_settings.time // 4
        time.sleep(wait_time)
        master = self.cluster_spec.servers[0]
        initial_nodes = self.test_config.cluster.initial_nodes[0]
        known_nodes = self.cluster_spec.servers[:initial_nodes]
        eject_n1ql_node = self.cluster_spec.servers_by_role("n1ql")[-1]
        self.rest.rebalance(master, known_nodes, [eject_n1ql_node])
        self.monitor.monitor_rebalance(master)
        self.worker_manager.wait_for_workers()

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

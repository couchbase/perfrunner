import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.profiler import with_profiles
from perfrunner.tests import PerfTest, TargetIterator


class N1QLTest(PerfTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': True,
        'n1ql_stats': True,
        'secondary_stats': True,
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
                for bucket in self.test_config.buckets:
                    if bucket in query_statement:
                        bucket_replaced = False
                        bucket_scopes = self.test_config.collection.collection_map[bucket]
                        for scope in bucket_scopes.keys():
                            for collection in bucket_scopes[scope].keys():
                                if bucket_scopes[scope][collection]["access"] == 1:
                                    query_target = "default:`{}`.`{}`.`{}`"\
                                        .format(bucket, scope, collection)
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
                plan = self.rest.explain_n1ql_statement(self.query_nodes[0], query_statement)
            else:
                plan = self.rest.explain_n1ql_statement(self.query_nodes[0], query['statement'])
            with open('query_plan_{}.json'.format(i), 'w') as fh:
                fh.write(pretty_dict(plan))

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

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
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        initial_nodes = self.test_config.cluster.initial_nodes
        self.rebalance(initial_nodes[0])

        if self.is_balanced():
            self.report_kpi()


class N1QLThroughputTest(N1QLTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput()
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
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        initial_nodes = self.test_config.cluster.initial_nodes
        rebalance_time, total_requests = self.rebalance(initial_nodes[0])

        if self.is_balanced():
            self.report_kpi(rebalance_time, total_requests)


class N1QLJoinTest(N1QLTest):

    ALL_BUCKETS = True

    def load_regular(self, load_settings, target):
        load_settings.items //= 2
        super(N1QLTest, self).load(settings=load_settings,
                                   target_iterator=(target, ))
        target.prefix = 'n1ql'
        super(N1QLTest, self).load(settings=load_settings,
                                   target_iterator=(target, ))

    def load_categories(self, load_settings, target):
        load_settings.items = load_settings.num_categories
        target.prefix = 'n1ql'
        super(N1QLTest, self).load(settings=load_settings,
                                   target_iterator=(target, ))

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
        self.load()
        self.wait_for_persistence()

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
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

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
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_indexes()
        self.wait_for_indexing()

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
        self.load()
        self.xattr_load()
        self.wait_for_persistence()
        self.check_num_items()

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
        self.load()
        self.xattr_load()
        self.wait_for_persistence()
        self.check_num_items()

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
        'n1ql_latency': True,
        'n1ql_stats': True,
        'net': False,
        'secondary_debugstats_index': True,
    }

    def run(self):
        self.import_data()

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
        'secondary_debugstats_index': True,
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
        self.import_data()

        time_elapsed = self.create_indexes()

        self.report_kpi(time_elapsed)


class BigFUNLatencyTest(N1QLLatencyTest):

    COLLECTORS = {
        'n1ql_latency': True,
        'n1ql_stats': True,
        'net': False,
        'secondary_debugstats_index': True,
    }

    def run(self):
        self.restore()
        self.wait_for_persistence()

        self.create_indexes()
        self.wait_for_indexing()

        self.access()

        self.report_kpi()


class N1QLFunctionTest(N1QLTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

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
            *self.metrics.avg_n1ql_throughput()
        )

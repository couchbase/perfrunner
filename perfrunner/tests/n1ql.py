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
            plan = self.rest.explain_n1ql_statement(self.query_nodes[0],
                                                    query['statement'])
            with open('query_plan_{}.json'.format(i), 'w') as fh:
                fh.write(pretty_dict(plan))

    def run(self):
        self.load()
        self.wait_for_persistence()

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


class N1QLThroughputTest(N1QLTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput()
        )


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


class N1QLJoinThroughputTest(N1QLJoinTest, N1QLThroughputTest):

    pass


class N1QLJoinLatencyTest(N1QLJoinTest, N1QLLatencyTest):

    pass


class N1QLBulkTest(N1QLTest):

    @with_stats
    @timeit
    def access(self, *args):
        statement = self.test_config.access_settings.n1ql_queries[0]['statement']

        self.rest.exec_n1ql_statement(self.query_nodes[0], statement)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.bulk_n1ql_throughput(time_elapsed)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

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

        self.create_indexes()
        self.wait_for_indexing()

        self.store_plans()

        self.access_bg()
        self.access()

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

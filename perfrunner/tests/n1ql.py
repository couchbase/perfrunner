from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest, TargetIterator


class N1QLTest(PerfTest):

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': True,
        'n1ql_stats': True,
        'net': False,
        'secondary_stats': True,
    }

    @with_stats
    def access(self, *args):
        super().sleep()

        self.worker_manager.wait_for_workers()

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
        self.download_certificate()

        access_settings = self.test_config.access_settings
        access_settings.items //= 2
        super().access_bg(settings=access_settings)

    def build_index(self):
        self.index.build()

    def create_index(self, bucket, index, query_node, index_node=None):
        self.index.create_index(bucket, index, query_node, index_node)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.build_index()

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


class N1QLJoinTest(N1QLThroughputTest):

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
            access_settings.buckets = self.test_config.buckets

            if doc_gen != access_settings.n1ql_gen:
                access_settings.n1ql_workers = 0

            super(N1QLTest, self).access_bg(settings=access_settings,
                                            target_iterator=(target, ))

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput()
        )


class N1QLBulkTest(N1QLTest):

    @with_stats
    @timeit
    def access(self, *args):
        statement = self.test_config.access_settings.n1ql_queries[0]['statement']

        query_node = self.cluster_spec.servers_by_role('n1ql')[0]
        self.rest.exec_n1ql_statement(query_node, statement)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.bulk_n1ql_throughput(time_elapsed)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.build_index()

        time_elapsed = self.access()

        self.report_kpi(time_elapsed)


class N1QLMixedThroughputTest(N1QLThroughputTest):

    def build_index(self):
        bucket = self.test_config.buckets[0]
        query_node = self.cluster_spec.servers_by_role('n1ql')[0]
        for index in self.test_config.n1ql_settings.indexes:
            self.create_index(bucket, index, query_node)


class N1QLDGMTest:

    COLLECTORS = {
        'n1ql_latency': True,
        'n1ql_stats': True,
        'net': False,
        'secondary_stats': True,
        'secondary_storage_stats': True,
    }


class N1QLDGMThroughputTest(N1QLDGMTest, N1QLThroughputTest):

    pass


class N1QLDGMLatencyTest(N1QLDGMTest, N1QLLatencyTest):

    pass


class N1QLXattrThroughputTest(N1QLThroughputTest):

    def xattr_load(self, *args, **kwargs):
        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        super().xattr_load(target_iterator=iterator)

    def run(self):
        self.load()
        self.xattr_load()
        self.wait_for_persistence()

        self.build_index()

        self.access_bg()
        self.access()

        self.report_kpi()

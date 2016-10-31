from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest, TargetIterator


class N1QLTest(PerfTest):

    COLLECTORS = {
        'n1ql_latency': True,
        'n1ql_stats': True,
        'secondary_stats': True,
    }

    def build_index(self):
        for name, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            query_node = servers[0].split(':')[0]
            for bucket, index in zip(self.test_config.buckets,
                                     self.test_config.n1ql_settings.indexes):
                self.create_index(query_node, bucket, index)

    def create_index(self, query_node, bucket, index):
        index_name, index_query = index.split('::')
        query = index_query.format(name=index_name, bucket=bucket)
        self.rest.exec_n1ql_statement(query_node, query)

        self.monitor.monitor_index_state(host=query_node, index_name=index_name)

    def create_prepared_statements(self):
        self.n1ql_queries = []
        statements = list()

        for query in self.test_config.access_settings.n1ql_queries:
            if 'prepared' in query and query['prepared']:
                stmt = 'PREPARE {} AS {}'.format(query['prepared'],
                                                 query['statement'])
                statements.append(stmt)
                del query['statement']
                query['prepared'] = '"' + query['prepared'] + '"'
            self.n1ql_queries.append(query)

        for name, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            for server in servers:
                query_node = server.split(':')[0]
                for statement in statements:
                    self.rest.exec_n1ql_statement(query_node, statement)

    @with_stats
    def access(self, *args):
        super(N1QLTest, self).timer()

        self.worker_manager.wait_for_workers()

    def load(self, *args):
        """Important: the test creates two data sets with different key
        prefixes.

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
        queries."""
        load_settings = self.test_config.load_settings
        load_settings.items /= 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        super(N1QLTest, self).load(load_settings, iterator)
        super(N1QLTest, self).load(load_settings)

    def access_bg(self, *args):
        self.workload = self.test_config.access_settings
        self.workload.items /= 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',
                                             self.workload.n1ql_queries)
        super(N1QLTest, self).access_bg(access_settings=self.workload)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.build_index()

        self.create_prepared_statements()

        self.access_bg()
        self.access()

        self.report_kpi()


class N1QLLatencyTest(N1QLTest):

    def _report_kpi(self):
        self.reporter.post_to_sf(
            *self.metric_helper.calc_query_latency(percentile=80)
        )


class N1QLThroughputTest(N1QLTest):

    def _report_kpi(self):
        self.reporter.post_to_sf(
            *self.metric_helper.calc_avg_n1ql_queries()
        )


class N1QLJoinTest(N1QLThroughputTest):

    def load_regular(self, load_settings, target):
        load_settings.items /= 2
        super(N1QLTest, self).load(load_settings, (target,))
        target.prefix = 'n1ql'
        super(N1QLTest, self).load(load_settings, (target,))

    def load_categories(self, load_settings, target):
        load_settings.items = load_settings.num_categories
        target.prefix = 'n1ql'
        super(N1QLTest, self).load(load_settings, (target,))

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
            self.workload = self.test_config.access_settings
            self.workload.doc_gen = doc_gen
            self.workload.items /= 2
            self.workload.n1ql_queries = self.n1ql_queries
            if doc_gen != 'ext_reverse_lookup':
                self.workload.n1ql_workers = 0

            super(N1QLTest, self).access_bg(access_settings=self.workload,
                                            target_iterator=(target, ))

    def _report_kpi(self):
        self.reporter.post_to_sf(
            *self.metric_helper.calc_avg_n1ql_queries()
        )

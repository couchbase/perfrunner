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
            for bucket in self.test_config.buckets:
                self._build_index(query_node, bucket)

    def _build_index(self, query_node, bucket):
        names = list()
        for index in self.test_config.n1ql_settings.indexes:
            if '{partition_id}' in index:
                for id in range(self.test_config.load_settings.doc_partitions):
                    index_name, index_query = index.split('::')
                    index_name = index_name.format(partition_id=id)
                    query = index_query.format(name=index_name, bucket=bucket,
                                               partition_id=id)
                    self.rest.exec_n1ql_statement(query_node, query)
                    names.append(index_name)
            else:
                index_name, index_query = index.split('::')
                query = index_query.format(name=index_name, bucket=bucket)
                self.rest.exec_n1ql_statement(query_node, query)
                names.append(index_name)

        for name in names:
            self.monitor.monitor_index_state(host=query_node, index_name=name)

    def _create_prepared_statements(self):
        self.n1ql_queries = []
        prepared_stmnts = list()
        for query in self.test_config.access_settings.n1ql_queries:
            if 'prepared' in query and query['prepared']:
                if '{partition_id}' in query['prepared']:
                    for id in range(self.test_config.load_settings.doc_partitions):
                        name = query['prepared'].format(partition_id=id)
                        part_stmt = query['statement'].format(partition_id=id)
                        stmt = 'PREPARE {} AS {}'.format(name, part_stmt)
                        prepared_stmnts.append(stmt)
                else:
                    stmt = 'PREPARE {} AS {}'.format(query['prepared'],
                                                     query['statement'])
                    prepared_stmnts.append(stmt)
                del query['statement']
                query['prepared'] = '"' + query['prepared'] + '"'
            self.n1ql_queries.append(query)

        for name, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            for server in servers:
                query_node = server.split(':')[0]
                for statement in prepared_stmnts:
                    self.rest.exec_n1ql_statement(query_node, statement)

    @with_stats
    def access(self, access_settings=None):
        super(N1QLTest, self).timer()

        self.worker_manager.wait_for_workers()

    def run(self):
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
        self.load(load_settings, iterator)
        self.load(load_settings)
        self.wait_for_persistence()

        self.build_index()

        self._create_prepared_statements()

        self.workload = self.test_config.access_settings
        self.workload.items /= 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',
                                             self.workload.n1ql_queries)
        self.access_bg(access_settings=self.workload)
        self.access(access_settings=self.workload)

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

import logger
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests import TargetIterator
from exceptions import NotImplementedError


class N1QLTest(PerfTest):

    COLLECTORS = {
        'n1ql_latency': True,
        'n1ql_stats': True,
        'secondary_stats': True
    }

    def __init__(self, *args, **kwargs):
        super(N1QLTest, self).__init__(*args, **kwargs)

    def build_index(self):
        for name, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            if not servers:
                raise Exception('No query servers specified for cluster \"{}\",'
                                ' cannot create indexes'.format(name))

            if not self.test_config.buckets:
                raise Exception('No buckets specified for cluster \"{}\",'
                                ' cannot create indexes'.format(name))

            query_node = servers[0].split(':')[0]
            for bucket in self.test_config.buckets:
                self._build_index(query_node, bucket)

    def _build_index(self, query_node, bucket):

        names = list()
        for index in self.test_config.n1ql_settings.indexes:

            if '{partition_id}' in index:
                for id in range(self.test_config.load_settings.doc_partitions):
                    index_name = index.split('::')[0].format(partition_id=id)
                    index_query = index.split('::')[1]
                    query = index_query.format(name=index_name, bucket=bucket,
                                               partition_id=id)
                    self.rest.n1ql_query(query_node, query)
                    names.append(index_name)
            else:
                index_name = index.split('::')[0]
                index_query = index.split('::')[1]
                query = index_query.format(name=index_name, bucket=bucket)
                self.rest.n1ql_query(query_node, query)
                names.append(index_name)

            for name in names:
                self.rest.wait_for_indexes_to_become_online(host=query_node,
                                                            index_name=name)

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
            if not servers:
                raise Exception('No query servers specified for cluster \"{}\",'
                                ' cannot create prepared statement'.format(name))

            if not self.test_config.buckets:
                raise Exception('No buckets specified for cluster \"{}\", '
                                'cannot create prepared statement'.format(name))

            for server in servers:
                query_node = server.split(':')[0]
                for stmt in prepared_stmnts:
                    self.rest.n1ql_query(query_node, stmt)

    @with_stats
    def access(self, access_settings=None):
        super(N1QLTest, self).timer()

    def run(self):
        raise NotImplementedError("N1QLTest is a base test and cannot be run")


class N1QLLatencyTest(N1QLTest):

    def __init__(self, *args, **kwargs):
        super(N1QLLatencyTest, self).__init__(*args, **kwargs)

    def run(self):
        load_settings = self.test_config.load_settings
        load_settings.items = load_settings.items / 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        self.load(load_settings, iterator)

        self.load(load_settings)
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

        self._create_prepared_statements()

        self.workload = self.test_config.access_settings
        self.workload.items = self.workload.items / 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',
                                             self.workload.n1ql_queries)

        self.access_bg(self.workload)
        self.access(self.workload)

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_query_latency(percentile=80)
            )


class N1QLThroughputTest(N1QLTest):

    def __init__(self, *args, **kwargs):
        super(N1QLThroughputTest, self).__init__(*args, **kwargs)

    def run(self):
        load_settings = self.test_config.load_settings
        load_settings.items = load_settings.items / 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        self.load(load_settings, iterator)

        self.load(load_settings)
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

        self._create_prepared_statements()

        self.workload = self.test_config.access_settings
        self.workload.items = self.workload.items / 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',
                                             self.workload.n1ql_queries)

        self.access_bg(self.workload)
        self.access(self.workload)

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_avg_n1ql_queries()
            )

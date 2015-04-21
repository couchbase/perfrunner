import logger
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.workloads.n1ql import INDEX_STATEMENTS


class N1QLTest(PerfTest):

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
        for index in self.test_config.n1ql_settings.indexes:
            query = index.format(bucket=bucket)
            self.rest.n1ql_query(query_node, query)

    @with_stats
    def access(self):
        super(N1QLTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()

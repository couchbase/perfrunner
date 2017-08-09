from itertools import product

from logger import logger
from perfrunner.helpers.misc import target_hash


class IndexHelper:

    def __init__(self, cluster_spec, test_config, rest, monitor):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.rest = rest
        self.monitor = monitor

    def build(self, one_index_per_bucket=True):
        logger.info('Building indexes')

        query_node = self.cluster_spec.servers_by_role('n1ql')[0]

        initial_nodes = self.test_config.cluster.initial_nodes[0]
        index_nodes = self.cluster_spec.servers_by_role('index')[:initial_nodes]

        if one_index_per_bucket:
            index_tuple = zip(self.test_config.buckets,
                              self.test_config.n1ql_settings.indexes)
        else:
            index_tuple = product(self.test_config.buckets,
                                  self.test_config.n1ql_settings.indexes)
        for bucket, index in index_tuple:
            for index_node in index_nodes:
                self.create_index(bucket, index, query_node, index_node)

    def create_index(self, bucket, index, query_node, index_node=None):
        index_name, statement = index.split('::')
        if not index_name:
            return

        statement = statement.format(name=index_name,
                                     hash=target_hash(index_node),
                                     bucket=bucket,
                                     index_node=index_node)

        self.rest.exec_n1ql_statement(query_node, statement)

        self.monitor.monitor_index_state(host=query_node, index_name=index_name)

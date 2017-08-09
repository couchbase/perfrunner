from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import read_json
from perfrunner.tests import PerfTest


class ElasticTest(PerfTest):

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)

        self.index_name = self.test_config.fts_settings.name
        self.order_by = self.test_config.fts_settings.order_by

    def delete_index(self):
        self.rest.delete_elastic_index(self.master_node, self.index_name)

    def create_index(self):
        definition = read_json(self.test_config.fts_settings.index_configfile)

        self.rest.create_elastic_index(self.master_node, self.index_name,
                                       definition)

    def restore(self):
        logger.info('Restoring data')
        self.remote.restore_without_index(self.test_config.fts_settings.storage,
                                          self.test_config.fts_settings.repo)

    def cleanup_and_restore(self):
        self.delete_index()
        self.restore()
        self.wait_for_persistence()

    def enable_replication(self):
        self.rest.add_remote_cluster(
            host=self.master_node,
            remote_host='{}:9091'.format(self.master_node),
            name='Elastic',
        )
        self.rest.start_replication(
            host=self.master_node,
            params={
                'replicationType': 'continuous',
                'toBucket': self.index_name,
                'fromBucket': self.test_config.buckets[0],
                'toCluster': 'Elastic',
                'type': 'capi',
            },
        )

    def wait_for_index(self):
        self.monitor.monitor_elastic_indexing_queue(self.master_node,
                                                    self.index_name)

    def wait_for_index_persistence(self):
        self.monitor.monitor_elastic_index_persistence(self.master_node,
                                                       self.index_name)

    @with_stats
    def access(self, *args):
        super().sleep()

    def access_bg(self, *args):
        access_settings = self.test_config.access_settings
        access_settings.fts_config = self.test_config.fts_settings
        super().access_bg(settings=access_settings)

    def run(self):
        self.cleanup_and_restore()

        self.create_index()
        self.enable_replication()
        self.wait_for_index()
        self.wait_for_index_persistence()

        self.access_bg()
        self.access()

        self.report_kpi()


class ElasticIndexTest(ElasticTest):

    @with_stats
    @timeit
    def build_index(self):
        self.create_index()
        self.enable_replication()
        self.wait_for_index()

    def calculate_index_size(self) -> int:
        stats = self.rest.get_elastic_stats(self.master_node)
        return stats['indices'][self.index_name]['total']['store']['size_in_bytes']

    def run(self):
        self.cleanup_and_restore()

        time_elapsed = self.build_index()

        self.wait_for_index_persistence()

        size = self.calculate_index_size()

        self.report_kpi(time_elapsed, size)

    def _report_kpi(self, time_elapsed: int, size: int):
        self.reporter.post(
            *self.metrics.fts_index(time_elapsed,
                                    order_by=self.order_by,
                                    name=' Elasticsearch 1.7')
        )
        self.reporter.post(
            *self.metrics.fts_index_size(size,
                                         order_by=self.order_by,
                                         name=' Elasticsearch 1.7')
        )


class ElasticLatencyTest(ElasticTest):

    COLLECTORS = {'elastic_stats': True}

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.latency_fts_queries(percentile=80,
                                              dbname='fts_latency',
                                              metric='elastic_latency_get',
                                              order_by=self.order_by,
                                              name=' Elasticsearch 1.7'
                                              ))


class ElasticThroughputTest(ElasticTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_fts_throughput(order_by=self.order_by,
                                             name=' Elasticsearch 1.7')
        )

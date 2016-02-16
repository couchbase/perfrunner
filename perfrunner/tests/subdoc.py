import time

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import pretty_dict
from perfrunner.tests import PerfTest


class SubdocTest(PerfTest):
    """
    The most basic KV workflow:
        Initial data load ->
            Persistence and intra-cluster replication (for consistency) ->
                Data compaction (for consistency) ->
                    "Hot" load or working set warm up ->
                        "access" phase or active workload
    """

    @with_stats
    def access(self, access_settings=None):
        super(SubdocTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.workload = self.test_config.access_settings

        self.access_bg()
        self.workload = self.test_config.access_settings
        self.access(self.workload)


class SubdocKVTest(SubdocTest):
    COLLECTORS = {'bandwidth': True}
    network_matrix_subdoc = {}

    @with_stats
    def access(self):
        self.workload = self.test_config.access_settings

        self.remote.start_bandwidth_monitor(int(self.workload.time / 60) + 1)
        self.access_bg()
        time.sleep(self.workload.time + 60)
        self.metric_db_servers_helper = MetricHelper(self)
        self.network_matrix_subdoc = self.metric_db_servers_helper.calc_network_bandwidth
        logger.info(
            'Network bandwidth for subdoc: {}'.format(pretty_dict(self.network_matrix_subdoc))
        )

        self.remote.kill_process('iptraf')

        self.remote.start_bandwidth_monitor(int(self.workload.time / 60) + 1)
        self.workload.workers = self.workload.subdoc_workers
        self.workload.subdoc_workers = 0
        self.access_bg(self.workload)
        time.sleep(self.workload.time + 60)
        self.metric_db_servers_helper = MetricHelper(self)
        self.network_matrix_full = self.metric_db_servers_helper.calc_network_bandwidth
        logger.info(
            'Network bandwidth for full doc: {}'.format(pretty_dict(self.network_matrix_full))
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.access()
        logger.info('network_matrix_subdoc, network_matrix_full',
                    self.network_matrix_subdoc, self.network_matrix_full)
        if self.test_config.stats_settings.enabled:
            metric = '{}_{}'.format(self.test_config.name,
                                    self.cluster_spec.name)
            metric_info = {
                'title': self.test_config.test_case.metric_title + '(in bytes)',
                'cluster': self.cluster_spec.name,
                'larger_is_better': self.test_config.test_case.larger_is_better,
                'level': self.test_config.test_case.level,
            }
            in_reduction = 100 - self.network_matrix_subdoc['in bytes'] * 100 / \
                self.network_matrix_full['in bytes']
            self.reporter.post_to_sf(in_reduction, metric=metric + '_in_bytes',
                                     metric_info=metric_info)
            metric_info = {
                'title': self.test_config.test_case.metric_title + '(out  bytes)',
                'cluster': self.cluster_spec.name,
                'larger_is_better': self.test_config.test_case.larger_is_better,
                'level': self.test_config.test_case.level,
            }
            out_reduction = 100 - self.network_matrix_subdoc['out bytes'] * 100 / \
                self.network_matrix_full['out bytes']
            self.reporter.post_to_sf(out_reduction, metric=metric + '_out_bytes',
                                     metric_info=metric_info)


class SubdocLatencyTest(SubdocTest):
    """
    Enables reporting of GET and SET latency.
    """

    COLLECTORS = {'subdoc_latency': True}
    LATENCY_METRICS = ('get', 'set')

    def run(self):
        super(SubdocLatencyTest, self).run()
        if self.test_config.stats_settings.enabled:
            for operation in self.LATENCY_METRICS:
                self.reporter.post_to_sf(*self.metric_helper.calc_kv_latency(
                    operation=operation, percentile=95, dbname='spring_subdoc_latency')
                )

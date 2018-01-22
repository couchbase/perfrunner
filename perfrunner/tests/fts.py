from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import RebalanceTest


class FTSTest(PerfTest):

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)

        self.index_name = self.test_config.fts_settings.name
        self.order_by = self.test_config.fts_settings.order_by

        initial_nodes = test_config.cluster.initial_nodes[0]
        all_fts_hosts = self.cluster_spec.servers_by_role('fts')
        self.active_fts_hosts = all_fts_hosts[:initial_nodes]
        self.fts_master_host = self.active_fts_hosts[0]

    def delete_index(self):
        self.rest.delete_fts_index(self.fts_master_host, self.index_name)

    def create_index(self):
        definition = read_json(self.test_config.fts_settings.index_configfile)
        definition.update({
            'name': self.index_name,
            'sourceName': self.test_config.buckets[0],
        })
        logger.info('Index definition: {}'.format(pretty_dict(definition)))

        self.rest.create_fts_index(self.fts_master_host, self.index_name,
                                   definition)

    def restore(self):
        logger.info('Restoring data')
        self.remote.restore_data(self.test_config.fts_settings.storage,
                                 self.test_config.fts_settings.repo)

    def cleanup_and_restore(self):
        self.delete_index()
        self.restore()
        self.wait_for_persistence()

    def wait_for_index(self):
        self.monitor.monitor_fts_indexing_queue(self.fts_master_host,
                                                self.index_name)

    def wait_for_index_persistence(self):
        self.monitor.monitor_fts_index_persistence(self.fts_master_host,
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
        self.wait_for_index()
        self.wait_for_index_persistence()

        self.access_bg()
        self.access()

        self.report_kpi()


class FTSIndexTest(FTSTest):

    COLLECTORS = {'fts_stats': True}

    @with_stats
    @timeit
    def build_index(self):
        self.create_index()
        self.wait_for_index()

    def calculate_index_size(self) -> int:
        metric = '{}:{}:{}'.format(self.test_config.buckets[0],
                                   self.index_name,
                                   'num_bytes_used_disk')
        size = 0
        for host in self.active_fts_hosts:
            stats = self.rest.get_fts_stats(host)
            size += stats[metric]
        return size

    def run(self):
        self.cleanup_and_restore()

        time_elapsed = self.build_index()

        self.wait_for_index_persistence()

        size = self.calculate_index_size()

        self.report_kpi(time_elapsed, size)

    def _report_kpi(self, time_elapsed: int, size: int):
        self.reporter.post(
            *self.metrics.fts_index(time_elapsed, order_by=self.order_by)
        )
        self.reporter.post(
            *self.metrics.fts_index_size(size, order_by=self.order_by)
        )


class FTSLatencyTest(FTSTest):

    COLLECTORS = {
        'fts_latency': True,
        'fts_stats': True,
    }

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.latency_fts_queries(percentile=80,
                                              dbname='fts_latency',
                                              metric='cbft_latency_get',
                                              order_by=self.order_by)
        )
        self.reporter.post(
            *self.metrics.latency_fts_queries(percentile=0,
                                              dbname='fts_latency',
                                              metric='cbft_latency_get',
                                              order_by=self.order_by)
        )


class FTSThroughputTest(FTSTest):

    COLLECTORS = {'fts_stats': True}

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_fts_throughput(order_by=self.order_by)
        )


class FTSRebalanceTest(FTSTest, RebalanceTest):

    COLLECTORS = {'fts_stats': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings

    def run(self):
        self.cleanup_and_restore()

        self.create_index()
        self.wait_for_index()
        self.wait_for_index_persistence()

        self.access_bg()
        self.rebalance_fts()

        self.report_kpi()

    def rebalance_fts(self):
        self.rebalance(services='kv,fts')

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.fts_rebalance_time(reb_time=self.rebalance_time,
                                             order_by=self.order_by)
        )


class FTSRebalanceTestThroughput(FTSRebalanceTest):

    COLLECTORS = {'fts_stats': True}


class FTSRebalanceTestLatency(FTSRebalanceTest):

    COLLECTORS = {
        'fts_latency': True,
        'fts_stats': True,
    }

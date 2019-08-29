from typing import List, Tuple

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import RebalanceTest
from perfrunner.workloads.bigfun.driver import bigfun
from perfrunner.workloads.bigfun.query_gen import Query


class BigFunTest(PerfTest):

    COLLECTORS = {'analytics': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0

    def create_datasets(self, bucket: str):
        logger.info('Creating datasets')
        for dataset, key in (
            ('GleambookUsers', 'id'),
            ('GleambookMessages', 'message_id'),
            ('ChirpMessages', 'chirpid'),
        ):
            statement = "CREATE DATASET `{}` ON `{}` WHERE `{}` IS NOT UNKNOWN;"\
                .format(dataset, bucket, key)
            self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                               statement)

    def create_index(self):
        logger.info('Creating indexes')
        for statement in (
            "CREATE INDEX usrSinceIdx   ON `GleambookUsers`(user_since: string);",
            "CREATE INDEX gbmSndTimeIdx ON `GleambookMessages`(send_time: string);",
            "CREATE INDEX cmSndTimeIdx  ON `ChirpMessages`(send_time: string);",
        ):
            self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                               statement)

    def disconnect_bucket(self, bucket: str):
        logger.info('Disconnecting the bucket: {}'.format(bucket))
        statement = 'DISCONNECT BUCKET `{}`;'.format(bucket)
        self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                           statement)

    def connect_buckets(self):
        logger.info('Connecting all buckets')
        statement = "CONNECT link Local"
        self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                           statement)

    def disconnect(self):
        for target in self.target_iterator:
            self.disconnect_bucket(target.bucket)

    def sync(self):
        for target in self.target_iterator:
            self.create_datasets(target.bucket)
            self.create_index()
        self.connect_buckets()
        for target in self.target_iterator:
            self.num_items += self.monitor.monitor_data_synced(target.node,
                                                               target.bucket)

    def re_sync(self):
        for target in self.target_iterator:
            self.connect_bucket(target.bucket)
            self.monitor.monitor_data_synced(target.node, target.bucket)

    def set_analytics_logging_level(self):
        log_level = self.test_config.analytics_settings.log_level
        self.rest.set_analytics_logging_level(self.analytics_nodes[0], log_level)
        self.rest.restart_analytics_cluster(self.analytics_nodes[0])
        if not self.rest.validate_analytics_logging_level(self.analytics_nodes[0], log_level):
            logger.error('Failed to set logging level {}'.format(log_level))

    def set_buffer_cache_page_size(self):
        page_size = self.test_config.analytics_settings.storage_buffer_cache_pagesize
        self.rest.set_analytics_page_size(self.analytics_nodes[0], page_size)
        self.rest.restart_analytics_cluster(self.analytics_nodes[0])

    def set_storage_compression_block(self):
        storage_compression_block = self.test_config.analytics_settings.storage_compression_block
        self.rest.set_analytics_storage_compression_block(self.analytics_nodes[0],
                                                          storage_compression_block)
        self.rest.restart_analytics_cluster(self.analytics_nodes[0])
        self.rest.validate_analytics_setting(self.analytics_nodes[0], 'storageCompressionBlock',
                                             storage_compression_block)

    def run(self):
        self.restore_local()
        self.wait_for_persistence()


class BigFunSyncTest(BigFunTest):

    def _report_kpi(self, sync_time: int):
        self.reporter.post(
            *self.metrics.avg_ingestion_rate(self.num_items, sync_time)
        )

    @with_stats
    @timeit
    def sync(self):
        super().sync()

    def run(self):
        super().run()

        sync_time = self.sync()

        self.report_kpi(sync_time)


class BigFunSyncWithCompressionTest(BigFunSyncTest):

    def run(self):
        self.set_storage_compression_block()
        super().run()


class BigFunSyncNoIndexTest(BigFunSyncTest):

    def create_index(self):
        pass


class BigFunIncrSyncTest(BigFunTest):

    def _report_kpi(self, sync_time: int):
        self.reporter.post(
            *self.metrics.avg_ingestion_rate(self.num_items, sync_time)
        )

    @with_stats
    @timeit
    def re_sync(self):
        super().re_sync()

    def run(self):
        super().run()

        self.sync()

        self.disconnect()

        super().run()

        sync_time = self.re_sync()

        self.report_kpi(sync_time)


class BigFunQueryTest(BigFunTest):

    QUERIES = 'perfrunner/workloads/bigfun/queries_with_index.json'

    @with_stats
    def access(self, *args, **kwargs) -> List[Tuple[Query, int]]:
        results = bigfun(self.rest,
                         nodes=self.analytics_nodes,
                         concurrency=self.test_config.access_settings.workers,
                         num_requests=int(self.test_config.access_settings.ops),
                         query_set=self.QUERIES)
        return [(query, latency) for query, latency in results]

    def _report_kpi(self, results: List[Tuple[Query, int]]):
        for query, latency in results:
            self.reporter.post(
                *self.metrics.analytics_latency(query, latency)
            )

    def run(self):
        super().run()

        self.sync()

        results = self.access()

        self.report_kpi(results)


class BigFunQueryWithCompressionTest(BigFunQueryTest):

    def run(self):
        self.set_storage_compression_block()
        super().run()


class BigFunQueryNoIndexTest(BigFunQueryTest):

    QUERIES = 'perfrunner/workloads/bigfun/queries_without_index.json'

    def create_index(self):
        pass


class BigFunQueryNoIndexWithCompressionTest(BigFunQueryWithCompressionTest):

    QUERIES = 'perfrunner/workloads/bigfun/queries_without_index.json'

    def create_index(self):
        pass


class BigFunQueryNoIndexWindowFunctionsTest(BigFunQueryNoIndexTest):

    QUERIES = 'perfrunner/workloads/bigfun/queries_without_index_window_functions.json'


class BigFunRebalanceTest(BigFunTest, RebalanceTest):

    ALL_HOSTNAMES = True

    def rebalance_cbas(self):
        self.rebalance(services='cbas')

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.rebalance_time(rebalance_time=self.rebalance_time)
        )

    def run(self):
        super().run()

        self.sync()

        self.rebalance_cbas()

        if self.is_balanced():
            self.report_kpi()

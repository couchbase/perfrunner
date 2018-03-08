from typing import Iterator

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import RebalanceTest
from perfrunner.workloads.bigfun import bigfun


class BigFunTest(PerfTest):

    COLLECTORS = {'analytics': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0

        self.analytics_nodes = self.rest.get_active_nodes_by_role(self.master_node,
                                                                  'cbas')

    def create_bucket(self, bucket: str):
        logger.info('Creating a new bucket: {}'.format(bucket))
        statement = 'CREATE BUCKET `{bucket}` WITH {{"name": "{bucket}"}};'\
            .format(bucket=bucket)
        self.rest.exec_analytics_statement(self.analytics_nodes[0], statement)

    def create_datasets(self, bucket: str):
        logger.info('Creating datasets')
        for dataset, key in (
            ('GleambookUsers', 'id'),
            ('GleambookMessages', 'message_id'),
            ('ChirpMessages', 'chirpid'),
        ):
            statement = "CREATE SHADOW DATASET `{}` ON `{}` WHERE `{}` IS NOT UNKNOWN;"\
                .format(dataset, bucket, key)
            self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                               statement)

    def create_index(self):
        logger.info('Creating indexes')
        for statement in (
            "CREATE INDEX usrSinceIdx ON `GleambookUsers`(user_since: string);",
            "CREATE INDEX authorIdIdx ON `GleambookMessages`(author_id: string);",
            "CREATE INDEX sndTimeIdx  ON `ChirpMessages`(send_time: string);",
        ):
            self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                               statement)

    def connect_bucket(self, bucket: str):
        logger.info('Connecting the bucket: {}'.format(bucket))
        statement = 'CONNECT BUCKET `{}`;'.format(bucket)
        self.rest.exec_analytics_statement(self.analytics_nodes[0],
                                           statement)

    def sync(self):
        for target in self.target_iterator:
            self.create_bucket(target.bucket)
            self.create_datasets(target.bucket)
            self.create_index()
            self.connect_bucket(target.bucket)

            self.num_items += self.monitor.monitor_data_synced(target.node,
                                                               target.bucket)

    def access(self, *args, **kwargs) -> Iterator:
        return bigfun(self.rest,
                      nodes=self.analytics_nodes,
                      concurrency=self.test_config.access_settings.workers,
                      num_requests=int(self.test_config.access_settings.ops))

    def run(self):
        self.restore()
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


class BigFunSyncNoIndexTest(BigFunSyncTest):

    def create_index(self):
        pass


class BigFunQueryTest(BigFunTest):

    def _report_kpi(self, results: Iterator):
        for query, latency in results:
            self.reporter.post(
                *self.metrics.analytics_latency(query, latency)
            )

    def run(self):
        super().run()

        self.sync()

        results = self.access()

        self.report_kpi(results)


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

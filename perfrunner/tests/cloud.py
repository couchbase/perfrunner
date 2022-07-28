import time

from perfrunner.helpers.cbmonitor import with_cloudwatch, with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import RebalanceKVTest
from perfrunner.tests.ycsb import YCSBN1QLTest


class CloudTest(PerfTest):

    def __init__(self, *args):
        super().__init__(*args)
        self.server_processes = ['beam.smp',
                                 'memcached',
                                 'projector',
                                 'prometheus']

    @with_stats
    def access(self, *args):
        pass

    def run(self):
        pass


class CloudIdleTest(CloudTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.cpu_utilization()
        )

        for process in self.server_processes:
            self.reporter.post(
                *self.metrics.avg_server_process_cpu(process)
            )

    @with_stats
    def access(self, *args):
        time.sleep(self.test_config.access_settings.time)

    def run(self):
        self.access()
        self.report_kpi()


class CloudIdleDocsTest(CloudIdleTest):

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.cpu_utilization()
        )

        for process in self.server_processes:
            self.reporter.post(
                *self.metrics.avg_server_process_cpu(process)
            )

    @with_cloudwatch
    @with_stats
    def access(self, *args):
        time.sleep(self.test_config.access_settings.time)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.remote.restart()
        time.sleep(200)
        self.access()
        self.report_kpi()


class CloudIdleKVN1QLTest(YCSBN1QLTest, CloudIdleTest):

    def __init__(self, *args):
        CloudIdleTest.__init__(self, *args)

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.cpu_utilization()
        )

        for process in self.server_processes:
            self.reporter.post(
                *self.metrics.avg_server_process_cpu(process)
            )

    @with_stats
    def access(self, *args):
        time.sleep(self.test_config.access_settings.time)

    def run(self):
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.create_indexes()
        self.wait_for_indexing()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.wait_for_indexing()

        self.access()

        self.report_kpi()


class CloudRebalanceKVTest(RebalanceKVTest, CloudTest):

    def __init__(self, *args):
        CloudTest.__init__(self, *args)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.reset_kv_stats()

        self.access_bg()
        self.rebalance(services="kv,index,n1ql,fts")

        if self.is_balanced():
            self.report_kpi()

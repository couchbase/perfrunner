import time

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class CloudTest(PerfTest):

    def __init__(self, *args):
        super().__init__(*args)
        self.server_processes = ['beam.smp',
                                 'cbq-engine',
                                 'indexer',
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

    @with_stats
    def access(self, *args):
        time.sleep(self.test_config.access_settings.time)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.access()
        self.report_kpi()

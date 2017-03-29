from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.local import clone_ycsb
from perfrunner.helpers.worker import ycsb_data_load_task, ycsb_task
from perfrunner.tests import PerfTest
from perfrunner.tests.n1ql import N1QLTest


class YCSBTest(PerfTest):

    def download_ycsb(self):
        clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                   branch=self.test_config.ycsb_settings.branch)

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=ycsb_data_load_task)

    @with_stats
    def access(self, *args, **kwargs):
        PerfTest.access(self, task=ycsb_task)

    def _report_kpi(self):
        self.reporter.post_to_sf(
            self.metric_helper.parse_ycsb_throughput()
        )

    def run(self):
        self.download_ycsb()

        self.load()
        self.wait_for_persistence()

        self.access()

        self.report_kpi()


class YCSBN1QLTest(YCSBTest, N1QLTest):

    def run(self):
        self.download_ycsb()

        self.load()
        self.wait_for_persistence()

        self.build_index()

        self.access()

        self.report_kpi()

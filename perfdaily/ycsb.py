from perfdaily import DailyTest
from perfrunner.tests.ycsb2 import YCSBN1QLTest as _YCSBN1QLTest
from perfrunner.tests.ycsb2 import YCSBTest as _YCSBTest


class YCSBTest(DailyTest, _YCSBTest):

    def run(self):
        self.remote.disable_cpu()

        super(YCSBTest, self).run()

        self.remote.enable_cpu()

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metric_helper.calc_ycsb_throughput()
        )


class YCSBN1QLTest(DailyTest, _YCSBN1QLTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metric_helper.calc_ycsb_throughput()
        )

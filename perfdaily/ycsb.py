from perfdaily import DailyTest
from perfrunner.tests.ycsb2 import YCSBN1QLTest as _YCSBN1QLTest
from perfrunner.tests.ycsb2 import YCSBTest as _YCSBTest


class YCSBThroughputTest(DailyTest, _YCSBTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metrics.calc_ycsb_throughput()
        )


class YCSBN1QLThroughputTest(DailyTest, _YCSBN1QLTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metrics.calc_ycsb_throughput()
        )

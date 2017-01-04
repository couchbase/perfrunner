from perfdaily import DailyTest
from perfrunner.tests.n1ql import N1QLThroughputTest as _N1QLThroughputTest


class N1QLThroughputTest(DailyTest, _N1QLThroughputTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metric_helper.cal_avg_n1ql_queries()
        )

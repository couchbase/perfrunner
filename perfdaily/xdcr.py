from perfdaily import DailyTest
from perfrunner.tests.xdcr import XdcrInitTest as _XdcrInitTest


class XdcrInitTest(DailyTest, _XdcrInitTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metric_helper.calc_avg_replication_rate(self.time_elapsed)
        )

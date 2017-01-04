from perfdaily import DailyTest
from perfrunner.tests.kv import PillowFightTest as _PillowFightTest


class PillowFightTest(DailyTest, _PillowFightTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metric_helper.calc_max_ops()
        )

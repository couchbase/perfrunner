from perfdaily import DailyTest
from perfrunner.tests.gsi import InitialIndexTest as _InitialIndexTest


class InitialIndexTest(DailyTest, _InitialIndexTest):

    def _report_kpi(self, time_elapsed):

        self.reporter.post_to_daily(metric='Initial Indexing Time (min)',
                                    value=time_elapsed)

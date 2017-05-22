from perfdaily import DailyTest
from perfrunner.tests.tools import BackupTest as _BackupTest


class BackupTest(DailyTest, _BackupTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metrics.backup_throughput(self.time_elapsed)
        )

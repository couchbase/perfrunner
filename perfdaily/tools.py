from perfdaily import DailyTest
from perfrunner.tests.tools import BackupTest as _BackupTest


class BackupTest(DailyTest, _BackupTest):

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.backup_throughput(time_elapsed)
        )

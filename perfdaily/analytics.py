from logger import logger
from perfdaily import DailyTest
from perfrunner.tests.analytics import (
    BigFunInitialSyncAndQueryTest as _BigFunInitialSyncAndQueryTest,
)


class BigFunInitialSyncAndQueryTest(DailyTest, _BigFunInitialSyncAndQueryTest):
    def report_sync_kpi(self, sync_time: float):
        logger.info(f"Initial sync time (s): {sync_time:.2f}")

        if not self.test_config.stats_settings.enabled:
            return

        self.reporter.post(*self.metrics.avg_ingestion_rate(self.num_items, sync_time))

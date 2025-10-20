from perfdaily import DailyTest
from perfrunner.tests.analytics import BigFunTest as _BigFunInitialSyncAndQueryTest
from perfrunner.tests.analytics import DatasetType


class BigFunInitialSyncAndQueryTest(DailyTest, _BigFunInitialSyncAndQueryTest):
    def report_ingestion_kpi(
        self,
        ingestion_stats: dict[DatasetType, tuple[int, float]],
        streaming_ingest_type: str = "initial",
    ):
        if not self.test_config.stats_settings.enabled:
            return

        for num_items, ingest_time in ingestion_stats.values():
            self.reporter.post(*self.metrics.avg_ingestion_rate(num_items, ingest_time))

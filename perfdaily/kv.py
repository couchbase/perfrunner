from perfdaily import DailyTest
from perfrunner.tests.dcp import JavaDCPThroughputTest as _DCPThroughputTest
from perfrunner.tests.kv import PillowFightTest as _PillowFightTest
from perfrunner.tests.magma import \
    CombinedLatencyAndRebalanceCDCTest as _CombinedLatencyAndRebalanceCDCTest
from perfrunner.tests.magma import PillowFightCDCTest as _PillowFightCDCTest
from perfrunner.tests.rebalance import RebalanceKVTest


class DCPThroughputTest(DailyTest, _DCPThroughputTest):

    pass


class PillowFightTest(DailyTest, _PillowFightTest):

    pass


class PillowFightCDCTest(DailyTest, _PillowFightCDCTest):

    pass


class RebalanceTest(DailyTest, RebalanceKVTest):

    pass


class CombinedLatencyAndRebalanceCDCTest(DailyTest, _CombinedLatencyAndRebalanceCDCTest):

    def report_latency_kpi(self):
        if self.test_config.stats_settings.enabled:
            percentiles = self.test_config.access_settings.latency_percentiles
            for operation in ('get', 'set', 'durable_set'):
                for metric in self.metrics.kv_latency(operation=operation, percentiles=percentiles):
                    self.reporter.post(*metric)

    def _report_kpi(self, *args):
        self.reporter.post(
            *self.metrics.rebalance_time(self.rebalance_time)
        )

from perfdaily import DailyTest
from perfrunner.tests.dcp import DCPThroughputTest as _DCPThroughputTest
from perfrunner.tests.kv import PillowFightTest as _PillowFightTest
from perfrunner.tests.rebalance import RebalanceKVTest


class DCPThroughputTest(DailyTest, _DCPThroughputTest):

    def _report_kpi(self, throughput):
        self.reporter.post_to_daily(metric='Avg Throughput (items/sec)',
                                    value=throughput)


class PillowFightTest(DailyTest, _PillowFightTest):

    def _report_kpi(self):
        self.reporter.post_to_daily(
            *self.metric_helper.calc_max_ops()
        )


class RebalanceTest(DailyTest, RebalanceKVTest):

    def _report_kpi(self, rebalance_time):
        self.reporter.post_to_daily(metric='Rebalance Time (min)',
                                    value=rebalance_time)

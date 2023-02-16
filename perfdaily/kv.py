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

    pass

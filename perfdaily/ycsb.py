from perfdaily import DailyTest
from perfrunner.tests.ycsb2 import YCSBN1QLThroughputTest as N1QLThroughputTest
from perfrunner.tests.ycsb2 import YCSBThroughputTest as ThroughputTest


class YCSBThroughputTest(DailyTest, ThroughputTest):

    pass


class YCSBN1QLThroughputTest(DailyTest, N1QLThroughputTest):

    pass

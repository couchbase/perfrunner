from perfdaily import DailyTest
from perfrunner.tests.magma import MagmaBenchmarkTest as _MagmaBenchmarkTest


class MagmaBenchmarkTest(DailyTest, _MagmaBenchmarkTest):

    pass

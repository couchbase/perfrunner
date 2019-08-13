from perfdaily import DailyTest
from perfrunner.tests.analytics import BigFunQueryTest as _BigFunQueryTest
from perfrunner.tests.analytics import BigFunSyncTest as _BigFunSyncTest


class BigFunQueryTest(DailyTest, _BigFunQueryTest):

    pass


class BigFunSyncTest(DailyTest, _BigFunSyncTest):

    pass

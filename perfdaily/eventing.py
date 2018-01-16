from perfdaily import DailyTest
from perfrunner.tests.eventing import FunctionsThroughputTest as _FunctionsTT
from perfrunner.tests.eventing import TimerThroughputTest as _TimersTT


class FunctionsThroughputTest(DailyTest, _FunctionsTT):

    pass


class TimerThroughputTest(DailyTest, _TimersTT):

    pass

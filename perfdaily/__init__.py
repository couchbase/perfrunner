from perfrunner.helpers.metrics import DailyMetricHelper
from perfrunner.tests import PerfTest


class DailyTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(DailyTest, self).__init__(*args, **kwargs)
        self.metric_helper = DailyMetricHelper(self)

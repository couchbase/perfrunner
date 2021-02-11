from perfrunner.helpers.metrics import DailyMetricHelper
from perfrunner.helpers.reporter import DailyReporter
from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.tests import PerfTest


class DailyTest(PerfTest):

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 *args):
        super().__init__(cluster_spec, test_config, *args)

        self.metrics = DailyMetricHelper(self)

        self.reporter = DailyReporter(cluster_spec, test_config, self.build)

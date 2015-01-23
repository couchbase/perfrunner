
from perfrunner.tests import PerfTest


class SingleNodeTest(PerfTest):

    def run(self):
        self.load()

        self.workload = self.test_config.access_settings
        self.access()

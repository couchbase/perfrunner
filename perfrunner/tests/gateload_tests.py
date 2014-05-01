from perfrunner.tests import PerfTest


class SyncGatewayGateloadTest(PerfTest):

    def start_gateloads(self):
        self.remote.kill_processes_gl()
        self.remote.start_gateload(self.test_config)

    def run(self):
        self.start_gateloads()

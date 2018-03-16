from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest


class DCPThroughputTest(PerfTest):

    def _report_kpi(self, time_elapsed: float):
        self.reporter.post(
            *self.metrics.dcp_throughput(time_elapsed)
        )

    @with_stats
    @timeit
    def access(self, *args):
        username, password = self.cluster_spec.rest_credentials

        for target in self.target_iterator:
            local.run_dcptest(
                host=target.node,
                username=username,
                password=password,
                bucket=target.bucket,
                num_items=self.test_config.load_settings.items,
                num_connections=self.test_config.dcp_settings.num_connections
            )

    def run(self):
        self.load()
        self.wait_for_persistence()

        time_elapsed = self.access()

        self.report_kpi(time_elapsed)

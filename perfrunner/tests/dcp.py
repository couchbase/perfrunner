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
        self.compact_bucket()

        time_elapsed = self.access()

        self.report_kpi(time_elapsed)


class JavaDCPThroughputTest(DCPThroughputTest):

    def init_java_dcp_client(self):
        local.clone_git_repo(repo=self.test_config.java_dcp_settings.repo,
                             branch=self.test_config.java_dcp_settings.branch)
        local.build_java_dcp_client()

    @with_stats
    @timeit
    def access(self, *args):
        for target in self.target_iterator:
            local.run_java_dcp_client(
                connection_string=target.connection_string,
                messages=self.test_config.load_settings.items,
                config_file=self.test_config.java_dcp_settings.config,
            )

    def run(self):
        self.init_java_dcp_client()

        super().run()

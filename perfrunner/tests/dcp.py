from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import java_dcp_client_task
from perfrunner.tests import PerfTest


class DCPThroughputTest(PerfTest):

    def _report_kpi(self, time_elapsed: float, clients: int, stream: str):
        self.reporter.post(
            *self.metrics.dcp_throughput(time_elapsed, clients, stream)
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
        self.check_num_items()
        self.compact_bucket()

        time_elapsed = self.access()

        self.report_kpi(time_elapsed,
                        int(self.test_config.java_dcp_settings.clients),
                        self.test_config.java_dcp_settings.stream)


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


class JavaDCPCollectionThroughputTest(DCPThroughputTest):

    def init_java_dcp_clients(self):
        if self.worker_manager.is_remote:
            self.remote.init_java_dcp_client(repo=self.test_config.java_dcp_settings.repo,
                                             branch=self.test_config.java_dcp_settings.branch,
                                             worker_home=self.worker_manager.WORKER_HOME,
                                             commit=self.test_config.java_dcp_settings.commit)

        else:
            local.clone_git_repo(repo=self.test_config.java_dcp_settings.repo,
                                 branch=self.test_config.java_dcp_settings.branch,
                                 commit=self.test_config.java_dcp_settings.commit)
            local.build_java_dcp_client()

    @with_stats
    @timeit
    @with_profiles
    def access(self, *args, **kwargs):
        access_settings = self.test_config.access_settings
        access_settings.workload_instances = int(self.test_config.java_dcp_settings.clients)
        PerfTest.access(self, task=java_dcp_client_task, settings=access_settings)

    def run(self):
        self.init_java_dcp_clients()
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()

        if self.test_config.access_settings.workers > 0:
            self.access_bg()

        time_elapsed = self.access()

        self.report_kpi(time_elapsed,
                        int(self.test_config.java_dcp_settings.clients),
                        self.test_config.java_dcp_settings.stream)

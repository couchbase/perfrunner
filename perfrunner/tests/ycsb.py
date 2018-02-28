import os

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.worker import ycsb_data_load_task, ycsb_task
from perfrunner.tests import PerfTest
from perfrunner.tests.n1ql import N1QLTest


class YCSBTest(PerfTest):

    def download_ycsb(self):
        if self.worker_manager.is_remote:
            self.remote.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                                   branch=self.test_config.ycsb_settings.branch,
                                   worker_home=self.worker_manager.WORKER_HOME)
        else:
            local.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                             branch=self.test_config.ycsb_settings.branch)

    def collect_export_files(self):
        if self.worker_manager.is_remote:
            os.mkdir('YCSB')
            self.remote.get_export_files(self.worker_manager.WORKER_HOME)

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=ycsb_data_load_task)

    @with_stats
    def access(self, *args, **kwargs):
        PerfTest.access(self, task=ycsb_task)

    def generate_keystore(self):
        if self.worker_manager.is_remote:
            self.remote.generate_ssl_keystore(self.ROOT_CERTIFICATE,
                                              self.test_config.access_settings
                                              .ssl_keystore_file,
                                              self.test_config.access_settings
                                              .ssl_keystore_password,
                                              self.worker_manager.WORKER_HOME)
        else:
            local.generate_ssl_keystore(self.ROOT_CERTIFICATE,
                                        self.test_config.access_settings.ssl_keystore_file,
                                        self.test_config.access_settings.ssl_keystore_password)

    def run(self):
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.access()

        self.report_kpi()


class YCSBThroughputTest(YCSBTest):

    def _report_kpi(self):
        self.collect_export_files()

        self.reporter.post(
            *self.metrics.ycsb_throughput()
        )


class YCSBSOETest(YCSBThroughputTest, N1QLTest):

    def run(self):
        self.download_ycsb()
        self.restore()
        self.wait_for_persistence()

        self.build_indexes()

        self.load()

        self.access()

        self.report_kpi()


class YCSBN1QLTest(YCSBTest, N1QLTest):

    def run(self):
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.build_indexes()

        self.access()

        self.report_kpi()


class YCSBN1QLThroughputTest(YCSBN1QLTest, YCSBThroughputTest):

    pass

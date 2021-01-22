import os
import shutil
import time

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import ycsb_data_load_task, ycsb_task
from perfrunner.tests import PerfTest
from perfrunner.tests.n1ql import N1QLTest


class YCSBTest(PerfTest):

    def download_ycsb(self):
        if self.worker_manager.is_remote:
            self.remote.init_ycsb(
                    repo=self.test_config.ycsb_settings.repo,
                    branch=self.test_config.ycsb_settings.branch,
                    worker_home=self.worker_manager.WORKER_HOME,
                    sdk_version=self.test_config.ycsb_settings.sdk_version)
        else:
            local.clone_git_repo(repo=self.test_config.ycsb_settings.repo,
                                 branch=self.test_config.ycsb_settings.branch)

    def collect_export_files(self):
        if self.worker_manager.is_remote:
            shutil.rmtree("YCSB", ignore_errors=True)
            os.mkdir('YCSB')
            self.remote.get_export_files(self.worker_manager.WORKER_HOME)

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=ycsb_data_load_task)

    @with_stats
    @with_profiles
    def access(self, *args, **kwargs):
        PerfTest.access(self, task=ycsb_task)
        self.wait_for_persistence()

    def access_bg(self, *args, **kwargs):
        PerfTest.access_bg(self, task=ycsb_task)

    @with_stats
    def collect_cb(self):
        duration = self.test_config.access_settings.time
        self.cb_start = duration*0.6
        time.sleep(self.cb_start)
        start_time = time.time()
        self.remote.collect_info()
        end_time = time.time()
        self.cb_time = round(end_time - start_time)
        self.worker_manager.wait_for_workers()

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
        if self.dynamic_infra:
            time.sleep(30)
        else:
            self.wait_for_persistence()
            self.check_num_items()

        if self.test_config.access_settings.cbcollect:
            self.access_bg()
            self.collect_cb()
        else:
            self.access()

        self.report_kpi()


class YCSBThroughputTest(YCSBTest):

    COLLECTORS = {'disk': True, 'net': True}

    def _report_kpi(self):
        self.collect_export_files()

        self.reporter.post(
            *self.metrics.ycsb_throughput()
        )


class YCSBDurabilityThroughputTest(YCSBTest):

    COLLECTORS = {'disk': True, 'net': True}

    def log_latency_percentiles(self, type: str, percentiles):
        for percentile in percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and type in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    logger.info("{}: {}".format(key, latency_dic[key]))

    def log_percentiles(self):
        logger.info("------------------")
        logger.info("Latency Percentiles")
        logger.info("-------READ-------")
        self.log_latency_percentiles("READ", [95, 96, 97, 98, 99])
        logger.info("------UPDATE------")
        self.log_latency_percentiles("UPDATE", [95, 96, 97, 98, 99])
        logger.info("------------------")

    def _report_kpi(self):
        self.collect_export_files()

        self.log_percentiles()

        for key, value in self.metrics.ycsb_get_max_latency().items():
            max_latency, _, _ = self.metrics.ycsb_slo_max_latency(key, value)
            logger.info("Max {} Latency: {}".format(key, max_latency))

        for key, value in self.metrics.ycsb_get_failed_ops().items():
            failures, _, _ = self.metrics.ycsb_failed_ops(key, value)
            logger.info("{} Failures: {}".format(key, failures))

        gcs, _, _ = self.metrics.ycsb_gcs()
        logger.info("Garbage Collections: {}".format(gcs))

        self.reporter.post(
            *self.metrics.ycsb_durability_throughput()
        )

        for percentile in self.test_config.ycsb_settings.latency_percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_slo_latency(key, latency_dic[key])
                    )


class YCSBLatencyTest(YCSBTest):

    COLLECTORS = {'disk': True, 'net': True}

    def _report_kpi(self):
        self.collect_export_files()

        for percentile in self.test_config.ycsb_settings.latency_percentiles:
            latency_dic = self.metrics.ycsb_get_latency(percentile=percentile)
            for key, value in latency_dic.items():
                if str(percentile) in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency(key, latency_dic[key])
                    )

        if self.test_config.ycsb_settings.average_latency == 1:
            latency_dic = self.metrics.ycsb_get_latency(
                percentile=99)

            for key, value in latency_dic.items():
                if "Average" in key \
                        and "CLEANUP" not in key \
                        and "FAILED" not in key:
                    self.reporter.post(
                        *self.metrics.ycsb_latency(key, latency_dic[key])
                    )


class YCSBSOETest(YCSBThroughputTest, N1QLTest):

    def run(self):
        self.download_ycsb()
        self.restore()
        self.wait_for_persistence()

        self.create_indexes()
        self.wait_for_indexing()

        self.load()

        self.access()

        self.report_kpi()


class YCSBN1QLTest(YCSBTest, N1QLTest):

    def run(self):
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.create_indexes()
        self.wait_for_indexing()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.wait_for_indexing()

        if self.test_config.access_settings.cbcollect:
            self.access_bg()
            self.collect_cb()
        else:
            self.access()

        self.report_kpi()


class YCSBN1QLLatencyTest(YCSBN1QLTest, YCSBLatencyTest):

    pass


class YCSBN1QLThroughputTest(YCSBN1QLTest, YCSBThroughputTest):

    pass

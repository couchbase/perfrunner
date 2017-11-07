import os
import shutil
import glob

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.helpers.worker import syncgateway_task_init_users, syncgateway_task_load_users, \
    syncgateway_task_load_docs, syncgateway_task_run_test, syncgateway_task_start_memcached

from perfrunner.helpers import local

from typing import Callable

from logger import logger

from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import ShowFastReporter
from perfrunner.helpers.worker import  WorkerManager

from perfrunner.settings import (
    ClusterSpec,
    PhaseSettings,
    TargetIterator,
    TestConfig,
)


class SGPerfTest(PerfTest):

    COLLECTORS = {'disk': False, 'ns_server': False, 'ns_server_overview': False, 'active_tasks': False}
    ALL_HOSTNAMES = True
    LOCAL_DIR = "YCSB"

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.memcached = MemcachedHelper(test_config)
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)
        self.build = os.environ.get('SGBUILD') or "0.0.0-000"
        self.metrics = MetricHelper(self)
        self.reporter = ShowFastReporter(cluster_spec, test_config, self.build)
        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config, verbose)
        self.settings = self.test_config.access_settings
        self.settings.syncgateway_settings = self.test_config.syncgateway_settings

    def download_ycsb(self):
        if self.worker_manager.is_remote:
            self.remote.clone_ycsb(repo=self.test_config.syncgateway_settings.repo,
                                   branch=self.test_config.syncgateway_settings.branch,
                                   worker_home=self.worker_manager.WORKER_HOME,
                                   ycsb_instances=int(self.test_config.syncgateway_settings.instances_per_client))
        else:
            local.clone_ycsb(repo=self.test_config.syncgateway_settings.repo,
                             branch=self.test_config.syncgateway_settings.branch)

    def collect_execution_logs(self):
        if self.worker_manager.is_remote:
            if os.path.exists(self.LOCAL_DIR):
                shutil.rmtree(self.LOCAL_DIR, ignore_errors=True)
            os.makedirs(self.LOCAL_DIR)
            self.remote.get_syncgateway_YCSB_logs(self.worker_manager.WORKER_HOME,
                                                  self.test_config.syncgateway_settings, self.LOCAL_DIR)

    def run_sg_phase(self,
                  phase: str,
                  task: Callable, settings: PhaseSettings,
                  timer: int = None,
                  distribute: bool = False) -> None:
        logger.info('Running {}: {}'.format(phase, pretty_dict(settings)))
        self.worker_manager.run_sg_tasks(task, settings, timer, distribute, phase)
        self.worker_manager.wait_for_workers()

    def start_memcached(self):
        self.run_sg_phase("start memcached", syncgateway_task_start_memcached, self.settings, self.settings.time, False)

    def load_users(self):
        self.run_sg_phase("load users", syncgateway_task_load_users, self.settings, self.settings.time, False)

    def init_users(self):
        self.run_sg_phase("init users", syncgateway_task_init_users, self.settings, self.settings.time, False)

    def load_docs(self):
        self.run_sg_phase("load docs", syncgateway_task_load_docs, self.settings, self.settings.time, False)

    @with_stats
    def run_test(self):
        self.run_sg_phase("run test", syncgateway_task_run_test, self.settings, self.settings.time, True)


    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.load_docs()
        self.init_users()
        self.run_test()
        self.report_kpi()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test_config.test_case.use_workers:
            self.worker_manager.download_celery_logs()
            self.worker_manager.terminate()

        if self.test_config.cluster.online_cores:
            self.remote.enable_cpu()

        if self.test_config.cluster.kernel_mem_limit:
            self.remote.reset_memory_settings()
            self.monitor.wait_for_servers()

class SGRead(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), GET doc by id")
        )

class SGSync(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput('Throughput (req/sec), GET docs via _changes')
        )

        self.reporter.post(
            *self.metrics.sg_latency('[INSERT], 95thPercentileLatency(us)',
                                     'Latency, round-trip write, 95 percentile (ms)')
        )

class SGWrite(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), POST doc")
        )
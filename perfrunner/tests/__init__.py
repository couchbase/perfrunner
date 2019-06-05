import time
from typing import Callable

from logger import logger

from perfrunner.helpers.index import IndexHelper
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import ShowFastReporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.restore import RestoreHelper
from perfrunner.helpers.worker import spring_task, WorkerManager
from perfrunner.settings import (
    ClusterSpec,
    PhaseSettings,
    TargetIterator,
    TestConfig,
)


class PerfTest:

    COLLECTORS = {}

    ROOT_CERTIFICATE = 'root.pem'

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(cluster_spec, test_config)

        self.memcached = MemcachedHelper(test_config)
        self.monitor = Monitor(cluster_spec, test_config, verbose)
        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)
        self.index = IndexHelper(cluster_spec, test_config, self.rest, self.monitor)

        self.master_node = next(cluster_spec.masters)
        self.build = "5.0.1-1301"  # self.rest.get_version(self.master_node)

        self.metrics = MetricHelper(self)
        self.reporter = ShowFastReporter(cluster_spec, test_config, self.build)

        self.cbmonitor_snapshots = []
        self.cbmonitor_clusters = []

        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config,
                                                verbose)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test_config.test_case.use_workers:
            self.worker_manager.download_celery_logs()
            self.worker_manager.terminate()

        if self.test_config.cluster.online_cores:
            self.remote.enable_cpu()

        if exc_type != KeyboardInterrupt:
            self.check_core_dumps()
            self.check_rebalance()
            self.check_failover()

        if self.test_config.cluster.kernel_mem_limit:
            self.remote.reset_memory_settings()
            self.monitor.wait_for_servers()

    def download_certificate(self) -> None:
        cert = self.rest.get_certificate(self.master_node)
        with open(self.ROOT_CERTIFICATE, 'w') as fh:
            fh.write(cert)

    def check_rebalance(self) -> None:
        for master in self.cluster_spec.masters:
            if self.rest.is_not_balanced(master):
                logger.interrupt('The cluster is not balanced')

    def check_failover(self) -> None:
        if hasattr(self, 'rebalance_settings'):
            if self.rebalance_settings.failover or \
                    self.rebalance_settings.graceful_failover:
                return

        for master in self.cluster_spec.masters:
            num_failovers = self.rest.get_failover_counter(master)
            if num_failovers:
                logger.interrupt(
                    'Failover happened {} time(s)'.format(num_failovers)
                )

    def check_core_dumps(self) -> None:
        dumps_per_host = self.remote.detect_core_dumps()
        core_dumps = {
            host: dumps for host, dumps in dumps_per_host.items() if dumps
        }
        if core_dumps:
            logger.interrupt(pretty_dict(core_dumps))

    def compact_bucket(self, wait: bool = True) -> None:
        for target in self.target_iterator:
            self.rest.trigger_bucket_compaction(target.node, target.bucket)

        if wait:
            for target in self.target_iterator:
                self.monitor.monitor_task(target.node, 'bucket_compaction')

    def wait_for_persistence(self) -> None:
        for target in self.target_iterator:
            self.monitor.monitor_disk_queues(target.node, target.bucket)
            self.monitor.monitor_dcp_queues(target.node, target.bucket)

    def check_num_items(self) -> None:
        for target in self.target_iterator:
            self.monitor.monitor_num_items(target.node, target.bucket,
                                           self.test_config.load_settings.items)

    def restore(self) -> None:
        with RestoreHelper(self.cluster_spec, self.test_config) as rh:
            rh.restore()
            rh.warmup()

    def run_phase(self,
                  phase: str,
                  task: Callable, settings: PhaseSettings,
                  target_iterator: TargetIterator,
                  timer: int = None,
                  wait: bool = True) -> None:
        logger.info('Running {}: {}'.format(phase, pretty_dict(settings)))
        self.worker_manager.run_tasks(task, settings, target_iterator, timer)
        if wait:
            self.worker_manager.wait_for_workers()

    def load(self,
             task: Callable = spring_task,
             settings: PhaseSettings = None,
             target_iterator: TargetIterator = None) -> None:
        if settings is None:
            settings = self.test_config.load_settings
        if target_iterator is None:
            target_iterator = self.target_iterator

        self.run_phase('load phase',
                       task, settings, target_iterator)

    def hot_load(self,
                 task: Callable = spring_task) -> None:
        settings = self.test_config.hot_load_settings

        self.run_phase('hot load phase',
                       task, settings, self.target_iterator)

    def access(self,
               task: Callable = spring_task,
               settings: PhaseSettings = None) -> None:
        if settings is None:
            settings = self.test_config.access_settings

        self.run_phase('access phase',
                       task, settings, self.target_iterator, settings.time)

    def access_bg(self, task: Callable = spring_task,
                  settings: PhaseSettings = None,
                  target_iterator: TargetIterator = None) -> None:
        if settings is None:
            settings = self.test_config.access_settings
        if target_iterator is None:
            target_iterator = self.target_iterator

        self.run_phase('background access phase',
                       task, settings, target_iterator, settings.time, False)

    def sleep(self) -> None:
        access_settings = self.test_config.access_settings
        logger.info('Running phase for {} seconds'.format(access_settings.time))
        time.sleep(access_settings.time)

    def report_kpi(self, *args, **kwargs) -> None:
        if self.test_config.stats_settings.enabled:
            self._report_kpi(*args, **kwargs)

    def _report_kpi(self, *args, **kwargs) -> None:
        pass

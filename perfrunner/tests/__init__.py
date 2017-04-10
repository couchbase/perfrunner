import time
from typing import Callable

from logger import logger

from perfrunner.helpers.cbmonitor import CbAgent
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.restore import RestoreHelper
from perfrunner.helpers.worker import spring_task, WorkerManager
from perfrunner.settings import TargetIterator, PhaseSettings


class PerfTest:

    COLLECTORS = {}

    MONITORING_DELAY = 10

    ROOT_CERTIFICATE = 'root.pem'

    def __init__(self, cluster_spec, test_config, verbose):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(cluster_spec, test_config)

        self.memcached = MemcachedHelper(test_config)
        self.monitor = Monitor(cluster_spec)
        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)
        self.restore_helper = RestoreHelper(cluster_spec, test_config, verbose)

        self.master_node = next(cluster_spec.yield_masters())
        self.build = self.rest.get_version(self.master_node)

        self.cbagent = CbAgent(self, verbose=verbose)
        self.metric_helper = MetricHelper(self)
        self.reporter = Reporter(self)
        self.reports = {}
        self.snapshots = []

        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config,
                                                self.remote)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test_config.cluster.throttle_cpu:
            self.remote.enable_cpu()

        if self.test_config.test_case.use_workers:
            self.worker_manager.terminate()

        if exc_type != KeyboardInterrupt:
            self.check_core_dumps()
            self.check_rebalance()
            self.check_failover()

    def download_certificate(self) -> None:
        cert = self.rest.get_certificate(self.master_node)
        with open(self.ROOT_CERTIFICATE, 'w') as fh:
            fh.write(cert)

    def check_rebalance(self) -> None:
        for master in self.cluster_spec.yield_masters():
            if self.rest.is_not_balanced(master):
                logger.interrupt('The cluster is not balanced')

    def check_failover(self) -> None:
        if hasattr(self, 'rebalance_settings'):
            if self.rebalance_settings.failover or \
                    self.rebalance_settings.graceful_failover:
                return

        for master in self.cluster_spec.yield_masters():
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

    def compact_bucket(self) -> None:
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                self.rest.trigger_bucket_compaction(master, bucket)
        time.sleep(self.MONITORING_DELAY)
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_task(master, 'bucket_compaction')

    def wait_for_persistence(self) -> None:
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                self.monitor.monitor_disk_queues(master, bucket)
                self.monitor.monitor_dcp_queues(master, bucket)

    def check_num_items(self) -> None:
        for target in self.target_iterator:
            stats = self.rest.get_bucket_stats(host_port=target.node,
                                               bucket=target.bucket)
            curr_items = stats['op']['samples'].get('curr_items')[-1]

            if curr_items != self.test_config.load_settings.items:
                logger.interrupt('Mismatch in the number of items: {}'
                                 .format(curr_items))

    def restore(self) -> None:
        self.restore_helper.restore()
        self.restore_helper.warmup()

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

        self.run_phase('load phase',
                       task, settings, self.target_iterator)

    def access(self,
               task: Callable = spring_task,
               settings: PhaseSettings = None) -> None:
        if settings is None:
            settings = self.test_config.access_settings

        self.run_phase('access phase',
                       task, settings, self.target_iterator)

    def access_bg(self, task: Callable = spring_task,
                  settings: PhaseSettings = None,
                  target_iterator: TargetIterator = None) -> None:
        if settings is None:
            settings = self.test_config.access_settings
        if target_iterator is None:
            target_iterator = self.target_iterator

        settings.index_type = self.test_config.index_settings.index_type
        settings.ddocs = getattr(self, 'ddocs', None)

        self.run_phase('background access phase',
                       task, settings, target_iterator, settings.time, False)

    def timer(self) -> None:
        access_settings = self.test_config.access_settings
        logger.info('Running phase for {} seconds'.format(access_settings.time))
        time.sleep(access_settings.time)

    def report_kpi(self, *args, **kwargs) -> None:
        if self.test_config.stats_settings.enabled:
            self._report_kpi(*args, **kwargs)

    def _report_kpi(self, *args, **kwargs) -> None:
        pass

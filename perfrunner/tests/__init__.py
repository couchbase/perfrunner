import exceptions as exc
import os
import shutil
import time
from multiprocessing import Event, Process

from logger import logger

from perfrunner.helpers.cbmonitor import CbAgent
from perfrunner.helpers.experiments import ExperimentHelper
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import log_phase, target_hash
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.worker import WorkerManager
from perfrunner.settings import TargetSettings


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config, prefix=None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.prefix = prefix

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        prefix = self.prefix
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.get_buckets():
                if self.prefix is None:
                    prefix = target_hash(master.split(':')[0])
                yield TargetSettings(master, bucket, username, password, prefix)


class PerfTest(object):

    COLLECTORS = {}

    def __init__(self, cluster_spec, test_config, experiment=None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config)

        self.memcached = MemcachedHelper(cluster_spec)
        self.monitor = Monitor(cluster_spec)
        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec)
        self.worker_manager = WorkerManager(cluster_spec)

        if experiment:
            self.experiment = ExperimentHelper(experiment,
                                               cluster_spec, test_config)

        self.master_node = cluster_spec.yield_masters().next()
        self.build = self.rest.get_version(self.master_node)

        self.cbagent = CbAgent(cluster_spec, test_config, self.build)
        self.metric_helper = MetricHelper(self)
        self.reporter = Reporter(self)
        self.reports = {}
        self.snapshots = []

        self.shutdown_event = Event()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.worker_manager.terminate()
        if exc_type != exc.KeyboardInterrupt:
            self.debug()
        for master in self.cluster_spec.yield_masters():
            num_failovers = self.rest.get_failover_counter(master)
            if num_failovers:
                logger.interrupt(
                    'Failover happened {} time(s)'.format(num_failovers)
                )

    def compact_bucket(self):
        for target in self.target_iterator:
            self.rest.trigger_bucket_compaction(target.node,
                                                target.bucket)
            self.monitor.monitor_task(target, 'bucket_compaction')

    def wait_for_persistence(self):
        for target in self.target_iterator:
            self.monitor.monitor_disk_queue(target)
            self.monitor.monitor_tap_replication(target)

    def load(self):
        load_settings = self.test_config.get_load_settings()
        log_phase('load phase', load_settings)
        self.worker_manager.run_workload(load_settings, self.target_iterator)

    def hot_load(self):
        hot_load_settings = self.test_config.get_hot_load_settings()

        if '2.0.0' < self.build < '2.1.0':
            log_phase('hot load phase', hot_load_settings)
            self.worker_manager.run_workload(hot_load_settings,
                                             self.target_iterator)

        hot_load_settings.seq_updates = False
        log_phase('hot load phase', hot_load_settings)
        self.worker_manager.run_workload(hot_load_settings,
                                         self.target_iterator)

    def access(self):
        access_settings = self.test_config.get_access_settings()
        log_phase('access phase', access_settings)
        self.worker_manager.run_workload(access_settings, self.target_iterator)

    def access_bg(self):
        access_settings = self.test_config.get_access_settings()
        log_phase('access in background', access_settings)
        p = Process(
            target=self.worker_manager.run_workload,
            args=(access_settings, self.target_iterator),
            kwargs={'shutdown_event': self.shutdown_event}
        )
        p.start()
        return p

    def timer(self):
        access_settings = self.test_config.get_access_settings()
        logger.info('Running phase for {} seconds'.format(access_settings.time))
        time.sleep(access_settings.time)

    def debug(self):
        self.remote.collect_info()
        self.reporter.save_web_logs()
        for root, _, files in os.walk('.'):
            for f in files:
                if f.endswith('.zip'):
                    shutil.move(os.path.join(root, f), '.')

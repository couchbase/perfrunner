import exceptions as exc
import glob
import shutil
import sys
import time

from logger import logger

from perfrunner.helpers.cbmonitor import CbAgent
from perfrunner.helpers.experiments import ExperimentHelper
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import log_phase, target_hash, pretty_dict
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper, SyncGatewayRequestHelper
from perfrunner.helpers.worker import WorkerManager
from perfrunner.settings import TargetSettings


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config, prefix=None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.prefix = prefix

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                if self.prefix is None:
                    prefix = target_hash(master.split(':')[0])
                yield TargetSettings(master, bucket, password, prefix)


class PerfTest(object):

    COLLECTORS = {}

    MONITORING_DELAY = 10

    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(cluster_spec, test_config)

        self.memcached = MemcachedHelper(test_config)
        self.monitor = Monitor(cluster_spec)
        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)

        if experiment:
            self.experiment = ExperimentHelper(experiment,
                                               cluster_spec, test_config)

        self.master_node = cluster_spec.yield_masters().next()
        if self.remote and self.remote.gateways:
            self.build = SyncGatewayRequestHelper().get_version(
                self.remote.gateways[0]
            )
        else:
            self.build = self.rest.get_version(self.master_node)

        self.cbagent = CbAgent(self)
        self.metric_helper = MetricHelper(self)
        self.reporter = Reporter(self)
        self.reports = {}
        self.snapshots = []
        self.master_events = []

        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test_config.test_case.use_workers:
            self.worker_manager.terminate()
        if exc_type != exc.KeyboardInterrupt and '--nodebug' not in sys.argv:
            self.debug()

            self.check_core_dumps()
            for master in self.cluster_spec.yield_masters():
                if not self.rest.is_balanced(master):
                    logger.interrupt('Rebalance failed')
                self.check_failover(master)

    def check_failover(self, master):
        if hasattr(self, 'rebalance_settings'):
            if self.rebalance_settings.failover or \
                    self.rebalance_settings.graceful_failover:
                return

        num_failovers = self.rest.get_failover_counter(master)
        if num_failovers:
            logger.interrupt(
                'Failover happened {} time(s)'.format(num_failovers)
            )

    def check_core_dumps(self):
        dumps_per_host = self.remote.detect_core_dumps()
        core_dumps = {
            host: dumps for host, dumps in dumps_per_host.items() if dumps
        }
        if core_dumps:
            logger.interrupt(pretty_dict(core_dumps))

    def compact_bucket(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                self.rest.trigger_bucket_compaction(master, bucket)
        time.sleep(self.MONITORING_DELAY)
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_task(master, 'bucket_compaction')

    def wait_for_persistence(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                self.monitor.monitor_disk_queues(master, bucket)
                self.monitor.monitor_tap_queues(master, bucket)
                self.monitor.monitor_upr_queues(master, bucket)

    def load(self, load_settings=None, target_iterator=None):
        if load_settings is None:
            load_settings = self.test_config.load_settings
        if target_iterator is None:
            target_iterator = self.target_iterator
        if self.test_config.spatial_settings:
            load_settings.spatial = self.test_config.spatial_settings
        log_phase('load phase', load_settings)
        self.worker_manager.run_workload(load_settings, target_iterator)
        self.worker_manager.wait_for_workers()

    def hot_load(self):
        hot_load_settings = self.test_config.hot_load_settings
        if self.test_config.spatial_settings:
            hot_load_settings.spatial = self.test_config.spatial_settings

        log_phase('hot load phase', hot_load_settings)
        self.worker_manager.run_workload(hot_load_settings,
                                         self.target_iterator)
        self.worker_manager.wait_for_workers()

    def access(self, access_settings=None):
        if access_settings is None:
            access_settings = self.test_config.access_settings
        if self.test_config.spatial_settings:
            access_settings.spatial = self.test_config.spatial_settings

        log_phase('access phase', access_settings)
        self.worker_manager.run_workload(access_settings, self.target_iterator)
        self.worker_manager.wait_for_workers()

    def access_bg(self, access_settings=None):
        if access_settings is None:
            access_settings = self.test_config.access_settings
        if self.test_config.spatial_settings:
            access_settings.spatial = self.test_config.spatial_settings

        log_phase('access phase in background', access_settings)
        access_settings.index_type = self.test_config.index_settings.index_type
        access_settings.ddocs = getattr(self, 'ddocs', None)
        self.worker_manager.run_workload(access_settings, self.target_iterator,
                                         timer=access_settings.time)

    def timer(self):
        access_settings = self.test_config.access_settings
        logger.info('Running phase for {} seconds'.format(access_settings.time))
        time.sleep(access_settings.time)

    def debug(self):
        self.remote.collect_info()
        for hostname in self.cluster_spec.yield_hostnames():
            for fname in glob.glob('{}/*.zip'.format(hostname)):
                shutil.move(fname, '{}.zip'.format(hostname))

        self.reporter.save_web_logs()

        if self.test_config.cluster.run_cbq:
            self.remote.collect_cbq_logs()
            for hostname in self.cluster_spec.yield_hostnames():
                for fname in glob.glob('{}/cbq.log'.format(hostname)):
                    shutil.move(fname, '{}-cbq.log'.format(hostname))

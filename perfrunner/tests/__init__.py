from hashlib import md5

from logger import logger

from perfrunner.helpers.cbmonitor import CbAgent
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.worker import WorkerManager
from perfrunner.settings import TargetSettings


def target_hash(*args):
    int_hash = hash(args)
    str_hash = md5(hex(int_hash)).hexdigest()
    return str_hash[:6]


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config, prefix=None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.prefix = prefix

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        prefix = self.prefix
        for master in self.cluster_spec.get_masters().values():
            for bucket in self.test_config.get_buckets():
                if self.prefix is None:
                    prefix = target_hash(master, bucket)
                yield TargetSettings(master, bucket, username, password, prefix)


class PerfTest(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config)

        self.cbagent = CbAgent(cluster_spec)
        self.monitor = Monitor(cluster_spec)
        self.rest = RestHelper(cluster_spec)
        self.reporter = Reporter(self)
        self.remote = RemoteHelper(cluster_spec)
        self.worker_manager = WorkerManager(cluster_spec)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.worker_manager.terminate()
        self.debug()

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
        logger.info('Running load phase: {0}'.format(load_settings))
        self.worker_manager.run_workload(load_settings, self.target_iterator)

    def hot_load(self):
        hot_load_settings = self.test_config.get_hot_load_settings()
        master_node = self.cluster_spec.get_masters().values()[0]
        if self.rest.get_version(master_node) > '2.0.1':
            hot_load_settings.seq_updates = False
        logger.info('Running hot load phase: {0}'.format(hot_load_settings))
        self.worker_manager.run_workload(hot_load_settings, self.target_iterator)

    def access(self):
        access_settings = self.test_config.get_access_settings()
        logger.info('Running access phase: {0}'.format(access_settings))
        self.worker_manager.run_workload(access_settings, self.target_iterator)

    def debug(self):
        self.remote.collect_info()
        self.reporter.save_web_logs()

from hashlib import md5

from logger import logger
from spring.wgen import WorkloadGen

from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import Reporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import TargetSettings


def target_hash(*args):
    int_hash = hash(args)
    str_hash = md5(hex(int_hash)).hexdigest()
    return str_hash[:6]


class PerfTest(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config)

        self.monitor = Monitor(cluster_spec, test_config)
        self.rest = RestHelper(cluster_spec)
        self.reporter = Reporter()
        self.remote = RemoteHelper(cluster_spec)

    def _compact_bucket(self):
        for target in self.target_iterator:
            self.rest.trigger_bucket_compaction(target.node,
                                                target.bucket)
            self.monitor.monitor_task(target, 'bucket_compaction')

    def _run_load_phase(self):
        load_settings = self.test_config.get_load_settings()
        if load_settings.ops:
            logger.info('Running load phase: {0} ops'.format(
                load_settings.ops))

            for target in self.target_iterator:
                wg = WorkloadGen(load_settings, target)
                wg.run()
                self.monitor.monitor_disk_queue(target)
                self.monitor.monitor_tap_replication(target)

    def _run_access_phase(self):
        access_settings = self.test_config.get_access_settings()
        if access_settings.ops:
            logger.info('Running access phase: {0} ops'.format(
                access_settings.ops))

            for target in self.target_iterator:
                wg = WorkloadGen(access_settings, target)
                wg.run()
                self.monitor.monitor_disk_queue(target)
                self.monitor.monitor_tap_replication(target)

    def _debug(self):
        self.remote.collect_info()


class TargetIterator(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        for cluster in self.cluster_spec.get_clusters():
            master = cluster[0]
            for bucket in self.test_config.get_buckets():
                prefix = target_hash(master, bucket)
                yield TargetSettings(master, bucket, username, password, prefix)

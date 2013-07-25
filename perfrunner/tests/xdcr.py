from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.settings import TargetSettings
from perfrunner.tests import target_hash, TargetIterator
from perfrunner.tests import PerfTest


class XdcrTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(XdcrTest, self).__init__(*args, **kwargs)
        self.metric_helper = MetricHelper(self)

    def _start_replication(self, m1, m2):
        name = target_hash(m1, m2)
        self.rest.add_remote_cluster(m1, m2, name)

        for bucket in self.test_config.get_buckets():
            self.rest.start_replication(m1, bucket, bucket, name)

    def init_xdcr(self):
        xdcr_settings = self.test_config.get_xdcr_settings()

        m1, m2 = self.cluster_spec.get_masters().values()

        if xdcr_settings.replication_type == 'unidir':
            self._start_replication(m1, m2)
        if xdcr_settings.replication_type == 'bidir':
            self._start_replication(m1, m2)
            self._start_replication(m2, m1)

        for target in self.target_iterator:
            self.monitor.monitor_xdcr_replication(target)

    @with_stats(xdcr_lag=True)
    def access(self):
        super(XdcrTest, self).access()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_xdcr()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.access()
        self.reporter.post_to_sf(
            *self.metric_helper.calc_max_replication_changes_left()
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_max_xdcr_lag()
        )


class SymmetricXdcrTest(XdcrTest):

    def __init__(self, *args, **kwargs):
        super(SymmetricXdcrTest, self).__init__(*args, **kwargs)
        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix="symmetric")


class SrcTargetIterator(TargetIterator):

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        src_master = self.cluster_spec.get_masters().values()[0]
        for bucket in self.test_config.get_buckets():
            prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, username, password, prefix)


class XdcrInitTest(XdcrTest):

    def load(self):
        load_settings = self.test_config.get_load_settings()
        logger.info('Running load phase: {0}'.format(load_settings))
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        self.worker_manager.run_workload(load_settings, src_target_iterator)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.reporter.start()
        self.init_xdcr()
        time_elapsed = self.reporter.finish('Initial replication')
        self.reporter.post_to_sf(
            self.metric_helper.calc_avg_replication_rate(time_elapsed)
        )

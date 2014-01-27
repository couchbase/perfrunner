from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import log_phase, target_hash
from perfrunner.settings import TargetSettings
from perfrunner.tests import TargetIterator
from perfrunner.tests import PerfTest


class XdcrTest(PerfTest):

    COLLECTORS = {'latency': True, 'xdcr_lag': True}

    def __init__(self, *args, **kwargs):
        super(XdcrTest, self).__init__(*args, **kwargs)
        self.settings = self.test_config.get_xdcr_settings()
        self.shutdown_event = None

    def _start_replication(self, m1, m2):
        name = target_hash(m1, m2)
        certificate = self.settings.use_ssl and self.rest.get_certificate(m2)
        self.rest.add_remote_cluster(m1, m2, name, certificate)

        for bucket in self.test_config.get_buckets():
            params = {
                'replicationType': 'continuous',
                'toBucket': bucket,
                'fromBucket': bucket,
                'toCluster': name
            }
            if self.settings.replication_protocol:
                params['type'] = self.settings.replication_protocol
            self.rest.start_replication(m1, params)

    def init_xdcr(self):
        m1, m2 = self.cluster_spec.yield_masters()

        if self.settings.replication_type == 'unidir':
            self._start_replication(m1, m2)
        if self.settings.replication_type == 'bidir':
            self._start_replication(m1, m2)
            self._start_replication(m2, m1)

        for target in self.target_iterator:
            self.monitor.monitor_xdcr_replication(target)

    @with_stats
    def access(self):
        super(XdcrTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_xdcr()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.workload = self.test_config.get_access_settings()
        bg_process = self.access_bg()
        self.access()
        bg_process.terminate()

        self.reporter.post_to_sf(
            *self.metric_helper.calc_replication_changes_left()
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_xdcr_lag()
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_max_beam_rss()
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
        src_master = self.cluster_spec.yield_masters().next()
        for bucket in self.test_config.get_buckets():
            prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, username, password, prefix)


class XdcrInitTest(XdcrTest):

    COLLECTORS = {}

    def load(self):
        load_settings = self.test_config.get_load_settings()
        log_phase('load phase', load_settings)
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        self.worker_manager.run_workload(load_settings, src_target_iterator)

    @with_stats
    def init_xdcr(self):
        super(XdcrInitTest, self).init_xdcr()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        from_ts, to_ts = self.init_xdcr()
        time_elapsed = (to_ts - from_ts) / 1000.0
        rate = self.metric_helper.calc_avg_replication_rate(time_elapsed)

        self.reporter.finish('Initial replication', time_elapsed)
        self.reporter.post_to_sf(rate)
        if hasattr(self, 'experiment'):
            self.experiment.post_results(rate)

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import log_phase, target_hash
from perfrunner.settings import TargetSettings
from perfrunner.tests import PerfTest, TargetIterator
from perfrunner.tests.kv import FlusherTest


class XdcrTest(PerfTest):

    COLLECTORS = {'latency': True, 'xdcr_lag': True}

    ALL_BUCKETS = True

    def __init__(self, *args, **kwargs):
        super(XdcrTest, self).__init__(*args, **kwargs)
        self.settings = self.test_config.xdcr_settings

    def _start_replication(self, m1, m2):
        name = target_hash(m1, m2)
        certificate = self.settings.use_ssl and self.rest.get_certificate(m2)
        self.rest.add_remote_cluster(m1, m2, name, certificate)

        for bucket in self.test_config.buckets:
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
            self.monitor.monitor_xdcr_replication(target.node, target.bucket)

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

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()

        self.reporter.post_to_sf(*self.metric_helper.calc_xdcr_lag())
        if self.remote.os != 'Cygwin':
            self.reporter.post_to_sf(*self.metric_helper.calc_max_beam_rss())


class SrcTargetIterator(TargetIterator):

    def __iter__(self):
        username, password = self.cluster_spec.rest_credentials
        prefix = self.prefix
        src_master = self.cluster_spec.yield_masters().next()
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, username, password, prefix)


class SymmetricXdcrTest(XdcrTest):

    def __init__(self, *args, **kwargs):
        super(SymmetricXdcrTest, self).__init__(*args, **kwargs)
        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')

    def load(self):
        load_settings = self.test_config.load_settings
        log_phase('load phase', load_settings)
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config,
                                                prefix='symmetric')
        self.worker_manager.run_workload(load_settings, src_target_iterator)
        self.worker_manager.wait_for_workers()


class XdcrInitTest(SymmetricXdcrTest):

    COLLECTORS = {}

    def load(self):
        load_settings = self.test_config.load_settings
        log_phase('load phase', load_settings)
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        self.worker_manager.run_workload(load_settings, src_target_iterator)
        self.worker_manager.wait_for_workers()

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


class XdcrInitInMemoryTest(XdcrInitTest, FlusherTest):

    def stop_persistence(self):
        servers = tuple(self.cluster_spec.yield_servers())
        dest_hostnames = [
            server.split(':')[0] for server in servers[len(servers) / 2:]
        ]
        for mc in self.mc_iterator():
            if mc.host in dest_hostnames:
                mc.stop_persistence()

    def run(self):
        self.stop_persistence()
        super(XdcrInitInMemoryTest, self).run()

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.settings import TargetSettings
from perfrunner.tests import target_hash, PerfTest, TargetIterator


class SrcTargetIterator(TargetIterator):

    def __iter__(self):
        username, password = self.cluster_spec.get_rest_credentials()
        src_cluster = self.cluster_spec.get_clusters()[0]
        src_master = src_cluster[0]
        for bucket in self.test_config.get_buckets():
                prefix = target_hash(src_master, bucket)
                yield TargetSettings(src_master, bucket, username, password, prefix)


class XdcrInitTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(XdcrInitTest, self).__init__(*args, **kwargs)
        self.src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                     self.test_config)

    def _run_load_phase(self):
        load_settings = self.test_config.get_load_settings()
        logger.info('Running load phase: {0}'.format(load_settings))
        self._run_workload(load_settings, self.src_target_iterator)

    def _start_replication(self, m1, m2):
        name = target_hash(m1, m2)
        self.rest.add_remote_cluster(m1, m2, name)

        for bucket in self.test_config.get_buckets():
            self.rest.start_replication(m1, bucket, bucket, name)

    @with_stats()
    def _init_xdcr(self):
        xdcr_settings = self.test_config.get_xdcr_settings()

        c1, c2 = self.cluster_spec.get_clusters()
        m1, m2 = c1[0], c2[0]

        if xdcr_settings.replication_type == 'unidir':
            self._start_replication(m1, m2)
        if xdcr_settings.replication_type == 'bidir':
            self._start_replication(m1, m2)
            self._start_replication(m2, m1)

        for target in self.target_iterator:
            self.monitor.monitor_xdcr_replication(target)

    def _calc_avg_rate(self, time_elapsed):
        initial_items = self.test_config.get_load_settings().ops
        buckets = self.test_config.get_num_buckets()
        return round(buckets * initial_items / (time_elapsed * 60))

    def run(self):
        self._run_load_phase()
        self._compact_bucket()

        self.reporter.start()
        self._init_xdcr()
        time_elapsed = self.reporter.finish('Initial replication')
        self.reporter.post_to_sf(self._calc_avg_rate(time_elapsed))

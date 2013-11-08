import time

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import log_phase, target_hash
from perfrunner.settings import TargetSettings
from perfrunner.tests import TargetIterator
from perfrunner.tests import PerfTest


class XdcrTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(XdcrTest, self).__init__(*args, **kwargs)
        self.settings = self.test_config.get_xdcr_settings()
        self.shutdown_event = None

    def _start_replication(self, m1, m2):
        name = target_hash(m1, m2)
        self.rest.add_remote_cluster(m1, m2, name)

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
        m1, m2 = self.cluster_spec.get_masters().values()

        if self.settings.replication_type == 'unidir':
            self._start_replication(m1, m2)
        if self.settings.replication_type == 'bidir':
            self._start_replication(m1, m2)
            self._start_replication(m2, m1)

        for target in self.target_iterator:
            self.monitor.monitor_xdcr_replication(target)

    @with_stats(xdcr_lag=True)
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
            *self.metric_helper.calc_avg_set_meta_ops()
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
        log_phase('load phase', load_settings)
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        self.worker_manager.run_workload(load_settings, src_target_iterator)

    @with_stats()
    def init_xdcr(self):
        super(XdcrInitTest, self).init_xdcr()

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


class XdcrTuningTest(XdcrInitTest):

    def run(self):
        super(XdcrTuningTest, self).run()
        for cluster, value in self.metric_helper.calc_cpu_utilizations().items():
            self.reporter.post_to_sf(
                value=value,
                metric='{}_avg_cpu_utilization_rate'.format(cluster)
            )


class XdcrDebugTest(SymmetricXdcrTest):

    POLLING_INTERVAL = 20

    @with_stats(xdcr_lag=True)
    def access(self):
        access_settings = self.test_config.get_access_settings()
        logger.info('Running phase for {} seconds'.format(access_settings.time))
        t0 = time.time()
        while time.time() - t0 < access_settings.time:
            for master in self.cluster_spec.get_masters().values():
                run_queues = self.rest.run_diag_eval(
                    master, 'erlang:statistics(run_queues).'
                )
                logger.info('Run queues at {}: {}'.format(master, run_queues))
            time.sleep(self.POLLING_INTERVAL)

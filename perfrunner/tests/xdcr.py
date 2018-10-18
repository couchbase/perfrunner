from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import target_hash
from perfrunner.helpers.profiler import with_profiles
from perfrunner.settings import TargetSettings
from perfrunner.tests import PerfTest, TargetIterator


class XdcrTest(PerfTest):

    ALL_BUCKETS = True

    CLUSTER_NAME = 'perf'

    COLLECTORS = {'xdcr_lag': True, 'xdcr_stats': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xdcr_settings = self.test_config.xdcr_settings

    def add_remote_cluster(self):
        m1, m2 = self.cluster_spec.masters
        certificate = self.xdcr_settings.demand_encryption and \
            self.rest.get_certificate(m2)
        secure_type = self.xdcr_settings.secure_type
        self.rest.add_remote_cluster(local_host=m1,
                                     remote_host=m2,
                                     name=self.CLUSTER_NAME,
                                     secure_type=secure_type,
                                     certificate=certificate)

    def replication_params(self, from_bucket: str, to_bucket: str):
        params = {
            'replicationType': 'continuous',
            'fromBucket': from_bucket,
            'toBucket': to_bucket,
            'toCluster': self.CLUSTER_NAME,
        }
        if self.xdcr_settings.filter_expression:
            params.update({
                'filterExpression': self.xdcr_settings.filter_expression,
            })
        return params

    def create_replication(self):
        for bucket in self.test_config.buckets:
            params = self.replication_params(from_bucket=bucket,
                                             to_bucket=bucket)
            self.rest.create_replication(self.master_node, params)

    def monitor_replication(self):
        for target in self.target_iterator:
            if self.rest.get_remote_clusters(target.node):
                self.monitor.monitor_xdcr_queues(target.node, target.bucket)

    @with_stats
    @with_profiles
    def access(self, *args):
        super().access(*args)

    def configure_wan(self):
        if self.xdcr_settings.wan_delay:
            hostnames = self.cluster_spec.servers
            src_list = [
                hostname for hostname in hostnames[len(hostnames) // 2:]
            ]
            dest_list = [
                hostname for hostname in hostnames[:len(hostnames) // 2]
            ]
            self.remote.enable_wan(self.xdcr_settings.wan_delay)
            self.remote.filter_wan(src_list, dest_list)

    def _report_kpi(self, *args):
        self.reporter.post(
            *self.metrics.xdcr_lag()
        )

        if self.test_config.stats_settings.post_cpu:
            self.reporter.post(
                *self.metrics.cpu_utilization()
            )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.add_remote_cluster()
        self.create_replication()
        self.monitor_replication()
        self.wait_for_persistence()

        self.hot_load()

        self.configure_wan()

        self.access()

        self.report_kpi()


class SrcTargetIterator(TargetIterator):

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        src_master = next(self.cluster_spec.masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, password, prefix)


class DestTargetIterator(TargetIterator):

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        masters = self.cluster_spec.masters
        src_master = next(masters)
        dest_master = next(masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(src_master, bucket)
            yield TargetSettings(dest_master, bucket, password, prefix)


class UniDirXdcrTest(XdcrTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')

    def load(self, *args):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config,
                                                prefix='symmetric')
        super().load(target_iterator=src_target_iterator)


class XdcrInitTest(XdcrTest):

    """Run initial XDCR.

    There is no ongoing workload and compaction is usually disabled.
    """

    COLLECTORS = {'xdcr_stats': True}

    @with_stats
    @with_profiles
    @timeit
    def init_xdcr(self):
        self.add_remote_cluster()
        self.create_replication()
        self.monitor_replication()

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.avg_replication_rate(time_elapsed)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.configure_wan()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class UniDirXdcrInitTest(XdcrInitTest):

    def load(self, *args):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        XdcrInitTest.load(self, target_iterator=src_target_iterator)

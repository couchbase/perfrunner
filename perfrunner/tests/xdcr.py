from multiprocessing import Pool

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


class AdvFilterXdcrTest(XdcrInitTest):

    def load(self, *args):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        super(XdcrInitTest, self).load(target_iterator=src_target_iterator)

    def xattr_load(self, *args):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        super(XdcrInitTest, self).xattr_load(target_iterator=src_target_iterator)

    def run(self):
        self.load()
        self.xattr_load()
        self.wait_for_persistence()

        self.compact_bucket(wait=True)

        self.configure_wan()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class XdcrPriorityThroughputTest(XdcrTest):

    COLLECTORS = {'xdcr_stats': True}

    CLUSTER_NAME = 'link'

    def load(self, *args):
        src_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                self.test_config)
        XdcrInitTest.load(self, target_iterator=src_target_iterator)

    def add_remote_clusters(self, num_xdcr_links: int):

        local_host = self.cluster_spec.servers[0]

        for i in range(1, num_xdcr_links+1):
            remote_host = self.cluster_spec.servers[i]
            cluster_name = self.CLUSTER_NAME + str(i)

            certificate = self.xdcr_settings.demand_encryption and \
                self.rest.get_certificate(remote_host)

            secure_type = self.xdcr_settings.secure_type
            self.rest.add_remote_cluster(local_host=local_host,
                                         remote_host=remote_host,
                                         name=cluster_name,
                                         secure_type=secure_type,
                                         certificate=certificate)

    def replication_params(self, from_bucket: str, to_bucket: str, link_name: str, priority: str):
        params = {
            'replicationType': 'continuous',
            'fromBucket': from_bucket,
            'toBucket': to_bucket,
            'toCluster': link_name,
            'priority': priority,
        }
        if self.xdcr_settings.filter_expression:
            params.update({
                'filterExpression': self.xdcr_settings.filter_expression,
            })
        return params

    def create_replication(self, link_name: str, priority: str):
        for bucket in self.test_config.buckets:
            params = self.replication_params(from_bucket=bucket,
                                             to_bucket=bucket,
                                             link_name=link_name,
                                             priority=priority)
            self.rest.create_replication(self.master_node, params)

    def create_replications(self, num_xdcr_links: int, xdcr_links_priority: list):

        for bucket in self.test_config.buckets:

            for i in range(1, num_xdcr_links+1):
                link_name = self.CLUSTER_NAME + str(i)
                priority = xdcr_links_priority[i-1]
                params = self.replication_params(from_bucket=bucket,
                                                 to_bucket=bucket,
                                                 link_name=link_name,
                                                 priority=priority)
                self.rest.create_replication(self.master_node, params)

    def get_cluster_uuid(self):
        cluster_uuid = []
        cluster_map = {}
        for target in self.target_iterator:
            for cluster in self.rest.get_remote_clusters(target.node):
                cluster_uuid.append(cluster.get('uuid'))
                cluster_map.update({cluster.get('name'): cluster.get('uuid')})
        return cluster_uuid, cluster_map

    def build_xdcrlink(self, uuid: str, from_bucket: str, to_bucket: str):
        xdcr_link = 'replications/{}/{}/{}/percent_completeness'.format(uuid,
                                                                        from_bucket,
                                                                        to_bucket)
        return xdcr_link

    @with_stats
    @with_profiles
    def monitor_parallel_replication(self, num_uuidlist: int, uuid_list: list):
        with Pool(processes=num_uuidlist) as pool:
            results = pool.map(func=self.calculate_replication_time, iterable=uuid_list)
        return results

    def calculate_replication_time(self, uuid: str):
        xdcr_time_map = {}
        for target in self.target_iterator:
            if self.rest.get_remote_clusters(target.node):
                xdcr_link = self.build_xdcrlink(uuid=uuid,
                                                from_bucket=target.bucket,
                                                to_bucket=target.bucket)
                start_time = self.monitor.xdcr_link_starttime(host=target.node, uuid=uuid)
                end_time = self.monitor.monitor_xdcr_completeness(host=target.node,
                                                                  bucket=target.bucket,
                                                                  xdcr_link=xdcr_link)

        replication_time = end_time - start_time
        xdcr_time_map.update({uuid: replication_time})
        return xdcr_time_map

    def map_link_xdcrtime(self, result_map: map, cluster_map: map):

        new_result_map = {}

        for result in result_map:
            for k, v in result.items():
                for key, value in cluster_map.items():
                    if k == value:
                        new_result_map.update({key: v})

        return new_result_map

    def _report_kpi(self, time_elapsed, xdcr_link):
        self.reporter.post(
            *self.metrics.avg_replication_multilink(time_elapsed, xdcr_link)
        )

    def run(self):

        num_xdcr_links = self.test_config.xdcr_settings.num_xdcr_links
        xdcr_links_priority = self.test_config.xdcr_settings.xdcr_links_priority

        self.load()
        self.wait_for_persistence()
        self.configure_wan()

        self.add_remote_clusters(num_xdcr_links=num_xdcr_links)
        uuid_list, cluster_map = self.get_cluster_uuid()
        self.create_replications(num_xdcr_links=num_xdcr_links,
                                 xdcr_links_priority=xdcr_links_priority)

        results = self.monitor_parallel_replication(num_uuidlist=len(uuid_list),
                                                    uuid_list=uuid_list)
        result_map = self.map_link_xdcrtime(result_map=results, cluster_map=cluster_map)

        for key, value in result_map.items():
            self.report_kpi(value, key)


class XdcrPriorityLatencyTest(XdcrPriorityThroughputTest):

    COLLECTORS = {'xdcr_lag': True, 'xdcr_stats': True}

    def _report_kpi(self, *args):
        self.reporter.post(
            *self.metrics.xdcr_lag()
        )

        if self.test_config.stats_settings.post_cpu:
            self.reporter.post(
                *self.metrics.cpu_utilization()
            )

    def run(self):

        num_xdcr_links = self.test_config.xdcr_settings.num_xdcr_links
        xdcr_links_priority = self.test_config.xdcr_settings.xdcr_links_priority

        self.load()

        self.wait_for_persistence()

        self.add_remote_clusters(num_xdcr_links=num_xdcr_links)

        self.create_replications(num_xdcr_links=num_xdcr_links,
                                 xdcr_links_priority=xdcr_links_priority)

        self.monitor_replication()

        self.wait_for_persistence()

        self.hot_load()

        self.configure_wan()

        self.access()

        self.report_kpi()


class XdcrBackfillLatencyTest(XdcrPriorityLatencyTest):

    def run(self):
        num_xdcr_links = self.test_config.xdcr_settings.num_xdcr_links
        xdcr_links_priority1, xdcr_links_priority2 = \
            self.test_config.xdcr_settings.xdcr_links_priority

        self.load()

        self.wait_for_persistence()

        self.add_remote_clusters(num_xdcr_links=num_xdcr_links)

        self.create_replication('link2', xdcr_links_priority2)

        self.monitor_replication()

        self.wait_for_persistence()

        self.hot_load()

        self.configure_wan()

        self.create_replication('link1', xdcr_links_priority1)

        self.access()

        self.report_kpi()

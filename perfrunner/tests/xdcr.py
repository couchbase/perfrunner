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
        self.compact_bucket(wait=True)

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

    def _report_kpi(self, throughput, xdcr_link):
        self.reporter.post(
            *self.metrics.avg_replication_throughput(throughput, xdcr_link)
        )

    @with_stats
    @with_profiles
    def wait_for_replication(self, cluster_map: map):
        xdcr_link1 = cluster_map.get('link1')
        xdcr_link2 = cluster_map.get('link2')
        for target in self.target_iterator:
            if self.rest.get_remote_clusters(target.node):
                start_time, link1_endtime, link2_items = \
                    self.monitor.monitor_xdcr_changes_left(host=target.node,
                                                           bucket=target.bucket,
                                                           xdcrlink1=xdcr_link1,
                                                           xdcrlink2=xdcr_link2)
        return start_time, link1_endtime, link2_items

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

        start_time, link1_endtime, link2_items = \
            self.wait_for_replication(cluster_map=cluster_map)

        link1_throughput = self.test_config.load_settings.items / (link1_endtime - start_time)
        link2_throughput = link2_items / (link1_endtime - start_time)

        self.report_kpi(link1_throughput, "link1")
        self.report_kpi(link2_throughput, "link2")


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


class XdcrCollectionMapTest(UniDirXdcrInitTest):

    def create_replication(self):

        for bucket in self.test_config.buckets:
            params = self.replication_params(from_bucket=bucket,
                                             to_bucket=bucket)
            self.rest.create_replication(self.master_node, params)

    def replication_params(self, from_bucket: str, to_bucket: str):
        collection_mapping = self.test_config.xdcr_settings.initial_collection_mapping

        params = {
            'replicationType': 'continuous',
            'fromBucket': from_bucket,
            'toBucket': to_bucket,
            'toCluster': self.CLUSTER_NAME,
            'checkpointInterval': 60,
            'colMappingRules': collection_mapping,
            'collectionsExplicitMapping': True
        }
        if self.xdcr_settings.filter_expression:
            params.update({
                'filterExpression': self.xdcr_settings.filter_expression,
            })
        return params


class XdcrCollectionBackfillTest(UniDirXdcrInitTest):

    def create_initial_replication(self):
        for bucket in self.test_config.buckets:
            params = self.initial_replication_params(from_bucket=bucket, to_bucket=bucket)

            self.rest.create_replication(self.master_node, params)

    def initial_replication_params(self, from_bucket: str, to_bucket: str):
        initial_collection_mapping = self.test_config.xdcr_settings.initial_collection_mapping
        collections_oso_mode = self.test_config.xdcr_settings.collections_oso_mode

        params = {
            'replicationType': 'continuous',
            'fromBucket': from_bucket,
            'toBucket': to_bucket,
            'toCluster': self.CLUSTER_NAME,
            'checkpointInterval': 60,
            'colMappingRules': initial_collection_mapping,
            'collectionsExplicitMapping': True,
            'collectionsOSOMode': collections_oso_mode
        }
        if self.xdcr_settings.filter_expression:
            params.update({
                'filterExpression': self.xdcr_settings.filter_expression,
            })
        return params

    def get_replication_id(self, src_master):
        replication_id = self.rest.get_xdcr_replication_id(host=src_master)
        return replication_id

    def get_replicated_docs(self, host: str):
        num_items = 0
        for bucket in self.test_config.buckets:
            num_items = self.monitor.get_num_items(host=host, bucket=bucket)
        return num_items

    def create_backfill_replication(self, replication_id: str):
        params = self.backfill_replication_params()
        self.rest.edit_replication(self.master_node, params, replication_id)

    def backfill_replication_params(self):
        backfill_collection_mapping = self.test_config.xdcr_settings.backfill_collection_mapping
        params = {
            'colMappingRules': backfill_collection_mapping,
            'collectionsSkipSrcValidation': True
        }
        return params

    def monitor_backfill_replication(self, dest_host: str, num_items: int):
        backfill_time = 0
        for bucket in self.test_config.buckets:
            backfill_time = self.monitor.monitor_num_backfill_items(host=dest_host,
                                                                    bucket=bucket,
                                                                    num_items=num_items)
        return backfill_time

    @with_stats
    @with_profiles
    def backfill_create_monitor(self, replication_id, dest_host):
        self.create_backfill_replication(replication_id=replication_id)

        backfill_time = self.monitor_backfill_replication(
            dest_host=dest_host,
            num_items=self.test_config.load_settings.items
        )
        return backfill_time

    def _report_kpi(self, throughput):
        self.reporter.post(
            *self.metrics.replication_throughput(throughput)
        )

    def run(self):

        src_master, dest_master = self.cluster_spec.masters

        self.load()
        self.wait_for_persistence()
        self.add_remote_cluster()
        self.create_initial_replication()
        self.monitor_replication()

        replication_id = self.get_replication_id(src_master=src_master)
        replicated_items = self.get_replicated_docs(host=dest_master)

        print('replicated_items : ', replicated_items)

        items_remaining = self.test_config.load_settings.items - replicated_items

        print('items_remaining : ', items_remaining)

        backfill_time = self.backfill_create_monitor(replication_id=replication_id,
                                                     dest_host=dest_master)

        throughput = items_remaining/backfill_time

        self.report_kpi(throughput)


class CollectionMigrationTest(XdcrCollectionMapTest):

    def create_replication(self):

        for bucket in self.test_config.buckets:
            params = self.replication_params(from_bucket=bucket,
                                             to_bucket=bucket)
            self.rest.create_replication(self.master_node, params)

    def replication_params(self, from_bucket: str, to_bucket: str):

        collection_mapping = self.test_config.xdcr_settings.initial_collection_mapping

        print('printing collection_mapping : ', collection_mapping)

        params = {
            'replicationType': 'continuous',
            'fromBucket': from_bucket,
            'toBucket': to_bucket,
            'toCluster': self.CLUSTER_NAME,
            'checkpointInterval': 60,
            'collectionsMigrationMode': True,
            'colMappingRules': collection_mapping,
            'collectionsExplicitMapping': True
        }
        if self.xdcr_settings.filter_expression:
            params.update({
                'filterExpression': self.xdcr_settings.filter_expression,
            })
        return params

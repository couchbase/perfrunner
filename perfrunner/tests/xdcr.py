import base64
from multiprocessing import Pool

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import target_hash
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import run_conflictsim_task, ycsb_data_load_task, ycsb_task
from perfrunner.settings import TargetSettings
from perfrunner.tests import PerfTest, TargetIterator
from perfrunner.tests.tools import RestoreTest
from perfrunner.utils.terraform import raise_for_status


class XdcrTest(PerfTest):

    ALL_BUCKETS = True

    CLUSTER_NAME = 'perf'

    COLLECTORS = {'xdcr_lag': True, 'xdcr_stats': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xdcr_settings = self.test_config.xdcr_settings
        self.load_target_iterator = TargetIterator(self.cluster_spec, self.test_config)

        if self.use_prometheus:
            # Explicitly register xdcr as part of this test
            self.collector_agent.add_service(services=["xdcr"])

    def load(self, *args):
        super().load(*args, target_iterator=self.load_target_iterator)

    def check_num_items(self, *args):
        super().check_num_items(*args, target_iterator=self.load_target_iterator)

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
        if self.xdcr_settings.mobile:
            logger.info("XDCR mobile replication is active")
            params.update({
                'mobile': 'active'
            })
        if self.xdcr_settings.eccv:
            for bucket in self.test_config.buckets:
                m1, m2 = self.cluster_spec.masters
                self.rest.enable_cross_clustering_versioning(m1, bucket)
                self.rest.enable_cross_clustering_versioning(m2, bucket)
        return params

    def create_replication(self):
        for bucket in self.test_config.buckets:
            params = self.replication_params(from_bucket=bucket,
                                             to_bucket=bucket)
            self.rest.create_replication(self.master_node, params)

    def delete_replications(self, replication_id: str):
        for bucket in self.test_config.buckets:
            self.rest.delete_replication(self.master_node, replication_id)

    def monitor_replication(self):
        items = self.test_config.load_settings.items
        mobile = self.test_config.xdcr_settings.mobile
        num_replication = 0
        for target in self.target_iterator:
            if self.rest.get_remote_clusters(target.node):
                num_nodes = int(self.test_config.cluster.initial_nodes[0])
                self.monitor.monitor_xdcr_queues(target.node, target.bucket, items,
                                                 num_replication, num_nodes, mobile)
                num_replication += 1

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
        self.check_num_items()

        self.add_remote_cluster()
        self.create_replication()
        self.monitor_replication()
        self.wait_for_persistence()

        self.hot_load()

        self.configure_wan()

        self.access()

        self.report_kpi()


class CapellaXdcrTest(XdcrTest):

    def add_remote_cluster(self):
        logger.warn('add_remote_cluster not supported on Capella. Not doing anything.')

    def configure_wan(self):
        logger.warn('configure_wan not supported on Capella. Not doing anything.')

    def replication_params(self, from_bucket: str, to_bucket: str, to_cluster: str,
                           direction: str, priority: str, network_usage_limit: int):
        params = {
            'direction': direction.lower(),
            'sourceBucket': base64.urlsafe_b64encode(from_bucket.encode()).decode(),
            'target': {
                'cluster': to_cluster,
                'bucket': base64.urlsafe_b64encode(to_bucket.encode()).decode()
            },
            'settings': {
                'priority': priority.lower()
            }
        }

        if filter_exp := self.xdcr_settings.filter_expression:
            params['settings'].update({
                'filterExpression': filter_exp,
            })

        if coll_map := self.xdcr_settings.initial_collection_mapping:
            # Turn something like:
            #   {"scope-1.collection-1":"scope-1.collection-1"}
            # into what we need for the Capella API
            refactored_coll_map = {}
            for source, target in coll_map.items():
                scope_pair = '{}:{}'.format(source.split('.')[0], target.split('.')[0])
                coll_pair = (source.split('.')[1], target.split('.')[1])
                if scope_pair not in refactored_coll_map:
                    refactored_coll_map[scope_pair] = [coll_pair]
                else:
                    refactored_coll_map[scope_pair].append(coll_pair)

            params['target'].update({
                'scopes': [
                    {
                        'source': scope_pair.split(':')[0],
                        'target': scope_pair.split(':')[1],
                        'collections': [
                            {
                                'source': source_coll,
                                'target': target_coll
                            }
                            for source_coll, target_coll in colls
                        ]
                    }
                    for scope_pair, colls in refactored_coll_map.items()
                ]
            })

        if network_usage_limit > 0:
            params['settings'].update({
                'networkUsageLimit': network_usage_limit
            })

        return params

    def create_replications(self, num_xdcr_links: int, xdcr_links_priority: list,
                            xdcr_link_directions: list, xdcr_link_network_limits: list):
        m1, m2 = self.cluster_spec.masters
        for bucket in self.test_config.buckets:
            for _, priority, direction, network_limit in zip(range(num_xdcr_links),
                                                             xdcr_links_priority,
                                                             xdcr_link_directions,
                                                             xdcr_link_network_limits):
                resp = self.rest.create_replication(
                    m1,
                    self.replication_params(
                        from_bucket=bucket,
                        to_bucket=bucket,
                        to_cluster=self.rest.hostname_to_cluster_id(m2),
                        direction=direction,
                        priority=priority,
                        network_usage_limit=network_limit
                    )
                )
                raise_for_status(resp)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.create_replications(
            num_xdcr_links=self.test_config.xdcr_settings.num_xdcr_links,
            xdcr_links_priority=self.test_config.xdcr_settings.xdcr_links_priority,
            xdcr_link_directions=self.test_config.xdcr_settings.xdcr_link_directions,
            xdcr_link_network_limits=self.test_config.xdcr_settings.xdcr_link_network_limits
        )
        self.monitor_replication()
        self.wait_for_persistence()

        self.hot_load()

        self.access()

        self.report_kpi()


class SrcTargetIterator(TargetIterator):

    def __iter__(self):
        username = self.cluster_spec.rest_credentials[0]
        if self.test_config.client_settings.python_client:
            if self.test_config.client_settings.python_client.split('.')[0] == "2":
                password = self.test_config.bucket.password
            else:
                password = self.cluster_spec.rest_credentials[1]
        else:
            password = self.cluster_spec.rest_credentials[1]
        prefix = self.prefix
        src_master = next(self.cluster_spec.masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, username, password, prefix)


class DestTargetIterator(TargetIterator):

    def __iter__(self):
        username = self.cluster_spec.rest_credentials[0]
        if self.test_config.client_settings.python_client:
            if self.test_config.client_settings.python_client.split('.')[0] == "2":
                password = self.test_config.bucket.password
            else:
                password = self.cluster_spec.rest_credentials[1]
        else:
            password = self.cluster_spec.rest_credentials[1]
        prefix = self.prefix
        masters = self.cluster_spec.masters
        src_master = next(masters)
        dest_master = next(masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(src_master, bucket)
            yield TargetSettings(dest_master, bucket, username, password, prefix)


class UniDirXdcrTest(XdcrTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')
        self.load_target_iterator = SrcTargetIterator(self.cluster_spec,
                                                      self.test_config,
                                                      prefix='symmetric')


class CapellaUniDirXdcrTest(UniDirXdcrTest, CapellaXdcrTest):
    pass


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
        if self.xdcr_settings.mobile:
            self.sgw_master_node = next(self.cluster_spec.sgw_masters)
            self.reporter.build = f"{self.rest.get_sgversion(self.sgw_master_node)}:{self.build}"

        self.reporter.post(*self.metrics.avg_replication_rate(time_elapsed))

    def run(self):
        self.load()
        self.wait_for_persistence()
        # check_num_items doesn't work when SGW is connected to server, as SGW adds its own
        # documents (sync, config, heartbeat)
        if not self.xdcr_settings.mobile:
            self.check_num_items()
        self.compact_bucket(wait=True)

        self.configure_wan()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class CapellaXdcrInitTest(XdcrInitTest, CapellaXdcrTest):

    @timeit
    def monitor_replication(self):
        super().monitor_replication()

    @with_stats
    @with_profiles
    def init_xdcr(self):
        self.create_replications(
            num_xdcr_links=self.test_config.xdcr_settings.num_xdcr_links,
            xdcr_links_priority=self.test_config.xdcr_settings.xdcr_links_priority,
            xdcr_link_directions=self.test_config.xdcr_settings.xdcr_link_directions,
            xdcr_link_network_limits=self.test_config.xdcr_settings.xdcr_link_network_limits
        )
        time_elapsed = self.monitor_replication()
        return time_elapsed

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class BiDirXdcrInitTest(XdcrInitTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.load_target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)
        self.dest_target_iterator = DestTargetIterator(self.cluster_spec, self.test_config)
        self.cluster_name1 = f"{self.CLUSTER_NAME}1"
        self.cluster_name2 = f"{self.CLUSTER_NAME}2"

    def add_remote_clusters(self):
        m1, m2 = self.cluster_spec.masters
        certificate_m2 = self.xdcr_settings.demand_encryption and \
            self.rest.get_certificate(m2)
        secure_type = self.xdcr_settings.secure_type
        self.rest.add_remote_cluster(local_host=m1,
                                     remote_host=m2,
                                     name=self.cluster_name1,
                                     secure_type=secure_type,
                                     certificate=certificate_m2)
        certificate_m1 = self.xdcr_settings.demand_encryption and \
            self.rest.get_certificate(m1)
        self.rest.add_remote_cluster(local_host=m2,
                                     remote_host=m1,
                                     name=self.cluster_name2,
                                     secure_type=secure_type,
                                     certificate=certificate_m1)

    def enable_cross_clustering_versioning(self):
        for bucket in self.test_config.buckets:
            m1, m2 = self.cluster_spec.masters
            self.rest.enable_cross_clustering_versioning(m1, bucket)
            self.rest.enable_cross_clustering_versioning(m2, bucket)

    def set_conflict_settings(self):
        logger.info("setting conflict settings")
        params = {
            'workerCount': self.xdcr_settings.worker_count,
            'connType': self.xdcr_settings.conn_type,
            'connLimit': self.xdcr_settings.conn_limit,
            'queueLen': self.xdcr_settings.queue_len
        }
        for bucket in self.test_config.buckets:
            m1, m2 = self.cluster_spec.masters
            self.rest.set_xdcr_conflict_settings(m1, params)
            self.rest.set_xdcr_conflict_settings(m2, params)

    def replication_params(self, from_bucket: str, to_bucket: str, replication_name: str):
        params = {
            'replicationType': 'continuous',
            'fromBucket': from_bucket,
            'toBucket': to_bucket,
            'toCluster': replication_name
        }
        if self.xdcr_settings.filter_expression:
            params.update({
                'filterExpression': self.xdcr_settings.filter_expression,
            })

        if self.xdcr_settings.mobile:
            logger.info("XDCR mobile replication is active")
            params.update({
                'mobile': 'active'
            })
        if self.test_config.cluster.conflict_buckets != 0:
            cLog = '{\"bucket\": \"conflict-bucket-1\", \"collection\": \"_default._default\"}'
            params.update({
                'conflictLogging': cLog,
                'cLogQueueCapacity': self.test_config.xdcr_settings.queue_len,
                'cLogWorkerCount': self.test_config.xdcr_settings.worker_count
            })
        return params

    def create_replications(self):
        m1, m2 = self.cluster_spec.masters
        for bucket in self.test_config.buckets:
            # Create replication from cluster 1 to cluster 2
            params = self.replication_params(from_bucket=bucket,
                                             to_bucket=bucket,
                                             replication_name=self.cluster_name1)
            self.rest.create_replication(m1, params)
            # Create replication from cluster 2 to cluster 1
            params = self.replication_params(from_bucket=bucket,
                                             to_bucket=bucket,
                                             replication_name=self.cluster_name2)
            self.rest.create_replication(m2, params)

    def check_num_items(self, target_iterator, *args):
        PerfTest.check_num_items(self, *args, target_iterator=target_iterator)

    def load(self, *args):
        logger.info("Loading the source bucket")
        PerfTest.load(self, *args, target_iterator=self.load_target_iterator)
        self.wait_for_persistence()
        self.check_num_items(self.load_target_iterator)
        logger.info("Finished loading the source bucket")

        logger.info("Loading the destination bucket")
        load_settings = self.test_config.load_settings
        total_items = load_settings.items
        load_settings.items = int(load_settings.items * (1 - load_settings.conflict_ratio))
        logger.info(f"The total number of items is: {total_items}")
        logger.info(f"The number of non-conflict docs is: {load_settings.items}")
        # To load non-conflict docs we use YCSB

        if load_settings.items != 0:
            non_conflict_dest_target_iterator = DestTargetIterator(
                self.cluster_spec, self.test_config, "non-conflict"
            )
            PerfTest.load(
                self,
                *args,
                target_iterator=non_conflict_dest_target_iterator,
                settings=load_settings,
            )

        logger.info("Finished loading non-conflict docs to the destination bucket")
        logger.info(f"The diff is: {total_items - load_settings.items}")
        load_settings.items = total_items - load_settings.items
        logger.info(f"The number of conflict docs is: {load_settings.items}")

        PerfTest.load(self, *args, target_iterator=self.dest_target_iterator,
                      settings=load_settings)
        self.wait_for_persistence()

        load_settings.items = total_items
        self.check_num_items(self.dest_target_iterator)

    @with_stats
    @with_profiles
    @timeit
    def init_xdcr(self):
        self.add_remote_clusters()
        self.create_replications()
        self.monitor_replication()

    def access(self, *args):
        PerfTest.access(self, *args, target_iterator=self.load_target_iterator)

    def _report_kpi(self, time_elapsed):
        logger.info("Time elapsed is: {}".format(time_elapsed))
        m1, m2 = self.cluster_spec.masters
        bucket_replica = self.test_config.bucket.replica_number
        m1_bucket_docs = self.monitor.get_num_items(host=m1, bucket="bucket-1",
                                                    bucket_replica=bucket_replica)
        logger.info(f"Number of docs in bucket-1 on {m1} is: {m1_bucket_docs}")
        m2_bucket_docs = self.monitor.get_num_items(host=m2, bucket="bucket-1",
                                                    bucket_replica=bucket_replica)
        logger.info(f"Number of docs in bucket-1 on {m2} is: {m2_bucket_docs}")
        total_docs = (
            m1_bucket_docs + m2_bucket_docs
        )
        if self.test_config.cluster.conflict_buckets != 0:
            m1_conflict_bucket_docs = self.monitor.get_num_items(
                host=m1, bucket="conflict-bucket-1", bucket_replica=bucket_replica
            )
            logger.info(
                f"Number of docs in conflict-bucket-1 on {m1} is: {m1_conflict_bucket_docs}"
            )
            m2_conflict_bucket_docs = self.monitor.get_num_items(
                host=m2, bucket="conflict-bucket-1", bucket_replica=bucket_replica
            )
            logger.info(
                f"Number of docs in conflict-bucket-1 on {m2} is: {m2_conflict_bucket_docs}"
            )
            total_docs += m1_conflict_bucket_docs + m2_conflict_bucket_docs
        logger.info(f"Total number of docs is: {total_docs}")
        m1_bucket_written = self.rest.get_xdcr_docs_written_total(host=m1, bucket="bucket-1")
        logger.info(f"Number of docs written to bucket-1 on {m1} is: {m1_bucket_written}")
        m2_bucket_written = self.rest.get_xdcr_docs_written_total(host=m2, bucket="bucket-1")
        logger.info(f"Number of docs written to bucket-1 on {m2} is: {m2_bucket_written}")
        total_docs_written = m1_bucket_written + m2_bucket_written
        logger.info(f"Total number of docs written is: {total_docs_written}")
        m1_bucket_dcp = self.rest.get_xdcr_docs_received_from_dcp_total(host=m1, bucket="bucket-1")
        logger.info(f"Number of docs received from DCP on {m1} is: {m1_bucket_dcp}")
        m2_bucket_dcp = self.rest.get_xdcr_docs_received_from_dcp_total(host=m2, bucket="bucket-1")
        logger.info(f"Number of docs received from DCP on {m2} is: {m2_bucket_dcp}")
        total_docs_dcp = m1_bucket_dcp + m2_bucket_dcp
        logger.info(f"Total number of docs received from DCP is: {total_docs_dcp}")
        self.reporter.post(
            *self.metrics.bidir_replication_rate_total_docs(time_elapsed)
        )
        self.reporter.post(
            *self.metrics.bidir_replication_rate_written_docs(time_elapsed, total_docs_written)
        )
        self.reporter.post(
            *self.metrics.bidir_replication_rate_dcp_docs(time_elapsed, total_docs_dcp)
        )

    def run(self):
        if self.xdcr_settings.eccv:
            self.enable_cross_clustering_versioning()
        self.load()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class BiDirXdcrUpdateTest(BiDirXdcrInitTest):

    def load(self, *args):
        logger.info("Loading the source bucket")
        self.build_ycsb(self.test_config.load_settings.ycsb_client)
        PerfTest.load(self, task=ycsb_data_load_task, target_iterator=self.load_target_iterator)
        self.wait_for_persistence()
        self.check_num_items(self.load_target_iterator)
        logger.info("Finished loading the source bucket")

    @timeit
    @with_stats
    def access(self, *args):
        logger.info("Updating the source bucket")
        self.build_ycsb(self.test_config.access_settings.ycsb_client)
        PerfTest.access_bg(self, task=ycsb_task, target_iterator=self.load_target_iterator)

        logger.info("Updating the destination bucket")
        access_settings = self.test_config.access_settings
        total_ops = access_settings.ops
        access_settings.ops = int(access_settings.ops * access_settings.conflict_ratio)
        logger.info(f"The total number of ops is: {total_ops}")
        logger.info(f"The number of conflict docs is: {access_settings.ops}")
        if access_settings.ops != 0:
            self.build_ycsb(self.test_config.load_settings.ycsb_client)
            PerfTest.access_bg(self, task=ycsb_task, target_iterator=self.dest_target_iterator,
                               settings=access_settings)
        logger.info("Monitoring XDCR replication")
        self.monitor_replication()

    def run(self):
        if self.xdcr_settings.eccv:
            self.enable_cross_clustering_versioning()
        if self.test_config.access_settings.ssl_mode == 'data':
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()
        logger.info("Loading the source bucket")
        self.load()

        logger.info("Starting the XDCR replication")
        time_elapsed = self.init_xdcr()
        logger.info("Time elapsed load is: {}".format(time_elapsed))
        logger.info("Finished the XDCR replication, starting the update conflict phase")
        acess_time = self.access()
        logger.info("Time elapsed access is: {}".format(acess_time))
        self.report_kpi(acess_time)


class UniDirXdcrConflictSimTest(BiDirXdcrInitTest):

    def do_conflicts(self, *args):
        logger.info("Starting ConflictSim")
        PerfTest.access(self,
                        task=run_conflictsim_task,
                        source_iterator=self.load_target_iterator,
                        target_iterator=self.dest_target_iterator,
                        settings=self.test_config.access_settings)

    @with_stats
    @with_profiles
    @timeit
    def init_xdcr(self):
        self.add_remote_clusters()
        self.create_replications()
        self.do_conflicts()

    def run(self):
        if self.xdcr_settings.eccv:
            self.enable_cross_clustering_versioning()

        self.load()
        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class UniDirXdcrInitConflictTest(BiDirXdcrInitTest):

    @with_stats
    @with_profiles
    @timeit
    def init_xdcr(self):
        self.add_remote_cluster()
        self.create_replication()
        self.monitor_replication()

    def run(self):
        if self.xdcr_settings.eccv:
            self.enable_cross_clustering_versioning()
        self.load()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)

class UniDirXdcrInitTest(XdcrInitTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.load_target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)


class UniDirXdcrInitRestoreTest(RestoreTest, UniDirXdcrInitTest):

    def __init__(self, *args, **kwargs):
        XdcrInitTest.__init__(self, *args, **kwargs)
        self.load_target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)

    def run(self):

        self.extract_tools()

        if self.test_config.backup_settings.use_tls or \
           self.test_config.restore_settings.use_tls:
            self.download_certificate()

        self.get_tool_versions()

        self.load()
        self.wait_for_persistence()
        # check_num_items doesn't work when SGW is connected to server, as SGW adds its own
        # documents (sync, config, heartbeat)
        if not self.xdcr_settings.mobile:
            self.check_num_items()
        self.compact_bucket(wait=True)

        self.configure_wan()

        time_elapsed = self.init_xdcr()

        logger.info(f"XDCR replication took: {time_elapsed}")

        xdcr_replication_rate = self.test_config.cluster.num_buckets \
            * self.test_config.load_settings.items / time_elapsed
        logger.info(f"XDCR replication rate is: {xdcr_replication_rate}")

        for bucket in self.test_config.buckets:
            m1, m2 = self.cluster_spec.masters
            self.backup(m2)
            replication_id = self.rest.get_xdcr_replication_id(host=m1)
            self.delete_replications(replication_id)
            self.flush_buckets(m1)
            self.flush_buckets(m2)

            try:
                time_elapsed = self.restore(m2)
            finally:
                self.collectlogs()
            break

        self.report_kpi(time_elapsed)


class CapellaUniDirXdcrInitTest(UniDirXdcrInitTest, CapellaXdcrInitTest):
    pass


class AdvFilterXdcrTest(XdcrInitTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.load_target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)

    def xattr_load(self, *args):
        super().xattr_load(*args, target_iterator=self.load_target_iterator)

    def run(self):
        self.load()
        self.xattr_load()
        self.wait_for_persistence()

        self.compact_bucket(wait=True)

        self.configure_wan()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class CapellaAdvFilterXdcrTest(AdvFilterXdcrTest, CapellaXdcrInitTest):

    def run(self):
        self.load()
        self.xattr_load()
        self.wait_for_persistence()
        self.check_num_items()

        time_elapsed = self.init_xdcr()
        self.report_kpi(time_elapsed)


class XdcrPriorityThroughputTest(XdcrTest):

    COLLECTORS = {'xdcr_stats': True}

    CLUSTER_NAME = 'link'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.load_target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)

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
        logger.info(f'xdcr_link1 is: {xdcr_link1}')
        logger.info(f'xdcr_link2 is: {xdcr_link2}')
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
        self.check_num_items()
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

        self.check_num_items()

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

        self.check_num_items()

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
        bucket_replica = self.test_config.bucket.replica_number
        for bucket in self.test_config.buckets:
            num_items = self.monitor.get_num_items(
                host=host, bucket=bucket, bucket_replica=bucket_replica
            )
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
        bucket_replica = self.test_config.bucket.replica_number
        for bucket in self.test_config.buckets:
            backfill_time = self.monitor.monitor_num_backfill_items(
                host=dest_host, bucket=bucket, bucket_replica=bucket_replica, num_items=num_items
            )
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
        self.check_num_items()
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
            'collectionsExplicitMapping': False
        }
        if self.xdcr_settings.filter_expression:
            params.update({
                'filterExpression': self.xdcr_settings.filter_expression,
            })
        return params

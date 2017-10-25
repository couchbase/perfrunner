import os
import time

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import target_hash
from perfrunner.helpers.worker import (
    cbas_bigfun_data_delete_task,
    cbas_bigfun_data_insert_task,
    cbas_bigfun_data_mixload_task,
    cbas_bigfun_data_query_task,
    cbas_bigfun_data_ttl_task,
    cbas_bigfun_data_update_index_task,
    cbas_bigfun_data_update_non_index_task,
    cbas_bigfun_wait_task,
)
from perfrunner.settings import TargetSettings
from perfrunner.tests import PerfTest, TargetIterator


class CBASTargetIterator(TargetIterator):

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        for master_node in self.cluster_spec.masters:
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            for bucket in self.test_config.buckets:
                if self.prefix is None:
                    prefix = target_hash(master_node, bucket)
                yield TargetSettings(cbas_node, bucket, password, prefix)


class CBASBigfunTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cbas_target_iterator = CBASTargetIterator(self.cluster_spec,
                                                       self.test_config,
                                                       prefix=None)

    def download_bigfun(self):
        if self.worker_manager.is_remote:
            self.remote.clone_bigfun(
                socialgen_repo=self.test_config.bigfun_settings.socialgen_repo,
                socialgen_branch=self.test_config.bigfun_settings.socialgen_branch,
                loader_repo=self.test_config.bigfun_settings.loader_repo,
                loader_branch=self.test_config.bigfun_settings.loader_branch,
                worker_home=self.worker_manager.WORKER_HOME)
        else:
            local.clone_bigfun(socialgen_repo=self.test_config.bigfun_settings.socialgen_repo,
                               socialgen_branch=self.test_config.bigfun_settings.socialgen_branch,
                               loader_repo=self.test_config.bigfun_settings.loader_repo,
                               loader_branch=self.test_config.bigfun_settings.loader_branch)

    def generate_doctemplates(self):
        if self.worker_manager.is_remote:
            for clientn in range(len(self.cluster_spec.workers)):
                self.remote.generate_doctemplates(
                    self.cluster_spec.workers[clientn],
                    self.worker_manager.WORKER_HOME,
                    self.test_config.bigfun_settings.workers,
                    self.test_config.bigfun_settings.user_docs,
                    clientn)
        else:
            local.generate_doctemplates(
                    self.test_config.bigfun_settings.workers,
                    self.test_config.bigfun_settings.user_docs,
                    0)

    def collect_export_files(self):
        if self.worker_manager.is_remote:
            if not os.path.exists('loader'):
                os.mkdir('loader')
            self.remote.get_bigfun_export_files(self.worker_manager.WORKER_HOME)

    def create_bigfun_bucket(self, cbas_node: str, bucket_name: str):
        query = "CREATE BUCKET `{bucket}`" \
                " WITH {{\"name\":\"{bucket}\"}};".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_dataset_1st_part(self, cbas_node: str, bucket_name: str):
        query = "CREATE SHADOW DATASET `GleambookUsers{bucket}` ON `{bucket}`" \
                " WHERE `id` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_dataset_2nd_part(self, cbas_node: str, bucket_name: str):
        query = "CREATE SHADOW DATASET `GleambookMessages{bucket}` ON `{bucket}`" \
                " WHERE `message_id` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "CREATE SHADOW DATASET `ChirpMessages{bucket}` ON `{bucket}`" \
                " WHERE `chirpid` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_dataset(self, cbas_node: str, bucket_name: str):
        self.create_bigfun_dataset_1st_part(cbas_node, bucket_name)
        self.create_bigfun_dataset_2nd_part(cbas_node, bucket_name)

    def create_bigfun_index_1st_part(self, cbas_node: str, bucket_name: str):
        query = "CREATE INDEX usrSinceIx ON" \
                " `GleambookUsers{bucket}`(user_since: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_index_2nd_part(self, cbas_node: str, bucket_name: str):
        query = "CREATE INDEX authorIdIx ON" \
                " `GleambookMessages{bucket}`(author_id: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "CREATE INDEX sndTimeIx ON" \
                " `ChirpMessages{bucket}`(send_time: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_index(self, cbas_node: str, bucket_name: str):
        self.create_bigfun_index_1st_part(cbas_node, bucket_name)
        self.create_bigfun_index_2nd_part(cbas_node, bucket_name)

    def drop_bigfun_index(self, cbas_node: str, bucket_name: str):
        query = "DROP INDEX `GleambookUsers{bucket}`.usrSinceIx".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP INDEX `GleambookMessages{bucket}`.authorIdIx".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP INDEX `ChirpMessages{bucket}`.sndTimeIx".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def connect_bigfun_bucket(self, cbas_node: str, bucket_name: str):
        query = "CONNECT BUCKET `{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def disconnect_bigfun_bucket(self, cbas_node: str, bucket_name: str):
        query = "DISCONNECT BUCKET `{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def drop_bigfun_dataset(self, cbas_node: str, bucket_name: str):
        query = "DROP DATASET `GleambookUsers{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP DATASET `GleambookMessages{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP DATASET `ChirpMessages{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    @timeit
    def create_bigfun_indexes(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_index(cbas_node, bucket_name)

    @timeit
    def drop_bigfun_indexes(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.drop_bigfun_index(cbas_node, bucket_name)

    def start_cbas_sync(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_bucket(cbas_node, bucket_name)
            self.create_bigfun_dataset(cbas_node, bucket_name)
            self.create_bigfun_index(cbas_node, bucket_name)
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    def start_cbas_sync_1st_part(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_bucket(cbas_node, bucket_name)
            self.create_bigfun_dataset_1st_part(cbas_node, bucket_name)
            self.create_bigfun_index_1st_part(cbas_node, bucket_name)
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    def start_cbas_sync_2nd_part(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_dataset_2nd_part(cbas_node, bucket_name)
            self.create_bigfun_index_2nd_part(cbas_node, bucket_name)
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    def restart_cbas_sync(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.disconnect_bigfun_bucket(cbas_node, bucket_name)
            self.drop_bigfun_dataset(cbas_node, bucket_name)
            self.create_bigfun_dataset(cbas_node, bucket_name)
            self.create_bigfun_index(cbas_node, bucket_name)
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    def disconnect_bucket(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.disconnect_bigfun_bucket(cbas_node, bucket_name)

    def connect_bucket(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    @timeit
    def monitor_cbas_synced_1st_part(self):
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced_1st_part(master_node,
                                                             bucket,
                                                             cbas_node)

    @timeit
    def monitor_cbas_synced(self):
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced(master_node,
                                                    bucket,
                                                    cbas_node)

    @timeit
    def monitor_cbas_synced_update_non_indexed_field(self):
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced_update_non_index(master_node,
                                                                     bucket,
                                                                     cbas_node)

    @timeit
    def monitor_cbas_synced_update_indexed_field(self):
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced_update_index(master_node,
                                                                 bucket,
                                                                 cbas_node)

    @timeit
    def monitor_cbas_synced_deleted(self):
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_deleted(master_node,
                                                     bucket,
                                                     cbas_node)

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=cbas_bigfun_data_insert_task)

    def insert(self, *args, **kwargs):
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_insert_task)

    def update_non_indexed_field(self, *args, **kwargs):
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_update_non_index_task)

    def update_indexed_field(self, *args, **kwargs):
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_update_index_task)

    def delete(self, *args, **kwargs):
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_delete_task)

    def ttl(self, *args, **kwargs):
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_ttl_task)

    @timeit
    def query(self, *args, **kwargs):
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_query_task,
                               target_iterator=self.cbas_target_iterator)

    @with_stats
    def access(self, *args, **kwargs):
        PerfTest.access(self, task=cbas_bigfun_wait_task)

    def run(self):
        self.download_bigfun()

        self.generate_doctemplates()

        self.load()

        self.access()

        self.report_kpi()

    def _report_kpi(self):
        self.collect_export_files()


class CBASBigfunDataSetTest(CBASBigfunTest):

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.initial_sync_latency = self.monitor_cbas_synced()

        self.restart_cbas_sync()

        self.restart_sync_latency = self.monitor_cbas_synced()

        self.update_non_indexed_field()

        self.non_index_update_sync_latency = self.monitor_cbas_synced_update_non_indexed_field()

        self.update_indexed_field()

        self.index_update_sync_latency = self.monitor_cbas_synced_update_indexed_field()

        self.delete()

        self.delete_sync_latency = self.monitor_cbas_synced_deleted()

        self.insert()

        self.monitor_cbas_synced()

        self.disconnect_bucket()

        self.update_indexed_field()

        self.connect_bucket()

        self.reconnect_index_update_sync_latency = self.monitor_cbas_synced_update_indexed_field()

        self.disconnect_bucket()

        self.update_non_indexed_field()

        self.connect_bucket()

        self.reconnect_non_index_update_sync_latency = \
            self.monitor_cbas_synced_update_non_indexed_field()

        self.disconnect_bucket()

        self.delete()

        self.connect_bucket()

        self.reconnect_delete_sync_latency = self.monitor_cbas_synced_deleted()

    def _report_kpi(self):
        self.collect_export_files()
        if self.initial_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.initial_sync_latency,
                                                "initial_sync_latency_sec",
                                                "Initial sync latency in second")
            )
        if self.restart_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.restart_sync_latency,
                                                "restart_cbas_sync_sec",
                                                "Restart cbas sync in second")
            )
        if self.non_index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(
                    self.non_index_update_sync_latency,
                    "non_index_update_sync_latency_sec",
                    "Nonindex update sync latency in second")
            )
        if self.index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.index_update_sync_latency,
                                                "index_update_sync_latency_sec",
                                                "Index update sync latency in second")
            )
        if self.delete_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.delete_sync_latency,
                                                "delete_sync_latency_sec",
                                                "Delete sync latency in second")
            )
        if self.reconnect_index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(
                    self.reconnect_index_update_sync_latency,
                    "reconnect_index_update_sync_latency_sec",
                    "Reconnect index update sync latency in second")
            )
        if self.reconnect_non_index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(
                    self.reconnect_non_index_update_sync_latency,
                    "reconnect_non_index_update_sync_latency_sec",
                    "Reconnect non index update sync latency in second")
            )
        if self.reconnect_delete_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(
                    self.reconnect_delete_sync_latency,
                    "reconnect_delete_sync_latency_sec",
                    "Reconnect delete sync latency in second")
            )


class CBASBigfunDataSetTTLTest(CBASBigfunTest):

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.initial_sync_latency = self.monitor_cbas_synced()

        self.ttl()

        self.ttl_sync_latency = self.monitor_cbas_synced_deleted()

        self.insert()

        self.monitor_cbas_synced()

    def _report_kpi(self):
        self.collect_export_files()
        if self.ttl_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.ttl_sync_latency,
                                                "ttl_sync_latency_sec",
                                                "ttl  sync latency in second")
            )


class CBASBigfunQueryTest(CBASBigfunTest):

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.monitor_cbas_synced()

        self.query()

    def _report_kpi(self):
        self.collect_export_files()
        query_latencies = self.metrics.parse_cbas_query_latencies()
        for key, value in query_latencies.items():
            self.reporter.post(
                *self.metrics.cbas_query_latency(
                    value,
                    key,
                    "Query latency in MS: " + key)
            )


class CBASBigfunQueryWithBGTest(CBASBigfunTest):

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.monitor_cbas_synced()

        # cbas_bigfun_data_mixload_task is used to trigger backgroup mutation.
        # all mutation are within 1/10 of the whole dataset to avoid too much
        # impact on the query and too much memory usage in current loader tool
        # We will need more test cases to cover wider mutaion
        self.access_bg(task=cbas_bigfun_data_mixload_task)

        querytime = self.query()

        if self.test_config.access_settings.time > querytime:
            time.sleep(self.test_config.access_settings.time - querytime)

    def _report_kpi(self):
        self.collect_export_files()
        query_latencies = self.metrics.parse_cbas_query_latencies()
        for key, value in query_latencies.items():
            self.reporter.post(
                *self.metrics.cbas_query_latency(
                    value,
                    key,
                    "Query latency in MS: " + key)
            )


class CBASBigfunDataSetP2Test(CBASBigfunTest):

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync_1st_part()

        self.sync_latency_1st_part = self.monitor_cbas_synced_1st_part()

        self.disconnect_bucket()

        self.start_cbas_sync_2nd_part()

        self.sync_latency_2nd_part = self.monitor_cbas_synced()

        self.disconnect_bucket()

        self.drop_index_latency = self.drop_bigfun_indexes()

        self.create_index_latency = self.create_bigfun_indexes()

        self.connect_bucket()

    def _report_kpi(self):
        self.collect_export_files()
        if self.sync_latency_1st_part is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.sync_latency_1st_part,
                                                "sync_latency_1st_part_sec",
                                                "Sync latency of 1st part in second")
            )
        if self.sync_latency_2nd_part is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.sync_latency_2nd_part,
                                                "sync_latency_2nd_part_sec",
                                                "Sync latency of 2nd part in second")
            )
        if self.drop_index_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.drop_index_latency,
                                                "drop_index_latency_sec",
                                                "Drop index latency in second")
            )
        if self.create_index_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.create_index_latency,
                                                "create_index_latency_sec",
                                                "Create index latency in second")
            )


class CBASBigfunCleanupBucketTest(CBASBigfunTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup_when_disconnected = self.test_config.bigfun_settings.cleanup_when_disconnected
        self.cleanup_method = self.test_config.bigfun_settings.cleanup_method

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.monitor_cbas_synced()

        if self.cleanup_when_disconnected:
            self.disconnect_bucket()

        if self.cleanup_method == 'flush':
            self.cluster.flush_buckets()
        elif self.cleanup_method == 'delete':
            self.cluster.delete_buckets()
            self.cluster.delete_rbac_users()
            self.cluster.create_buckets()
            self.cluster.add_rbac_users()

        if self.cleanup_when_disconnected:
            self.connect_bucket()

        self.cleanup_bucket_latency = self.monitor_cbas_synced_deleted()

        self.insert()

        self.insert_after_cleanup_bucket_latency = self.monitor_cbas_synced()

    def _report_kpi(self):
        self.collect_export_files()
        if self.cleanup_bucket_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.cleanup_bucket_latency,
                                                "cleanup_bucket_latency_sec",
                                                "Cleanup bucket latency in second")
            )
        if self.insert_after_cleanup_bucket_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_sync_latency(self.insert_after_cleanup_bucket_latency,
                                                "insert_after_cleanup_bucket_latency_sec",
                                                "Insert after cleanup bucket latency in second")
            )

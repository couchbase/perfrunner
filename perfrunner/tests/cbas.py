import os

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import target_hash
from perfrunner.helpers.worker import (
    cbas_bigfun_data_delete_task,
    cbas_bigfun_data_insert_task,
    cbas_bigfun_data_query_task,
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

    def create_bigfun_dataset(self, cbas_node: str, bucket_name: str):
        query = "CREATE SHADOW DATASET `GleambookUsers{bucket}` ON `{bucket}`" \
                " WHERE `id` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "CREATE SHADOW DATASET `GleambookMessages{bucket}` ON `{bucket}`" \
                " WHERE `message_id` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "CREATE SHADOW DATASET `ChirpMessages{bucket}` ON `{bucket}`" \
                " WHERE `chirpid` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_index(self, cbas_node: str, bucket_name: str):
        query = "CREATE INDEX usrSinceIx ON" \
                " `GleambookUsers{bucket}`(user_since: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "CREATE INDEX authorIdIx ON" \
                " `GleambookMessages{bucket}`(author_id: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "CREATE INDEX sndTimeIx ON" \
                " `ChirpMessages{bucket}`(send_time: string);".format(bucket=bucket_name)
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

    def start_cbas_sync(self):
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_bucket(cbas_node, bucket_name)
            self.create_bigfun_dataset(cbas_node, bucket_name)
            self.create_bigfun_index(cbas_node, bucket_name)
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

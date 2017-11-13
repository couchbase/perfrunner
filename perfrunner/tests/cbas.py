import os
import sys
import threading
import time

from logger import logger
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
from perfrunner.tests.rebalance import RebalanceTest, RecoveryTest


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

    CBASMETRIC_CLASSNAME = "CBASBigfunMetricInfo"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cbas_target_iterator = CBASTargetIterator(self.cluster_spec,
                                                       self.test_config,
                                                       prefix=None)

    def download_bigfun(self):
        logger.info('Downloading bigfun git repo.')
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
        logger.info('Generating bigfun dataset with {} users splitted to {} partitions.'.format(
            self.test_config.bigfun_settings.user_docs, self.test_config.bigfun_settings.workers))
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
        logger.info('Collecting files to export.')
        if self.worker_manager.is_remote:
            if not os.path.exists('loader'):
                os.mkdir('loader')
            self.remote.get_bigfun_export_files(self.worker_manager.WORKER_HOME)

    def create_bigfun_bucket(self, cbas_node: str, bucket_name: str):
        logger.info('Create bigfun bucket {}'.format(bucket_name))
        query = "CREATE BUCKET `{bucket}`" \
                " WITH {{\"name\":\"{bucket}\"}};".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_dataset_1st_part(self, cbas_node: str, bucket_name: str):
        logger.info('Create bigfun GleambookUsers dataset on bucket {}'.format(bucket_name))
        query = "CREATE SHADOW DATASET `GleambookUsers{bucket}` ON `{bucket}`" \
                " WHERE `id` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_dataset_2nd_part(self, cbas_node: str, bucket_name: str):
        logger.info('Create bigfun GleambookMessages dataset on bucket {}'.format(bucket_name))
        query = "CREATE SHADOW DATASET `GleambookMessages{bucket}` ON `{bucket}`" \
                " WHERE `message_id` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        logger.info('Create bigfun ChirpMessages dataset on bucket {}'.format(bucket_name))
        query = "CREATE SHADOW DATASET `ChirpMessages{bucket}` ON `{bucket}`" \
                " WHERE `chirpid` is not UNKNOWN;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_dataset(self, cbas_node: str, bucket_name: str):
        self.create_bigfun_dataset_1st_part(cbas_node, bucket_name)
        self.create_bigfun_dataset_2nd_part(cbas_node, bucket_name)

    def create_bigfun_index_1st_part(self, cbas_node: str, bucket_name: str):
        logger.info('Create bigfun usrSinceIx index on GleambookUsers{}'.format(bucket_name))
        query = "CREATE INDEX usrSinceIx ON" \
                " `GleambookUsers{bucket}`(user_since: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_index_2nd_part(self, cbas_node: str, bucket_name: str):
        logger.info('Create bigfun authorIdIx index on GleambookMessages{}'.format(bucket_name))
        query = "CREATE INDEX authorIdIx ON" \
                " `GleambookMessages{bucket}`(author_id: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        logger.info('Create bigfun sndTimeIx index on ChirpMessages{}'.format(bucket_name))
        query = "CREATE INDEX sndTimeIx ON" \
                " `ChirpMessages{bucket}`(send_time: string);".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def create_bigfun_index(self, cbas_node: str, bucket_name: str):
        self.create_bigfun_index_1st_part(cbas_node, bucket_name)
        self.create_bigfun_index_2nd_part(cbas_node, bucket_name)

    def drop_bigfun_index(self, cbas_node: str, bucket_name: str):
        logger.info('Drop all bigfun indexes on bucket {}'.format(bucket_name))
        query = "DROP INDEX `GleambookUsers{bucket}`.usrSinceIx".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP INDEX `GleambookMessages{bucket}`.authorIdIx".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP INDEX `ChirpMessages{bucket}`.sndTimeIx".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def connect_bigfun_bucket(self, cbas_node: str, bucket_name: str):
        logger.info('Connect bigfun bucket {}'.format(bucket_name))
        query = "CONNECT BUCKET `{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def disconnect_bigfun_bucket(self, cbas_node: str, bucket_name: str):
        logger.info('Disconnect bigfun bucket {}'.format(bucket_name))
        query = "DISCONNECT BUCKET `{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    def drop_bigfun_dataset(self, cbas_node: str, bucket_name: str):
        logger.info('Drop bigfun dataset on bucket {}'.format(bucket_name))
        query = "DROP DATASET `GleambookUsers{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP DATASET `GleambookMessages{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)
        query = "DROP DATASET `ChirpMessages{bucket}`;".format(bucket=bucket_name)
        self.rest.run_analytics_query(cbas_node, query)

    @timeit
    def create_bigfun_indexes(self):
        logger.info('Create bigfun dataset indexes')
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_index(cbas_node, bucket_name)

    @timeit
    def drop_bigfun_indexes(self):
        logger.info('Drop bigfun dataset indexes')
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.drop_bigfun_index(cbas_node, bucket_name)

    def start_cbas_sync(self):
        logger.info('Start CBAS bigfun data set syncing')
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_bucket(cbas_node, bucket_name)
            self.create_bigfun_dataset(cbas_node, bucket_name)
            self.create_bigfun_index(cbas_node, bucket_name)
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    def start_cbas_sync_1st_part(self):
        logger.info('Start CBAS bigfun data set 1st part syncing')
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_bucket(cbas_node, bucket_name)
            self.create_bigfun_dataset_1st_part(cbas_node, bucket_name)
            self.create_bigfun_index_1st_part(cbas_node, bucket_name)
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    def start_cbas_sync_2nd_part(self):
        logger.info('Start CBAS bigfun data set 2nd part syncing')
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.create_bigfun_dataset_2nd_part(cbas_node, bucket_name)
            self.create_bigfun_index_2nd_part(cbas_node, bucket_name)
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    def restart_cbas_sync(self):
        logger.info('Restart CBAS bigfun data set syncing')
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
        logger.info('Disconnect CBAS bigfun data set')
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.disconnect_bigfun_bucket(cbas_node, bucket_name)

    def connect_bucket(self):
        logger.info('Connect CBAS bigfun data set')
        for target in self.target_iterator:
            bucket_name = target.bucket
            master_node = target.node
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.connect_bigfun_bucket(cbas_node, bucket_name)

    @timeit
    def monitor_cbas_synced_1st_part(self):
        logger.info('Monitoring bigfun data 1st part syncing.')
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced_1st_part(master_node,
                                                             bucket,
                                                             cbas_node)

    @timeit
    def monitor_cbas_synced(self):
        self._monitor_cbas_synced()

    def _monitor_cbas_synced(self):
        logger.info('Monitoring bigfun full data syncing.')
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced(master_node,
                                                    bucket,
                                                    cbas_node)

    @timeit
    def monitor_cbas_synced_update_non_indexed_field(self):
        logger.info('Monitoring bigfun non index data update syncing.')
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced_update_non_index(master_node,
                                                                     bucket,
                                                                     cbas_node)

    @timeit
    def monitor_cbas_synced_update_indexed_field(self):
        logger.info('Monitoring bigfun index data update syncing.')
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_synced_update_index(master_node,
                                                                 bucket,
                                                                 cbas_node)

    @timeit
    def monitor_cbas_synced_deleted(self):
        logger.info('Monitoring bigfun data delete syncing.')
        for target in self.target_iterator:
            master_node = target.node
            bucket = target.bucket
            cbas_node = self.cluster_spec.servers_by_master_by_role(master_node, "cbas")[0]
            self.monitor.monitor_bigfun_data_deleted(master_node,
                                                     bucket,
                                                     cbas_node)

    def load(self, *args, **kwargs):
        logger.info('Start task for loading bigfun data.')
        t0 = time.time()
        PerfTest.load(self, task=cbas_bigfun_data_insert_task)
        self.initial_load_latency = time.time() - t0

    @timeit
    def insert(self, *args, **kwargs):
        logger.info('Start task for inserting bigfun data.')
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_insert_task)

    @timeit
    def update_non_indexed_field(self, *args, **kwargs):
        logger.info('Start task for updating bigfun non index data.')
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_update_non_index_task)

    @timeit
    def update_indexed_field(self, *args, **kwargs):
        logger.info('Start task for updating bigfun index data.')
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_update_index_task)

    @timeit
    def delete(self, *args, **kwargs):
        logger.info('Start task for deleting bigfun data.')
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_delete_task)

    @timeit
    def ttl(self, *args, **kwargs):
        logger.info('Start task for setting bigfun data TTL.')
        PerfTest.trigger_tasks(self, task=cbas_bigfun_data_ttl_task)

    @timeit
    def query(self, *args, **kwargs):
        logger.info('Start task for querying bigfun data.')
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
        if self.initial_load_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.initial_load_latency,
                                           "initial_load_latency_sec",
                                           "Initial document injestion latency (second)")
            )


class CBASBigfunDataSetTest(CBASBigfunTest):

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.initial_sync_latency = self.monitor_cbas_synced()

        self.restart_cbas_sync()

        self.restart_sync_latency = self.monitor_cbas_synced()

        self.update_non_indexed_field_latency = self.update_non_indexed_field()

        self.non_index_update_sync_latency = self.monitor_cbas_synced_update_non_indexed_field()

        self.update_indexed_field_latency = self.update_indexed_field()

        self.index_update_sync_latency = self.monitor_cbas_synced_update_indexed_field()

        self.delete_latency = self.delete()

        self.delete_sync_latency = self.monitor_cbas_synced_deleted()

        self.reinsert_latency = self.insert()

        self.reinsert_sync_latency = self.monitor_cbas_synced()

        self.disconnect_bucket()

        self.disconnect_update_indexed_field_latency = self.update_indexed_field()

        self.connect_bucket()

        self.reconnect_index_update_sync_latency = self.monitor_cbas_synced_update_indexed_field()

        self.disconnect_bucket()

        self.disconnect_update_non_indexed_field_latency = self.update_non_indexed_field()

        self.connect_bucket()

        self.reconnect_non_index_update_sync_latency = \
            self.monitor_cbas_synced_update_non_indexed_field()

        self.disconnect_bucket()

        self.disconnect_delete_latency = self.delete()

        self.connect_bucket()

        self.reconnect_delete_sync_latency = self.monitor_cbas_synced_deleted()

    def _report_kpi(self):
        super()._report_kpi()
        if self.initial_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.initial_sync_latency,
                                           "initial_sync_latency_sec",
                                           "Initial Analytics sync latency (second)")
            )
        if self.restart_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.restart_sync_latency,
                                           "restart_cbas_sync_sec",
                                           "Restart Analytics sync latency (second)")
            )
        if self.update_non_indexed_field_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.update_non_indexed_field_latency,
                                           "update_non_indexed_field_latency_sec",
                                           "Update nonindexed document field latency (second)")
            )
        if self.non_index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(
                    self.non_index_update_sync_latency,
                    "non_index_update_sync_latency_sec",
                    "Nonindex update Analytics sync latency (second)")
            )
        if self.update_indexed_field_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.update_indexed_field_latency,
                                           "update_indexed_field_latency_sec",
                                           "Update indexed document field latency (second)")
            )
        if self.index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.index_update_sync_latency,
                                           "index_update_sync_latency_sec",
                                           "Index update Analytics sync latency (second)")
            )
        if self.delete_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.delete_latency,
                                           "delete_latency_sec",
                                           "Delete document latency (second)")
            )
        if self.delete_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.delete_sync_latency,
                                           "delete_sync_latency_sec",
                                           "Delete Analytics sync latency (second)")
            )
        if self.reinsert_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.reinsert_latency,
                                           "reinsert_latency_sec",
                                           "Reinsert document latency (second)")
            )
        if self.reinsert_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.reinsert_sync_latency,
                                           "reinsert_sync_latency_sec",
                                           "Reinsert Analytics sync latency (second)")
            )
        if self.disconnect_update_indexed_field_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(
                    self.disconnect_update_indexed_field_latency,
                    "disconnect_update_indexed_field_latency_sec",
                    "Update indexed document field latency (second) with cbas disconnected")
            )
        if self.reconnect_index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(
                    self.reconnect_index_update_sync_latency,
                    "reconnect_index_update_sync_latency_sec",
                    "Index update Analytics sync latency (second) with cbas reconnected")
            )
        if self.disconnect_update_non_indexed_field_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(
                    self.disconnect_update_non_indexed_field_latency,
                    "disconnect_update_non_indexed_field_latency_sec",
                    "Update non indexed document field latency (second) with cbas disconnected")
            )
        if self.reconnect_non_index_update_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(
                    self.reconnect_non_index_update_sync_latency,
                    "reconnect_non_index_update_sync_latency_sec",
                    "Non index update Analytics sync latency (second) with cbas reconnected")
            )
        if self.disconnect_delete_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(
                    self.disconnect_delete_latency,
                    "disconnect_delete_latency_sec",
                    "Delete document latency (second) with cbas disconnected")
            )
        if self.reconnect_delete_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(
                    self.reconnect_delete_sync_latency,
                    "reconnect_delete_sync_latency_sec",
                    "Delete Analytics sync latency (second) with cbas reconnected")
            )


class CBASBigfunDataSetTTLTest(CBASBigfunTest):

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.initial_sync_latency = self.monitor_cbas_synced()

        self.ttl_latency = self.ttl()

        self.ttl_sync_latency = self.monitor_cbas_synced_deleted()

        self.insert()

        self.monitor_cbas_synced()

    def _report_kpi(self):
        super()._report_kpi()
        if self.initial_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.initial_sync_latency,
                                           "initial_sync_latency_sec",
                                           "Initial Analytics sync latency (second)")
            )
        if self.ttl_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.ttl_latency,
                                           "ttl_latency_sec",
                                           "TTL document update latency (second)")
            )
        if self.ttl_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.ttl_sync_latency,
                                           "ttl_sync_latency_sec",
                                           "TTL Analytics sync latency (second)")
            )


class CBASBigfunStableStateTest(CBASBigfunTest):

    """Stable state CBAS syncing test.

    Test after initial syncing of CBAS data
    test that measures latency of CBAS data
    syncing in ms (via "cbas_lag" collector)
    """

    COLLECTORS = {'cbas_lag': True}

    def run(self):
        self.download_bigfun()

        self.generate_doctemplates()

        self.load()

        self.start_cbas_sync()

        self.initial_sync_latency = self.monitor_cbas_synced()

        self.access()

        self.report_kpi()

    def _report_kpi(self):
        super()._report_kpi()
        if self.initial_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.initial_sync_latency,
                                           "initial_sync_latency_sec",
                                           "Initial Analytics sync latency (second)")
            )
        self.reporter.post(*self.metrics.cbas_lag())


class CBASBigfunQueryTest(CBASBigfunStableStateTest):

    REPORT_QUERY_DETAIL = True

    @with_stats
    def access(self, *args, **kwargs):
        self.query()

    def _report_kpi(self):
        super()._report_kpi()
        query_stats = self.metrics.parse_cbas_query_highlevel_metrics()
        self.reporter.post(
            *self.metrics.cbas_query_metric(
                query_stats['total_query_number'],
                'total_query_number',
                'Total query number')
        )
        self.reporter.post(
            *self.metrics.cbas_query_metric(
                query_stats['success_query_rate'],
                'success_query_rate',
                'Success query rate')
        )
        self.reporter.post(
            *self.metrics.cbas_query_metric(
                query_stats['avg_query_latency'],
                'avg_query_latency',
                'Avg query latency (ms)')
        )
        if self.REPORT_QUERY_DETAIL:
            query_latencies = self.metrics.parse_cbas_query_latencies()
            for key, value in query_latencies.items():
                self.reporter.post(
                    *self.metrics.cbas_query_metric(
                        value,
                        key.replace(' ', '_'),
                        "Avg query latency (ms) " + key)
                )


class CBASBigfunQueryWithBGTest(CBASBigfunQueryTest):

    @with_stats
    def access(self, *args, **kwargs):
        """cbas_bigfun_data_mixload_task is used to trigger backgroup mutation.

        All mutation are within 1/10 of the whole dataset to avoid too much
        impact on the query and too much memory usage in current loader tool
        We will need more test cases to cover wider mutaion
        """
        logger.info('Start CBAS bigfun mix load task')
        self.access_bg(task=cbas_bigfun_data_mixload_task)

        querytime = self.query()

        if self.test_config.access_settings.time > querytime:
            logger.info('Sleep for {} seconds'.format(self.test_config.access_settings.time -
                                                      querytime))
            time.sleep(self.test_config.access_settings.time - querytime)


class CBASRebalanceTest(RebalanceTest):

    REBALANCE_SERVICES = None

    def post_rebalance_new_nodes(self, new_nodes):
        """If we rebalanced-in new cbas nodes, need to apply cbas node settings to them."""
        if self.REBALANCE_SERVICES == 'cbas':
            settings = self.test_config.cbas_settings.node_settings
            for analytics_node in new_nodes:
                for parameter, value in settings.items():
                    self.rest.set_cbas_node_settings(analytics_node,
                                                     {parameter: value})
                self.rest.restart_analytics(analytics_node)


class CBASBigfunQueryWithBGRebalanceTest(CBASBigfunQueryTest, CBASRebalanceTest):

    """Test measure query latency, cbas_lag during kv node rebalance."""

    REBALANCE_SERVICES = None

    REPORT_QUERY_DETAIL = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0
        self.rebalance_latency = 0
        self.thr_exceptions = []

    def query_thr(self):
        try:
            logger.info('Start query')
            self.query()
            logger.info('Finished query')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    def rebalance_thr(self):
        try:
            logger.info('Start rebalancing')
            t0 = time.time()
            self._rebalance(services=self.REBALANCE_SERVICES)
            if not self.is_balanced():
                raise Exception("cluster was not rebalanced after rebalance job")
            self.rebalance_latency = time.time() - t0  # Rebalance time in seconds
            logger.info('Finished rebalancing')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    @with_stats
    def access(self, *args, **kwargs):
        logger.info('Start CBAS bigfun mixload task')
        self.access_bg(task=cbas_bigfun_data_mixload_task)
        t0 = time.time()
        thr1 = threading.Thread(target=self.query_thr)
        thr2 = threading.Thread(target=self.rebalance_thr)
        thr1.start()
        thr2.start()
        thr1.join()
        thr2.join()
        logger.info('Query and rebalance are done')
        if len(self.thr_exceptions) > 0:
            raise self.thr_exceptions[0][1]
        if self.test_config.access_settings.time > (time.time() - t0):
            logger.info('Sleep for {} seconds'.format(self.test_config.access_settings.time -
                                                      (time.time() - t0)))
            time.sleep(self.test_config.access_settings.time - (time.time() - t0))

    def _report_kpi(self):
        CBASBigfunQueryTest._report_kpi(self)
        if self.rebalance_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.rebalance_latency,
                                           "rebalance_latency_sec",
                                           "Rebalance latency (second)")
            )


class CBASBigfunQueryWithBGRebalanceCBASTest(CBASBigfunQueryWithBGRebalanceTest):

    """Test measure query latency, cbas_lag during cbas node rebalance."""

    REBALANCE_SERVICES = 'cbas'


class CBASBigfunDataSyncRebalanceTest(CBASBigfunTest, CBASRebalanceTest):

    """Test measure initial cbas sync latency during kv node rebalance."""

    REBALANCE_SERVICES = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0
        self.rebalance_latency = 0
        self.initial_sync_latency = 0
        self.thr_exceptions = []

    def cbas_sync_thr(self):
        try:
            logger.info('Start monitoring CBAS syncing')
            t0 = time.time()
            self._monitor_cbas_synced()
            self.initial_sync_latency = time.time() - t0  # CBAS sync time in seconds
            logger.info('Finished monitoring CBAS syncing')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    def rebalance_thr(self):
        try:
            logger.info('Start rebalancing')
            t0 = time.time()
            self._rebalance(services=self.REBALANCE_SERVICES)
            if not self.is_balanced():
                raise Exception("cluster was not rebalanced after rebalance job")
            self.rebalance_latency = time.time() - t0  # Rebalance time in seconds
            logger.info('Finished rebalancing')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    @with_stats
    def access(self, *args, **kwargs):
        logger.info('Start syncing')
        self.start_cbas_sync()
        thr1 = threading.Thread(target=self.cbas_sync_thr)
        thr2 = threading.Thread(target=self.rebalance_thr)
        thr1.start()
        thr2.start()
        thr1.join()
        thr2.join()
        logger.info('Done syncing and rebalancing')
        if len(self.thr_exceptions) > 0:
            raise self.thr_exceptions[0][1]

    def _report_kpi(self):
        CBASBigfunTest._report_kpi(self)
        if self.initial_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.initial_sync_latency,
                                           "initial_sync_latency_sec",
                                           "Initial Analytics sync latency (second)")
            )
        if self.rebalance_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.rebalance_latency,
                                           "rebalance_latency_sec",
                                           "Rebalance latency (second)")
            )


class CBASBigfunDataSyncRebalanceCBASTest(CBASBigfunDataSyncRebalanceTest):

    """Test measure initial cbas sync latency during cbas node rebalance."""

    REBALANCE_SERVICES = 'cbas'


class CBASBigfunQueryWithBGRecoveryTest(CBASBigfunQueryTest, RecoveryTest):

    """Test measure cbas query latency, cbas_lag during kv node failover and recovery."""

    REPORT_QUERY_DETAIL = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0
        self.recovery_latency = 0
        self.thr_exceptions = []

    def query_thr(self):
        try:
            logger.info('Start query')
            self.query()
            logger.info('Done query')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    def recovery_thr(self):
        try:
            logger.info('Start failover')
            logger.info('Sleeping {} seconds before triggering failover'
                        .format(self.rebalance_settings.delay_before_failover))
            time.sleep(self.rebalance_settings.delay_before_failover)
            t0 = time.time()
            self._failover()
            self.failover_latency = time.time() - t0  # Failover time in seconds
            logger.info('Sleeping for {} seconds before rebalance'
                        .format(self.test_config.rebalance_settings.start_after))
            time.sleep(self.test_config.rebalance_settings.start_after)
            t0 = time.time()
            self._rebalance()
            if not self.is_balanced():
                raise Exception("cluster was not rebalanced after recovery job")
            self.recovery_latency = time.time() - t0  # Rebalance time in seconds
            logger.info('Sleeping for {} seconds after rebalance'
                        .format(self.test_config.rebalance_settings.stop_after))
            time.sleep(self.test_config.rebalance_settings.stop_after)
            logger.info('Done failover')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    @with_stats
    def access(self, *args, **kwargs):
        logger.info('Start CBAS bigfun mixload')
        self.access_bg(task=cbas_bigfun_data_mixload_task)
        t0 = time.time()
        thr1 = threading.Thread(target=self.query_thr)
        thr2 = threading.Thread(target=self.recovery_thr)
        thr1.start()
        thr2.start()
        thr1.join()
        thr2.join()
        logger.info('Done query and failover')
        if len(self.thr_exceptions) > 0:
            raise self.thr_exceptions[0][1]
        if self.test_config.access_settings.time > (time.time() - t0):
            logger.info('Sleep {} seconds'.format(self.test_config.access_settings.time -
                                                  (time.time() - t0)))
            time.sleep(self.test_config.access_settings.time - (time.time() - t0))

    def _report_kpi(self):
        CBASBigfunQueryTest._report_kpi(self)
        if self.failover_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.failover_latency,
                                           "failover_latency_sec",
                                           "Failover latency (second)")
            )
        if self.recovery_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.recovery_latency,
                                           "reovery_latency_sec",
                                           "Reovery latency (second)")
            )


class CBASBigfunDataSyncRecoveryTest(CBASBigfunTest, RecoveryTest):

    """Test measure cbas initial syncing latency during kv node failover and recovery."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0
        self.recovery_latency = 0
        self.initial_sync_latency = 0
        self.thr_exceptions = []

    def cbas_sync_thr(self):
        try:
            logger.info('Start monitoring CBAS syncing')
            t0 = time.time()
            self._monitor_cbas_synced()
            self.initial_sync_latency = time.time() - t0  # CBAS sync time in seconds
            logger.info('Done monitoring CBAS syncing')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    def recovery_thr(self):
        try:
            logger.info('Start failover')
            logger.info('Sleeping {} seconds before triggering failover'
                        .format(self.rebalance_settings.delay_before_failover))
            time.sleep(self.rebalance_settings.delay_before_failover)
            t0 = time.time()
            self._failover()
            self.failover_latency = time.time() - t0  # Failover time in seconds
            logger.info('Sleeping for {} seconds before rebalance'
                        .format(self.test_config.rebalance_settings.start_after))
            time.sleep(self.test_config.rebalance_settings.start_after)
            t0 = time.time()
            self._rebalance()
            if not self.is_balanced():
                raise Exception("cluster was not rebalanced after recovery job")
            self.recovery_latency = time.time() - t0  # Rebalance time in seconds
            logger.info('Sleeping for {} seconds after rebalance'
                        .format(self.test_config.rebalance_settings.stop_after))
            time.sleep(self.test_config.rebalance_settings.stop_after)
            logger.info('Finish failover')
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    @with_stats
    def access(self, *args, **kwargs):
        logger.info('Start syncing')
        self.start_cbas_sync()
        thr1 = threading.Thread(target=self.cbas_sync_thr)
        thr2 = threading.Thread(target=self.recovery_thr)
        thr1.start()
        thr2.start()
        thr1.join()
        thr2.join()
        logger.info('Done syncing and failover')
        if len(self.thr_exceptions) > 0:
            raise self.thr_exceptions[0][1]

    def _report_kpi(self):
        CBASBigfunTest._report_kpi(self)
        if self.initial_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.initial_sync_latency,
                                           "initial_sync_latency_sec",
                                           "Initial Analytics sync latency (second)")
            )
        if self.failover_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.failover_latency,
                                           "failover_latency_sec",
                                           "Failover latency (second)")
            )
        if self.recovery_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.recovery_latency,
                                           "reovery_latency_sec",
                                           "Reovery latency (second)")
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
        super()._report_kpi()
        if self.sync_latency_1st_part is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.sync_latency_1st_part,
                                           "sync_latency_1st_part_sec",
                                           "1st part Analytics sync latency (second)")
            )
        if self.sync_latency_2nd_part is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.sync_latency_2nd_part,
                                           "sync_latency_2nd_part_sec",
                                           "2nd part Analytics sync latency (second)")
            )
        if self.drop_index_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.drop_index_latency,
                                           "drop_index_latency_sec",
                                           "Drop index latency (second)")
            )
        if self.create_index_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.create_index_latency,
                                           "create_index_latency_sec",
                                           "Create index latency (second)")
            )


class CBASBigfunCleanupBucketTest(CBASBigfunTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup_when_disconnected = self.test_config.bigfun_settings.cleanup_when_disconnected
        self.cleanup_method = self.test_config.bigfun_settings.cleanup_method

    @with_stats
    def access(self, *args, **kwargs):
        self.start_cbas_sync()

        self.initial_sync_latency = self.monitor_cbas_synced()

        if self.cleanup_when_disconnected:
            self.disconnect_bucket()

        if self.cleanup_method == 'flush':
            logger.info('Start flushing buckets')
            self.cluster.flush_buckets()
            logger.info('Done flushing buckets')
        elif self.cleanup_method == 'delete':
            logger.info('Start deleting and recreating buckets')
            self.cluster.delete_buckets()
            self.cluster.delete_rbac_users()
            self.cluster.create_buckets()
            self.cluster.add_rbac_users()
            self.cluster.wait_until_warmed_up()
            logger.info('Done deleting and recreating buckets')

        if self.cleanup_when_disconnected:
            self.connect_bucket()

        self.cleanup_bucket_latency = self.monitor_cbas_synced_deleted()

        self.reinsert_latency = self.insert()

        self.reinsert_sync_latency = self.monitor_cbas_synced()

    def _report_kpi(self):
        super()._report_kpi()
        if self.initial_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.initial_sync_latency,
                                           "initial_sync_latency_sec",
                                           "Initial Analytics sync latency (second)")
            )
        if self.cleanup_bucket_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.cleanup_bucket_latency,
                                           "cleanup_bucket_latency_sec",
                                           "Cleanup bucket Analytics sync latency (second)")
            )
        if self.reinsert_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.reinsert_latency,
                                           "reinsert_latency_sec",
                                           "Reinsert document latency (second)")
            )
        if self.reinsert_sync_latency is not None:
            self.reporter.post(
                *self.metrics.cbas_latency(self.reinsert_sync_latency,
                                           "reinsert_sync_latency_sec",
                                           "Reinsert Analytics sync latency (second)")
            )

import json
import random
import time
from typing import List, Tuple

import requests

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.rest import (
    ANALYTICS_PORT,
    ANALYTICS_PORT_SSL,
    FTS_PORT,
    FTS_PORT_SSL,
    GOLDFISH_NEBULA_ANALYTICS_PORT,
    QUERY_PORT,
    QUERY_PORT_SSL,
)
from perfrunner.helpers.worker import tpcds_initial_data_load_task
from perfrunner.settings import CH2ConnectionSettings
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import CapellaRebalanceKVTest, RebalanceTest
from perfrunner.tests.xdcr import SrcTargetIterator
from perfrunner.workloads.bigfun.driver import QueryMethod, bigfun
from perfrunner.workloads.bigfun.query_gen import Query
from perfrunner.workloads.tpcdsfun.driver import tpcds

QueryLatencyPair = Tuple[Query, int]


class BigFunTest(PerfTest):

    COLLECTORS = {'analytics': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0
        self.analytics_settings = self.test_config.analytics_settings
        self.config_file = self.analytics_settings.analytics_config_file
        self.analytics_link = self.analytics_settings.analytics_link
        if self.analytics_link == "Local":
            self.data_node = self.master_node
            self.analytics_node = self.analytics_nodes[0]
        else:
            self.data_node, self.analytics_node = self.cluster_spec.masters

    def create_datasets(self, bucket: str):
        self.disconnect_link()
        logger.info('Creating datasets')
        for dataset, key in (
            ('GleambookUsers-1', 'id'),
            ('GleambookMessages-1', 'message_id'),
            ('ChirpMessages-1', 'chirpid'),
        ):
            statement = "CREATE DATASET `{}` ON `{}` AT `{}` WHERE `{}` IS NOT UNKNOWN;"\
                .format(dataset, bucket, self.analytics_link, key)
            self.rest.exec_analytics_statement(self.analytics_node,
                                               statement)

    def create_datasets_collections(self, bucket: str):
        self.disconnect_link()
        logger.info('Creating datasets')
        with open(self.config_file, "r") as json_file:
            analytics_config = json.load(json_file)
        dataset_list = analytics_config["Analytics"]
        if analytics_config["DefaultCollection"]:
            for dataset in dataset_list:
                statement = "CREATE DATASET `{}` ON `{}` AT `{}` " \
                            "WHERE `type` = \"{}\" and meta().id like \"%-{}\";"\
                    .format(dataset["Dataset"], bucket, self.analytics_link,
                            dataset["Type"], dataset["Group"])
                self.rest.exec_analytics_statement(self.analytics_node, statement)
        else:
            for dataset in dataset_list:
                statement = "CREATE DATASET `{}` ON `{}`.`scope-1`.`{}` AT `{}`;"\
                    .format(dataset["Dataset"], bucket, dataset["Collection"], self.analytics_link)
                self.rest.exec_analytics_statement(self.analytics_node, statement)

    def create_index(self):
        logger.info('Creating indexes')
        for statement in (
            "CREATE INDEX usrSinceIdx   ON `GleambookUsers-1`(user_since: string);",
            "CREATE INDEX gbmSndTimeIdx ON `GleambookMessages-1`(send_time: string);",
            "CREATE INDEX cmSndTimeIdx  ON `ChirpMessages-1`(send_time: string);",
        ):
            self.rest.exec_analytics_statement(self.analytics_node,
                                               statement)

    def create_index_collections(self):
        logger.info('Creating indexes')
        with open(self.config_file, "r") as json_file:
            analytics_config = json.load(json_file)
        index_list = analytics_config["Analytics"]

        for index in index_list:
            statement = "CREATE INDEX `{}` ON `{}`({}: string);"\
                .format(index["Index"], index["Dataset"], index["Field"])
            self.rest.exec_analytics_statement(self.analytics_node, statement)

    def disconnect_bucket(self, bucket: str):
        logger.info('Disconnecting the bucket: {}'.format(bucket))
        statement = 'DISCONNECT BUCKET `{}`;'.format(bucket)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def connect_link(self):
        logger.info('Connecting Link {}'.format(self.analytics_link))
        statement = "CONNECT link {}".format(self.analytics_link)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def disconnect_link(self):
        logger.info('DISCONNECT LINK {}'.format(self.analytics_link))
        statement = "DISCONNECT LINK {}".format(self.analytics_link)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def disconnect(self):
        for target in self.target_iterator:
            self.disconnect_bucket(target.bucket)

    def sync(self):
        for bucket in self.test_config.buckets:
            if self.config_file:
                self.create_datasets_collections(bucket)
                self.create_index_collections()
            else:
                self.create_datasets(bucket)
                self.create_index()
        self.connect_link()
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(self.data_node,
                                                               bucket,
                                                               self.analytics_node)

    def re_sync(self):
        self.connect_link()
        for bucket in self.test_config.buckets:
            self.monitor.monitor_data_synced(self.data_node, bucket, self.analytics_node)

    def set_analytics_logging_level(self):
        log_level = self.analytics_settings.log_level
        self.rest.set_analytics_logging_level(self.analytics_node, log_level)
        self.rest.restart_analytics_cluster(self.analytics_node)
        if not self.rest.validate_analytics_logging_level(self.analytics_node, log_level):
            logger.error('Failed to set logging level {}'.format(log_level))

    def set_buffer_cache_page_size(self):
        page_size = self.analytics_settings.storage_buffer_cache_pagesize
        self.rest.set_analytics_page_size(self.analytics_node, page_size)
        self.rest.restart_analytics_cluster(self.analytics_node)

    def set_storage_compression_block(self):
        storage_compression_block = self.analytics_settings.storage_compression_block
        self.rest.set_analytics_storage_compression_block(self.analytics_node,
                                                          storage_compression_block)
        self.rest.restart_analytics_cluster(self.analytics_node)
        self.rest.validate_analytics_setting(self.analytics_node, 'storageCompressionBlock',
                                             storage_compression_block)

    def get_dataset_items(self, dataset: str):
        logger.info('Get number of items in dataset {}'.format(dataset))
        statement = "SELECT COUNT(*) from `{}`;".format(dataset)
        result = self.rest.exec_analytics_statement(self.analytics_node, statement)
        num_items = result.json()['results'][0]['$1']
        logger.info("Number of items in dataset {}: {}".format(dataset, num_items))
        return num_items

    def restore_remote_storage(self):
        self.remote.extract_cb_any(filename='couchbase',
                                   worker_home=self.worker_manager.WORKER_HOME)
        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)

        credential = local.read_aws_credential(
            self.test_config.backup_settings.aws_credential_path)
        self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()

        self.remote.restore(cluster_spec=self.cluster_spec,
                            master_node=self.master_node,
                            threads=self.test_config.restore_settings.threads,
                            worker_home=self.worker_manager.WORKER_HOME,
                            archive=self.test_config.restore_settings.backup_storage,
                            repo=self.test_config.restore_settings.backup_repo,
                            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
                            obj_region=self.test_config.backup_settings.obj_region,
                            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
                            use_tls=self.test_config.restore_settings.use_tls,
                            map_data=self.test_config.restore_settings.map_data,
                            encrypted=self.test_config.restore_settings.encrypted,
                            passphrase=self.test_config.restore_settings.passphrase)
        self.wait_for_persistence()

    def run(self):
        self.restore_local()
        self.wait_for_persistence()


class BigFunSyncTest(BigFunTest):

    def _report_kpi(self, sync_time: int):
        self.reporter.post(
            *self.metrics.avg_ingestion_rate(self.num_items, sync_time)
        )

    @with_stats
    @timeit
    def sync(self):
        super().sync()

    def run(self):
        super().run()

        if self.analytics_link != "Local":
            rest_username, rest_password = self.cluster_spec.rest_credentials
            local.create_remote_link(self.analytics_link, self.data_node, self.analytics_node,
                                     rest_username, rest_password)

        sync_time = self.sync()

        self.report_kpi(sync_time)


class BigFunSyncCloudTest(BigFunSyncTest):

    def run(self):
        self.restore_remote_storage()

        if self.analytics_link != "Local":
            rest_username, rest_password = self.cluster_spec.rest_credentials
            local.create_remote_link(self.analytics_link, self.data_node, self.analytics_node,
                                     rest_username, rest_password)

        sync_time = self.sync()

        self.report_kpi(sync_time)


class BigFunDropDatasetTest(BigFunTest):

    def _report_kpi(self, num_items, time_elapsed):
        self.reporter.post(
            *self.metrics.avg_drop_rate(num_items, time_elapsed)
        )

    @with_stats
    @timeit
    def drop_dataset(self, drop_dataset: str):
        for target in self.target_iterator:
            self.rest.delete_collection(host=target.node,
                                        bucket=target.bucket,
                                        scope="scope-1",
                                        collection=drop_dataset)
        self.monitor.monitor_dataset_drop(self.analytics_node, drop_dataset)

    def run(self):
        super().run()
        super().sync()

        drop_dataset = self.analytics_settings.drop_dataset
        num_items = self.get_dataset_items(drop_dataset)

        drop_time = self.drop_dataset(drop_dataset)

        self.report_kpi(num_items, drop_time)


class BigFunSyncWithCompressionTest(BigFunSyncTest):

    def run(self):
        self.set_storage_compression_block()
        super().run()


class BigFunSyncNoIndexTest(BigFunSyncTest):

    def create_index(self):
        pass

    def create_index_collections(self):
        pass


class BigFunIncrSyncTest(BigFunTest):

    def _report_kpi(self, sync_time: int):
        self.reporter.post(
            *self.metrics.avg_ingestion_rate(self.num_items, sync_time)
        )

    @with_stats
    @timeit
    def re_sync(self):
        super().re_sync()

    def run(self):
        super().run()

        self.sync()

        self.disconnect_link()

        super().run()

        sync_time = self.re_sync()

        self.report_kpi(sync_time)


class BigFunQueryTest(BigFunTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.QUERIES = self.analytics_settings.queries

    def warmup(self, nodes: list = []) -> List[QueryLatencyPair]:
        if len(nodes) == 0:
            analytics_nodes = self.analytics_nodes
        else:
            analytics_nodes = nodes
        logger.info("analytics_nodes = {}".format(analytics_nodes))
        results = bigfun(self.rest,
                         nodes=analytics_nodes,
                         concurrency=self.test_config.access_settings.analytics_warmup_workers,
                         num_requests=int(self.test_config.access_settings.analytics_warmup_ops),
                         query_set=self.QUERIES)

        return [(query, latency) for query, latency in results]

    @with_stats
    def access(self, nodes: list = [], *args, **kwargs) -> List[QueryLatencyPair]:
        if len(nodes) == 0:
            analytics_nodes = self.analytics_nodes
        else:
            analytics_nodes = nodes
        logger.info("analytics_nodes = {}".format(analytics_nodes))
        results = bigfun(self.rest,
                         nodes=analytics_nodes,
                         concurrency=int(self.test_config.access_settings.workers),
                         num_requests=int(self.test_config.access_settings.ops),
                         query_set=self.QUERIES)
        return [(query, latency) for query, latency in results]

    def _report_kpi(self, results: List[QueryLatencyPair]):
        for query, latency in results:
            self.reporter.post(
                *self.metrics.analytics_latency(query, latency)
            )

    def run(self):
        random.seed(8095)
        super().run()

        self.sync()

        logger.info('Running warmup phase')
        self.warmup()

        logger.info('Running access phase')
        results = self.access()

        self.report_kpi(results)


class BigFunQueryCloudTest(BigFunQueryTest):

    def run(self):
        random.seed(8095)
        self.restore_remote_storage()

        self.sync()

        logger.info('Running warmup phase')
        self.warmup()

        logger.info('Running access phase')
        results = self.access()

        self.report_kpi(results)


class BigFunQueryNoIndexCloudTest(BigFunQueryCloudTest):

    def create_index(self):
        pass

    def create_index_collections(self):
        pass


class BigFunQueryWithCompressionTest(BigFunQueryTest):

    def run(self):
        self.set_storage_compression_block()
        super().run()


class BigFunQueryNoIndexTest(BigFunQueryTest):

    def create_index(self):
        pass

    def create_index_collections(self):
        pass


class BigFunQueryNoIndexExternalTest(BigFunQueryTest):

    def set_up_s3_link(self):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        external_dataset_type = self.analytics_settings.external_dataset_type
        external_dataset_region = self.analytics_settings.external_dataset_region
        access_key_id, secret_access_key =\
            local.get_aws_credential(self.analytics_settings.aws_credential_path)
        baseurl = self.rest._get_api_url(self.analytics_node, '',
                                         ANALYTICS_PORT, ANALYTICS_PORT_SSL)
        local.set_up_s3_link(rest_username, rest_password, baseurl,
                             external_dataset_type, external_dataset_region,
                             access_key_id, secret_access_key)

    def create_external_datasets(self):
        logger.info('Creating external datasets')
        with open(self.config_file, "r") as json_file:
            analytics_config = json.load(json_file)
        dataset_list = analytics_config["Analytics"]
        external_bucket = self.analytics_settings.external_bucket
        file_format = self.analytics_settings.external_file_format
        file_include = self.analytics_settings.external_file_include
        for dataset in dataset_list:
            statement = "CREATE EXTERNAL DATASET `{}` ON `{}` AT `external_link` " \
                        "USING '{}' WITH {{ 'format': '{}', 'include': '*.{}' }};"\
                .format(dataset["Dataset"], external_bucket, dataset["Collection"],
                        file_format, file_include)
            logger.info("statement: {}".format(statement))
            self.rest.exec_analytics_statement(self.analytics_node, statement)

    @with_stats
    def access(self, nodes: list = [], *args, **kwargs) -> List[QueryLatencyPair]:
        if len(nodes) == 0:
            analytics_nodes = self.analytics_nodes
        else:
            analytics_nodes = nodes
        logger.info("analytics_nodes = {}".format(analytics_nodes))
        results = bigfun(self.rest,
                         nodes=analytics_nodes,
                         concurrency=int(self.test_config.access_settings.workers),
                         num_requests=int(self.test_config.access_settings.ops),
                         query_set=self.QUERIES,
                         query_method=QueryMethod.CURL_CBAS)
        return [(query, latency) for query, latency in results]

    def run(self):
        random.seed(8095)
        self.set_up_s3_link()
        self.create_external_datasets()

        logger.info('Running access phase')
        results = self.access()

        self.report_kpi(results)


class GoldfishCopyFromS3Test(BigFunQueryNoIndexExternalTest):

    COLLECTORS = {'ns_server': False, 'active_tasks': False, 'analytics': True}

    def __init__(self, *args, **kwargs):
        PerfTest.__init__(self, *args, **kwargs)

        self.num_items = 0
        self.analytics_settings = self.test_config.analytics_settings
        self.config_file = self.analytics_settings.analytics_config_file
        self.analytics_link = self.analytics_settings.analytics_link

        self.is_capella_goldfish = self.cluster_spec.capella_infrastructure and \
            self.cluster_spec.goldfish_infrastructure

        if self.is_capella_goldfish:
            self.nebula_endpoint = self.cluster_spec.config.items('goldfish_nebula')[0][1]
            self.rest.rest_username, self.rest.rest_password = \
                self.cluster_spec.goldfish_nebula_credentials[0]
            cb_version = \
                self.cluster_spec.infrastructure_settings['goldfish_cb_versions'].split()[0]
            version_number, build_number = cb_version.split('-')
            self.cb_build_tuple = tuple(map(int, version_number.split('.'))) + (int(build_number),)
        else:
            self.cb_build_tuple = self.cluster.build_tuple

        self.analytics_node = self.analytics_nodes[0]

        self.QUERIES = self.analytics_settings.queries

    def exec_analytics_statement(self, statement: str) -> requests.Response:
        if self.is_capella_goldfish:
            return self.rest.exec_analytics_statement_goldfish_nebula(self.nebula_endpoint,
                                                                      statement)
        return self.rest.exec_analytics_statement(self.analytics_node, statement)

    def set_up_s3_link(self):
        external_dataset_type = self.analytics_settings.external_dataset_type
        external_dataset_region = self.analytics_settings.external_dataset_region
        access_key_id, secret_access_key =\
            local.get_aws_credential(self.analytics_settings.aws_credential_path)

        if self.is_capella_goldfish:
            baseurl = 'https://{}:{}'.format(self.nebula_endpoint, GOLDFISH_NEBULA_ANALYTICS_PORT)
        else:
            baseurl = self.rest._get_api_url(self.analytics_node, '',
                                             ANALYTICS_PORT, ANALYTICS_PORT_SSL)

        local.set_up_s3_link(self.rest.rest_username, self.rest.rest_password, baseurl,
                             external_dataset_type, external_dataset_region,
                             access_key_id, secret_access_key)

    def create_standalone_datasets(self):
        logger.info('Creating standalone datasets')
        with open(self.config_file, "r") as json_file:
            analytics_config = json.load(json_file)
        dataset_list = analytics_config["Analytics"]
        for dataset in dataset_list:
            statement = "CREATE DATASET `{}` PRIMARY KEY (`key`: string)".format(dataset["Dataset"])
            logger.info("statement: {}".format(statement))
            self.exec_analytics_statement(statement)

    @with_stats
    @timeit
    def ingest_data(self):
        logger.info('Ingesting data from S3 using COPY FROM')

        with open(self.config_file, "r") as json_file:
            analytics_config = json.load(json_file)

        dataset_list = analytics_config["Analytics"]
        external_bucket = self.analytics_settings.external_bucket
        file_format = self.analytics_settings.external_file_format
        file_include = self.analytics_settings.external_file_include
        path_keyword = 'USING' if self.cb_build_tuple < (8, 0, 0, 1452) else 'PATH'

        for dataset in dataset_list:
            statement = (
                "COPY INTO `{}` FROM `{}` AT `external_link` {} '{}' "
                "WITH {{ 'format': '{}', 'include': '*.{}' }};"
            ).format(
                dataset["Dataset"],
                external_bucket,
                path_keyword,
                dataset["Collection"],
                file_format,
                file_include
            )
            logger.info("statement: {}".format(statement))

            t0 = time.time()
            self.exec_analytics_statement(statement)
            logger.info('Statement execution time: {}'.format(time.time() - t0))

    @with_stats
    def access(self) -> List[QueryLatencyPair]:
        if self.is_capella_goldfish:
            nodes = [self.nebula_endpoint]
            query_method = QueryMethod.PYTHON_GOLDFISH_NEBULA
        else:
            nodes = self.analytics_nodes
            query_method = QueryMethod.CURL_CBAS

        logger.info("analytics_nodes = {}".format(nodes))
        results = bigfun(self.rest,
                         nodes=nodes,
                         concurrency=int(self.test_config.access_settings.workers),
                         num_requests=int(self.test_config.access_settings.ops),
                         query_set=self.QUERIES,
                         query_method=query_method)
        return [(query, latency) for query, latency in results]

    def run(self):
        random.seed(8095)

        self.set_up_s3_link()
        self.create_standalone_datasets()

        copy_from_time = self.ingest_data()
        logger.info('Total data ingestion time using COPY FROM: {}'.format(copy_from_time))

        results = self.access()
        self.report_kpi(results)


class BigFunQueryFailoverTest(BigFunQueryTest):

    def failover(self):
        logger.info("Starting node failover")
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.test_config.rebalance_settings.failed_nodes
        active_analytics_nodes = self.analytics_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            master = servers[0]

            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            for node in failed:
                self.rest.fail_over(master, node)
                active_analytics_nodes.remove(node)

        logger.info("sleep for 120 seconds")
        time.sleep(120)
        t_start = self.remote.detect_hard_failover_start(self.master_node)
        t_end = self.remote.detect_failover_end(self.master_node)
        logger.info("failover starts at {}".format(t_start))
        logger.info("failover ends at {}".format(t_end))
        return active_analytics_nodes

    def run(self):
        random.seed(8095)
        self.restore_local()
        self.wait_for_persistence()

        self.sync()

        self.disconnect_link()
        self.monitor.monitor_cbas_pending_ops(self.analytics_nodes)
        active_analytics_nodes = self.failover()

        logger.info('Running warmup phase')
        self.warmup(nodes=active_analytics_nodes)

        logger.info('Running access phase')
        results = self.access(nodes=active_analytics_nodes)

        self.report_kpi(results)


class BigFunQueryNoIndexWithCompressionTest(BigFunQueryWithCompressionTest):

    def create_index(self):
        pass

    def create_index_collections(self):
        pass


class BigFunRebalanceTest(BigFunTest, RebalanceTest):

    ALL_HOSTNAMES = True

    def rebalance_cbas(self):
        self.rebalance(services='cbas')

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.rebalance_time(rebalance_time=self.rebalance_time)
        )

    def run(self):
        super().run()

        self.sync()

        self.rebalance_cbas()

        if self.is_balanced():
            self.report_kpi()


class BigFunRebalanceCloudTest(BigFunRebalanceTest):

    ALL_HOSTNAMES = True

    def run(self):
        self.restore_remote_storage()

        self.sync()

        self.rebalance_cbas()

        if self.is_balanced():
            self.report_kpi()


class BigFunRebalanceCapellaTest(BigFunRebalanceTest, CapellaRebalanceKVTest):

    ALL_HOSTNAMES = True

    def run(self):
        self.restore_remote_storage()

        self.sync()

        self.rebalance_cbas()

        if self.is_balanced():
            self.report_kpi()


class BigFunConnectTest(BigFunTest):

    def _report_kpi(self, avg_connect_time: int, avg_disconnect_time: int):
        self.reporter.post(
            *self.metrics.analytics_avg_connect_time(avg_connect_time)
        )

        self.reporter.post(
            *self.metrics.analytics_avg_disconnect_time(avg_disconnect_time)
        )

    @timeit
    def connect_analytics_link(self):
        super().connect_link()

    @timeit
    def disconnect_analytics_link(self):
        super().disconnect_link()

    @with_stats
    def connect_cycle(self, ops: int):
        total_connect_time = 0
        total_disconnect_time = 0
        for op in range(ops):
            disconnect_time = self.disconnect_analytics_link()
            logger.info("disconnect time: {}".format(disconnect_time))
            connect_time = self.connect_analytics_link()
            logger.info("connect time: {}".format(connect_time))
            total_connect_time += connect_time
            total_disconnect_time += disconnect_time
        return total_connect_time/ops, total_disconnect_time/ops

    def run(self):
        super().run()

        if self.analytics_link != "Local":
            rest_username, rest_password = self.cluster_spec.rest_credentials
            local.create_remote_link(self.analytics_link, self.data_node, self.analytics_node,
                                     rest_username, rest_password)

        self.sync()

        avg_connect_time, avg_disconnect_time = \
            self.connect_cycle(int(self.test_config.access_settings.ops))

        self.report_kpi(avg_connect_time, avg_disconnect_time)


class TPCDSTest(PerfTest):

    TPCDS_DATASETS = [
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    ]

    TPCDS_INDEXES = [("c_customer_sk_idx",
                      "customer(c_customer_sk:STRING)",
                      "customer"),
                     ("d_date_sk_idx",
                      "date_dim(d_date_sk:STRING)",
                      "date_dim"),
                     ("d_date_idx",
                      "date_dim(d_date:STRING)",
                      "date_dim"),
                     ("d_month_seq_idx",
                      "date_dim(d_month_seq:BIGINT)",
                      "date_dim"),
                     ("d_year_idx",
                      "date_dim(d_year:BIGINT)",
                      "date_dim"),
                     ("i_item_sk_idx",
                      "item(i_item_sk:STRING)",
                      "item"),
                     ("s_state_idx",
                      "store(s_state:STRING)",
                      "store"),
                     ("s_store_sk_idx",
                      "store(s_store_sk:STRING)",
                      "store"),
                     ("sr_returned_date_sk_idx",
                      "store_returns(sr_returned_date_sk:STRING)",
                      "store_returns"),
                     ("ss_sold_date_sk_idx",
                      "store_sales(ss_sold_date_sk:STRING)",
                      "store_sales")]

    COLLECTORS = {'analytics': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0
        self.analytics_link = self.test_config.analytics_settings.analytics_link
        if self.analytics_link == "Local":
            self.data_node = self.master_node
            self.analytics_node = self.analytics_nodes[0]
        else:
            self.data_node, self.analytics_node = self.cluster_spec.masters

    def download_tpcds_couchbase_loader(self):
        if self.worker_manager.is_remote:
            self.remote.init_tpcds_couchbase_loader(
                repo=self.test_config.tpcds_loader_settings.repo,
                branch=self.test_config.tpcds_loader_settings.branch,
                worker_home=self.worker_manager.WORKER_HOME)
        else:
            local.init_tpcds_couchbase_loader(
                repo=self.test_config.tpcds_loader_settings.repo,
                branch=self.test_config.tpcds_loader_settings.branch)

    def set_max_active_writable_datasets(self):
        self.rest.set_analytics_max_active_writable_datasets(self.analytics_node, 24)
        self.rest.restart_analytics_cluster(self.analytics_node)
        self.rest.validate_analytics_setting(self.analytics_node,
                                             'storageMaxActiveWritableDatasets', 24)
        time.sleep(5)

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=tpcds_initial_data_load_task)

    def create_datasets(self, bucket: str):
        logger.info('Creating datasets')
        for dataset in self.TPCDS_DATASETS:
            statement = "CREATE DATASET `{}` ON `{}` WHERE table_name = '{}';" \
                .format(dataset, bucket, dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def create_indexes(self):
        logger.info('Creating indexes')
        for index in self.TPCDS_INDEXES:
            statement = "CREATE INDEX {} ON {};".format(index[0], index[1])
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def drop_indexes(self):
        logger.info('Dropping indexes')
        for index in self.TPCDS_INDEXES:
            statement = "DROP INDEX {}.{};".format(index[2], index[0])
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def disconnect_link(self):
        logger.info('DISCONNECT LINK {}'.format(self.analytics_link))
        statement = "DISCONNECT LINK {}".format(self.analytics_link)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def connect_buckets(self):
        logger.info('Connecting Link {}'.format(self.analytics_link))
        statement = "CONNECT link {}".format(self.analytics_link)
        res = self.rest.exec_analytics_statement(self.analytics_node, statement)
        logger.info("Result: {}".format(str(res)))
        time.sleep(5)

    def create_primary_indexes(self):
        logger.info('Creating primary indexes')
        for dataset in self.TPCDS_DATASETS:
            statement = "CREATE PRIMARY INDEX ON {};".format(dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def drop_primary_indexes(self):
        logger.info('Dropping primary indexes')
        for dataset in self.TPCDS_DATASETS:
            statement = "DROP INDEX {}.primary_idx_{};".format(dataset, dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def sync(self):
        self.disconnect_link()
        for bucket in self.test_config.buckets:
            self.create_datasets(bucket)
        self.connect_buckets()
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(self.data_node,
                                                               bucket,
                                                               self.analytics_node)

    def run(self):
        self.download_tpcds_couchbase_loader()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()


class TPCDSQueryTest(TPCDSTest):

    COUNT_QUERIES = 'perfrunner/workloads/tpcdsfun/count_queries.json'
    QUERIES = 'perfrunner/workloads/tpcdsfun/queries.json'

    @with_stats
    def access(self, *args, **kwargs) -> Tuple[List[QueryLatencyPair], List[QueryLatencyPair],
                                               List[QueryLatencyPair], List[QueryLatencyPair]]:

        logger.info('Running COUNT queries without primary key index')
        results = tpcds(self.rest,
                        nodes=self.analytics_nodes,
                        concurrency=self.test_config.access_settings.workers,
                        num_requests=int(self.test_config.access_settings.ops),
                        query_set=self.COUNT_QUERIES)
        count_without_index_results = [(query, latency) for query, latency in results]

        self.create_primary_indexes()

        logger.info('Running COUNT queries with primary key index')
        results = tpcds(self.rest,
                        nodes=self.analytics_nodes,
                        concurrency=self.test_config.access_settings.workers,
                        num_requests=int(self.test_config.access_settings.ops),
                        query_set=self.COUNT_QUERIES)
        count_with_index_results = [(query, latency) for query, latency in results]

        self.drop_primary_indexes()

        logger.info('Running queries without index')
        results = tpcds(
            self.rest,
            nodes=self.analytics_nodes,
            concurrency=self.test_config.access_settings.workers,
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.QUERIES)
        without_index_results = [(query, latency) for query, latency in results]

        self.create_indexes()

        logger.info('Running queries with index')
        results = tpcds(
            self.rest,
            nodes=self.analytics_nodes,
            concurrency=self.test_config.access_settings.workers,
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.QUERIES)
        with_index_results = [(query, latency) for query, latency in results]

        return \
            count_without_index_results, \
            count_with_index_results, \
            without_index_results, \
            with_index_results

    def _report_kpi(self, results: List[QueryLatencyPair], with_index: bool):
        for query, latency in results:
            self.reporter.post(
                *self.metrics.analytics_volume_latency(query, latency, with_index)
            )

    def run(self):
        super().run()

        self.sync()

        count_results_no_index, count_results_with_index, results_no_index, \
            results_with_index = self.access()

        self.report_kpi(count_results_no_index, with_index=False)
        self.report_kpi(count_results_with_index, with_index=True)
        self.report_kpi(results_no_index, with_index=False)
        self.report_kpi(results_with_index, with_index=True)


class CH2Test(PerfTest):

    CH2_DATASETS = [
        "customer",
        "district",
        "history",
        "item",
        "neworder",
        "orders",
        "stock",
        "warehouse",
        "supplier",
        "nation",
        "region",
    ]

    CH2_INDEXES = [("cu_w_id_d_id_last",
                    "customer(c_w_id, c_d_id, c_last)",
                    "customer"),
                   ("di_id_w_id",
                    "district(d_id, d_w_id)",
                    "district"),
                   ("no_o_id_d_id_w_id",
                    "neworder(no_o_id, no_d_id, no_w_id)",
                    "neworder"),
                   ("or_id_d_id_w_id_c_id",
                    "orders(o_id, o_d_id, o_w_id, o_c_id)",
                    "orders"),
                   ("or_w_id_d_id_c_id",
                    "orders(o_w_id, o_d_id, o_c_id)",
                    "orders"),
                   ("wh_id",
                    "warehouse(w_id)",
                    "warehouse")]

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'secondary_stats': True,
        'ns_server_system': True,
        'analytics': True,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0
        self.analytics_link = self.test_config.analytics_settings.analytics_link
        self.data_node = self.master_node
        self.analytics_node = self.analytics_nodes[0]
        self.analytics_statements = self.test_config.ch2_settings.analytics_statements
        self.storage_format = self.test_config.analytics_settings.storage_format

    def _report_kpi(self):
        measure_time = self.test_config.ch2_settings.duration - \
                       self.test_config.ch2_settings.warmup_duration
        total_time, rate, test_duration = \
            self.metrics.ch2_metric(duration=measure_time,
                                    logfile=self.test_config.ch2_settings.workload)
        tpm = round(rate*60/test_duration, 2)
        response_time = round(total_time/1000000/rate, 2)

        self.reporter.post(
            *self.metrics.ch2_tmp(tpm, self.test_config.ch2_settings.tclients)
        )

        self.reporter.post(
            *self.metrics.ch2_response_time(response_time, self.test_config.ch2_settings.tclients)
        )

        if self.test_config.ch2_settings.workload == 'ch2_mixed':
            self.reporter.post(
                *self.metrics.ch2_analytics_query_time(test_duration,
                                                       self.test_config.ch2_settings.tclients)
            )

    def create_datasets(self):
        logger.info('Creating datasets')
        for dataset in self.CH2_DATASETS:
            if self.storage_format:
                statement = 'CREATE DATASET `{}` WITH {{"storage-format": {{"format": "{}"}}}} ' \
                            'ON bench.ch2.{} AT `{}`;' \
                    .format(dataset, self.storage_format, dataset, self.analytics_link)
            else:
                statement = "CREATE DATASET `{}` ON bench.ch2.{} AT `{}`;" \
                    .format(dataset, dataset, self.analytics_link)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def create_analytics_indexes(self):
        if self.analytics_statements:
            logger.info('Creating analytics indexes')
            for statement in self.analytics_statements:
                logger.info('Running: {}'.format(statement))
                self.rest.exec_analytics_statement(self.analytics_node,
                                                   statement)

    def create_indexes(self):
        logger.info('Creating indexes')
        for index in self.CH2_INDEXES:
            statement = "CREATE INDEX {} ON bench.ch2.{} using gsi;".format(index[0], index[1])
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def drop_indexes(self):
        logger.info('Dropping indexes')
        for index in self.CH2_INDEXES:
            statement = "DROP INDEX {} ON bench.ch2.{} using gsi;".format(index[0], index[2])
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def disconnect_link(self):
        logger.info('DISCONNECT LINK {}'.format(self.analytics_link))
        statement = "DISCONNECT LINK {}".format(self.analytics_link)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def connect_link(self):
        logger.info('Connecting Link {}'.format(self.analytics_link))
        statement = "CONNECT link {}".format(self.analytics_link)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def create_primary_indexes(self):
        logger.info('Creating primary indexes')
        for dataset in self.CH2_DATASETS:
            statement = "CREATE PRIMARY INDEX ON {};".format(dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def drop_primary_indexes(self):
        logger.info('Dropping primary indexes')
        for dataset in self.CH2_DATASETS:
            statement = "DROP INDEX {}.primary_idx_{};".format(dataset, dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def sync(self):
        self.disconnect_link()
        self.create_datasets()
        self.create_analytics_indexes()
        self.connect_link()
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(self.data_node,
                                                               bucket,
                                                               self.analytics_node)

    def _create_ch2_conn_settings(self) -> CH2ConnectionSettings:
        if self.test_config.cluster.enable_n2n_encryption:
            query_port = QUERY_PORT_SSL
            cbas_port = ANALYTICS_PORT_SSL
        else:
            query_port = QUERY_PORT
            cbas_port = ANALYTICS_PORT

        query_urls = ['{}:{}'.format(node, query_port) for node in self.query_nodes]
        userid, password = self.cluster_spec.rest_credentials

        return CH2ConnectionSettings(
            userid=userid,
            password=password,
            analytics_url='{}:{}'.format(self.analytics_nodes[0], cbas_port),
            query_url=query_urls[0],
            multi_query_url=",".join(query_urls),
            data_url=self.data_nodes[0],
            multi_data_url=",".join(self.data_nodes)
        )

    @with_stats
    def run_ch2_local(self, log_file: str = ''):
        logger.info("running {}".format(self.test_config.ch2_settings.workload))
        local.ch2_run_task(self._create_ch2_conn_settings(), self.test_config.ch2_settings,
                           log_file=log_file or self.test_config.ch2_settings.workload)

    def load_ch2_local(self):
        logger.info("load CH2 docs")
        local.ch2_load_task(self._create_ch2_conn_settings(), self.test_config.ch2_settings)

    @with_stats
    def run_ch2_remote(self, log_file: str = ''):
        logger.info("running {}".format(self.test_config.ch2_settings.workload))
        self.remote.ch2_run_task(self._create_ch2_conn_settings(), self.test_config.ch2_settings,
                                 self.worker_manager.WORKER_HOME,
                                 log_file=log_file or self.test_config.ch2_settings.workload)

    def load_ch2_remote(self):
        logger.info("load CH2 docs")
        self.remote.ch2_load_task(self._create_ch2_conn_settings(), self.test_config.ch2_settings,
                                  worker_home=self.worker_manager.WORKER_HOME)

    def restart(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def run(self):
        local.clone_git_repo(repo=self.test_config.ch2_settings.repo,
                             branch=self.test_config.ch2_settings.branch)

        if self.test_config.ch2_settings.use_backup:
            self.restore_local()
        else:
            self.load_ch2_local()

        self.wait_for_persistence()
        self.restart()
        self.sync()
        self.create_indexes()

        self.run_ch2_local()
        if self.test_config.ch2_settings.workload != 'ch2_analytics':
            self.report_kpi()


class CH2CloudTest(CH2Test):

    def restore(self):
        credential = local.read_aws_credential(self.test_config.backup_settings.aws_credential_path)
        self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()

        self.remote.restore(cluster_spec=self.cluster_spec,
                            master_node=self.master_node,
                            threads=self.test_config.restore_settings.threads,
                            worker_home=self.worker_manager.WORKER_HOME,
                            archive=self.test_config.restore_settings.backup_storage,
                            repo=self.test_config.restore_settings.backup_repo,
                            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
                            obj_region=self.test_config.backup_settings.obj_region,
                            use_tls=self.test_config.restore_settings.use_tls,
                            map_data=self.test_config.restore_settings.map_data,
                            encrypted=self.test_config.restore_settings.encrypted,
                            passphrase=self.test_config.restore_settings.passphrase)

    def run(self):
        self.remote.init_ch2(repo=self.test_config.ch2_settings.repo,
                             branch=self.test_config.ch2_settings.branch,
                             worker_home=self.worker_manager.WORKER_HOME)

        if self.test_config.ch2_settings.use_backup:
            self.remote.extract_cb_any(filename='couchbase',
                                       worker_home=self.worker_manager.WORKER_HOME)
            self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)
            self.restore()
        else:
            self.load_ch2_remote()

        self.wait_for_persistence()
        self.restart()
        self.sync()
        if (self.test_config.ch2_settings.create_gsi_index and
                not self.cluster_spec.goldfish_infrastructure):
            self.create_indexes()

        self.run_ch2_remote()
        self.remote.get_ch2_logfile(worker_home=self.worker_manager.WORKER_HOME,
                                    logfile=self.test_config.ch2_settings.workload)
        if self.test_config.ch2_settings.workload != 'ch2_analytics':
            self.report_kpi()


class CH2CloudRemoteLinkTest(CH2CloudTest):

    def __init__(self, *args, **kwargs):
        PerfTest.__init__(self, *args, **kwargs)

        self.num_items = 0
        self.analytics_link = self.test_config.analytics_settings.analytics_link
        self.data_node, self.analytics_node = self.cluster_spec.masters
        self.analytics_statements = self.test_config.ch2_settings.analytics_statements
        self.storage_format = self.test_config.analytics_settings.storage_format
        self.target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)

    @property
    def data_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.data_node, 'kv')

    @property
    def query_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.data_node, 'n1ql')

    @property
    def analytics_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.analytics_node, 'cbas')

    def restart(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for bucket in self.test_config.buckets:
            self.monitor.monitor_warmup(self.memcached, self.data_node, bucket)
        self.monitor.monitor_analytics_node_active(self.analytics_node)

    def restore(self):
        credential = local.read_aws_credential(self.test_config.backup_settings.aws_credential_path)
        self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()

        self.remote.restore(cluster_spec=self.cluster_spec,
                            master_node=self.data_node,
                            threads=self.test_config.restore_settings.threads,
                            worker_home=self.worker_manager.WORKER_HOME,
                            archive=self.test_config.restore_settings.backup_storage,
                            repo=self.test_config.restore_settings.backup_repo,
                            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
                            obj_region=self.test_config.backup_settings.obj_region,
                            use_tls=self.test_config.restore_settings.use_tls,
                            map_data=self.test_config.restore_settings.map_data,
                            encrypted=self.test_config.restore_settings.encrypted,
                            passphrase=self.test_config.restore_settings.passphrase)

    @with_stats
    @timeit
    def sync(self):
        super().sync()

    def report_sync_kpi(self, sync_time: int):
        logger.info('Sync time: {}s'.format(sync_time))
        self.reporter.post(
            *self.metrics.avg_ingestion_rate(self.num_items, sync_time)
        )

    def run(self):
        self.remote.init_ch2(repo=self.test_config.ch2_settings.repo,
                             branch=self.test_config.ch2_settings.branch,
                             worker_home=self.worker_manager.WORKER_HOME)

        if self.test_config.ch2_settings.use_backup:
            self.remote.extract_cb_any(filename='couchbase',
                                       worker_home=self.worker_manager.WORKER_HOME)
            self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)
            # Restore to KV cluster
            self.restore()
        else:
            # Load into KV cluster
            self.load_ch2_remote()

        # Only wait for and restart the KV cluster
        self.wait_for_persistence()
        self.restart()

        rest_username, rest_password = self.cluster_spec.rest_credentials
        local.create_remote_link(self.analytics_link, self.data_node, self.analytics_node,
                                 rest_username, rest_password)

        sync_time = self.sync()
        self.report_sync_kpi(sync_time)

        if (self.test_config.ch2_settings.create_gsi_index and
                not self.cluster_spec.goldfish_infrastructure):
            self.create_indexes()

        self.run_ch2_remote()
        self.remote.get_ch2_logfile(worker_home=self.worker_manager.WORKER_HOME,
                                    logfile=self.test_config.ch2_settings.workload)
        if self.test_config.ch2_settings.workload != 'ch2_analytics':
            self.report_kpi()


class CH2GoldfishPauseResumeTest(CH2CloudRemoteLinkTest):

    def __init__(self, *args, **kwargs):
        PerfTest.__init__(self, *args, **kwargs)
        self.cluster_spec.set_inactive_clusters_by_idx([2])

        self.num_items = 0
        self.analytics_link = self.test_config.analytics_settings.analytics_link
        self.data_node, self.analytics_node = self.cluster_spec.masters
        self.analytics_statements = self.test_config.ch2_settings.analytics_statements
        self.storage_format = self.test_config.analytics_settings.storage_format
        self.target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)

    @with_stats
    def pause(self) -> int:
        logger.info('Pausing analytics cluster...')
        retries = 5
        while retries >= 0:
            self.rest.pause_analytics_cluster(self.analytics_node)

            t0 = time.time()
            status = self.monitor.monitor_cbas_pause_status(self.analytics_node)
            pause_time = time.time() - t0

            if status == 'complete':
                logger.info('Pause operation completed. Time taken: {}s'.format(pause_time))
                return pause_time
            else:
                if status == 'failed':
                    logger.warn('Pause failed, retrying... ({} retries left)'.format(retries))
                elif status == 'notRunning':
                    logger.warn('Pause operation did not start, retrying... ({} retries left)'
                                .format(retries))
                retries -= 1

        logger.interrupt('Failed to pause analytics cluster.')

    def run(self):
        super().run()

        # Disconnect links before pause
        self.disconnect_link()

        self.pause()

        # Set up second analytics cluster
        self.cluster_spec.set_active_clusters_by_idx([2])
        analytics_node = next(self.cluster_spec.masters)

        self.cluster.set_goldfish_s3_bucket()
        self.cluster.add_aws_credential()

        self.cluster.tune_logging()
        self.cluster.set_data_path()
        self.cluster.set_analytics_path()

        # Provision cluster and start resume timer
        self.cluster.set_mem_quotas()
        self.cluster.set_services()
        self.cluster.rename()
        self.cluster.set_auth()
        t0 = time.time()

        # Wait for analytics service to be ready (then stop timer)
        self.monitor.monitor_analytics_node_active(analytics_node)
        resume_time = (t1 := time.time()) - t0
        logger.info('Time taken to resume single analytics node: {}s'.format(resume_time))

        # Rebalance in remaining nodes
        self.cluster.add_nodes()
        self.cluster.rebalance()
        rebalance_time = time.time() - t1
        logger.info('Time taken to rebalance in remaining nodes: {}s'.format(rebalance_time))

        # Finish set up
        self.cluster.enable_auto_failover()
        self.cluster.create_buckets()
        self.cluster.set_analytics_settings()
        self.cluster.wait_until_healthy()
        logger.info('Total time taken for new cluster to be ready: {}s'.format(time.time() - t0))

        # Switch over to second analytics cluster
        self.cluster_spec.set_inactive_clusters_by_idx([1])
        self.data_node, self.analytics_node = self.cluster_spec.masters

        log_file = '{}_post_resume'.format(self.test_config.ch2_settings.workload)
        self.run_ch2_remote(log_file=log_file)
        self.remote.get_ch2_logfile(worker_home=self.worker_manager.WORKER_HOME, logfile=log_file)
        if self.test_config.ch2_settings.workload != 'ch2_analytics':
            self.report_kpi()


class CH2GoldfishKafkaLinksIngestionTest(CH2CloudTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_links_settings = self.test_config.goldfish_kafka_links_settings
        self.docs_per_collection = {}
        self.kafka_link_connected = False

        source = self.kafka_links_settings.link_source
        self.source_details = {"source": source}

        if source == "MONGODB":
            self.docs_per_collection = {
                collection: self.count_collection_docs_mongodb(collection)
                for collection in self.CH2_DATASETS
            }
            self.source_details.update({
                "connectionFields": {
                    "connectionUri": self.kafka_links_settings.mongodb_uri
                }
            })
        elif source == 'DYNAMODB':
            logger.interrupt('DynamoDB source is unsupported by perfrunner for Kafka Links')
        elif source == 'MYSQLDB':
            logger.interrupt('MySQL source is unsupported by perfrunner for Kafka Links')
        else:
            logger.interrupt('Unknown Kafka Link source type: {}'.format(source))

        logger.info('Kafka Link source details: {}'.format(pretty_dict(self.source_details)))
        logger.info('Docs per collection in source database: {}'
                    .format(pretty_dict(self.docs_per_collection)))

        self.num_items = sum(self.docs_per_collection.values())

    def count_collection_docs_mongodb(self, collection: str) -> int:
        from pymongo import MongoClient
        client = MongoClient(self.kafka_links_settings.mongodb_uri)
        db = client[self.kafka_links_settings.remote_database_name]
        coll = db[collection]
        return coll.estimated_document_count()

    def create_kafka_link(self):
        logger.info('Creating Kafka Link')

        statement = 'CREATE LINK `{}` TYPE KAFKA WITH {{"sourceDetails": {}}}'\
            .format(self.analytics_link, json.dumps(self.source_details))

        logger.info('Running: {}'.format(statement))
        res = self.rest.exec_analytics_statement(self.analytics_node, statement)
        logger.info("Result: {}".format(str(res)))

    def create_datasets(self):
        logger.info('Creating standalone datasets')

        for dataset in self.CH2_DATASETS:
            statement = "CREATE DATASET `{0}` PRIMARY KEY (`{1}`: string) ON {2}.{0} AT `{3}`;" \
                .format(dataset, self.kafka_links_settings.primary_key_field,
                        self.kafka_links_settings.remote_database_name, self.analytics_link)
            logger.info('Running: {}'.format(statement))
            self.rest.exec_analytics_statement(self.analytics_node, statement)

    @with_stats
    def sync(self) -> int:
        """Set up data ingestion and return time taken to ingest all data (excluding setup time)."""
        self.create_datasets()
        self.create_analytics_indexes()
        self.connect_link()

        t0 = time.time()
        self.monitor.monitor_cbas_kafka_link_connect_status(self.analytics_node,
                                                            self.analytics_link)
        link_connect_time = time.time() - t0
        self.kafka_link_connected = True
        logger.info('Link connection time: {}s'.format(link_connect_time))

        self.monitor.monitor_cbas_kafka_link_data_ingestion_status(self.analytics_node,
                                                                   self.docs_per_collection)
        data_ingest_time = time.time() - link_connect_time
        logger.info('Data ingestion time: {}s'.format(data_ingest_time))

        logger.info('Total time from link connection -> all data ingested: {}s'
                    .format(link_connect_time + data_ingest_time))

        return data_ingest_time

    def _report_kpi(self, sync_time: int):
        self.reporter.post(
            *self.metrics.avg_ingestion_rate(self.num_items, sync_time)
        )

    def run(self):
        try:
            self.create_kafka_link()
            data_ingest_time = self.sync()
            self.report_kpi(data_ingest_time)
        finally:
            if self.kafka_link_connected:
                logger.info('Getting Connector ARNs to be able to look up logs')
                if not self.cluster.get_msk_connect_connector_arns():
                    logger.warn('Failed to get Kafka Connect connector ARNs')
                self.disconnect_link()


class CH3Test(PerfTest):

    CH3_DATASETS = [
        "customer",
        "district",
        "history",
        "item",
        "neworder",
        "orders",
        "stock",
        "warehouse",
        "supplier",
        "nation",
        "region",
    ]

    CH3_INDEXES = [("cu_w_id_d_id_last",
                    "customer(c_w_id, c_d_id, c_last)",
                    "customer"),
                   ("di_id_w_id",
                    "district(d_id, d_w_id)",
                    "district"),
                   ("no_o_id_d_id_w_id",
                    "neworder(no_o_id, no_d_id, no_w_id)",
                    "neworder"),
                   ("or_id_d_id_w_id_c_id",
                    "orders(o_id, o_d_id, o_w_id, o_c_id)",
                    "orders"),
                   ("or_w_id_d_id_c_id",
                    "orders(o_w_id, o_d_id, o_c_id)",
                    "orders"),
                   ("wh_id",
                    "warehouse(w_id)",
                    "warehouse")]

    CH3_FTS_INDEXES = [
        "customerFTSI",
        "itemFTSI",
        "ordersFTSI",
        "mutiCollectionFTSI",
        "nonAnalyticFTSI",
        "ngramFTSI",
    ]

    COLLECTORS = {
        'iostat': False,
        'memory': False,
        'n1ql_latency': False,
        'n1ql_stats': True,
        'secondary_stats': True,
        'ns_server_system': True,
        'analytics': True,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0
        self.analytics_link = self.test_config.analytics_settings.analytics_link
        self.data_node = self.master_node
        self.analytics_node = self.analytics_nodes[0]
        self.fts_node = self.fts_nodes[0]
        self.analytics_statements = self.test_config.ch3_settings.analytics_statements

    def _report_kpi(self):
        measure_time = self.test_config.ch3_settings.duration - \
                       self.test_config.ch3_settings.warmup_duration
        total_time, rate, test_duration, fts_duration, fts_client, fts_qph = \
            self.metrics.ch3_metric(duration=measure_time,
                                    logfile=self.test_config.ch3_settings.workload)

        tpm = round(rate*60/test_duration, 2)
        response_time = round(total_time/1000/rate, 2)
        fts_query_time = round(fts_duration/1000, 2)
        fts_client_time = round(fts_client/1000, 2)

        self.reporter.post(
            *self.metrics.ch2_tmp(tpm, self.test_config.ch3_settings.tclients)
        )

        self.reporter.post(
            *self.metrics.ch2_response_time(response_time, self.test_config.ch3_settings.tclients)
        )

        if self.test_config.ch3_settings.workload == 'ch3_mixed':
            self.reporter.post(
                *self.metrics.ch2_analytics_query_time(test_duration,
                                                       self.test_config.ch3_settings.tclients)
            )

            self.reporter.post(
                *self.metrics.ch3_fts_query_time(fts_query_time,
                                                 self.test_config.ch3_settings.tclients)
            )

            self.reporter.post(
                *self.metrics.ch3_fts_client_time(fts_client_time,
                                                  self.test_config.ch3_settings.tclients)
            )

            self.reporter.post(
                *self.metrics.ch3_fts_qph(fts_qph, self.test_config.ch3_settings.tclients)
            )

    def create_datasets(self):
        logger.info('Creating datasets')
        for dataset in self.CH3_DATASETS:
            statement = "CREATE DATASET `{}` ON bench.ch3.{};" \
                .format(dataset, dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def create_analytics_indexes(self):
        if self.analytics_statements:
            logger.info('Creating analytics indexes')
            for statement in self.analytics_statements:
                logger.info('Running: {}'.format(statement))
                self.rest.exec_analytics_statement(self.analytics_node,
                                                   statement)

    def create_indexes(self):
        logger.info('Creating indexes')
        for index in self.CH3_INDEXES:
            statement = "CREATE INDEX {} ON bench.ch3.{} using gsi;".format(index[0], index[1])
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def drop_indexes(self):
        logger.info('Dropping indexes')
        for index in self.CH3_INDEXES:
            statement = "DROP INDEX {} ON bench.ch3.{} using gsi;".format(index[0], index[2])
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def disconnect_link(self):
        logger.info('DISCONNECT LINK {}'.format(self.analytics_link))
        statement = "DISCONNECT LINK {}".format(self.analytics_link)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def connect_link(self):
        logger.info('Connecting Link {}'.format(self.analytics_link))
        statement = "CONNECT link {}".format(self.analytics_link)
        self.rest.exec_analytics_statement(self.analytics_node,
                                           statement)

    def create_primary_indexes(self):
        logger.info('Creating primary indexes')
        for dataset in self.CH3_DATASETS:
            statement = "CREATE PRIMARY INDEX ON {};".format(dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def drop_primary_indexes(self):
        logger.info('Dropping primary indexes')
        for dataset in self.CH3_DATASETS:
            statement = "DROP INDEX {}.primary_idx_{};".format(dataset, dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

    def sync(self):
        self.disconnect_link()
        self.create_datasets()
        self.create_analytics_indexes()
        self.connect_link()
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(self.data_node,
                                                               bucket,
                                                               self.analytics_node)

    @with_stats
    def run_ch3(self):
        if self.test_config.cluster.enable_n2n_encryption:
            query_port = QUERY_PORT_SSL
            fts_port = FTS_PORT_SSL
            cbas_port = ANALYTICS_PORT_SSL
        else:
            query_port = QUERY_PORT
            fts_port = FTS_PORT
            cbas_port = ANALYTICS_PORT

        query_urls = ['{}:{}'.format(node, query_port) for node in self.query_nodes]
        userid, password = self.cluster_spec.rest_credentials
        conn_settings = CH2ConnectionSettings(
            userid=userid,
            password=password,
            analytics_url='{}:{}'.format(self.analytics_nodes[0], cbas_port),
            query_url=query_urls[0],
            multi_query_url=",".join(query_urls),
            fts_url='{}:{}'.format(self.fts_nodes[0], fts_port)
        )

        logger.info("running {}".format(self.test_config.ch3_settings.workload))
        local.ch3_run_task(conn_settings, self.test_config.ch3_settings,
                           log_file=self.test_config.ch3_settings.workload)

    def create_fts_indexes(self):
        local.ch3_create_fts_index(
            cluster_spec=self.cluster_spec,
            fts_node=self.fts_node
        )

    def wait_for_fts_index_persistence(self):
        for index_name in self.CH3_FTS_INDEXES:
            self.monitor.monitor_fts_index_persistence(
                hosts=self.fts_nodes,
                index=index_name,
                bkt=self.test_config.buckets[0]
            )

    def load_ch3(self):
        if self.test_config.cluster.enable_n2n_encryption:
            query_port = QUERY_PORT_SSL
        else:
            query_port = QUERY_PORT

        query_urls = ['{}:{}'.format(node, query_port) for node in self.query_nodes]
        userid, password = self.cluster_spec.rest_credentials
        conn_settings = CH2ConnectionSettings(
            userid=userid,
            password=password,
            data_url=self.data_nodes[0],
            multi_data_url=",".join(self.data_nodes),
            query_url=query_urls[0],
            multi_query_url=",".join(query_urls)
        )

        logger.info("running {}".format(self.test_config.ch3_settings.workload))
        local.ch3_load_task(conn_settings, self.test_config.ch3_settings)

    def restart(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def run(self):
        local.clone_git_repo(repo=self.test_config.ch3_settings.repo,
                             branch=self.test_config.ch3_settings.branch)

        if self.test_config.ch3_settings.use_backup:
            self.restore_local()
        else:
            self.load_ch3()

        self.wait_for_persistence()
        self.restart()
        self.sync()
        self.create_indexes()
        self.wait_for_indexing()
        self.create_fts_indexes()
        self.wait_for_fts_index_persistence()

        self.run_ch3()
        if self.test_config.ch3_settings.workload != 'ch3_analytics':
            self.report_kpi()

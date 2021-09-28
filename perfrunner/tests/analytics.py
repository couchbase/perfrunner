import json
import random
import time
from typing import List, Tuple

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.worker import tpcds_initial_data_load_task
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import RebalanceTest
from perfrunner.workloads.bigfun.driver import bigfun
from perfrunner.workloads.bigfun.query_gen import Query
from perfrunner.workloads.tpcdsfun.driver import tpcds


class BigFunTest(PerfTest):

    COLLECTORS = {'analytics': True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0
        self.config_file = self.test_config.analytics_settings.analytics_config_file
        self.analytics_link = self.test_config.analytics_settings.analytics_link
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
        with open(self.config_file, "r") as jsonFile:
            analytics_config = json.load(jsonFile)
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
        with open(self.config_file, "r") as jsonFile:
            analytics_config = json.load(jsonFile)
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
        log_level = self.test_config.analytics_settings.log_level
        self.rest.set_analytics_logging_level(self.analytics_node, log_level)
        self.rest.restart_analytics_cluster(self.analytics_node)
        if not self.rest.validate_analytics_logging_level(self.analytics_node, log_level):
            logger.error('Failed to set logging level {}'.format(log_level))

    def set_buffer_cache_page_size(self):
        page_size = self.test_config.analytics_settings.storage_buffer_cache_pagesize
        self.rest.set_analytics_page_size(self.analytics_node, page_size)
        self.rest.restart_analytics_cluster(self.analytics_node)

    def set_storage_compression_block(self):
        storage_compression_block = self.test_config.analytics_settings.storage_compression_block
        self.rest.set_analytics_storage_compression_block(self.analytics_node,
                                                          storage_compression_block)
        self.rest.restart_analytics_cluster(self.analytics_node)
        self.rest.validate_analytics_setting(self.analytics_node, 'storageCompressionBlock',
                                             storage_compression_block)

    def get_dataset_items(self, dataset: str):
        logger.info('Get number of items in dataset {}'.format(dataset))
        statement = "SELECT COUNT(*) from `{}`;".format(dataset)
        result = self.rest.exec_analytics_query(self.analytics_node, statement)
        logger.info("Number of items in dataset {}: {}".
                    format(dataset, result['results'][0]['$1']))
        return result['results'][0]['$1']

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
            local.create_remote_link(self.analytics_link, self.data_node, self.analytics_node)

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

        drop_dataset = self.test_config.analytics_settings.drop_dataset
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
        self.QUERIES = self.test_config.analytics_settings.queries

    def warmup(self) -> List[Tuple[Query, int]]:
        results = bigfun(self.rest,
                         nodes=self.analytics_nodes,
                         concurrency=self.test_config.access_settings.analytics_warmup_workers,
                         num_requests=int(self.test_config.access_settings.analytics_warmup_ops),
                         query_set=self.QUERIES)

        return [(query, latency) for query, latency in results]

    @with_stats
    def access(self, *args, **kwargs) -> List[Tuple[Query, int]]:
        results = bigfun(self.rest,
                         nodes=self.analytics_nodes,
                         concurrency=int(self.test_config.access_settings.workers),
                         num_requests=int(self.test_config.access_settings.ops),
                         query_set=self.QUERIES)
        return [(query, latency) for query, latency in results]

    def _report_kpi(self, results: List[Tuple[Query, int]]):
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


class BigFunQueryWithCompressionTest(BigFunQueryTest):

    def run(self):
        self.set_storage_compression_block()
        super().run()


class BigFunQueryNoIndexTest(BigFunQueryTest):

    def create_index(self):
        pass

    def create_index_collections(self):
        pass


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
            local.create_remote_link(self.analytics_link, self.data_node, self.analytics_node)

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
    def access(self, *args, **kwargs) -> (List[Tuple[Query, int]], List[Tuple[Query, int]]):

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

    def _report_kpi(self, results: List[Tuple[Query, int]], with_index: bool):
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

    def create_datasets(self):
        logger.info('Creating datasets')
        for dataset in self.CH2_DATASETS:
            statement = "CREATE DATASET `{}` ON bench.ch2.{};" \
                .format(dataset, dataset)
            logger.info('Running: {}'.format(statement))
            res = self.rest.exec_analytics_statement(self.analytics_node, statement)
            logger.info("Result: {}".format(str(res)))
            time.sleep(5)

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
        self.connect_link()
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(self.data_node,
                                                               bucket,
                                                               self.analytics_node)

    @with_stats
    def run_ch2(self):
        query_url = self.query_nodes[0] + ":8093"
        analytics_url = self.analytics_nodes[0] + ":8095"
        query_nodes_port = []
        for node in self.query_nodes:
            query_nodes_port.append(node + ":8093")
        multi_query_url = ",".join(query_nodes_port)

        logger.info("running {}".format(self.test_config.ch2_settings.workload))
        local.ch2_run_task(
            cluster_spec=self.cluster_spec,
            warehouses=self.test_config.ch2_settings.warehouses,
            aclients=self.test_config.ch2_settings.aclients,
            tclients=self.test_config.ch2_settings.tclients,
            duration=self.test_config.ch2_settings.duration,
            iterations=self.test_config.ch2_settings.iterations,
            warmup_iterations=self.test_config.ch2_settings.warmup_iterations,
            warmup_duration=self.test_config.ch2_settings.warmup_duration,
            query_url=query_url,
            multi_query_url=multi_query_url,
            analytics_url=analytics_url,
            log_file=self.test_config.ch2_settings.workload
        )

    def restart(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def run(self):
        self.restore_local()
        self.wait_for_persistence()
        self.restart()
        self.sync()
        self.create_indexes()

        local.clone_git_repo(repo=self.test_config.ch2_settings.repo,
                             branch=self.test_config.ch2_settings.branch)

        self.run_ch2()
        self.report_kpi()

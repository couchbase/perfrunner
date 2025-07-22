import copy
import itertools
import json
import os
import random
import re
import time
from dataclasses import dataclass
from typing import Callable, Literal, Optional, Union
from urllib.parse import urlparse

import numpy as np
import requests
from celery import group
from requests_toolbelt.adapters import socket_options

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import (
    get_cloud_storage_bucket_stats,
    pretty_dict,
)
from perfrunner.helpers.rest import (
    ANALYTICS_PORT,
    ANALYTICS_PORT_SSL,
    FTS_PORT,
    FTS_PORT_SSL,
    QUERY_PORT,
    QUERY_PORT_SSL,
)
from perfrunner.helpers.worker import ch2_load, tpcds_initial_data_load_task
from perfrunner.settings import (
    CH2,
    AnalyticsCBOSampleSize,
    AnalyticsExternalFileFormat,
    AnalyticsExternalTableFormat,
    CH2ConnectionSettings,
    CH2Schema,
    ColumnarSettings,
)
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import (
    CapellaRebalanceKVTest,
    DynamicServiceRebalanceTest,
    RebalanceTest,
)
from perfrunner.tests.xdcr import SrcTargetIterator
from perfrunner.workloads.bigfun.driver import QueryMethod, bigfun
from perfrunner.workloads.bigfun.query_gen import Query
from perfrunner.workloads.tpcdsfun.driver import tpcds

QueryLatencyPair = tuple[Query, int]


def sqlpp_escape(*identifiers: str) -> Union[str, tuple[str, ...]]:
    """Return identifiers escaped with backticks for use in SQL++ queries.

    Assumes that "." characters are bucket/scope/collection separators.
    """
    result = tuple(
        ".".join(f"`{i.strip('`')}`" for i in identifier.split(".")) for identifier in identifiers
    )
    return result if len(result) > 1 else result[0]


@dataclass(frozen=True)
class DatasetDef:
    name: str
    source: Optional[str] = None
    where: Optional[str] = None

    def create_at_link_statement(self, link_name: str, storage_format: Optional[str] = None) -> str:
        name, source, link_name = sqlpp_escape(self.name, self.source or self.name, link_name)

        with_clause = ""
        if storage_format:
            with_clause = f' WITH {{"storage-format": {{"format": "{storage_format}"}}}}'

        return f"CREATE DATASET {name}{with_clause} ON {source} AT {link_name} {self.where or ''}"

    def create_standalone_statement(
        self, pk_field: str = "key", pk_type: str = "string", storage_format: Optional[str] = None
    ) -> str:
        name, pk_field, pk_type = sqlpp_escape(self.name, pk_field, pk_type)

        with_clause = ""
        if storage_format:
            with_clause = f' WITH {{"storage-format": {{"format": "{storage_format}"}}}}'

        return f"CREATE DATASET {name} PRIMARY KEY ({pk_field}: {pk_type}){with_clause}"

    def create_external_statement(
        self,
        external_bucket: str,
        file_format: AnalyticsExternalFileFormat = AnalyticsExternalFileFormat.DEFAULT,
        table_format: AnalyticsExternalTableFormat = AnalyticsExternalTableFormat.DEFAULT,
        file_ext: Optional[str] = None,
    ) -> str:
        name, external_bucket = sqlpp_escape(self.name, external_bucket)

        with_clause_options = {}
        if file_format is not AnalyticsExternalFileFormat.DEFAULT:
            with_clause_options["format"] = file_format.value
        if table_format is not AnalyticsExternalTableFormat.DEFAULT:
            with_clause_options["table-format"] = table_format.value
        if file_ext:
            with_clause_options["include"] = f"*.{file_ext}"

        cmd = (
            f"CREATE EXTERNAL DATASET {name} ON {external_bucket} AT `external_link` "
            f"USING '{self.source or self.name}'"
        )

        if with_clause_options:
            cmd += f" WITH {json.dumps(with_clause_options)}"

        return cmd

    def copy_into_statement(
        self,
        external_bucket: str,
        file_format: AnalyticsExternalFileFormat = AnalyticsExternalFileFormat.DEFAULT,
        file_ext: Optional[str] = None,
        path_keyword: Literal["PATH", "USING"] = "PATH",
    ) -> str:
        name, external_bucket = sqlpp_escape(self.name, external_bucket)

        with_clause_options = {}
        if file_format is not AnalyticsExternalFileFormat.DEFAULT:
            with_clause_options["format"] = file_format.value
        if file_ext:
            with_clause_options["include"] = f"*.{file_ext}"

        cmd = (
            f"COPY INTO {name} FROM {external_bucket} "
            f"AT `external_link` {path_keyword} '{self.source or self.name}'"
        )

        if with_clause_options:
            cmd += f" WITH {json.dumps(with_clause_options)}"

        return cmd

    def create_primary_idx_statement(self) -> str:
        return f"CREATE PRIMARY INDEX ON {sqlpp_escape(self.name)}"

    def drop_primary_idx_statement(self) -> str:
        return f"DROP INDEX {self.name}.primary_idx_{self.name}"

    def analyze_statement(
        self,
        sample_size: AnalyticsCBOSampleSize = AnalyticsCBOSampleSize.DEFAULT,
    ) -> str:
        with_clause = ""
        if sample_size is not AnalyticsCBOSampleSize.DEFAULT:
            with_clause = f' WITH {{ "sample": "{sample_size.value}" }}'
        return f"ANALYZE ANALYTICS COLLECTION {sqlpp_escape(self.name)}{with_clause}"


@dataclass(frozen=True)
class IndexDef:
    name: str
    collection: str
    fields: tuple[str]

    def create_statement(self) -> str:
        name, collection = sqlpp_escape(self.name, self.collection)
        return f"CREATE INDEX {name} ON {collection}({', '.join(self.fields)})"


class AnalyticsTest(PerfTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0
        self.analytics_settings = self.test_config.analytics_settings
        self.analytics_link = self.analytics_settings.analytics_link
        self.storage_format = self.test_config.analytics_settings.storage_format
        self.config_file = self.analytics_settings.analytics_config_file
        self.rest_session = None

        if self.config_file:
            with open(self.config_file, "r") as f:
                self.dataset_config = json.load(f)

    def __exit__(self, *args):
        if self.rest.is_columnar(self.analytics_node) and self.cluster_spec.cloud_infrastructure:
            self.report_columnar_cloud_storage_stats()

        super().__exit__(*args)

    @property
    def is_capella_columnar(self) -> bool:
        return (
            self.cluster_spec.capella_infrastructure and self.cluster_spec.columnar_infrastructure
        )

    @property
    def data_node(self) -> str:
        return next(self.cluster_spec.masters)

    @property
    def analytics_node(self) -> str:
        # If we have several clusters, we assume the second cluster to be the analytics cluster
        if len(masters := list(self.cluster_spec.masters)) > 1:
            return masters[1]

        return self.analytics_nodes[0]

    @property
    def datasets(self) -> list[DatasetDef]:
        return []

    @property
    def indexes(self) -> list[IndexDef]:
        return []

    def long_query_session(self) -> requests.Session:
        if self.rest_session is None:
            self.rest_session = requests.Session()
            url = urlparse(
                self.rest._get_api_url(
                    host=self.analytics_node,
                    path="analytics/service",
                    plain_port=ANALYTICS_PORT,
                    ssl_port=ANALYTICS_PORT_SSL,
                )
            )
            keep_alive = socket_options.TCPKeepAliveAdapter(idle=120, count=20, interval=30)
            self.rest_session.mount(f"{url.scheme}://{url.netloc}", keep_alive)

        return self.rest_session

    def exec_analytics_statement(
        self, host: str, statement: str, *, verbose: bool = False
    ) -> requests.Response:
        """Execute an analytics statement."""
        if verbose:
            logger.info(f"Running: {statement}")

        url = self.rest._get_api_url(
            host=host,
            path="analytics/service",
            plain_port=ANALYTICS_PORT,
            ssl_port=ANALYTICS_PORT_SSL,
        )
        resp = self.rest.session_post(
            self.long_query_session(), url=url, data={"statement": statement}
        )

        if verbose:
            logger.info(f"Result: {resp}")

        return resp

    def _run_statements(
        self,
        defs: list[Union[DatasetDef, IndexDef]],
        get_statement: Callable[[Union[DatasetDef, IndexDef]], str],
        *,
        verbose: bool = False,
    ):
        """Run analytics statements for each given dataset or index."""
        for def_ in defs:
            statement = get_statement(def_)
            self.exec_analytics_statement(self.analytics_node, statement, verbose=verbose)

    def create_datasets_at_link(self, *, verbose: bool = False):
        logger.info("Creating datasets")
        self._run_statements(
            self.datasets,
            lambda dataset: dataset.create_at_link_statement(
                self.analytics_link, self.storage_format
            ),
            verbose=verbose,
        )

    def create_standalone_datasets(self, *, verbose: bool = False):
        logger.info("Creating standalone datasets")
        self._run_statements(
            self.datasets,
            lambda dataset: dataset.create_standalone_statement(storage_format=self.storage_format),
            verbose=verbose,
        )

    def create_external_datasets(self, *, verbose: bool = False):
        logger.info("Creating external datasets")
        self._run_statements(
            self.datasets,
            lambda dataset: dataset.create_external_statement(
                self.analytics_settings.external_bucket,
                self.analytics_settings.external_file_format,
                self.analytics_settings.external_table_format,
                self.analytics_settings.external_file_include,
            ),
            verbose=verbose,
        )

    def create_analytics_indexes(self, *, verbose: bool = False):
        logger.info("Creating indexes")
        self._run_statements(self.indexes, lambda index: index.create_statement(), verbose=verbose)

    def create_primary_indexes(self, *, verbose: bool = False):
        logger.info("Creating primary indexes")
        self._run_statements(
            self.datasets, lambda dataset: dataset.create_primary_idx_statement(), verbose=verbose
        )

    def drop_primary_indexes(self, *, verbose: bool = False):
        logger.info("Dropping primary indexes")
        self._run_statements(
            self.datasets, lambda dataset: dataset.drop_primary_idx_statement(), verbose=verbose
        )

    def analyze_datasets(
        self,
        sample_size: AnalyticsCBOSampleSize = AnalyticsCBOSampleSize.DEFAULT,
        *,
        verbose: bool = False,
    ):
        logger.info(f"Analyzing datasets for CBO using {sample_size.name.lower()} sample size")
        self._run_statements(
            self.datasets,
            lambda dataset: dataset.analyze_statement(sample_size),
            verbose=verbose,
        )

    def create_external_link(self):
        external_dataset_type = self.analytics_settings.external_dataset_type

        kwargs = {
            "analytics_node": self.analytics_node,
            "link_name": "external_link",
            "link_type": external_dataset_type,
        }

        if external_dataset_type == "s3":
            external_dataset_region = self.analytics_settings.external_dataset_region
            access_key_id, secret_access_key = local.get_aws_credential(
                self.analytics_settings.aws_credential_path
            )
            kwargs |= {
                "s3_region": external_dataset_region,
                "s3_access_key_id": access_key_id,
                "s3_secret_access_key": secret_access_key,
            }
        elif external_dataset_type == "gcs":
            with open(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "r") as f:
                kwargs["gcs_json_creds"] = json.load(f)
        else:
            logger.interrupt(
                "Could not create external link. "
                f"Perfrunner doesn't support external link type: {external_dataset_type}"
            )

        self.rest.create_analytics_link(**kwargs)

    def sync(self):
        self.disconnect_link()
        self.create_datasets_at_link()
        self.create_analytics_indexes()
        self.connect_link()
        bucket_replica = self.test_config.bucket.replica_number
        sql_suite = self.test_config.access_settings.sql_suite
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(
                self.data_node, bucket, bucket_replica, self.analytics_node, sql_suite
            )

    def connect_link(self):
        logger.info(f"Connecting Link {self.analytics_link}")
        statement = f"CONNECT LINK {self.analytics_link}"
        self.rest.exec_analytics_statement(self.analytics_node, statement)

    def disconnect_link(self):
        logger.info(f"Disconnecting Link {self.analytics_link}")
        statement = f"DISCONNECT LINK {self.analytics_link}"
        self.rest.exec_analytics_statement(self.analytics_node, statement)

    def _restore_remote(self):
        self.remote.extract_cb_any(
            filename="couchbase", worker_home=self.worker_manager.WORKER_HOME
        )
        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)

        archive = self.test_config.restore_settings.backup_storage

        if archive.startswith("s3://"):
            credential = local.read_aws_credential(
                self.test_config.backup_settings.aws_credential_path
            )
            self.remote.create_aws_credential(credential)

        self.remote.client_drop_caches()

        if self.test_config.restore_settings.modify_storage_dir_name:
            suffix_repo = "aws"
            if self.cluster_spec.capella_infrastructure:
                suffix_repo = self.cluster_spec.capella_backend
            archive += f"/{suffix_repo}"

        self.remote.restore(
            cluster_spec=self.cluster_spec,
            master_node=self.master_node,
            threads=self.test_config.restore_settings.threads,
            worker_home=self.worker_manager.WORKER_HOME,
            archive=archive,
            repo=self.test_config.restore_settings.backup_repo,
            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
            obj_region=self.test_config.backup_settings.obj_region,
            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
            use_tls=self.test_config.restore_settings.use_tls,
            map_data=self.test_config.restore_settings.map_data,
            encrypted=self.test_config.restore_settings.encrypted,
            passphrase=self.test_config.restore_settings.passphrase,
            include_data=self.test_config.restore_settings.include_data,
        )

    def restore_data(self):
        if self.cluster_spec.cloud_infrastructure:
            self._restore_remote()
        else:
            self.restore_local()

    def copy_data_from_object_store(self, datasets: list[DatasetDef] = []) -> tuple[int, float]:
        """Ingest data from cloud object store using COPY FROM.

        Returns the total number of items copied and the total time taken to copy the data.
        """
        logger.info("Ingesting data from cloud object store using COPY FROM")

        datasets_to_import = datasets or self.datasets

        external_bucket = self.analytics_settings.external_bucket
        file_format = self.analytics_settings.external_file_format
        file_include = self.analytics_settings.external_file_include

        path_keyword = "PATH"
        if not self.is_capella_columnar and (
            (8, 0, 0, 0) < self.cluster.build_tuple < (8, 0, 0, 1452)
        ):
            path_keyword = "USING"

        total_items_copied = 0
        total_copy_time = 0
        for dataset in datasets_to_import:
            statement = dataset.copy_into_statement(
                external_bucket, file_format, file_include, path_keyword
            )
            t0 = time.time()
            self.exec_analytics_statement(self.analytics_node, statement, verbose=True)
            copy_time = time.time() - t0
            logger.info(f"Statement execution time: {copy_time}")
            items_copied = self.get_dataset_items(dataset.name)
            logger.info(f"Average ingestion rate (items/sec): {items_copied / copy_time:.2f}")
            total_items_copied += items_copied
            total_copy_time += copy_time

        return total_items_copied, total_copy_time

    def report_columnar_cloud_storage_stats(self):
        """Report cloud storage bucket stats for Columnar tests."""
        analytics_settings = self.rest.get_analytics_settings(self.analytics_node)
        if (bucket_name := analytics_settings.get("blobStorageBucket")) is None:
            logger.warning(
                "No cloud storage bucket found in analytics settings."
                "Cannot report cloud storage bucket stats."
            )
            return

        blob_storage_scheme = analytics_settings.get("blobStorageScheme")
        get_cloud_storage_bucket_stats(f"{blob_storage_scheme}://{bucket_name}")

    def get_dataset_items(self, dataset: str) -> int:
        statement = f"SELECT COUNT(*) from {sqlpp_escape(dataset)};"
        result = self.exec_analytics_statement(self.analytics_node, statement)
        num_items = result.json()["results"][0]["$1"]
        logger.info(f"Number of items in dataset {dataset}: {num_items}")
        return num_items


class BigFunTest(AnalyticsTest):
    COLLECTORS = {"analytics": True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.QUERIES = self.analytics_settings.queries

    @property
    def indexes(self) -> list[IndexDef]:
        if self.config_file:
            return [
                IndexDef(index["Index"], index["Dataset"], (f"{index['Field']}: string",))
                for index in self.dataset_config["Analytics"]
            ]

        return [
            IndexDef("usrSinceIdx", "GleambookUsers-1", ("user_since: string",)),
            IndexDef("gbmSndTimeIdx", "GleambookMessages-1", ("send_time: string",)),
            IndexDef("cmSndTimeIdx", "ChirpMessages-1", ("send_time: string",)),
        ]

    @property
    def datasets(self) -> list[DatasetDef]:
        datasets = []
        for bucket in self.test_config.buckets:
            if self.config_file:
                for dataset in self.dataset_config["Analytics"]:
                    name = dataset["Dataset"]
                    where = None
                    if self.dataset_config["DefaultCollection"]:
                        source = f"`{bucket}`"
                        where = (
                            f"WHERE `type` = \"{dataset['Type']}\" "
                            f"AND meta().id LIKE \"%-{dataset['Group']}\";"
                        )
                    else:
                        source = f"`{bucket}`.`scope-1`.`{dataset['Collection']}`"

                    datasets.append(DatasetDef(name, source, where))
            else:
                datasets += [
                    DatasetDef(dataset, f"`{bucket}`", f"WHERE `{key}` IS NOT UNKNOWN;")
                    for dataset, key in (
                        ("GleambookUsers-1", "id"),
                        ("GleambookMessages-1", "message_id"),
                        ("ChirpMessages-1", "chirpid"),
                    )
                ]

        return datasets

    def re_sync(self):
        self.connect_link()
        bucket_replica = self.test_config.bucket.replica_number
        sql_suite = self.test_config.access_settings.sql_suite
        for bucket in self.test_config.buckets:
            self.monitor.monitor_data_synced(
                self.data_node, bucket, bucket_replica, self.analytics_node, sql_suite
            )


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
        super().restore_data()
        self.wait_for_persistence()

        if self.analytics_link != "Local":
            self.rest.create_analytics_link(
                self.analytics_node, self.analytics_link, "couchbase", cb_data_node=self.data_node
            )

        sync_time = self.sync()

        self.report_kpi(sync_time)


class BigFunIncrSyncTest(BigFunSyncTest):
    def _report_kpi(self, sync_time: int):
        self.reporter.post(
            *self.metrics.avg_ingestion_rate(self.num_items, sync_time, "incremental")
        )

    @with_stats
    @timeit
    def re_sync(self):
        super().re_sync()

    def sync(self) -> float:
        super().sync()
        self.disconnect_link()
        super().restore_data()
        self.wait_for_persistence()
        return self.re_sync()


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
        super().restore_data()
        self.wait_for_persistence()
        super().sync()

        drop_dataset = self.analytics_settings.drop_dataset
        num_items = self.get_dataset_items(drop_dataset)

        drop_time = self.drop_dataset(drop_dataset)

        self.report_kpi(num_items, drop_time)


class BigFunQueryTest(BigFunTest):

    def warmup(self, nodes: list = []) -> list[QueryLatencyPair]:
        if len(nodes) == 0:
            analytics_nodes = self.analytics_nodes
        else:
            analytics_nodes = nodes
        logger.info("analytics_nodes = {}".format(analytics_nodes))
        results = bigfun(
            self.rest,
            nodes=analytics_nodes,
            concurrency=self.test_config.access_settings.analytics_warmup_workers,
            num_requests=int(self.test_config.access_settings.analytics_warmup_ops),
            query_set=self.QUERIES,
            request_params=self.analytics_settings.bigfun_request_params,
        )

        return [(query, latency) for query, latency in results]

    @with_stats
    def access(self, nodes: list = [], *args, **kwargs) -> list[QueryLatencyPair]:
        if len(nodes) == 0:
            analytics_nodes = self.analytics_nodes
        else:
            analytics_nodes = nodes
        logger.info("analytics_nodes = {}".format(analytics_nodes))
        results = bigfun(
            self.rest,
            nodes=analytics_nodes,
            concurrency=int(self.test_config.access_settings.workers),
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.QUERIES,
            request_params=self.analytics_settings.bigfun_request_params,
        )
        return [(query, latency) for query, latency in results]

    def _report_kpi(self, results: list[QueryLatencyPair]):
        for query, latency in results:
            self.reporter.post(
                *self.metrics.analytics_latency(query, latency)
            )

    def run(self):
        random.seed(8095)
        super().restore_data()
        self.wait_for_persistence()

        self.sync()

        logger.info('Running warmup phase')
        self.warmup()

        logger.info('Running access phase')
        results = self.access()

        self.report_kpi(results)


class BigFunQueryNoIndexTest(BigFunQueryTest):
    def create_analytics_indexes(self, *args, **kwargs):
        pass


class BigFunQueryNoIndexExternalTest(BigFunQueryTest):
    @property
    def datasets(self) -> list[DatasetDef]:
        return [
            DatasetDef(dataset["Dataset"], dataset["Collection"])
            for dataset in self.dataset_config["Analytics"]
        ]

    @with_stats
    def access(self, nodes: list = [], *args, **kwargs) -> list[QueryLatencyPair]:
        if len(nodes) == 0:
            analytics_nodes = self.analytics_nodes
        else:
            analytics_nodes = nodes
        logger.info("analytics_nodes = {}".format(analytics_nodes))
        results = bigfun(
            self.rest,
            nodes=analytics_nodes,
            concurrency=int(self.test_config.access_settings.workers),
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.QUERIES,
            query_method=QueryMethod.CURL_CBAS,
            request_params=self.analytics_settings.bigfun_request_params,
        )
        return [(query, latency) for query, latency in results]

    def run(self):
        random.seed(8095)
        self.create_external_link()
        self.create_external_datasets()

        logger.info('Running access phase')
        results = self.access()

        self.report_kpi(results)


class ColumnarCopyFromObjectStoreTest(BigFunQueryNoIndexExternalTest):
    COLLECTORS = {'ns_server': False, 'active_tasks': False, 'analytics': True}

    @with_stats
    def copy_data_from_object_store(self, datasets: list[DatasetDef] = []) -> tuple[int, float]:
        return super().copy_data_from_object_store(datasets)

    @with_stats
    def access(self) -> list[QueryLatencyPair]:
        nodes = self.analytics_nodes
        query_method = QueryMethod.CURL_CBAS

        logger.info("analytics_nodes = {}".format(nodes))
        results = bigfun(
            self.rest,
            nodes=nodes,
            concurrency=int(self.test_config.access_settings.workers),
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.QUERIES,
            query_method=query_method,
            request_params=self.analytics_settings.bigfun_request_params,
        )
        return [(query, latency) for query, latency in results]

    def report_ingestion_kpi(self, ingestion_items: int, ingestion_time: float):
        if self.test_config.stats_settings.enabled:
            v, snapshots, metric_info = self.metrics.avg_ingestion_rate(
                ingestion_items,
                ingestion_time,
                f"copy_from_{self.analytics_settings.external_dataset_type.lower()}",
            )
            metric_info["category"] = "sync"
            self.reporter.post(v, snapshots, metric_info)

    def run(self):
        random.seed(8095)

        self.create_external_link()
        self.create_standalone_datasets(verbose=True)

        copy_from_items, copy_from_time = self.copy_data_from_object_store()
        logger.info(f"Total items ingested using COPY FROM: {copy_from_items}")
        logger.info(f"Total data ingestion time using COPY FROM (s): {copy_from_time:.2f}")
        self.report_ingestion_kpi(copy_from_items, copy_from_time)

        results = self.access()
        self.report_kpi(results)


class ColumnarCopyToObjectStoreTest(ColumnarCopyFromObjectStoreTest):
    @with_stats
    def access(self):
        copy_to_settings = self.test_config.columnar_copy_to_settings

        with open(copy_to_settings.object_store_query_file, "r") as f:
            queries = json.load(f)

        obj_store_uri = self.cluster_spec.backup
        obj_store_name = obj_store_uri.split("://")[1]

        query_template = (
            f"COPY {{}} TO `{obj_store_name}` AT `external_link` PATH ({{}}) {{}} {{}} {{}}"
        )

        objects, size = get_cloud_storage_bucket_stats(obj_store_uri, aws_profile="default")

        for fmt, mopf, (comp_type, comp_level) in copy_to_settings.all_param_combinations():
            for query in queries:
                comp = comp_type + (f"-{comp_level}" if comp_level else "")
                output_path_prefix = f"\"mopf-{mopf}/{fmt}/compression-{comp}/{query['id']}\""

                output_path_expr = output_path_prefix
                if output_path_exps := query.get("output_path_exps"):
                    output_path_expr += f", {', '.join(output_path_exps)}"

                partition_clause = ""
                if partition_exps := query.get("partition_exps"):
                    partition_clause = f"PARTITION BY {', '.join(partition_exps)}"

                order_clause = ""
                if order_exps := query.get("order_exps"):
                    order_clause = f"ORDER BY {', '.join(order_exps)}"

                over_clause = ""
                if partition_clause or order_clause:
                    over_clause = (
                        f"OVER ({' '.join(filter(None, (partition_clause, order_clause)))})"
                    )

                schema_clause = ""
                if fmt == "csv":
                    obj_type_def = json.dumps(query["obj_type_def"]).replace('"', "")
                    schema_clause = f"TYPE ({obj_type_def})"

                with_options = {"format": fmt, "max-objects-per-file": mopf}
                if comp_type != "none":
                    with_options["compression"] = comp_type
                    if comp_level:
                        with_options["gzipCompressionLevel"] = comp_level

                with_clause = f"WITH {json.dumps(with_options)}"

                statement = query_template.format(
                    query["source_def"], output_path_expr, over_clause, schema_clause, with_clause
                )

                t0 = time.time()
                resp = self.exec_analytics_statement(self.analytics_node, statement, verbose=True)
                latency = time.time() - t0

                logger.info(resp.json())
                logger.info(f"client-side query response time (s): {latency}")

                new_objects, new_size = get_cloud_storage_bucket_stats(
                    obj_store_uri, aws_profile="default"
                )
                if not (new_objects > objects and new_size > size):
                    logger.warning(
                        "Cloud storage bucket object count and data size have not both increased. "
                        "COPY TO statement has not written any data!"
                    )
                objects, size = new_objects, new_size

    def run(self):
        random.seed(8095)

        self.create_external_link()
        self.create_standalone_datasets(verbose=True)

        # If object_store_import_datasets is empty, all pre-defined datasets will be imported
        datasets_to_import = [
            d
            for d in self.datasets
            if d.name in self.test_config.columnar_settings.object_store_import_datasets
        ]

        copy_from_items, copy_from_time = self.copy_data_from_object_store(datasets_to_import)
        logger.info(f"Total items ingested using COPY FROM: {copy_from_items}")
        logger.info(f"Total data ingestion time using COPY FROM (s): {copy_from_time:.2f}")

        self.access()


class ColumnarCopyToKVRemoteLinkTest(ColumnarCopyFromObjectStoreTest):
    COLLECTORS = {"ns_server": True, "active_tasks": False, "analytics": True}

    @with_stats
    def access(self):
        with open(self.test_config.columnar_copy_to_settings.kv_query_file, "r") as f:
            queries = json.load(f)

        query_template = f"COPY {{}} TO {{}} AT `{self.analytics_link}` KEY {{}}"

        for query in queries:
            statement = query_template.format(
                query["source_def"], query["dest_coll_qualified_name"], query["key"]
            )

            t0 = time.time()
            resp = self.exec_analytics_statement(self.analytics_node, statement)
            latency = time.time() - t0

            logger.info(resp.json())
            logger.info(f"client-side query response time (s): {latency}")

    def run(self):
        self.create_external_link()
        self.create_standalone_datasets(verbose=True)

        copy_from_items, copy_from_time = self.copy_data_from_object_store()
        logger.info(f"Total items ingested using COPY FROM: {copy_from_items}")
        logger.info(f"Total data ingestion time using COPY FROM (s): {copy_from_time:.2f}")

        self.rest.create_analytics_link(
            self.analytics_node, self.analytics_link, "couchbase", cb_data_node=self.data_node
        )
        self.connect_link()

        self.access()


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


class BigFunRebalanceTest(BigFunTest, RebalanceTest):

    ALL_HOSTNAMES = True

    def rebalance_cbas(self):
        self.rebalance(services='cbas')

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.rebalance_time(rebalance_time=self.rebalance_time)
        )

    def run(self):
        super().restore_data()
        self.wait_for_persistence()

        self.sync()

        self.rebalance_cbas()

        if self.is_balanced():
            self.report_kpi()

class AnalyticsDynamicServiceRebalanceTest(BigFunRebalanceTest, DynamicServiceRebalanceTest):
    pass


class BigFunRebalanceCapellaTest(BigFunRebalanceTest, CapellaRebalanceKVTest):
    pass


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
        super().restore_data()
        self.wait_for_persistence()

        if self.analytics_link != "Local":
            self.rest.create_analytics_link(
                self.analytics_node, self.analytics_link, "couchbase", cb_data_node=self.data_node
            )

        self.sync()

        avg_connect_time, avg_disconnect_time = \
            self.connect_cycle(int(self.test_config.access_settings.ops))

        self.report_kpi(avg_connect_time, avg_disconnect_time)


class TPCDSTest(AnalyticsTest):
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

    TPCDS_INDEXES = [
        IndexDef("c_customer_sk_idx", "customer", ("c_customer_sk:STRING",)),
        IndexDef("d_date_sk_idx", "date_dim", ("d_date_sk:STRING",)),
        IndexDef("d_date_idx", "date_dim", ("d_date:STRING",)),
        IndexDef("d_month_seq_idx", "date_dim", ("d_month_seq:BIGINT",)),
        IndexDef("d_year_idx", "date_dim", ("d_year:BIGINT",)),
        IndexDef("i_item_sk_idx", "item", ("i_item_sk:STRING",)),
        IndexDef("s_state_idx", "store", ("s_state:STRING",)),
        IndexDef("s_store_sk_idx", "store", ("s_store_sk:STRING",)),
        IndexDef("sr_returned_date_sk_idx", "store_returns", ("sr_returned_date_sk:STRING",)),
        IndexDef("ss_sold_date_sk_idx", "store_sales", ("ss_sold_date_sk:STRING",)),
    ]

    COLLECTORS = {'analytics': True}

    @property
    def indexes(self) -> list[IndexDef]:
        return self.TPCDS_INDEXES

    @property
    def datasets(self) -> list[DatasetDef]:
        return [
            DatasetDef(name, f"`{bucket}`", f"WHERE table_name = '{name}'")
            for bucket in self.test_config.buckets
            for name in self.TPCDS_DATASETS
        ]

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

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=tpcds_initial_data_load_task)

    def sync(self):
        self.disconnect_link()
        self.create_datasets_at_link()
        self.connect_link()
        bucket_replica = self.test_config.bucket.replica_number
        sql_suite = self.test_config.access_settings.sql_suite
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(
                self.data_node, bucket, bucket_replica, self.analytics_node, sql_suite
            )

    def run(self):
        self.download_tpcds_couchbase_loader()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()


class TPCDSQueryTest(TPCDSTest):

    COUNT_QUERIES = 'perfrunner/workloads/tpcdsfun/count_queries.json'
    QUERIES = 'perfrunner/workloads/tpcdsfun/queries.json'

    @with_stats
    def access(self, *args, **kwargs) -> tuple[list[QueryLatencyPair], list[QueryLatencyPair],
                                               list[QueryLatencyPair], list[QueryLatencyPair]]:

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

        self.create_analytics_indexes()

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

    def _report_kpi(self, results: list[QueryLatencyPair], with_index: bool):
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


class CH2Test(AnalyticsTest):
    BUCKET = "bench"

    DATASETS = [
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

    FLAT_DATASETS = DATASETS + [
        "customer_item_categories",
        "customer_addresses",
        "customer_phones",
        "orders_orderline",
        "item_categories",
    ]

    GSI_INDEXES = [
        ("cu_w_id_d_id_last", "customer", ("c_w_id", "c_d_id", "c_last")),
        ("di_id_w_id", "district", ("d_id", "d_w_id")),
        ("no_o_id_d_id_w_id", "neworder", ("no_o_id", "no_d_id", "no_w_id")),
        ("or_id_d_id_w_id_c_id", "orders", ("o_id", "o_d_id", "o_w_id", "o_c_id")),
        ("or_w_id_d_id_c_id", "orders", ("o_w_id", "o_d_id", "o_c_id")),
        ("wh_id", "warehouse", ("w_id",)),
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
        self.schema = self.test_config.ch2_settings.schema
        self.dataset_names = (
            self.FLAT_DATASETS if self.schema is CH2Schema.CH2PPF else self.DATASETS
        )
        self.analytics_index_def_statements = []
        if self.test_config.ch2_settings.analytics_index_def_file:
            with open(self.test_config.ch2_settings.analytics_index_def_file, "r") as f:
                for statement in f.read().split(";"):
                    if cleaned := re.sub(r"\s+", " ", statement.strip()):
                        self.analytics_index_def_statements.append(cleaned)

    @property
    def datasets(self) -> list[DatasetDef]:
        dataset_names = self.dataset_names
        if self.test_config.ch2_settings.use_backup and (
            included := self.test_config.restore_settings.include_data
        ):
            dataset_names = [coll_string.split(".")[-1] for coll_string in included.split(",")]
        return [
            DatasetDef(name, f"{self.BUCKET}.{self.schema.value}.{name}") for name in dataset_names
        ]

    @property
    def indexes(self) -> list[IndexDef]:
        return [
            IndexDef(name, f"{self.BUCKET}.{self.schema.value}.{coll}", fields)
            for name, coll, fields in self.GSI_INDEXES
        ]

    def _report_kpi(self, log_file: Optional[str] = None, extra_metric_id_suffix: str = ""):
        measure_time = (
            self.test_config.ch2_settings.duration - self.test_config.ch2_settings.warmup_duration
        )
        ch2_metrics = self.metrics.ch2_metrics(
            duration=measure_time, logfile=log_file or self.test_config.ch2_settings.workload
        )

        if self.test_config.ch2_settings.tclients:
            self.reporter.post(
                *self.metrics.ch2_tpm(
                    round(ch2_metrics.tpm, 2),
                    self.test_config.ch2_settings.tclients,
                    extra_metric_id_suffix,
                )
            )
            self.reporter.post(
                *self.metrics.ch2_response_time(
                    round(ch2_metrics.txn_response_time, 2),
                    self.test_config.ch2_settings.tclients,
                    extra_metric_id_suffix,
                )
            )

        if self.test_config.ch2_settings.aclients:
            self.reporter.post(
                *self.metrics.ch2_geo_mean_query_time(
                    ch2_metrics.geo_mean_cbas_query_time,
                    self.test_config.ch2_settings.tclients,
                    extra_metric_id_suffix,
                )
            )
            self.reporter.post(
                *self.metrics.ch2_analytics_query_set_time(
                    ch2_metrics.average_cbas_query_set_time,
                    self.test_config.ch2_settings.tclients,
                    extra_metric_id_suffix,
                )
            )
            self.reporter.post(
                *self.metrics.ch2_analytics_qph(
                    ch2_metrics.cbas_qph,
                    self.test_config.ch2_settings.tclients,
                    extra_metric_id_suffix,
                )
            )

    def create_analytics_indexes(self):
        if statements := self.analytics_index_def_statements:
            logger.info('Creating analytics indexes')
            for statement in statements:
                self.exec_analytics_statement(self.analytics_node, statement, verbose=True)

    def create_gsi_indexes(self):
        logger.info("Creating indexes")
        for index_def in self.indexes:
            statement = f"{index_def.create_statement()} using gsi;"
            logger.info(f"Running: {statement}")
            res = self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            logger.info(f"Result: {res}")
            time.sleep(5)

    def sync(self) -> float:
        self.disconnect_link()
        self.create_datasets_at_link(verbose=True)
        self.create_analytics_indexes()
        self.connect_link()

        t0 = time.time()
        bucket_replica = self.test_config.bucket.replica_number
        sql_suite = self.test_config.access_settings.sql_suite
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(
                self.data_node, bucket, bucket_replica, self.analytics_node, sql_suite
            )
        return time.time() - t0

    def _create_ch2_conn_settings(self) -> CH2ConnectionSettings:
        query_port = QUERY_PORT
        cbas_port = ANALYTICS_PORT

        if use_tls := self.test_config.cluster.enable_n2n_encryption:
            query_port = QUERY_PORT_SSL
            cbas_port = ANALYTICS_PORT_SSL

        query_urls = [f"{node}:{query_port}" for node in self.query_nodes]
        userid, password = self.cluster_spec.rest_credentials

        return CH2ConnectionSettings(
            userid=userid,
            password=password,
            analytics_url=f"{self.analytics_node}:{cbas_port}",
            query_url=query_urls[0] if query_urls else None,
            multi_query_url=",".join(query_urls),
            data_url=self.data_nodes[0],
            multi_data_url=",".join(self.data_nodes),
            use_tls=use_tls,
        )

    def _distributed_ch2_load(self):
        conn_settings = self._create_ch2_conn_settings()
        ch2_settings = self.test_config.ch2_settings
        load_tasks = ch2_settings.load_tasks

        total_warehouses = ch2_settings.warehouses
        min_warehouses_per_task = total_warehouses // load_tasks
        leftover = total_warehouses % load_tasks

        warehouses_per_task = [min_warehouses_per_task] * load_tasks
        for i in range(leftover):
            warehouses_per_task[i] += 1

        task_sigs = []
        workers = itertools.cycle(self.cluster_spec.workers)
        starting_warehouse = 1
        for i, warehouses in enumerate(warehouses_per_task):
            worker = next(workers)

            task_settings = copy.deepcopy(ch2_settings)
            task_settings.warehouses = warehouses
            task_settings.starting_warehouse = starting_warehouse

            sig = ch2_load.si(conn_settings, task_settings, "nestcollections", f"ch2_load_{i}").set(
                queue=worker
            )
            task_sigs.append(sig)

            starting_warehouse += warehouses

        async_result = group(task_sigs).apply_async()
        logger.info(f"Running CH2 load task group: {async_result}")
        async_result.get()
        logger.info("CH2 load task group finished")

    def load_ch2(self):
        logger.info("load CH2 docs")
        if (ch2_settings := self.test_config.ch2_settings).load_tasks > 1:
            self._distributed_ch2_load()
        elif self.worker_manager.is_remote:
            self.remote.ch2_load_task(
                self._create_ch2_conn_settings(),
                ch2_settings,
                worker_home=self.worker_manager.WORKER_HOME,
            )
        else:
            local.ch2_load_task(self._create_ch2_conn_settings(), ch2_settings)

    @with_stats
    def run_ch2(self, log_file: str = "", ch2_settings: Optional[CH2] = None):
        logger.info(f"Running {self.test_config.ch2_settings.workload}")
        log_file = log_file or self.test_config.ch2_settings.workload
        ch2_settings = ch2_settings or self.test_config.ch2_settings

        if self.worker_manager.is_remote:
            self.remote.ch2_run_task(
                self._create_ch2_conn_settings(),
                ch2_settings,
                self.worker_manager.WORKER_HOME,
                log_file=log_file,
            )
            self.remote.get_ch2_logfile(
                worker_home=self.worker_manager.WORKER_HOME, logfile=log_file
            )
        else:
            local.ch2_run_task(self._create_ch2_conn_settings(), ch2_settings, log_file=log_file)

    def restart(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                self.monitor.monitor_warmup(self.memcached, master, bucket)

    def init_ch2_repo(self):
        if self.worker_manager.is_remote:
            self.remote.init_ch2(
                repo=self.test_config.ch2_settings.repo,
                branch=self.test_config.ch2_settings.branch,
                worker_home=self.worker_manager.WORKER_HOME,
                cherrypick=self.test_config.ch2_settings.cherrypick,
            )
        else:
            local.clone_git_repo(
                repo=self.test_config.ch2_settings.repo,
                branch=self.test_config.ch2_settings.branch,
                cherrypick=self.test_config.ch2_settings.cherrypick,
            )

    def run(self):
        self.init_ch2_repo()

        if self.test_config.ch2_settings.use_backup:
            self.restore_data()
        else:
            self.load_ch2()

        self.wait_for_persistence()
        self.restart()
        self.sync()
        if (
            self.test_config.ch2_settings.create_gsi_index
            and not self.cluster_spec.columnar_infrastructure
        ):
            self.create_gsi_indexes()

        if self.test_config.analytics_settings.use_cbo:
            self.analyze_datasets(self.test_config.analytics_settings.cbo_sample_size, verbose=True)

        self.run_ch2()
        if self.test_config.ch2_settings.workload != 'ch2_analytics':
            self.report_kpi()


class CH2RemoteLinkTest(CH2Test):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)

    @property
    def data_nodes(self) -> list[str]:
        return self.rest.get_active_nodes_by_role(self.data_node, 'kv')

    @property
    def query_nodes(self) -> list[str]:
        return self.rest.get_active_nodes_by_role(self.data_node, 'n1ql')

    @property
    def analytics_nodes(self) -> list[str]:
        return self.rest.get_active_nodes_by_role(self.analytics_node, 'cbas')

    def restart(self):
        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for bucket in self.test_config.buckets:
            self.monitor.monitor_warmup(self.memcached, self.data_node, bucket)
        self.monitor.monitor_analytics_node_active(self.analytics_node)

    def report_sync_kpi(self, sync_time: int):
        logger.info(f"Sync time (s): {sync_time}")

        if self.test_config.stats_settings.enabled:
            v, snapshots, metric_info = self.metrics.avg_ingestion_rate(self.num_items, sync_time)
            metric_info["category"] = "sync"
            self.reporter.post(v, snapshots, metric_info)

    @with_stats
    def sync(self) -> float:
        self.rest.create_analytics_link(
            self.analytics_node, self.analytics_link, "couchbase", cb_data_node=self.data_node
        )
        sync_time = super().sync()
        self.report_sync_kpi(sync_time)


class CH2ColumnarSimulatedPauseResumeTest(CH2RemoteLinkTest):
    def __init__(self, *args, **kwargs):
        CH2Test.__init__(self, *args, **kwargs)
        self.cluster_spec.set_inactive_clusters_by_idx([2])
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

        self.cluster.set_columnar_cloud_storage()
        self.cluster.add_columnar_cloud_storage_creds()

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

        log_file = '{}_post_resume'.format(self.test_config.ch2_settings.workload)

        if self.test_config.analytics_settings.use_cbo:
            self.analyze_datasets(self.test_config.analytics_settings.cbo_sample_size, verbose=True)

        self.run_ch2(log_file=log_file)
        if self.test_config.ch2_settings.workload != 'ch2_analytics':
            self.report_kpi()


class CH2ColumnarStandaloneDatasetTest(CH2Test, ColumnarCopyFromObjectStoreTest):
    COLLECTORS = {
        "ns_server": False,
        "ns_server_system": True,
        "active_tasks": False,
        "analytics": True,
    }

    @property
    def datasets(self) -> list[DatasetDef]:
        return [DatasetDef(name) for name in self.dataset_names]

    def _create_ch2_conn_settings(self) -> CH2ConnectionSettings:
        userid, password = self.cluster_spec.rest_credentials
        if self.is_capella_columnar:
            userid, password = self.cluster_spec.capella_admin_credentials[0]

        use_tls = self.test_config.cluster.enable_n2n_encryption or self.is_capella_columnar
        port = ANALYTICS_PORT_SSL if use_tls else ANALYTICS_PORT

        return CH2ConnectionSettings(
            userid=userid,
            password=password,
            analytics_url=f"{self.analytics_node}:{port}",
            use_tls=use_tls,
        )

    def setup(self):
        local.clone_git_repo(
            repo=self.test_config.ch2_settings.repo,
            branch=self.test_config.ch2_settings.branch,
            cherrypick=self.test_config.ch2_settings.cherrypick,
        )

        self.create_external_link()
        self.create_standalone_datasets(verbose=True)

        self.create_analytics_indexes()

        copy_from_items, copy_from_time = self.copy_data_from_object_store()
        logger.info(f"Total items ingested using COPY FROM: {copy_from_items}")
        logger.info(f"Total data ingestion time using COPY FROM (s): {copy_from_time:.2f}")
        self.report_ingestion_kpi(copy_from_items, copy_from_time)

        if self.test_config.analytics_settings.use_cbo:
            self.analyze_datasets(self.test_config.analytics_settings.cbo_sample_size, verbose=True)

    def benchmark(self):
        self.run_ch2()
        self.report_kpi()

    def run(self):
        self.setup()
        self.benchmark()


class CH2CapellaColumnarCopyToObjectStoreTest(
    ColumnarCopyToObjectStoreTest, CH2ColumnarStandaloneDatasetTest
):
    pass


class CH2CapellaColumnarRemoteLinkTest(CH2RemoteLinkTest):
    COLLECTORS = {
        "iostat": False,
        "memory": False,
        "n1ql_latency": False,
        "n1ql_stats": True,
        "secondary_stats": False,
        "ns_server_system": True,
        "analytics": True,
        "active_tasks": False,
        "ns_server": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        (self.data_cluster_user, self.data_cluster_pwd), (self.columnar_user, self.columnar_pwd) = (
            self.cluster_spec.capella_admin_credentials
        )
        self.reporter.build = (
            f"{self.rest.get_version(self.analytics_node)} : {self.reporter.build}"
        )

    @with_stats
    def restore_data(self):
        super().restore_data()

    @with_stats
    def sync(self) -> float:
        return CH2Test.sync(self)

    def _create_ch2_conn_settings(self) -> CH2ConnectionSettings:
        query_port = QUERY_PORT_SSL
        query_urls = [f"{node}:{query_port}" for node in self.query_nodes]
        return CH2ConnectionSettings(
            userid=self.data_cluster_user,
            password=self.data_cluster_pwd,
            userid_analytics=self.columnar_user,
            password_analytics=self.columnar_pwd,
            analytics_url=f"{self.analytics_node}:{ANALYTICS_PORT_SSL}",
            query_url=query_urls[0] if query_urls else None,
            multi_query_url=",".join(query_urls),
            data_url=self.data_nodes[0],
            multi_data_url=",".join(self.data_nodes),
            use_tls=True,
        )

    def run(self):
        self.init_ch2_repo()

        instance_id = self.rest.instance_ids[0]
        self.rest.create_capella_remote_link(
            instance_id, self.analytics_link, self.cluster_spec.capella_cluster_ids[0]
        )
        self.monitor.wait_for_columnar_remote_link_ready(
            instance_id, self.analytics_link, timeout_secs=1200
        )
        self.disconnect_link()

        if self.test_config.analytics_settings.ingest_during_load:
            self.create_datasets_at_link()
            self.create_analytics_indexes()
            self.connect_link()

        if self.test_config.ch2_settings.use_backup:
            self.restore_data()
        else:
            self.load_ch2()

        if not self.test_config.analytics_settings.ingest_during_load:
            # Only wait for the KV cluster
            self.wait_for_persistence()
            sync_time = self.sync()
            self.report_sync_kpi(sync_time)
        else:
            bucket_replica = self.test_config.bucket.replica_number
            sql_suite = self.test_config.access_settings.sql_suite
            for bucket in self.test_config.buckets:
                self.num_items += self.monitor.monitor_data_synced(
                    self.data_node, bucket, bucket_replica, self.analytics_node, sql_suite
                )

        if self.test_config.ch2_settings.create_gsi_index:
            self.create_gsi_indexes()

        if self.test_config.analytics_settings.use_cbo:
            self.analyze_datasets(self.test_config.analytics_settings.cbo_sample_size, verbose=True)

        self.run_ch2()
        self.report_kpi()


class CapellaColumnarManualOnOffTest(PerfTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.instance_id = self.cluster_spec.controlplane_settings["columnar_ids"].split()[0]

    @property
    def on_off_settings(self) -> ColumnarSettings:
        return self.test_config.columnar_settings

    @timeit
    def turn_off(self):
        self.rest.turn_off_instance(self.instance_id)
        self.monitor.wait_for_columnar_instance_turn_off(
            self.instance_id,
            poll_interval_secs=self.on_off_settings.on_off_poll_interval,
            timeout_secs=self.on_off_settings.on_off_timeout,
        )

    def turn_on(self):
        self.rest.turn_on_instance(self.instance_id)
        t0 = time.time()
        self.monitor.wait_for_columnar_instance_turn_on(
            self.instance_id,
            poll_interval_secs=self.on_off_settings.on_off_poll_interval,
            timeout_secs=self.on_off_settings.on_off_timeout,
        )
        return time.time() - t0

    def run(self):
        on_duration = self.on_off_settings.on_duration
        off_duration = self.on_off_settings.off_duration

        turn_on_times, turn_off_times = [], []
        for i in range(max(cycles := self.on_off_settings.on_off_cycles, 1)):
            logger.info(f"Starting on/off cycle {i + 1}/{cycles}")

            logger.info(f"Waiting {on_duration} seconds before turning columnar instance off.")
            time.sleep(on_duration)

            turn_off_time = self.turn_off()
            logger.info(f"Time to turn columnar instance off (seconds): {turn_off_time:.2f}")
            turn_off_times.append(turn_off_time)

            logger.info(f"Waiting {off_duration} seconds before turning columnar instance on.")
            time.sleep(off_duration)

            turn_on_time = self.turn_on()
            logger.info(f"Time to turn columnar instance on (seconds): {turn_on_time:.2f}")
            turn_on_times.append(turn_on_time)

        for times, action in [(turn_off_times, "off"), (turn_on_times, "on")]:
            logger.info(f"All times (seconds) to turn columnar instance {action}: {times}")
            if times:
                logger.info(
                    f"Average time to turn columnar instance {action} (seconds): "
                    f"{sum(times) / len(times):.2f}"
                )


class CH2CapellaColumnarUnlimitedStorageTest(
    CH2ColumnarStandaloneDatasetTest, CapellaColumnarManualOnOffTest
):
    def run(self):
        self.setup()

        if not self.test_config.columnar_settings.unlimited_storage_skip_baseline:
            super().benchmark()
        else:
            self.report_columnar_cloud_storage_stats()

        self.instance_id = self.cluster_spec.controlplane_settings["columnar_ids"].split()[0]

        turn_off_time = self.turn_off()
        logger.info(f"Time to turn columnar instance off (seconds): {turn_off_time:.2f}")
        turn_on_time = self.turn_on()
        logger.info(f"Time to turn columnar instance on (seconds): {turn_on_time:.2f}")

        self.cluster.wait_until_healthy()

        if self.test_config.columnar_settings.debug_sweep_threshold_enabled:
            # enable debug sweep threshold
            self.rest.set_analytics_config_settings(
                self.analytics_node,
                "service",
                {
                    "cloudStorageDebugModeEnabled": True,
                    "cloudStorageDebugSweepThresholdSize": (
                        self.test_config.columnar_settings.sweep_threshold_bytes
                    ),
                },
            )
            self.rest.restart_analytics_cluster(self.analytics_node)
            self.cluster.wait_until_healthy(polling_interval_secs=10, max_retries=120)

        if self.test_config.analytics_settings.use_cbo:
            self.analyze_datasets(self.test_config.analytics_settings.cbo_sample_size, verbose=True)

        new_sf_title = f"{self.test_config.showfast.title}, POST-RESUME"
        self.test_config.config["showfast"]["title"] = new_sf_title
        self.test_config.update_spec_file()

        log_file = f"{self.test_config.ch2_settings.workload}_post_resume"
        ch2_settings = self.test_config.ch2_settings
        ch2_settings.warmup_iterations = 0
        ch2_settings.iterations = 1
        self.run_ch2(log_file=log_file, ch2_settings=ch2_settings)

        self.report_kpi(log_file=log_file, extra_metric_id_suffix="post_resume")


class CH2ColumnarKafkaLinksIngestionTest(CH2Test):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.kafka_links_settings = self.test_config.columnar_kafka_links_settings
        self.docs_per_collection = {}
        self.kafka_link_connected = False

        source = self.kafka_links_settings.link_source
        self.source_details = {"source": source}

        if source == "MONGODB":
            self.docs_per_collection = {
                collection: self.count_collection_docs_mongodb(collection)
                for collection in self.dataset_names
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

        if self.is_capella_columnar:
            self.COLLECTORS = {'ns_server': False, 'active_tasks': False, 'analytics': True}

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

        self.exec_analytics_statement(self.analytics_node, statement, verbose=True)

    def create_datasets_at_link(self):
        logger.info('Creating standalone datasets')

        for dataset in self.dataset_names:
            statement = "CREATE DATASET `{0}` PRIMARY KEY (`{1}`: string) ON {2}.{0} AT `{3}`;" \
                .format(dataset, self.kafka_links_settings.primary_key_field,
                        self.kafka_links_settings.remote_database_name, self.analytics_link)
            self.exec_analytics_statement(self.analytics_node, statement, verbose=True)

    @with_stats
    def sync(self) -> float:
        """Set up data ingestion and return time taken to ingest all data (excluding setup time)."""
        self.create_datasets_at_link()
        self.connect_link()

        t0 = time.time()
        self.monitor.monitor_cbas_kafka_link_connect_status(self.analytics_node,
                                                            self.analytics_link)
        link_connect_time = time.time() - t0
        self.kafka_link_connected = True
        logger.info('Link connection time: {}s'.format(link_connect_time))

        self.monitor.monitor_cbas_kafka_link_data_ingestion_status(
            self.analytics_node,
            self.docs_per_collection,
            timeout_mins=self.test_config.columnar_kafka_links_settings.ingestion_timeout_mins,
        )
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
                if not self.is_capella_columnar:
                    logger.info('Getting Connector ARNs to be able to look up logs')
                    if not self.cluster.get_msk_connect_connector_arns():
                        logger.warn('Failed to get Kafka Connect connector ARNs')
                self.disconnect_link()


class CH2ColumnarStandaloneDatasetTruncateTest(CH2ColumnarStandaloneDatasetTest):
    def copy_data_from_object_store(self, datasets: list[DatasetDef] = []) -> tuple[int, float]:
        return AnalyticsTest.copy_data_from_object_store(self, datasets)

    def ingest_dataset(self, dataset: DatasetDef):
        _, ingest_time = self.copy_data_from_object_store([dataset])
        self.timings["ingest"][dataset.name].append(ingest_time)

    def empty_dataset(
        self,
        dataset: DatasetDef,
        statements: list[str],
        op: Literal["truncate", "delete", "recreate"],
    ):
        t0 = time.time()
        for statement in statements:
            st0 = time.time()
            self.exec_analytics_statement(self.analytics_node, statement, verbose=True)
            logger.info(f"Statement execution time (s): {time.time() - st0:.2f}")
        empty_time = time.time() - t0
        self.timings[op][dataset.name].append(empty_time)

        num_items = self.get_dataset_items(dataset.name)
        if num_items != 0:
            logger.interrupt(
                f"Failed to empty dataset {dataset.name} ({op}): {num_items} items left."
            )

    def truncate_dataset(self, dataset: DatasetDef):
        statement = f"TRUNCATE DATASET {sqlpp_escape(dataset.name)}"
        self.empty_dataset(dataset, [statement], "truncate")

    def delete_from_dataset(self, dataset: DatasetDef):
        statement = f"DELETE FROM {sqlpp_escape(dataset.name)}"
        self.empty_dataset(dataset, [statement], "delete")

    def recreate_dataset(self, dataset: DatasetDef):
        statements = [
            f"DROP DATASET {sqlpp_escape(dataset.name)}",
            dataset.create_standalone_statement(),
        ]
        self.empty_dataset(dataset, statements, "recreate")

    @with_stats
    def access(self, datasets: list[DatasetDef]):
        for _ in range(3):
            for d in datasets:
                self.ingest_dataset(d)
                self.truncate_dataset(d)

                self.ingest_dataset(d)
                self.delete_from_dataset(d)

                self.ingest_dataset(d)
                self.recreate_dataset(d)

        logger.info(f"Raw timings: {pretty_dict(self.timings)}")

    def summarize_timings(self):
        summary_timings = {
            op: {
                dname: {"mean": np.mean(values), "std": np.std(values)}
                for dname, values in op_timings.items()
            }
            for op, op_timings in self.timings.items()
        }
        logger.info(f"Summarized timings: {pretty_dict(summary_timings)}")

    def run(self):
        self.create_external_link()
        self.create_standalone_datasets(verbose=True)

        self.create_analytics_indexes()

        # If object_store_import_datasets is empty, all pre-defined datasets will be imported
        datasets_to_import = [
            d
            for d in self.datasets
            if d.name in self.test_config.columnar_settings.object_store_import_datasets
        ]

        self.timings = {
            op: {d.name: [] for d in datasets_to_import}
            for op in ["ingest", "truncate", "delete", "recreate"]
        }

        self.access(datasets_to_import)
        self.summarize_timings()


class CH2CapellaColumnarRemoteLinkTruncateTest(
    CH2CapellaColumnarRemoteLinkTest, CH2ColumnarStandaloneDatasetTruncateTest
):
    @property
    def datasets(self) -> list[DatasetDef]:
        return CH2CapellaColumnarRemoteLinkTest.datasets.fget(self)

    def empty_dataset(
        self,
        dataset: DatasetDef,
        statements: list[str],
        op: Literal["truncate", "recreate"],
    ):
        bucket, scope, coll = dataset.source.split(".")

        t0 = time.time()
        for statement in statements:
            st0 = time.time()
            self.exec_analytics_statement(self.analytics_node, statement, verbose=True)
            logger.info(f"Statement execution time (s): {time.time() - st0:.2f}")
        empty_time = time.time() - t0
        self.timings[op][dataset.name].append(empty_time)

        self.monitor.monitor_data_synced(
            self.data_node,
            bucket,
            self.test_config.bucket.replica_number,
            self.analytics_node,
            self.test_config.access_settings.sql_suite,
            scope,
            coll,
        )
        self.timings["ingest"][dataset.name].append(time.time() - t0)

    def recreate_dataset(self, dataset: DatasetDef):
        statements = [
            f"DROP DATASET {sqlpp_escape(dataset.name)}",
            dataset.create_at_link_statement(self.analytics_link, self.storage_format),
        ]
        self.empty_dataset(dataset, statements, "recreate")

    @with_stats
    def access(self):
        for _ in range(3):
            for d in self.datasets:
                self.truncate_dataset(d)
                self.recreate_dataset(d)

        logger.info(f"Raw timings: {pretty_dict(self.timings)}")

    def run(self):
        instance_id = self.rest.instance_ids[0]
        self.rest.create_capella_remote_link(
            instance_id, self.analytics_link, self.cluster_spec.capella_cluster_ids[0]
        )
        self.monitor.wait_for_columnar_remote_link_ready(
            instance_id, self.analytics_link, timeout_secs=1200
        )
        self.disconnect_link()

        if self.test_config.analytics_settings.ingest_during_load:
            self.create_datasets_at_link()
            self.create_analytics_indexes()
            self.connect_link()

        if self.test_config.ch2_settings.use_backup:
            self.restore_data()
        else:
            self.init_ch2_repo()
            self.load_ch2()

        if not self.test_config.analytics_settings.ingest_during_load:
            # Only wait for the KV cluster
            self.wait_for_persistence()
            sync_time = self.sync()
            self.report_sync_kpi(sync_time)
        else:
            bucket_replica = self.test_config.bucket.replica_number
            sql_suite = self.test_config.access_settings.sql_suite
            for bucket in self.test_config.buckets:
                self.num_items += self.monitor.monitor_data_synced(
                    self.data_node, bucket, bucket_replica, self.analytics_node, sql_suite
                )

        self.timings = {
            op: {d.name: [] for d in self.datasets} for op in ["ingest", "truncate", "recreate"]
        }

        self.access()
        self.summarize_timings()


class CH3Test(CH2Test):
    SCOPE = "bench.ch3"

    GSI_INDEXES = [
        ("cu_w_id_d_id_last", "customer", ("c_w_id", "c_d_id", "c_last")),
        ("di_id_w_id", "district", ("d_id", "d_w_id")),
        ("no_o_id_d_id_w_id", "neworder", ("no_o_id", "no_d_id", "no_w_id")),
        ("or_id_d_id_w_id_c_id", "orders", ("o_id", "o_d_id", "o_w_id, o_c_id")),
        ("or_w_id_d_id_c_id", "orders", ("o_w_id", "o_d_id", "o_c_id")),
        ("wh_id", "warehouse", ("w_id",)),
    ]

    FTS_INDEXES = [
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

    @property
    def fts_node(self) -> str:
        return self.fts_nodes[0]

    def _report_kpi(self):
        measure_time = (
            self.test_config.ch3_settings.duration - self.test_config.ch3_settings.warmup_duration
        )
        ch3_metrics = self.metrics.ch3_metrics(
            duration=measure_time, logfile=self.test_config.ch3_settings.workload
        )

        self.reporter.post(
            *self.metrics.ch2_tpm(round(ch3_metrics.tpm, 2), self.test_config.ch3_settings.tclients)
        )

        self.reporter.post(
            *self.metrics.ch2_response_time(
                round(ch3_metrics.txn_response_time, 2), self.test_config.ch3_settings.tclients
            )
        )

        if self.test_config.ch3_settings.workload == 'ch3_mixed':
            self.reporter.post(
                *self.metrics.ch2_analytics_query_set_time(
                    ch3_metrics.average_cbas_query_set_time, self.test_config.ch3_settings.tclients
                )
            )

            self.reporter.post(
                *self.metrics.ch3_fts_query_time(
                    round(ch3_metrics.average_fts_query_set_time_ms / 1000, 2),
                    self.test_config.ch3_settings.tclients,
                )
            )

            self.reporter.post(
                *self.metrics.ch3_fts_client_time(
                    round(ch3_metrics.average_fts_client_time_ms / 1000, 2),
                    self.test_config.ch3_settings.tclients,
                )
            )

            self.reporter.post(
                *self.metrics.ch3_fts_qph(
                    ch3_metrics.fts_qph, self.test_config.ch3_settings.tclients
                )
            )


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
        for index_name in self.FTS_INDEXES:
            self.monitor.monitor_fts_index_persistence(
                hosts=self.fts_nodes, index=index_name, bucket=self.test_config.buckets[0]
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
        self.create_gsi_indexes()
        self.wait_for_indexing()
        self.create_fts_indexes()
        self.wait_for_fts_index_persistence()

        self.run_ch3()
        if self.test_config.ch3_settings.workload != 'ch3_analytics':
            self.report_kpi()


class ScanTest(AnalyticsTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.num_items = 0
        self.base_path = "file:///data2/backup/analytics/STEPS/"

    def import_tables(self):
        for table in ["R", "S", "T", "U", "V"]:
            import_file = f'{self.base_path}{table}.tbl'
            local.cbimport(
                master_node=self.master_node,
                cluster_spec=self.cluster_spec,
                bucket=table,
                data_type='csv',
                data_format='',
                import_file=import_file,
                scope_collection_exp='',
                generate_key='key::%rand%',
                threads=16,
                field_separator='"|"',
                infer_types=True
            )

        table_key_map = [
            ("region", "r_regionkey"), ("nation", "n_nationkey"), ("supplier", "s_suppkey"),
            ("customer", "c_custkey"), ("part", "p_partkey"),
            ("partsupp", "ps_partkey%:%ps_suppkey"),
            ("orders", "o_orderkey"), ("lineitem", "l_orderkey%:%l_linenumber")
        ]

        for mapping in table_key_map:
            import_file = f'{self.base_path}TPCH/{mapping[0]}.tbl'
            generate_key = f'key::%{mapping[1]}%'
            local.cbimport(
                master_node=self.master_node,
                cluster_spec=self.cluster_spec,
                bucket=mapping[0],
                data_type='csv',
                data_format='',
                import_file=import_file,
                scope_collection_exp='',
                generate_key=generate_key,
                threads=16,
                field_separator='"|"',
                infer_types=True
            )

    def create_and_analyze_datasets(self):
        for script in ["cr_datasets", "cr_indexesRSTUV", "analyze"]:
            script_file = f'{self.base_path.replace("file://", "")}{script}.sql'
            local.cbq(
                analytics_node=self.analytics_node,
                cluster_spec=self.cluster_spec,
                script=script_file
            )

    def _report_kpi(self, time_taken):
        sql_suite = self.test_config.access_settings.sql_suite
        self.reporter.post(
            *self.metrics.analytics_time_taken(time_taken, sql_suite)
        )

    @with_stats
    @timeit
    def all_operations(self):
        sql_suite = self.test_config.access_settings.sql_suite
        path = f"/data2/backup/analytics/SQL/{sql_suite}.sql"
        logger.info("Executing {}.sql...".format(sql_suite))
        local.cbq(
            analytics_node=self.analytics_node,
            cluster_spec=self.cluster_spec,
            script=path
        )

    def sync(self):
        self.disconnect_link()
        self.create_and_analyze_datasets()
        self.connect_link()
        bucket_replica = self.test_config.bucket.replica_number
        sql_suite = self.test_config.access_settings.sql_suite
        for bucket in self.test_config.buckets:
            self.num_items += self.monitor.monitor_data_synced(
                self.data_node, bucket, bucket_replica, self.analytics_node, sql_suite
            )

    def run(self):
        self.restore_local()
        self.import_tables()
        self.sync()
        time_taken = self.all_operations()
        self.report_kpi(time_taken)

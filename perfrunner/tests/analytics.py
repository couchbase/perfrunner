import copy
import itertools
import json
import os
import random
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import (
    Any,
    Callable,
    Iterable,
    Literal,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
)
from urllib.parse import urlparse

import numpy as np
import requests
import yaml
from celery import group
from requests_toolbelt.adapters import socket_options

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import (
    get_azure_storage_account_key,
    get_cloud_storage_bucket_stats,
    pretty_dict,
    remove_nulls,
)
from perfrunner.helpers.rest import (
    ANALYTICS_PORT,
    ANALYTICS_PORT_SSL,
    FTS_PORT,
    FTS_PORT_SSL,
    QUERY_PORT,
    QUERY_PORT_SSL,
)
from perfrunner.helpers.server import ServerInfoManager
from perfrunner.helpers.worker import ch2_load, tpcds_initial_data_load_task
from perfrunner.settings import (
    CH2,
    AnalyticsCBOSampleSize,
    AnalyticsExternalFileFormat,
    AnalyticsExternalTableFormat,
    CH2ConnectionSettings,
    ColumnarSettings,
    TestConfig,
)
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import (
    CapellaRebalanceKVTest,
    DynamicServiceRebalanceTest,
    RebalanceTest,
)
from perfrunner.tests.xdcr import SrcTargetIterator
from perfrunner.workloads.bigfun.driver import bigfun
from perfrunner.workloads.bigfun.query_gen import Query
from perfrunner.workloads.tpcdsfun.driver import tpcds

QueryLatencyPair = tuple[Query, int]


class DatasetType(Enum):
    REMOTE = "remote"
    STANDALONE = "standalone"
    EXTERNAL = "external"
    KAFKA = "kafka"

    def is_streaming(self) -> bool:
        return self in [DatasetType.REMOTE, DatasetType.KAFKA]

    def needs_ingest(self) -> bool:
        return self.is_streaming() or self is DatasetType.STANDALONE


def sqlpp_escape(
    identifiers: Union[str, list[str]], delimiter: Optional[str] = "."
) -> Union[str, tuple[str, ...]]:
    """Return identifier(s) escaped with backticks for use in SQL++ queries.

    If provided, `delimiter` characters are treated as bucket/scope/collection separators.
    """
    if str_identifier := isinstance(identifiers, str):
        identifiers = [identifiers]

    result = tuple(
        (delimiter or "").join(f"`{i.strip('`')}`" for i in identifier.split(delimiter or None))
        for identifier in identifiers
    )
    return result[0] if str_identifier else result


@runtime_checkable
class ClusterLinkOps(Protocol):
    """The cluster-side operations LinkManager needs to realize analytics links.

    This is the narrow, explicit contract for the operations that actually touch
    the cluster (REST calls, SQL++ statements, credentials).
    """

    def create_remote_link(self, link: "RemoteLink") -> None: ...

    def create_kafka_link(self, link: "KafkaLink") -> None: ...

    def create_external_link(self, link: "ExternalLink") -> None: ...

    def connect_link(self, link_name: str) -> None: ...

    def disconnect_link(self, link_name: str) -> None: ...


@dataclass(frozen=True)
class Link:
    """A first-class analytics link.

    A pure value object: it knows its identity, type and how to describe its own
    creation (via an injected ClusterLinkOps), but performs no cluster access
    itself.
    """

    name: str

    @property
    def dataset_type(self) -> DatasetType:
        raise NotImplementedError("Subclasses must implement this property")

    def is_streaming(self) -> bool:
        return self.dataset_type.is_streaming()

    def disconnect_after_create(self) -> bool:
        # Streaming links auto-connect on creation, so they are disconnected right
        # after so that ingest can connect them explicitly and in a controlled order.
        # Non-streaming (external) links have no connection lifecycle.
        return self.is_streaming()

    def create(self, ops: ClusterLinkOps) -> None:
        raise NotImplementedError("Subclasses must implement this method")


@dataclass(frozen=True)
class RemoteLink(Link):
    @property
    def dataset_type(self) -> DatasetType:
        return DatasetType.REMOTE

    def create(self, ops: ClusterLinkOps) -> None:
        # "Local" is the built-in link and must never be (re)created.
        if self.name != "Local":
            ops.create_remote_link(self)


@dataclass(frozen=True)
class KafkaLink(Link):
    link_source: str
    # Optional: source types added in future (i.e. non-MongoDB) won't have a Mongo URI.
    mongodb_uri: Optional[str] = None

    @property
    def dataset_type(self) -> DatasetType:
        return DatasetType.KAFKA

    def create(self, ops: ClusterLinkOps) -> None:
        ops.create_kafka_link(self)


@dataclass(frozen=True)
class ExternalLink(Link):
    # EXTERNAL and STANDALONE datasets both use an external link.
    link_type: str
    # Optional: only the s3 link type requires a region (enforced in __post_init__).
    region: Optional[str] = None
    # Optional: only used by the azureblob link type.
    azure_storage_account: Optional[str] = None

    def __post_init__(self):
        if self.link_type == "s3":
            assert self.region, "s3 external link requires a region"
        if self.link_type == "azureblob":
            assert self.azure_storage_account, (
                "azureblob external link requires an Azure storage account"
            )

    @property
    def dataset_type(self) -> DatasetType:
        return DatasetType.EXTERNAL

    def create(self, ops: ClusterLinkOps) -> None:
        ops.create_external_link(self)


@dataclass(frozen=True)
class BaseDatasetDef:
    name: str

    def create_primary_idx_statement(self) -> str:
        return f"CREATE PRIMARY INDEX ON {sqlpp_escape(self.name)}"

    def drop_primary_idx_statement(self) -> str:
        return f"DROP INDEX {self.name}.primary_idx_{self.name}"

    def analyze_statement(
        self,
        sample_size: AnalyticsCBOSampleSize,
        sample_seed: int,
    ) -> str:
        with_clause_options = {"sample-seed": sample_seed}
        if sample_size is not AnalyticsCBOSampleSize.DEFAULT:
            with_clause_options["sample"] = sample_size.value
        return (
            f"ANALYZE ANALYTICS COLLECTION {sqlpp_escape(self.name)} "
            f"WITH {json.dumps(with_clause_options)}"
        )

    def get_type(self) -> DatasetType:
        raise NotImplementedError("Subclasses must implement this method")

    def is_streaming(self) -> bool:
        return self.get_type().is_streaming()

    def needs_ingest(self) -> bool:
        return self.get_type().needs_ingest()

    def create_statement(self) -> str:
        raise NotImplementedError("Subclasses must implement this method")


@dataclass(frozen=True)
class RemoteDatasetDef(BaseDatasetDef):
    source_bucket: str
    source_scope: str
    source_collection: str
    link: RemoteLink
    storage_format: Optional[str] = None
    where_clause: Optional[str] = None
    transform_func: Optional[str] = None

    @property
    def link_name(self) -> str:
        return self.link.name

    def get_type(self) -> DatasetType:
        return DatasetType.REMOTE

    @property
    def fully_qualified_source(self) -> str:
        return f"{self.source_bucket}.{self.source_scope}.{self.source_collection}"

    @property
    def with_clause_options(self) -> dict[str, Any]:
        options = {}
        if self.storage_format:
            options["storage-format"] = {"format": self.storage_format}
        return options

    def create_statement(self) -> str:
        name, source, link_name = sqlpp_escape(
            [self.name, self.fully_qualified_source, self.link_name]
        )

        with_clause = ""
        if opts := self.with_clause_options:
            with_clause = f" WITH {json.dumps(opts)}"

        # these shouldn't both be defined, but if they are then 'where' takes precedence.
        where_clause = f" WHERE {self.where_clause}" if self.where_clause else ""
        transform_clause = (
            f" APPLY FUNCTION {sqlpp_escape(self.transform_func)}" if self.transform_func else ""
        )

        return f"CREATE DATASET {name}{with_clause} ON {source} AT {link_name}" + (
            where_clause or transform_clause
        )


@dataclass(frozen=True)
class KafkaDatasetDef(BaseDatasetDef):
    source_topics: list[str]
    link: KafkaLink
    primary_key_field_names: list[str]
    primary_key_field_types: list[str]
    storage_format: Optional[str] = None
    where_clause: Optional[str] = None
    transform_func: Optional[str] = None

    def __post_init__(self):
        assert len(self.primary_key_field_names) == len(self.primary_key_field_types)

    @property
    def link_name(self) -> str:
        return self.link.name

    def get_type(self) -> DatasetType:
        return DatasetType.KAFKA

    @property
    def with_clause_options(self) -> dict[str, Any]:
        options = {}
        if self.storage_format:
            options["storage-format"] = {"format": self.storage_format}
        return options

    def create_statement(self) -> str:
        name, link_name = sqlpp_escape([self.name, self.link_name])
        topics = sqlpp_escape(self.source_topics, delimiter=None)
        pk_names = sqlpp_escape(self.primary_key_field_names)
        pk_types = sqlpp_escape(self.primary_key_field_types)

        field_list = [f"{name}: {t}" for name, t in zip(pk_names, pk_types)]

        with_clause = ""
        if opts := self.with_clause_options:
            with_clause = f" WITH {json.dumps(opts)}"

        # these shouldn't both be defined, but if they are then 'where' takes precedence.
        where_clause = f" WHERE {self.where_clause}" if self.where_clause else ""
        transform_clause = (
            f" APPLY FUNCTION {sqlpp_escape(self.transform_func)}" if self.transform_func else ""
        )

        return (
            f"CREATE DATASET {name}{with_clause} PRIMARY KEY ({', '.join(field_list)}) ON "
            f"{', '.join(topics)} AT {link_name}{where_clause or transform_clause}"
        )


@dataclass(frozen=True)
class StandaloneDatasetDef(BaseDatasetDef):
    primary_key_field_names: list[str]
    primary_key_field_types: list[str]
    storage_format: Optional[str] = None
    obj_store_name: Optional[str] = None
    link: Optional[ExternalLink] = None
    object_store_path: Optional[str] = None

    def __post_init__(self):
        assert len(self.primary_key_field_names) == len(self.primary_key_field_types)

    @property
    def link_name(self) -> Optional[str]:
        return self.link.name if self.link else None

    def get_type(self) -> DatasetType:
        return DatasetType.STANDALONE

    @property
    def with_clause_options(self) -> dict[str, Any]:
        options = {}
        if self.storage_format:
            options["storage-format"] = {"format": self.storage_format}
        return options

    def create_statement(self) -> str:
        name = sqlpp_escape(self.name)
        pk_names = sqlpp_escape(self.primary_key_field_names)
        pk_types = sqlpp_escape(self.primary_key_field_types)

        field_list = [f"{name}: {t}" for name, t in zip(pk_names, pk_types)]
        autogenerate_pk = (
            len(self.primary_key_field_types) == 1
            and self.primary_key_field_types[0].lower() == "uuid"
        )
        field_list_str = f"({', '.join(field_list)})" + (
            " AUTOGENERATED" if autogenerate_pk else ""
        )

        with_clause = ""
        if opts := self.with_clause_options:
            with_clause = f" WITH {json.dumps(opts)}"

        return f"CREATE DATASET {name} PRIMARY KEY {field_list_str}{with_clause}"

    def copy_into_statement(
        self,
        obj_store_name: Optional[str] = None,
        link_name: Optional[str] = None,
        path: Optional[str] = None,
        file_format: AnalyticsExternalFileFormat = AnalyticsExternalFileFormat.DEFAULT,
        include: Optional[list[str]] = None,
    ) -> str:
        obj_store_name = obj_store_name or self.obj_store_name
        link_name = link_name or self.link_name

        assert obj_store_name is not None, (
            "obj_store_name must be specified for COPY INTO statement"
        )
        assert link_name is not None, "link_name must be specified for COPY INTO statement"

        name, obj_store_name, link_name = sqlpp_escape([self.name, obj_store_name, link_name])

        with_clause_options = {}
        if file_format is not AnalyticsExternalFileFormat.DEFAULT:
            with_clause_options["format"] = file_format.value
        if include:
            with_clause_options["include"] = include

        with_clause = ""
        if opts := with_clause_options:
            with_clause = f" WITH {json.dumps(opts)}"

        return (
            f"COPY INTO {name} FROM {obj_store_name} AT {link_name} "
            f"PATH '{path or self.object_store_path or self.name}'{with_clause}"
        )


@dataclass(frozen=True)
class ExternalDatasetDef(BaseDatasetDef):
    obj_store_name: str
    link: ExternalLink
    path: Optional[str] = None
    file_format: AnalyticsExternalFileFormat = AnalyticsExternalFileFormat.DEFAULT
    table_format: AnalyticsExternalTableFormat = AnalyticsExternalTableFormat.DEFAULT
    include: list[str] = field(default_factory=list)

    @property
    def link_name(self) -> str:
        return self.link.name

    def get_type(self) -> DatasetType:
        return DatasetType.EXTERNAL

    @property
    def with_clause_options(self) -> dict[str, Any]:
        options = {}
        if self.file_format is not AnalyticsExternalFileFormat.DEFAULT:
            options["format"] = self.file_format.value
        if self.table_format is not AnalyticsExternalTableFormat.DEFAULT:
            options["table-format"] = self.table_format.value
        if self.include:
            options["include"] = self.include
        return options

    def create_statement(self) -> str:
        name, obj_store_name, link_name = sqlpp_escape(
            [self.name, self.obj_store_name, self.link_name]
        )

        with_clause = ""
        if opts := self.with_clause_options:
            with_clause = f" WITH {json.dumps(opts)}"

        return (
            f"CREATE EXTERNAL DATASET {name} ON {obj_store_name} AT {link_name} "
            f"USING '{self.path or self.name}'{with_clause}"
        )


@dataclass(frozen=True)
class IndexDef:
    name: str
    collection: str
    elements: tuple[str]  # index fields or array index elements

    # "INCLUDE", "EXCLUDE" or None to omit.
    # Determines whether missing or null values are included in the index.
    unknown_modifier: Optional[str] = None

    def create_statement(self) -> str:
        name, collection = sqlpp_escape([self.name, self.collection])
        unknown = ""
        if self.unknown_modifier:
            unknown = f" {self.unknown_modifier} UNKNOWN KEY"
        return f"CREATE INDEX {name} ON {collection}({', '.join(self.elements)}){unknown}"


class LinkSet:
    """A queryable, de-duplicated collection of analytics links.

    Built once from the configured datasets (datasets drive which links are
    needed), then queried.
    """

    def __init__(self, links: Iterable[Link] = ()):
        self._by_name: dict[str, Link] = {}
        for link in links:
            self.add(link)

    def add(self, link: Link) -> Link:
        """Register a link, returning the existing one if the name is taken."""
        existing = self._by_name.setdefault(link.name, link)
        assert existing == link, (
            f"Conflicting configuration for link {link.name!r}: {existing} != {link}"
        )
        return existing

    def get(self, name: str) -> Optional[Link]:
        return self._by_name.get(name)

    def all(self) -> list[Link]:
        return list(self._by_name.values())

    def streaming(self) -> list[Link]:
        return [link for link in self.all() if link.is_streaming()]


class LinkManager:
    """Owns the LinkSet, tracks link connection state, and realizes links.

    All cluster access is delegated to the injected ClusterLinkOps, so the
    manager depends on that narrow contract rather than on AnalyticsTest.
    """

    def __init__(self, ops: ClusterLinkOps, links: LinkSet):
        self._ops = ops
        self.links = links
        self._connected: set[str] = set()

    def create_all(self):
        """Create every link derived from the configured datasets."""
        for link in self.links.all():
            link.create(self._ops)
            if link.disconnect_after_create():
                self.disconnect(link.name)

    def ensure(self, link: Link, *, connect: bool = False):
        """Create a link if it isn't already known, optionally connecting it.

        Used for links that aren't derived from a dataset (e.g. a COPY TO
        target) or that may or may not already exist.
        """
        if self.links.get(link.name) is None:
            self.links.add(link)
            link.create(self._ops)
        if connect:
            self.connect(link.name)

    def connect(self, name: str):
        if name in self._connected:
            return
        self._ops.connect_link(name)
        self._connected.add(name)

    def disconnect(self, name: str):
        self._ops.disconnect_link(name)
        self._connected.discard(name)

    def connect_all(self, names: Iterable[str]):
        for name in names:
            self.connect(name)

    def disconnect_all(self, names: Iterable[str]):
        for name in names:
            self.disconnect(name)

    def connect_streaming(self):
        self.connect_all(link.name for link in self.links.streaming())

    def disconnect_streaming(self):
        self.disconnect_all(link.name for link in self.links.streaming())

    def is_connected(self, name: str) -> bool:
        return name in self._connected


class DatasetConfigParser:
    """Builds dataset definitions from a dataset config file and test settings.

    Pure with respect to the cluster: it depends only on configuration, so it can
    be constructed and unit-tested without a running test/cluster.
    """

    def __init__(self, test_config: TestConfig):
        self.test_config = test_config
        self.analytics_settings = test_config.analytics_settings
        self.ext_data_settings = test_config.analytics_external_data_settings

    def parse(
        self, dataset_conf_file: Optional[str] = None
    ) -> tuple[list[BaseDatasetDef], dict, LinkSet]:
        """Parse the config file, returning the dataset defs, raw config and links.

        The LinkSet is built as datasets are parsed, and each dataset holds a
        reference to its (de-duplicated) link in that set.
        """
        raw_config = {}
        if dataset_conf_file:
            with open(dataset_conf_file, "r") as f:
                raw_config = yaml.safe_load(f)

        family = raw_config.get("family")
        dataset_confs = raw_config.get("datasets", [])

        datasets: list[BaseDatasetDef] = []
        self.link_set = LinkSet()
        standalone_ds_overrides = {
            target: {"object_store_path": source}
            for source, target in self.test_config.columnar_settings.object_store_import_datasets
        }

        for dataset_conf in dataset_confs:
            repeats = dataset_conf.get("repeat", 1)
            for repeat in range(1, repeats + 1):
                ds_type = DatasetType(dataset_conf.get("type").lower())
                name = dataset_conf.get("name").format(repeat=repeat)

                if ds_type is DatasetType.REMOTE:
                    ds_def = self._create_remote_dataset_from_config(
                        name, dataset_conf, family, repeat
                    )
                elif ds_type is DatasetType.KAFKA:
                    ds_def = self._create_kafka_dataset_from_config(name, dataset_conf, repeat)
                elif ds_type is DatasetType.STANDALONE:
                    if standalone_ds_overrides:
                        if (override := standalone_ds_overrides.get(name)) is not None:
                            dataset_conf.update(override)
                        else:
                            continue
                    ds_def = self._create_standalone_dataset_from_config(name, dataset_conf)
                elif ds_type is DatasetType.EXTERNAL:
                    ds_def = self._create_external_dataset_from_config(name, dataset_conf)

                datasets.append(ds_def)

        return datasets, raw_config, self.link_set

    def _create_remote_dataset_from_config(
        self, name: str, config: dict, family: str, repeat: int
    ) -> RemoteDatasetDef:
        storage_format = config.get("storage_format", self.analytics_settings.storage_format)

        source = config["source"]
        source_bucket = source["bucket"]
        if family == "ch2":
            source_scope = self.test_config.ch2_settings.schema.value
            source_collection = name
        else:
            source_scope = source["scope"]
            source_collection = source["collection"]

        return RemoteDatasetDef(
            name=name,
            source_bucket=source_bucket,
            source_scope=source_scope,
            source_collection=source_collection.format(repeat=repeat),
            link=self.link_set.add(RemoteLink(self.analytics_settings.couchbase_link_name)),
            storage_format=storage_format,
            where_clause=config.get("where", "").format(repeat=repeat),
            transform_func=config.get("transform_func"),
        )

    def _create_kafka_dataset_from_config(
        self, name: str, config: dict, repeat: int
    ) -> KafkaDatasetDef:
        storage_format = config.get("storage_format", self.analytics_settings.storage_format)
        kafka_settings = self.test_config.columnar_kafka_links_settings

        return KafkaDatasetDef(
            name=name,
            source_topics=config.get(
                "source_topics", [f"{kafka_settings.remote_database_name}.{name}"]
            ),
            link=self.link_set.add(
                KafkaLink(
                    "kafka_link",
                    link_source=kafka_settings.link_source,
                    mongodb_uri=kafka_settings.mongodb_uri,
                )
            ),
            primary_key_field_names=config.get(
                "primary_key_field_names", [kafka_settings.primary_key_field]
            ),
            primary_key_field_types=config.get("primary_key_field_types", ["string"]),
            storage_format=storage_format,
            where_clause=config.get("where", "").format(repeat=repeat),
            transform_func=config.get("transform_func"),
        )

    def _external_link(self) -> ExternalLink:
        """Build (and register) the external link shared by external/standalone datasets."""
        return self.link_set.add(
            ExternalLink(
                self.ext_data_settings.external_link_name,
                link_type=self.ext_data_settings.link_type,
                region=self.ext_data_settings.region,
                azure_storage_account=self.ext_data_settings.azure_storage_account,
            )
        )

    def _create_standalone_dataset_from_config(
        self, name: str, config: dict
    ) -> StandaloneDatasetDef:
        storage_format = config.get("storage_format", self.analytics_settings.storage_format)

        return StandaloneDatasetDef(
            name=name,
            primary_key_field_names=config.get("primary_key_field_names", ["key"]),
            primary_key_field_types=config.get("primary_key_field_types", ["string"]),
            storage_format=storage_format,
            obj_store_name=self.ext_data_settings.obj_store_name,
            link=self._external_link(),
            object_store_path=config.get("object_store_path"),
        )

    def _create_external_dataset_from_config(self, name: str, config: dict) -> ExternalDatasetDef:
        return ExternalDatasetDef(
            name=name,
            obj_store_name=self.ext_data_settings.obj_store_name,
            link=self._external_link(),
            path=config.get("path"),
            file_format=config.get("file_format", self.ext_data_settings.file_format),
            table_format=config.get("table_format", self.ext_data_settings.table_format),
            include=config.get("include", self.ext_data_settings.file_include or []),
        )


class DatasetCollection:
    """A passive, queryable collection of dataset definitions and its raw config.

    Holds no cluster/test reference: it is built by DatasetConfigParser and queried
    by AnalyticsTest, which owns all cluster orchestration.
    """

    def __init__(self, datasets: list[BaseDatasetDef], raw_config: dict):
        self.datasets = datasets
        self.raw_config = raw_config

    @property
    def family(self) -> Optional[str]:
        return self.raw_config.get("family")

    def of_type(self, *types: DatasetType) -> list[BaseDatasetDef]:
        return [d for d in self.datasets if d.get_type() in types]

    def has_type(self, dataset_type: DatasetType) -> bool:
        return any(d.get_type() is dataset_type for d in self.datasets)

    def needs_ingest(self) -> bool:
        return any(d.needs_ingest() for d in self.datasets)

    def extend(self, datasets: Iterable[BaseDatasetDef]):
        self.datasets.extend(datasets)

    def replace(self, datasets: list[BaseDatasetDef]):
        self.datasets = datasets


def _count_collection_docs_mongodb(uri: str, db: str, collections: list[str]) -> dict[str, int]:
    from pymongo import MongoClient

    client = MongoClient(uri)
    db = client[db]
    return {coll: db[coll].estimated_document_count() for coll in collections}


class AnalyticsTest(PerfTest):
    COLLECTORS = {"analytics": True, "ns_server_system": True}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_iterator = SrcTargetIterator(self.cluster_spec, self.test_config)
        self.analytics_settings = self.test_config.analytics_settings
        self.ext_data_settings = self.test_config.analytics_external_data_settings
        self.couchbase_link_name = self.analytics_settings.couchbase_link_name
        self.storage_format = self.test_config.analytics_settings.storage_format
        self.rest_session = None
        self.have_already_restored_data = False

        self.index_configs = []
        if index_conf_file := self.analytics_settings.index_conf_file:
            with open(index_conf_file, "r") as f:
                self.index_configs = yaml.safe_load(f)

        analytics_node_version = (
            ServerInfoManager().get_server_info_by_master_node(self.analytics_cluster_master).build
        )
        if analytics_node_version != self.reporter.build:
            self.reporter.build = f"{analytics_node_version} : {self.reporter.build}"

        datasets, raw_config, links = DatasetConfigParser(self.test_config).parse(
            self.analytics_settings.dataset_conf_file
        )
        self.dataset_collection = DatasetCollection(datasets, raw_config)
        self.link_manager = LinkManager(self, links)

        self.kafka_links_settings = self.test_config.columnar_kafka_links_settings
        self.target_docs_per_kafka_coll = {}
        if self.dataset_collection.has_type(DatasetType.KAFKA):
            self.target_docs_per_kafka_coll = self.get_kafka_source_db_coll_counts()

    def __exit__(self, *args):
        if (
            ServerInfoManager().get_server_info(-1).is_columnar
            and self.cluster_spec.cloud_infrastructure
        ):
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
    def analytics_cluster_master(self) -> str:
        # If we have several clusters, we assume the second cluster to be the analytics cluster
        if len(masters := list(self.cluster_spec.masters)) > 1:
            return masters[1]

        return self.master_node

    @property
    def analytics_nodes(self) -> list[str]:
        return self.rest.get_active_nodes_by_role(self.analytics_cluster_master, "cbas")

    @property
    def analytics_node(self) -> str:
        return self.analytics_nodes[0]

    @property
    def datasets(self) -> list[BaseDatasetDef]:
        return self.dataset_collection.datasets

    @property
    def indexes(self) -> list[IndexDef]:
        indexes = []
        ds_repeats = {
            d["name"]: d.get("repeat", 1)
            for d in self.dataset_collection.raw_config.get("datasets", [])
        }
        ds_names = set(d.name for d in self.datasets)

        for idx in self.index_configs:
            idx_name = idx["name"]
            idx_ds = idx["dataset"]

            indexes.extend(
                IndexDef(
                    idx_name.format(repeat=r),
                    idx_ds.format(repeat=r),
                    tuple(idx["elements"]),
                    idx.get("unknown_modifier"),
                )
                for r in range(1, ds_repeats.get(idx_ds, 1) + 1)
                if idx_ds.format(repeat=r) in ds_names
            )

        return indexes

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
        self, host: str, statement: str, *, verbose: bool = False, with_retry: bool = True
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

        post_func = self.rest.session_post if with_retry else self.rest._session_post
        resp = post_func(self.long_query_session(), url=url, data={"statement": statement})

        if verbose:
            logger.info(f"Result: {resp}")

        return resp

    def restart_all_nodes(self):
        if self.capella_infra:
            return

        self.remote.stop_server()
        self.remote.drop_caches()
        self.remote.start_server()
        for bucket in self.test_config.buckets:
            self.monitor.monitor_warmup(self.memcached, self.data_node, bucket)
        self.monitor.monitor_analytics_node_active(self.analytics_node)

    def _run_statements(
        self,
        defs: list[Union[BaseDatasetDef, IndexDef]],
        get_statement: Callable[[Union[BaseDatasetDef, IndexDef]], str],
        *,
        verbose: bool = False,
    ):
        """Run analytics statements for each given dataset or index."""
        for def_ in defs:
            statement = get_statement(def_)
            self.exec_analytics_statement(self.analytics_node, statement, verbose=verbose)

    def create_analytics_indexes(self, *, verbose: bool = False):
        if not (indexes := self.indexes):
            logger.info("No analytics secondary indexes to create")
            return

        logger.info(f"Creating {len(indexes)} analytics secondary indexes")
        self._run_statements(indexes, lambda index: index.create_statement(), verbose=verbose)

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
        sample_size: AnalyticsCBOSampleSize,
        sample_seed: int,
        *,
        verbose: bool = False,
    ):
        logger.info(
            f"Analyzing datasets for CBO using {sample_size.name.lower()} sample size "
            f"and sample seed {sample_seed}"
        )
        self._run_statements(
            self.datasets,
            lambda dataset: dataset.analyze_statement(sample_size, sample_seed),
            verbose=verbose,
        )

    # --- Dataset orchestration: drives the parsed DatasetCollection against the cluster ---

    def create_datasets(self, *, verbose: bool = False):
        logger.info(f"Creating {len(self.datasets)} datasets")
        for dataset in self.datasets:
            statement = dataset.create_statement()
            self.exec_analytics_statement(self.analytics_node, statement, verbose=verbose)

    def ingest_data(
        self, dataset_types: Optional[list[DatasetType]] = None
    ) -> dict[DatasetType, tuple[int, float]]:
        dispatcher = {
            DatasetType.REMOTE: self._ingest_remote_datasets,
            DatasetType.KAFKA: self._ingest_kafka_datasets,
            DatasetType.STANDALONE: self._ingest_standalone_datasets,
        }

        valid_ds_types = set(dataset_types or list(DatasetType)) & set(dispatcher.keys())

        grouped_datasets = {}
        for dataset in self.dataset_collection.of_type(*valid_ds_types):
            grouped_datasets.setdefault(dataset.get_type(), []).append(dataset)

        if not grouped_datasets:
            logger.warning("Tried to ingest data but no ingestible datasets found.")
            return {}

        assert len(grouped_datasets) == 1, "Ingesting multiple dataset types is not supported yet."

        ds_type, datasets = list(grouped_datasets.items())[0]
        timing_results = {ds_type: dispatcher[ds_type](datasets)}

        return timing_results

    @with_stats
    def sync(self):
        return self.ingest_data()

    @with_stats
    def resync(self):
        return self.ingest_data([DatasetType.REMOTE])

    def _ingest_remote_datasets(self, datasets: list[RemoteDatasetDef]) -> tuple[int, float]:
        self.link_manager.connect_all(ds.link_name for ds in datasets)

        t0 = time.time()
        if self.analytics_settings.ingest_during_load:
            self.load_kv_data()

        num_items = 0
        bucket_replica = self.test_config.bucket.replica_number
        for bucket in self.test_config.buckets:
            num_items += self.monitor.monitor_data_synced(
                self.data_node, bucket, bucket_replica, self.analytics_node
            )
        return num_items, time.time() - t0

    def _ingest_standalone_datasets(
        self, datasets: list[StandaloneDatasetDef]
    ) -> tuple[int, float]:
        return self.copy_data_from_object_store(datasets)

    def _ingest_kafka_datasets(self, datasets: list[KafkaDatasetDef]) -> tuple[int, float]:
        self.link_manager.connect_all(ds.link_name for ds in datasets)
        t0 = time.time()
        self.monitor.monitor_cbas_kafka_link_data_ingestion_status(
            self.analytics_node,
            self.target_docs_per_kafka_coll,
            timeout_mins=self.test_config.columnar_kafka_links_settings.ingestion_timeout_mins,
        )
        return sum(self.target_docs_per_kafka_coll.values()), time.time() - t0

    def log_ingestion_stats(self, ingestion_stats: dict[DatasetType, tuple[int, float]]):
        display_stats = {}
        for ds_type, (num_items, ingest_time) in ingestion_stats.items():
            if not ds_type.needs_ingest():
                continue

            display_stats[f"{ds_type.value} collections"] = {
                "Items ingested": num_items,
                "Ingest time (s)": round(ingest_time, 2),
                "Average ingest rate (items/s)": round(num_items / ingest_time, 2),
            }

        logger.info(f"Ingestion stats: {pretty_dict(display_stats)}")

    def initial_load_and_sync(
        self,
        *,
        create_indexes: bool = True,
        restart_after_kv_load: bool = False,
    ) -> dict[DatasetType, tuple[int, float]]:
        need_kv_load = self.dataset_collection.has_type(DatasetType.REMOTE)
        if need_kv_load and not self.analytics_settings.ingest_during_load:
            self.load_kv_data()
            self.wait_for_persistence()
            if restart_after_kv_load:
                self.restart_all_nodes()

        self.link_manager.create_all()
        self.create_datasets(verbose=len(self.datasets) <= 25)

        if create_indexes:
            self.create_analytics_indexes(verbose=len(self.indexes) <= 25)

        if not self.dataset_collection.needs_ingest():
            return {}

        stats = self.sync()

        self.log_ingestion_stats(stats)
        return stats

    def incremental_sync(self) -> dict[DatasetType, tuple[int, float]]:
        valid_datasets = self.dataset_collection.of_type(DatasetType.REMOTE)

        if not valid_datasets:
            return {}

        self.link_manager.disconnect_all(ds.link_name for ds in valid_datasets)
        self.load_kv_data()

        stats = self.resync()

        self.log_ingestion_stats(stats)
        return stats

    def _create_capella_remote_link(self, link_name: str):
        instance_id = self.rest.instance_ids[0]
        self.rest.create_capella_remote_link(
            instance_id, link_name, self.cluster_spec.capella_cluster_ids[0]
        )
        self.monitor.wait_for_columnar_remote_link_ready(instance_id, link_name, timeout_secs=1200)

    def create_remote_link(self, link: RemoteLink):
        if self.is_capella_columnar:
            self._create_capella_remote_link(link.name)
        else:
            self.rest.create_analytics_link(
                self.analytics_node, link.name, "couchbase", cb_data_node=self.data_node
            )

    def create_external_link(self, link: ExternalLink):
        link_type = link.link_type

        kwargs = {
            "analytics_node": self.analytics_node,
            "link_name": link.name,
            "link_type": link_type,
        }

        if link_type == "s3":
            access_key_id, secret_access_key = local.get_aws_credential(
                self.analytics_settings.aws_credential_path
            )
            kwargs |= {
                "s3_region": link.region,
                "s3_access_key_id": access_key_id,
                "s3_secret_access_key": secret_access_key,
            }
        elif link_type == "gcs":
            with open(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), "r") as f:
                kwargs["gcs_json_creds"] = json.load(f)
        elif link_type == "azureblob":
            storage_acc_name = (
                link.azure_storage_account or self.ext_data_settings.azure_storage_account
            )
            kwargs |= {
                "az_account_name": storage_acc_name,
                "az_account_key": get_azure_storage_account_key(storage_acc_name),
                "az_endpoint": f"https://{storage_acc_name}.blob.core.windows.net",
            }
        else:
            logger.interrupt(
                "Could not create external link. "
                f"Perfrunner doesn't support external link type: {link_type}"
            )

        self.rest.create_analytics_link(**kwargs)

    def create_kafka_link(self, link: KafkaLink):
        logger.info("Creating Kafka Link")
        statement = (
            f"CREATE LINK `{link.name}` TYPE KAFKA "
            f'WITH {{"sourceDetails": {json.dumps(self._kafka_source_details(link))}}}'
        )
        self.exec_analytics_statement(self.analytics_node, statement, verbose=True)

    def _kafka_source_details(self, link: KafkaLink) -> dict:
        source_details = {"source": link.link_source}

        if link.link_source == "MONGODB":
            source_details.update({"connectionFields": {"connectionUri": link.mongodb_uri}})
        else:
            logger.interrupt(f"Unsupported Kafka Link source type: {link.link_source}")

        return source_details

    def connect_link(self, link_name: str):
        logger.info(f"Connecting Link {link_name}")
        statement = f"CONNECT LINK {link_name}"
        self.exec_analytics_statement(self.analytics_node, statement)

    def disconnect_link(self, link_name: str):
        logger.info(f"Disconnecting Link {link_name}")
        statement = f"DISCONNECT LINK {link_name}"
        self.exec_analytics_statement(self.analytics_node, statement)

    def _restore_remote(self):
        if not self.have_already_restored_data:
            self.remote.extract_cb_any(
                filename="couchbase", worker_home=self.worker_manager.WORKER_HOME
            )
        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)

        archive = self.test_config.restore_settings.backup_storage

        if archive.startswith("s3://") and not self.have_already_restored_data:
            credential = local.read_aws_credential(
                self.test_config.backup_settings.aws_credential_path
            )
            self.remote.create_aws_credential(credential)

        self.remote.client_drop_caches()

        if self.test_config.restore_settings.use_csp_specific_archive:
            archive += f"/{self.cluster_spec.csp.lower()}"

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
            env_vars=self.test_config.restore_settings.env_vars,
        )

    def restore_data(self):
        if (wm := getattr(self, "worker_manager", None)) and wm.is_remote:
            self._restore_remote()
        else:
            self.restore_local(extract_archive=not self.have_already_restored_data)

        restore_include_data = self.test_config.restore_settings.include_data
        if restore_include_data:
            filtered_datasets = [
                ds
                for ds in self.dataset_collection.of_type(DatasetType.REMOTE)
                if ds.fully_qualified_source in restore_include_data.split(",")
            ]
            self.dataset_collection.replace(filtered_datasets)

        self.have_already_restored_data = True

    def _create_ch2_conn_settings(self) -> CH2ConnectionSettings:
        query_port = QUERY_PORT
        cbas_port = ANALYTICS_PORT

        use_tls = self.test_config.cluster.enable_n2n_encryption or self.is_capella_columnar
        if use_tls:
            query_port = QUERY_PORT_SSL
            cbas_port = ANALYTICS_PORT_SSL

        query_urls = [f"{node}:{query_port}" for node in self.query_nodes]

        user, pwd = self.cluster_spec.rest_credentials
        analytics_user, analytics_pwd = None, None
        if self.is_capella_columnar:
            user, pwd = self.cluster_spec.capella_admin_credentials[0]
            if len(self.cluster_spec.capella_admin_credentials) > 1:
                analytics_user, analytics_pwd = self.cluster_spec.capella_admin_credentials[1]

        return CH2ConnectionSettings(
            userid=user,
            password=pwd,
            userid_analytics=analytics_user,
            password_analytics=analytics_pwd,
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

    def init_ch2_repo(self):
        if getattr(self, (attr := "already_have_ch2_repo"), False):
            return

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

        setattr(self, attr, True)

    def download_tpcds_couchbase_loader(self):
        if getattr(self, (attr := "already_have_tpcds_loader"), False):
            return

        if self.worker_manager.is_remote:
            self.remote.init_tpcds_couchbase_loader(
                repo=self.test_config.tpcds_loader_settings.repo,
                branch=self.test_config.tpcds_loader_settings.branch,
                worker_home=self.worker_manager.WORKER_HOME,
            )
        else:
            local.init_tpcds_couchbase_loader(
                repo=self.test_config.tpcds_loader_settings.repo,
                branch=self.test_config.tpcds_loader_settings.branch,
            )

        setattr(self, attr, True)

    def load_tpcds(self):
        PerfTest.load(self, task=tpcds_initial_data_load_task)

    def load_kv_data(self):
        family = self.dataset_collection.family

        if family == "bigfun":
            self.restore_data()
        elif family == "ch2":
            if self.test_config.ch2_settings.use_backup:
                self.restore_data()
            else:
                self.init_ch2_repo()
                self.load_ch2()
        elif family == "tpcds":
            self.download_tpcds_couchbase_loader()
            self.load_tpcds()

    def copy_data_from_object_store(
        self, datasets: list[StandaloneDatasetDef]
    ) -> tuple[int, float]:
        """Ingest data from cloud object store using COPY FROM.

        Returns the total number of items copied and the total time taken to copy the data.
        """
        logger.info("Ingesting data from cloud object store using COPY FROM")

        total_items_copied = 0
        total_copy_time = 0
        for dataset in datasets:
            statement = dataset.copy_into_statement(
                file_format=self.ext_data_settings.file_format,
                include=self.ext_data_settings.file_include,
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
        get_cloud_storage_bucket_stats(
            f"{blob_storage_scheme}://{bucket_name}",
            az_storage_acc=self.cluster_spec.azure_storage_account,
        )

    def get_dataset_items(self, dataset: str) -> int:
        statement = f"SELECT COUNT(*) from {sqlpp_escape(dataset)};"
        result = self.exec_analytics_statement(self.analytics_node, statement)
        num_items = result.json()["results"][0]["$1"]
        logger.info(f"Number of items in dataset {dataset}: {num_items}")
        return num_items

    def monitor_cbas_pending_ops(self):
        t0 = time.time()
        self.monitor.monitor_cbas_pending_ops(self.analytics_nodes)
        logger.info(f"Time spent waiting to finish pending ops (s): {time.time() - t0:.2f}")

    def report_ingestion_kpi(
        self,
        ingestion_stats: dict[DatasetType, tuple[int, float]],
        streaming_ingest_type: str = "initial",
    ):
        if not self.test_config.stats_settings.enabled:
            return

        for ds_type, (num_items, ingest_time) in ingestion_stats.items():
            if ds_type in [DatasetType.REMOTE, DatasetType.KAFKA]:
                ingest_rate, snapshots, metric_info = self.metrics.avg_ingestion_rate(
                    num_items, ingest_time, streaming_ingest_type
                )
                metric_info["category"] = "sync"
                if self.test_config.showfast.component != "analyticscloud":
                    metric_info["subCategory"] = streaming_ingest_type.title()
            elif ds_type is DatasetType.STANDALONE:
                ingest_rate, snapshots, metric_info = self.metrics.avg_ingestion_rate(
                    num_items,
                    ingest_time,
                    f"copy_from_{self.ext_data_settings.link_type.lower()}",
                )
                metric_info["category"] = "sync"
            else:
                continue

            self.reporter.post(ingest_rate, snapshots, metric_info)

    def get_average_encoded_doc_size(self, dataset: str) -> float:
        logger.info(f"Getting average encoded document size in dataset {dataset}")
        limit = 1063 * 4  # sample size same as "high" sample size in Analytics CBO
        statement = (
            "SELECT AVG(sizes) FROM "
            f"(SELECT VALUE ENCODED_SIZE(x) FROM {sqlpp_escape(dataset)} x LIMIT {limit}) AS sizes;"
        )
        result = self.exec_analytics_statement(self.analytics_node, statement)
        avg_size = result.json()["results"][0]["$1"]
        logger.info(f"Average encoded document size in dataset {dataset} (bytes): {avg_size:.2f}")
        return avg_size

    def get_kafka_source_db_coll_counts(self) -> dict[str, int]:
        logger.info("Getting initial doc counts for Kafka source database collections.")

        coll_counts = {}
        if (source := self.kafka_links_settings.link_source) == "MONGODB":
            coll_counts = _count_collection_docs_mongodb(
                self.kafka_links_settings.mongodb_uri,
                self.kafka_links_settings.remote_database_name,
                [ds.name for ds in self.dataset_collection.of_type(DatasetType.KAFKA)],
            )
        else:
            logger.interrupt(f"Unsupported Kafka Link source type: {source}")

        logger.info(f"Docs per collection in Kafka source database: {pretty_dict(coll_counts)}")
        return coll_counts


class DropDatasetTest(AnalyticsTest):
    def _report_kpi(self, num_items, time_elapsed):
        self.reporter.post(*self.metrics.avg_drop_rate(num_items, time_elapsed))

    @with_stats
    @timeit
    def drop_dataset(self, drop_dataset: str):
        for target in self.target_iterator:
            self.rest.delete_collection(
                host=target.node, bucket=target.bucket, scope="scope-1", collection=drop_dataset
            )
        self.monitor.monitor_dataset_drop(self.analytics_node, drop_dataset)

    def run(self):
        self.initial_load_and_sync()

        drop_dataset = self.analytics_settings.drop_dataset
        num_items = self.get_dataset_items(drop_dataset)

        drop_time = self.drop_dataset(drop_dataset)

        self.report_kpi(num_items, drop_time)


class BigFunTest(AnalyticsTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.QUERIES = self.analytics_settings.queries

    def _run_bigfun_queries(
        self, nodes: list, concurrency: int, num_requests: int
    ) -> list[QueryLatencyPair]:
        logger.info(f"analytics_{nodes=}")
        results = bigfun(
            self.rest,
            nodes=nodes,
            concurrency=concurrency,
            num_requests=num_requests,
            query_set=self.QUERIES,
            request_params=self.analytics_settings.bigfun_request_params,
        )

        return [(query, latency) for query, latency in results]

    def warmup(self, nodes: Optional[list] = None) -> list[QueryLatencyPair]:
        return self._run_bigfun_queries(
            nodes=nodes or self.analytics_nodes,
            concurrency=self.test_config.access_settings.analytics_warmup_workers,
            num_requests=self.test_config.access_settings.analytics_warmup_ops,
        )

    @with_stats
    def access(self, nodes: Optional[list] = None) -> list[QueryLatencyPair]:
        return self._run_bigfun_queries(
            nodes=nodes or self.analytics_nodes,
            concurrency=self.test_config.access_settings.workers,
            num_requests=int(self.test_config.access_settings.ops),
        )

    def _report_kpi(self, results: list[QueryLatencyPair]):
        for query, latency in results:
            self.reporter.post(*self.metrics.analytics_latency(query, latency))

    def run(self):
        random.seed(8095)
        sync_timings = self.initial_load_and_sync()
        self.report_ingestion_kpi(sync_timings)

        if (workers := self.test_config.access_settings.workers) < 1:
            logger.info(f"Number of analytics query workers = {workers}. Skipping query phases.")
            return

        if any(ds.get_type() != DatasetType.EXTERNAL for ds in self.datasets):
            logger.info("Running warmup phase")
            self.warmup()

        logger.info("Running access phase")
        results = self.access()

        if results:
            self.report_kpi(results)
        else:
            logger.warning("Query phase finished executing but returned no results.")


class IncrementalSyncTest(AnalyticsTest):
    def run(self):
        sync_timings = self.initial_load_and_sync()
        self.report_ingestion_kpi(sync_timings)

        if not self.test_config.analytics_settings.resync:
            return

        resync_timings = self.incremental_sync()
        self.report_ingestion_kpi(resync_timings, "incremental")


@dataclass(unsafe_hash=True)
class CopyToParameters:
    output_format: str
    max_objects_per_file: Optional[str] = None
    compression: Optional[str] = None
    gzip_compression_level: Optional[str] = None

    # Parquet specific options
    row_group_size: Optional[str] = None
    page_size: Optional[str] = None
    max_schemas: Optional[int] = None

    def __post_init__(self):
        if str(self.compression).lower() == "none":
            self.compression = None

        if self.compression not in ("gz", "gzip"):
            self.gzip_compression_level = None

        if self.output_format != "parquet":
            self.row_group_size = None
            self.page_size = None
            self.max_schemas = None
            if self.compression in ("snappy", "zstd"):
                self.compression = None

    def to_dict(self) -> dict:
        return remove_nulls(
            {
                "format": self.output_format,
                "max-objects-per-file": self.max_objects_per_file,
                "compression": self.compression,
                "gzipCompressionLevel": self.gzip_compression_level,
                "row-group-size": self.row_group_size,
                "page-size": self.page_size,
                "max-schemas": self.max_schemas,
            }
        )

    def gen_output_path_prefix(self) -> str:
        comp = (self.compression or "none") + (
            f"-{self.gzip_compression_level}" if self.gzip_compression_level else ""
        )
        return "/".join(
            filter(
                None,
                [
                    f"mopf-{self.max_objects_per_file}",
                    self.output_format,
                    f"compression-{comp}",
                    f"rgsize-{self.row_group_size}" if self.row_group_size else None,
                    f"psize-{self.page_size}" if self.page_size else None,
                    f"max-schemas-{self.max_schemas}" if self.max_schemas else None,
                ],
            )
        )


class ColumnarCopyToObjectStoreTest(AnalyticsTest):
    def gen_query_statement(
        self,
        query_config: dict,
        obj_store_name: str,
        params: CopyToParameters,
        repeat: int = 0,
        link_name: str = "external_link",
    ) -> str:
        query_template = (
            f"COPY {{}} TO `{obj_store_name}` AT `{link_name}` PATH ({{}}) {{}} {{}} {{}}"
        )

        query_id = query_config["id"]
        output_path_expr = f'"{params.gen_output_path_prefix()}/{query_id}-{repeat}"'
        if output_path_exps := query_config.get("output_path_exps"):
            output_path_expr += f", {', '.join(output_path_exps)}"

        partition_clause = ""
        if partition_exps := query_config.get("partition_exps"):
            partition_clause = f"PARTITION BY {', '.join(partition_exps)}"

        order_clause = ""
        if order_exps := query_config.get("order_exps"):
            order_clause = f"ORDER BY {', '.join(order_exps)}"

        over_clause = ""
        if partition_clause or order_clause:
            over_clause = f"OVER ({' '.join(filter(None, (partition_clause, order_clause)))})"

        schema_clause = ""
        if params.output_format == "csv" or (
            params.output_format == "parquet"
            and not self.test_config.columnar_copy_to_settings.parquet_schema_inference
        ):
            obj_type_def = json.dumps(query_config["obj_type_def"]).replace('"', "")
            schema_clause = f"TYPE ({obj_type_def})"

        with_clause = f"WITH {json.dumps(params.to_dict())}"

        return query_template.format(
            query_config["source_def"],
            output_path_expr,
            over_clause,
            schema_clause,
            with_clause,
        )

    def gen_all_queries(self, repeat: int = 0) -> dict[str, str]:
        query_statements = {}

        copy_to_settings = self.test_config.columnar_copy_to_settings

        with open(copy_to_settings.object_store_query_file, "r") as f:
            query_configs = yaml.safe_load(f)

        obj_store_name = self.cluster_spec.backup.split("://")[1]

        valid_param_combinations = set(
            CopyToParameters(*p) for p in copy_to_settings.all_param_combinations
        )

        for params in valid_param_combinations:
            for conf in query_configs:
                query_id = f"{params.gen_output_path_prefix()}/{conf['id']}"
                query_statements[query_id] = self.gen_query_statement(
                    conf, obj_store_name, params, repeat, link_name=self.copy_to_link_name
                )

        return query_statements

    @with_stats
    def access(self) -> dict[str, list[float]]:
        obj_store_uri = self.cluster_spec.backup
        az_storage_acc = self.cluster_spec.azure_storage_account
        objects, size = get_cloud_storage_bucket_stats(
            obj_store_uri, aws_profile="default", az_storage_acc=az_storage_acc
        )
        query_times = defaultdict(list)

        for i in range(self.test_config.columnar_copy_to_settings.query_loops):
            for query_id, statement in self.gen_all_queries(repeat=i).items():
                t0 = time.time()
                resp = self.exec_analytics_statement(
                    self.analytics_node, statement, verbose=True, with_retry=False
                )
                latency = time.time() - t0

                if not resp.ok:
                    logger.error(f"Query failed: {resp.text}")
                    query_times[query_id].append(float("nan"))
                    continue

                query_times[query_id].append(latency)

                logger.info(resp.json())
                logger.info(f"client-side query response time (s): {latency}")

                new_objects, new_size = get_cloud_storage_bucket_stats(
                    obj_store_uri, aws_profile="default", az_storage_acc=az_storage_acc
                )
                if not (new_objects > objects and new_size > size):
                    logger.warning(
                        "Cloud storage bucket object count and data size have not "
                        "both increased. COPY TO statement has not written any data!"
                    )
                objects, size = new_objects, new_size

        return query_times

    def run(self):
        random.seed(8095)
        self.initial_load_and_sync()

        csp = self.cluster_spec.capella_backend or self.cluster_spec.cloud_provider
        self.copy_to_link_name = (
            "copy_to_azureblob_link"
            if csp == "azure"
            else self.ext_data_settings.external_link_name
        )
        if csp == "azure":
            self.link_manager.ensure(
                ExternalLink(
                    self.copy_to_link_name,
                    link_type="azureblob",
                    azure_storage_account=self.cluster_spec.azure_storage_account,
                )
            )
        query_times = self.access()
        logger.info(f"Raw query times (seconds): {pretty_dict(query_times)}")
        summarised_times = {
            query_id: {"mean": np.mean(latencies), "std": np.std(latencies)}
            for query_id, latencies in query_times.items()
        }
        logger.info(f"Summarised query times (seconds): {pretty_dict(summarised_times)}")


class ColumnarCopyToKVRemoteLinkTest(AnalyticsTest):
    @with_stats
    def access(self):
        with open(self.test_config.columnar_copy_to_settings.kv_query_file, "r") as f:
            queries = yaml.safe_load(f)

        query_template = f"COPY {{}} TO {{}} AT `{self.couchbase_link_name}` KEY {{}}"

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
        self.initial_load_and_sync()

        # Ensure a connected remote link exists (initial_load_and_sync creates one only if
        # there are remote datasets).
        self.link_manager.ensure(RemoteLink(self.couchbase_link_name), connect=True)

        self.access()


class BigFunQueryFailoverTest(BigFunTest):
    def failover(self):
        logger.info("Starting node failover")
        clusters = self.cluster_spec.clusters
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.test_config.rebalance_settings.failed_nodes
        active_analytics_nodes = self.analytics_nodes

        for (_, servers), initial_nodes in zip(clusters, initial_nodes):
            master = servers[0]

            failed = servers[initial_nodes - failed_nodes : initial_nodes]

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
        self.initial_load_and_sync()

        self.link_manager.disconnect_streaming()
        self.monitor_cbas_pending_ops()
        active_analytics_nodes = self.failover()

        logger.info("Running warmup phase")
        self.warmup(nodes=active_analytics_nodes)

        logger.info("Running access phase")
        results = self.access(nodes=active_analytics_nodes)

        self.report_kpi(results)


class AnalyticsRebalanceTest(AnalyticsTest, RebalanceTest):
    ALL_HOSTNAMES = True

    def rebalance_cbas(self):
        services = "cbas"
        cluster_idx = self.cluster_spec.get_cluster_idx_by_node(self.analytics_node)
        if ServerInfoManager().get_server_info(cluster_idx).is_columnar:
            services = "kv,cbas"
        self.rebalance(services=services)

    def _report_kpi(self):
        self.reporter.post(*self.metrics.rebalance_time(rebalance_time=self.rebalance_time))

    def run(self):
        self.initial_load_and_sync()

        self.link_manager.disconnect_streaming()
        self.monitor_cbas_pending_ops()

        self.rebalance_cbas()

        if self.is_balanced():
            self.report_kpi()


class AnalyticsDynamicServiceRebalanceTest(AnalyticsRebalanceTest, DynamicServiceRebalanceTest):
    pass


class CapellaAnalyticsRebalanceTest(AnalyticsRebalanceTest, CapellaRebalanceKVTest):
    pass


class ConnectTest(AnalyticsTest):
    def _report_kpi(self, avg_connect_time: int, avg_disconnect_time: int):
        self.reporter.post(*self.metrics.analytics_avg_connect_time(avg_connect_time))

        self.reporter.post(*self.metrics.analytics_avg_disconnect_time(avg_disconnect_time))

    @timeit
    def connect_analytics_link(self):
        super().connect_link(self.couchbase_link_name)

    @timeit
    def disconnect_analytics_link(self):
        super().disconnect_link(self.couchbase_link_name)

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
        return total_connect_time / ops, total_disconnect_time / ops

    def run(self):
        self.initial_load_and_sync()

        avg_connect_time, avg_disconnect_time = self.connect_cycle(
            int(self.test_config.access_settings.ops)
        )

        self.report_kpi(avg_connect_time, avg_disconnect_time)


class TPCDSQueryTest(AnalyticsTest):
    COUNT_QUERIES = "perfrunner/workloads/tpcdsfun/count_queries.yaml"
    QUERIES = "perfrunner/workloads/tpcdsfun/queries.yaml"

    @property
    def indexes(self) -> list[IndexDef]:
        return [
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

    @with_stats
    def access(
        self, *args, **kwargs
    ) -> tuple[
        list[QueryLatencyPair],
        list[QueryLatencyPair],
        list[QueryLatencyPair],
        list[QueryLatencyPair],
    ]:
        logger.info("Running COUNT queries without primary key index")
        results = tpcds(
            self.rest,
            nodes=self.analytics_nodes,
            concurrency=self.test_config.access_settings.workers,
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.COUNT_QUERIES,
        )
        count_without_index_results = [(query, latency) for query, latency in results]

        self.create_primary_indexes()

        logger.info("Running COUNT queries with primary key index")
        results = tpcds(
            self.rest,
            nodes=self.analytics_nodes,
            concurrency=self.test_config.access_settings.workers,
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.COUNT_QUERIES,
        )
        count_with_index_results = [(query, latency) for query, latency in results]

        self.drop_primary_indexes()

        logger.info("Running queries without index")
        results = tpcds(
            self.rest,
            nodes=self.analytics_nodes,
            concurrency=self.test_config.access_settings.workers,
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.QUERIES,
        )
        without_index_results = [(query, latency) for query, latency in results]

        self.create_analytics_indexes()

        logger.info("Running queries with index")
        results = tpcds(
            self.rest,
            nodes=self.analytics_nodes,
            concurrency=self.test_config.access_settings.workers,
            num_requests=int(self.test_config.access_settings.ops),
            query_set=self.QUERIES,
        )
        with_index_results = [(query, latency) for query, latency in results]

        return (
            count_without_index_results,
            count_with_index_results,
            without_index_results,
            with_index_results,
        )

    def _report_kpi(self, results: list[QueryLatencyPair], with_index: bool):
        for query, latency in results:
            self.reporter.post(*self.metrics.analytics_volume_latency(query, latency, with_index))

    def run(self):
        self.initial_load_and_sync(create_indexes=False)
        self.compact_bucket()

        self.download_tpcds_couchbase_loader()
        count_results_no_index, count_results_with_index, results_no_index, results_with_index = (
            self.access()
        )

        self.report_kpi(count_results_no_index, with_index=False)
        self.report_kpi(count_results_with_index, with_index=True)
        self.report_kpi(results_no_index, with_index=False)
        self.report_kpi(results_with_index, with_index=True)


class CH2Test(AnalyticsTest):
    BUCKET = "bench"

    GSI_INDEXES = [
        ("cu_w_id_d_id_last", "customer", ("c_w_id", "c_d_id", "c_last")),
        ("di_id_w_id", "district", ("d_id", "d_w_id")),
        ("no_o_id_d_id_w_id", "neworder", ("no_o_id", "no_d_id", "no_w_id")),
        ("or_id_d_id_w_id_c_id", "orders", ("o_id", "o_d_id", "o_w_id", "o_c_id")),
        ("or_w_id_d_id_c_id", "orders", ("o_w_id", "o_d_id", "o_c_id")),
        ("wh_id", "warehouse", ("w_id",)),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = self.test_config.ch2_settings.schema

        if self.capella_infra:
            self.COLLECTORS.update({"iostat": False, "memory": False})

        if self.query_nodes:
            self.COLLECTORS.update({"n1ql_stats": True, "secondary_stats": True})

    @property
    def gsi_indexes(self) -> list[IndexDef]:
        return [
            IndexDef(name, f"{self.BUCKET}.{self.schema.value}.{coll}", fields)
            for name, coll, fields in self.GSI_INDEXES
        ]

    def _report_kpi(self, log_file: Optional[str] = None, extra_metric_id_suffix: str = ""):
        ch2_metrics = self.metrics.ch2_metrics(
            logfile=log_file or self.test_config.ch2_settings.workload,
            tclients=self.test_config.ch2_settings.tclients,
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
                    ch2_metrics.geo_mean_cbas_query_time_secs,
                    self.test_config.ch2_settings.tclients,
                    extra_metric_id_suffix,
                )
            )
            self.reporter.post(
                *self.metrics.ch2_analytics_query_set_time(
                    ch2_metrics.average_cbas_query_set_time_secs,
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

    def create_gsi_indexes(self):
        logger.info("Creating GSI indexes")
        for index_def in self.gsi_indexes:
            statement = f"{index_def.create_statement()} USING GSI;"
            logger.info(f"Running: {statement}")
            res = self.rest.exec_n1ql_statement(self.query_nodes[0], statement)
            logger.info(f"Result: {res}")
            time.sleep(5)

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

    def setup(self):
        sync_timings = self.initial_load_and_sync(restart_after_kv_load=True)
        self.report_ingestion_kpi(sync_timings)

        if self.test_config.ch2_settings.create_gsi_index:
            self.create_gsi_indexes()

        if self.test_config.analytics_settings.use_cbo:
            self.analyze_datasets(
                self.test_config.analytics_settings.cbo_sample_size,
                self.test_config.analytics_settings.cbo_sample_seed,
                verbose=True,
            )

    def benchmark(self):
        self.init_ch2_repo()
        self.run_ch2()
        self.report_kpi()

    def run(self):
        self.setup()
        self.benchmark()


class TransformOnIngestTest(AnalyticsTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.transform_config = {}
        if transform_def_file := self.test_config.columnar_settings.dataset_transform_def_file:
            with open(transform_def_file, "r") as f:
                self.transform_config = yaml.safe_load(f)

    def create_transform_udfs(self):
        logger.info("Creating transform UDFs")
        for transforms in self.transform_config.values():
            for transform in transforms:
                if transform.get("type") != "udf":
                    continue

                udf_body = transform.get("body", "")
                assert udf_body, f"UDF body for {transform['name']} is empty"
                udf_param = next(re.finditer(r"(?:from|FROM) \[(\w+)\]", udf_body)).group(1)
                statement = (
                    f"CREATE OR REPLACE TRANSFORM FUNCTION `{transform['name']}` ({udf_param}) {{ "
                    f"SELECT VALUE doc FROM ( {udf_body} ) AS doc LIMIT 1 }};"
                )
                self.exec_analytics_statement(self.analytics_node, statement, verbose=True)

    def add_transformed_datasets(self):
        streaming_datasets_by_name = {ds.name: ds for ds in self.datasets if ds.is_streaming()}
        transformed_datasets = []
        for ds_name, transforms in self.transform_config.items():
            for transform in transforms:
                t_type = transform["type"]
                transform_ds_name = f"{ds_name}_{t_type}_{transform['name']}"
                where_clause = transform["body"] if t_type == "where" else None
                transform_func = transform["name"] if t_type == "udf" else None
                if existing_ds := streaming_datasets_by_name.get(ds_name):
                    new_ds = replace(
                        existing_ds,
                        name=transform_ds_name,
                        where_clause=where_clause,
                        transform_func=transform_func,
                    )
                    transformed_datasets.append(new_ds)

        self.dataset_collection.extend(transformed_datasets)

    @with_stats
    def access(self) -> dict:
        results = {}
        for d in self.dataset_collection.of_type(DatasetType.REMOTE):
            self.exec_analytics_statement(self.analytics_node, d.create_statement(), verbose=True)

            bucket, scope, coll = d.fully_qualified_source.split(".")
            self.link_manager.connect(d.link_name)
            t0 = time.time()
            self.monitor.monitor_data_synced(
                data_node=self.data_node,
                bucket=bucket,
                bucket_replica=self.test_config.bucket.replica_number,
                analytics_node=self.analytics_node,
                scope=scope,
                coll=coll,
            )
            ingest_time = time.time() - t0
            self.link_manager.disconnect(d.link_name)

            ingested_items = self.get_dataset_items(d.name)
            avg_item_size = self.get_average_encoded_doc_size(d.name)
            items_per_sec = ingested_items / ingest_time if ingest_time > 0 else 0
            bytes_per_sec = items_per_sec * avg_item_size
            logger.info(f"Ingestion time for {d.name} (s): {ingest_time}")
            logger.info(f"Average items/sec: {items_per_sec:.2f}")
            logger.info(f"Average MB/sec: {bytes_per_sec / 1e6:.2f}")
            results[d.name] = {
                "time": ingest_time,
                "items": ingested_items,
                "avg_item_size": avg_item_size,
                "items_per_sec": items_per_sec,
                "bytes_per_sec": bytes_per_sec,
            }

            self.exec_analytics_statement(
                self.analytics_node, f"DROP DATASET {sqlpp_escape(d.name)} IF EXISTS", verbose=True
            )
            self.monitor.monitor_cbas_pending_ops(self.analytics_nodes)

        return results

    def run(self):
        self.create_transform_udfs()
        self.add_transformed_datasets()

        self.link_manager.create_all()
        self.load_kv_data()

        results = self.access()
        logger.info(f"Results: {pretty_dict(results)}")


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


class CH2CapellaColumnarUnlimitedStorageTest(CH2Test, CapellaColumnarManualOnOffTest):
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
            self.analyze_datasets(
                self.test_config.analytics_settings.cbo_sample_size,
                self.test_config.analytics_settings.cbo_sample_seed,
                verbose=True,
            )

        new_sf_title = f"{self.test_config.showfast.title}, POST-RESUME"
        self.test_config.config["showfast"]["title"] = new_sf_title
        self.test_config.update_spec_file()

        log_file = f"{self.test_config.ch2_settings.workload}_post_resume"
        ch2_settings = self.test_config.ch2_settings
        ch2_settings.warmup_iterations = 0
        ch2_settings.iterations = 1
        self.run_ch2(log_file=log_file, ch2_settings=ch2_settings)

        self.report_kpi(log_file=log_file, extra_metric_id_suffix="post_resume")


class InitialIngestOnlyTest(AnalyticsTest):
    def run(self):
        sync_timings = self.initial_load_and_sync()
        self.report_ingestion_kpi(sync_timings)


class StandaloneDatasetTruncateTest(AnalyticsTest):
    def ingest_dataset(self, dataset: StandaloneDatasetDef):
        _, ingest_time = self.copy_data_from_object_store([dataset])
        self.timings["ingest"][dataset.name].append(ingest_time)

    def empty_dataset(
        self,
        dataset: StandaloneDatasetDef,
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

    def truncate_dataset(self, dataset: StandaloneDatasetDef):
        statement = f"TRUNCATE DATASET {sqlpp_escape(dataset.name)}"
        self.empty_dataset(dataset, [statement], "truncate")

    def delete_from_dataset(self, dataset: StandaloneDatasetDef):
        statement = f"DELETE FROM {sqlpp_escape(dataset.name)}"
        self.empty_dataset(dataset, [statement], "delete")

    def recreate_dataset(self, dataset: StandaloneDatasetDef):
        statements = [
            f"DROP DATASET {sqlpp_escape(dataset.name)}",
            dataset.create_statement(),
        ]
        self.empty_dataset(dataset, statements, "recreate")

    @with_stats
    def access(self, datasets: list[StandaloneDatasetDef]):
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
        self.link_manager.create_all()
        self.create_datasets(verbose=True)
        self.create_analytics_indexes()

        standalone_datasets = self.dataset_collection.of_type(DatasetType.STANDALONE)

        self.timings = {
            op: {d.name: [] for d in standalone_datasets}
            for op in ["ingest", "truncate", "delete", "recreate"]
        }

        self.access(standalone_datasets)
        self.summarize_timings()


class RemoteDatasetTruncateTest(StandaloneDatasetTruncateTest):
    def empty_dataset(
        self,
        dataset: RemoteDatasetDef,
        statements: list[str],
        op: Literal["truncate", "recreate"],
    ):
        t0 = time.time()
        for statement in statements:
            st0 = time.time()
            self.exec_analytics_statement(self.analytics_node, statement, verbose=True)
            logger.info(f"Statement execution time (s): {time.time() - st0:.2f}")
        empty_time = time.time() - t0
        self.timings[op][dataset.name].append(empty_time)

        self.monitor.monitor_data_synced(
            self.data_node,
            dataset.source_bucket,
            self.test_config.bucket.replica_number,
            self.analytics_node,
            dataset.source_scope,
            dataset.source_collection,
        )
        self.timings["ingest"][dataset.name].append(time.time() - t0)

    def recreate_dataset(self, dataset: RemoteDatasetDef):
        statements = [
            f"DROP DATASET {sqlpp_escape(dataset.name)}",
            dataset.create_statement(),
        ]
        self.empty_dataset(dataset, statements, "recreate")

    @with_stats
    def access(self, datasets: list[RemoteDatasetDef]):
        for _ in range(3):
            for d in datasets:
                self.truncate_dataset(d)
                self.recreate_dataset(d)

        logger.info(f"Raw timings: {pretty_dict(self.timings)}")

    def run(self):
        self.initial_load_and_sync()

        remote_datasets = self.dataset_collection.of_type(DatasetType.REMOTE)

        self.timings = {
            op: {d.name: [] for d in remote_datasets} for op in ["ingest", "truncate", "recreate"]
        }

        self.access(remote_datasets)
        self.summarize_timings()


class CH3Test(CH2Test):
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

    @property
    def fts_node(self) -> str:
        return self.fts_nodes[0]

    def _report_kpi(self):
        ch3_metrics = self.metrics.ch3_metrics(
            logfile=self.test_config.ch2_settings.workload,
            tclients=self.test_config.ch2_settings.tclients,
        )

        self.reporter.post(
            *self.metrics.ch2_tpm(round(ch3_metrics.tpm, 2), self.test_config.ch2_settings.tclients)
        )

        self.reporter.post(
            *self.metrics.ch2_response_time(
                round(ch3_metrics.txn_response_time, 2), self.test_config.ch2_settings.tclients
            )
        )

        if self.test_config.ch2_settings.workload == "ch3_mixed":
            self.reporter.post(
                *self.metrics.ch2_analytics_query_set_time(
                    ch3_metrics.average_cbas_query_set_time_secs,
                    self.test_config.ch2_settings.tclients,
                )
            )

            self.reporter.post(
                *self.metrics.ch3_fts_query_time(
                    round(ch3_metrics.average_fts_query_set_time_ms / 1000, 2),
                    self.test_config.ch2_settings.tclients,
                )
            )

            self.reporter.post(
                *self.metrics.ch3_fts_client_time(
                    round(ch3_metrics.average_fts_client_time_ms / 1000, 2),
                    self.test_config.ch2_settings.tclients,
                )
            )

            self.reporter.post(
                *self.metrics.ch3_fts_qph(
                    ch3_metrics.fts_qph, self.test_config.ch2_settings.tclients
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

        query_urls = [f"{node}:{query_port}" for node in self.query_nodes]
        userid, password = self.cluster_spec.rest_credentials
        conn_settings = CH2ConnectionSettings(
            userid=userid,
            password=password,
            analytics_url=f"{self.analytics_nodes[0]}:{cbas_port}",
            query_url=query_urls[0],
            multi_query_url=",".join(query_urls),
            fts_url=f"{self.fts_nodes[0]}:{fts_port}",
        )

        logger.info(f"running {self.test_config.ch2_settings.workload}")
        local.ch2_run_task(
            conn_settings,
            self.test_config.ch2_settings,
            log_file=self.test_config.ch2_settings.workload,
        )

    def create_fts_indexes(self):
        local.ch3_create_fts_index(cluster_spec=self.cluster_spec, fts_node=self.fts_node)

    def wait_for_fts_index_persistence(self):
        hosts = self.fts_nodes
        bucket = self.test_config.buckets[0]

        if self.server_info.build_tuple < (7, 6, 3, 0):
            wait_func = self.monitor.monitor_fts_index_persistence
        else:
            wait_func = self.monitor.monitor_fts_index_persistence_and_merges

        for index_name in self.FTS_INDEXES:
            wait_func(hosts=hosts, index=index_name, bucket=bucket)

    def load_ch3(self):
        if self.test_config.cluster.enable_n2n_encryption:
            query_port = QUERY_PORT_SSL
        else:
            query_port = QUERY_PORT

        query_urls = [f"{node}:{query_port}" for node in self.query_nodes]
        userid, password = self.cluster_spec.rest_credentials
        conn_settings = CH2ConnectionSettings(
            userid=userid,
            password=password,
            data_url=self.data_nodes[0],
            multi_data_url=",".join(self.data_nodes),
            query_url=query_urls[0],
            multi_query_url=",".join(query_urls),
        )

        logger.info(f"running {self.test_config.ch2_settings.workload}")
        local.ch2_load_task(conn_settings, self.test_config.ch2_settings)

    def load_kv_data(self):
        if self.test_config.ch2_settings.use_backup:
            self.restore_local()
        else:
            self.load_ch3()

    def run(self):
        local.clone_git_repo(
            repo=self.test_config.ch2_settings.repo, branch=self.test_config.ch2_settings.branch
        )

        self.initial_load_and_sync(restart_after_kv_load=True)

        self.create_gsi_indexes()
        self.wait_for_indexing()
        self.create_fts_indexes()
        self.wait_for_fts_index_persistence()

        self.run_ch3()
        if self.test_config.ch2_settings.workload != "ch3_analytics":
            self.report_kpi()


class ScanTest(AnalyticsTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
                node=self.analytics_node,
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
            node=self.analytics_node,
            cluster_spec=self.cluster_spec,
            script=path
        )

    def sync(self):
        self.link_manager.disconnect(self.couchbase_link_name)
        self.create_and_analyze_datasets()
        self.link_manager.connect(self.couchbase_link_name)

        bucket_replica = self.test_config.bucket.replica_number
        for bucket in self.test_config.buckets:
            self.monitor.monitor_data_synced(
                self.data_node, bucket, bucket_replica, self.analytics_node
            )

    def run(self):
        self.restore_local()
        self.import_tables()
        self.sync()
        time_taken = self.all_operations()
        self.report_kpi(time_taken)

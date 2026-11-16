"""Iceberg Variant collections: the DDL they generate and the configs that feed it."""

import glob
import os
import re
from types import SimpleNamespace
from unittest import TestCase

import yaml

os.environ.setdefault("WORKER_TYPE", "local")

from perfrunner.settings import AnalyticsExternalDataSettings, TestConfig  # noqa: E402
from perfrunner.tests import analytics  # noqa: E402
from perfrunner.tests.analytics import (  # noqa: E402
    DatasetConfigParser,
    ExternalLink,
    IcebergCollectionDef,
)
from perfrunner.workloads.analytics.custom.driver import (  # noqa: E402
    expand_query_configs,
    split_query_conf,
)

TESTS = sorted(glob.glob("tests/cloud/analytics/iceberg_variant_*.test"))
EXTERNAL_DATA = {"link_type": "s3", "region": "us-east-1", "catalog_name": "variant_glue"}


class IcebergCollectionTest(TestCase):
    def test_create_statement(self):
        link = ExternalLink("s3_link", link_type="s3", region="us-east-1")
        collection = IcebergCollectionDef(
            name="variant_shredded_stats_off",
            catalog="variant_glue",
            link=link,
            namespace="binance",
            table_name="variant_ab_shredded",
            options={"variantStatsPushdown": "false"},
        )
        self.assertEqual(
            collection.create_statement(),
            "CREATE EXTERNAL COLLECTION `variant_shredded_stats_off` ON `variant_glue` "
            'AT `s3_link` WITH {"table-format": "iceberg", "namespace": "binance", '
            '"tableName": "variant_ab_shredded", "variantStatsPushdown": "false"}',
        )

    def test_create_catalog_statement(self):
        test = SimpleNamespace(
            ext_data_settings=AnalyticsExternalDataSettings(EXTERNAL_DATA),
        )
        self.assertEqual(
            analytics.IcebergVariantTest.create_catalog_statement(test),
            "CREATE CATALOG `variant_glue` TYPE Iceberg SOURCE AWS_GLUE AT `s3_link` WITH {}",
        )


class IcebergVariantConfigTest(TestCase):
    def test_every_query_reads_a_collection_its_test_creates(self):
        for path in TESTS:
            test_config = TestConfig()
            test_config.parse(path)
            settings = test_config.analytics_settings
            datasets, _, _ = DatasetConfigParser(test_config).parse(settings.dataset_conf_file)
            for dataset in datasets:
                dataset.create_statement()

            with open(settings.custom_query_conf_file) as f:
                configs, _ = split_query_conf(yaml.safe_load(f))
            names = {d.name for d in datasets}
            for query in expand_query_configs(configs):
                for collection in re.findall(r"\bFROM (\w+)", query.statement):
                    self.assertIn(collection, names, f"{path}: {query.name}")

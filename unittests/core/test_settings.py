"""Settings parsing, plus the corpus validators over every .test config and cluster spec."""

import glob
import tempfile
from pathlib import Path
from unittest import TestCase

from perfrunner.settings import CBProduct, ClusterSpec, TestConfig


class SettingsTest(TestCase):
    def test_stale_update_after(self):
        test_config = TestConfig()
        test_config.parse("tests/query_lat_20M_basic.test")
        query_params = test_config.access_settings.query_params
        self.assertEqual(query_params, {"stale": "false"})

    def test_cluster_specs(self):
        for file_name in glob.glob("clusters/*.spec") + glob.glob(
            "cloud/infrastructure/**/*.spec", recursive=True
        ):
            cluster_spec = ClusterSpec()
            cluster_spec.parse(file_name, override=None)

    def test_override(self):
        test_config = TestConfig()
        test_config.parse("tests/query_lat_20M_basic.test", override=["cluster.mem_quota.5555"])
        self.assertEqual(test_config.cluster.mem_quota, 5555)

    def test_soe_backup_repo(self):
        for file_name in glob.glob("tests/soe/*.test"):
            test_config = TestConfig()
            test_config.parse(file_name)
            self.assertNotEqual(test_config.restore_settings.backup_repo, "")

    def test_moving_working_set_settings(self):
        for file_name in glob.glob("tests/gsi/plasma/*.test"):
            test_config = TestConfig()
            test_config.parse(file_name)
            if test_config.access_settings.working_set_move_time:
                self.assertNotEqual(test_config.access_settings.working_set, 100)
                self.assertEqual(test_config.access_settings.working_set_access, 100)

    def test_every_test_config_parses(self):
        """No .test file may fail to parse; 13 did before this and could never run."""
        failures = []
        for file_name in sorted(glob.glob("tests/**/*.test", recursive=True)):
            try:
                TestConfig().parse(file_name)
            except Exception as e:
                failures.append(f"{file_name}: {type(e).__name__}: {e}")
        self.assertEqual(failures, [], f"{len(failures)} config(s) failed to parse")

    def test_fts_configs(self):
        for file in glob.glob("tests/fts/enduser/tests_dgm/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "end_user_dgm")
            self.assertEqual(test_config.showfast.sub_category, "Latency")

        for file in glob.glob("tests/fts/enduser/tests_dgm/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "end_user_dgm")
            self.assertEqual(test_config.showfast.sub_category, "Throughput")

        for file in glob.glob("tests/fts/enduser/tests_dgm/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "end_user_dgm")
            self.assertEqual(test_config.showfast.sub_category, "Index")

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "end_user_non_dgm")
            self.assertEqual(test_config.showfast.sub_category, "Latency")

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "end_user_non_dgm")
            self.assertEqual(test_config.showfast.sub_category, "Throughput")

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "end_user_non_dgm")
            self.assertEqual(test_config.showfast.sub_category, "Index")

        for file in glob.glob("tests/fts/multi_node/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark_3_nodes")
            self.assertEqual(test_config.showfast.sub_category, "Latency")

        for file in glob.glob("tests/fts/multi_node/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark_3_nodes")
            self.assertEqual(test_config.showfast.sub_category, "Throughput")

        for file in glob.glob("tests/fts/multi_node/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark_3_nodes")
            self.assertEqual(test_config.showfast.sub_category, "Index")

        for file in glob.glob("tests/fts/rebalance/*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark")
            self.assertEqual(test_config.showfast.sub_category, "Rebalance")

        for file in glob.glob("tests/fts/single_node/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark")
            self.assertEqual(test_config.showfast.sub_category, "Latency")

        for file in glob.glob("tests/fts/single_node/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark")
            self.assertEqual(test_config.showfast.sub_category, "Throughput")

        for file in glob.glob("tests/fts/single_node/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark")
            self.assertEqual(test_config.showfast.sub_category, "Index")

        for file in glob.glob("tests/fts/single_node_kv/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark_kv")
            self.assertEqual(test_config.showfast.sub_category, "Latency")

        for file in glob.glob("tests/fts/single_node_kv/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, "benchmark_kv")
            self.assertEqual(test_config.showfast.sub_category, "Throughput")


class ProductsByServerTest(TestCase):
    """Core tier: `products_by_server` is the sole input to `get_product()`/`get_install_dir()`.

    Those decide the install prefix in ~25 places and the systemd unit name in
    `restart`/`stop_server`/`start_server`, so breaking `products_by_server` breaks every
    columnar run at once -- the settings-parsing category the core-tier rules name explicitly.
    Pure (tempfile + ConfigParser, no subprocess/socket/SSH/sleep) and deterministic.
    """

    def _spec(self, content: str) -> ClusterSpec:
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".spec", delete=False, encoding="utf-8"
        )
        tmp.write(content)
        tmp.close()
        self.addCleanup(lambda: Path(tmp.name).unlink(missing_ok=True))

        cluster_spec = ClusterSpec()
        cluster_spec.parse(tmp.name, override=None)
        return cluster_spec

    SINGLE_CLUSTER_COLUMNAR = """\
[infrastructure]
provider = aws
type = ec2
service = columnar
{columnar_product_line}

[clusters]
goldfish =
        192.168.1.1:kv,cbas
        192.168.1.2:kv,cbas
"""

    TWO_CLUSTER_COLUMNAR = """\
[infrastructure]
provider = aws
type = ec2
service = columnar
{columnar_product_line}

[clusters]
datasource =
        192.168.1.1:kv
goldfish =
        192.168.2.1:kv,cbas
"""

    def test_single_cluster_columnar_no_key_defaults_to_enterprise_analytics(self):
        """Backward compat: specs written before Operational Insights existed stay EA."""
        cluster_spec = self._spec(self.SINGLE_CLUSTER_COLUMNAR.format(columnar_product_line=""))
        products = cluster_spec.products_by_server
        self.assertEqual(
            products,
            {
                "192.168.1.1": CBProduct.ENTERPRISE_ANALYTICS,
                "192.168.1.2": CBProduct.ENTERPRISE_ANALYTICS,
            },
        )

    def test_single_cluster_columnar_with_key_is_operational_insights(self):
        cluster_spec = self._spec(
            self.SINGLE_CLUSTER_COLUMNAR.format(
                columnar_product_line="columnar_product = operational-insights"
            )
        )
        products = cluster_spec.products_by_server
        self.assertEqual(
            products,
            {
                "192.168.1.1": CBProduct.OPERATIONAL_INSIGHTS,
                "192.168.1.2": CBProduct.OPERATIONAL_INSIGHTS,
            },
        )

    def test_two_cluster_columnar_no_key(self):
        cluster_spec = self._spec(self.TWO_CLUSTER_COLUMNAR.format(columnar_product_line=""))
        products = cluster_spec.products_by_server
        self.assertEqual(products["192.168.1.1"], CBProduct.COUCHBASE_SERVER)
        self.assertEqual(products["192.168.2.1"], CBProduct.ENTERPRISE_ANALYTICS)

    def test_two_cluster_columnar_with_key(self):
        cluster_spec = self._spec(
            self.TWO_CLUSTER_COLUMNAR.format(
                columnar_product_line="columnar_product = operational-insights"
            )
        )
        products = cluster_spec.products_by_server
        self.assertEqual(products["192.168.1.1"], CBProduct.COUCHBASE_SERVER)
        self.assertEqual(products["192.168.2.1"], CBProduct.OPERATIONAL_INSIGHTS)

    def test_non_columnar_spec_with_key_present_is_couchbase_server(self):
        """A stray `columnar_product` key on a non-columnar spec must not leak through."""
        cluster_spec = self._spec(
            """\
[infrastructure]
provider = aws
type = ec2
columnar_product = operational-insights

[clusters]
cluster1 =
        192.168.1.1:kv
"""
        )
        self.assertFalse(cluster_spec.columnar_infrastructure)
        products = cluster_spec.products_by_server
        self.assertEqual(products, {"192.168.1.1": CBProduct.COUCHBASE_SERVER})

    def test_capella_columnar_is_couchbase_server(self):
        cluster_spec = self._spec(
            """\
[infrastructure]
provider = capella
backend = aws
service = columnar

[clusters]
provisioned =
        10.0.0.1:kv
goldfish =
        10.0.0.2:kv,cbas
"""
        )
        self.assertTrue(cluster_spec.capella_infrastructure)
        products = cluster_spec.products_by_server
        self.assertEqual(
            products,
            {
                "10.0.0.1": CBProduct.COUCHBASE_SERVER,
                "10.0.0.2": CBProduct.COUCHBASE_SERVER,
            },
        )

    def test_inactive_cluster_hosts_still_resolve(self):
        """`RemoteLinux.get_product`'s dict index must not raise for an inactive cluster's hosts."""
        cluster_spec = self._spec(self.TWO_CLUSTER_COLUMNAR.format(columnar_product_line=""))
        cluster_spec.set_active_clusters_by_name(["goldfish"])

        products = cluster_spec.products_by_server
        self.assertIn("192.168.1.1", products)
        self.assertIn("192.168.2.1", products)

    def test_unknown_columnar_product_value_defaults_to_enterprise_analytics(self):
        cluster_spec = self._spec(
            self.SINGLE_CLUSTER_COLUMNAR.format(
                columnar_product_line="columnar_product = some-future-product"
            )
        )
        self.assertEqual(cluster_spec.columnar_product, CBProduct.ENTERPRISE_ANALYTICS)
        products = cluster_spec.products_by_server
        self.assertEqual(products["192.168.1.1"], CBProduct.ENTERPRISE_ANALYTICS)

    def test_maybe_set_columnar_product_invalidates_cache_on_same_instance(self):
        cluster_spec = self._spec(self.SINGLE_CLUSTER_COLUMNAR.format(columnar_product_line=""))
        # Populate the cached_property as Enterprise Analytics before overriding it.
        self.assertEqual(
            cluster_spec.products_by_server["192.168.1.1"], CBProduct.ENTERPRISE_ANALYTICS
        )

        cluster_spec.maybe_set_columnar_product(CBProduct.OPERATIONAL_INSIGHTS)

        self.assertEqual(
            cluster_spec.products_by_server["192.168.1.1"], CBProduct.OPERATIONAL_INSIGHTS
        )

    def test_maybe_set_columnar_product_persists_to_a_freshly_parsed_spec(self):
        cluster_spec = self._spec(self.SINGLE_CLUSTER_COLUMNAR.format(columnar_product_line=""))
        cluster_spec.maybe_set_columnar_product(CBProduct.OPERATIONAL_INSIGHTS)

        reparsed = ClusterSpec()
        reparsed.parse(cluster_spec.fname, override=None)

        self.assertEqual(
            reparsed.products_by_server["192.168.1.1"], CBProduct.OPERATIONAL_INSIGHTS
        )

    def test_maybe_set_columnar_product_on_prem_spec_stays_on_prem(self):
        """Setting the key on an on-prem spec must not create an `[infrastructure]` section."""
        cluster_spec = self._spec(
            """\
[clusters]
cluster1 =
        192.168.1.1:kv
"""
        )
        self.assertFalse(cluster_spec.cloud_infrastructure)

        cluster_spec.maybe_set_columnar_product(CBProduct.OPERATIONAL_INSIGHTS)

        self.assertFalse(cluster_spec.cloud_infrastructure)

    def test_maybe_set_columnar_product_couchbase_server_leaves_recorded_product_intact(self):
        """A `couchbase-server` install must never overwrite the recorded columnar product.

        That package goes on the datasource cluster of a two-cluster columnar spec and says
        nothing about the columnar cluster. The cost is that reinstalling plain Couchbase Server
        over a single-cluster columnar spec leaves the key stale, and `get_install_dir()` keeps
        returning the columnar prefix -- a shape no test uses today.
        """
        cluster_spec = self._spec(
            self.SINGLE_CLUSTER_COLUMNAR.format(
                columnar_product_line="columnar_product = operational-insights"
            )
        )

        cluster_spec.maybe_set_columnar_product(CBProduct.COUCHBASE_SERVER)

        self.assertEqual(
            cluster_spec.infrastructure_settings.get("columnar_product"), "operational-insights"
        )
        self.assertEqual(
            cluster_spec.products_by_server["192.168.1.1"], CBProduct.OPERATIONAL_INSIGHTS
        )

    def test_columnar_datasource_cluster_is_always_listed_first(self):
        """Corpus validator for the shape `cluster_product` hardcodes.

        A multi-cluster columnar spec pairs a Couchbase Server datasource with the columnar
        cluster, and `cluster_product` reads that off the cluster index alone. A new spec
        listing the columnar cluster first would hand its nodes CBProduct.COUCHBASE_SERVER, so
        `get_install_dir()` and the systemd unit in `restart`/`stop_server`/`start_server`
        would be wrong for the whole run.
        """
        checked, offenders = [], []
        for file_name in glob.glob("clusters/*.spec") + glob.glob(
            "cloud/infrastructure/**/*.spec", recursive=True
        ):
            cluster_spec = ClusterSpec()
            cluster_spec.parse(file_name, override=None)
            # Capella takes the early return in `cluster_product`, so its order is irrelevant.
            if not cluster_spec.columnar_infrastructure or cluster_spec.capella_infrastructure:
                continue
            if len(clusters := list(cluster_spec.infrastructure_clusters.values())) < 2:
                continue

            checked.append(file_name)
            if any("cbas" in node.partition(":")[2].split(",") for node in clusters[0].split()):
                offenders.append(file_name)

        self.assertEqual(
            offenders, [], "columnar cluster listed first; the datasource cluster must be"
        )
        self.assertTrue(checked, "No multi-cluster columnar specs left; this test is now vacuous")

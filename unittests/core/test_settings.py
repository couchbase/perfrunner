"""Settings parsing, plus the corpus validators over every .test config and cluster spec."""

import glob
from unittest import TestCase

from perfrunner.settings import ClusterSpec, TestConfig


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

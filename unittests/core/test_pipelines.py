"""Corpus validator: every test config a pipeline names exists."""

import glob
import json
import os
from pathlib import Path
from unittest import TestCase

from perfrunner.helpers.misc import pretty_dict


class PipelineTest(TestCase):
    def test_existence_of_test_configs(self):
        """Check if all test configs in the pipelines are present in the tests directory."""
        all_missing_test_configs = {}
        filenames_to_paths = {}

        test_config_keys = ["test_config", "test", "analytics_test_config", "kv_test_config"]

        for root, _, files in os.walk("tests"):
            for file in files:
                if not file.endswith(".test"):
                    continue

                if file not in filenames_to_paths:
                    filenames_to_paths[file] = []
                filenames_to_paths[file].append(root)

        for fn in glob.glob("tests/pipelines/*.json"):
            with open(fn, "r") as f:
                test_cases = json.load(f)

            for stage, stage_tests in test_cases.items():
                test_configs = [
                    t
                    for test in stage_tests
                    for t in [test[k] for k in test_config_keys if k in test]
                ]
                missing_stage_test_configs = []

                for test_config in test_configs:
                    parent_path = str(Path(test_config).parent)
                    name = Path(test_config).name

                    if (paths := filenames_to_paths.get(name, [])) and parent_path == ".":
                        continue
                    elif any(root.endswith(parent_path) for root in paths):
                        continue
                    else:
                        missing_stage_test_configs.append(test_config)

                if missing_stage_test_configs:
                    if fn not in all_missing_test_configs:
                        all_missing_test_configs[fn] = {}
                    all_missing_test_configs[fn][stage] = missing_stage_test_configs

        self.assertDictEqual(
            all_missing_test_configs,
            {},
            "\nTest configs from the following pipeline files are missing: \n"
            + pretty_dict(all_missing_test_configs),
        )

    def test_stages(self):
        stages = {
            "Analytics",
            "Eventing",
            "FTS",
            "Tools",
            "Views",
            "GSI",
            "GSI-DGM",
            "N1QL",
            "N1QL-Windows",
            "N1QL-Arke",
            "YCSB",
            "YCSB-Hebe",
            "KV",
            "KV-DGM",
            "KV-Windows",
            "KV-Athena",
            "KV-Hercules",
            "Rebalance",
            "Rebalance-C1",
            "Rebalance-C2",
            "Rebalance-Demeter",
            "Rebalance-Large-Scale",
            "Rebalance-Large-Scale-C1",
            "Rebalance-Large-Scale-C2",
            "XDCR",
            "XDCR-Windows",
            "XDCR-C1",
            "XDCR-C2",
        }
        for pipeline in (
            "tests/pipelines/weekly-watson.json",
            "tests/pipelines/weekly-spock.json",
            "tests/pipelines/weekly-vulcan.json",
            "tests/pipelines/weekly-alice.json",
        ):
            with open(pipeline) as fh:
                test_cases = json.load(fh)
                self.assertEqual(stages, set(test_cases), pipeline)

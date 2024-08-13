import json
import os
import shutil
from enum import Enum
from functools import cached_property
from glob import glob

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.worker import vectordb_bench_task
from perfrunner.settings import TestConfig
from perfrunner.tests import PerfTest


class VectorDBBenchTest(PerfTest):
    COLLECTORS = {"fts_stats": True, "utilisation_stats": True}

    def __init__(self, cluster_spec: local.ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.vectordb_settings = test_config.vectordb_settings
        self.vectordb_settings.label = self.build_tag
        self.vectordb_settings.case_settings.update(
            self.get_case_settings(self.vectordb_settings.index_type)
        )

    @cached_property
    def build_tag(self):
        return os.getenv("BUILD_TAG", "local")

    def download_vectordb_bench(self):
        if self.worker_manager.is_remote:
            self.worker_manager.remote.download_vectordb_bench(
                repo=self.vectordb_settings.repo,
                branch=self.vectordb_settings.branch,
                worker_home=self.worker_manager.WORKER_HOME,
            )
        else:
            local.clone_git_repo(
                repo=self.vectordb_settings.repo, branch=self.vectordb_settings.branch
            )

    def build_vectordb_bench(self):
        if self.worker_manager.is_remote:
            self.worker_manager.remote.update_pyenv_and_install_python(py_version="3.11.8")
            self.worker_manager.remote.build_vectordb_bench(self.worker_manager.WORKER_HOME)
        else:
            local.build_vectordb_bench()

    @with_stats
    def run_workload(self):
        logger.info(f"Running the following cases: {self.vectordb_settings.cases}")
        phase = self.generic_phase(
            phase_name="workload case",
            default_settings=self.vectordb_settings,
            default_mixed_settings=None,
            task=vectordb_bench_task,
        )
        self.worker_manager.run_fg_phases(phase)

    def get_case_settings(self, index_type: str) -> dict:
        case_settings = {}
        if index_type.upper() == "FTS":
            # FTS specific stats settings
            self.jts_access = self.test_config.vectordb_settings
            self.jts_access.couchbase_index_name = "bucket-1_vector_index"
            self.jts_access.fts_index_map = {"fts_index_map": {"bucket": "bucket-1"}}
            return case_settings

        self.COLLECTORS = {
            "secondary_stats": True,
            "secondary_debugstats": True,
            "secondary_debugstats_bucket": True,
            "secondary_debugstats_index": True,
        }
        case_settings = {
            "description": self.test_config.gsi_settings.vector_description or "IVF,SQ8",
            "nprobes": self.test_config.gsi_settings.vector_nprobes,
        }
        if train_list := self.test_config.gsi_settings.vector_train_list:
            case_settings["train_list"] = train_list
        if scan_nprobes := self.test_config.gsi_settings.vector_scan_probes:
            case_settings["scan_nprobes"] = scan_nprobes
        if vector_similarity := self.test_config.gsi_settings.vector_similarity:
            case_settings["vector_similarity"] = vector_similarity
        return case_settings

    def collect_results(self) -> dict:
        database = self.vectordb_settings.database
        pattern = (
            f"VectorDBBench/vectordb_bench/results/{database}/"
            f"result_*_{self.build_tag}_{database.lower()}.json"
        )

        if self.worker_manager.is_remote:
            shutil.rmtree("VectorDBBench", ignore_errors=True)
            os.mkdir("VectorDBBench")
            self.worker_manager.remote.get_vectordb_result_files(
                self.worker_manager.WORKER_HOME, str(pattern)
            )
            # Change the pattern to the local downloaded path
            pattern = f"VectorDBBench/result_*_{self.build_tag}_{database.lower()}.json"

        results = dict()
        logger.info(f"Generating results from {pattern}")
        for filename in glob(pattern):
            with open(filename) as file:
                data = json.load(file)
                for result in data.get("results", []):
                    case_id = result.get("task_config", {}).get("case_config", {}).get("case_id")
                    results[case_id] = result.get("metrics")

        return results

    def _report_kpi(self, *args, **kwargs):
        for case, metrics in self.collect_results().items():
            case_type = CaseType(case)
            for metric in self.metrics.vectordb_bench_metrics(
                metrics=metrics,
                base_title=case_type.case_title,
                case_id=case_type.name.lower(),
            ):
                self.reporter.post(*metric)

    def run(self):
        self.download_vectordb_bench()
        self.build_vectordb_bench()

        self.run_workload()

        self.report_kpi()


class CaseType(Enum):
    """Possible cases as defined by VectorDBBench."""

    CapacityDim128 = 1
    CapacityDim960 = 2

    Performance768D100M = 3
    Performance768D10M = 4
    Performance768D1M = 5

    Performance768D10M1P = 6
    Performance768D1M1P = 7
    Performance768D10M99P = 8
    Performance768D1M99P = 9

    Performance1536D500K = 10
    Performance1536D5M = 11

    Performance1536D500K1P = 12
    Performance1536D5M1P = 13
    Performance1536D500K99P = 14
    Performance1536D5M99P = 15

    Custom = 100

    Performance1536D50K = 200  # May not exist on the main branch

    @property
    def case_title(self) -> dict:
        return CASE_DEFINITIONS.get(self, "Custom or unknown case")


CASE_DEFINITIONS = {
    # Load capacity cases
    CaseType.CapacityDim128: "Capacity (SIFT, 128 Dim Repeated)",
    CaseType.CapacityDim960: "Capacity (GIST, 960 Dim Repeated)",
    # Search performance cases
    CaseType.Performance768D100M: "Search Performance (LAION, 100M Dataset, 768 Dim)",
    CaseType.Performance768D10M: "Search Performance (COHERE, 10M Dataset, 768 Dim)",
    CaseType.Performance768D1M: "Search Performance (COHERE, 1M Dataset, 768 Dim)",
    CaseType.Performance1536D500K: "Search Performance (OpenAI, 500K Dataset, 1536 Dim)",
    CaseType.Performance1536D5M: "Search Performance (OpenAI, 5M Dataset, 1536 Dim)",
    CaseType.Performance1536D50K: "Search Performance (OpenAI, 50K Dataset, 1536 Dim)",
    # Filtering search performance cases
    CaseType.Performance768D10M1P: "Filtering Search Performance "
    "(COHERE, 10M Dataset, 768 Dim, Filter 1%)",
    CaseType.Performance768D1M1P: "Filtering Search Performance "
    "(COHERE, 1M Dataset, 768 Dim, Filter 1%)",
    CaseType.Performance768D10M99P: "Filtering Search Performance "
    "(COHERE, 10M Dataset, 768 Dim, Filter 99%)",
    CaseType.Performance768D1M99P: "Filtering Search Performance "
    "(COHERE, 1M Dataset, 768 Dim, Filter 99%)",
    CaseType.Performance1536D500K1P: "Filtering Search Performance "
    "(OpenAI, 500K Dataset, 1536 Dim, Filter 1%)",
    CaseType.Performance1536D5M1P: "Filtering Search Performance "
    "(OpenAI, 5M Dataset, 1536 Dim, Filter 1%)",
    CaseType.Performance1536D500K99P: "Filtering Search Performance "
    "(OpenAI, 500K Dataset, 1536 Dim, Filter 99%)",
    CaseType.Performance1536D5M99P: "Filtering Search Performance "
    "(OpenAI, 5M Dataset, 1536 Dim, Filter 99%)",
}

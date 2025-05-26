import json
import os
from glob import glob
from time import sleep
from typing import Optional
from uuid import uuid4

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.worker import aibench_task
from perfrunner.settings import AIGatewayTargetSettings, AIServicesSettings, ClusterSpec, TestConfig
from perfrunner.tests import PerfTest


class AIWorkflow:
    def __init__(
        self,
        ai_services_settings: AIServicesSettings,
        hosted_model_id: Optional[str],
        infra_uuid: Optional[str],
    ):
        self.flow_uuid = infra_uuid
        self.ai_services_settings = ai_services_settings
        self.hosted_model_id = hosted_model_id

    def _get_api_key(self):
        return os.getenv("PROVIDER_KEY", "")

    def _get_embedding_model(self):
        if self.ai_services_settings.model_source == "internal" and self.hosted_model_id:
            # If we have a model id and did not intentionally set source to external,
            # we are using a Capella hosted model
            return {"capellaHosted": {"id": self.hosted_model_id}}

        # Otherwise we are using an external model specified by the test
        return {
            "external": {
                "name": f"perftestkey{self.flow_uuid}",
                "modelName": self.ai_services_settings.model_name,
                "provider": self.ai_services_settings.model_provider,
                "apiKey": self._get_api_key(),
            }
        }

    def get_workflow_payload(
        self, bucket: str, scope: str = "_default", collection: str = "_default"
    ) -> dict:
        try:
            access_key_id, secret_access_key = local.get_aws_credential(
                self.ai_services_settings.aws_credential_path, True
            )
        except Exception:
            access_key_id, secret_access_key = ("", "")

        payload = {
            "type": self.ai_services_settings.workflow_type,
            "schemaFields": self.ai_services_settings.schema_fields,
            "embeddingModel": self._get_embedding_model(),
            "cbKeyspace": {
                "bucket": bucket,
                "scope": scope,
                "collection": collection,
            },
            "vectorIndexName": self.ai_services_settings.fts_index_name,
            "embeddingFieldName": "emb",
            "name": f"perfflow{self.flow_uuid}",
        }
        if self.ai_services_settings.workflow_type == "unstructured":
            payload.update(
                {
                    "chunkingStrategy": {
                        "strategyType": self.ai_services_settings.chunking_strategy,
                        "chunkSize": self.ai_services_settings.chunk_size,
                    },
                    "dataSource": {
                        "bucket": self.ai_services_settings.s3_bucket,
                        "path": self.ai_services_settings.s3_path,
                        "region": self.ai_services_settings.s3_bucket_region,
                        "accessKey": access_key_id,
                        "secretKey": secret_access_key,
                        "name": f"s3dataset{self.flow_uuid}",
                    },
                }
            )
        logger.info(f"Workflow payload: {payload}")
        return payload


class WorkflowIngestionAndLatencyTest(PerfTest):
    COLLECTORS = {
        "eventing_stats": True,
        "ns_server_system": True,
        "fts_stats": True,
        "ai_workflow_stats": True,
    }

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.ai_services_settings = self.test_config.ai_services_settings
        self.hosted_model_id = self._get_embedding_model_id()
        # FTS specific settings
        self.jts_access = self.test_config.access_settings
        self.jts_access.couchbase_index_name = self.ai_services_settings.fts_index_name
        self.jts_access.fts_index_map = {"fts_index_map": {"bucket": test_config.buckets[0]}}
        self.functions = {}

        self.runtimes = {}

    def _get_embedding_model_id(self) -> Optional[str]:
        """
        Retrieve the embedding model ID from the infrastructure model services.

        Returns:
            Optional[str]: The embedding model ID if available, otherwise None.
        """
        # Embedding model, when present, can be interated with the workflow
        models = self.cluster_spec.infrastructure_model_services
        return models.get("embedding-generation", {}).get("model_id")

    @timeit
    def deploy_workflow(self):
        try:
            self.workflow_id = self.rest.create_workflow(
                self.master_node, self.workflow.get_workflow_payload(self.test_config.buckets[0])
            )
            workflow_details = self.monitor.wait_for_workflow_status(
                host=self.eventing_nodes[0], workflow_id=self.workflow_id
            )
            logger.info(f"Workflow details: {pretty_dict(workflow_details)}")
            # When a workflow reaches a running state, it will start processing the data
        except Exception as e:
            logger.error(f"Error while waiting for workflow deployment: {e}")

    @timeit
    def destroy_workflow(self):
        try:
            self.rest.delete_workflow(self.master_node, self.workflow_id)
            self.monitor.wait_for_workflow_status(
                host=self.eventing_nodes[0], workflow_id=self.workflow_id, status=""
            )
        except Exception as e:
            logger.error(f"Error while waiting for undeploy: {e}")
            sleep(60)

    @timeit
    def data_ingestion(self):
        # We dont know how many docs we will endup with, as it depends on the workflow chunking
        # strategy. As such we need a way here to decide for how long to run the ingestion.
        # We have two options:
        # 1. Run for a fixed amount of time
        # 2. Run until all the UDS workflow runs finish

        access_settings = self.test_config.access_settings
        if access_settings.time > 0:
            logger.info(f"Running ingestion for {access_settings.time} seconds")
            sleep(access_settings.time)
        else:
            logger.info("Running ingestion. Waiting for workflow to complete")
            self.monitor.wait_for_workflow_status(
                host=self.eventing_nodes[0],
                workflow_id=self.workflow_id,
                status="completed",
                max_retries=1200,
            )

    def prepare_and_deploy_workflow(self):
        cluster_uuid = self.cluster_spec.infrastructure_settings.get("uuid", uuid4().hex[:6])
        self.workflow = AIWorkflow(self.ai_services_settings, self.hosted_model_id, cluster_uuid)
        sleep(30)  # collect some initial metrics before starting the workflow
        workflow_deploy_time = self.deploy_workflow()
        self.runtimes["workflow_deploy_time"] = workflow_deploy_time

        eventing_functions = (
            self.rest.get_eventing_apps(self.eventing_nodes[0]).get("apps", []) or []
        )
        if len(eventing_functions) > 0:
            logger.info(f"Eventing functions: {pretty_dict(eventing_functions)}")

    @with_stats
    def run_workflow(self):
        self.prepare_and_deploy_workflow()
        ingestion_time = self.data_ingestion()
        self.runtimes["ingestion_time"] = ingestion_time

    def run(self):
        self.run_workflow()

        latencies = self.get_eventing_latencies()
        logger.info(f"{latencies=}")
        self.report_kpi()

        self.runtimes["workflow_delete_time"] = self.destroy_workflow()
        logger.info(f"{self.runtimes=}")

    def _report_kpi(self):
        workflow_details = self.rest.get_workflow_details(self.master_node, self.workflow_id)
        uds_metadata = workflow_details.get("workflowRuns", [{}])[0].get("udsMetadata")
        ingestion_time = self.runtimes.get("ingestion_time", 0)
        if not ingestion_time:
            return
        self.reporter.post(
            *self.metrics.uds_throughput(
                ingestion_time=ingestion_time,
                num_successful_files=uds_metadata.get("numSuccessfulFiles", 0),
            )
        )

        if self.test_config.access_settings.time <= 0:
            # Also report ingestion time if we are not running for a fixed amount of time
            self.reporter.post(*self.metrics.uds_processing_time(ingestion_time))

    def get_eventing_latencies(self) -> dict:
        latency_stats = self.process_latency_stats()
        latencies = {}
        for percentile in self.test_config.access_settings.latency_percentiles:
            latencies[f"P{percentile}"] = self.metrics.function_latency(
                percentile=percentile, latency_stats=latency_stats
            )[0]
        return latencies

    def process_latency_stats(self):
        ret_val = {}
        all_stats = self.rest.get_eventing_stats(node=self.eventing_nodes[0], full_stats=True)
        for stat in all_stats:
            latency_stats = stat.get("latency_stats", {})
            ret_val[stat["function_name"]] = sorted(latency_stats.items(), key=lambda x: int(x[0]))

            curl_latency_stats = stat.get("curl_latency_stats", {})
            ret_val["curl_latency_" + stat["function_name"]] = sorted(
                curl_latency_stats.items(), key=lambda x: int(x[0])
            )

        return ret_val


class AutoVecWorkflowTest(WorkflowIngestionAndLatencyTest):
    @timeit
    def wait_for_eventing_backlog(self):
        self.monitor.monitor_eventing_dcp_mutation(
            self.eventing_nodes[0], self.test_config.load_settings.items
        )

    @with_stats
    def run_workflow(self):
        self.prepare_and_deploy_workflow()
        autovec_time = self.wait_for_eventing_backlog()
        self.runtimes["autovec_time"] = autovec_time
        sleep(120)  # collect metrics after eventing work is done

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.run_workflow()
        latencies = self.get_eventing_latencies()

        self.runtimes["workflow_delete_time"] = self.destroy_workflow()
        logger.info(f"{self.runtimes=}")
        logger.info(f"{latencies=}")


class AIGatewayTest(PerfTest):
    # Depending on the model
    MODEL_KIND_ENDPOINTS = {
        "embedding-generation": ["embeddings"],
        "text-generation": ["chat-completions"],
    }

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.aibench_settings = self.test_config.aibench_settings
        self.aibench_settings.tag = self.build_tag
        self.username, self.password = self.cluster_spec.rest_credentials

        model = self.cluster_spec.infrastructure_model_services.get(
            self.aibench_settings.model_kind
        )
        self.gateway_endpoint = model.get("model_endpoint")
        self.aibench_settings.model_name = model.get("model_name")
        self.target_iterator = [
            AIGatewayTargetSettings(
                host=self.cluster_spec.workers[0],
                bucket=self.test_config.buckets[0],
                username=self.username,
                password=self.password,
                endpoint=self.gateway_endpoint,
            )
        ]
        ai_gateway_info = self.rest.get_ai_gateway_info(self.gateway_endpoint)
        logger.info(f"AI Gateway info: {ai_gateway_info}")
        self.gateway_version = ai_gateway_info.get("version")
        self.reporter.build = f"{self.gateway_version}:{self.build}"

    @property
    def build_tag(self):
        return os.getenv("BUILD_TAG", "local")

    def download_aibench(self):
        if not self.worker_manager.is_remote:
            return
        # Download the repo locally first
        local.clone_git_repo(
            repo=self.aibench_settings.repo,
            branch=self.aibench_settings.branch,
            keep_if_exists=True,  # Workaround for local testing, no need to re-download the repo
        )
        local.copy_aibench_to_remote(
            hosts=self.cluster_spec.workers,
            user=self.cluster_spec.ssh_credentials[0],
            password=self.cluster_spec.ssh_credentials[1],
            worker_home=self.worker_manager.WORKER_HOME,
        )

    def build_aibench(self):
        if not self.worker_manager.is_remote:
            return
        self.worker_manager.remote.update_pyenv_and_install_python(py_version="3.11.8")
        self.worker_manager.remote.build_aibench(worker_home=self.worker_manager.WORKER_HOME)

    def run_aibench(self):
        for endpoint in self.MODEL_KIND_ENDPOINTS[self.aibench_settings.model_kind]:
            self.aibench_settings.endpoint = endpoint
            phase = self.generic_phase(
                phase_name="ai_bench",
                default_settings=self.aibench_settings,
                default_mixed_settings=None,
                task=aibench_task,
            )
            self.worker_manager.run_fg_phases(phase)

            sleep(120)  # Sleep to allow for rate limits to reset

    def collect_results(self) -> dict:
        if not self.worker_manager.is_remote:
            return {}
        self.worker_manager.remote.get_aibench_result_files(
            worker_home=self.worker_manager.WORKER_HOME
        )
        results = {}
        for filename in glob("ai_bench/results/*.json"):
            with open(filename) as file:
                results[filename] = json.load(file)
        return results

    def _report_kpi(self):
        for filename, results in self.collect_results().items():
            logger.info(f"Collected {filename}: {pretty_dict(results)}")

            if results.pop("errors"):
                # A workload is considered failed if it has any errors and will not be reported
                logger.error(f"Workload {filename} failed")
                continue
            for metric in self.metrics.aibench_metrics(results):
                self.reporter.post(*metric)

    def run(self):
        self.download_aibench()
        self.build_aibench()

        self.run_aibench()
        self.report_kpi()

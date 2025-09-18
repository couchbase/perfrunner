import json
import os
from glob import glob
from time import sleep
from typing import Optional
from uuid import uuid4

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.config_files import TimeTrackingFile
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.worker import aibench_task
from perfrunner.settings import (
    AIGatewayTargetSettings,
    AIServicesSettings,
    ClusterSpec,
    TestConfig,
)
from perfrunner.tests import PerfTest
from perfrunner.tests.fts import FTSTest
from perfrunner.tests.n1ql import N1QLTest
from perfrunner.utils.terraform import ControlPlaneManager


class AIWorkflow:
    def __init__(
        self,
        ai_services_settings: AIServicesSettings,
        hosted_model: dict,
        infra_uuid: Optional[str],
        s3_integration_id: Optional[str],
        openai_integration_id: Optional[str],
    ):
        self.flow_uuid = infra_uuid
        self.ai_services_settings = ai_services_settings
        self.hosted_model = hosted_model
        self.s3_integration_id = s3_integration_id
        self.openai_integration_id = openai_integration_id

    def _get_embedding_model(self) -> dict:
        if self.ai_services_settings.model_source == "internal" and self.hosted_model:
            # If we have a model id and did not intentionally set source to external,
            # we are using a Capella hosted model
            model_id = self.hosted_model.get("model_id")
            return {
                "capellaHosted": {
                    "id": model_id,
                    "modelName": self.hosted_model.get("model_name"),
                    "apiKeyId": ControlPlaneManager.get_model_api_key_id(model_id),
                    "apiKeyToken": ControlPlaneManager.get_model_api_key(model_id),
                    "privateEndpointEnabled": False,
                }
            }

        # Otherwise we are using an external model specified by the test
        return {
            "external": {
                "id": self.openai_integration_id,
                "modelName": self.ai_services_settings.model_name,
                "provider": self.ai_services_settings.model_provider,
            }
        }

    def get_workflow_payload(
        self,
        index_name: str,
        bucket: str,
        scope: str = "_default",
        collection: str = "_default",
    ) -> dict:
        payload = {
            "type": self.ai_services_settings.workflow_type,
            "embeddingModel": self._get_embedding_model(),
            "cbKeyspace": {
                "bucket": bucket,
                "scope": scope,
                "collection": collection,
            },
            "vectorIndexName": index_name,
            "embeddingFieldMappings": {
                "emb": {"sourceFields": self.ai_services_settings.schema_fields}
            },
            "name": f"perfflow{self.flow_uuid}",
        }
        if self.ai_services_settings.workflow_type == "unstructured":
            payload.update(
                {
                    "chunkingStrategy": {
                        "strategyType": self.ai_services_settings.chunking_strategy,
                        "chunkSize": self.ai_services_settings.chunk_size,
                    },
                    "dataSource": {"id": self.s3_integration_id},
                }
            )
            # Unstructured workflows should not set embeddingFieldMappings
            payload.pop("embeddingFieldMappings")
        logger.info(f"Workflow payload: {payload}")
        return payload


class WorkflowIngestionAndLatencyTest(FTSTest):
    COLLECTORS = {
        "eventing_stats": True,
        "ns_server_system": True,
        "fts_stats": True,
        "ai_workflow_stats": True,
    }

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.ai_services_settings = self.test_config.ai_services_settings
        self.hosted_model = self._get_embedding_model()
        self.functions = {}
        self.runtimes = {}
        # Default index name to use for the workflow
        self.fts_index_name = self.jts_access.couchbase_index_name
        self.s3_integration_id = ""
        self.openai_integration_id = ""
        self.workflow_id = None

    def _get_api_key(self):
        return os.getenv("PROVIDER_KEY", "")

    def _get_embedding_model(self) -> dict:
        """
        Retrieve the embedding model from the infrastructure model services.

        Returns:
            dict: The embedding model details if available, otherwise None.
        """
        # Embedding model, when present, can be interated with the workflow
        models = self.cluster_spec.infrastructure_model_services
        return models.get("embedding-generation", {})

    @timeit
    def deploy_workflow(self):
        try:
            payload = self.workflow.get_workflow_payload(
                self.fts_index_name, self.test_config.buckets[0]
            )
            self.workflow_id = self.rest.create_workflow(self.master_node, payload)
            # Store the workflow id in the cluster spec so it can be destroyed later
            self.cluster_spec.config.set("controlplane", "workflow_id", self.workflow_id)
            self.cluster_spec.update_spec_file()
            workflow_details = self.monitor.wait_for_workflow_status(
                host=self.eventing_nodes[0], workflow_id=self.workflow_id
            )
            logger.info(f"Workflow details: {pretty_dict(workflow_details)}")
            # When a workflow reaches a running state, it will start processing the data
        except Exception as e:
            logger.error(f"Error while waiting for workflow deployment: {e}")

    @timeit
    def wait_for_workflow_completion_or_time(self):
        # For UDS, we dont know how many docs we will endup with, as it depends on the workflow
        # chunking strategy. As such we need a way here to decide for how long to run the ingestion.
        # So we introduce two options:
        # 1. Run for a fixed amount of time
        # 2. Run until all the UDS workflow runs finish

        access_settings = self.test_config.access_settings
        if access_settings.time > 0:
            logger.info(f"Running workflow for {access_settings.time} seconds")
            sleep(access_settings.time)
        else:
            logger.info("Running workflow. Waiting for workflow to complete")
            self.monitor.wait_for_workflow_status(
                host=self.eventing_nodes[0],
                workflow_id=self.workflow_id,
                status="completed",
                max_retries=1200,
            )

    def create_integrations(self, cluster_uuid: str):
        """
        Create integrations for the workflow if needed.

        - S3 integration for unstructured workflows
        - OpenAI integration for internal models
        """
        # Check if we need to create an s3 integration
        if self.ai_services_settings.workflow_type == "unstructured":
            try:
                access_key_id, secret_access_key = local.get_aws_credential(
                    self.ai_services_settings.aws_credential_path, True
                )
            except Exception:
                access_key_id, secret_access_key = ("", "")

            self.s3_integration_id = self.rest.create_s3_integration(
                name=f"perfs3{cluster_uuid}",
                access_key=access_key_id,
                secret_key=secret_access_key,
                bucket=self.ai_services_settings.s3_bucket,
                region=self.ai_services_settings.s3_bucket_region,
                folder_path=self.ai_services_settings.s3_path,
            )
            logger.info(f"Created s3 integration: {self.s3_integration_id}")

        # Check if we need to create an openai integration
        if self.ai_services_settings.model_source == "internal":
            # If we are using an internal model, we don't need to create an openai integration
            return

        self.openai_integration_id = self.rest.create_openai_integration(
            name=f"perfopenai{cluster_uuid}",
            access_key=self._get_api_key(),
        )
        logger.info(f"Created openAI integration: {self.openai_integration_id}")

    def prepare_and_deploy_workflow(self):
        cluster_uuid = self.cluster_spec.infrastructure_settings.get("uuid", uuid4().hex[:6])
        self.create_integrations(cluster_uuid)
        self.workflow = AIWorkflow(
            self.ai_services_settings,
            self.hosted_model,
            cluster_uuid,
            self.s3_integration_id,
            self.openai_integration_id,
        )
        sleep(30)  # collect some initial metrics before starting the workflow
        workflow_deploy_time = self.deploy_workflow()
        if not self.workflow_id:
            raise Exception("Failed to create workflow")

        self.runtimes["workflow_deploy_time"] = workflow_deploy_time

        eventing_functions = (
            self.rest.get_eventing_apps(self.eventing_nodes[0]).get("apps", []) or []
        )
        if len(eventing_functions) > 0:
            logger.info(f"Eventing functions: {pretty_dict(eventing_functions)}")

    def create_indexes_for_workflow(self):
        if not self.ai_services_settings.create_index:
            return
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.wait_for_index_persistence()
        # If the FTS index was created by the test, the name will have changed.
        # Get the new name from the map
        self.fts_index_name = list(self.fts_index_map.keys())[0]

    @with_stats
    def run_workflow(self):
        self.prepare_and_deploy_workflow()
        workflow_time = self.wait_for_workflow_completion_or_time()
        self.runtimes["workflow_time"] = workflow_time

    def run(self):
        self.create_indexes_for_workflow()

        self.run_workflow()

        latencies = self.get_eventing_latencies()
        logger.info(f"{latencies=}")
        logger.info(f"{self.runtimes=}")

        self.report_kpi()

    def _report_kpi(self):
        workflow_details = self.rest.get_workflow_details(self.master_node, self.workflow_id)
        uds_metadata = workflow_details.get("workflowRuns", [{}])[0].get("udsMetadata", {})
        autovec_metadata = workflow_details.get("workflowRuns", [{}])[0].get(
            "vectorizationMetadata", {}
        )
        workflow_time = self.runtimes.get("workflow_time", 0)
        logger.info(f"UDS metadata: {uds_metadata}")
        logger.info(f"Autovec metadata: {autovec_metadata}")
        if not workflow_time:
            return

        model_name = self.ai_services_settings.model_name
        if self.hosted_model:
            model_name = self.hosted_model.get("model_name")

        if success_files := uds_metadata.get("numSuccessfulFiles", 0):
            self.reporter.post(
                *self.metrics.uds_throughput(
                    ingestion_time=workflow_time,
                    num_successful_files=success_files,
                    model_name=model_name,
                )
            )
        if success_embeddings := autovec_metadata.get("numSuccessfulEmbeddingWrites", 0):
            self.reporter.post(
                *self.metrics.vectorization_throughput(
                    autovec_time=workflow_time,
                    num_successful_embeddings=success_embeddings,
                    model_name=model_name,
                )
            )

        if self.test_config.access_settings.time <= 0:
            # Also report workflow execution time if we are not running for a fixed duration
            self.reporter.post(
                *self.metrics.workflow_execution_time(
                    workflow_time=workflow_time, model_name=model_name
                )
            )

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

    def __exit__(self, exc_type, exc_value, traceback):
        # Before doing other cleanup, we need to collect eventing app logs
        eventing_logs = self.rest.get_eventing_app_logs(
            self.eventing_nodes[0], self.test_config.buckets[0]
        )
        if eventing_logs:
            # store each log to a file function_name.log
            for function_name, log in eventing_logs.items():
                with open(f"{function_name}.log", "w") as f:
                    f.write(log)

        super().__exit__(exc_type, exc_value, traceback)


class AutoVecWorkflowTest(WorkflowIngestionAndLatencyTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        super().run()


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
        self.api_key = ControlPlaneManager.get_model_api_key(model.get("model_id"))
        first_bucket = self.test_config.buckets[0] if self.test_config.buckets else ""
        self.target_iterator = [
            AIGatewayTargetSettings(
                host="",
                bucket=first_bucket,
                endpoint=self.gateway_endpoint,
                api_key=self.api_key,
            )
        ]

        self.gateway_version = self.get_ai_gateway_info().get("version")
        cp_version = self.rest.get_cp_version()
        build_str = f"{self.gateway_version}:{cp_version}"
        if self.build != "0.0.0":
            build_str = f"{self.build}:{build_str}"
        self.reporter.build = build_str

    def get_ai_gateway_info(self) -> dict:
        """Get the AI Gateway information."""
        resp_data = self.rest.get(
            url=f"{self.gateway_endpoint}/v1/info",
            headers={"Authorization": f"Bearer {self.api_key}"},
            auth=None,  # Ensures it is not overridden with basic auth
        ).json()
        resp_data.pop("models", [])  # Remove models from the response due to large size
        logger.info(f"AI Gateway info: {pretty_dict(resp_data)}")
        return resp_data

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
            for concurrency in self.aibench_settings.concurrencies:
                logger.info(f"Running AI Bench with {concurrency} workers on {endpoint} endpoint")
                self.aibench_settings.workers = concurrency
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

    def report_model_deployment_time(self):
        with TimeTrackingFile() as timings:
            model_deployment_time = timings.get("model_deployment")

        if not model_deployment_time:
            return

        models = self.cluster_spec.infrastructure_model_services
        for model_kind, deployment_time in model_deployment_time.items():
            model_name = models.get(model_kind, {}).get("model_name", "")
            self.reporter.post(
                *self.metrics.model_deployment_time(deployment_time, model_name, model_kind)
            )

    def _report_kpi(self):
        self.report_model_deployment_time()
        for filename, results in self.collect_results().items():
            logger.info(f"Collected {filename}: {pretty_dict(results)}")

            if results.pop("errors"):
                # A workload has failed requests, log the error but report the calculated results
                logger.error(f"Workload {filename} contains errors")

            for metric in self.metrics.aibench_metrics(results):
                self.reporter.post(*metric)


    def run(self):
        self.download_aibench()
        self.build_aibench()

        self.run_aibench()
        self.report_kpi()


class AIFunctionsTest(N1QLTest):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.ai_services_settings = self.test_config.ai_services_settings
        self.llm_model = self._get_llm_model()
        self.model_name = self.ai_services_settings.model_name
        if self.llm_model:
            self.model_name = self.llm_model.get("model_name")

    def _get_llm_model(self) -> dict:
        """
        Retrieve the LLM model from the infrastructure model services.

        Returns:
            dict: The LLM model details if available.
        """
        models = self.cluster_spec.infrastructure_model_services
        return models.get("text-generation", {})

    def _create_ai_functions_payload(self) -> dict:
        provider = self.ai_services_settings.model_provider.lower()
        payload = {
            "name": self.ai_services_settings.functions_names,
            "modelSource": provider,
        }
        if provider == "capella":
            model_id = self.llm_model.get("model_id")
            payload.update(
                {
                    f"{provider}Model": {
                        "modelName": self.model_name,
                        "modelID": model_id,
                        "apiKeyID": ControlPlaneManager.get_model_api_key_id(model_id),
                        "apiKeyToken": ControlPlaneManager.get_model_api_key(model_id),
                        "privateNetworking": False,
                        "region": self.cluster_spec.cloud_region,
                        "aiGatewayURL": self.llm_model.get("model_endpoint"),
                    },
                }
            )
        else:
            payload.update(
                {
                    f"{provider}Model": {
                        "modelID": self.model_name,
                        "integrationID": self.openai_integration_id,
                    }
                }
            )

        return payload

    def create_openai_integration(self, cluster_uuid: str):
        """
        Create integrations to use for the AI functions.

        - OpenAI integration for external models
        """
        # Check if we need to create an openai integration
        if self.ai_services_settings.model_provider.lower() == "capella":
            # If we are using a Capella model, we don't need to create an integration
            return

        self.openai_integration_id = self.rest.create_openai_integration(
            name=f"perfopenai{cluster_uuid}",
            access_key=os.getenv("PROVIDER_KEY", ""),
        )
        logger.info(f"Created openAI integration: {self.openai_integration_id}")

    def deploy_ai_functions(self):
        uuid = self.cluster_spec.infrastructure_settings.get("uuid", uuid4().hex[:6])
        self.create_openai_integration(uuid)
        payload = self._create_ai_functions_payload()
        logger.info(f"Deploying AI functions with payload: {pretty_dict(payload)}")
        self.rest.create_ai_functions(self.master_node, payload)
        # Cant deteministically monitor deployment due to AV-108636, so wait for 30 seconds
        sleep(30)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.create_indexes()
        self.wait_for_indexing()
        self.store_plans()

        self.deploy_ai_functions()
        # Workaround for AV-110058
        self.rest.refresh_cluster_allowlist(self.master_node)

        self.access_bg()
        self.access()

        self.report_kpi()

class QueryThroughputWithAIFunctionsTest(AIFunctionsTest):
    def _report_kpi(self):
        self.reporter.post(*self.metrics.avg_n1ql_throughput(self.master_node))


class QueryLatencyWithAIFunctionsTest(AIFunctionsTest):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.COLLECTORS["n1ql_latency"] = True

    def _report_kpi(self):
        self.reporter.post(*self.metrics.query_latency(percentile=90))

import json
import os
from glob import glob
from time import sleep
from typing import Callable, Optional
from uuid import uuid4

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.config_files import TimeTrackingFile
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.rest import RestType
from perfrunner.helpers.worker import aibench_task
from perfrunner.settings import (
    AIGatewayTargetSettings,
    AIServicesSettings,
    ClusterSpec,
    TestConfig,
)
from perfrunner.tests import PerfTest
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
                    "apiKeyId": ControlPlaneManager.get_model_api_key_id(),
                    "apiKeyToken": ControlPlaneManager.get_model_api_key(),
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
            "createIndexes": self.ai_services_settings.create_index,
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
                    "pageNumbers": self.ai_services_settings.pages,
                    "exclusions": self.ai_services_settings.exclusions,
                }
            )
            # Unstructured workflows should not set embeddingFieldMappings
            payload.pop("embeddingFieldMappings")
        logger.info(f"Workflow payload: {payload}")
        return payload


class HostedModelInfo:
    """Manage information about hosted models and AI Gateway."""

    def __init__(self, models: dict, rest: RestType, monitor_func: Callable[[], dict]):
        self.rest = rest
        self.models = models
        self.embedding_model = self.models.get("embedding-generation", {})
        self.llm_model = self.models.get("text-generation", {})

        self.ai_gateway_version = None
        self.model_endpoint = None
        self.api_key = None
        # If any of the model kind is available, the endpoint will be the same for both
        if self.embedding_model:
            self.model_endpoint = self.embedding_model.get("model_endpoint")
        if self.llm_model:
            self.model_endpoint = self.llm_model.get("model_endpoint")

        if not self.model_endpoint:
            return

        self.api_key = ControlPlaneManager.get_model_api_key()
        resp_data = self.rest.get(
            url=f"{self.model_endpoint}/v1/info",
            headers={"Authorization": f"Bearer {self.api_key}"},
            auth=None,  # Ensures it is not overridden with basic auth
        ).json()
        resp_data.pop("models", [])  # Remove models from the response due to large size
        logger.info(f"AI Gateway info: {pretty_dict(resp_data)}")
        self.ai_gateway_version = resp_data.get("version")

        self._wait_for_ai_gateway_models_health(monitor_func)

    def _get_ai_gateway_models_status(self) -> dict:
        resp_data = self.rest.get(
            url=f"{self.model_endpoint}/v1/models",
            headers={"Authorization": f"Bearer {self.api_key}"},
            auth=None,  # Ensures it is not overridden with basic auth
        ).json()
        return resp_data.get("data", [])

    def _wait_for_ai_gateway_models_health(self, monitor_func: Callable[[], dict]):
        if not self.model_endpoint:
            return

        monitor_func(self._get_ai_gateway_models_status)


class AIServicesTest(PerfTest):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.ai_services_settings = self.test_config.ai_services_settings
        # Initialise models information
        self.hosted_model_info = HostedModelInfo(
            self.cluster_spec.infrastructure_model_services,
            self.rest,
            self.monitor.wait_for_ai_gateway_models_health,
        )
        # Include Capella and AI-Gateway versions when present
        cp_version = self.rest.get_cp_version()
        build_str = f"{self.hosted_model_info.ai_gateway_version}:{cp_version}"
        if self.build != "0.0.0":
            build_str = f"{build_str}:{self.build}"
        self.reporter.build = build_str

    def get_provider_api_key(self) -> str:
        return os.getenv("PROVIDER_KEY", "")


class WorkflowIngestionAndLatencyTest(AIServicesTest):
    COLLECTORS = {
        "eventing_stats": True,
        "ns_server_system": True,
        "ai_workflow_stats": True,
    }

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.functions = {}
        self.runtimes = {}
        self.s3_integration_id = ""
        self.openai_integration_id = ""
        self.workflow_id = None
        self.wer_results = None
        self.f1_results = None

    @timeit
    def deploy_workflow(self):
        try:
            payload = self.workflow.get_workflow_payload(self.test_config.buckets[0])
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
        try:
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
        except Exception as e:
            # When doing E2E, an AutoVec workflow can fail although ingestion was successful.
            # We can still process the UDS workflow results in this case.
            logger.error(f"Error while waiting for workflow completion: {e}")

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
            access_key=self.get_provider_api_key(),
        )
        logger.info(f"Created openAI integration: {self.openai_integration_id}")

    def prepare_and_deploy_workflow(self):
        cluster_uuid = self.cluster_spec.infrastructure_settings.get("uuid", uuid4().hex[:6])
        self.create_integrations(cluster_uuid)
        self.workflow = AIWorkflow(
            self.ai_services_settings,
            self.hosted_model_info.embedding_model,
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

    @with_stats
    def run_workflow(self):
        self.prepare_and_deploy_workflow()
        workflow_time = self.wait_for_workflow_completion_or_time()
        self.runtimes["workflow_time"] = workflow_time

    def run(self):
        self.run_workflow()

        latencies = self.get_eventing_latencies()
        logger.info(f"{latencies=}")
        logger.info(f"{self.runtimes=}")

        # Calculate WER scores
        if self.ai_services_settings.do_e2e_evaluation:
            sleep(120)  # Sleep to allow for rate limits to reset
            self.calculate_wer_and_f1_scores()

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

        model_name = self.ai_services_settings.model_name
        if self.hosted_model_info.embedding_model:
            model_name = self.hosted_model_info.embedding_model.get("model_name")

        if self.wer_results:
            avg_wer = float(self.wer_results.get("avg_wer", 1.0))
            self.reporter.post(
                *self.metrics.custom_metric(
                    round(avg_wer, 2),
                    f"Average WER, {{}} ({model_name})",
                    "avg_wer",
                )
            )

        if self.f1_results:
            avg_f1 = float(self.f1_results.get("avg_f1", 1.0))
            self.reporter.post(
                *self.metrics.custom_metric(
                    round(avg_f1, 2),
                    f"Average F1 score, {{}} {model_name}",
                    "avg_f1",
                )
            )

        if not workflow_time:
            return

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

    def calculate_wer_and_f1_scores(self):
        try:
            # Initialise SimpleRAG
            from perfrunner.utils.ai.rag import SimpleRAG

            llm_model_name = None
            embedding_model_name = self.hosted_model_info.embedding_model.get("model_name")
            if self.hosted_model_info.llm_model:
                llm_model_name = self.hosted_model_info.llm_model.get("model_name")

            simple_rag = SimpleRAG(
                model_endpoint=self.hosted_model_info.model_endpoint,
                master_node=self.master_node,
                api_key=self.hosted_model_info.api_key,
                embedding_model_name=embedding_model_name,
                llm_model_name=llm_model_name,
                credentials=self.cluster_spec.rest_credentials,
                bucket_name=self.test_config.buckets[0],
                gt_file_path=self.ai_services_settings.gt_file_path,
            )

            # Run WER calculation
            self.wer_results = simple_rag.run_wer_calculation()
            logger.info(f"WER calculation results: {pretty_dict(self.wer_results)}")

            # To calculate F1 score we need both LLM and embedding model to be available
            if self.hosted_model_info.llm_model and self.hosted_model_info.embedding_model:
                self.f1_results = simple_rag.run_f1_calculation()
                logger.info(f"F1 calculation results: {pretty_dict(self.f1_results)}")

        except Exception as e:
            logger.error(f"Error during E2E evaluation: {e}")

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


class AIGatewayTest(AIServicesTest):
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

        self.aibench_settings.model_name = self.hosted_model_info.models.get(
            self.aibench_settings.model_kind
        ).get("model_name")
        first_bucket = self.test_config.buckets[0] if self.test_config.buckets else ""
        self.target_iterator = [
            AIGatewayTargetSettings(
                host="",
                bucket=first_bucket,
                endpoint=self.hosted_model_info.model_endpoint,
                api_key=self.hosted_model_info.api_key,
            )
        ]

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


class AIFunctionsTest(AIServicesTest, N1QLTest):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.model_name = self.ai_services_settings.model_name
        self.model_id = ""
        if self.hosted_model_info.llm_model:
            self.model_name = self.hosted_model_info.llm_model.get("model_name")
            self.model_id = self.hosted_model_info.llm_model.get("model_id")

    def _create_ai_functions_payload(self) -> dict:
        provider = self.ai_services_settings.model_provider.lower()
        payload = {
            "name": self.ai_services_settings.functions_names,
            "modelSource": provider,
        }
        if provider == "capella":
            payload.update(
                {
                    f"{provider}Model": {
                        "modelName": self.model_name,
                        "modelID": self.model_id,
                        "apiKeyID": ControlPlaneManager.get_model_api_key_id(),
                        "apiKeyToken": ControlPlaneManager.get_model_api_key(),
                        "privateNetworking": False,
                        "region": self.cluster_spec.cloud_region,
                        "aiGatewayURL": self.hosted_model_info.model_endpoint,
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
            access_key=self.get_provider_api_key(),
        )
        logger.info(f"Created openAI integration: {self.openai_integration_id}")

    @timeit
    def deploy_ai_functions(self):
        uuid = self.cluster_spec.infrastructure_settings.get("uuid", uuid4().hex[:6])
        self.create_openai_integration(uuid)
        payload = self._create_ai_functions_payload()
        logger.info(f"Deploying AI functions with payload: {pretty_dict(payload)}")
        self.rest.create_ai_functions(self.master_node, payload)
        self.monitor.wait_for_ai_functions_healthy(
            self.master_node, self.ai_services_settings.functions_names
        )

    def run(self):
        functions_deployment_time = self.deploy_ai_functions()
        logger.info(f"AI Functions deployment time: {functions_deployment_time} seconds")
        # Workaround for AV-110058
        self.rest.refresh_cluster_allowlist(self.master_node)

        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.create_indexes()
        self.wait_for_indexing()
        self.store_plans()

        self.access_bg()
        self.access()

        self.report_kpi()

class QueryThroughputWithAIFunctionsTest(AIFunctionsTest):
    def _report_kpi(self):
        model_name_title = f"({self.model_name})"
        self.reporter.post(
            *self.metrics.avg_n1ql_throughput(
                self.master_node, custom_title_postfix=model_name_title
            )
        )


class QueryLatencyWithAIFunctionsTest(AIFunctionsTest):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        super().__init__(cluster_spec, test_config, verbose)
        self.COLLECTORS["n1ql_latency"] = True

    def _report_kpi(self):
        model_name_title = f"({self.model_name})"
        self.reporter.post(
            *self.metrics.query_latency(percentile=90, custom_title_postfix=model_name_title)
        )

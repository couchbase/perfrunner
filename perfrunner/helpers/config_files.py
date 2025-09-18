import json
import os
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from time import time
from typing import Callable, Optional, Union

import yaml
from decorator import decorator

from logger import logger
from perfrunner.helpers.misc import create_build_tuple, pretty_dict
from perfrunner.settings import BucketSettings, ClusterSpec, Config, TestConfig


@decorator
def supported_for(
    method: Callable, since: tuple = None, upto: tuple = None, feature: str = None, *args, **kwargs
):
    version = create_build_tuple(args[0].version or "1.0.0")
    if not since or since <= version:
        if not upto or upto >= version:
            return method(*args, **kwargs)

    msg_args = [
        f"Ignoring setting {feature}. Feature only supported",
        f"from version {since}" if since else "",
        f"upto version {upto}" if upto else "",
    ]
    logger.warn(" ".join(msg_args))


@contextmanager
def record_time(operation_name: str, sub_operation_name: Optional[str] = None):
    """Record the execution time of a code block to the TimeTrackingFile.

    Args:
        operation_name: Name to use as the key in timing data.
        sub_operation_name: Optional name to use as the inner key in timing data.
    Time is recorded in seconds.
    """
    t0 = time()
    try:
        yield
    finally:
        duration = time() - t0
        with TimeTrackingFile() as t:
            t.update(operation_name, duration, sub_operation_name)


class FileType(Enum):
    INI = 0
    YAML = 1
    JSON = 2


class ConfigFile:
    """Base helper class for interacting with YAML, JSON and INI files.

    Classes inheriting from this can be used with or without a context manager.
    With context manager, `load_config()` will be called automatic during entering the context and
    `write()` will be called when exiting the context. When not using a context manager, one will
    need to manually call `load_config()` and appropriately call `write()`. This is useful when
    loading configuration is not needed, and rather just source or dest paths are needed.
    For example: `MyObjectClass().dest_file`.
    """

    def __init__(self, file_path: str, file_type: FileType = None):
        self.source_file = self.dest_file = file_path
        self.file_type = file_type or self._get_type()

    def _get_type(self):
        if self.source_file.endswith(".yaml"):
            return FileType.YAML
        elif self.source_file.endswith(".json"):
            return FileType.JSON
        return FileType.INI

    def __enter__(self):
        self.load_config()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.write()

    def load_config(self):
        # Some YAML files may contain more that one dict objects. This is stored in `all_config`.
        self.all_configs = self.read_file()
        # For most other use cases, `config` is the content of the whole file.
        self.config = self.all_configs[0]

    def reload_from_dest(self):
        """Reload the content of a destination file and use it for config data.

        This is useful when source and dest files are different and the dest file has been written
        to before. It should be called first before updating anything in the config.
        """
        self.source_file = self.dest_file
        self.load_config()

    def read_file(self) -> list[dict]:
        """Read the content of the source file based on the file type.

        Returns a list of python dict objects as read from the file. For most file types, this will
        be just one object. Some YAML files may, however, contain more than one objetcs.
        """
        # If the source file does not exist, return an empty dict.
        # File will be created when calling write()
        if not Path(self.source_file).is_file():
            self.ini_config = None
            return [{}]

        if self.file_type != FileType.INI:
            with open(self.source_file) as file:
                return (
                    list(yaml.load_all(file, Loader=yaml.FullLoader))
                    if self.file_type == FileType.YAML
                    else [json.load(file)]
                )
        else:
            self.ini_config = Config()
            self.ini_config.parse(self.source_file)
            # For simplicity, we can treat ConfigParser object as a dict
            return [self.ini_config.config]

    def write(self):
        """Write the content of all configurations to the destination file."""
        if self.file_type != FileType.INI:
            with open(self.dest_file, "w") as file:
                if self.file_type == FileType.YAML:
                    yaml.dump_all(self.all_configs, file)
                else:
                    json.dump(self.config, file, indent=4)
        else:
            if not self.ini_config:
                # The file didnt exist and we are working with an empty config
                self.ini_config = Config()
                self.ini_config.config.read_dict(self.config)
            self.ini_config.update_spec_file(self.dest_file)


class CAOFiles(ConfigFile):
    """Base class for all CAO YAML configurations."""

    def __init__(self, template_name: str, version: str):
        path = f"cloud/operator/templates/{template_name}.yaml"
        super().__init__(path, FileType.YAML)
        self.version = version
        dest_name = template_name.removesuffix("_template")
        self.dest_file = f"cloud/operator/{dest_name}.yaml"


class CAOConfigFile(CAOFiles):
    """CAO config file for deploying the operator and admission controller.

    It configures:
    - CAO and admission controller deployment (pods and services)
    - CAO and admission controller service account
    - Role and role bindings
    """

    def __init__(self, version: str, operator_tag: str, controller_tag: str):
        super().__init__("config_template", version)
        self.operator_tag = operator_tag
        self.controller_tag = controller_tag

    def setup_config(self):
        self._inject_config_tag()
        self._inject_version_annotations()

    def _inject_config_tag(self):
        for config in self.all_configs:
            if config["kind"] != "Deployment":
                continue
            if config["metadata"]["name"] == "couchbase-operator":
                config["spec"]["template"]["spec"]["containers"][0]["image"] = self.operator_tag
            else:
                config["spec"]["template"]["spec"]["containers"][0]["image"] = self.controller_tag

    def _inject_version_annotations(self):
        for config in self.all_configs:
            config["metadata"]["annotations"]["config.couchbase.com/version"] = self.version


class CAOCouchbaseClusterFile(CAOFiles):
    """Couchbase cluster configuration."""

    CLUSTER_DEST_DIR = "cloud/operator"

    def __init__(
        self,
        version: str,
        cluster_spec: ClusterSpec,
        cluster_name: str,
        test_config: TestConfig = None,
    ):
        super().__init__("couchbase-cluster_template", version)
        self.cluster_name = cluster_name
        self.dest_file = f"{self.CLUSTER_DEST_DIR}/{self.cluster_name}.yaml"
        self.test_config = test_config or TestConfig()
        self.cluster_spec = cluster_spec

    def set_cluster_name(self):
        self.config["metadata"]["name"] = self.cluster_name

    def set_server_spec(self, server_tag: str):
        self.config["spec"]["image"] = server_tag

    def set_server_count(self):
        self.config["spec"]["servers"][0]["size"] = self.test_config.cluster.initial_nodes[0]

    def set_backup(self, backup_tag: str):
        # Only setup backup for backup tests
        if backup_tag:
            self.config["spec"]["backup"]["image"] = backup_tag
        else:
            self.config.get("spec", {}).pop("backup")

    def set_exporter(self, exporter_tag: str, refresh_rate: int):
        # Only deploy the exporter sidecar if requested
        if exporter_tag:
            self.config["spec"]["monitoring"]["prometheus"].update(
                {"image": exporter_tag, "refreshRate": refresh_rate}
            )
        else:
            self.config.get("spec", {}).pop("monitoring")

    def set_memory_quota(self):
        self.config["spec"]["cluster"].update(
            {
                "dataServiceMemoryQuota": f"{self.test_config.cluster.mem_quota}Mi",
                "indexServiceMemoryQuota": f"{self.test_config.cluster.index_mem_quota}Mi",
            }
        )

        if self.test_config.cluster.fts_index_mem_quota:
            self.config["spec"]["cluster"].update(
                {"searchServiceMemoryQuota": f"{self.test_config.cluster.fts_index_mem_quota}Mi"}
            )
        if self.test_config.cluster.analytics_mem_quota:
            self.config["spec"]["cluster"].update(
                {"analyticsServiceMemoryQuota": f"{self.test_config.cluster.analytics_mem_quota}Mi"}
            )
        if self.test_config.cluster.eventing_mem_quota:
            self.config["spec"]["cluster"].update(
                {"eventingServiceMemoryQuota": f"{self.test_config.cluster.eventing_mem_quota}Mi"}
            )

    def set_index_settings(self):
        index_nodes = self.cluster_spec.servers_by_role("index")
        settings = self.test_config.gsi_settings.settings
        if index_nodes and settings:
            self.config["spec"]["cluster"].update(
                {"indexStorageSetting": settings["indexer.settings.storage_mode"]}
            )

    def set_services(self):
        server_types = dict()
        server_roles = self.cluster_spec.roles
        for _, role in server_roles.items():
            # If role is empty, it is a node used for overprovisioning, so dont set it up
            if not role:
                continue

            role = role.replace("kv", "data").replace("n1ql", "query")
            server_type_count = server_types.get(role, 0)
            server_types[role] = server_type_count + 1

        istio = str(self.cluster_spec.istio_enabled(cluster_name="k8s_cluster_1")).lower()

        cluster_servers = []
        volume_claims = []
        for server_role, server_role_count in server_types.items():
            node_selector = {"NodeRoles": "couchbase1"}
            spec = {
                "imagePullSecrets": [{"name": "regcred"}],
                "nodeSelector": node_selector,
            }

            sg_name = server_role.replace(",", "-")
            sg_def = self.test_config.get_sever_group_definition(sg_name)
            sg_nodes = int(sg_def.get("nodes", server_role_count))
            volume_size = sg_def.get("volume_size", "1000GB")
            volume_size = volume_size.replace("GB", "Gi")
            volume_size = volume_size.replace("MB", "Mi")
            pod_def = {
                "spec": spec,
                "metadata": {"annotations": {"sidecar.istio.io/inject": istio}},
            }

            server_def = {
                "name": sg_name,
                "services": server_role.split(","),
                "pod": pod_def,
                "size": sg_nodes,
                "volumeMounts": {"default": sg_name},
            }

            volume_claim_def = {
                "metadata": {"name": sg_name},
                "spec": {"resources": {"requests": {"storage": volume_size}}},
            }
            cluster_servers.append(server_def)
            volume_claims.append(volume_claim_def)

        self.config["spec"]["servers"] = cluster_servers
        self.config["spec"]["volumeClaimTemplates"] = volume_claims

    def configure_auto_compaction(self):
        compaction_settings = self.test_config.compaction
        db_percent = int(compaction_settings.db_percentage)
        views_percent = int(compaction_settings.view_percentage)

        self.config["spec"]["cluster"].update(
            {
                "autoCompaction": {
                    "databaseFragmentationThreshold": {"percent": db_percent},
                    "viewFragmentationThreshold": {"percent": views_percent},
                    "parallelCompaction": bool(str(compaction_settings.parallel).lower()),
                }
            }
        )

    def set_auto_failover(self):
        enabled = self.test_config.bucket.autofailover_enabled
        failover_timeouts = self.test_config.bucket.failover_timeouts
        disk_failover_timeout = self.test_config.bucket.disk_failover_timeout

        self.config["spec"]["cluster"].update(
            {
                "autoFailoverMaxCount": 1,
                "autoFailoverServerGroup": bool(enabled),
                "autoFailoverOnDataDiskIssues": bool(enabled),
                "autoFailoverOnDataDiskIssuesTimePeriod": f"{disk_failover_timeout}s",
                "autoFailoverTimeout": f"{failover_timeouts[-1]}s",
            }
        )

    def set_cpu_settings(self):
        server_groups = self.config["spec"]["servers"]
        online_vcpus = self.test_config.cluster.online_cores * 2
        for server_group in server_groups:
            server_group.update({"resources": {"limits": {"cpu": online_vcpus}}})

    def set_memory_settings(self):
        if not self.test_config.cluster.kernel_mem_limit:
            return

        tune_services = set()
        # CAO uses different service names than perfrunner
        for service in self.test_config.cluster.kernel_mem_limit_services:
            if service == "kv":
                service = "data"
            elif service == "n1ql":
                service = "query"
            elif service == "fts":
                service = "search"
            elif service == "cbas":
                service = "analytics"
            tune_services.add(service)

        server_groups = self.config["spec"]["servers"]
        kernel_memory = self.test_config.cluster.kernel_mem_limit
        for server_group in server_groups:
            services_in_group = set(server_group["services"])
            if services_in_group.intersection(tune_services) and kernel_memory != 0:
                server_group.update({"resources": {"limits": {"memory": f"{kernel_memory}Mi"}}})

    def configure_autoscaling(self, sever_group: str):
        server_groups = self.config["spec"]["servers"]
        for server_group in server_groups:
            if server_group["name"] == server_group:
                server_group.update({"autoscaleEnabled": True})

    def configure_upgrade(self):
        upgrade_settings = self.test_config.upgrade_settings
        if not upgrade_settings.target_version:
            return

        self.config["spec"].update(
            {
                "upgradeProcess": upgrade_settings.upgrade_process,
                "upgradeStrategy": upgrade_settings.upgrade_strategy,
            }
        )

    @supported_for(since=(2, 6, 0), feature="CNG")
    def set_cng_version(self, cng_tag: Optional[str]):
        if not cng_tag:
            return

        self.config["spec"]["networking"].update(
            {
                "cloudNativeGateway": {
                    "image": cng_tag,
                }
            }
        )

    def configure_networking(self):
        # Setup basic tls support
        external_client = self.cluster_spec.external_client
        n2n_encryption = self.test_config.cluster.enable_n2n_encryption
        ssl_enabled_modes = ("data", "auth")
        ssl_enabled = (
            external_client
            or n2n_encryption
            or self.test_config.load_settings.ssl_mode in ssl_enabled_modes
            or self.test_config.access_settings.ssl_mode in ssl_enabled_modes
        )
        if ssl_enabled:
            self.config["spec"]["networking"].update(
                {
                    "tls": {
                        "rootCAs": ["couchbase-server-ca"],
                        "secretSource": {"serverSecretName": "couchbase-server-tls"},
                    }
                }
            )

        if n2n_encryption:
            # Rename to match the CAO spec values [ControlPlaneOnly, All, Strict]
            n2n_encryption = {"control": "ControlPlaneOnly"}.get(
                n2n_encryption, n2n_encryption.capitalize()
            )
            self.config["spec"]["networking"]["tls"]["nodeToNodeEncryption"] = n2n_encryption

        if external_client:
            lb_scheme = self.test_config.load_balancer_settings.lb_scheme
            template = {
                "metadata": {
                    "annotations": {
                        "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "ip",
                        "service.beta.kubernetes.io/aws-load-balancer-scheme": lb_scheme,
                    }
                },
                "spec": {"type": "LoadBalancer"},
            }

            self.config["spec"]["networking"].update(
                {
                    "adminConsoleServiceTemplate": template,
                    "exposedFeatureServiceTemplate": template,
                    "dns": {"domain": f"{self.cluster_name}.cbperfoc.com"},
                }
            )
            exposed_features = self.config["spec"]["networking"]["exposedFeatures"]
            if "external-cluster-connection" not in exposed_features:
                exposed_features.append("external-cluster-connection")

    def configure_migration(self):
        """Configure the cluster to migrate data from an on-premise source cluster."""
        migration_settings = self.test_config.migration_settings
        if not migration_settings.enabled:
            return

        self.config["spec"].update(
            {
                "migration": {
                    "unmanagedClusterHost": migration_settings.source_cluster,
                    "numUnmanagedNodes": migration_settings.num_unmanaged_nodes,
                    "maxConcurrentMigrations": migration_settings.max_concurrent_migrations,
                }
            }
        )


class CAOCouchbaseBucketFile(CAOFiles):
    """Couchbase bucket configuration."""

    def __init__(self, bucket_name: str):
        super().__init__("bucket_template", "")
        self.dest_file = f"cloud/operator/{bucket_name}.yaml"
        self.bucket_name = bucket_name

    def set_bucket_settings(self, bucket_quota: int, bucket_settings: BucketSettings):
        self.config["metadata"]["name"] = self.bucket_name
        self.config["spec"].update(
            {
                "memoryQuota": f"{bucket_quota}Mi",
                "replicas": bucket_settings.replica_number,
                "evictionPolicy": bucket_settings.eviction_policy,
                "compressionMode": bucket_settings.compression_mode or "off",
                "conflictResolution": bucket_settings.conflict_resolution_type or "seqno",
                "storageBackend": bucket_settings.backend_storage,
            }
        )


class CAOCouchbaseBackupFile(CAOFiles):
    """Backup configuration."""

    def __init__(self):
        super().__init__("backup_template", "")

    def set_schedule_time(self, cron_schedule: str):
        self.config["spec"]["full"]["schedule"] = cron_schedule


class CAOHorizontalAutoscalerFile(CAOFiles):
    """Horizontal pod autoscaler configuration."""

    def __init__(self):
        super().__init__("autoscaler_template", "")

    def setup_pod_autoscaler(
        self,
        cluster_name: str,
        server_group: str,
        min_nodes: int,
        max_nodes: int,
        target_metric: str,
        target_type: str,
        target_value: str,
    ):
        self.config["spec"]["scaleTargetRef"]["name"] = f"{server_group}.{cluster_name}"
        self.config["spec"]["minReplicas"] = min_nodes
        self.config["spec"]["maxReplicas"] = max_nodes
        self.config["spec"]["metrics"] = [
            {
                "type": "Pods",
                "pods": {
                    "metric": {"name": target_metric},
                    "target": {"type": target_type, target_type: target_value},
                },
            }
        ]


class CAOWorkerFile(CAOFiles):
    """Worker pods configuration."""

    def __init__(self, cluster_spec: ClusterSpec):
        super().__init__("worker_template", "")
        self.cluster_spec = cluster_spec

    def update_worker_spec(self):
        """Configure replica and resource limits."""
        # Update worker pods replicas
        self.config["spec"]["replicas"] = len(self.cluster_spec.workers)

        # Update worker pods resource limits
        k8s = self.cluster_spec.infrastructure_section("k8s")
        self.config["spec"]["template"]["spec"]["containers"][0].update(
            {
                "resources": {
                    "limits": {
                        "cpu": k8s.get("worker_cpu_limit", 80),
                        "memory": f"{k8s.get('worker_mem_limit', 128)}Gi",
                    }
                }
            }
        )


class CAOSyncgatewayDeploymentFile(CAOFiles):
    """Deployment setup for syncgateway.

    It configures:
    - Syncgateway bootstrap configuration as a k8s secret
    - Syncgateway deployment
    - Syncgateway service
    """

    def __init__(self, syncgateway_image: str, node_count: int, cb_cluster_name: str):
        super().__init__("syncgateway_template", "")
        self.syncgateway_image = syncgateway_image
        self.node_count = node_count
        self.cb_cluster_name = cb_cluster_name

    def configure_sgw(self):
        for config in self.all_configs:
            if config["kind"] == "Deployment":
                config["spec"]["template"]["spec"]["containers"][0][
                    "image"
                ] = self.syncgateway_image
                config["spec"]["replicas"] = self.node_count


class LoadBalancerControllerFile(CAOFiles):
    """Apply deployed cluster name to the LBC controller spec."""

    def __init__(self):
        super().__init__("lbc_template", "")

    def configure_lbc(self, cluster_name: str):
        # Replace the deployment args with the current deployed EKS cluster name
        for config in self.all_configs:
            if config["kind"] == "Deployment":
                args = config["spec"]["template"]["spec"]["containers"][0]["args"]
                args.remove("--cluster-name=your-cluster-name")
                args.append(f"--cluster-name={cluster_name}")


class IngressFile(CAOFiles):
    """Configure ingress with correct scheme and target backend."""

    def __init__(self, cluster_name: str):
        super().__init__("ingress_template", "")
        self.cluster_name = cluster_name
        self.dest_file = f"cloud/operator/{self.cluster_name}-ingress.yaml"

    def configure_ingress(self, is_cng: bool, lb_scheme: str, cert_arn: str):
        self.config["metadata"]["annotations"].update(
            {
                "alb.ingress.kubernetes.io/certificate-arn": cert_arn,
                "alb.ingress.kubernetes.io/scheme": lb_scheme,
            }
        )
        self.config["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"] = (
            f"{self.cluster_name}"
        )

        if is_cng:
            self.config["metadata"]["annotations"].update(
                {
                    "alb.ingress.kubernetes.io/backend-protocol-version": "GRPC",
                }
            )
            self.config["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"].update(
                {
                    "name": f"{self.cluster_name}-cloud-native-gateway-service",
                    "port": {"number": 443},
                }
            )


class TimeTrackingFile(ConfigFile):
    """File for tracking time taken for resource management operations during a test.

    'Resource management operations' include, but aren't limited to:
      - Capella cluster deployment
      - Bucket creation
      - App Services deployment
    """

    def __init__(self):
        super().__init__("timings.json", FileType.JSON)

    def update(self, key: str, value: Union[float, dict], inner_key: Optional[str] = None):
        """Update the timing data with a new key-value pair.

        Args:
            key: Name of the operation being timed
            value: Duration of the operation in seconds
            inner_key: Optional inner key to update within an existing dictionary value
        """
        if inner_key:
            if not isinstance(self.config.get(key), dict):
                # If the key is not a dictionary, create an empty dictionary for it.
                # This will override the existing value but ensures that the key can be updated.
                self.config[key] = {}
            self.config[key].update({inner_key: value})
        else:
            self.config.update({key: value})

    def get(self, key: str) -> Union[float, dict]:
        """Get the timing data for a specific operation.

        Args:
            key: Name of the operation

        Returns:
            Duration of the operation in seconds, or a dictionary containing timing data
        """
        return self.config.get(key, 0)

    def log_content(self):
        """Log the current content of the timing data file."""
        logger.info(f"Current timing data: {pretty_dict(self.config)}")


class ClusterAnsibleInventoryFile(ConfigFile):
    """Generates Ansible INI inventory file from a cluster spec.

    If cluster name is provided, the file is generated in the `clusters` directory.
    Otherwise, it generates the `cloud/infrastructure/cloud.ini` file.

    Sections can be created in two ways:
    - Special sections: `couchbase_servers`, `syncgateways`, `clients`, `kafka_brokers`
    - Service-based sections: any other section such as `kv`, `index`, `n1ql` and more
    """

    def __init__(self, username: str, password: str, cluster_name: str = None):
        ini_file = "cloud/infrastructure/cloud.ini"
        if cluster_name:
            ini_file = f"clusters/{cluster_name}.ini"

        super().__init__(ini_file, FileType.INI)
        self.username = username
        self.password = password

    def load_config(self):
        super().load_config()
        # Set the ansible ssh access variables using cluster credentials
        if "all:vars" not in self.config:
            self.config["all:vars"] = {}
        self.config["all:vars"].update(
            {"ansible_user": self.username, "ansible_ssh_pass": self.password}
        )

    def add_hosts_section(self, section: str, hosts: list[str]):
        """Add hosts to the specific section of the inventory file."""
        if hosts:
            self.config[section] = {host: "" for host in hosts}

    def set_servers(self, servers: list[str]):
        """Update the `couchbase_servers` section of the inventory file."""
        self.add_hosts_section("couchbase_servers", servers)

    def set_syncgateways(self, syncgateways: list[str]):
        """Update the `syncgateways` section of the inventory file."""
        self.add_hosts_section("syncgateways", syncgateways)

    def set_clients(self, clients: dict[str, list[str]]):
        """Update the `clients` section of the inventory file."""
        self.add_hosts_section("clients", clients)

    def set_kafka_brokers(self, kafka_brokers: list[str]):
        """Update the `kafka_brokers` section of the inventory file."""
        self.add_hosts_section("kafka_brokers", kafka_brokers)


class MetadataFile(ConfigFile):
    """JSON file for storing test metadata."""

    DEFAULT_KEY_GROUP = "default"

    def __init__(self, file_path: str, file_type: FileType = FileType.JSON):
        super().__init__(file_path, file_type)

    def get_metadata(self, key_group: str, key: str) -> str:
        """Get metadata with a specific key from a key group.

        If the key_group doesn't exist, fall back to the default key group.

        Then if the key doesn't exist, search for the key in the default key group.
        """
        if key_group not in self.config:
            logger.warning(
                f"Key group '{key_group}' not found in metadata file. Using default group."
            )

        # Combine the default group with the target group so that any keys not in the target group
        # are taken from the default group.
        combined_group = self.config.get(self.DEFAULT_KEY_GROUP, {}) | self.config.get(
            key_group, {}
        )
        return combined_group.get(key, "")


class SecretsFile(MetadataFile):
    """JSON file for storing test secrets."""

    def __init__(self):
        super().__init__(os.getenv("PERFRUNNER_SECRETS_JSON", ".secrets.json"))


class ClusterMetadataFile(MetadataFile):
    """YAML file containing cluster metadata."""

    def __init__(self, csp: Optional[str] = None):
        # Separate cluster metadata files for on-prem and cloud clusters. This is because for
        # on-prem we have a limited number of clusters whose parameters are shared for cluster specs
        # derived from the same machines. For cloud clusters, parameters are derived from instance
        # types used in the cluster spec.
        cluster_metadata_file = "clusters/cluster_metadata.yaml"
        if csp:
            cluster_metadata_file = f"cloud/infrastructure/{csp}_cluster_metadata.yaml"

        super().__init__(cluster_metadata_file, FileType.YAML)

    def get_parameters(self, name: str, overrides: dict = {}) -> dict:
        """Get cluster parameters for the given name."""
        cluster_params = self.config.get(self.DEFAULT_KEY_GROUP, {}).get(
            "parameters", {}
        ) | self.config.get(name, {}).get("parameters", {})

        # We want to treat OS and CPU keys as all uppercase,
        # and everything else as first letter uppercase
        for key, value in overrides.items():
            capkey = key.capitalize()
            if capkey in ("Os", "Cpu", "Gpu"):
                capkey = key.upper()
            cluster_params[capkey] = value
        return cluster_params

    def write(self):
        # Ensures we dont write back to the metadata file
        pass

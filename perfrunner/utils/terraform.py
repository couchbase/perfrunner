#!/usr/bin/env python
import json
import os
from argparse import ArgumentParser, Namespace
from collections import Counter
from ipaddress import IPv4Network
from time import sleep, time
from typing import Optional
from uuid import uuid4

import requests
from capella.columnar.CapellaAPI import CapellaAPI as CapellaAPIColumnar
from capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIDedicated
from fabric.api import local
from requests.exceptions import HTTPError

from logger import logger
from perfrunner.helpers.config_files import (
    ClusterAnsibleInventoryFile,
    ConfigFile,
    TimeTrackingFile,
    record_time,
)
from perfrunner.helpers.misc import (
    maybe_atoi,
    my_public_ip,
    pretty_dict,
    run_local_shell_command,
)
from perfrunner.settings import ClusterSpec, TestConfig

CAPELLA_CREDS_FILE = '.capella_creds'
DATADOG_LINK_TEMPLATE = (
    'https://app.datadoghq.com/logs?'
    'query=status%3Aerror%20%40{}&'
    'cols=host%2Cservice&index=%2A&messageDisplay=inline&stream_sort=time%2Cdesc&viz=stream&'
    'from_ts={}&'
    'to_ts={}&'
    'live=true'
)
SERVICES_CAPELLA_TO_PERFRUNNER = {
    'Data': 'kv',
    'Analytics': 'cbas',
    'Query': 'n1ql',
    'Index': 'index',
    'Search': 'fts',
    'Eventing': 'eventing'
}

SERVICES_PERFRUNNER_TO_CAPELLA = {
    'kv': 'data',
    'cbas': 'analytics',
    'n1ql': 'query',
    'index': 'index',
    'fts': 'search',
    'eventing': 'eventing'
}

AWS_DEFAULT_REGION = "us-east-1"
AWS_REGIONS = [
    "us-east-1",
    "us-east-2",
    "us-west-2",
    "ca-central-1",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-southeast-1",
    "ap-south-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-central-1",
    "sa-east-1",
]

GCP_DEFAULT_ZONE = "us-west1-b"
GCP_ZONES = [
    "us-west1-b",
    "us-west1-a",
    "us-west1-c",
    "us-central1-a",
    "us-central1-b",
    "us-central1-c",
    "us-central1-f",
]

AZURE_DEFAULT_REGION = "eastus"
AZURE_REGIONS = ["eastus", "eastus2", "westus2", "westeurope", "uksouth"]


def raise_for_status(resp: requests.Response):
    try:
        resp.raise_for_status()
    except HTTPError as e:
        logger.error('HTTP Error {}: response content: {}'.format(resp.status_code, resp.content))
        raise e


def format_datadog_link(cluster_id: str = None, dataplane_id: str = None,
                        database_id: str = None) -> str:
    filters = [
        'clusterId%3A{}'.format(cluster_id) if cluster_id else None,
        'dataplaneId%3A{}'.format(dataplane_id) if dataplane_id else None,
        'databaseId%3A{}'.format(database_id) if database_id else None
    ]

    filters = [f for f in filters if f]
    filter_string = '%20%40'.join(filters) if filters else ''

    now = time()
    one_hour_ago = now - (60 * 60)

    return DATADOG_LINK_TEMPLATE.format(filter_string, int(one_hour_ago * 1000), int(now * 1000))


class CloudVMDeployer:
    IMAGE_MAP = {
        "aws": {
            "clusters": {
                "x86_64": "perf-server-x86-ubuntu20-2023-07",
                "arm": "perf-server-arm-us-east",
                "al2": "perf-server-al_x86-2022-03-us-east",
            },
            "clients": "perf-client-x86-ubuntu20-2023-06-v3",
            "utilities": "perf-broker-us-east",
            "syncgateways": "perf-server-x86-ubuntu20-2023-07",
            "kafka_brokers": "perf-client-x86-ubuntu20-2023-06-v3",
        },
        "gcp": {
            "clusters": {
                "x86_64": "perftest-server-x86-ubuntu20-2023-07",
                "arm": "perftest-server-arm64-ubuntu20-2024-10-v2",
            },
            "clients": "perftest-client-x86-ubuntu20-2024-10",
            "utilities": "perftest-broker-disk-image",
            "syncgateways": "perftest-server-x86-ubuntu20-2023-07",
        },
        "azure": {
            "clusters": {
                "x86_64": "perf-server-x86-ubuntu20-image-def",
            },
            "clients": "perf-client-x86-ubuntu20-image-def",
            "utilities": "perf-broker-image-def",
            "syncgateways": "perf-server-x86-ubuntu20-image-def",
        },
    }

    UTILITY_NODE_TYPES = {
        "aws": {"default": "t3a.large"},
        "gcp": {"default": "e2-standard-2"},
        "azure": {"default": "Standard_B2as_v2"},
    }

    def __init__(self, infra_spec: ClusterSpec, options: Namespace):
        self.infra_spec = infra_spec
        self.options = options
        self.os_arch = self.infra_spec.infrastructure_settings.get("os_arch", "x86_64")

        self.csp = (
            self.infra_spec.capella_backend.lower()
            if (csp := self.infra_spec.cloud_provider.lower()) == "capella"
            else csp
        )

        self.uuid = self.infra_spec.infrastructure_settings.get("uuid", uuid4().hex[:6])
        self.infra_spec.config.set("infrastructure", "uuid", self.uuid)

        self.node_list = {
            "clusters": [
                n
                for nodes in self.infra_spec.infrastructure_clusters.values()
                for n in nodes.strip().split()
            ],
            "clients": self.infra_spec.clients,
            "syncgateways": self.infra_spec.sgw_servers,
            "kafka_brokers": self.infra_spec.kafka_servers,
        }

        self.cloud_storage = bool(
            int(self.infra_spec.infrastructure_settings.get('cloud_storage', 0))
        )

        zone = self.options.zone or GCP_DEFAULT_ZONE
        if self.csp == "gcp":
            if zone not in GCP_ZONES:
                logger.interrupt(f"Invalid GCP zone. Please choose from: {pretty_dict(GCP_ZONES)}")

            self.zone, self.region = zone, zone.rsplit("-", 1)[0]
        elif self.csp == "aws":
            if (region := zone[:-1]) in AWS_REGIONS:
                logger.info("Valid AWS zone provided. Overriding region with zone.")
                self.zone, self.region = zone, region
            elif (region := self.options.region or AWS_DEFAULT_REGION) in AWS_REGIONS:
                logger.info("No valid AWS zone provided. Ignoring zone.")
                self.zone, self.region = None, region
            else:
                logger.interrupt(
                    "No valid AWS region or zone provided. "
                    f"Valid regions: {pretty_dict(AWS_REGIONS)}"
                )
        elif self.csp == "azure":
            if (region := self.options.region or AZURE_DEFAULT_REGION) not in AZURE_REGIONS:
                logger.interrupt(
                    f"Invalid Azure region. Please choose from: {pretty_dict(AZURE_REGIONS)}"
                )
            self.zone, self.region = None, region
        else:
            logger.interrupt(f"Unrecognised cloud service provider: {self.csp}")

        logger.info(f"Deployer region, zone: {self.region}, {self.zone}")
        self.infra_spec.config.set("infrastructure", "region", self.region)
        self.infra_spec.update_spec_file()

    def deploy(self) -> dict:
        # Configure terraform
        if not self.populate_tfvars():
            return {}
        self.terraform_init(self.csp)

        # Deploy resources
        self.terraform_apply(self.csp)

        # Get info about deployed resources and update cluster spec file
        output = self.terraform_output(self.csp)
        self.update_spec(output)
        return output

    def destroy(self):
        self.terraform_destroy(self.csp)

    def create_tfvar_nodes(self) -> dict[str, dict]:
        tfvar_nodes = {
            "utilities": {}
            if not self.infra_spec.infrastructure_utilities
            else {
                "1": {
                    "image": self.options.utility_image or self.IMAGE_MAP[self.csp]["utilities"],
                    "instance_type": self.UTILITY_NODE_TYPES[self.csp].get(
                        self.infra_spec.utility_profile
                    ),
                }
            },
            "clusters": {},
            "clients": {},
            "syncgateways": {},
            "kafka_brokers": {},
        }

        for role, nodes in self.node_list.items():
            # If this is a capella test, skip cluster nodes
            if self.infra_spec.capella_infrastructure and (role in ["clusters", "syncgateways"]):
                continue

            i = 0
            for node in nodes:
                node_cluster, node_group = node.split(':')[0].split('.', 2)[1:]
                parameters = self.infra_spec.infrastructure_config()[node_group.split('.')[0]]

                parameters['node_group'] = node_group

                # Try getting image name from cli option
                image = getattr(
                    self.options,
                    "{}_image".format(
                        {
                            "clusters": "cluster",
                            "clients": "client",
                            "syncgateways": "sgw",
                            "kafka_brokers": "kafka",
                        }[role]
                    ),
                )

                # If image name isn't provided as cli param, use hardcoded defaults
                if image is None:
                    image = self.IMAGE_MAP[self.csp][role]
                    if role == "clusters":
                        image = image.get(self.os_arch, image['x86_64'])

                parameters['image'] = image

                # Set disk size and type
                parameters['volume_size'] = int(parameters.get('volume_size', 0))

                storage_class = parameters.get('storage_class', parameters.get('volume_type', None))
                if not storage_class:
                    node_cluster_config = self.infra_spec.infrastructure_section(node_cluster)
                    storage_class = node_cluster_config.get('storage_class')
                parameters['storage_class'] = storage_class

                # Set CSP-specific options
                if self.csp == "azure":
                    parameters["disk_tier"] = parameters.get("disk_tier", "")
                else:
                    # AWS and GCP
                    parameters["iops"] = int(parameters.get("iops", 0))
                    parameters["volume_throughput"] = int(parameters.get("volume_throughput", 0))
                    if self.csp == "gcp":
                        parameters["local_nvmes"] = int(parameters.get("local_nvmes", 0))

                parameters.pop("instance_capacity", None)

                tfvar_nodes[role][str(i := i+1)] = parameters

        return tfvar_nodes

    def _get_managed_id(self) -> str:
        """Return Azure managed identity id based on the active subscription."""
        if self.csp != "azure":
            return ""

        stdout, _, returncode = run_local_shell_command(
            "az identity show --name perfrunner-mi --resource-group perf-resources-eastus",
            err_msg="Failed to get managed identity 'perfrunner-mi' in the current subscription",
        )
        if returncode != 0:
            return ""
        return json.loads(stdout).get("id", "").replace("/resourcegroups/", "/resourceGroups/")

    def _can_terraform(self) -> tuple[dict, bool]:
        """Return the tfvar nodes and a boolean indicating if terraform can be run."""
        tfvar_nodes = self.create_tfvar_nodes()

        if not any(tfvar_nodes.values()) and not self.cloud_storage:
            logger.warn("Nothing to deploy with Terraform.")
            return {}, False

        return tfvar_nodes, True

    def _get_allowed_ips(self) -> list[str]:
        """Return a list of IPs that are allowed to access the deployed resources."""
        allowed_ips = [
            f"{cidr}/32" if "/" not in cidr else cidr
            for cidr in os.environ.get("PERF_ALLOWED_IPS", "").split(",")
            if cidr
        ]
        myip = my_public_ip()
        if not any(IPv4Network(cidr).overlaps(IPv4Network(myip)) for cidr in allowed_ips):
            # Azure NSGs require non-overlapping CIDRs/addresses in security rules, so at minimum
            # we will check if our public ip is already covered in allowed_ips
            allowed_ips.append(f"{myip}/32")
        return allowed_ips

    def populate_tfvars(self) -> bool:
        logger.info("Setting tfvars")
        global_tag = self.options.tag if self.options.tag else ''
        if self.csp == "gcp":
            # GCP doesn't allow uppercase letters in tags
            global_tag = global_tag.lower()

        tfvar_nodes, can_terraform = self._can_terraform()
        if not can_terraform:
            return False

        tfvars = {
            "cloud_region": self.region,
            "cloud_zone": self.zone or "",
            "cluster_nodes": tfvar_nodes["clusters"],
            "client_nodes": tfvar_nodes["clients"],
            "utility_nodes": tfvar_nodes["utilities"],
            "syncgateway_nodes": tfvar_nodes["syncgateways"],
            "kafka_nodes": tfvar_nodes["kafka_brokers"],
            "cloud_storage": self.cloud_storage,
            "global_tag": global_tag,
            "uuid": self.uuid,
            "managed_id": self._get_managed_id(),
            "allowed_ips": self._get_allowed_ips(),
        }

        with ConfigFile(f"terraform/{self.csp}/terraform.tfvars.json") as tfvars_file:
            tfvars_file.config = tfvars

        return True

    # Initializes terraform environment.
    def terraform_init(self, csp: str):
        logger.info('Initializating Terraform (terraform init)')
        local(f"cd terraform/{csp} && terraform init >> terraform.log")

    # Apply and output terraform deployment.
    def terraform_apply(self, csp: str):
        logger.info('Building and executing Terraform deployment plan '
                    '(terraform plan, terraform apply)')
        local(
            f"cd terraform/{csp} && "
            "terraform plan -out tfplan.out >> terraform.log && "
            "terraform apply -auto-approve tfplan.out"
        )

    def terraform_output(self, csp: str):
        output = json.loads(local(f"cd terraform/{csp} && terraform output -json", capture=True))
        return output

    def terraform_destroy(self, csp: str):
        logger.info('Building and executing Terraform destruction plan '
                    '(terraform plan -destroy, terraform apply)')
        _, _, returncode = run_local_shell_command(
            f"cd terraform/{csp} && terraform validate", quiet=True
        )
        if returncode != 0:
            logger.info('No resources to destroy with Terraform.')
            return

        local(
            f"cd terraform/{csp} && "
            "terraform plan -destroy -out tfplan_destroy.out >> terraform.log && "
            "terraform apply -auto-approve tfplan_destroy.out"
        )

    # Update spec file with deployed infrastructure.
    def update_spec(self, output):
        # Handle utility nodes first as these don't have customizable node groups
        if self.infra_spec.config.has_section("utilities") and (
            ips := list(output["utility_instance_ips"]["value"].values())
        ):
            public_ips = [ip["public_ip"] for ip in ips]
            self.infra_spec.config.set("utilities", "hosts", "\n".join(public_ips))
            if private_ips := [ip["private_ip"] for ip in ips if "private_ip" in ip]:
                if not self.infra_spec.config.has_section((section := "utility_private_ips")):
                    self.infra_spec.config.add_section(section)
                self.infra_spec.config.set(section, "hosts", "\n".join(private_ips))

        sections = ["clusters", "clients", "syncgateways", "kafka_clusters"]
        cluster_dicts = [
            self.infra_spec.infrastructure_clusters,
            self.infra_spec.infrastructure_clients,
            self.infra_spec.infrastructure_syncgateways,
            self.infra_spec.infrastructure_kafka_clusters,
        ]
        output_keys = [
            "cluster_instance_ips",
            "client_instance_ips",
            "syncgateway_instance_ips",
            "kafka_instance_ips",
        ]
        private_sections = [
            "cluster_private_ips",
            "client_private_ips",
            "syncgateway_private_ips",
            "kafka_private_ips",
        ]

        for section, cluster_dict, output_key, private_section in zip(
            sections, cluster_dicts, output_keys, private_sections
        ):
            if (not self.infra_spec.config.has_section(section)) or (
                self.infra_spec.capella_infrastructure and (section in ["clusters", "syncgateways"])
            ):
                continue

            for cluster, nodes in cluster_dict.items():
                node_list = nodes.strip().split()
                node_info = {
                    "public_ips": [None for _ in node_list],
                    "private_ips": [None for _ in node_list],
                    "subnet_ids": [None for _ in node_list],
                }

                for _, info in output[output_key]["value"].items():
                    node_group = info["node_group"]
                    for i, node in enumerate(node_list):
                        hostname, *extras = node.split(":", maxsplit=1)
                        if hostname.split(".", 2)[-1] == node_group:
                            public_ip = info["public_ip"]
                            if extras:
                                public_ip += ":{}".format(*extras)
                            node_info["public_ips"][i] = public_ip

                            if "private_ip" in info:
                                node_info["private_ips"][i] = info["private_ip"]

                            if "subnet_id" in info:
                                node_info["subnet_ids"][i] = info["subnet_id"]
                            break

                self.infra_spec.config.set(
                    section, cluster, "\n" + "\n".join(node_info["public_ips"])
                )

                if any((private_ips := node_info["private_ips"])):
                    if not self.infra_spec.config.has_section(private_section):
                        self.infra_spec.config.add_section(private_section)
                    self.infra_spec.config.set(
                        private_section, cluster, "\n" + "\n".join(private_ips)
                    )

                if section == "kafka_clusters" and any((subnet_ids := node_info["subnet_ids"])):
                    if not self.infra_spec.config.has_section((section := "kafka_subnet_ids")):
                        self.infra_spec.config.add_section(section)
                    self.infra_spec.config.set(section, cluster, "\n" + "\n".join(subnet_ids))

        if self.cloud_storage:
            cloud_storage_info = output['cloud_storage']['value']
            bucket_url = cloud_storage_info['storage_bucket']
            if (section := "storage") not in self.infra_spec.config.sections():
                self.infra_spec.config.add_section(section)
            self.infra_spec.config.set(section, "backup", bucket_url)
            if self.csp == "azure":
                storage_acc = cloud_storage_info['storage_account']
                self.infra_spec.config.set(section, "storage_acc", storage_acc)

        self.infra_spec.update_spec_file()

        with ClusterAnsibleInventoryFile(*self.infra_spec.ssh_credentials) as cloud_ini:
            cloud_ini.set_servers(self.infra_spec.servers)
            cloud_ini.set_syncgateways(self.infra_spec.sgw_servers)
            cloud_ini.set_clients(self.infra_spec.clients)
            cloud_ini.set_kafka_brokers(self.infra_spec.kafka_brokers)


class ControlPlaneManager:
    """Prepares the control plane and updates cluster spec file for Capella perf tests."""

    PROJECT_DELETE_TIMEOUT_MINS = 10

    def __init__(
        self,
        infra_spec: ClusterSpec,
        public_api_url: Optional[str] = None,
        org_id: Optional[str] = None,
        project_id: Optional[str] = None,
        deployment_tag: Optional[str] = None,
    ):
        logger.debug("Control plane manager initialising...")
        self.infra_spec = infra_spec
        self.org_id = org_id
        self.project_id = project_id
        self.tag = deployment_tag

        self.uuid = self.infra_spec.infrastructure_settings.get("uuid", uuid4().hex[:6])
        self.infra_spec.config.set("infrastructure", "uuid", self.uuid)

        if "controlplane" not in self.infra_spec.config:
            self.infra_spec.config.add_section("controlplane")

        if not public_api_url:
            logger.info(
                "No Capella public API URL provided. "
                "Looking for Capella environment info in cluster spec."
            )
            if not (public_api_url := self.infra_spec.controlplane_settings.get("public_api_url")):
                logger.interrupt(
                    "No Capella environment info specified in cluster spec. "
                    "Cannot determine Capella public API URL to proceed."
                )
        else:
            logger.info(
                "Capella public API URL provided for initialization. "
                "Saving Capella environment info to cluster spec."
            )
            env = (
                public_api_url.removeprefix("https://")
                .removesuffix(".nonprod-project-avengers.com")
                .split(".", 1)[1]
            )
            self.infra_spec.config.set("controlplane", "env", env)

        self.infra_spec.update_spec_file()

        logger.info(f"Using Capella public API URL: {public_api_url}")
        self.api_client = CapellaAPIDedicated(
            public_api_url,
            None,
            None,
            os.getenv("CBC_USER"),
            os.getenv("CBC_PWD"),
            os.getenv("CBC_TOKEN_FOR_INTERNAL_SUPPORT"),
        )
        logger.debug("Control plane manager initialised.")

    def save_project_id(self, create_project_if_none: bool = True):
        """After this method is called, a project ID will be saved in the cluster spec file.

        A project ID will be searched for in the following places:
         1. Args provided when creating this class instance
         2. In the cluster spec file

        If no project ID is found, a new project will be created if `create_project_if_none` is
        True.
        """
        if self.project_id is None:
            logger.info(
                "No project ID set during initialization. Looking for project ID in cluster spec."
            )
            if (project_id := self.infra_spec.controlplane_settings.get("project")) is None:
                logger.info("No project ID found in cluster spec.")
                if create_project_if_none:
                    project_name = self.tag or f"perf-{self.uuid}"
                    project_id = self._create_project(self.org_id, project_name)
                    logger.info(f"Created project with ID: {project_id}")
                    self.infra_spec.config.set("controlplane", "project_created", "1")
            else:
                logger.info(f"Found project ID in cluster spec: {project_id}")
            self.project_id = project_id
        else:
            logger.info(f"Project ID already set during initialization: {self.project_id}")

        logger.info("Saving project ID in cluster spec file.")
        self.infra_spec.config.set("controlplane", "project", self.project_id)
        self.infra_spec.update_spec_file()

    def save_org_id(self):
        """After this method is called, an org ID will be saved in the cluster spec file.

        An org ID will be searched for in the following places:
         1. Args provided when creating this class instance
         2. In the cluster spec file

         If this doesn't yield an org ID, then the control plane will be queried to find an org
         that is accessible to the user.
        """
        if self.org_id is None:
            logger.info(
                "No organization ID set during initialization. "
                "Looking for organization ID in cluster spec."
            )
            if (org := self.infra_spec.controlplane_settings.get("org")) is None:
                logger.info("No organization ID found in cluster spec.")
                org = self._find_org()
                logger.info(f"Found organization ID: {org}")
            else:
                logger.info(f"Found organization ID in cluster spec: {org}")
            self.org_id = org
        else:
            logger.info(f"Organization ID already set during initialization: {self.org_id}.")

        logger.info("Saving organization ID in cluster spec file.")
        self.infra_spec.config.set("controlplane", "org", self.org_id)
        self.infra_spec.update_spec_file()

    @staticmethod
    def get_api_keys() -> Optional[tuple[str, str]]:
        """Get API keys from env vars or the creds file."""
        access_key = os.getenv("CBC_ACCESS_KEY")
        secret_key = os.getenv("CBC_SECRET_KEY")

        if bool(access_key and secret_key):
            logger.info("Found API keys in env vars.")
            return access_key, secret_key

        if not os.path.isfile(CAPELLA_CREDS_FILE):
            logger.info("No creds file found and no API keys set as env vars.")
            return None

        with open(CAPELLA_CREDS_FILE, "r") as f:
            creds = json.load(f)
            access_key = creds.get("access")
            secret_key = creds.get("secret")
            if access_key and secret_key:
                logger.info("Found API keys in creds file.")
                return access_key, secret_key
            logger.info("No API keys found in creds file.")

        return None

    def save_api_keys(self):
        """After this method is called, a set of control plane API keys will be available.

        API keys will be searched for in the following places:
         1. Environment variables
         2. In the creds file

        If no API keys are found, new API keys will be generated and saved in the creds file.
        """
        logger.info("Getting control plane API keys...")
        if self.get_api_keys() is None:
            key_name = self.tag or f"perf-{self.uuid}"
            key_id, access_key, secret_key = self._generate_api_key(self.org_id, key_name)
            creds = {"id": key_id, "access": access_key, "secret": secret_key}
            with open(CAPELLA_CREDS_FILE, "w") as f:
                json.dump(creds, f)
            logger.info("Saved API keys in creds file.")

    def _find_org(self) -> str:
        logger.info("Finding an accessible organization...")
        resp = self.api_client.list_accessible_tenants()
        raise_for_status(resp)
        org = resp.json()[0]["id"]
        return org

    def _generate_api_key(self, org_id: str, name: str) -> tuple[str, str, str]:
        logger.info(f"Generating control plane API keys in organization {org_id}...")
        resp = self.api_client.create_access_secret_key(name, org_id)
        raise_for_status(resp)
        payload = resp.json()
        return payload["id"], payload["access"], payload["secret"]

    def _create_project(self, org_id: str, name: str) -> str:
        logger.info(f"Creating project in organization {org_id}...")
        resp = self.api_client.create_project(org_id, name)
        raise_for_status(resp)
        project = resp.json()["id"]
        return project

    def clean_up_project(self):
        """Destroy project if one was created."""
        if not int(self.infra_spec.controlplane_settings.get("project_created", 0)):
            logger.info("No project was created during this run. Skipping project deletion.")
            return

        interval_secs = 30
        t0 = time()
        while time() < t0 + self.PROJECT_DELETE_TIMEOUT_MINS * 60:
            logger.info(
                f"Trying to deleting project {self.project_id} from organization {self.org_id}..."
            )
            resp = self.api_client.delete_project(self.org_id, self.project_id)
            if (
                resp.status_code == 400
                and (rjson := resp.json())["errorType"] == "DeleteProjectsWithDatabases"
            ):
                logger.warning(rjson["message"])
                sleep(interval_secs)
                continue

            raise_for_status(resp)
            logger.info("Project successfully queued for deletion.")
            return

        logger.error(
            f"Timed out after {self.PROJECT_DELETE_TIMEOUT_MINS} mins "
            "waiting for project to delete."
        )

    def clean_up_api_keys(self):
        """Revoke API keys stored in creds file."""
        if os.path.isfile(CAPELLA_CREDS_FILE):
            logger.info("Revoking API key...")
            with open(CAPELLA_CREDS_FILE, "r") as f:
                creds = json.load(f)
                self.api_client.revoke_access_secret_key(self.org_id, creds["id"])
            os.remove(CAPELLA_CREDS_FILE)

    @staticmethod
    def save_model_api_key(model_id: str, model_api_key_id: str, model_api_key: str):
        """Save the model API key in the temporary credentials file."""
        existing_creds = {}
        try:
            with open(CAPELLA_CREDS_FILE, "r") as f:
                existing_creds = json.load(f)
        except Exception:
            pass

        model_keys = existing_creds.get("model_api_keys", {})
        model_keys.update(
            {model_id: {"model_api_key_id": model_api_key_id, "model_api_key": model_api_key}}
        )
        existing_creds.update({"model_api_keys": model_keys})
        with open(CAPELLA_CREDS_FILE, "w") as f:
            json.dump(existing_creds, f)

    @staticmethod
    def get_model_api_key(model_id: str) -> str:
        """Get the model API key from the temporary credentials file."""
        if not os.path.isfile(CAPELLA_CREDS_FILE):
            return ""

        with open(CAPELLA_CREDS_FILE, "r") as f:
            existing_creds = json.load(f)
        return existing_creds.get("model_api_keys", {}).get(model_id, {}).get("model_api_key", "")

    @staticmethod
    def get_model_api_key_id(model_id: str) -> str:
        """Get the model API key ID from the temporary credentials file."""
        if not os.path.isfile(CAPELLA_CREDS_FILE):
            return ""

        with open(CAPELLA_CREDS_FILE, "r") as f:
            existing_creds = json.load(f)
        return (
            existing_creds.get("model_api_keys", {}).get(model_id, {}).get("model_api_key_id", "")
        )


class CapellaProvisionedDeployer(CloudVMDeployer):
    def __init__(self, infra_spec: ClusterSpec, options: Namespace):
        super().__init__(infra_spec, options)

        self.test_config = None
        if options.test_config:
            test_config = TestConfig()
            test_config.parse(options.test_config, override=options.override)
            self.test_config = test_config

        if self.test_config and self.test_config.rebalance_settings.nodes_after:
            self.node_list['clusters'] = [
                node
                for nodes, initial_num in zip(self.infra_spec.infrastructure_clusters.values(),
                                              self.test_config.cluster.initial_nodes)
                for node in nodes.strip().split()[:initial_num]
            ]

        self.provisioned_api = CapellaAPIDedicated(
            self.infra_spec.controlplane_settings["public_api_url"],
            None,
            None,
            os.getenv("CBC_USER"),
            os.getenv("CBC_PWD"),
            os.getenv("CBC_TOKEN_FOR_INTERNAL_SUPPORT"),
        )

        if api_keys := ControlPlaneManager.get_api_keys():
            self.provisioned_api.ACCESS, self.provisioned_api.SECRET = api_keys

        self.tenant_id = self.infra_spec.controlplane_settings["org"]
        self.project_id = self.infra_spec.controlplane_settings["project"]

        # right now this depends on capella columnar clusters being "inactive" in the infra spec
        self.cluster_ids = self.infra_spec.capella_cluster_ids

        self.capella_timeout = max(0, self.options.capella_timeout)

    def deploy(self):
        non_capella_output = None
        if not self.options.capella_only:
            # Deploy non-capella resource
            non_capella_output = super().deploy()

        # Deploy capella cluster
        with record_time("capella_provisioned_cluster"):
            self.deploy_cluster()

        if self.options.disable_autoscaling:
            self.disable_autoscaling()

        # Update cluster spec file
        self.update_capella_spec()

        if not non_capella_output and self.options.vpc_peering:
            # Do VPC peering
            network_info = non_capella_output['network']['value']
            self.peer_vpc(network_info, self.cluster_ids[0])

    def destroy(self):
        # Tear down VPC peering connection
        self.destroy_peering_connection()

        # Destroy non-capella resources
        self.terraform_destroy(self.csp)

        # Destroy capella cluster
        if not self.options.keep_cluster:
            # A provisional cluster can have a workflow attached that needs to be destroyed first
            self.destroy_workflow()
            self.destroy_cluster()
            self.wait_for_cluster_destroy()

    def wait_for_cluster_destroy(self):
        logger.info('Waiting for clusters to be destroyed...')

        pending_clusters = [cluster_id for cluster_id in self.cluster_ids]
        timeout_mins = self.capella_timeout
        interval_secs = 30
        t0 = time()
        while pending_clusters and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            resp = self.provisioned_api.get_project_clusters(self.tenant_id, self.project_id)
            raise_for_status(resp)

            pending_clusters = []
            for cluster in resp.json().get("data", []):
                if (cluster_id := cluster.get("data", {}).get("id")) in self.cluster_ids:
                    pending_clusters.append(cluster_id)
                    logger.info(f"Cluster {cluster_id} not destroyed yet")

        if pending_clusters:
            logger.error(f"Timed out after {timeout_mins} mins waiting for clusters to delete.")
            for cluster_id in pending_clusters:
                logger.error(
                    f"DataDog link for debugging (cluster {cluster_id}): "
                    f"{format_datadog_link(cluster_id=cluster_id)}"
                )
        else:
            logger.info("All clusters destroyed.")

    @staticmethod
    def capella_server_group_sizes(node_schemas):
        server_groups = {}
        for node in node_schemas:
            name, services = node.split(':')[:2]

            _, cluster, node_group, _ = name.split('.')
            services_set = tuple(set(services.split(',')))

            node_tuple = (node_group, services_set)

            if cluster not in server_groups:
                server_groups[cluster] = [node_tuple]
            else:
                server_groups[cluster].append(node_tuple)

        server_group_sizes = {
            cluster: Counter(node_tuples) for cluster, node_tuples in server_groups.items()
        }

        return server_group_sizes

    @staticmethod
    def construct_capella_server_groups(infra_spec,
                                        node_schemas,
                                        enable_disk_autoscaling: bool = True):
        """Create correct server group objects for deploying clusters using internal API.

        Sample server group template:
        ```
        {
            "count": 3,
            "services": [
                {"type": "kv"},
                {"type": "index"}
            ],
            "compute": {
                "type": "r5.2xlarge",
                "cpu": 0,
                "memoryInGb": 0
            },
            "disk": {
                "type": "io2",
                "sizeInGb": 50,
                "iops": 3000
            }
        }
        ```
        """
        server_groups = CapellaProvisionedDeployer.capella_server_group_sizes(node_schemas)

        cluster_list = []
        for cluster, server_groups in server_groups.items():
            server_list = []
            cluster_params = infra_spec.infrastructure_section(cluster)

            for (node_group, services), size in server_groups.items():
                node_group_config = infra_spec.infrastructure_section(node_group)

                storage_class = node_group_config.get(
                    'volume_type', node_group_config.get(
                        'storage_class', cluster_params.get('storage_class')
                    )
                ).lower()

                if infra_spec.capella_backend == 'azure' and 'disk_tier' in node_group_config:
                    disk_tier = node_group_config['disk_tier']
                else:
                    disk_tier = ""

                server_group = {
                    'count': size,
                    'services': [{'type': svc} for svc in services],
                    'compute': {
                        'type': node_group_config['instance_type'],
                        'cpu': 0,
                        'memoryInGb': 0
                    },
                    'disk': {
                        'type': storage_class
                        if infra_spec.capella_backend != 'azure'
                        else disk_tier,
                        'sizeInGb': int(node_group_config['volume_size']),
                    },
                    'diskAutoScaling': {
                        "enabled": enable_disk_autoscaling
                    }
                }

                if infra_spec.capella_backend == 'aws':
                    server_group['disk']['iops'] = int(node_group_config.get('iops', 3000))

                server_list.append(server_group)

            cluster_list.append(server_list)

        return cluster_list

    def deploy_cluster(self):
        specs = self.construct_capella_server_groups(
            self.infra_spec,
            self.node_list['clusters'],
            self.options.enable_disk_autoscaling)
        names = ['perf-cluster-{}'.format(self.uuid) for _ in specs]
        if len(names) > 1:
            names = ['{}-{}'.format(name, i) for i, name in enumerate(names)]

        for name, spec in zip(names, specs):
            config = {
                "cidr": self.get_available_cidr(),
                "name": name,
                "description": "",
                "projectId": self.project_id,
                "provider": {"aws": "hostedAWS", "gcp": "hostedGCP", "azure": "hostedAzure"}[
                    self.csp
                ],
                "region": self.region,
                "singleAZ": not self.options.multi_az,
                "server": self.options.capella_cb_version,
                "configurationType": "multiNode" if int(spec[0]["count"]) != 1 else "singleNode",
                "specs": spec,
                "package": "enterprise",
            }

            logger.info(pretty_dict(config))

            if (server := self.options.capella_cb_version) and (ami := self.options.capella_ami):
                config['overRide'] = {
                    'token': os.getenv('CBC_OVERRIDE_TOKEN'),
                    'server': server,
                    'image': ami
                }
                config.pop('server')
                logger.info('Deploying cluster with custom AMI: {}'
                            .format(ami))
            if release_id := self.options.release_id:
                config['overRide'].update({'releaseId': release_id})

            resp = self.provisioned_api.create_cluster_customAMI(self.tenant_id, config)
            raise_for_status(resp)
            cluster_id = resp.json().get('id')
            self.cluster_ids.append(cluster_id)

            logger.info('Initialised cluster deployment for cluster {} \n  config: {}'.
                        format(cluster_id, config))
            logger.info('Saving cluster ID to spec file.')
            self.infra_spec.config.set("controlplane", "cluster_ids", "\n".join(self.cluster_ids))
            self.infra_spec.update_spec_file()

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_clusters = [cluster_id for cluster_id in self.cluster_ids]
        t0 = time()
        while pending_clusters and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            statuses = []
            for cluster_id in pending_clusters:
                status = (
                    self.provisioned_api.get_cluster_internal(
                        self.tenant_id, self.project_id, cluster_id
                    )
                    .json()
                    .get("data", {})
                    .get("status", {})
                    .get("state")
                )
                logger.info('Cluster state for {}: {}'.format(cluster_id, status))
                if status == "deployment_failed":
                    logger.error('Deployment failed for cluster {}. DataDog link for debugging: {}'
                                 .format(cluster_id, format_datadog_link(cluster_id=cluster_id)))
                    exit(1)
                statuses.append(status)

            pending_clusters = [
                pending_clusters[i] for i, status in enumerate(statuses) if status != 'healthy'
            ]

        if pending_clusters:
            logger.error('Deployment timed out after {} mins'.format(timeout_mins))
            for cluster_id in pending_clusters:
                logger.error('DataDog link for debugging (cluster {}): {}'
                             .format(cluster_id, format_datadog_link(cluster_id=cluster_id)))
            exit(1)

    def destroy_cluster(self):
        for cluster_id in self.cluster_ids:
            logger.info(f"Deleting Capella cluster {cluster_id}...")
            resp = self.provisioned_api.delete_cluster_internal(
                self.tenant_id, self.project_id, cluster_id
            )
            raise_for_status(resp)
            logger.info("Capella cluster successfully queued for deletion.")

    def get_available_cidr(self):
        resp = self.provisioned_api.get_deployment_options(self.tenant_id, self.csp)

        return resp.json().get("cidr", {}).get("suggestedBlock")

    def get_deployed_cidr(self, cluster_id: str) -> Optional[str]:
        resp = self.provisioned_api.get_cluster_internal(
            self.tenant_id, self.project_id, cluster_id
        )
        return resp.json().get("data", {}).get("cidr")

    def get_hostnames(self, cluster_id):
        resp = self.provisioned_api.get_nodes(tenant_id=self.tenant_id,
                                              project_id=self.project_id,
                                              cluster_id=cluster_id)
        nodes = resp.json()['data']
        nodes = [node['data'] for node in nodes]
        services_per_node = {node['hostname']: node['services'] for node in nodes}

        kv_nodes = []
        non_kv_nodes = []
        for hostname, services in services_per_node.items():
            services_string = ','.join(SERVICES_CAPELLA_TO_PERFRUNNER[svc] for svc in services)
            if 'kv' in services_string:
                kv_nodes.append("{}:{}".format(hostname, services_string))
            else:
                non_kv_nodes.append("{}:{}".format(hostname, services_string))

        ret_list = kv_nodes + non_kv_nodes
        return ret_list

    def update_capella_spec(self):
        if not self.infra_spec.config.has_section("clusters_schemas"):
            self.infra_spec.config.add_section("clusters_schemas")
        for option, value in self.infra_spec.infrastructure_clusters.items():
            self.infra_spec.config.set('clusters_schemas', option, value)

        for i, cluster_id in enumerate(self.cluster_ids):
            hostnames = self.get_hostnames(cluster_id)
            cluster = self.infra_spec.config.options('clusters')[i]
            self.infra_spec.config.set('clusters', cluster, '\n' + '\n'.join(hostnames))

        self.infra_spec.update_spec_file()

    def disable_autoscaling(self):
        if self.provisioned_api.TOKEN_FOR_INTERNAL_SUPPORT is None:
            logger.error('Cannot create circuit breaker to prevent auto-scaling. No value found '
                         'for CBC_TOKEN_FOR_INTERNAL_SUPPORT, so cannot authenticate with CP API.')
            return

        for cluster_id in self.cluster_ids:
            logger.info(
                'Creating deployment circuit breaker to prevent auto-scaling for cluster {}.'
                .format(cluster_id)
            )

            resp = self.provisioned_api.create_circuit_breaker(cluster_id)
            raise_for_status(resp)

            resp = self.provisioned_api.get_circuit_breaker(cluster_id)
            raise_for_status(resp)

            logger.info('Circuit breaker created: {}'.format(pretty_dict(resp.json())))

    def peer_vpc(self, network_info, cluster_id):
        logger.info('Setting up VPC peering...')
        if self.csp == "aws":
            peering_connection = self._peer_vpc_aws(network_info, cluster_id)
        elif self.csp == "gcp":
            peering_connection, dns_managed_zone, client_vpc = self._peer_vpc_gcp(network_info,
                                                                                  cluster_id)
            self.infra_spec.config.set('infrastructure', 'dns_managed_zone', dns_managed_zone)
            self.infra_spec.config.set('infrastructure', 'client_vpc', client_vpc)

        if peering_connection:
            self.infra_spec.config.set('infrastructure', 'peering_connection', peering_connection)
            self.infra_spec.update_spec_file()
        else:
            exit(1)

    def _peer_vpc_aws(self, network_info, cluster_id) -> str:
        # Initiate VPC peering
        logger.info('Initiating peering')

        client_vpc = network_info['vpc_id']
        cidr = network_info['subnet_cidr']
        route_table = network_info['route_table_id']
        cluster_cidr = self.get_deployed_cidr(cluster_id)

        logger.info('Adding Capella private network (AWS): VPC ID = {}'.format(client_vpc))

        account_id = local('AWS_PROFILE=default env/bin/aws sts get-caller-identity '
                           '--query Account --output text',
                           capture=True)

        data = {
            "name": "perftest-network",
            "aws": {
                "accountId": account_id,
                "vpcId": client_vpc,
                "region": self.region,
                "cidr": cidr
            },
            "provider": "aws"
        }

        peering_connection_id = None

        try:
            resp = self.provisioned_api.create_private_network(
                self.tenant_id, self.project_id, cluster_id, data)
            private_network_id = resp.json()['id']

            # Get AWS CLI commands that we need to run to complete the peering process
            logger.info('Accepting peering request')
            resp = self.provisioned_api.get_private_network(
                self.tenant_id, self.project_id, cluster_id, private_network_id)
            aws_commands = resp.json()['data']['commands']
            peering_connection_id = resp.json()['data']['aws']['providerId']

            # Finish peering process using AWS CLI
            for command in aws_commands:
                local("AWS_PROFILE=default env/bin/{}".format(command))

            # Finally, set up route table in our client VPC
            logger.info('Configuring route table in client VPC')
            local(
                (
                    "AWS_PROFILE=default env/bin/aws --region {} ec2 create-route "
                    "--route-table-id {} "
                    "--destination-cidr-block {} "
                    "--vpc-peering-connection-id {}"
                ).format(self.region, route_table, cluster_cidr, peering_connection_id)
            )
        except Exception as e:
            logger.error('Failed to complete VPC peering: {}'.format(e))

        return peering_connection_id

    def _peer_vpc_gcp(self, network_info, cluster_id) -> tuple[str, str]:
        # Initiate VPC peering
        logger.info('Initiating peering')

        client_vpc = network_info['vpc_id']
        cidr = network_info['subnet_cidr']
        service_account = local("gcloud config get account", capture=True)

        logger.info('Adding Capella private network (GCP): VPC name = {}'.format(client_vpc))

        project_id = local('gcloud config get project', capture=True)

        data = {
            "name": "perftest-network",
            "gcp": {
                "projectId": project_id,
                "networkName": client_vpc,
                "cidr": cidr,
                "serviceAccount": service_account
            },
            "provider": "gcp"
        }

        peering_connection_name = None
        dns_managed_zone_name = None

        try:
            resp = self.provisioned_api.create_private_network(
                self.tenant_id, self.project_id, cluster_id, data)
            private_network_id = resp.json()['id']

            # Get gcloud commands that we need to run to complete the peering process
            logger.info('Accepting peering request')
            resp = self.provisioned_api.get_private_network(
                self.tenant_id, self.project_id, cluster_id, private_network_id)
            gcloud_commands = resp.json()['data']['commands']

            # Finish peering process using gcloud
            for command in gcloud_commands:
                local(command)

            peering_connection_name, capella_vpc_uri = local(
                (
                    'gcloud compute networks peerings list '
                    '--network={} '
                    '--format="value(peerings[].name,peerings[].network)"'
                ).format(client_vpc),
                capture=True
            ).split()

            dns_managed_zone_name = local(
                (
                    'gcloud dns managed-zones list '
                    '--filter="(peeringConfig.targetNetwork.networkUrl = {})" '
                    '--format="value(name)"'
                ).format(capella_vpc_uri),
                capture=True
            )
        except Exception as e:
            logger.error('Failed to complete VPC peering: {}'.format(e))

        return peering_connection_name, dns_managed_zone_name, client_vpc

    def destroy_peering_connection(self):
        logger.info("Destroying peering connection...")
        if self.csp == "aws":
            self._destroy_peering_connection_aws()
        elif self.csp == "gcp":
            self._destroy_peering_connection_gcp()

    def _destroy_peering_connection_aws(self):
        peering_connection = self.infra_spec.infrastructure_settings.get('peering_connection', None)

        if not peering_connection:
            logger.warn('No peering connection ID found in cluster spec; nothing to destroy.')
            return

        local(
            (
                "AWS_PROFILE=default env/bin/aws "
                "--region {} ec2 delete-vpc-peering-connection "
                "--vpc-peering-connection-id {}"
            ).format(self.region, peering_connection)
        )

    def _destroy_peering_connection_gcp(self):
        peering_connection = self.infra_spec.infrastructure_settings.get('peering_connection', None)

        if not peering_connection:
            logger.warn('No peering connection ID found in cluster spec; nothing to destroy.')
            return

        dns_managed_zone = self.infra_spec.infrastructure_settings['dns_managed_zone']
        client_vpc = self.infra_spec.infrastructure_settings['client_vpc']

        local('gcloud compute networks peerings delete {} --network={}'
              .format(peering_connection, client_vpc))
        local('gcloud dns managed-zones delete {}'.format(dns_managed_zone))

    def destroy_workflow(self):
        # In the future we could go through all the deployed workflows attached to the
        # cluster and destroy them. But, as of now, the APIs to check this may not exist in some
        # branches. Instead here we will only destroy the workflow if the test has explicitly
        # deployed it.
        workflow_id = self.infra_spec.controlplane_settings.get("workflow_id")
        if not workflow_id:
            return

        cluster_id = self.cluster_ids[0]
        try:
            logger.info(f"Destroying workflow {workflow_id}")
            resp = self.provisioned_api.delete_autovec_workflow(
                self.tenant_id, self.project_id, cluster_id, workflow_id
            )
            raise_for_status(resp)
            self.wait_for_workflow_destroy(cluster_id, workflow_id)
        except Exception as e:
            logger.error(f"Error while waiting for a workflow to be destroyed: {e}")

    def wait_for_workflow_destroy(self, cluster_id: str, workflow_id: str):
        logger.info(f"Waiting for workflow {workflow_id} to be destroyed")
        while True:
            resp = self.provisioned_api.get_autovec_workflow(
                self.tenant_id, self.project_id, cluster_id, workflow_id
            )
            if resp.status_code == 404:
                break


class EKSDeployer(CloudVMDeployer):
    pass


class AppServicesDeployer(CapellaProvisionedDeployer):
    def __init__(self, infra_spec: ClusterSpec, options: Namespace):
        super().__init__(infra_spec, options)
        self.cluster_id = self.cluster_ids[0]

    def deploy(self):
        # Configure terraform
        logger.info('Started deploying the AS')
        # Deploy capella cluster
        # Create sgw backend(app services)
        with record_time("app_services_cluster"):
            sgw_cluster_id = self.deploy_cluster_internal_api()

        with record_time("app_services_db"):
            self.deploy_app_endpoints(sgw_cluster_id)

        # Add allowed IPs
        logger.info("Whitelisting IPs")
        client_ips = self.infra_spec.clients
        if self.csp == "aws":
            client_ips = [
                dns.split(".")[0].removeprefix("ec2-").replace("-", ".") for dns in client_ips
            ]
        logger.info(f"The client list is: {client_ips}")
        for client_ip in client_ips:
            self.provisioned_api.add_allowed_ip_sgw(
                self.tenant_id, self.project_id, sgw_cluster_id, self.cluster_id, client_ip
            )

        # Allow my IP
        logger.info("Whitelisting own IP on sgw backend")
        self.provisioned_api.allow_my_ip_sgw(
            self.tenant_id,
            self.project_id,
            self.cluster_id,
            sgw_cluster_id,
        )

    def deploy_app_endpoints(self, sgw_cluster_id: str):
        user = {
            "email": "",
            "password": "Password123!",
            "name": "guest",
            "disabled": False,
            "admin_channels": ["*"],
            "admin_roles": [],
        }

        for bucket_name in self.test_config.buckets:
            # Create sgw database(app endpoint)
            logger.info("Deploying sgw database")
            sgw_db_name = self.deploy_sgw_db(sgw_cluster_id, bucket_name)

            # Set sync function
            logger.info("Setting sync function")
            self.provisioned_api.update_sync_function_sgw(
                self.tenant_id,
                self.project_id,
                self.cluster_id,
                sgw_cluster_id,
                sgw_db_name,
                "function (doc) { channel(doc.channels); }",
            )

            # Add app roles
            logger.info("Adding app roles")
            self.provisioned_api.add_app_role_sgw(
                self.tenant_id,
                self.project_id,
                self.cluster_id,
                sgw_cluster_id,
                sgw_db_name,
                {"name": "moderator", "admin_channels": []},
            )

            self.provisioned_api.add_app_role_sgw(
                self.tenant_id,
                self.project_id,
                self.cluster_id,
                sgw_cluster_id,
                sgw_db_name,
                {"name": "admin", "admin_channels": []},
            )

            # Add user
            logger.info("Adding user")
            self.provisioned_api.add_user_sgw(
                self.tenant_id,
                self.project_id,
                self.cluster_id,
                sgw_cluster_id,
                sgw_db_name,
                user,
            )

            # Add admin user
            bucket_count = bucket_name.split("-")[1]
            admin_name = f"admin{bucket_count}"
            logger.info(f"Adding admin user: {admin_name}")
            admin_user = {
                "name": admin_name,
                "password": "Password123!",
                "allEndpoints": True,
                "endpoints": [],
            }
            resp = self.provisioned_api.add_admin_user_sgw(
                self.tenant_id, self.project_id, self.cluster_id, sgw_cluster_id, admin_user
            )
            logger.info(f"The response is: {resp}")
            # Update cluster spec file
            self.update_app_services_spec(sgw_cluster_id, sgw_db_name)

    def destroy(self):
        # Destroy capella cluster
        sgw_cluster_id = self.infra_spec.infrastructure_settings['app_services_cluster']
        self.destroy_cluster_internal_api(sgw_cluster_id)

    def update_app_services_spec(self, sgw_cluster_id, sgw_db_name):
        # Get sgw public and private ips
        resp = self.provisioned_api.get_sgw_links(
            self.tenant_id, self.project_id, self.cluster_id, sgw_cluster_id, sgw_db_name
        )

        logger.info(f'The connect response is: {resp}')
        adminurl = resp.json().get('data').get('adminURL').split(':')[1].split('//')[1]
        logger.info(f'The admin url is: {adminurl}')
        if "sgw_schemas" not in self.infra_spec.config:
            self.infra_spec.config.add_section('sgw_schemas')
        for option, value in self.infra_spec.infrastructure_syncgateways.items():
            self.infra_spec.config.set('sgw_schemas', option, value)

        sgw_option = self.infra_spec.config.options('syncgateways')[0]
        sgw_list = []
        for _ in range(0, self.test_config.syncgateway_settings.nodes):
            sgw_list.append(adminurl)
        logger.info(f'the sgw list is: {sgw_list}')
        self.infra_spec.config.set('syncgateways', sgw_option, '\n' + '\n'.join(sgw_list))
        self.infra_spec.update_spec_file()

    @staticmethod
    def capella_sgw_group_sizes(self, node_schemas):
        sgw_groups = {}
        for node in node_schemas:
            _, cluster, node_group, _ = node.split('.')
            if cluster not in sgw_groups:
                sgw_groups[cluster] = [node_group]
            else:
                sgw_groups[cluster].append(node_group)

        sgw_group_sizes = {
            cluster: Counter(node_groups) for cluster, node_groups in sgw_groups.items()
        }

        return sgw_group_sizes

    def construct_capella_sgw_groups(self, infra_spec, node_schemas):
        sgw_groups = AppServicesDeployer.capella_sgw_group_sizes(node_schemas)

        for sgw_groups in sgw_groups.items():

            for (node_group) in sgw_groups.items():
                node_group_config = infra_spec.infrastructure_section(node_group)

                return node_group_config['instance_type']

    def deploy_cluster_internal_api(self):
        """Sample config.

        {
            "clusterId": "71eff4a9-bc4a-442f-8196-3339f9128c08",
            "name": "my-sync-gateway-backend",
            "description": "sgw backend that drives my amazing app",
            "desired_capacity": 2
            "compute": {"type":"c5.large"}
        }
        """
        config = {
            "clusterId": self.cluster_id,
            "name": "perf-sgw-{}".format(self.uuid),
            "description": "",
            "desired_capacity": self.test_config.syncgateway_settings.nodes,
            "compute": {"type": self.test_config.syncgateway_settings.instance}
        }

        if self.options.capella_sgw_version:
            config['version'] = self.options.capella_sgw_version

        logger.info(f'The payload is: {config}')

        resp = self.provisioned_api.create_sgw_backend(self.tenant_id, config)
        raise_for_status(resp)
        sgw_cluster_id = resp.json().get('id')
        logger.info(f'Initialised app services deployment {sgw_cluster_id}')
        logger.info('Saving app services ID to spec file.')

        self.infra_spec.config.set('infrastructure', 'app_services_cluster', sgw_cluster_id)
        self.infra_spec.update_spec_file()

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_sgw_cluster = sgw_cluster_id
        t0 = time()
        while pending_sgw_cluster and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            status = (
                self.provisioned_api.get_sgw_info(
                    self.tenant_id, self.project_id, self.cluster_id, sgw_cluster_id
                )
                .json()
                .get("data")
                .get("status")
                .get("state")
            )
            logger.info(f'Cluster state for {pending_sgw_cluster}: {status}')
            if status == "deploymentFailed":
                logger.error('Deployment failed for cluster {}. DataDog link for debugging: {}'
                             .format(pending_sgw_cluster,
                                     format_datadog_link(cluster_id=pending_sgw_cluster)))
                exit(1)

            if status == "healthy":
                break

        return sgw_cluster_id

    def deploy_sgw_db(self, sgw_cluster_id: str, bucket_name: str) -> str:
        """Sample config.

        {
            "name": "sgw-1",
            "sync": "",
            "bucket": "bucket-1",
            "delta_sync": false,
            "import_filter": ""
        }
        """
        bucket_count = int(bucket_name.split("-")[1])
        sgw_db_name = f'db-{bucket_count}'
        logger.info("The sgw db name is: {}".format(sgw_db_name))
        config = {
            "name": sgw_db_name,
            "bucket": bucket_name,
            "delta_sync": False,
        }
        collections_map = self.test_config.collection.collection_map

        if collections_map:
            """Add collection parameters
            "scopes": {
                "scope-1": {
                    "collections": {
                        "collection-1": {
                            "sync": "function(doc1){channel(doc1.channels);}",
                            "import_filter": "function(doc1){return true;}"
                        }
                    }
                }
            }
            """

            bucket = bucket_name
            target_scope_collections = collections_map[bucket]
            target_scopes = set()
            target_collections = set()

            for scope, collections in target_scope_collections.items():
                for collection, options in collections.items():
                    if options['load'] == 1 and options['access'] == 1:
                        target_scopes.add(scope)
                        target_collections.add(collection)

            scope_count = len(target_scopes)
            collection_count = len(target_collections)

            logger.info(f'The number of scopes is: {scope_count}')

            logger.info(f'The number of collections is: {collection_count}')
            config["scopes"] = {}
            scopes = {}
            for scope in target_scopes:
                logger.info(f'The current scope is: {scope}')
                collections = {}
                collections["collections"] = {}
                for collection in target_collections:
                    collections["collections"][collection] = {
                        "import_filter": "",
                        "sync": "function (doc) { channel(doc.channels); }"
                    }
                scopes[scope] = collections
            config["scopes"] = scopes
        logger.info(f'The configuration is: {config}')

        with record_time("app_services_per_db", sgw_db_name):
            self._deploy_app_services_db(sgw_cluster_id, sgw_db_name, bucket_count, config)

        return sgw_db_name

    def _deploy_app_services_db(
        self, sgw_cluster_id: str, sgw_db_name: str, bucket_count: int, config: dict
    ):
        resp = self.provisioned_api.create_sgw_database(
            self.tenant_id, self.project_id, self.cluster_id, sgw_cluster_id, config
        )
        logger.info(f'The response for creating the sgw db is: {resp}')
        sgw_db_id = resp.json().get('id')
        logger.info(f"Initialised sgw database deployment {sgw_db_id}")

        num_sgw = 0
        count = 0
        # We need to have the same number of sgw databases as the number of buckets
        logger.info(f"Waiting for {sgw_db_name} sgw databases to be online")
        while num_sgw < bucket_count:
            sleep(10)
            resp = self.provisioned_api.get_sgw_databases(
                self.tenant_id, self.project_id, self.cluster_id, sgw_cluster_id
            ).json()
            new_resp = resp.get('data')
            if new_resp is not None:
                num_sgw = 0
                for resp_bit in new_resp:
                    data_resp = resp_bit.get('data')
                    state = data_resp.get('state')
                    logger.info(f'The state is: {state}')

                    if state == 'Offline':
                        # Resume sgw database
                        logger.info('Resuming sgw database')
                        self.provisioned_api.resume_sgw_database(
                            self.tenant_id,
                            self.project_id,
                            self.cluster_id,
                            sgw_cluster_id,
                            sgw_db_name,
                        )

                    if state == 'Online':
                        num_sgw += 1
            count += 1
            logger.info(f'The number of sgw databases is: {num_sgw}')
            if count > 1000:
                logger.error("SGW database deployment timed out")
                exit(1)

        logger.info("SGW databases successfully created")

    def destroy_cluster_internal_api(self, sgw_cluster_id):
        logger.info('Deleting Capella App Services cluster...')
        resp = self.provisioned_api.delete_sgw_backend(
            self.tenant_id, self.project_id, self.cluster_id, sgw_cluster_id
        )

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_sgw_cluster = sgw_cluster_id
        t0 = time()
        while pending_sgw_cluster and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)
            try:
                status = self.provisioned_api.get_sgw_info(
                    self.tenant_id, self.project_id, self.cluster_id, sgw_cluster_id
                )
                status = status.json().get('data').get('status').get('state')
                logger.info(f'Cluster state for {pending_sgw_cluster}: {status}')
            except Exception as e:
                logger.info(f'The exception is: {e}')
                sleep(30)
                break

        raise_for_status(resp)
        logger.info('Capella App Services cluster successfully queued for deletion.')


class CapellaColumnarDeployer(CloudVMDeployer):
    def __init__(self, infra_spec: ClusterSpec, options: Namespace):
        super().__init__(infra_spec, options)

        test_config = TestConfig()
        if options.test_config:
            test_config.parse(options.test_config, override=options.override)
        else:
            test_config.override(options.override)
        self.test_config = test_config

        access, secret = None, None
        if api_keys := ControlPlaneManager.get_api_keys():
            access, secret = api_keys

        self.columnar_api = CapellaAPIColumnar(
            self.infra_spec.controlplane_settings["public_api_url"],
            secret,
            access,
            os.getenv("CBC_USER"),
            os.getenv("CBC_PWD"),
            os.getenv("CBC_TOKEN_FOR_INTERNAL_SUPPORT"),
        )

        self.tenant_id = self.infra_spec.controlplane_settings["org"]
        self.project_id = self.infra_spec.controlplane_settings["project"]
        self.instance_ids = self.infra_spec.controlplane_settings.get("columnar_ids", "").split()

        # right now this depends on capella operational clusters being "inactive" in the infra spec
        self.cluster_ids = self.infra_spec.capella_cluster_ids

        self.deployment_durations = {}
        self.capella_timeout = max(0, self.options.capella_timeout)

    def deploy(self):
        if not self.options.capella_only:
            super().deploy()

        # Deploy capella cluster(s)
        self.deploy_columnar_instances()
        self.wait_for_columnar_instance_deploy()

        # Update cluster spec file
        self.update_columnar_spec()
        self.update_allowlists()

    def update_allowlists(self):
        logger.info('Updating allowlists for Columnar clusters')
        myip = my_public_ip()

        client_ips = (
            [
                dns.split(".")[0].removeprefix("ec2-").replace("-", ".")
                for dns in self.infra_spec.clients
            ]
            if self.csp == "aws"
            else self.infra_spec.clients
        )

        all_ips = [myip] + client_ips
        logger.info(f'Allowing IPs: {pretty_dict(all_ips)}')

        for instance_id in self.instance_ids:
            logger.info(f'Updating allowlist for instance {instance_id}')
            for ip in all_ips:
                self.columnar_api.allow_ip(self.tenant_id, self.project_id, instance_id, f'{ip}/32')

    def deploy_columnar_instances(self):
        instance_sizes = [len(servers) for _, servers in self.infra_spec.clusters]
        node_groups = [servers[0].split(".")[2] for _, servers in self.infra_spec.clusters]
        names = [f"perf-columnar-{self.uuid}" for _ in instance_sizes]
        if len(names) > 1:
            names = [f'{name}-{i}'for i, name in enumerate(names)]

        logger.info("Fetching deployment options for Columnar instances.")
        resp = self.columnar_api.get_deployment_options(self.tenant_id, self.csp, self.region)
        raise_for_status(resp)
        deployment_options = resp.json()
        instance_type_options = {c.pop("key"): c for c in deployment_options["compute"]}

        for name, size, node_group in zip(names, instance_sizes, node_groups):
            node_group_info = self.infra_spec.infrastructure_section(node_group)
            instance_type = node_group_info.get("instance_type")

            if not (instance_type_info := instance_type_options.get(instance_type)):
                logger.interrupt(
                    f"Invalid instance type: {instance_type}\n"
                    f"Valid options: {pretty_dict(instance_type_options)}"
                )

            config = {
                "name": name,
                "description": "",
                "provider": self.csp,
                "region": self.region,
                "nodes": size,
                "instanceTypes": {
                    "vcpus": f"{instance_type_info['cpus']}vCPUs",
                    "memory": f"{instance_type_info['memoryInGb']}GB",
                },
                "availabilityZone": "multi" if self.options.multi_az else "single",
                "package": {"key": "enterprise", "timezone": "PT"},
            }

            logger.info(f"Deploying Columnar instance with config: {pretty_dict(config)}")

            if ami := self.options.columnar_ami:
                logger.info(f"Overriding Columnar AMI: {ami}")
                config |= {
                    "overRide": {
                        "image": ami,
                        "token": os.getenv("CBC_OVERRIDE_TOKEN"),
                    }
                }

            resp = self.columnar_api.create_columnar_instance(
                self.tenant_id, self.project_id, config
            )
            raise_for_status(resp)

            instance_id = resp.json().get("id")
            self.deployment_durations[instance_id] = time()
            self.instance_ids.append(instance_id)

            logger.info(f"Initialised Columnar instance deployment {instance_id}")
            logger.info("Saving Columnar instance ID to spec file.")
            self.infra_spec.config.set("controlplane", "columnar_ids", "\n".join(self.instance_ids))
            self.infra_spec.update_spec_file()

    def wait_for_columnar_instance_deploy(self):
        timeout_mins = self.capella_timeout
        interval_secs = self.test_config.deployment.capella_poll_interval_secs
        pending_instances = [instance_id for instance_id in self.instance_ids]
        logger.info("Waiting for Columnar instance(s) to be deployed...")
        t0 = time()
        poll_duration = 0
        while pending_instances and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs - poll_duration if poll_duration < interval_secs else 0)
            poll_start = time()

            statuses = []
            for instance_id in pending_instances:
                resp = self.columnar_api.get_specific_columnar_instance(
                    self.tenant_id, self.project_id, instance_id
                )
                raise_for_status(resp)
                resp_payload = resp.json()
                status = resp_payload["data"]["state"]
                logger.info(f"Instance state for {instance_id}: {status}")

                if status == "deploy_failed":
                    logger.error(f"Deployment failed for Columnar instance {instance_id}")
                    exit(1)
                elif status == "healthy":
                    self.deployment_durations[instance_id] = (
                        time() - self.deployment_durations[instance_id]
                    )
                    logger.info(
                        f"Columnar instance {instance_id} deployed successfully after "
                        f"{self.deployment_durations[instance_id]}s"
                    )

                    cluster_id = resp_payload["data"]["config"]["clusterId"]
                    logger.info(f"Cluster ID for instance {instance_id}: {cluster_id}")
                    self.cluster_ids.append(cluster_id)
                    existing_cluster_ids = self.infra_spec.controlplane_settings.get(
                        "cluster_ids", ""
                    )
                    self.infra_spec.config.set(
                        "controlplane", "cluster_ids", existing_cluster_ids + f"\n{cluster_id}"
                    )
                    self.infra_spec.update_spec_file()

                statuses.append(status)

            pending_instances = [
                pending_instances[i] for i, status in enumerate(statuses) if status != "healthy"
            ]

            poll_duration = time() - poll_start

        if pending_instances:
            logger.error(f"Deployment timed out after {timeout_mins} mins")
            exit(1)

        logger.info("Successfully deployed all Columnar instances")
        timing_results = "\n\t".join(
            f"{iid}: [{duration - interval_secs:4.0f} - {duration:4.0f}]s"
            for iid, duration in self.deployment_durations.items()
        )
        logger.info(f"Deployment timings:\n\t{timing_results}")

        with TimeTrackingFile() as t:
            t.update("columnar_instances", self.deployment_durations)

    def destroy(self):
        if not self.options.capella_only:
            # Destroy non-capella resources
            self.terraform_destroy(self.csp)

        # Destroy Columnar instance(s)
        if not self.options.keep_cluster:
            self.destroy_columnar_instance()
            self.wait_for_columnar_instance_destroy()

    def destroy_columnar_instance(self):
        for instance_id in self.instance_ids:
            logger.info(f"Deleting Columnar instance {instance_id}...")
            resp = self.columnar_api.delete_columnar_instance(
                self.tenant_id, self.project_id, instance_id
            )
            raise_for_status(resp)
            logger.info("Columnar instance successfully queued for deletion.")

    def wait_for_columnar_instance_destroy(self):
        logger.info("Waiting for Columnar instances to be destroyed...")

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_clusters = [cluster_id for cluster_id in self.cluster_ids]
        t0 = time()
        while pending_clusters and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            still_pending_clusters = []
            for cluster_id, instance_id in zip(pending_clusters, self.instance_ids):
                resp = self.columnar_api.get_cluster_info_internal(cluster_id)
                if resp.status_code != 404:
                    raise_for_status(resp)
                    logger.info(
                        f"Cluster {cluster_id} (for instance {instance_id}) not destroyed yet."
                    )
                    still_pending_clusters.append(cluster_id)

            pending_clusters = still_pending_clusters

        if pending_clusters:
            logger.error(
                f"Timed out after {timeout_mins} mins waiting for "
                "Columnar instance clusters to delete."
            )
        else:
            logger.info("All Columnar instance clusters destroyed.")

    def update_columnar_spec(self):
        """Update the infrastructure spec file with hostnames of Columnar compute nodes."""
        for instance_id, (cluster_label, _) in zip(self.instance_ids, self.infra_spec.clusters):
            resp = self.columnar_api.get_columnar_nodes(instance_id)
            resp.raise_for_status()
            nodes = resp.json()

            hostnames_with_services = []
            for n in nodes:
                hostname = n["config"]["hostname"]
                services = ",".join(s["type"] for s in n["config"]["services"])
                hostnames_with_services.append(f"{hostname}:{services}")

            logger.info(
                f"Cluster nodes for instance {instance_id}: {pretty_dict(hostnames_with_services)}"
            )

            self.infra_spec.config.set(
                "clusters", cluster_label, "\n" + "\n".join(hostnames_with_services)
            )

        self.infra_spec.update_spec_file()


class CapellaModelServicesDeployer(CapellaProvisionedDeployer):
    def deploy(self):
        # Deploy a provisional cluster first
        super().deploy()
        # Now we can deploy the model services
        self.deploy_model_services()

    def destroy(self):
        # First destroy any model services resources
        try:
            self.destroy_model_services()
        except Exception as e:
            logger.error(f"Error while destroying model services: {e}")

        # Now we can destroy the cluster
        return super().destroy()

    def deploy_model_services(self):
        logger.info("Deploying model services...")
        models = self.infra_spec.infrastructure_model_services
        # We need model information from the  model catalog to deploy the models
        catalog_data = self._get_model_catalog()
        deployed_models = []
        # We always want to deploy embedding model first as an LLM with value-added services may
        # have dependency on the embedding model
        for model_kind in ["embedding-generation", "text-generation"]:
            model_config = models.get(model_kind)
            if not model_config:
                continue
            model_name = model_config.get("model_name")
            # We dont have a way to get model by name from the catalog so we need to iterate over
            # the catalog and find the model matching the name
            catalog_model = next(
                (model for model in catalog_data if model.get("modelName") == model_name),
                None,
            )
            if not catalog_model:
                logger.error(f"Model {model_name} not found in the catalog")
                continue

            model_id = model_config.get("model_id")
            if model_id:
                # For working with shared existing models and debugging purposes
                logger.info(f"Skipping deploying {model_name}, using existing model id {model_id}")
                self._store_model_endpoint(model_id, model_kind)
            else:
                with record_time("model_deployment", model_kind):
                    model_id = self.deploy_model(model_kind, model_config, catalog_model)
            deployed_models.append(model_id)

        if deployed_models:
            self.create_model_api_key(deployed_models)
        else:
            # We didnt deploy any models, so we log the model catalog data for debugging purposes
            logger.info(f"Model catalog data: {pretty_dict(catalog_data)}")
            raise Exception("No models deployed")

    def _store_model_endpoint(self, model_id: str, model_kind: str):
        model_endpoint = self._get_model_data(model_id).get("network", {}).get("endpoint")
        self.infra_spec.config.set(model_kind, "model_endpoint", model_endpoint)
        self.infra_spec.update_spec_file()

    def deploy_model(self, model_kind: str, model_config: dict, catalog: dict) -> str:
        model_name = model_config.get("model_name")
        logger.info(f"Deploying model {model_name} using catalog entry: {pretty_dict(catalog)}")
        payload = {
            "name": model_name,
            "modelCatalogId": catalog.get("id"),
            "config": {
                "provider": self.csp,
                "region": self.region,
                "multiAZ": self.options.multi_az,
                "compute": {
                    "instanceType": model_config.get("instance_type"),
                    "instanceCount": int(model_config.get("instance_capacity", 1)),
                },
            },
        }

        if model_kind == "embedding-generation":
            payload["parameters"] = {
                "tuning": {"dimensions": self.test_config.ai_services_settings.model_dimensions}
            }

        logger.info(f"Deploying model with payload: {payload}")
        try:
            resp = self.provisioned_api.deploy_model(self.tenant_id, payload)
            resp.raise_for_status()
            hosted_model_id = resp.json().get("id")
            logger.info(f"Hosted model created with id {hosted_model_id}")
            # If we get an id, we want to store it so we can destroy it later even
            # if the rest of the code fails
            self.infra_spec.config.set(model_kind, "model_id", hosted_model_id)
            self.infra_spec.update_spec_file()

            self.wait_for_hosted_model_status(hosted_model_id, "healthy")
            self._store_model_endpoint(hosted_model_id, model_kind)
            return hosted_model_id
        except Exception as e:
            logger.error(f"Error while deploying a hosted model: {e}")
            raise e  # Reraise the exception to stop the deployment

    def destroy_model_services(self):
        logger.info("Destroying model services...")
        models = self.infra_spec.infrastructure_model_services
        # We destroy an llm first before an embedding model, if it exists
        for model_kind in ["text-generation", "embedding-generation"]:
            model_config = models.get(model_kind, {})
            model_id = model_config.get("model_id")
            if not model_id:
                continue

            if not maybe_atoi(model_config.get("can_delete", "true")):
                # Allow using shared models which we should not delete in our automation
                logger.info(f"Skipping destroying {model_kind} model, id: {model_id}")
                continue

            try:
                logger.info(f"Destroying {model_kind} model, id: {model_id}")
                with record_time("model_deletion", model_kind):
                    resp = self.provisioned_api.delete_model(self.tenant_id, model_id)
                    resp.raise_for_status()
                    logger.info(f"Model {model_id} successfully queued for deletion.")
                    self.wait_for_hosted_model_status(model_id, "")
            except Exception as e:
                logger.error(f"Error while destroying model {model_id}: {e}")

    def wait_for_hosted_model_status(self, model_id: str, status: str):
        if status:
            logger.info(f"Waiting for hosted model {model_id} to reach status {status}")
        retries = 0
        status = status.lower()
        interval_secs = 60  # Check for status every minute
        while True:
            model_details = self._get_model_data(model_id)
            model_status = model_details.get("modelConfig", {}).get("status", "").lower()
            if model_status == status:
                logger.info(f"Deployed model: {model_details}")
                return
            if retries % 10 == 0:  # Log model status every 10 minutes
                logger.info(f"Hosted model status: {model_status}")
            if retries >= 60:
                # From current measurements, if a model does not reach a desired state
                # after an hour, it is likely to never reach that state
                raise Exception(
                    f"Hosted model {model_id} failed to reach the desired status {model_details}"
                )

            retries += 1
            sleep(interval_secs)

    def _get_model_data(self, model_id: str) -> dict:
        resp = self.provisioned_api.get_model_details(self.tenant_id, model_id)
        return resp.json().get("data", {})

    def _get_model_catalog(self) -> dict:
        """Get the model catalog from the environment Catalog."""
        logger.info(
            f"Getting model catalog from {self.infra_spec.controlplane_settings['env']} catalog..."
        )
        resp = self.provisioned_api.get_model_catalog()
        return resp.json() or []

    def create_model_api_key(self, model_ids: list[str]):
        """Get an API key for a list of model ids."""
        payload = {
            "name": f"model_api_key-{self.uuid}",
            "description": "",
            "expiryDuration": 60 * 60 * 24,  # 1 day
            "accessPolicy": {
                "allowedIPs": self._get_allowed_ips(),
            },
        }
        logger.info(f"Creating models API keys with payload: {pretty_dict(payload)}")
        for model_id in model_ids:
            resp = self.provisioned_api.create_model_api_key(self.tenant_id, model_id, payload)
            api_key_data = resp.json()
            logger.info(f"Model API key created: {api_key_data}")
            ControlPlaneManager.save_model_api_key(
                model_id, api_key_data.get("keyId", ""), api_key_data.get("apiKey", "")
            )

# CLI args.
def get_args():
    parser = ArgumentParser()

    parser.add_argument(
        "-c", "--cluster", required=True, help="the path to a infrastructure specification file"
    )
    parser.add_argument(
        "--test-config", required=False, help="the path to the test configuration file"
    )
    parser.add_argument("--verbose", action="store_true", help="enable verbose logging")
    parser.add_argument("-r", "--region", help="the cloud region (AWS, Azure)")
    parser.add_argument("-z", "--zone", help="the cloud zone (AWS, GCP)")
    parser.add_argument("--cluster-image", help="Image/AMI name to use for cluster nodes")
    parser.add_argument("--client-image", help="Image/AMI name to use for client nodes")
    parser.add_argument("--utility-image", help="Image/AMI name to use for utility nodes")
    parser.add_argument("--sgw-image", help="Image/AMI name to use for sync gateway nodes")
    parser.add_argument("--kafka-image", help="Image/AMI name to use for Kafka nodes")
    parser.add_argument("--capella-public-api-url", help="public API URL for Capella environment")
    parser.add_argument("--capella-tenant", help="tenant ID for Capella deployment")
    parser.add_argument("--capella-project", help="project ID for Capella deployment")
    parser.add_argument("--capella-cb-version", help="cb version to use for Capella deployment")
    parser.add_argument("--capella-sgw-version", help="SGW version to use for Capella deployment")
    parser.add_argument("--capella-ami", help="custom AMI to use for Capella deployment")
    parser.add_argument(
        "--capella-sgw-ami", help="custom AMI to use for App Services Capella Deployment"
    )
    parser.add_argument("--columnar-ami", help="custom AMI to use for Columnar deployment")
    parser.add_argument("--release-id", help="release id for managing releases")
    parser.add_argument(
        "--vpc-peering", action="store_true", help="enable VPC peering for Capella deployment"
    )
    parser.add_argument(
        "--capella-timeout",
        type=int,
        default=20,
        help="Timeout (minutes) for Capella deployment when using internal API",
    )
    parser.add_argument(
        "--disable-autoscaling",
        action="store_true",
        help="Disable cluster auto-scaling for Capella clusters by creating a "
        "deployment circuit breaker for the cluster.",
    )
    parser.add_argument(
        "--keep-cluster",
        action="store_true",
        help="Don't destroy cluster, only the clients and utilities",
    )
    parser.add_argument(
        "--capella-only",
        action="store_true",
        help="Only deploy Capella resources. Will not deploy perf client or utility nodes.",
    )
    parser.add_argument("-t", "--tag", help="Global tag for launched instances.")
    parser.add_argument(
        "--enable-disk-autoscaling",
        action="store_true",
        default=True,
        help="Enables Capella disk autoscaling",
    )
    parser.add_argument(
        "--multi-az",
        action="store_true",
        default=False,
        help="Deploy Capella cluster to multiple Availability Zones",
    )
    parser.add_argument("override", nargs="*", help="custom cluster and/or test settings")

    return parser.parse_args()


def destroy():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster, override=args.override)

    if infra_spec.cloud_provider != 'capella':
        deployer = CloudVMDeployer(infra_spec, args)
    elif infra_spec.serverless_infrastructure:
        logger.interrupt("Serverless no longer supported in perfrunner.")
    elif infra_spec.app_services == 'true':
        deployer = AppServicesDeployer(infra_spec, args)
    elif infra_spec.columnar_infrastructure:
        if prov_cluster := infra_spec.prov_cluster_in_columnar_test:
            infra_spec.set_inactive_clusters_by_name([prov_cluster])
            CapellaColumnarDeployer(infra_spec, args).destroy()
            infra_spec.set_active_clusters_by_name([prov_cluster])
            deployer = CapellaProvisionedDeployer(infra_spec, args)
        else:
            deployer = CapellaColumnarDeployer(infra_spec, args)
    elif infra_spec.has_model_services_infrastructure:
        deployer = CapellaModelServicesDeployer(infra_spec, args)
    else:
        deployer = CapellaProvisionedDeployer(infra_spec, args)

    deployer.destroy()

    if infra_spec.cloud_provider == "capella" and infra_spec.app_services != "true":
        cp_prepper = ControlPlaneManager(
            infra_spec,
            args.capella_public_api_url,
            args.capella_tenant,
            args.capella_project,
        )
        cp_prepper.save_org_id()
        cp_prepper.clean_up_api_keys()
        if not args.keep_cluster:
            cp_prepper.save_project_id(False)
            cp_prepper.clean_up_project()


def main():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster, override=args.override)

    if infra_spec.cloud_provider != 'capella':
        deployer = CloudVMDeployer(infra_spec, args)
    else:
        cp_prepper = ControlPlaneManager(
            infra_spec,
            args.capella_public_api_url,
            args.capella_tenant,
            args.capella_project,
            args.tag,
        )
        cp_prepper.save_org_id()
        cp_prepper.save_api_keys()
        cp_prepper.save_project_id()

        if infra_spec.serverless_infrastructure:
            logger.interrupt("Serverless no longer supported in perfrunner.")
        elif infra_spec.app_services == "true":
            deployer = AppServicesDeployer(infra_spec, args)
        elif infra_spec.columnar_infrastructure:
            if prov_cluster := infra_spec.prov_cluster_in_columnar_test:
                infra_spec.set_active_clusters_by_name([prov_cluster])
                CapellaProvisionedDeployer(infra_spec, args).deploy()
                infra_spec.set_inactive_clusters_by_name([prov_cluster])
                args.capella_only = True
            deployer = CapellaColumnarDeployer(infra_spec, args)
        elif infra_spec.has_model_services_infrastructure:
            deployer = CapellaModelServicesDeployer(infra_spec, args)
        else:
            deployer = CapellaProvisionedDeployer(infra_spec, args)

    deployer.deploy()
    # Log the timing data at the end of the deployment
    with TimeTrackingFile() as t:
        t.log_content()

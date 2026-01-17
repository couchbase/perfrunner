import csv
import itertools
import json
import os
import re
from configparser import ConfigParser, NoOptionError, NoSectionError
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
from itertools import chain, combinations, permutations
from typing import Any, Iterable, Iterator, Optional, Tuple
from uuid import uuid4

from decorator import decorator

from logger import logger
from perfrunner.helpers.misc import (
    SafeEnum,
    creds_tuple,
    maybe_atoi,
    run_aws_cli_command,
    target_hash,
)

CBMONITOR_HOST = 'cbmonitor.sc.couchbase.com'
SHOWFAST_HOST = 'showfast.sc.couchbase.com'  # 'localhost:8000'
REPO = 'https://github.com/couchbase/perfrunner'
CAPELLA_PUBLIC_API_URL_TEMPLATE = "https://cloudapi.{}.nonprod-project-avengers.com"

class CBProduct(Enum):
    COUCHBASE_SERVER = "couchbase-server"
    ENTERPRISE_ANALYTICS = "enterprise-analytics"


class CBProfile(Enum):
    DEFAULT = "default"
    PROVISIONED = "provisioned"
    COLUMNAR = "columnar"
    ANALYTICS = "analytics"


@decorator
def safe(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except (NoSectionError, NoOptionError) as e:
        logger.warn('Failed to get option from config: {}'.format(e))


class Config:

    def __init__(self):
        self.config = ConfigParser()
        self.name = ''
        self.fname = ''

    def parse(self, fname: str, override: Optional[list[str]] = None):
        logger.info('Reading configuration file: {}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt("File doesn't exist: {}".format(fname))
        self.config.optionxform = str
        self.config.read(fname)

        self.fname = fname
        basename = os.path.basename(fname)
        self.name = os.path.splitext(basename)[0]

        if override is not None:
            self.override(override)

    def _reinsert_spaces_in_overrides(self, overrides: list[str]) -> list[str]:
        """Reinsert spaces into overrides that were removed by shell word splitting."""
        new_overrides = []
        quote_stack = []
        current_override = ""
        for arg in overrides:
            for char in arg:
                if char not in ["'", '"']:
                    continue

                if quote_stack and char == quote_stack[-1]:
                    quote_stack.pop()
                else:
                    quote_stack.append(char)

            if not quote_stack:
                new_overrides.append(current_override + arg)
                current_override = ""
            else:
                current_override += arg + " "

        return new_overrides

    def override(self, overrides: list[str]):
        overrides = [
            [col.strip("'") for col in row]  # csv.reader strips double quotes but not single quotes
            for row in csv.reader(self._reinsert_spaces_in_overrides(overrides), delimiter=".")
        ]

        for section, option, value in overrides:
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, option, value)

    def update_spec_file(self, new_file: str = None):
        dest_file = new_file or self.fname
        with open(dest_file, "w") as f:
            self.config.write(f)

    @safe
    def _get_options_as_dict(self, section: str) -> dict:
        if section in self.config.sections():
            return {p: v for p, v in self.config.items(section)}
        else:
            return {}


class ClusterSpec(Config):

    def __init__(self):
        super().__init__()
        self.inactive_cluster_idxs = set()

    @property
    def dynamic_infrastructure(self) -> bool:
        return self.cloud_infrastructure and self.kubernetes_infrastructure

    @property
    def cloud_infrastructure(self) -> bool:
        return 'infrastructure' in self.config.sections()

    @property
    def csp(self) -> str:
        """Cloud Service Provider."""
        if self.cloud_infrastructure:
            return self.capella_backend or self.cloud_provider
        return ""

    @property
    def cloud_provider(self) -> str:
        return self.config.get('infrastructure', 'provider', fallback='')

    @property
    def cloud_region(self) -> str:
        return self.config.get("infrastructure", "region", fallback="")

    @property
    def capella_backend(self) -> str:
        return self.config.get('infrastructure', 'backend', fallback='')

    @property
    def app_services(self):
        return self.config.get('infrastructure', 'app_services', fallback='')

    @property
    def kubernetes_infrastructure(self) -> bool:
        if self.cloud_infrastructure and not self.capella_infrastructure:
            return self.infrastructure_settings.get("type", "kubernetes") == "kubernetes"
        return False

    @property
    def capella_infrastructure(self) -> bool:
        if self.cloud_infrastructure:
            return self.infrastructure_settings.get("provider", "aws") == "capella"
        return False

    @property
    def columnar_infrastructure(self) -> bool:
        if self.cloud_infrastructure:
            return self.infrastructure_settings.get("service", "") == "columnar"
        return False

    @property
    def prov_cluster_in_columnar_test(self) -> Optional[str]:
        """For Columnar tests that use a provisioned cluster, return provisioned cluster name."""
        if self.columnar_infrastructure:
            return self.infrastructure_settings.get("provisioned_cluster")
        return None

    @property
    def has_model_services_infrastructure(self) -> bool:
        return self.config.get("infrastructure", "model_services", fallback="false") == "true"

    @property
    def generated_cloud_config_path(self):
        if self.cloud_infrastructure:
            return "cloud/infrastructure/generated/infrastructure_config.json"
        return None

    @property
    def controlplane_settings(self) -> dict[str, str]:
        settings = self.infrastructure_section("controlplane")
        if env := settings.get("env"):
            settings["public_api_url"] = CAPELLA_PUBLIC_API_URL_TEMPLATE.format(env)
        return settings

    @property
    def infrastructure_settings(self):
        if self.config.has_section('infrastructure'):
            return {k: v for k, v in self.config.items('infrastructure')}
        return {}

    @property
    def infrastructure_clusters(self):
        if not self.config.has_section("clusters"):
            return {}
        return {k: v for i, (k, v) in enumerate(self.config.items('clusters'))
                if i not in self.inactive_cluster_idxs}

    @property
    def infrastructure_clients(self):
        if self.config.has_section('clients'):
            return {k: v for k, v in self.config.items('clients')}
        return {}

    @property
    def infrastructure_syncgateways(self):
        if self.config.has_section('syncgateways'):
            return {k: v for k, v in self.config.items('syncgateways')}
        return {}

    @property
    def infrastructure_utilities(self):
        if self.config.has_section('utilities'):
            return {k: v for k, v in self.config.items('utilities')}
        return {}

    @property
    def infrastructure_kafka_clusters(self):
        if self.config.has_section('kafka_clusters'):
            return {k: v for k, v in self.config.items('kafka_clusters')}
        return {}

    @property
    def infrastructure_model_services(self) -> dict:
        # We currently can only have an embedding, an LLM model or both.
        # We can't have multiple models of the same type
        models = {}
        for model_kind in ["embedding-generation", "text-generation"]:
            if self.config.has_section(model_kind):
                models[model_kind] = self._get_options_as_dict(model_kind)
        return models

    @property
    def external_client(self) -> bool:
        return self.infrastructure_settings.get('external_client', 'false') == 'true'

    @property
    def is_openshift(self) -> bool:
        return self.cloud_provider == 'openshift'

    @property
    def clusters_modified_names(self) -> list[str]:
        """Return cluster names appended with the deployment id."""
        # When exposing the cluster through route53, cluster names are used to create records.
        # To avoid conflicting records when running multiple tests,
        # we append the deployment id to the names.

        if not self.config.has_section("clusters"):
            return []
        cluster_uuid = self.get_or_create_infrastructure_uuid()
        return [
            f"{cluster_name}-{cluster_uuid}" for cluster_name in self.config.options("clusters")
        ]

    def get_or_create_infrastructure_uuid(self) -> str:
        """Get the UUID for the infrastructure if it exists, otherwise create a new one."""
        if not self.config.has_section("infrastructure"):
            return ""  # For on-prem tests, we don't need a UUID

        cluster_uuid = self.infrastructure_settings.get("uuid")
        if not cluster_uuid:
            # Create a new UUID for the infrastructure and save it to the spec file
            cluster_uuid = uuid4().hex[:6]
            self.config.set("infrastructure", "uuid", cluster_uuid)
            self.update_spec_file()
        return cluster_uuid

    def kubernetes_version(self, cluster_name: str) -> str:
        return self.infrastructure_section(cluster_name).get("version", "1.32")

    def istio_enabled(self, cluster_name):
        istio_enabled = self.infrastructure_section(cluster_name).get('istio_enabled', 0)
        istio_enabled = bool(int(istio_enabled))
        return istio_enabled

    def kubernetes_storage_class(self, cluster_name):
        return self.infrastructure_section(cluster_name) \
            .get('storage_class', 'default')

    def kubernetes_clusters(self):
        k8s_clusters = []
        if 'k8s' in self.config.sections():
            for k, v in self.config.items('k8s'):
                k8s_clusters += v.split(",")
        return k8s_clusters

    def infrastructure_section(self, section: str) -> dict[str, str]:
        if section in self.config.sections():
            return {k: v for k, v in self.config.items(section)}
        return {}

    def infrastructure_config(self):
        infra_config = {}
        for section in self.config.sections():
            infra_config[section] = {p: v for p, v in self.config.items(section)}
        return infra_config

    @cached_property
    def products_by_server(self) -> dict[str, CBProduct]:
        if not self.config.has_section("clusters"):
            return {}

        properties = {}
        clusters = self.config.items("clusters")

        def cluster_product(i: int) -> CBProduct:
            if self.capella_infrastructure or not self.columnar_infrastructure:
                return CBProduct.COUCHBASE_SERVER

            if len(clusters) == 1:
                return CBProduct.ENTERPRISE_ANALYTICS

            return list(CBProduct)[min(i, 1)]

        for i, (_, nodes) in enumerate(clusters):
            product = cluster_product(i)
            for node in nodes.split():
                properties[node.split(":")[0]] = product

        return properties

    @property
    def clusters(self) -> Iterator:
        for cluster_name, servers in self.infrastructure_clusters.items():
            hosts = [s.split(':')[0] for s in servers.split()]
            yield cluster_name, hosts

    @property
    def clusters_schemas(self) -> Iterator:
        if self.capella_infrastructure:
            for i, (cluster_name, servers) in enumerate(self.config.items('clusters_schemas')):
                if i not in self.inactive_cluster_idxs:
                    schemas = servers.split()
                    yield cluster_name, schemas

    @property
    def sgw_clusters(self) -> Iterator:
        for cluster_name, servers in self.config.items('syncgateways'):
            hosts = [s.split(':')[0] for s in servers.split()]
            yield cluster_name, hosts

    @property
    def cbl_clusters(self) -> Iterator:
        for cluster_name, servers in self.config.items('cblites'):
            hosts = [s.split(':')[0] for s in servers.split()]
            yield cluster_name, hosts

    @property
    def kafka_clusters(self) -> Iterator:
        for cluster_name, servers in self.config.items('kafka_clusters'):
            hosts = [s.split(':')[0] for s in servers.split()]
            yield cluster_name, hosts

    @property
    def masters(self) -> Iterator[str]:
        for _, servers in self.clusters:
            yield servers[0]

    @property
    def sgw_masters(self) -> Iterator[str]:
        if self.config.has_section('syncgateways'):
            for _, servers in self.sgw_clusters:
                yield servers[0]
        else:
            for _, servers in self.clusters:
                yield servers[0]

    @property
    def servers(self) -> list[str]:
        servers = [node for _, cluster_servers in self.clusters for node in cluster_servers]
        return servers

    @property
    def clients(self) -> list[str]:
        clients = []
        for client_servers in self.infrastructure_clients.values():
            clients += client_servers.strip().split('\n')
        return clients

    @property
    def client_ips(self) -> list[str]:
        """Return client IPs."""
        if self.capella_backend == "aws":
            return [
                dns.split(".")[0].removeprefix("ec2-").replace("-", ".") for dns in self.clients
            ]
        return self.clients

    @property
    def utility_profile(self) -> str:
        return self.infrastructure_utilities.get("profile", "default").strip().lower()

    @property
    def utilities(self) -> list[str]:
        return self.infrastructure_utilities.get("hosts", "").strip().split()

    @property
    def sgw_servers(self) -> list[str]:
        servers = []
        if self.config.has_section('syncgateways'):
            for _, cluster_servers in self.sgw_clusters:
                for server in cluster_servers:
                    servers.append(server)
        return servers

    @property
    def cbl_clients(self) -> list[str]:
        servers = []
        if self.config.has_section('cblites'):
            for _, cluster_servers in self.cbl_clusters:
                for server in cluster_servers:
                    servers.append(server)
        return servers

    @property
    def kafka_servers(self) -> list[str]:
        servers = []
        if self.config.has_section('kafka_clusters'):
            for _, cluster_servers in self.kafka_clusters:
                for server in cluster_servers:
                    servers.append(server)
        return servers

    @property
    def using_private_cluster_ips(self) -> bool:
        return self.config.has_section('cluster_private_ips')

    @property
    def using_private_client_ips(self) -> bool:
        return self.config.has_section('client_private_ips')

    @property
    def using_private_utility_ips(self) -> bool:
        return self.config.has_section('utility_private_ips')

    @property
    def using_private_syncgateway_ips(self) -> bool:
        return self.config.has_section('syncgateway_private_ips')

    @property
    def clusters_private(self) -> Iterator:
        if self.using_private_cluster_ips:
            for i, (cluster_name, ips) in enumerate(self.config.items('cluster_private_ips')):
                if i not in self.inactive_cluster_idxs:
                    yield cluster_name, ips.split()

    @property
    def clients_private(self) -> Iterator:
        if self.using_private_client_ips:
            for cluster_name, private_ips in self.config.items('client_private_ips'):
                yield cluster_name, private_ips.split()

    @property
    def utilities_private(self) -> Iterator:
        if self.using_private_utility_ips:
            for cluster_name, private_ips in self.config.items('utility_private_ips'):
                yield cluster_name, private_ips.split()

    @property
    def syncgateways_private(self) -> Iterator:
        if self.using_private_syncgateway_ips:
            for cluster_name, private_ips in self.config.items('syncgateway_private_ips'):
                yield cluster_name, private_ips.split()

    @property
    def servers_public_to_private_ip(self) -> dict:
        private_clusters = dict(self.clusters_private)
        ip_map = {}
        for cluster, public_ips in self.clusters:
            private_ips = private_clusters.get(cluster, [])
            for i, public_ip in enumerate(public_ips):
                private_ip = private_ips[i] if private_ips else None
                ip_map[public_ip] = private_ip
        return ip_map

    @property
    def using_instance_ids(self):
        return self.config.has_section('cluster_instance_ids')

    @property
    def instance_ids_per_cluster(self):
        iids_per_cluster = {}
        if self.using_instance_ids:
            iids_per_cluster = {k: v.split() for i, (k, v) in
                                enumerate(self.config.items('cluster_instance_ids'))
                                if i not in self.inactive_cluster_idxs}
        return iids_per_cluster

    @property
    def servers_hostname_to_instance_id(self) -> dict:
        iids_per_cluster = self.instance_ids_per_cluster
        host_map = {}
        for cluster, hostnames in self.clusters:
            iids = iids_per_cluster.get(cluster, [])
            for i, hostname in enumerate(hostnames):
                iid = iids[i] if iids else None
                host_map[hostname] = iid
        return host_map

    @property
    def cluster_instance_ids(self) -> list[str]:
        iids = []
        for cluster_iids in self.instance_ids_per_cluster.values():
            iids += cluster_iids
        return iids

    @property
    def sgw_instance_ids_per_cluster(self):
        return {k: v.split() for k, v in self.config.items('sgw_instance_ids')}

    @property
    def sgw_instance_ids(self) -> list[str]:
        iids = []
        for sgw_iids in self.sgw_instance_ids_per_cluster.values():
            iids += sgw_iids
        return iids

    @property
    def server_instance_ids_by_role(self, role: str) -> list[str]:
        has_service = []
        if self.using_instance_ids:
            for cluster_name, servers in self.infrastructure_clusters.items():
                for i, server in enumerate(servers.split()):
                    _, roles = server.split(':')
                    if role in roles:
                        has_service.append(self.instance_ids_per_cluster[cluster_name][i])
        return has_service

    def cluster_servers(self, node: str) -> list[str]:
        """Return all servers from the same cluster as the given node."""
        for _, servers in self.clusters:
            if node in servers:
                return servers
        return []

    def servers_by_role(self, role: str) -> list[str]:
        has_service = []
        for servers in self.infrastructure_clusters.values():
            for server in servers.split():
                host, roles, *group = server.split(':')
                if role in roles:
                    has_service.append(host)
        return has_service

    def servers_by_role_initial_nodes_only(self, role: str, initial_nodes: list[int]) -> list[str]:
        has_service = []
        for servers, nodes in zip(self.infrastructure_clusters.values(), initial_nodes):
            for server in servers.split()[:nodes]:
                host, roles, *group = server.split(":")
                if role in roles:
                    has_service.append(host)
        return has_service

    def servers_by_cluster_and_role(self, role: str) -> list[str]:
        has_service = []
        for servers in self.infrastructure_clusters.values():
            cluster_has_service = []
            for server in servers.split():
                host, roles, *group = server.split(':')
                if role in roles:
                    cluster_has_service.append(host)
            has_service.append(cluster_has_service)
        return has_service

    def servers_by_role_from_first_cluster(self, role: str) -> list[str]:
        has_service = []
        servers = list(self.infrastructure_clusters.values())[0]
        for server in servers.split():
            host, roles, *group = server.split(':')
            if role in roles:
                has_service.append(host)
        return has_service

    def get_cluster_idx_by_node(self, node: str) -> int:
        for i, (_, servers) in enumerate(self.clusters):
            if node in servers:
                return i
        raise ValueError(f"Node {node} not found in any cluster.")

    @property
    def roles(self) -> dict[str, str]:
        server_roles = {}
        for servers in self.infrastructure_clusters.values():
            for server in servers.split():
                host, roles, *group = server.split(':')
                # Use "empty" as a placeholder for empty services
                server_roles[host] = roles if roles != "empty" else ""
        return server_roles

    @property
    def servers_and_roles(self) -> list[Tuple[str, str]]:
        server_and_roles = []
        for servers in self.infrastructure_clusters.values():
            for server in servers.split():
                host, roles, *group = server.split(':')
                server_and_roles.append((host, roles))
        return server_and_roles

    @property
    def workers(self) -> list[str]:
        if self.cloud_infrastructure:
            if self.kubernetes_infrastructure and not self.external_client:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += ["{}.{}".format(k, host) for host in v.split()]
                return clients
            else:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += [host for host in v.split()]
                return clients
        else:
            return self.config.get("clients", "hosts", fallback="").split()

    @property
    def syncgateways(self) -> list[str]:
        if self.cloud_infrastructure:
            if self.kubernetes_infrastructure:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += ["{}.{}".format(k, host) for host in v.split()]
                return clients
            else:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += [host for host in v.split()]
                return clients
        else:
            return self.config.get('syncgateways', 'hosts').split()

    def _kafka_nodes_by_role(self, role: str) -> list[str]:
        has_service = []
        for servers in self.infrastructure_kafka_clusters.values():
            for server in servers.split():
                host, roles = server.split(':')
                if role in roles:
                    has_service.append(host)
        return has_service

    @property
    def kafka_zookeepers(self) -> list[str]:
        return self._kafka_nodes_by_role('zk')

    @property
    def kafka_brokers(self) -> list[str]:
        return self._kafka_nodes_by_role('broker')

    @property
    def kafka_settings(self) -> list[str]:
        return self.infrastructure_section('kafka')

    @property
    def kafka_subnets_per_cluster(self) -> list[str]:
        if self.config.has_section('kafka_subnet_ids'):
            return {k: v.split() for k, v in self.config.items('kafka_subnet_ids')}
        return {}

    @property
    def kafka_broker_subnet_ids(self) -> list[str]:
        subnet_ids = []
        for cluster_name, servers in self.infrastructure_kafka_clusters.items():
            for i, server in enumerate(servers.split()):
                _, roles = server.split(':')
                if 'broker' in roles:
                    subnet_ids.append(self.kafka_subnets_per_cluster[cluster_name][i])
        return subnet_ids

    @property
    def data_path(self) -> Optional[str]:
        return self.config.get("storage", "data", fallback=None)

    @property
    def index_path(self) -> Optional[str]:
        return self.config.get("storage", "index", fallback=self.data_path)

    @property
    def analytics_paths(self) -> list[str]:
        analytics_paths = self.config.get('storage', 'analytics', fallback=None)
        if analytics_paths is not None:
            return analytics_paths.split()
        return []

    @property
    def paths(self) -> Iterator[str]:
        for path in set([self.data_path, self.index_path] + self.analytics_paths):
            if path is not None:
                yield path

    @property
    def backup(self) -> Optional[str]:
        return self.config.get('storage', 'backup', fallback=None)

    @property
    def azure_storage_account(self) -> Optional[str]:
        return self.config.get("storage", "azure_storage_account", fallback=None)

    @property
    def columnar_storage_backend(self) -> Optional[str]:
        return self.config.get("storage", "columnar_storage_backend", fallback=None)

    def _get_secret(self, key: str) -> str:
        from perfrunner.helpers.config_files import SecretsFile

        key_group = self.config.get("metadata", "source", fallback=SecretsFile.DEFAULT_KEY_GROUP)
        with SecretsFile() as s:
            return s.get_metadata(key_group, key)

    @cached_property
    def rest_credentials(self) -> tuple[str, str]:
        return creds_tuple(self._get_secret("rest"))

    @cached_property
    def ssh_credentials(self) -> tuple[str, str]:
        return creds_tuple(self._get_secret("ssh"))

    @cached_property
    def client_credentials(self) -> tuple[str, str]:
        return creds_tuple(self._get_secret("client"))

    @cached_property
    def aws_key_name(self) -> str:
        return self._get_secret("aws_key_name")

    @property
    def capella_admin_credentials(self) -> list[tuple[str, str]]:
        return [
            creds_tuple(creds)
            for i, creds in enumerate(self.config.get("credentials", "admin", fallback="").split())
            if i not in self.inactive_cluster_idxs
        ]

    @property
    def parameters(self) -> dict:
        from perfrunner.helpers.config_files import ClusterMetadataFile

        overrides = self._get_options_as_dict("parameters")
        cluster_name = self.config.get(
            "metadata", "cluster", fallback=ClusterMetadataFile.DEFAULT_KEY_GROUP
        )
        with ClusterMetadataFile(self.csp) as metadata_file:
            return metadata_file.get_parameters(cluster_name, overrides)

    @property
    def capella_cluster_ids(self) -> list[str]:
        return [
            cid
            for i, cid in enumerate(self.controlplane_settings.get("cluster_ids", "").split())
            if i not in self.inactive_cluster_idxs
        ]

    @property
    def server_group_map(self) -> dict:
        server_grp_map = {}
        for servers in self.infrastructure_clusters.values():
            for server in servers.split():
                host, roles, *group = server.split(':')
                if len(group):
                    server_grp_map[host] = group[0]
        return server_grp_map

    def set_capella_admin_credentials(self) -> None:
        if self.capella_infrastructure:
            logger.info('Getting cluster admin credentials.')
            user = 'couchbase-cloud-admin'
            pwds = []

            command_template = (
                'secretsmanager get-secret-value --region us-east-1 '
                '--secret-id {}_dp-admin '
                '--query "SecretString" '
                '--output text'
            )

            for cluster_id in self.capella_cluster_ids:
                pwd = run_aws_cli_command(command_template, cluster_id)
                if pwd:
                    pwds.append(pwd)

            creds = "\n".join(f"{user}:{pwd}" for pwd in pwds)
            if "credentials" not in self.config:
                self.config.add_section("credentials")
            existing_creds = self.config["credentials"].get("admin", "")
            self.config["credentials"]["admin"] = f"{existing_creds}\n{creds}".replace("%", "%%")
            self.update_spec_file()

    def get_aws_iid(self, hostname: str, region: str) -> str:
        command_template = (
            'ec2 describe-instances --region {} '
            '--filter "Name=ip-address,Values=$(dig +short {})" '
            '--query "Reservations[].Instances[].InstanceId" '
            '--output text'
        )
        iid = run_aws_cli_command(command_template, region, hostname)
        return iid

    def set_capella_instance_ids(self) -> None:
        if self.capella_backend == 'aws':
            logger.info('Getting cluster instance IDs')

            if not self.config.has_section('cluster_instance_ids'):
                self.config.add_section('cluster_instance_ids')

            for cluster_name, hosts in self.clusters:
                iids = []
                for host in hosts:
                    iid = self.get_aws_iid(host, self.cloud_region)
                    logger.info("Instance ID for {}: {}".format(host, iid))
                    iids.append(iid)
                self.config.set("cluster_instance_ids", cluster_name, "\n" + "\n".join(iids))
            self.update_spec_file()

    def set_sgw_instance_ids(self) -> None:
        if not self.config.has_section("sgw_instance_ids"):
            self.config.add_section("sgw_instance_ids")

        if self.capella_backend == "aws":
            command_template = (
                "ec2 describe-instances --region {} "
                '--filters "Name=tag-key,Values=couchbase-cloud-syncgateway-id" '
                '"Name=tag:couchbase-app-services,Values={}" '
                '--query "Reservations[].Instances[].InstanceId" '
                "--output text"
            )
            stdout = run_aws_cli_command(
                command_template, self.cloud_region, self.controlplane_settings["cluster_ids"]
            )
            sgids = stdout.split()
            logger.info("Found Instance IDs for sgw: {}".format(", ".join(sgids)))
            self.config.set("sgw_instance_ids", "sync_gateways", "\n", +"\n".join(sgids))
            self.update_spec_file()

    def _cluster_names_to_idxs(self, cluster_names: Iterable[str]) -> list[int]:
        cluster_idxs = []
        for i, cluster_name in enumerate(self.config.options('clusters')):
            if cluster_name in cluster_names:
                cluster_idxs.append(i)

        return cluster_idxs

    def set_active_clusters_by_name(self, cluster_names: Iterable[str]):
        self.set_active_clusters_by_idx(self._cluster_names_to_idxs(cluster_names))

    def set_inactive_clusters_by_name(self, cluster_names: Iterable[str]):
        self.set_inactive_clusters_by_idx(self._cluster_names_to_idxs(cluster_names))

    def set_active_clusters_by_idx(self, cluster_idxs: Iterable[int]):
        if not (new_active_clusters := set(cluster_idxs)):
            logger.warn('No valid active clusters specified. Not changing active clusters')
            return

        self.inactive_cluster_idxs = set(range(len(self.config.options('clusters')))) - \
            new_active_clusters

        logger.info('Active clusters set to {}'
                    .format(list(self.infrastructure_clusters.keys())))

    def set_inactive_clusters_by_idx(self, cluster_idxs: Iterable[int]):
        new_inactive_clusters = set(cluster_idxs)

        if new_inactive_clusters == set(range(len(self.config.options('clusters')))):
            logger.warn('Cannot set all clusters to inactive. Not changing active clusters')
            return

        self.inactive_cluster_idxs = new_inactive_clusters
        logger.info('Active clusters set to {}'
                    .format(list(self.infrastructure_clusters.keys())))

    def set_all_clusters_active(self):
        self.inactive_cluster_idxs = set()
        logger.info('All clusters set to active')


class SystemdLimitsSettings:
    UNIVERSAL_KEY = "*"

    def __init__(self, options: dict):
        universal_limits = options.get(self.UNIVERSAL_KEY, {})

        self.limits = {
            service: {k: v for k, v in universal_limits.items()}
            for service in ["kv", "n1ql", "index", "fts", "eventing", "cbas"]
        }

        for service, limits in self.limits.items():
            limits |= options.get(service, {})

    @property
    def has_any_limits(self) -> bool:
        return any(limits for limits in self.limits.values())


class TestCaseSettings:

    USE_WORKERS = 1
    RESET_WORKERS = 0
    THRESHOLD = -10

    def __init__(self, options: dict):
        self.test_module = '.'.join(options.get('test').split('.')[:-1])
        self.test_class = options.get('test').split('.')[-1]
        self.use_workers = int(options.get('use_workers', self.USE_WORKERS))
        self.reset_workers = int(options.get('reset_workers', self.RESET_WORKERS))


class ShowFastSettings:

    THRESHOLD = -10

    def __init__(self, options: dict):
        self.title = options.get('title')
        self.component = options.get('component', '')
        self.category = options.get('category', '')
        self.sub_category = options.get('sub_category', '')
        self.order_by = options.get('orderby', '')
        self.build_label = options.get('build_label', '')
        self.threshold = int(options.get("threshold", self.THRESHOLD))


class DeploymentSettings:

    MONITOR_DEPLOYMENT_TIME = 'false'
    CAPELLA_POLL_INTERVAL_SECS = 30

    def __init__(self, options: dict):
        self.monitor_deployment_time = maybe_atoi(options.get('monitor_deployment_time',
                                                              self.MONITOR_DEPLOYMENT_TIME))
        self.capella_poll_interval_secs = int(options.get('capella_poll_interval_secs',
                                                          self.CAPELLA_POLL_INTERVAL_SECS))


class ClusterSettings:

    NUM_BUCKETS = 1
    NUM_VBUCKETS = 1024

    MEM_QUOTA = 0
    INDEX_MEM_QUOTA = 256
    FTS_INDEX_MEM_QUOTA = 0
    ANALYTICS_MEM_QUOTA = 0
    EVENTING_MEM_QUOTA = 0
    QUERY_MEMORY_QUOTA = 0

    EVENTING_BUCKET_MEM_QUOTA = 0
    EVENTING_METADATA_BUCKET_MEM_QUOTA = 0
    EVENTING_METADATA_BUCKET_NAME = 'eventing'
    EVENTING_BUCKETS = 0

    CONFLICT_BUCKETS = 0
    CONFLICT_BUCKET_MEM_QUOTA = 0

    KERNEL_MEM_LIMIT = 0
    KV_KERNEL_MEM_LIMIT = 0
    KERNEL_MEM_LIMIT_SERVICES = 'fts', 'index'
    ONLINE_CORES = 0
    SGW_ONLINE_CORES = 0
    ENABLE_CPU_CORES = 'true'
    ENABLE_N2N_ENCRYPTION = None
    ENABLE_QUERY_AWR = None
    QUERY_AWR_BUCKET = 'bucket-1'
    QUERY_AWR_SCOPE = 'scope-awr'
    QUERY_AWR_COLLECTION = 'collection-awr'
    BUCKET_NAME = 'bucket-1'
    DISABLE_UI_HTTP = None
    SHOW_CP_VERSION = None
    CNG_ENABLED = "false"

    IPv6 = 0

    def __init__(self, options: dict):
        self.mem_quota = int(options.get('mem_quota', self.MEM_QUOTA))
        self.index_mem_quota = int(options.get('index_mem_quota',
                                               self.INDEX_MEM_QUOTA))
        self.fts_index_mem_quota = int(options.get('fts_index_mem_quota',
                                                   self.FTS_INDEX_MEM_QUOTA))
        self.analytics_mem_quota = int(options.get('analytics_mem_quota',
                                                   self.ANALYTICS_MEM_QUOTA))
        self.eventing_mem_quota = int(options.get('eventing_mem_quota',
                                                  self.EVENTING_MEM_QUOTA))
        self.query_mem_quota = int(options.get("query_mem_quota", self.QUERY_MEMORY_QUOTA))

        self.initial_nodes = [
            int(nodes) for nodes in options.get('initial_nodes', '1').split()
        ]

        self.num_buckets = int(options.get('num_buckets',
                                           self.NUM_BUCKETS))
        self.eventing_bucket_mem_quota = int(options.get('eventing_bucket_mem_quota',
                                                         self.EVENTING_BUCKET_MEM_QUOTA))
        self.eventing_metadata_bucket_mem_quota = \
            int(options.get('eventing_metadata_bucket_mem_quota',
                            self.EVENTING_METADATA_BUCKET_MEM_QUOTA))
        self.eventing_metadata_bucket_name = options.get(
            "eventing_metadata_bucket_name", self.EVENTING_METADATA_BUCKET_NAME
        )
        # When true, allows reserving the memory required for the eventing metadata bucket
        # without creating it.
        self.skip_metadata_bucket_creation = maybe_atoi(
            options.get("skip_metadata_bucket_creation", "false")
        )
        self.eventing_buckets = int(options.get('eventing_buckets',
                                                self.EVENTING_BUCKETS))
        self.num_vbuckets = int(options.get('num_vbuckets', self.NUM_VBUCKETS))
        self.conflict_buckets = int(options.get('conflict_buckets',
                                                self.CONFLICT_BUCKETS))
        self.conflict_bucket_mem_quota = int(options.get('conflict_bucket_mem_quota',
                                                         self.CONFLICT_BUCKET_MEM_QUOTA))
        self.online_cores = int(options.get('online_cores',
                                            self.ONLINE_CORES))
        self.sgw_online_cores = int(options.get('sgw_online_cores',
                                                self.SGW_ONLINE_CORES))
        self.enable_cpu_cores = maybe_atoi(options.get('enable_cpu_cores', self.ENABLE_CPU_CORES))
        self.ipv6 = int(options.get('ipv6', self.IPv6))
        self.kernel_mem_limit = options.get('kernel_mem_limit',
                                            self.KERNEL_MEM_LIMIT)
        self.kv_kernel_mem_limit = options.get('kv_kernel_mem_limit',
                                               self.KV_KERNEL_MEM_LIMIT)
        self.enable_n2n_encryption = options.get('enable_n2n_encryption',
                                                 self.ENABLE_N2N_ENCRYPTION)
        self.enable_query_awr = options.get('enable_query_awr',
                                            self.ENABLE_QUERY_AWR)
        self.query_awr_bucket = options.get('query_awr_bucket',
                                            self.QUERY_AWR_BUCKET)
        self.query_awr_scope = options.get('query_awr_scope',
                                           self.QUERY_AWR_SCOPE)
        self.query_awr_collection = options.get('query_awr_collection',
                                                self.QUERY_AWR_COLLECTION)
        self.ui_http = options.get('ui_http', self.DISABLE_UI_HTTP)
        self.show_cp_version = options.get("show_cp_version", self.SHOW_CP_VERSION)

        kernel_mem_limit_services = options.get('kernel_mem_limit_services')
        if kernel_mem_limit_services:
            self.kernel_mem_limit_services = kernel_mem_limit_services.split()
        else:
            self.kernel_mem_limit_services = self.KERNEL_MEM_LIMIT_SERVICES

        self.bucket_name = options.get('bucket_name', self.BUCKET_NAME)

        self.cloud_server_groups = options.get('bucket_name', self.BUCKET_NAME)
        self.enable_indexer_systemd_mem_limits = options.get(
            "enable_indexer_systemd_mem_limits", False
        )
        # When enabled, start Couchbase server in cgroup managemed mode
        self.cgroup_managed = maybe_atoi(options.get("cgroup_managed", "false"))
        self.profile = options.get("profile")  # ns-server profile to use

        self.cng_enabled = maybe_atoi(options.get("cng_enabled", self.CNG_ENABLED))

        if ',' in self.bucket_name:
            if len(self.bucket_name.split(",")) != self.num_buckets:
                logger.error("Mismatch between number of bucket names and number of buckets.")
            else:
                self.bucket_name = self.bucket_name.split(",")

        # Debugging
        self.install_debug_sym = maybe_atoi(options.get("install_debug_sym", "false"))


class StatsSettings:

    ENABLED = 1
    POST_TO_SF = 0

    INTERVAL = 5
    LAT_INTERVAL = 1

    POST_CPU = 0

    CLIENT_PROCESSES = []
    SERVER_PROCESSES = ['beam.smp',
                        'ns_server',  # For metrics REST API collector
                        'cbft',
                        'cbq-engine',
                        'indexer',
                        'memcached',
                        'sync_gateway']
    TRACED_PROCESSES = []

    SECONDARY_STATSFILE = '/root/statsfile'

    REPORT_FOR_ALL_CLUSTERS = 0

    def __init__(self, options: dict):
        self.enabled = int(options.get('enabled', self.ENABLED))
        self.post_to_sf = int(options.get('post_to_sf', self.POST_TO_SF))
        self.interval = int(options.get('interval', self.INTERVAL))
        self.lat_interval = float(options.get('lat_interval',
                                              self.LAT_INTERVAL))
        self.post_cpu = int(options.get('post_cpu', self.POST_CPU))
        self.client_processes = self.CLIENT_PROCESSES + \
            options.get('client_processes', '').split()
        self.server_processes = self.SERVER_PROCESSES + \
            options.get('server_processes', '').split()
        self.traced_processes = self.TRACED_PROCESSES + \
            options.get('traced_processes', '').split()
        self.secondary_statsfile = options.get('secondary_statsfile',
                                               self.SECONDARY_STATSFILE)
        self.use_prometheus_metrics = maybe_atoi(options.get("use_prometheus_metrics", "false"))
        # During transition period, allow supplyig this as override
        self.metrics_system_host = options.get("metrics_system_host")

        # Not used by all test classes, but can be used to decide whether to report KPIs for all
        # clusters or just the first (the default)
        self.report_for_all_clusters = maybe_atoi(options.get('report_for_all_clusters',
                                                              str(self.REPORT_FOR_ALL_CLUSTERS)))


class ProfilingSettings:

    INTERVAL = 300  # 5 minutes

    NUM_PROFILES = 1

    PROFILES = 'cpu'

    SERVICES = ''

    CPU_INTERVAL = 10  # 10 seconds

    LINUX_PERF_PROFILE_DURATION = 10  # seconds

    LINUX_PERF_FREQUENCY = 99

    LINUX_PERF_CALLGRAPH = 'lbr'     # optional lbr, dwarf

    LINUX_PERF_DELAY_MULTIPLIER = 2

    COLLECT_CBBACKUPMGR_PROFILES = "false"

    def __init__(self, options: dict):
        self.services = options.get('services',
                                    self.SERVICES).split()
        self.interval = int(options.get('interval',
                                        self.INTERVAL))
        self.num_profiles = int(options.get('num_profiles',
                                            self.NUM_PROFILES))
        self.profiles = options.get('profiles',
                                    self.PROFILES).split(',')
        self.cpu_interval = int(options.get('cpu_interval', self.CPU_INTERVAL))
        self.linux_perf_profile_duration = int(options.get('linux_perf_profile_duration',
                                                           self.LINUX_PERF_PROFILE_DURATION))

        self.linux_perf_profile_flag = bool(options.get('linux_perf_profile_flag'))

        self.linux_perf_frequency = int(options.get('linux_perf_frequency',
                                                    self.LINUX_PERF_FREQUENCY))

        self.linux_perf_callgraph = options.get('linux_perf_callgraph',
                                                self.LINUX_PERF_CALLGRAPH)

        self.linux_perf_delay_multiplier = int(options.get('linux_perf_delay_multiplier',
                                                           self.LINUX_PERF_DELAY_MULTIPLIER))

        self.collect_cbbackupmgr_profiles = maybe_atoi(
            options.get("collect_cbbackupmgr_profiles", self.COLLECT_CBBACKUPMGR_PROFILES)
        )

    def get_cbbackupmgr_profiling_env(
        self, operation: str, base_dir: Optional[str] = None
    ) -> dict[str, str]:
        if not self.collect_cbbackupmgr_profiles:
            return {}

        env = {}
        base_dir = base_dir if base_dir is not None else "."

        # In case cbbackupmgr is run multiple times, to avoid overwriting profiles
        tag = uuid4().hex[:4]

        if "cpu" in self.profiles:
            env["CB_GOLANG_PPROF"] = f"{base_dir}/{operation}_cpu_{tag}.pprof"
        if "mem" in self.profiles:
            env["CB_GOLANG_MEM_PROF"] = f"{base_dir}/{operation}_mem_{tag}.pprof"

        return env


class BucketSettings:

    PASSWORD = 'password'
    REPLICA_NUMBER = 1
    REPLICA_INDEX = 0
    EVICTION_POLICY = 'valueOnly'  # alt: fullEviction
    BUCKET_TYPE = 'membase'  # alt: ephemeral
    AUTOFAILOVER_ENABLED = 'true'
    DEFAULT_FAILOVER_MIN_TIMEOUTS = [5, 30]  # sec
    DEFAULT_DATA_DISK_FAILURE_TIMEOUT = 10  # sec
    BACKEND_STORAGE = 'couchstore'
    CONFLICT_RESOLUTION_TYPE = 'seqno'
    FLUSH = True
    MIN_DURABILITY = 'none'
    DOC_TTL_UNIT = None
    DOC_TTL_VALUE = 0
    MAGMA_SEQ_TREE_DATA_BLOCK_SIZE = 0
    HISTORY_SECONDS = 0
    HISTORY_BYTES = 0
    MAX_TTL = 0
    ENCRYPTION_AT_REST = "false"
    DEK_INTERVAL_SECS = 30 * 24 * 60 * 60
    DEK_LIFETIME_SECS = 365 * 24 * 60 * 60
    MAX_DEKS = 50

    def __init__(self, options: dict):
        self.password = options.get('password', self.PASSWORD)

        self.replica_number = int(options.get('replica_number', self.REPLICA_NUMBER))

        self.replica_index = int(options.get('replica_index', self.REPLICA_INDEX))

        self.eviction_policy = options.get('eviction_policy', self.EVICTION_POLICY)

        self.bucket_type = options.get('bucket_type', self.BUCKET_TYPE)

        self.conflict_resolution_type = options.get('conflict_resolution_type',
                                                    self.CONFLICT_RESOLUTION_TYPE)

        self.compression_mode = options.get('compression_mode')

        self.autofailover_enabled = options.get('autofailover_enabled',
                                                self.AUTOFAILOVER_ENABLED).lower()

        failover_min = options.get('failover_min')
        if failover_min:
            self.failover_timeouts = [int(failover_min)]
            self.failover_timeouts.extend(self.DEFAULT_FAILOVER_MIN_TIMEOUTS)
        else:
            self.failover_timeouts = self.DEFAULT_FAILOVER_MIN_TIMEOUTS

        # failoverOnDataDiskIssues[timePeriod]
        self.disk_failover_timeout = int(options.get('disk_failover_timeout',
                                                     self.DEFAULT_DATA_DISK_FAILURE_TIMEOUT))

        self.backend_storage = options.get('backend_storage', self.BACKEND_STORAGE)

        self.flush = bool(options.get('flush', self.FLUSH))

        self.min_durability = options.get('min_durability', self.MIN_DURABILITY)

        self.doc_ttl_unit = options.get('doc_ttl_unit', self.DOC_TTL_UNIT)

        self.doc_ttl_value = options.get('doc_ttl_value', self.DOC_TTL_VALUE)

        self.magma_seq_tree_data_block_size = int(options.get('magma_seq_tree_data_block_size',
                                                              self.MAGMA_SEQ_TREE_DATA_BLOCK_SIZE))

        self.history_seconds = int(options.get('history_seconds', self.HISTORY_SECONDS))

        self.history_bytes = int(options.get('history_bytes', self.HISTORY_BYTES))

        self.max_ttl = int(options.get('max_ttl', self.MAX_TTL))

        self.encryption_at_rest = maybe_atoi(
            options.get("encryption_at_rest", self.ENCRYPTION_AT_REST)
        )
        self.dek_interval_secs = int(
            options.get("dek_rotation_interval_secs", self.DEK_INTERVAL_SECS)
        )
        self.dek_lifetime_secs = int(options.get("dek_lifetime_secs", self.DEK_LIFETIME_SECS))
        self.max_deks = int(options.get("max_deks", self.MAX_DEKS))


class CollectionSettings:

    CONFIG = None
    COLLECTION_MAP = None
    USE_BULK_API = 1
    SCOPES_PER_BUCKET = 0
    COLLECTIONS_PER_SCOPE = 0
    COLLECTION_STAT_GROUPS = {}

    def __init__(self, options: dict, buckets: Iterable[str] = None):
        self.config = options.get('config', self.CONFIG)
        self.collection_map = self.COLLECTION_MAP
        self.use_bulk_api = int(options.get('use_bulk_api', self.USE_BULK_API))
        self.scopes_per_bucket = int(options.get('scopes_per_bucket', self.SCOPES_PER_BUCKET))
        self.collections_per_scope = int(options.get('collections_per_scope',
                                                     self.COLLECTIONS_PER_SCOPE))
        if self.config is not None:
            with open(self.config) as f:
                self.collection_map = json.load(f)
        elif self.scopes_per_bucket > 0 and buckets:
            self.collection_map = self.create_uniform_collection_map(buckets)

        self.collection_stat_groups = self.COLLECTION_STAT_GROUPS
        if self.collection_map:
            self.collection_stat_groups = set([
                options.get('stat_group')
                for scopes in self.collection_map.values()
                for collections in scopes.values()
                for options in collections.values()
            ]) - {None}

    def create_uniform_collection_map(self, buckets: Iterable[str]):
        coll_map = {
            bucket: {
                'scope-{}'.format(i + 1): {
                    'collection-{}'.format(j + 1): {
                        'access': 1,
                        'load': 1
                    }
                    for j in range(self.collections_per_scope)
                }
                for i in range(self.scopes_per_bucket)
            }
            for bucket in buckets
        }

        for bucket_scopes in coll_map.values():
            bucket_scopes['_default'] = {
                '_default': {
                    'access': 0,
                    'load': 0
                }
            }

        return coll_map

    def get_definition(self) -> Optional[dict]:
        """
        Return collections definition summary intended for printing usecases only.

        If a config file is provided, the returned dictionary contains the config file path.
        Otherwise, it will contain the defined per bucket and per scope values.
        """
        if self.config:
            return {"config": self.config}
        elif not self.collection_map:
            return None

        return {
            "scopes_per_bucket": self.scopes_per_bucket,
            "collections_per_scope": self.collections_per_scope,
        }


class UserSettings:

    NUM_USERS_PER_BUCKET = 0

    def __init__(self, options: dict):
        self.num_users_per_bucket = int(options.get('num_users_per_bucket',
                                                    self.NUM_USERS_PER_BUCKET))


class CompactionSettings:

    DB_PERCENTAGE = 30
    VIEW_PERCENTAGE = 30
    PARALLEL = True
    BUCKET_COMPACTION = 'true'
    MAGMA_FRAGMENTATION_PERCENTAGE = 50

    def __init__(self, options: dict):
        self.db_percentage = options.get('db_percentage',
                                         self.DB_PERCENTAGE)
        self.view_percentage = options.get('view_percentage',
                                           self.VIEW_PERCENTAGE)
        self.parallel = options.get('parallel', self.PARALLEL)
        self.bucket_compaction = options.get('bucket_compaction', self.BUCKET_COMPACTION)
        self.magma_fragmentation_percentage = options.get('magma_fragmentation_percentage',
                                                          self.MAGMA_FRAGMENTATION_PERCENTAGE)

    def __str__(self):
        return str(self.__dict__)


class RebalanceSettings:

    SWAP = 0
    FAILOVER = 'hard'  # Atl: graceful
    DELTA_RECOVERY = 0  # Full recovery by default
    DELAY_BEFORE_FAILOVER = 600
    START_AFTER = 1200
    STOP_AFTER = 180
    FTS_PARTITIONS = "1"
    FTS_MAX_DCP_PARTITIONS = "0"

    def __init__(self, options: dict):
        nodes_after = options.get('nodes_after', '').split()
        self.nodes_after = [int(num_nodes) for num_nodes in nodes_after]
        self.swap = int(options.get('swap', self.SWAP))

        self.failed_nodes = int(options.get('failed_nodes', 1))
        self.failover = options.get('failover', self.FAILOVER)
        self.delay_before_failover = int(options.get('delay_before_failover',
                                                     self.DELAY_BEFORE_FAILOVER))
        self.delta_recovery = int(options.get('delta_recovery',
                                              self.DELTA_RECOVERY))

        self.start_after = int(options.get('start_after', self.START_AFTER))
        self.stop_after = int(options.get('stop_after', self.STOP_AFTER))
        self.services = options.get('services', 'kv')
        self.rebalance_config = options.get('rebalance_config', None)
        self.rebalance_timeout = int(options.get('rebalance_timeout', 0))
        self.reb_out_one_by_one = maybe_atoi(options.get("reb_out_one_by_one", "false"))
        self.latency_reporting_windows = [
            # options: pre, during, post
            s.lower()
            for s in options.get("latency_reporting_windows", "").replace(",", " ").split()
        ]

        # The reblance settings for FTS
        self.ftspartitions = options.get('ftspartitions', self.FTS_PARTITIONS)
        self.fts_max_dcp_partitions = options.get('fts_max_dcp_partitions',
                                                  self.FTS_MAX_DCP_PARTITIONS)
        self.fts_node_level_parameters = {}
        if self.ftspartitions != self.FTS_PARTITIONS:
            self.fts_node_level_parameters["maxConcurrentPartitionMovesPerNode"] = \
                self.ftspartitions
        if self.fts_max_dcp_partitions != self.FTS_MAX_DCP_PARTITIONS:
            self.fts_node_level_parameters["maxFeedsPerDCPAgent"] = self.fts_max_dcp_partitions

        # Dynamic service rebalance settings
        # Number of nodes to add or remove the service from.
        # If positive, the service will be added to the number of nodes specified.
        # If negative, the service will be removed from the number of nodes specified.
        self.replace_nodes = int(options.get("replace_nodes", 1))
        # When adding a service to a new node, select one with this service
        # but not the service being moved.
        self.colocate_with = options.get("colocate_with", "empty")


class UpgradeSettings(RebalanceSettings):
    DEFAULT_PROCESS = "SwapRebalance"  # Alt: DeltaRecovery
    DEFAULT_STRATEGY = "RollingUpgrade"  # Alt: ImmediateUpgrade
    # Alt: k8s - Specifies to either upgrade Couchbase cluster or k8s cluster
    DEFAULT_UPGRADE_TYPE = "couchbase"
    # For k8s upgrades, either do a simulated upgrade or a full upgrade
    DEFAULT_UPGRADE_MODE = "simulated"

    def __init__(self, options: dict):
        super().__init__(options)
        self.upgrade_process = options.get("process", self.DEFAULT_PROCESS)
        self.upgrade_strategy = options.get("strategy", self.DEFAULT_STRATEGY)
        self.upgrade_type = options.get("type", self.DEFAULT_UPGRADE_TYPE)
        self.target_version = options.get("target_version")
        self.k8s_upgrade_mode = options.get("mode", self.DEFAULT_UPGRADE_MODE)

    def is_server_upgrade(self) -> bool:
        return self.upgrade_type == self.DEFAULT_UPGRADE_TYPE

    def is_simulated_upgrade(self) -> bool:
        return self.k8s_upgrade_mode == self.DEFAULT_UPGRADE_MODE


class PhaseSettings:

    TIME = 3600 * 24
    USE_BACKUP = 'false'
    RESET_THROTTLE_LIMIT = 'true'

    DOC_GEN = 'basic'
    POWER_ALPHA = 0
    ZIPF_ALPHA = 0
    KEY_PREFIX = None

    CREATES = 0
    READS = 0
    UPDATES = 0
    DELETES = 0
    READS_AND_UPDATES = 0
    FTS_UPDATES = 0
    TTL = 0

    OPS = 0
    TARGET = 0

    HOT_READS = False
    SEQ_UPSERTS = False

    TIMESERIES_REGULAR = 'false'
    TIMESERIES_ENABLE = 'true'
    TIMESERIES_START = 0
    TIMESERIES_HOURS_PER_DOC = 1
    TIMESERIES_DOCS_PER_DEVICE = 1
    TIMESERIES_TOTAL_DAYS = 1

    GAP = 3000
    DISTRIBUTION = "step"
    DEBUG_MODE = 1

    BATCH_SIZE = 1000
    BATCHES = 1
    SPRING_BATCH_SIZE = 100

    ITERATIONS = 1

    ASYNC = False

    KEY_FMTR = 'decimal'

    ITEMS = 0
    SIZE = 2048
    ADDITIONAL_ITEMS = 0

    PHASE = 0
    INSERT_TEST_FLAG = 0

    MEM_LOW_WAT = 0
    MEM_HIGH_WAT = 0

    WORKING_SET = 100
    WORKING_SET_ACCESS = 100
    WORKING_SET_MOVE_TIME = 0
    WORKING_SET_MOVE_DOCS = 0

    THROUGHPUT = float('inf')
    QUERY_THROUGHPUT = float('inf')
    N1QL_THROUGHPUT = float('inf')

    VIEW_QUERY_PARAMS = '{}'

    WORKERS = 0
    QUERY_WORKERS = 0
    N1QL_WORKERS = 0
    FTS_DATA_SPREAD_WORKERS = None
    FTS_DATA_SPREAD_WORKER_TYPE = "default"
    WORKLOAD_INSTANCES = 1

    N1QL_OP = 'read'
    N1QL_BATCH_SIZE = 100
    N1QL_TIMEOUT = 0
    N1QL_QUERY_WEIGHT = ""

    ARRAY_SIZE = 10
    NUM_CATEGORIES = 10 ** 6
    NUM_REPLIES = 100
    RANGE_DISTANCE = 10

    ITEM_SIZE = 64
    SIZE_VARIATION_MIN = 1
    SIZE_VARIATION_MAX = 1024

    RECORDED_LOAD_CACHE_SIZE = 0
    INSERTS_PER_WORKERINSTANCE = 0

    RUN_EXTRA_ACCESS = 'false'

    EPOLL = 'true'
    BOOST = 48

    YCSB_FIELD_COUNT = 10
    YCSB_FIELD_LENGTH = 100
    YCSB_INSERTSTART = 0

    SSL_MODE = 'none'
    SSL_AUTH_KEYSTORE = "certificates/auth.keystore"
    SSL_DATA_KEYSTORE = "certificates/data.keystore"
    SSL_KEYSTOREPASS = "storepass"
    CERTIFICATE_FILE = "root.pem"
    SHOW_TLS_VERSION = False
    CIPHER_LIST = None
    MIN_TLS_VERSION = None

    PERSIST_TO = 0
    REPLICATE_TO = 0

    TIMESERIES = 0

    CBCOLLECT = 0
    CBCOLLECT_TIMEOUT = 1200

    CONNSTR_PARAMS = "{'ipv6': 'allow', 'enable_tracing': 'false'}"

    YCSB_CLIENT = 'couchbase2'
    YCSB_DEFAULT_WORKLOAD_PATH = 'workloads/workloada'

    DURABILITY = None

    YCSB_KV_ENDPOINTS = 1
    YCSB_ENABLE_MUTATION_TOKEN = None

    YCSB_RETRY_STRATEGY = 'default'
    YCSB_RETRY_LOWER = 1
    YCSB_RETRY_UPPER = 500
    YCSB_RETRY_FACTOR = 2
    YCSB_OUT_OF_ORDER = 0
    YCSB_SPLIT_WORKLOAD = 0

    RANGE_SCAN_SAMPLING = "false"
    PREFIX_SCAN = "false"
    ORDERED = "true"

    TRANSACTIONSENABLED = 0

    NUM_ATRS = 1024

    YCSB_JVM_ARGS = None

    TPCDS_SCALE_FACTOR = 1

    DOCUMENTSINTRANSACTION = 4
    TRANSACTIONREADPROPORTION = 0.25
    TRANSACTIONUPDATEPROPORTION = 0.75
    TRANSACTIONINSERTPROPORTION = 0

    REQUESTDISTRIBUTION = 'zipfian'

    ANALYTICS_WARMUP_OPS = 0
    ANALYTICS_WARMUP_WORKERS = 0

    SQL_SUITE = 'scans1.1'
    SCAN_SUITE = '1'

    COLLECTION_MAP = None
    CUSTOM_PILLOWFIGHT = False

    USERS = None
    USER_MOD_THROUGHPUT = float('inf')
    USER_MOD_WORKERS = 0
    COLLECTION_MOD_WORKERS = 0
    COLLECTION_MOD_THROUGHPUT = float('inf')

    JAVA_DCP_STREAM = 'all'
    JAVA_DCP_CONFIG = None
    JAVA_DCP_CLIENTS = 0
    SPLIT_WORKLOAD = None
    SPLIT_WORKLOAD_THROUGHPUT = 0
    SPLIT_WORKLOAD_WORKERS = 0

    DOCUMENT_GROUPS = 1
    N1QL_SHUTDOWN_TYPE = None

    LATENCY_PERCENTILES = [99.9]
    THROUGHPUT_PERCENTILES = [90]

    WORKLOAD_MIX = []
    NUM_BUCKETS = []

    PER_COLLECTION_LATENCY = False

    CAPELLA_ENDPOINT = ''

    CONFLICT_RATIO = 0

    def __init__(self, options: dict):
        # Common settings
        self.time = int(options.get('time', self.TIME))
        self.use_backup = maybe_atoi(options.get('use_backup', self.USE_BACKUP))
        self.reset_throttle_limit = maybe_atoi(options.get('reset_throttle_limit',
                                                           self.RESET_THROTTLE_LIMIT))

        # KV settings
        self.doc_gen = options.get('doc_gen', self.DOC_GEN)
        self.power_alpha = float(options.get('power_alpha', self.POWER_ALPHA))
        self.zipf_alpha = float(options.get('zipf_alpha', self.ZIPF_ALPHA))
        self.key_prefix = options.get('key_prefix', self.KEY_PREFIX)

        self.size = int(options.get('size', self.SIZE))
        self.items = int(options.get('items', self.ITEMS))
        self.additional_items = int(options.get('additional_items', self.ADDITIONAL_ITEMS))

        self.phase = int(options.get('phase', self.PHASE))
        self.insert_test_flag = int(options.get('insert_test_flag', self.INSERT_TEST_FLAG))

        self.mem_low_wat = int(options.get('mem_low_wat', self.MEM_LOW_WAT))
        self.mem_high_wat = int(options.get('mem_high_wat', self.MEM_HIGH_WAT))

        self.creates = int(options.get('creates', self.CREATES))
        self.reads = int(options.get('reads', self.READS))
        self.updates = int(options.get('updates', self.UPDATES))
        self.deletes = int(options.get('deletes', self.DELETES))
        self.modify_doc_loader = options.get('modify_doc_loader', None)
        self.ttl = int(options.get('ttl', self.TTL))
        self.reads_and_updates = int(options.get('reads_and_updates',
                                                 self.READS_AND_UPDATES))
        self.fts_updates_swap = int(options.get('fts_updates_swap',
                                                self.FTS_UPDATES))
        self.fts_updates_reverse = int(options.get('fts_updates_reverse',
                                                   self.FTS_UPDATES))

        self.ops = float(options.get('ops', self.OPS))
        self.throughput = float(options.get('throughput', self.THROUGHPUT))

        self.working_set = float(options.get('working_set', self.WORKING_SET))
        self.working_set_access = int(options.get('working_set_access',
                                                  self.WORKING_SET_ACCESS))
        self.working_set_move_time = int(options.get('working_set_move_time',
                                                     self.WORKING_SET_MOVE_TIME))
        self.working_set_moving_docs = int(options.get('working_set_moving_docs',
                                                       self.WORKING_SET_MOVE_DOCS))
        self.workers = int(options.get('workers', self.WORKERS))
        self.run_async = bool(int(options.get('async', self.ASYNC)))
        self.key_fmtr = options.get('key_fmtr', self.KEY_FMTR)

        self.hot_reads = self.HOT_READS
        self.seq_upserts = self.SEQ_UPSERTS

        self.timeseries_regular = maybe_atoi(options.get('timeseries_regular',
                                                         self.TIMESERIES_REGULAR))
        self.timeseries_enable = maybe_atoi(options.get('timeseries_enable',
                                                        self.TIMESERIES_ENABLE))
        self.timeseries_start = int(options.get('timeseries_start', self.TIMESERIES_START))
        self.timeseries_hours_per_doc = int(options.get('timeseries_hours_per_doc',
                                                        self.TIMESERIES_HOURS_PER_DOC))
        self.timeseries_docs_per_device = int(options.get('timeseries_docs_per_device',
                                                          self.TIMESERIES_DOCS_PER_DEVICE))
        self.timeseries_total_days = int(options.get('timeseries_total_days',
                                                     self.TIMESERIES_TOTAL_DAYS))

        self.iterations = int(options.get('iterations', self.ITERATIONS))

        self.gap = int(options.get('gap', self.GAP))
        self.distribution = options.get('distribution', self.DISTRIBUTION)
        self.debug_mode = int(options.get('debug_mode', self.DEBUG_MODE))

        self.batch_size = int(options.get('batch_size', self.BATCH_SIZE))
        self.batches = int(options.get('batches', self.BATCHES))
        self.spring_batch_size = int(options.get('spring_batch_size', self.SPRING_BATCH_SIZE))

        self.workload_instances = int(options.get('workload_instances',
                                                  self.WORKLOAD_INSTANCES))

        self.connstr_params = eval(options.get('connstr_params', self.CONNSTR_PARAMS))

        self.run_extra_access = maybe_atoi(options.get('run_extra_access', self.RUN_EXTRA_ACCESS))

        self.latency_percentiles = options.get('latency_percentiles', self.LATENCY_PERCENTILES)
        if isinstance(self.latency_percentiles, str):
            self.latency_percentiles = [float(x) for x in self.latency_percentiles.split(',')]

        self.throughput_percentiles = options.get('throughput_percentiles',
                                                  self.THROUGHPUT_PERCENTILES)
        if isinstance(self.throughput_percentiles, str):
            self.throughput_percentiles = [float(x) for x in self.throughput_percentiles.split(',')]

        # Views settings
        self.ddocs = None
        self.index_type = None
        self.query_params = eval(options.get('query_params',
                                             self.VIEW_QUERY_PARAMS))
        self.query_workers = int(options.get('query_workers',
                                             self.QUERY_WORKERS))
        self.query_throughput = float(options.get('query_throughput',
                                                  self.QUERY_THROUGHPUT))

        # N1QL settings
        self.n1ql_gen = options.get('n1ql_gen')

        self.n1ql_workers = int(options.get('n1ql_workers', self.N1QL_WORKERS))
        self.n1ql_op = options.get('n1ql_op', self.N1QL_OP)
        self.n1ql_throughput = float(options.get('n1ql_throughput',
                                                 self.N1QL_THROUGHPUT))
        self.n1ql_batch_size = int(options.get('n1ql_batch_size',
                                               self.N1QL_BATCH_SIZE))
        self.array_size = int(options.get('array_size', self.ARRAY_SIZE))
        self.num_categories = int(options.get('num_categories',
                                              self.NUM_CATEGORIES))
        self.num_replies = int(options.get('num_replies', self.NUM_REPLIES))
        self.range_distance = int(options.get('range_distance',
                                              self.RANGE_DISTANCE))
        self.n1ql_timeout = int(options.get('n1ql_timeout', self.N1QL_TIMEOUT))
        self.n1ql_query_type = options.get('n1ql_query_type', None)
        if 'n1ql_queries' in options:
            self.n1ql_queries = options.get('n1ql_queries').strip().split(',')
            n1ql_query_weight = options.get('n1ql_query_weight', self.N1QL_QUERY_WEIGHT).strip()
            if len(n1ql_query_weight):
                self.n1ql_query_weight = [int(w) for w in n1ql_query_weight.split(',')]
            else:
                self.n1ql_query_weight = [1] * len(self.n1ql_queries)

        # 2i settings
        self.item_size = int(options.get('item_size', self.ITEM_SIZE))
        self.size_variation_min = int(options.get('size_variation_min',
                                                  self.SIZE_VARIATION_MIN))
        self.size_variation_max = int(options.get('size_variation_max',
                                                  self.SIZE_VARIATION_MAX))

        # Syncgateway settings
        self.syncgateway_settings = None

        # YCSB settings
        self.workload_path = options.get('workload_path', self.YCSB_DEFAULT_WORKLOAD_PATH)
        self.recorded_load_cache_size = int(options.get('recorded_load_cache_size',
                                                        self.RECORDED_LOAD_CACHE_SIZE))
        self.inserts_per_workerinstance = int(options.get('inserts_per_workerinstance',
                                                          self.INSERTS_PER_WORKERINSTANCE))
        self.epoll = options.get("epoll", self.EPOLL)
        self.boost = options.get('boost', self.BOOST)
        self.target = float(options.get('target', self.TARGET))
        self.field_count = int(options.get('field_count', self.YCSB_FIELD_COUNT))
        self.field_length = int(options.get('field_length', self.YCSB_FIELD_LENGTH))
        self.kv_endpoints = int(options.get('kv_endpoints', self.YCSB_KV_ENDPOINTS))
        self.enable_mutation_token = options.get('enable_mutation_token',
                                                 self.YCSB_ENABLE_MUTATION_TOKEN)
        self.ycsb_client = options.get('ycsb_client', self.YCSB_CLIENT)
        self.ycsb_out_of_order = int(options.get('out_of_order', self.YCSB_OUT_OF_ORDER))
        self.insertstart = int(options.get('insertstart', self.YCSB_INSERTSTART))
        self.ycsb_split_workload = int(options.get('ycsb_split_workload', self.YCSB_SPLIT_WORKLOAD))

        self.range_scan_sampling = options.get('range_scan_sampling', self.RANGE_SCAN_SAMPLING)
        self.prefix_scan = options.get('prefix_scan', self.PREFIX_SCAN)
        self.ordered = options.get('ordered', self.ORDERED)

        # trasnsaction settings
        self.transactionsenabled = int(options.get('transactionsenabled',
                                                   self.TRANSACTIONSENABLED))
        self.documentsintransaction = int(options.get('documentsintransaction',
                                                      self.DOCUMENTSINTRANSACTION))
        self.transactionreadproportion = options.get('transactionreadproportion',
                                                     self.TRANSACTIONREADPROPORTION)
        self.transactionupdateproportion = options.get('transactionupdateproportion',
                                                       self.TRANSACTIONUPDATEPROPORTION)
        self.transactioninsertproportion = options.get('transactioninsertproportion',
                                                       self.TRANSACTIONINSERTPROPORTION)
        self.requestdistribution = options.get('requestdistribution',
                                               self.REQUESTDISTRIBUTION)

        # multiple of 1024
        self.num_atrs = int(options.get('num_atrs', self.NUM_ATRS))

        # Subdoc & XATTR
        self.subdoc_field = options.get('subdoc_field')
        self.xattr_field = options.get('xattr_field')

        # SSL settings
        self.ssl_mode = (options.get('ssl_mode', self.SSL_MODE))
        self.ssl_keystore_password = self.SSL_KEYSTOREPASS
        if self.ssl_mode == 'auth':
            self.ssl_keystore_file = self.SSL_AUTH_KEYSTORE
        else:
            self.ssl_keystore_file = self.SSL_DATA_KEYSTORE
        self.certificate_file = self.CERTIFICATE_FILE
        self.show_tls_version = options.get('show_tls_version', self.SHOW_TLS_VERSION)
        self.cipher_list = options.get('cipher_list', self.CIPHER_LIST)
        if self.cipher_list:
            self.cipher_list = self.cipher_list.split(',')

        self.min_tls_version = options.get('min_tls_version',
                                           self.MIN_TLS_VERSION)

        # Durability settings

        self.durability_set = False
        if options.get('persist_to', None) or \
                options.get('replicate_to', None) or \
                options.get('durability', None):
            self.durability_set = True

        self.replicate_to = int(options.get('replicate_to', self.REPLICATE_TO))
        self.persist_to = int(options.get('persist_to', self.PERSIST_TO))
        if options.get('durability', self.DURABILITY) is not None:
            self.durability = int(options.get('durability'))
        else:
            self.durability = self.DURABILITY

        # YCSB Retry Strategy settings
        self.retry_strategy = options.get('retry_strategy', self.YCSB_RETRY_STRATEGY)
        self.retry_lower = int(options.get('retry_lower', self.YCSB_RETRY_LOWER))
        self.retry_upper = int(options.get('retry_upper', self.YCSB_RETRY_UPPER))
        self.retry_factor = int(options.get('retry_factor', self.YCSB_RETRY_FACTOR))

        # CbCollect Setting
        self.cbcollect = int(options.get('cbcollect',
                                         self.CBCOLLECT))
        self.cbcollect_timeout = int(options.get('cbcollect_timeout', self.CBCOLLECT_TIMEOUT))
        self.cbcollect_regexp = options.get("cbcollect_task_regexp")
        # Latency Setting
        self.timeseries = int(options.get('timeseries',
                                          self.TIMESERIES))

        self.ycsb_jvm_args = options.get('ycsb_jvm_args', self.YCSB_JVM_ARGS)
        self.tpcds_scale_factor = int(options.get('tpcds_scale_factor', self.TPCDS_SCALE_FACTOR))

        # Analytics settings
        self.sql_suite = options.get('sql_suite', self.SQL_SUITE)
        self.scan_suite = options.get('scan_suite', self.SCAN_SUITE)

        self.analytics_warmup_ops = int(options.get('analytics_warmup_ops',
                                                    self.ANALYTICS_WARMUP_OPS))
        self.analytics_warmup_workers = int(options.get('analytics_warmup_workers',
                                                        self.ANALYTICS_WARMUP_WORKERS))

        capella_endpoint = options.get('capella_endpoint', self.CAPELLA_ENDPOINT)
        if capella_endpoint:
            self.capella_endpoints = capella_endpoint.strip().split()
        else:
            self.capella_endpoints = []

        # collection map placeholder
        self.collections = self.COLLECTION_MAP

        self.custom_pillowfight = self.CUSTOM_PILLOWFIGHT

        self.users = self.USERS

        self.user_mod_workers = int(options.get('user_mod_workers', self.USER_MOD_WORKERS))

        self.user_mod_throughput = float(options.get('user_mod_throughput',
                                                     self.USER_MOD_THROUGHPUT))

        self.collection_mod_workers = int(options.get('collection_mod_workers',
                                                      self.COLLECTION_MOD_WORKERS))
        self.collection_mod_throughput = float(options.get('collection_mod_throughput',
                                                           self.COLLECTION_MOD_THROUGHPUT))
        self.java_dcp_stream = self.JAVA_DCP_STREAM
        self.java_dcp_config = self.JAVA_DCP_CONFIG
        self.java_dcp_clients = self.JAVA_DCP_CLIENTS

        self.doc_groups = int(options.get('doc_groups', self.DOCUMENT_GROUPS))

        self.fts_data_spread_workers = options.get(
            'fts_data_spread_workers',
            self.FTS_DATA_SPREAD_WORKERS
        )
        if self.fts_data_spread_workers is not None:
            self.fts_data_spread_workers = int(self.fts_data_spread_workers)

        self.fts_data_spread_worker_type = "default"
        self.split_workload = options.get('split_workload', self.SPLIT_WORKLOAD)
        self.split_workload_throughput = options.get('split_workload_throughput',
                                                     self.SPLIT_WORKLOAD_THROUGHPUT)
        self.split_workload_workers = options.get('split_workload_throughput',
                                                  self.SPLIT_WORKLOAD_WORKERS)
        self.n1ql_shutdown_type = options.get('n1ql_shutdown_type', self.N1QL_SHUTDOWN_TYPE)

        self.workload_mix = self.WORKLOAD_MIX
        if workload_mix := options.get('workload_mix'):
            self.workload_mix = workload_mix.split(',')
        self.workload_name = ''

        self.num_buckets = self.NUM_BUCKETS
        if bucket_numbers := options.get('num_buckets'):
            self.num_buckets = []
            for b in bucket_numbers.split(','):
                if '-' not in b:
                    self.num_buckets.append(int(b))
                else:
                    start, end = b.split('-')
                    self.num_buckets.extend(range(int(start), int(end) + 1))

        # Default setting. Is set based on collection stat groups in configure_collection_settings
        self.per_collection_latency = self.PER_COLLECTION_LATENCY

        self.conflict_ratio = float(options.get('conflict_ratio', self.CONFLICT_RATIO))

    def __str__(self) -> str:
        return str(self.__dict__)

    def configure_bucket_list(self, buckets):
        for b_id in self.num_buckets:
            if not 1 <= b_id <= len(buckets):
                raise ValueError('Bucket number out of range: {}. '
                                 'Bucket numbers should be in range [1, no. of buckets]. '
                                 'Current no. of buckets: {}'.format(b_id, len(buckets)))

        self.bucket_list = [buckets[i-1] for i in self.num_buckets] or buckets

    def configure_doc_settings(self, load_settings):
        self.doc_gen = load_settings.doc_gen
        self.array_size = load_settings.array_size
        self.num_categories = load_settings.num_categories
        self.num_replies = load_settings.num_replies
        self.size = load_settings.size
        self.key_fmtr = load_settings.key_fmtr
        self.size_variation_max = load_settings.size_variation_max
        self.size_variation_min = load_settings.size_variation_min

    def configure_client_settings(self, client_settings):
        if hasattr(client_settings, "pillowfight"):
            self.custom_pillowfight = True

    def configure_collection_settings(self, collection_settings: CollectionSettings):
        if collection_settings.collection_map is not None:
            self.collections = collection_settings.collection_map
            self.per_collection_latency = bool(collection_settings.collection_stat_groups)

    def configure_user_settings(self, user_settings: UserSettings):
        self.users = user_settings.num_users_per_bucket

    def configure_java_dcp_settings(self, java_dcp_settings):
        self.java_dcp_config = java_dcp_settings.config
        self.java_dcp_clients = java_dcp_settings.clients
        self.java_dcp_stream = java_dcp_settings.stream

    def configure(self, test_config):
        raise NotImplementedError()

    @staticmethod
    def compare_phase_settings(settings_list) -> Tuple[dict, list[dict]]:
        options = [set(s.__dict__) for s in settings_list]
        all_options = set.union(*options)
        diff_options = all_options - set.intersection(*options)
        for option in all_options:
            values = []
            for settings in settings_list:
                if hasattr(settings, option):
                    v = getattr(settings, option)
                    if not values:
                        values.append(v)
                    else:
                        if v != values[-1]:
                            diff_options.add(option)
                            break
                else:
                    diff_options.add(option)

        common_settings = {
            option: getattr(settings_list[0], option)
            for option in all_options - diff_options
        }

        # For each task, print out the settings which are different
        diff_settings = [
            {option: getattr(settings, option, None) for option in diff_options}
            for settings in settings_list
        ]

        return common_settings, diff_settings


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPSERTS = True

    UNIFORM_COLLECTION_LOAD_TIME = 0
    CONCURRENT_COLLECTION_LOAD = 0

    def __init__(self, options: dict):
        super().__init__(options)
        self.uniform_collection_load_time = int(options.get('uniform_collection_load_time',
                                                            self.UNIFORM_COLLECTION_LOAD_TIME))
        self.concurrent_collection_load = (
            self.uniform_collection_load_time or
            int(options.get('concurrent_collection_load', self.CONCURRENT_COLLECTION_LOAD))
        )

    def configure(self, test_config):
        self.configure_client_settings(test_config.client_settings)
        self.configure_collection_settings(test_config.collection)
        self.configure_bucket_list(test_config.buckets)


class JTSAccessSettings(PhaseSettings):

    JTS_REPO = "https://github.com/couchbaselabs/JTS"
    JTS_REPO_BRANCH = "master"
    JTS_HOME_DIR = "JTS"
    JTS_RUN_CMD = "java -jar target/JTS-1.0-jar-with-dependencies.jar"
    JTS_LOGS_DIR = "JTSlogs"
    FTS_PARTITIONS = "1"
    FTS_MAX_DCP_PARTITIONS = "0"
    FTS_FILE_BASED_REBAL_DISABLED = "true"

    def __init__(self, options: dict):
        super().__init__(options)
        self.jts_repo = self.JTS_REPO
        self.jts_home_dir = self.JTS_HOME_DIR
        self.jts_run_cmd = self.JTS_RUN_CMD
        self.jts_logs_dir = self.JTS_LOGS_DIR
        self.jts_repo_branch = options.get("jts_repo_branch", self.JTS_REPO_BRANCH)
        self.jts_instances = options.get("jts_instances", "1")
        self.test_total_docs = options.get("test_total_docs", "1000000")
        self.test_query_workers = options.get("test_query_workers", "10")
        self.test_kv_workers = options.get("test_kv_workers", "0")
        self.test_kv_throughput_goal = options.get("test_kv_throughput_goal", "1000")
        self.test_data_file = options.get("test_data_file", "../tests/fts/low.txt")
        self.test_driver = options.get("test_driver", "couchbase")
        self.test_stats_limit = options.get("test_stats_limit", "1000000")
        self.test_stats_aggregation_step = options.get("test_stats_aggregation_step", "1000")
        self.test_debug = options.get("test_debug", "false")
        self.test_query_type = options.get("test_query_type", "term")
        self.test_query_limit = options.get("test_query_limit", "10")
        self.test_query_field = options.get("test_query_field", "text")
        self.test_mutation_field = options.get("test_mutation_field", None)
        self.test_worker_type = options.get("test_worker_type", "latency")
        self.couchbase_index_name = options.get("couchbase_index_name", "perf_fts_index")
        self.couchbase_index_configfile = options.get("couchbase_index_configfile")
        self.couchbase_index_type = options.get("couchbase_index_type")
        self.workload_instances = int(self.jts_instances)
        self.time = options.get('test_duration', "600")
        self.warmup_query_workers = options.get("warmup_query_workers", "0")
        self.warmup_time = options.get('warmup_time', "0")
        self.custom_num_buckets = options.get('custom_num_buckets', "0")
        # index creation - async or sync
        self.index_creation_style = options.get('index_creation_style', 'sync')
        # Geo Queries parameters
        self.test_geo_polygon_coord_list = options.get("test_geo_polygon_coord_list", "")
        self.test_query_lon_width = options.get("test_query_lon_width", "2")
        self.test_query_lat_height = options.get("test_query_lat_height", "2")
        self.test_geo_distance = options.get("test_geo_distance", "5mi")
        # File based rebalance parameter
        self.fts_file_based_rebal_disabled = options.get('fts_file_based_rebal_disabled',
                                                         self.FTS_FILE_BASED_REBAL_DISABLED)
        # Flex Queries parameters
        self.test_flex = options.get("test_flex", 'false')
        self.test_flex_query_type = options.get('test_flex_query_type', 'array_predicate')
        # Collection settings
        self.test_collection_query_mode = options.get('test_collection_query_mode', 'default')
        self.couchbase_index_configmap = options.get('couchbase_index_configmap', None)
        # Test query mode for mixed query
        self.test_query_mode = options.get('test_query_mode', 'default')
        # Number of indexes per index - group
        self.indexes_per_group = int(options.get('indexes_per_group', '1'))
        # index_group is the number of collections per index
        # if index_group is 1; all the collections are present in the index_def type mapping
        self.index_groups = int(options.get('index_groups', '1'))
        self.fts_index_map = {}
        self.collections_enabled = False
        self.test_collection_specific_count = \
            int(options.get('test_collection_specific_count', '1'))

        # Extra parameters for the FTS debugging
        self.ftspartitions = options.get('ftspartitions', self.FTS_PARTITIONS)
        self.fts_max_dcp_partitions = options.get('fts_max_dcp_partitions',
                                                  self.FTS_MAX_DCP_PARTITIONS)
        self.fts_node_level_parameters = {}
        # Adding bucket wise latency logger for fts
        # (please use this only with multi_query_support JTS branch)
        self.logging_method = options.get('logging_method', None)
        if self.ftspartitions != self.FTS_PARTITIONS:
            self.fts_node_level_parameters["maxConcurrentPartitionMovesPerNode"] = \
                self.ftspartitions
        if self.fts_max_dcp_partitions != self.FTS_MAX_DCP_PARTITIONS:
            self.fts_node_level_parameters["maxFeedsPerDCPAgent"] = self.fts_max_dcp_partitions

        if self.fts_file_based_rebal_disabled != self.FTS_FILE_BASED_REBAL_DISABLED:
            self.fts_node_level_parameters["disableFileTransferRebalance"] = \
                self.fts_file_based_rebal_disabled
        self.raw_query_map_file = options.get('raw_query_map_file', None)
        self.report_percentiles = options.get('report_percentiles', "80,95").split(',')
        self.ground_truth_file_name = options.get('ground_truth_file_name', None)
        self.ground_truth_s3_path = options.get('ground_truth_s3_path', None)
        self.k_nearest_neighbour = int(options.get("k_nearest_neighbour", 3))
        self.vector_dimension = int(options.get("vector_dimension", 0))
        self.fts_load_workers = int(options.get("fts_load_workers", "100"))
        self.test_query_field2 = options.get("test_query_field2", None)
        self.vector_similarity_type = options.get("vector_similarity_type", None)
        self.index_partitions = options.get("index_partitions", None)
        self.index_replicas = options.get("index_replicas", None)
        self.search_query_timeout_in_sec = options.get("search_query_timeout_in_sec", None)
        self.test_geojson_query_type = options.get("test_geojson_query_type", None)
        self.test_geojson_query_relation = options.get("test_geojson_query_relation", None)
        self.aggregation_buffer_ms = options.get("aggregation_buffer_ms", "1000")
        self.max_segment_size = options.get("max_segment_size", None)
        self.vector_index_optimized_for = options.get("vector_index_optimized_for", None)
        self.skip_indexing_collection = options.get("skip_indexing_collection", None)

    def __str__(self) -> str:
        return str(self.__dict__)

    def configure(self, test_config):
        pass


class HotLoadSettings(PhaseSettings):

    HOT_READS = True

    def configure(self, test_config):
        self.configure_doc_settings(test_config.load_settings)
        self.configure_client_settings(test_config.client_settings)
        self.configure_collection_settings(test_config.collection)
        self.configure_bucket_list(test_config.buckets)


class XattrLoadSettings(LoadSettings):

    SEQ_UPSERTS = True

    def configure(self, test_config):
        self.configure_bucket_list(test_config.buckets)

class XDCRSettings:

    WAN_DELAY = 0
    NUM_XDCR_LINKS = 1
    XDCR_LINKS_PRIORITY = 'HIGH'
    INITIAL_COLLECTION_MAPPING = ''    # std format {"scope-1:collection-1":"scope-1:collection-1"}
    BACKFILL_COLLECTION_MAPPING = ''   # ----------------------"----------------------------------
    XDCR_LINK_DIRECTIONS = 'one-way'
    XDCR_LINK_NETWORK_LIMITS = '0'
    MOBILE = None    #options: None, active
    ECCV = None    #options: None, active

    WORKER_COUNT = 20
    CONN_TYPE = 'gocbcore'   # options: gocbcore, gomemcached
    CONN_LIMIT = 10
    QUEUE_LEN = 500

    def __init__(self, options: dict):
        self.demand_encryption = options.get('demand_encryption')
        self.filter_expression = options.get('filter_expression')
        self.secure_type = options.get('secure_type')
        self.wan_delay = int(options.get('wan_delay',
                                         self.WAN_DELAY))

        self.num_xdcr_links = int(options.get('num_xdcr_links', self.NUM_XDCR_LINKS))
        self.xdcr_links_priority = options.get('xdcr_links_priority',
                                               self.XDCR_LINKS_PRIORITY).split(',')
        self.initial_collection_mapping = options.get('initial_collection_mapping',
                                                      self.INITIAL_COLLECTION_MAPPING)
        self.backfill_collection_mapping = options.get('backfill_collection_mapping',
                                                       self.BACKFILL_COLLECTION_MAPPING)
        self.collections_oso_mode = bool(options.get('collections_oso_mode'))

        self.mobile = options.get('mobile', self.MOBILE)

        self.eccv = options.get('eccv', self.ECCV)

        self.worker_count = int(options.get('worker_count', self.WORKER_COUNT))
        self.conn_type = options.get('conn_type', self.CONN_TYPE)
        self.conn_limit = int(options.get('conn_limit', self.CONN_LIMIT))
        self.queue_len = int(options.get('queue_len', self.QUEUE_LEN))

        # Capella-specific settings
        self.xdcr_link_directions = options.get('xdcr_link_directions',
                                                self.XDCR_LINK_DIRECTIONS).split(',')
        self.xdcr_link_network_limits = [
            int(x) for x in options.get('xdcr_link_network_limits',
                                        self.XDCR_LINK_NETWORK_LIMITS).split(',')
        ]

    def __str__(self) -> str:
        return str(self.__dict__)


class ViewsSettings:

    VIEWS = '[1]'
    DISABLED_UPDATES = 0

    def __init__(self, options: dict):
        self.views = eval(options.get('views', self.VIEWS))
        self.disabled_updates = int(options.get('disabled_updates',
                                                self.DISABLED_UPDATES))
        self.index_type = options.get('index_type')

    def __str__(self) -> str:
        return str(self.__dict__)


class GSISettings:

    CBINDEXPERF_CONFIGFILE = ''
    CBINDEXPERF_CONCURRENCY = 0
    CBINDEXPERF_CLIENTS = 5
    CBINDEXPERF_LIMIT = 0
    CBINDEXPERF_REPEAT = 0
    CBINDEXPERF_CONFIGFILES = ''
    CBINDEXPERF_GCPERCENT = 100
    RUN_RECOVERY_TEST = 0
    INCREMENTAL_LOAD_ITERATIONS = 0
    SCAN_TIME = 1200
    INCREMENTAL_ONLY = 0
    REPORT_INITIAL_BUILD_TIME = 0
    DISABLE_PERINDEX_STATS = False
    AWS_CREDENTIAL_PATH = None
    DEFAULT_NPROBES = 10

    def __init__(self, options: dict):
        self.indexes = {}
        if options.get('indexes') is not None:
            myindexes = options.get('indexes')
            if ".json" in myindexes:
                # index definitions passed in as json file
                with open(myindexes) as f:
                    self.indexes = json.load(f)
            else:
                for index_def in myindexes.split('#'):
                    name, field = index_def.split(':')
                    if '"' in field:
                        field = field.replace('"', '\\\"')
                    self.indexes[name] = field

        self.cbindexperf_configfile = options.get('cbindexperf_configfile',
                                                  self.CBINDEXPERF_CONFIGFILE)
        self.cbindexperf_concurrency = int(options.get('cbindexperf_concurrency',
                                                       self.CBINDEXPERF_CONCURRENCY))
        self.cbindexperf_repeat = int(options.get('cbindexperf_repeat',
                                                  self.CBINDEXPERF_REPEAT))
        self.cbindexperf_configfiles = options.get('cbindexperf_configfiles',
                                                   self.CBINDEXPERF_CONFIGFILES)
        self.cbindexperf_gcpercent = int(options.get('cbindexperf_gcpercent',
                                         self.CBINDEXPERF_GCPERCENT))
        self.cbindexperf_clients = int(options.get('cbindexperf_clients',
                                       self.CBINDEXPERF_CLIENTS))
        self.cbindexperf_limit = int(options.get('cbindexperf_limit',
                                                 self.CBINDEXPERF_LIMIT))
        self.run_recovery_test = int(options.get('run_recovery_test',
                                                 self.RUN_RECOVERY_TEST))
        self.incremental_only = int(options.get('incremental_only',
                                                self.INCREMENTAL_ONLY))
        self.incremental_load_iterations = int(options.get('incremental_load_iterations',
                                                           self.INCREMENTAL_LOAD_ITERATIONS))
        self.scan_time = int(options.get('scan_time', self.SCAN_TIME))
        self.report_initial_build_time = int(options.get('report_initial_build_time',
                                                         self.REPORT_INITIAL_BUILD_TIME))

        self.disable_perindex_stats = options.get('disable_perindex_stats',
                                                  self.DISABLE_PERINDEX_STATS)
        self.aws_credential_path = options.get("aws_credential_path", self.AWS_CREDENTIAL_PATH)
        # GSI vector settings
        self.vector_dimension = int(options.get('vector_dimension', 0))
        self.vector_description = options.get('vector_description', "'IVF,SQ4'")
        self.vector_similarity = options.get('vector_similarity', 'L2')
        self.index_def_prefix = options.get("index_def_prefix", None)
        self.vector_train_list = options.get("train_list")
        self.vector_nprobes = options.get("nprobes", self.DEFAULT_NPROBES)
        self.vector_filter_percentage = options.get("vector_filter_percentage", 0)
        self.vector_scan_probes = options.get("vector_scan_probes", 0)
        self.vector_def_prefix = options.get("vector_def_prefix", None)
        self.num_index_replica = options.get("num_index_replica", None)
        self.include_columns = options.get("include_columns", None)

        self.override_index_def = options.get("override_index_def", None)
        self.partition_by_clause = options.get("partition_by_clause", None)
        self.num_partition = options.get("num_partition", None)
        self.vector_index_type = options.get("vector_index_type", "composite")

        self.settings = {}
        for option in options:
            if option.startswith(('indexer', 'projector', 'queryport', 'planner')):
                value = options.get(option)
                if '.' in value:
                    self.settings[option] = maybe_atoi(value, t=float)
                else:
                    self.settings[option] = maybe_atoi(value, t=int)

        if self.settings:
            if self.settings['indexer.settings.storage_mode'] == 'forestdb' or \
                    self.settings['indexer.settings.storage_mode'] == 'plasma':
                self.storage = self.settings['indexer.settings.storage_mode']
            else:
                self.storage = 'memdb'
        self.settings['indexer.cpu.throttle.target'] = \
            self.settings.get('indexer.cpu.throttle.target', 1.00)

        self.excludeNode = None
        if self.settings.get('planner.excludeNode'):
            self.excludeNode = self.settings.get('planner.excludeNode')
            self.settings.pop("planner.excludeNode")

        self.vector_reranking = maybe_atoi(options.get('vector_reranking', "true"))

    def __str__(self) -> str:
        return str(self.__dict__)


class DCPSettings:

    NUM_CONNECTIONS = 4
    INVOKE_WARM_UP = 0

    def __init__(self, options: dict):
        self.num_connections = int(options.get('num_connections',
                                               self.NUM_CONNECTIONS))
        self.invoke_warm_up = int(options.get('invoke_warm_up',
                                              self.INVOKE_WARM_UP))

    def __str__(self) -> str:
        return str(self.__dict__)


class N1QLSettings:

    def __init__(self, options: dict):
        self.cbq_settings = {
            option: maybe_atoi(value) for option, value in options.items()
        }

    def __str__(self) -> str:
        return str(self.__dict__)


class IndexSettings:

    FTS_INDEX_NAME = ''
    FTS_INDEX_CONFIG_FILE = ''
    TOP_DOWN = False
    INDEXES_PER_COLLECTION = 1
    REPLICAS = 0

    def __init__(self, options: dict):
        self.raw_statements = options.get('statements')
        self.fields = options.get('fields', None)
        self.replicas = int(options.get('replicas', self.REPLICAS))
        self.collection_map = options.get('collection_map')
        self.indexes_per_collection = int(options.get('indexes_per_collection',
                                                      self.INDEXES_PER_COLLECTION))
        self.top_down = bool(options.get('top_down', self.TOP_DOWN))
        self.couchbase_fts_index_name = options.get('couchbase_fts_index_name',
                                                    self.FTS_INDEX_NAME)
        self.couchbase_fts_index_configfile = options.get('couchbase_fts_index_configfile',
                                                          self.FTS_INDEX_CONFIG_FILE)
        self.ground_truth_file_name = options.get('ground_truth_file_name', None)
        self.ground_truth_s3_path = options.get('ground_truth_s3_path', None)
        self.top_k_results = options.get('top_k_results', 10)
        self.statements = self.create_index_statements()
        self.vector_query_path = options.get("vector_query_path", None)
        if self.vector_query_path:
            with open(self.vector_query_path,'r') as f:
                self.vector_query_map = f.readlines()

    def create_index_statements(self) -> list[str]:
        #  Here we generate all permutations of all subsets of index fields
        #  The total number generate given n fields is the following:
        #
        #  Sum from k=0 to n, n!/k! where
        #
        #  n=3  sum = 16
        #  n=4  sum = 65
        #  n=5  sum = 326
        #  n=6  sum = 1957
        if self.collection_map and self.fields:
            statements = []
            build_statements = []
            if self.fields.strip() == 'primary':
                for bucket, scopes in self.collection_map.items():
                    for scope, collections in scopes.items():
                        for collection, options in collections.items():
                            index_num = 1
                            if options['load'] == 1:
                                collection_num = collection.replace("collection-", "")
                                index_name = 'pi{}_{}'\
                                    .format(collection_num, index_num)
                                new_statement = \
                                    "CREATE PRIMARY INDEX {} ON default:`{}`.`{}`.`{}`". \
                                    format(index_name, bucket, scope, collection)
                                with_clause = " WITH {'defer_build': 'true',"
                                if self.replicas > 0:
                                    with_clause += "'num_replica': " + str(self.replicas) + ","
                                with_clause = with_clause[:-1]
                                with_clause += "}"
                                new_statement += with_clause
                                statements.append(new_statement)
                                build_statement = "BUILD INDEX ON default:`{}`.`{}`.`{}`('{}')" \
                                    .format(bucket, scope, collection, index_name)
                                build_statements.append(build_statement)
                                index_num += 1
            else:
                fields = self.fields.strip().split(',')
                parsed_fields = []
                index = 1
                for field in fields:
                    while field.count("(") != field.count(")"):
                        field = ",".join([field, fields[index]])
                        del fields[index]
                    index += 1
                    parsed_fields.append(field)
                fields = parsed_fields
                field_combos = list(chain.from_iterable(combinations(fields, r)
                                                        for r in range(1, len(fields)+1)))
                if self.top_down:
                    field_combos.reverse()
                for bucket, scopes in self.collection_map.items():
                    for scope, collections in scopes.items():
                        for collection, options in collections.items():
                            if options['load'] == 1:
                                indexes_created = 0
                                collection_num = collection.replace("collection-", "")
                                for field_subset in field_combos:
                                    subset_permutations = list(permutations(list(field_subset)))
                                    for permutation in subset_permutations:
                                        index_field_list = list(permutation)

                                        index_name = "i{}_{}".format(collection_num,
                                                                     str(indexes_created+1))
                                        index_fields = ",".join(index_field_list)
                                        new_statement = \
                                            "CREATE INDEX {} ON default:`{}`.`{}`.`{}`({})".\
                                            format(
                                                index_name,
                                                bucket,
                                                scope,
                                                collection,
                                                index_fields)
                                        with_clause = " WITH {'defer_build': 'true',"
                                        if self.replicas > 0:
                                            with_clause += \
                                                "'num_replica': " + str(self.replicas) + ","
                                        with_clause = with_clause[:-1]
                                        with_clause += "}"
                                        new_statement += with_clause
                                        statements.append(new_statement)
                                        build_statement = \
                                            "BUILD INDEX ON default:`{}`.`{}`.`{}`('{}')" \
                                            .format(bucket, scope, collection, index_name)
                                        build_statements.append(build_statement)
                                        indexes_created += 1
                                        if indexes_created == self.indexes_per_collection:
                                            break
                                    if indexes_created == self.indexes_per_collection:
                                        break
            statements = statements + build_statements
            return statements
        elif self.raw_statements:
            return self.raw_statements.strip().split('\n')
        elif self.raw_statements is None and self.fields is None:
            return []
        else:
            raise Exception('Index options must include one statement, '
                            'or fields (if collections enabled)')

    @property
    def indexes(self):
        if self.collection_map:
            indexes = []
            for statement in self.statements:
                match = re.search(r'CREATE .*INDEX (.*) ON', statement)
                if match:
                    indexes.append(match.group(1))
            indexes_per_collection = set(indexes)
            index_map = {}
            for bucket, scopes in self.collection_map.items():
                for scope, collections in scopes.items():
                    for collection, options in collections.items():
                        if options['load'] == 1:
                            bucket_map = index_map.get(bucket, {})
                            if bucket_map == {}:
                                index_map[bucket] = {}
                            scope_map = index_map[bucket].get(scope, {})
                            if scope_map == {}:
                                index_map[bucket][scope] = {}
                            coll_map = index_map[bucket][scope].get(collection, {})
                            if coll_map == {}:
                                index_map[bucket][scope][collection] = {}
                            for index_name in list(indexes_per_collection):
                                index_map[bucket][scope][collection][index_name] = ""
            return index_map
        else:
            indexes = []
            for statement in self.statements:
                match = re.search(r'CREATE .*INDEX (.*) ON', statement)
                if match:
                    indexes.append(match.group(1))
            return indexes

    def __str__(self) -> str:
        return str(self.__dict__)


class N1QLFunctionSettings(IndexSettings):
    pass


class AccessSettings(PhaseSettings):

    OPS = float('inf')

    def define_queries(self, config):
        queries = []
        for query_name in self.n1ql_queries:
            query = config.get_n1ql_query_definition(query_name)
            queries.append(query)
        self.n1ql_queries = queries

    def configure(self, test_config):
        self.configure_java_dcp_settings(test_config.java_dcp_settings)
        self.configure_client_settings(test_config.client_settings)
        self.configure_user_settings(test_config.users)
        self.configure_collection_settings(test_config.collection)

        load_settings = test_config.load_settings
        self.configure_doc_settings(load_settings)
        self.doc_groups = load_settings.doc_groups
        self.range_distance = load_settings.range_distance

        if self.split_workload is not None:
            with open(self.split_workload) as f:
                self.split_workload = json.load(f)

        if hasattr(self, 'n1ql_queries'):
            self.define_queries(test_config)

        self.configure_bucket_list(test_config.buckets)


class ExtraAccessSettings(PhaseSettings):

    OPS = float('inf')

    def configure(self, test_config):
        self.configure_java_dcp_settings(test_config.java_dcp_settings)
        self.configure_client_settings(test_config.client_settings)
        self.configure_user_settings(test_config.users)
        self.configure_collection_settings(test_config.collection)

        load_settings = test_config.load_settings
        self.configure_doc_settings(load_settings)
        self.doc_groups = load_settings.doc_groups
        self.range_distance = load_settings.range_distance

        self.configure_bucket_list(test_config.buckets)


class CbbackupmgrSettings:
    THREADS = None
    USE_TLS = False
    SHOW_TLS_VERSION = False
    MIN_TLS_VERSION = None
    ENCRYPTED = False
    PASSPHRASE = "couchbase"
    INCLUDE_DATA = None

    def __init__(self, options: dict):
        self.threads = options.get("threads", self.THREADS)
        self.use_tls = int(options.get("use_tls", self.USE_TLS))
        self.show_tls_version = int(options.get("show_tls_version", self.SHOW_TLS_VERSION))
        self.min_tls_version = options.get("min_tls_version", self.MIN_TLS_VERSION)
        self.encrypted = int(options.get("encrypted", self.ENCRYPTED))
        self.passphrase = options.get("passphrase", self.PASSPHRASE)
        self.include_data = options.get("include_data", self.INCLUDE_DATA)
        self.env_vars = dict(
            tuple(kv.split("="))
            for kv in options.get("env_vars", "").replace(" ", "").split(",")
            if kv
        )


class BackupSettings(CbbackupmgrSettings):
    COMPRESSION = False
    STORAGE_TYPE = None
    SINK_TYPE = None
    SHARDS = None
    OBJ_STAGING_DIR = None
    OBJ_REGION = None
    OBJ_ACCESS_KEY_ID = None
    AWS_CREDENTIAL_PATH = None
    BACKUP_DIRECTORY = None

    def __init__(self, options: dict):
        super().__init__(options)
        self.compression = int(options.get("compression", self.COMPRESSION))
        self.storage_type = options.get("storage_type", self.STORAGE_TYPE)
        self.sink_type = options.get("sink_type", self.SINK_TYPE)
        self.shards = options.get("shards", self.SHARDS)
        self.obj_staging_dir = options.get("obj_staging_dir", self.OBJ_STAGING_DIR)
        self.obj_region = options.get("obj_region", self.OBJ_REGION)
        self.obj_access_key_id = options.get("obj_access_key_id", self.OBJ_ACCESS_KEY_ID)
        self.aws_credential_path = options.get("aws_credential_path", self.AWS_CREDENTIAL_PATH)
        self.backup_directory = options.get("backup_directory", self.BACKUP_DIRECTORY)


class RestoreSettings(CbbackupmgrSettings):
    DOCS_PER_COLLECTION = 0
    BACKUP_STORAGE = "/backups"
    BACKUP_REPO = ""
    IMPORT_FILE = ""
    THREADS = 16
    MAP_DATA = None

    def __init__(self, options):
        super().__init__(options)
        self.docs_per_collections = int(
            options.get("docs_per_collection", self.DOCS_PER_COLLECTION)
        )
        self.backup_storage = options.get("backup_storage", self.BACKUP_STORAGE)
        self.backup_repo = options.get("backup_repo", self.BACKUP_REPO)
        self.import_file = options.get("import_file", self.IMPORT_FILE)
        self.map_data = options.get("map_data", self.MAP_DATA)
        self.use_csp_specific_archive = bool(options.get("use_csp_specific_archive", 0))
        self.filter_keys = options.get("filter_keys", None)

    def __str__(self) -> str:
        return str(self.__dict__)


class ImportSettings:
    IMPORT_FILE = ""
    DOCS_PER_COLLECTION = 0

    def __init__(self, options):
        self.docs_per_collections = int(
            options.get("docs_per_collection", self.DOCS_PER_COLLECTION)
        )
        self.import_file = options.get("import_file", self.IMPORT_FILE)

    def __str__(self) -> str:
        return str(self.__dict__)


class ExportSettings:

    THREADS = None
    IMPORT_FILE = None
    TYPE = 'json'  # csv or json
    FORMAT = 'lines'  # lines, list
    KEY_FIELD = None
    LOG_FILE = None
    FIELD_SEPARATOR = None
    LIMIT_ROWS = False
    SKIP_ROWS = False
    INFER_TYPES = False
    OMIT_EMPTY = False
    ERRORS_LOG = None  # error log file
    COLLECTION_FIELD = None
    SCOPE_FEILD = None
    SCOPE_COLLECTION_EXP = None

    def __init__(self, options: dict):
        self.threads = options.get('threads', self.THREADS)
        self.type = options.get('type', self.TYPE)
        self.format = options.get('format', self.FORMAT)
        self.import_file = options.get('import_file', self.IMPORT_FILE)
        self.key_field = options.get('key_field', self.KEY_FIELD)
        self.log_file = options.get('log_file', self.LOG_FILE)
        self.log_file = options.get('log_file', self.LOG_FILE)
        self.field_separator = options.get('field_separator',
                                           self.FIELD_SEPARATOR)
        self.limit_rows = int(options.get('limit_rows', self.LIMIT_ROWS))
        self.skip_rows = int(options.get('skip_rows', self.SKIP_ROWS))
        self.infer_types = int(options.get('infer_types', self.INFER_TYPES))
        self.omit_empty = int(options.get('omit_empty', self.OMIT_EMPTY))
        self.errors_log = options.get('errors_log', self.ERRORS_LOG)
        self.collection_field = options.get('collection_field', self.COLLECTION_FIELD)
        self.scope_field = options.get('scope_field', self.SCOPE_FEILD)
        self.scope_collection_exp = options.get('scope_collection_exp', self.SCOPE_COLLECTION_EXP)


class EventingSettings:
    WORKER_COUNT = 3
    CPP_WORKER_THREAD_COUNT = 2
    TIMER_WORKER_POOL_SIZE = 1
    WORKER_QUEUE_CAP = 100000
    TIMER_TIMEOUT = 0
    TIMER_FUZZ = 0
    CONFIG_FILE = "tests/eventing/config/function_sample.json"
    REQUEST_URL = "http://172.23.99.247:8080/cgi-bin/text/1kb_text_200"
    EVENTING_DEST_BKT_DOC_GEN = "basic"
    CURSOR_AWARE = False

    def __init__(self, options: dict):
        self.functions = {}
        if options.get('functions') is not None:
            for function_def in options.get('functions').split(','):
                name, filename = function_def.split(':')
                self.functions[name.strip()] = filename.strip()

        self.worker_count = int(options.get("worker_count", self.WORKER_COUNT))
        self.cpp_worker_thread_count = int(options.get("cpp_worker_thread_count",
                                                       self.CPP_WORKER_THREAD_COUNT))
        self.timer_worker_pool_size = int(options.get("timer_worker_pool_size",
                                                      self.TIMER_WORKER_POOL_SIZE))
        self.worker_queue_cap = int(options.get("worker_queue_cap",
                                                self.WORKER_QUEUE_CAP))
        self.timer_timeout = int(options.get("timer_timeout",
                                             self.TIMER_TIMEOUT))
        self.timer_fuzz = int(options.get("timer_fuzz",
                                          self.TIMER_FUZZ))
        self.config_file = options.get("config_file", self.CONFIG_FILE)
        self.request_url = options.get("request_url", self.REQUEST_URL)
        self.eventing_dest_bkt_doc_gen = options.get("eventing_dest_bkt_doc_gen",
                                                     self.EVENTING_DEST_BKT_DOC_GEN)
        self.cursor_aware = options.get("cursor_aware", self.CURSOR_AWARE)
        self.functions_count = int(options.get("functions_count", 0))
        self.num_nodes_running = int(options.get("num_nodes_running", 1))

    def __str__(self) -> str:
        return str(self.__dict__)


class MagmaSettings:
    COLLECT_PER_SERVER_STATS = 0
    STORAGE_QUOTA_PERCENTAGE = 0
    MAGMA_MIN_MEMORY_QUOTA = 0

    def __init__(self, options: dict):
        self.collect_per_server_stats = int(options.get("collect_per_server_stats",
                                                        self.COLLECT_PER_SERVER_STATS))
        self.storage_quota_percentage = int(options.get("storage_quota_percentage",
                                                        self.STORAGE_QUOTA_PERCENTAGE))
        self.magma_min_memory_quota = int(options.get("magma_min_memory_quota",
                                                      self.MAGMA_MIN_MEMORY_QUOTA))


class AnalyticsCBOSampleSize(SafeEnum):
    DEFAULT = ""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AnalyticsExternalFileFormat(SafeEnum):
    DEFAULT = ""
    JSON = "json"
    CSV = "csv"
    TSV = "tsv"
    PARQUET = "parquet"
    AVRO = "avro"


class AnalyticsExternalTableFormat(SafeEnum):
    DEFAULT = ""
    DELTALAKE = "delta"
    ICEBERG = "apache-iceberg"


class AnalyticsSettings:

    NUM_IO_DEVICES = 1
    REPLICA_ANALYTICS = 0
    QUERIES = ""
    DATASET_CONF_FILE = ""
    INDEX_CONF_FILE = ""
    DROP_DATASET = ""
    ANALYTICS_LINK = "Local"
    EXTERNAL_DATASET_TYPE = "s3"  # alt: gcs, azureblob
    EXTERNAL_DATASET_REGION = "us-east-1"
    EXTERNAL_BUCKET = None
    EXTERNAL_FILE_INCLUDE = None
    AWS_CREDENTIAL_PATH = None
    EXTERNAL_AZURE_STORAGE_ACCOUNT = "cbperfstorage"
    STORAGE_FORMAT = ""
    USE_CBO = "false"
    INGEST_DURING_LOAD = "false"
    RESYNC = "true"

    def __init__(self, options: dict):
        self.num_io_devices = int(options.pop('num_io_devices',
                                              self.NUM_IO_DEVICES))
        self.replica_analytics = int(
            options.pop('replica_analytics', self.REPLICA_ANALYTICS)
        )
        self.queries = options.pop("queries", self.QUERIES)
        self.dataset_conf_file = options.pop("dataset_conf_file", self.DATASET_CONF_FILE)
        self.index_conf_file = options.pop("index_conf_file", self.INDEX_CONF_FILE)
        self.drop_dataset = options.pop("drop_dataset", self.DROP_DATASET)
        self.analytics_link = options.pop("analytics_link", self.ANALYTICS_LINK)
        self.external_dataset_type = options.pop("external_dataset_type",
                                                 self.EXTERNAL_DATASET_TYPE)
        self.external_dataset_region = options.pop("external_dataset_region",
                                                   self.EXTERNAL_DATASET_REGION)
        self.external_bucket = options.pop("external_bucket", self.EXTERNAL_BUCKET)
        self.external_file_format = AnalyticsExternalFileFormat(
            options.pop("external_file_format", "").lower()
        )
        self.external_table_format = AnalyticsExternalTableFormat(
            options.pop("external_table_format", "").lower()
        )
        self.external_file_include = options.pop("external_file_include",
                                                 self.EXTERNAL_FILE_INCLUDE)
        self.aws_credential_path = options.pop('aws_credential_path', self.AWS_CREDENTIAL_PATH)
        self.external_azure_storage_account = options.pop(
            "external_azure_storage_account", self.EXTERNAL_AZURE_STORAGE_ACCOUNT
        )
        self.storage_format = options.pop('storage_format', self.STORAGE_FORMAT)

        self.columnar_storage_partitions = int(options.pop("columnar_storage_partitions", 0))
        self.use_cbo = maybe_atoi(options.pop("use_cbo", self.USE_CBO))
        self.cbo_sample_size = AnalyticsCBOSampleSize(options.pop("cbo_sample_size", "").lower())
        self.ingest_during_load = maybe_atoi(
            options.pop("ingest_during_load", self.INGEST_DURING_LOAD)
        )
        self._bigfun_request_params_undecoded = options.pop("bigfun_request_params", "{}")

        # Only applicable to incremental ingestion tests
        self.resync = maybe_atoi(options.pop("resync", self.RESYNC))

        # Remaining settings are for analytics config REST API
        self.config_settings = {k: maybe_atoi(v) for k, v in options.items()}

    @property
    def bigfun_request_params(self) -> dict:
        try:
            return json.loads(self._bigfun_request_params_undecoded)
        except json.JSONDecodeError:
            logger.warning("Failed to decode JSON from bigfun_request_params option")
            return {}


class ColumnarKafkaLinksSettings:

    # Kafka Broker settings
    PARTITIONS_PER_TOPIC = 1

    # General connector settings
    MIN_WORKER_COUNT = 1
    MAX_WORKER_COUNT = 2
    MIN_CPU_PER_WORKER = 1

    # Sink connector settings
    BATCH_SIZE_BYTES = 5000000
    MAX_TASKS_PER_WORKER = 1

    # Source settings
    LINK_SOURCE = 'MYSQLDB'  # MONGODB | DYNAMODB | MYSQLDB

    # Test settings
    INGESTION_TIMEOUT_MINS = 360

    def __init__(self, options: dict):
        self.partitions_per_topic = options.get('partitions_per_topic', self.PARTITIONS_PER_TOPIC)

        self.mongodb_connector_custom_plugin = options.get('mongodb_connector_custom_plugin')
        self.cbas_connector_custom_plugin = options.get('cbas_connector_custom_plugin')
        self.msk_connect_worker_config = options.get('msk_connect_worker_config')
        self.msk_connect_service_exec_role = options.get('msk_connect_service_exec_role')
        self.connector_log_bucket = options.get('connector_log_bucket')

        self.min_worker_count = options.get('min_worker_count', self.MIN_WORKER_COUNT)
        self.max_worker_count = options.get('max_worker_count', self.MAX_WORKER_COUNT)
        self.min_cpu_per_worker = options.get('min_cpu_per_worker', self.MIN_CPU_PER_WORKER)

        self.batch_size_bytes = options.get('batch_size_bytes', self.BATCH_SIZE_BYTES)
        self.max_tasks_per_worker = options.get('max_tasks_per_worker', self.MAX_TASKS_PER_WORKER)

        self.link_source = options.get('link_source', self.LINK_SOURCE)
        self.mongodb_uri = options.get('mongodb_uri')

        self.remote_database_name = options.get('remote_database_name')
        self.primary_key_field = options.get('primary_key_field')

        self.ingestion_timeout_mins = int(options.get('ingestion_timeout_mins',
                                          self.INGESTION_TIMEOUT_MINS))


class ColumnarCopyToSettings:
    QUERY_LOOPS = 3
    OBJECT_STORE_FILE_FORMATS = "json"  # alt: csv, parquet
    MAX_OBJECTS_PER_FILES = "10000"
    OBJECT_STORE_COMPRESSIONS = "none"  # alt: gzip, snappy (parquet-only), zstd (parquet-only)
    GZIP_COMPRESSION_LEVELS = "6"  # ranges from 1-9
    PARQUET_SCHEMA_INFERENCE = "false"

    def __init__(self, options: dict):
        self.query_loops = int(options.get("query_loops", self.QUERY_LOOPS))
        self.object_store_file_formats = (
            options.get("object_store_file_formats", self.OBJECT_STORE_FILE_FORMATS)
            .replace(",", " ")
            .split()
        )
        self.max_objects_per_files = (
            options.get("max_objects_per_files", self.MAX_OBJECTS_PER_FILES)
            .replace(",", " ")
            .split()
        )
        self.object_store_compressions = (
            options.get("object_store_compressions", self.OBJECT_STORE_COMPRESSIONS)
            .replace(",", " ")
            .split()
        )
        self.gzip_compression_levels = (
            options.get("gzip_compression_levels", self.GZIP_COMPRESSION_LEVELS)
            .replace(",", " ")
            .split()
        )
        self.parquet_schema_inference = maybe_atoi(
            options.get("parquet_schema_inference", self.PARQUET_SCHEMA_INFERENCE)
        )
        self.parquet_row_group_sizes = (
            options.get("parquet_row_group_sizes", "").replace(",", " ").split()
        ) or [None]
        self.parquet_page_sizes = (
            options.get("parquet_page_sizes", "").replace(",", " ").split()
        ) or [None]
        self.parquet_max_schemas = (
            options.get("parquet_max_schemas", "").replace(",", " ").split()
        ) or [None]
        self.object_store_query_file = options.get("object_store_query_file")
        self.kv_query_file = options.get("kv_query_file")
        self.pairwise_param_combinations = maybe_atoi(
            options.get("pairwise_param_combinations", "false")
        )

        self.all_param_combinations  # run for validation

    @property
    def all_param_combinations(self) -> list[tuple[Any, ...]]:
        """Return combinations of COPY TO statement "WITH" clause parameters.

        Default is to use cartesian product of all parameters, unless
        `self.pairwise_param_combinations` is True in which case parameters are combined pairwise.
        """
        all_param_lists = [
            self.object_store_file_formats,
            self.max_objects_per_files,
            self.object_store_compressions,
            self.gzip_compression_levels,
            self.parquet_row_group_sizes,
            self.parquet_page_sizes,
            self.parquet_max_schemas,
        ]

        if not self.pairwise_param_combinations:
            return list(itertools.product(*all_param_lists))

        param_list_lengths = set(len(p) for p in all_param_lists)
        num_lengths = len(param_list_lengths)
        max_len = max(param_list_lengths)
        if num_lengths > 2 or (num_lengths == 2 and 1 not in param_list_lengths):
            raise ValueError(
                f"Expected all parameter lists to contain either 1 or {max_len} elements. "
                f"Got parameter lists with lengths: {param_list_lengths}."
            )

        return list(zip(*map(lambda ps: ps * max_len if len(ps) == 1 else ps, all_param_lists)))


class ColumnarSettings:
    ON_OFF_CYCLES = 3
    ON_DURATION_SECS = 120
    OFF_DURATION_SECS = 300
    ON_OFF_POLL_INTERVAL_SECS = 10
    ON_OFF_TIMEOUT_SECS = 1200

    SWEEP_THRESHOLD_BYTES = 1_000_000_000

    def __init__(self, options: dict):
        self.on_off_cycles = int(options.get("on_off_cycles", self.ON_OFF_CYCLES))
        self.on_duration = int(options.get("on_duration", self.ON_DURATION_SECS))
        self.off_duration = int(options.get("off_duration", self.OFF_DURATION_SECS))
        self.on_off_poll_interval = int(
            options.get("on_off_poll_interval", self.ON_OFF_POLL_INTERVAL_SECS)
        )
        self.on_off_timeout = int(options.get("on_off_timeout", self.ON_OFF_TIMEOUT_SECS))

        self.unlimited_storage_skip_baseline = maybe_atoi(
            options.get("unlimited_storage_skip_baseline", "false")
        )
        self.debug_sweep_threshold_enabled = maybe_atoi(
            options.get("debug_sweep_threshold_enabled", "false")
        )
        # per-node sweep threshold for columnar selective caching
        self.sweep_threshold_bytes = int(
            options.get("sweep_threshold_bytes", self.SWEEP_THRESHOLD_BYTES)
        )
        # setting to override which datasets are imported from an object store (e.g. S3, GCS),
        # where applicable. Can be used e.g. to import only a subset of all datasets.
        # datasets can be specified as a pair <source>:<target> or just <source>, in which case
        # <target> is assumed to be the same as <source>
        self.object_store_import_datasets = [
            (s[0], s[1] if len(s) > 1 else s[0])
            for dataset in options.get("object_store_import_datasets", "").replace(",", " ").split()
            if (s := dataset.split(":"))
        ]
        self.dataset_transform_def_file = options.get("dataset_transform_def_file")

        # Enterprise Analytics settings
        self.blob_storage_endpoint = options.get("blob_storage_endpoint")
        self.blob_storage_bucket = options.get("blob_storage_bucket")
        self.blob_storage_scheme = options.get("blob_storage_scheme")
        self.blob_storage_region = options.get("blob_storage_region")


class AuditSettings:

    ENABLED = True

    EXTRA_EVENTS = ''

    def __init__(self, options: dict):
        self.enabled = bool(options.get('enabled', self.ENABLED))
        self.extra_events = set(options.get("extra_events", self.EXTRA_EVENTS).split())


class SGWAuditSettings(AuditSettings):

    ENABLED = False


class YCSBSettings:

    REPO = 'https://github.com/couchbaselabs/YCSB.git'
    BRANCH = 'master'
    SDK_VERSION = None
    LATENCY_PERCENTILES = [98]
    AVERAGE_LATENCY = 0

    def __init__(self, options: dict):
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)
        self.sdk_version = options.get('sdk_version', self.SDK_VERSION)
        self.latency_percentiles = options.get('latency_percentiles', self.LATENCY_PERCENTILES)
        if isinstance(self.latency_percentiles, str):
            self.latency_percentiles = [int(x) for x in self.latency_percentiles.split(',')]
        self.average_latency = int(options.get('average_latency', self.AVERAGE_LATENCY))

    def __str__(self) -> str:
        return str(self.__dict__)


class SDKTestingSettings:

    ENABLE_SDKTEST = 0
    SDK_TYPE = ['java', 'libc', 'python']
    SDK_TIMEOUT_DEFAULT = 2500  # ms
    SDK_CONFIG_POLL_INTERVAL_DEFAULT = 2500  # ms

    def __init__(self, options: dict):
        self.enable_sdktest = int(options.get('enable_sdktest', self.ENABLE_SDKTEST))
        self.sdk_type = self.SDK_TYPE + options.get('sdk_type', '').split()
        self.sdk_timeout = int(options.get('sdk_timeout', self.SDK_TIMEOUT_DEFAULT))
        self.config_poll_interval = int(options.get('config_poll_interval',
                                                    self.SDK_CONFIG_POLL_INTERVAL_DEFAULT))
        self.benchmark_name = options.get('bench_name')

    def __str__(self) -> str:
        return str(self.__dict__)


class ClientSettings:

    LIBCOUCHBASE = None
    PYTHON_CLIENT = None
    TABLEAU_CONNECTOR = None

    def __init__(self, options: dict):
        self.libcouchbase = options.get('libcouchbase', self.LIBCOUCHBASE)
        self.python_client = options.get('python_client', self.PYTHON_CLIENT)
        self.java_client = options.get('java_client', None)
        self.go_client = options.get('go_client', None)
        self.dotnet_client = options.get('dotnet_client', None)
        self.tableau_connector = options.get('tableau_connector', self.TABLEAU_CONNECTOR)
        self.cherrypick = options.get("cherrypick")

        # Provide an easy way to figure out the sdk version for code that dont care which
        # SDK it is interacting with
        self.sdk_version = self.dotnet_client or self.go_client or self.java_client \
            or self.python_client or self.libcouchbase  # libcouchbase should always be last

    def __str__(self) -> str:
        return str(self.__dict__)


class JavaDCPSettings:

    REPO = 'https://github.com/couchbase/java-dcp-client.git'

    BRANCH = 'master'

    COMMIT = None

    STREAM = 'all'

    CLIENTS = 1

    def __init__(self, options: dict):
        self.config = options.get('config')
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)
        self.commit = options.get('commit', self.COMMIT)
        self.stream = options.get('stream', self.STREAM)
        self.clients = int(options.get('clients', self.CLIENTS))

    def __str__(self) -> str:
        return str(self.__dict__)


class DCPDrainSettings(PhaseSettings):
    """
    Phase-style settings holder for [dcpdrain].

    Inherits PhaseSettings so it integrates with PerfTest.generic_phase.
    Recognized keys (INI): binary_path, clients, num_connections,
    buffer_size, acknowledge_ratio, extra_args, name, stream.
    """

    def __init__(self, options: dict):
        # PhaseSettings often expects strings from config - call super to initialize common fields
        super().__init__(options)  # let PhaseSettings pick up standard fields (time, workers, etc.)

        # binary path
        self.binary_path = options.get('binary_path')

        # underlying dcpdrain flags
        self.num_connections = int(options.get('num_connections', 128))

        self.buffer_size = int(options.get('buffer_size', 0))
        try:
            self.acknowledge_ratio = float(options.get('acknowledge_ratio', 0.5))
        except Exception:
            self.acknowledge_ratio = 0.5

        # TLS flags
        # Accept common boolean strings
        v = options.get("use_tls")
        self.use_tls = bool(maybe_atoi(v)) if v is not None else False


        # Optional explicit tls triple: "cert.pem,key.pem,ca.pem"
        self.tls_opts = options.get("tls_opts", None)

        self.extra_args = options.get('extra_args', '')
        self.name = options.get('name', None)
        self.stream = options.get('stream', 'all')  # default to 'all' so metrics pick it up
        # TODO: consider using an Enum for stream names
        # (e.g., DCPStream.DCP, DCPStream.DCPDRAIN, DCPStream.ALL)


class MagmaBenchmarkSettings:

    NUM_KVSTORES = 1
    WRITE_BATCHSIZE = 1000
    KEY_LEN = 40
    DOC_SIZE = 1024
    NUM_DOCS = 100000000
    NUM_WRITES = 1000000000
    NUM_READS = 1000000000
    NUM_READERS = 32
    NUM_WRITERS = 128
    MEM_QUOTA = 1048576
    FS_CACHE_SIZE = 5368709120
    WRITE_MULTIPLIER = 5
    DATA_DIR = "/data"
    ENGINE = "magma"
    ENGINE_CONFIG = ""

    def __init__(self, options: dict):
        self.num_kvstores = int(options.get('num_kvstores', self.NUM_KVSTORES))
        self.write_batchsize = int(options.get('write_batchsize', self.WRITE_BATCHSIZE))
        self.key_len = int(options.get('key_len', self.KEY_LEN))
        self.doc_size = int(options.get('doc_size', self.DOC_SIZE))
        self.num_docs = int(options.get('num_docs', self.NUM_DOCS))
        self.num_writes = int(options.get('num_writes', self.NUM_WRITES))
        self.num_reads = int(options.get('num_reads', self.NUM_READS))
        self.num_readers = int(options.get('num_readers', self.NUM_READERS))
        self.num_writers = int(options.get('num_writers', self.NUM_WRITERS))
        self.memquota = int(options.get('memquota', self.MEM_QUOTA))
        self.fs_cache_size = int(options.get('fs_cache_size', self.FS_CACHE_SIZE))
        self.write_multiplier = int(options.get('write_multiplier', self.WRITE_MULTIPLIER))
        self.data_dir = options.get('data_dir', self.DATA_DIR)
        self.engine = options.get('engine', self.ENGINE)
        self.engine_config = options.get('engine_config', self.ENGINE_CONFIG)

    def __str__(self) -> str:
        return str(self.__dict__)


class TPCDSLoaderSettings:

    REPO = 'https://github.com/couchbaselabs/cbas-perf-support.git'
    BRANCH = 'master'

    def __init__(self, options: dict):
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)

    def __str__(self) -> str:
        return str(self.__dict__)


@dataclass
class CH2ConnectionSettings:
    userid: str
    password: str
    userid_analytics: Optional[str] = None
    password_analytics: Optional[str] = None
    query_url: Optional[str] = None
    multi_query_url: Optional[str] = None
    analytics_url: Optional[str] = None
    data_url: Optional[str] = None
    multi_data_url: Optional[str] = None
    use_tls: bool = False

    @property
    def cli_args_common(self) -> list[str]:
        flags = [f"--userid {self.userid}", f"--password='{self.password}'"]

        if self.use_tls:
            flags += ["--tls"]

        if self.userid_analytics:
            flags += [f"--userid-analytics {self.userid_analytics}"]

        if self.password_analytics:
            flags += [f"--password-analytics='{self.password_analytics}'"]

        return flags

    def cli_args_str_run(self, tclients: int, aclients: int) -> str:
        """Return a string of connection-related CLI arguments for running benchmark phase."""
        flags = self.cli_args_common

        if tclients > 0:
            flags += [
                f"--query-url {self.query_url}",
                f"--multi-query-url {self.multi_query_url}",
            ]

        if aclients > 0:
            flags += [f"--analytics-url {self.analytics_url}"]

        return " ".join(flags)

    def cli_args_str_load(self, load_mode: str) -> str:
        """Return a string of connection-related CLI arguments for running load phase."""
        flags = self.cli_args_common

        if load_mode == "qrysvc-load":
            flags += [
                f"--query-url {self.query_url}",
                f"--multi-query-url {self.multi_query_url}",
            ]
        else:
            flags += [
                f"--data-url {self.data_url}",
                f"--multi-data-url {self.multi_data_url}",
            ]

        return " ".join(flags)


class CH2Schema(SafeEnum):
    CH2 = "ch2"  # Original CH2 with mostly flat schema
    CH2P = "ch2p"  # CH2+: More nested than CH2, no extra, unused fields. "Between" CH2 and CH2++
    CH2PP = "ch2pp"  # CH2++: More nested than CH2+ with tunable number of extra, unused fields
    CH2PPF = "ch2ppf"  # Flat CH2++: fully normalized CH2++ schema


class CH2:
    REPO = "https://github.com/couchbaselabs/ch2.git"
    BRANCH = "main"
    WAREHOUSES = 1000
    DATAGEN_SEED = -1  # valid seed is >= 0
    ACLIENTS = 0
    TCLIENTS = 0
    ITERATIONS = 1
    WARMUP_ITERATIONS = 0
    WARMUP_DURATION_SECS = 0
    DURATION_SECS = 0
    WORKLOAD = "ch2_mixed"
    USE_BACKUP = "true"
    LOAD_TCLIENTS = 0
    LOAD_TASKS = 1
    LOAD_MODE = "datasvc-bulkload"
    CREATE_GSI_INDEX = "true"
    DEBUG = "false"
    TXTIMEOUT_SECS = 3
    USE_UNOPTIMIZED_QUERIES = "false"
    IGNORE_SKIP_INDEX_HINTS = "false"
    CUSTOMER_EXTRA_FIELDS = 128
    ORDERS_EXTRA_FIELDS = 128
    ITEM_EXTRA_FIELDS = 128
    ACLIENT_REQUEST_PARAMS = ""

    def __init__(self, options: dict):
        self.repo = options.get("repo", self.REPO)
        self.branch = options.get("branch", self.BRANCH)
        self.cherrypick = options.get("cherrypick")
        self.warehouses = int(options.get("warehouses", self.WAREHOUSES))
        self.datagen_seed = int(options.get("datagen_seed", self.DATAGEN_SEED))
        self.aclients = int(options.get("aclients", self.ACLIENTS))
        self.tclients = int(options.get("tclients", self.TCLIENTS))
        self.load_tclients = int(options.get("load_tclients", self.LOAD_TCLIENTS))
        self.load_tasks = int(options.get("load_tasks", self.LOAD_TASKS))
        self.load_mode = options.get("load_mode", self.LOAD_MODE)
        self.iterations = int(options.get("iterations", self.ITERATIONS))
        self.warmup_iterations = int(options.get("warmup_iterations", self.WARMUP_ITERATIONS))
        self.warmup_duration = int(options.get("warmup_duration", self.WARMUP_DURATION_SECS))
        self.duration = int(options.get("duration", self.DURATION_SECS))
        self.workload = options.get("workload", self.WORKLOAD)
        self.use_backup = maybe_atoi(options.get("use_backup", self.USE_BACKUP))
        self.create_gsi_index = maybe_atoi(options.get("create_gsi_index", self.CREATE_GSI_INDEX))
        self.debug = maybe_atoi(options.get("debug", self.DEBUG))
        self.txtimeout = float(options.get("txtimeout", self.TXTIMEOUT_SECS))
        self.unoptimized_queries = maybe_atoi(
            options.get("unoptimized_queries", self.USE_UNOPTIMIZED_QUERIES)
        )
        self.ignore_skip_index_hints = maybe_atoi(
            options.get("ignore_skip_index_hints", self.IGNORE_SKIP_INDEX_HINTS)
        )
        self.schema = CH2Schema(schema) if (schema := options.get("schema")) else CH2Schema.CH2
        self.customer_extra_fields = int(
            options.get("customer_extra_fields", self.CUSTOMER_EXTRA_FIELDS)
        )
        self.orders_extra_fields = int(options.get("orders_extra_fields", self.ORDERS_EXTRA_FIELDS))
        self.item_extra_fields = int(options.get("item_extra_fields", self.ITEM_EXTRA_FIELDS))
        self.aclient_request_params = options.get(
            "aclient_request_params", self.ACLIENT_REQUEST_PARAMS
        )

        self.starting_warehouse = 1

    def __str__(self) -> str:
        return str(self.__dict__)

    def cli_args_str_run(self) -> str:
        """Return a string of workload-related CLI arguments for running benchmark phase."""
        flags = [
            "--debug" if self.debug else None,
            f"--warehouses {self.warehouses}",
            f"--aclients {self.aclients}" if self.aclients else None,
            f"--tclients {self.tclients}" if self.tclients else None,
            f"--duration {self.duration}" if self.duration else None,
            f"--warmup-duration {self.warmup_duration}" if self.warmup_duration else None,
            f"--query-iterations {self.iterations}" if self.iterations else None,
            f"--warmup-query-iterations {self.warmup_iterations}"
            if self.warmup_iterations
            else None,
            f"--txtimeout {self.txtimeout}",
            "--no-load",
            "--nonOptimizedQueries" if self.unoptimized_queries else None,
            "--ignore-skip-index-hints" if self.ignore_skip_index_hints else None,
            f"--{self.schema.value}" if self.schema is not CH2Schema.CH2 else None,
            f"--aclient-request-params '{self.aclient_request_params}'"
            if self.aclient_request_params
            else None,
        ]
        return " ".join(filter(None, flags))

    def cli_args_str_load(self) -> str:
        """Return a string of workload-related CLI arguments for running load phase."""
        flags = [
            "--debug" if self.debug else None,
            f"--starting_warehouse {self.starting_warehouse}",
            f"--warehouses {self.warehouses}",
            f"--datagenSeed {self.datagen_seed}" if self.datagen_seed >= 0 else None,
            f"--tclients {self.load_tclients}",
            "--no-execute",
            f"--{self.load_mode}",
            f"--{self.schema.value}" if self.schema is not CH2Schema.CH2 else None,
            f"--customerExtraFields {self.customer_extra_fields}"
            if self.schema is CH2Schema.CH2PP
            else None,
            f"--ordersExtraFields {self.orders_extra_fields}"
            if self.schema is CH2Schema.CH2PP
            else None,
            f"--itemExtraFields {self.item_extra_fields}"
            if self.schema is CH2Schema.CH2PP
            else None,
        ]
        return " ".join(filter(None, flags))


@dataclass
class CH3ConnectionSettings(CH2ConnectionSettings):
    fts_url: Optional[str] = None

    def cli_args_str_run(self, tclients: int, aclients: int, fclients: int) -> str:
        """Return a string of connection-related CLI arguments for running benchmark phase."""
        args_str = super().cli_args_str_run(tclients, aclients)

        if fclients > 0:
            args_str += f" --fts-url {self.fts_url}"

        return args_str


class CH3(CH2):
    REPO = "https://github.com/couchbaselabs/ch3.git"
    FCLIENTS = 0
    WORKLOAD = "ch3_mixed"

    def __init__(self, options: dict):
        super().__init__(options)
        self.schema = CH2Schema.CH2
        self.unoptimized_queries = False
        self.fclients = int(options.get("fclients", self.FCLIENTS))

    def __str__(self) -> str:
        return str(self.__dict__)

    def cli_args_str_run(self) -> str:
        """Return a string of workload-related CLI arguments for running benchmark phase."""
        return f"{super().cli_args_str_run()} --fclients {self.fclients}"


class PYTPCCSettings:

    WAREHOUSE = 1
    CLIENT_THREADS = 1
    DURATION = 600
    MULTI_QUERY_NODE = 0
    DRIVER = 'n1ql'
    QUERY_PORT = '8093'
    KV_PORT = '8091'
    RUN_SQL_SHELL = 'run_sqlcollections.sh'
    RUN_FUNCTION_SHELL = 'run_sqlfunctions.sh'
    CBRINDEX_SQL = 'cbcrindexcollection_replicas3.sql'
    CBRFUNCTION_SQL = 'cbcrjsfunctions.sql'
    COLLECTION_CONFIG = 'cbcrbucketcollection_20GB.sh'
    DURABILITY_LEVEL = 'majority'
    SCAN_CONSISTENCY = 'not_bounded'
    TXTIMEOUT = 3.0
    TXT_CLEANUP_WINDOW = 0
    PYTPCC_BRANCH = 'py3'
    PYTPCC_REPO = 'https://github.com/couchbaselabs/py-tpcc.git'
    INDEX_REPLICAS = 0

    def __init__(self, options: dict):
        self.warehouse = int(options.get('warehouse', self.WAREHOUSE))
        self.client_threads = int(options.get('client_threads',
                                              self.CLIENT_THREADS))
        self.duration = int(options.get('duration', self.DURATION))
        self.multi_query_node = int(options.get('multi_query_node',
                                                self.MULTI_QUERY_NODE))
        self.driver = options.get('driver', self.DRIVER)
        self.query_port = options.get('query_port', self.QUERY_PORT)
        self.kv_port = options.get('kv_port', self.KV_PORT)
        self.run_sql_shell = options.get('run_sql_shell', self.RUN_SQL_SHELL)
        self.run_function_shell = options.get('run_function_shell', self.RUN_FUNCTION_SHELL)
        self.cbrindex_sql = options.get('cbrindex_sql', self.CBRINDEX_SQL)
        self.cbrfunction_sql = options.get('cbrfunction_sql', self.CBRFUNCTION_SQL)
        self.collection_config = options.get('collection_config',
                                             self.COLLECTION_CONFIG)
        self.durability_level = options.get('durability_level',
                                            self.DURABILITY_LEVEL)
        self.scan_consistency = options.get('scan_consistency',
                                            self.SCAN_CONSISTENCY)
        self.txtimeout = options.get('txtimeout', self.TXTIMEOUT)
        self.txt_cleanup_window = int(options.get('txt_cleanup_window',
                                                  self.TXT_CLEANUP_WINDOW))
        self.pytpcc_branch = options.get('pytpcc_branch', self.PYTPCC_BRANCH)
        self.pytpcc_repo = options.get('pytpcc_repo', self.PYTPCC_REPO)
        self.use_pytpcc_backup = bool(options.get('use_pytpcc_backup'))
        self.index_replicas = int(options.get('index_replicas',
                                              self.INDEX_REPLICAS))

    def __str__(self) -> str:
        return str(self.__dict__)


class AutoscalingSettings:

    ENABLED = False
    MIN_NODES = 0
    MAX_NODES = 0
    SERVER_GROUP = None
    TARGET_METRIC = None
    TARGET_TYPE = None
    TARGET_VALUE = None

    def __init__(self, options: dict):
        self.min_nodes = options.get('min_nodes', self.MIN_NODES)
        self.max_nodes = options.get('max_nodes', self.MAX_NODES)
        self.server_group = options.get('server_group', self.SERVER_GROUP)
        self.target_metric = options.get('target_metric', self.TARGET_METRIC)
        self.target_type = options.get('target_type', self.TARGET_TYPE)
        self.target_value = options.get('target_value', self.TARGET_VALUE)
        self.enabled = self.ENABLED
        if self.min_nodes and self.max_nodes:
            self.enabled = True

    def __str__(self) -> str:
        return str(self.__dict__)


class TableauSettings:

    HOST = 'localhost'
    API_VERSION = '3.14'
    CREDENTIALS = {
        'username': 'admin',
        'password': 'password'
    }
    DATASOURCE = None
    CONNECTOR_VENDOR = None

    def __init__(self, options: dict):
        self.host = options.get('host', self.HOST)
        self.api_version = options.get('api_version', self.API_VERSION)
        self.datasource = options.get('datasource', self.DATASOURCE)
        self.connector_vendor = options.get('connector_vendor', self.CONNECTOR_VENDOR)
        credentials = options.get('credentials')
        if credentials:
            uname, password = credentials.split(':')
            self.credentials = {'username': uname, 'password': password}
        else:
            self.credentials = self.CREDENTIALS


class SyncgatewaySettings:
    REPO = 'https://github.com/couchbaselabs/YCSB.git'
    YCSB_COMMAND = 'syncgateway'
    BRANCH = 'tmp-sqw-weekly-updated-c3'
    WORKLOAD = 'workloads/syncgateway_blank'
    USERS = 100
    CHANNELS = 1
    CHANNLES_PER_USER = 1
    LOAD_CLIENTS = 1
    CLIENTS = 4
    NODES = 4
    CHANNELS_PER_DOC = 1
    DOCUMENTS = 1000000
    DOCUMENTSPULL = 500000
    DOCUMENTSPUSH = 500000
    ROUNDTRIP_WRITE = "false"
    READ_MODE = 'documents'          # |documents|changes
    FEED_READING_MODE = 'withdocs'   # |withdocs|idsonly
    FEED_MODE = 'longpoll'           # |longpoll|normal
    INSERT_MODE = 'byuser'           # |byuser|bykey
    AUTH = "true"
    PULLPROPORTION = 0.5
    PUSHPROPORTION = 0.5
    READPROPORTION = 1
    UPDATEPROPORTION = 0
    INSERTPROPORTION = 0
    SCANPROPORTION = 0
    REQUESTDISTRIBUTION = 'zipfian'  # |zipfian|uniform
    LOG_TITE = 'sync_gateway_default'
    LOAD_THREADS = 1
    THREADS = 10
    INSERTSTART = 0
    STAR = "false"
    GRANT_ACCESS = "false"
    GRANT_ACCESS_IN_SCAN = "false"
    CHANNELS_PER_GRANT = 1
    FIELDCOUNT = 10
    FIELDLENGTH = 100
    REPLICATOR2 = "false"
    BASIC_AUTH = "false"
    MEM_CPU_STATS = False
    IMPORT_NODES = 1
    SSL_MODE_SGW = 'none'
    ROUNDTRIP_WRITE_LOAD = "false"
    LOG_STREAMING_TYPE = None
    SG_REPLICATION_TYPE = "push"
    SG_CONFLICT_RESOLUTION = "default"
    SG_READ_LIMIT = 1
    SG_LOADER_THREADS = 50
    SG_DOCLOADER_THREAD = 50
    SG_BLACKHOLEPULLER_CLIENTS = 6
    SG_BLACKHOLEPULLER_USERS = 0
    SG_BLACKHOLEPULLER_TIMEOUT = 600    # in seconds
    SG_BLACKHOLEPULLER_SUBPROTOCOL = 3
    SG_BLACKHOLEPULLER_USE_PROPOSE_CHANGES = False
    SG_DOCSIZE = 10240
    SGTOOL_CHANGEBATCHSET = 200

    REPLICATION_TYPE = None
    REPLICATION_CONCURRENCY = 1
    DELTA_SYNC = ''
    DELTASYNC_CACHEHIT_RATIO = 0
    DOCTYPE = 'simple'
    DOC_DEPTH = '1'
    WRITEALLFIELDS = 'true'
    READALLFIELDS = 'true'
    UPDATEFIELDCOUNT = 1

    E2E = ''
    YCSB_RETRY_COUNT = 5
    YCSB_RETRY_INTERVAL = 1
    CBL_PER_WORKER = 0
    CBL_TARGET = "127.0.0.1"
    RAMDISK_SIZE = 0
    CBL_THROUGHPUT = 0
    SG_LOAD_THROUGHPUT = 0
    COLLECT_CBL_LOGS = 0
    CBL_VERBOSE_LOGGING = 0
    TROUBLEMAKER = None
    COLLECT_SGW_LOGS = 0
    COLLECT_SGW_CONSOLE = 0
    DATA_INTEGRITY = 'false'
    REPLICATION_AUTH = 1
    DEFAULT_RESYNC_NEW_FUNCTION = (
        "function (doc) { channel(doc.channels); access(doc.accessTo, doc.access); }"
    )

    def __init__(self, options: dict):
        self.repo = options.get('ycsb_repo', self.REPO)
        self.branch = options.get('ycsb_branch', self.BRANCH)
        self.ycsb_command = options.get('ycsb_command', self.YCSB_COMMAND)
        self.workload = options.get('workload_path', self.WORKLOAD)
        self.users = options.get('users', self.USERS)
        self.channels = options.get('channels', self.CHANNELS)
        self.channels_per_user = options.get('channels_per_user', self.CHANNLES_PER_USER)
        self.channels_per_doc = options.get('channels_per_doc', self.CHANNELS_PER_DOC)
        self.documents = int(options.get("documents", self.DOCUMENTS))
        self.documents_workset = options.get("documents_workset", self.documents)
        self.documentspull = options.get('documentspull', self.DOCUMENTSPULL)
        self.documentspush = options.get('documentspush', self.DOCUMENTSPUSH)
        self.roundtrip_write = options.get('roundtrip_write', self.ROUNDTRIP_WRITE)
        self.read_mode = options.get('read_mode', self.READ_MODE)
        self.feed_mode = options.get('feed_mode', self.FEED_MODE)
        self.feed_reading_mode = options.get('feed_reading_mode', self.FEED_READING_MODE)
        self.auth = options.get('auth', self.AUTH)
        self.pullproportion = options.get('pullproportion', self.PULLPROPORTION)
        self.pushproportion = options.get('pushproportion', self.PUSHPROPORTION)
        self.readproportion = options.get('readproportion', self.READPROPORTION)
        self.updateproportion = options.get('updateproportion', self.UPDATEPROPORTION)
        self.insertproportion = options.get('insertproportion', self.INSERTPROPORTION)
        self.scanproportion = options.get('scanproportion', self.SCANPROPORTION)
        self.requestdistribution = options.get('requestdistribution', self.REQUESTDISTRIBUTION)
        self.log_title = options.get('log_title', self.LOG_TITE)
        self.instances_per_client = options.get('instances_per_client', 1)
        self.load_instances_per_client = options.get('load_instances_per_client', 1)
        self.instance = options.get('instance', '')
        self.threads_per_instance = 1
        self.load_threads = options.get('load_threads', self.LOAD_THREADS)
        self.threads = options.get('threads', self.THREADS)
        self.insertstart = options.get("insertstart", self.INSERTSTART)
        self.insert_mode = options.get('insert_mode', self.INSERT_MODE)
        self.load_clients = options.get('load_clients', self.LOAD_CLIENTS)
        self.clients = options.get('clients', self.CLIENTS)
        self.nodes = int(options.get('nodes', self.NODES))
        self.starchannel = options.get('starchannel', self.STAR)
        self.grant_access = options.get('grant_access', self.GRANT_ACCESS)
        self.warmup_cache = maybe_atoi(options.get('warmup_cache', "false"))
        self.channels_per_grant = options.get('channels_per_grant', self.CHANNELS_PER_GRANT)
        self.grant_access_in_scan = options.get('grant_access_in_scan', self.GRANT_ACCESS_IN_SCAN)
        self.build_label = options.get('build_label', '')
        self.fieldcount = options.get('fieldcount', self.FIELDCOUNT)
        self.fieldlength = options.get('fieldlength', self.FIELDLENGTH)

        self.replicator2 = options.get('replicator2', self.REPLICATOR2)
        self.basic_auth = options.get('basic_auth', self.BASIC_AUTH)
        self.mem_cpu_stats = options.get('mem_cpu_stats', self.MEM_CPU_STATS)

        self.import_nodes = int(options.get('import_nodes', self.IMPORT_NODES))
        self.ssl_mode_sgw = (options.get('ssl_mode_sgw', self.SSL_MODE_SGW))

        self.roundtrip_write_load = options.get('roundtrip_write_load', self.ROUNDTRIP_WRITE_LOAD)
        # possible options: datadog, sumologic, generic_http
        self.log_streaming = options.get("log_streaming", self.LOG_STREAMING_TYPE)
        self.sg_replication_type = options.get('sg_replication_type', self.SG_REPLICATION_TYPE)
        self.sg_conflict_resolution = options.get('sg_conflict_resolution',
                                                  self.SG_CONFLICT_RESOLUTION)
        self.sg_read_limit = int(options.get('sg_read_limit', self.SG_READ_LIMIT))
        self.sg_loader_threads = int(options.get("sg_loader_threads", self.SG_LOADER_THREADS))
        self.sg_docloader_thread = int(options.get("sg_docloader_thread", self.SG_DOCLOADER_THREAD))
        self.sg_blackholepuller_client = int(options.get("sg_blackholepuller_client",
                                                         self.SG_BLACKHOLEPULLER_CLIENTS))
        self.sg_blackholepuller_users = int(options.get("sg_blackholepuller_users",
                                                        self.SG_BLACKHOLEPULLER_USERS))
        self.sg_blackholepuller_timeout = options.get("sg_blackholepuller_timeout",
                                                      self.SG_BLACKHOLEPULLER_TIMEOUT)
        self.sg_blackholepuller_subprotocol = options.get("sg_blackholepuller_subprotocol",
                                                          self.SG_BLACKHOLEPULLER_SUBPROTOCOL)
        self.sg_blackholepuller_use_propose_changes = bool(options.get(
            "sg_blackholepuller_use_propose_changes",
            self.SG_BLACKHOLEPULLER_USE_PROPOSE_CHANGES,
        ))
        self.sg_docsize = int(options.get("sg_docsize", self.SG_DOCSIZE))
        self.sgtool_changebatchset = int(options.get("sgtool_changebatchset",
                                                     self.SGTOOL_CHANGEBATCHSET))
        self.resync_new_function = options.get("resync_new_function",
                                               self.DEFAULT_RESYNC_NEW_FUNCTION)
        self.db_config_path = options.get("db_config_path", None)

        self.delta_sync = self.DELTA_SYNC
        self.e2e = self.E2E
        self.replication_type = options.get('replication_type', self.REPLICATION_TYPE)
        if self.replication_type:
            if self.replication_type in ["PUSH", "PULL"]:
                self.delta_sync = 'true'
            if self.replication_type in ["E2E_PUSH", "E2E_PULL", "E2E_BIDI"]:
                self.e2e = 'true'
        self.deltasync_cachehit_ratio = options.get(
            'deltasync_cachehit_ratio', self.DELTASYNC_CACHEHIT_RATIO)
        self.replication_concurrency = options.get(
            'replication_concurrency', self.REPLICATION_CONCURRENCY)
        self.doctype = options.get('doctype', self.DOCTYPE)
        self.doc_depth = options.get('doc_depth', self.DOC_DEPTH)
        self.writeallfields = options.get('writeallfields', self.WRITEALLFIELDS)
        self.readallfields = options.get('readallfields', self.READALLFIELDS)
        self.updatefieldcount = options.get('updatefieldcount', self.UPDATEFIELDCOUNT)
        self.ycsb_retry_count = int(options.get('ycsb_retry_count', self.YCSB_RETRY_COUNT))
        self.ycsb_retry_interval = int(options.get('ycsb_retry_interval', self.YCSB_RETRY_INTERVAL))
        self.cbl_per_worker = int(options.get('cbl_per_worker', self.CBL_PER_WORKER))
        self.cbl_target = self.CBL_TARGET
        self.ramdisk_size = int(options.get('ramdisk_size', self.RAMDISK_SIZE))
        self.cbl_throughput = int(options.get('cbl_throughput', self.CBL_THROUGHPUT))
        self.sg_load_throughput = int(options.get('sg_load_throughput', self.SG_LOAD_THROUGHPUT))
        self.collect_cbl_logs = int(options.get('collect_cbl_logs', self.COLLECT_CBL_LOGS))
        self.cbl_verbose_logging = int(options.get('cbl_verbose_logging', self.CBL_VERBOSE_LOGGING))
        self.troublemaker = options.get('troublemaker', self.TROUBLEMAKER)
        self.collect_sgw_logs = int(options.get('collect_sgw_logs', self.COLLECT_SGW_LOGS))
        self.collect_sgw_console = int(options.get('collect_sgw_console', self.COLLECT_SGW_CONSOLE))
        self.data_integrity = options.get('data_integrity', self.DATA_INTEGRITY)
        self.replication_auth = int(options.get('replication_auth', self.REPLICATION_AUTH))

    def __str__(self) -> str:
        return str(self.__dict_)


class DiagEvalSettings:

    DEFAULT_RESTART_DELAY = 5  # seconds

    def __init__(self, options: dict):
        self.restart_delay = int(options.get('restart_delay', self.DEFAULT_RESTART_DELAY))
        payloads = options.get('payloads')
        if payloads:
            self.payloads = payloads.strip().split('\n')
        else:
            self.payloads = None


class VectorDBBenchSettings(PhaseSettings):
    REPO = "https://github.com/couchbaselabs/VectorDBBench.git"
    BRANCH = "couchbase-client"
    DEFAULT_CASE = "Performance1536D500K"

    def __init__(self, options: dict):
        super().__init__(options)
        self.repo = options.get("repo", self.REPO)
        self.branch = options.get("branch", self.BRANCH)
        # The specified branch should have an implementation of specified database
        self.database = options.get("database", "Couchbase")
        # CaseType Enum from `{repo}vectordb_bench/backend/cases.py`
        self.cases = options.get("cases", self.DEFAULT_CASE)
        self.use_shuffled_data = maybe_atoi(options.get("shuffled", "True"))
        self.dataset_local_path = options.get("dataset_path", "/tmp/vectordb_bench/dataset")
        self.index_type = options.get("index_type", "FTS")  # alt: CVI, BHIVE
        # Label and case setting are generated by the test class
        self.label = None
        self.case_settings = {}

    def __str__(self) -> str:
        return str(self.__dict__)


class LoadBalancerSettings:
    """Provides setting to control deployment of application and network load balancer."""

    DEFAULT_LB_SCHEME = "internal"  # alt: internet-facing
    DEFAULT_LBC_CONFIG = {"lbc": "2.9.0", "cert-manager": "1.12.3"}

    def __init__(self, options: dict):
        # A lodbalancer type: nlb | alb. If None, a LB will not be deployed
        self.lb_type = options.get("type")
        self.lb_scheme = options.get("scheme", self.DEFAULT_LB_SCHEME)
        self.create_ingress = maybe_atoi(options.get("create_ingress", "false"))
        # Certificate manager and load-balancer controller versions to install
        lbc_config = options.get("lbc_config")
        if lbc_config:
            self.lbc_config = json.loads(lbc_config)
        else:
            self.lbc_config = self.DEFAULT_LBC_CONFIG


class AIServicesSettings:
    """Provides settings to control deployment of workflow integrations."""

    def __init__(self, options: dict):
        # Generic Workflow settings
        self.workflow_type = options.get("workflow_type", "structured")  # alt: unstructured
        self.schema_fields = options.get("schema_fields", "text-to-embed").split(",")
        # True to manually create the FTS index before running the workflow
        self.create_index = maybe_atoi(options.get("create_index", "true"))

        # Pre-processing settings
        self.chunk_size = int(options.get("chunk_size", 512))  # current min: 256, max: 8192
        # Options: RECURSIVE_SPLITTER | SEMANTIC_SPLITTER
        self.chunking_strategy = options.get("chunking_strategy", "RECURSIVE_SPLITTER").upper()
        pages_str = options.get("pages", "")
        self.pages = [int(p) for p in pages_str.split(",") if p.strip() != ""]
        self.exclusions = [e for e in options.get("exclusions", "").split(",") if e.strip() != ""]

        # Integrations settings
        self.s3_bucket = options.get("s3_bucket", "ai-doclaynet-dataset")
        self.s3_path = options.get("s3_path", "core_pngs")
        self.s3_bucket_region = options.get("s3_bucket_region", "us-east-1")

        # Model settings, for external model not deployed by Capella.
        # These will only be used during testing when `model_source` is external
        self.model_source = options.get("model_source", "internal")  # alt: external
        self.model_name = options.get("model_name", "text-embedding-3-small")
        # Options: capella | openAI | bedrock
        self.model_provider = options.get("provider", "openAI")
        self.model_dimensions = int(options.get("model_dimensions", "1024"))
        # AI functions names
        self.functions_names = options.get("functions_names", "").split(",")

        # WER & F1 calculation settings
        # If enabled WER will be calculated along with F1 score
        self.do_e2e_evaluation = maybe_atoi(options.get("do_e2e_eval", "false"))
        self.gt_file_path = options.get("gt_file_path", "ai-uda-qa/bench_paper_tab_qa.json")

        # Will be fixed as part of CBPS-1490
        self.aws_credential_path = options.get("aws_credential_path", "/root/.ssh")


class AIBenchSettings(PhaseSettings):
    """Provides settings for AI Bench workload generator."""

    REPO = "https://github.com/couchbaselabs/ai_bench.git"
    BRANCH = "main"
    DEFAULT_CONCURRENCIES = "4,32,50"

    def __init__(self, options: dict):
        super().__init__(options)
        self.repo = options.get("repo", self.REPO)
        self.branch = options.get("branch", self.BRANCH)

        self.model_kind = options.get("model_kind")
        self.dataset = options.get("dataset", "random")
        self.subset = options.get("subset", "all")
        self.split = options.get("split")

        # Model serving request parameters
        self.max_tokens = options.get("max_tokens", 256)
        self.best_of = options.get("best_of", 1)
        self.logprobs = options.get("logprobs")
        self.ignore_eos = maybe_atoi(options.get("ignore_eos", "false"))
        self.encoding_format = options.get("encoding_format", "base64")

        concurrencies = options.get("concurrencies", self.DEFAULT_CONCURRENCIES)
        self.concurrencies = [int(c) for c in concurrencies.split(",")]
        # Tool configuration
        self.handler = options.get("handler", "openai")

        self.endpoint = ""  # Individal endpoint to run the workload on
        self.tag = ""
        self.ops = int(self.ops)


class AppTelemetrySettings:
    """Provides settings to control app telemetry."""

    def __init__(self, options: dict):
        self.enabled = maybe_atoi(options.get("enabled", "false"))
        self.scrape_interval = int(options.get("scrape_interval", "60"))  # seconds
        self.max_clients_per_node = int(options.get("max_clients_per_node", "1024"))


class MigrationSettings(RebalanceSettings):
    """Provides settings to control cluster migration.

    If enabled, the k8s-managed cluster will be created from an on-premise source cluster.
    """

    START_AFTER = 180
    STOP_AFTER = 30

    def __init__(self, options: dict):
        super().__init__(options)
        self.enabled = maybe_atoi(options.get("enabled", "false"))
        self.source_cluster = options.get("migration_source_cluster")
        self.num_unmanaged_nodes = int(
            options.get("num_unmanaged_nodes", 0)
        )  # 0 means migrate all nodes
        self.max_concurrent_migrations = int(options.get("max_concurrent_migrations", 1))
        # Time in seconds to wait for migration to complete before considering it failed
        # This may defer based on the size of the cluster
        self.migration_timeout_seconds = int(options.get("migration_timeout_seconds", 2700))


class TestConfig(Config):

    def _configure_phase_settings(method):  # noqa: N805
        """Decorate phase settings properties to configure them."""

        def wrapper(self):
            phase_settings = method(self)
            phase_settings.configure(self)
            return phase_settings

        return wrapper

    @property
    def test_case(self) -> TestCaseSettings:
        options = self._get_options_as_dict('test_case')
        return TestCaseSettings(options)

    @property
    def showfast(self) -> ShowFastSettings:
        options = self._get_options_as_dict('showfast')
        return ShowFastSettings(options)

    @property
    def deployment(self) -> DeploymentSettings:
        options = self._get_options_as_dict('deployment')
        return DeploymentSettings(options)

    @property
    def cluster(self) -> ClusterSettings:
        options = self._get_options_as_dict('cluster')
        return ClusterSettings(options)

    @property
    def systemd_env_vars(self) -> dict:
        options = self._get_options_as_dict("systemd_env_var")

        if num_vbuckets := self.cluster.num_vbuckets:
            options["COUCHBASE_NUM_VBUCKETS"] = num_vbuckets

        return options

    @property
    def systemd_limits(self) -> SystemdLimitsSettings:
        """Specify systemd service resource limits for Couchbase Server.

        Limits can be specified per MDS service, but will be applied per *node*, so it left to the
        user to ensure that limits don't conflict if multiple services are running on the same node.

        Options and values are exactly as defined for cgroups v2:
        https://man7.org/linux/man-pages/man5/systemd.resource-control.5.html

        Example usage:
        ```
        [systemd_limit]
        # limits for all nodes
        AllowedCPUs = 0-7

        [systemd_limit:index]
        # limits for nodes running index service
        MemoryMax = 20G

        [systemd_limit:kv,index]
        # limits for nodes running kv OR index service
        IOReadBandwidthMax = /data 100M
        IOWriteBandwidthMax = /data 100M
        ```
        """
        options = {}
        for section in self.config.sections():
            if section.startswith("systemd_limit"):
                services = (
                    section.split(":")[-1].split(",")
                    if ":" in section
                    else [SystemdLimitsSettings.UNIVERSAL_KEY]
                )
                for service in services:
                    key = service.strip()
                    if key not in options:
                        options[key] = {}
                    options[key] |= self._get_options_as_dict(section)
        return SystemdLimitsSettings(options)

    @property
    def bucket(self) -> BucketSettings:
        options = self._get_options_as_dict('bucket')
        return BucketSettings(options)

    @property
    def collection(self) -> CollectionSettings:
        options = self._get_options_as_dict('collection')
        settings = CollectionSettings(options, self.buckets)
        return settings

    @property
    def users(self) -> UserSettings:
        options = self._get_options_as_dict('users')
        return UserSettings(options)

    @property
    def diag_eval(self) -> DiagEvalSettings:
        """Specify arbitrary diag/eval payload to be run during cluster configuration.

        This can be specified as follows
        ```
        [diag_eval]
        payloads =
                  ns_config:set(option, value).
                  ns_config:command(option, value).
        restart_delay = N
        ```
        Multiple command can also be specified on one line separated by comma.
        """
        options = self._get_options_as_dict('diag_eval')
        return DiagEvalSettings(options)

    @property
    def bucket_extras(self) -> dict:
        bucket_extras = self._get_options_as_dict('bucket_extras')
        options = self._get_options_as_dict('access')
        access = AccessSettings(options)
        if access.durability_set:
            if "num_writer_threads" not in bucket_extras:
                bucket_extras["num_writer_threads"] = "disk_io_optimized"
        return bucket_extras

    @property
    def buckets(self) -> list[str]:
        if self.cluster.num_buckets == 1 and self.cluster.bucket_name != "bucket-1":
            return [self.cluster.bucket_name]
        elif self.cluster.num_buckets != 1 and isinstance(self.cluster.bucket_name, list):
            return self.cluster.bucket_name
        else:
            return [
                'bucket-{}'.format(i + 1) for i in range(self.cluster.num_buckets)
            ]

    @property
    def eventing_buckets(self) -> list[str]:
        return [
            'eventing-bucket-{}'.format(i + 1) for i in range(self.cluster.eventing_buckets)
        ]

    @property
    def eventing_metadata_bucket(self) -> list[str]:
        return [
            'eventing'
        ]

    @property
    def conflict_buckets(self) -> list[str]:
        return [
            'conflict-bucket-{}'.format(i + 1) for i in range(self.cluster.conflict_buckets)
        ]

    @property
    def compaction(self) -> CompactionSettings:
        options = self._get_options_as_dict('compaction')
        return CompactionSettings(options)

    @property
    def restore_settings(self) -> RestoreSettings:
        options = self._get_options_as_dict('restore')
        settings = RestoreSettings(options)
        settings.env_vars |= self.profiling_settings.get_cbbackupmgr_profiling_env("restore")
        return settings

    @property
    def import_settings(self) -> ImportSettings:
        options = self._get_options_as_dict('import')
        return ImportSettings(options)

    @property
    @_configure_phase_settings
    def load_settings(self):
        load_options = self._get_options_as_dict('load')
        load_settings = LoadSettings(load_options)
        return load_settings

    @property
    def mixed_load_settings(self) -> list[LoadSettings]:
        base_settings = self.load_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('load', base_settings)
        return mix

    @property
    @_configure_phase_settings
    def hot_load_settings(self) -> HotLoadSettings:
        options = self._get_options_as_dict('hot_load')
        hot_load = HotLoadSettings(options)
        return hot_load

    @property
    def mixed_hot_load_settings(self) -> list[HotLoadSettings]:
        base_settings = self.hot_load_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('hot_load', base_settings)
        return mix

    @property
    @_configure_phase_settings
    def xattr_load_settings(self) -> XattrLoadSettings:
        options = self._get_options_as_dict('xattr_load')
        xattr_settings = XattrLoadSettings(options)
        return xattr_settings

    @property
    def mixed_xattr_load_settings(self) -> list[XattrLoadSettings]:
        base_settings = self.xattr_load_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('xattr_load', base_settings)
        return mix

    @property
    def xdcr_settings(self) -> XDCRSettings:
        options = self._get_options_as_dict('xdcr')
        return XDCRSettings(options)

    @property
    def views_settings(self) -> ViewsSettings:
        options = self._get_options_as_dict('views')
        return ViewsSettings(options)

    @property
    def gsi_settings(self) -> GSISettings:
        options = self._get_options_as_dict('secondary')
        return GSISettings(options)

    @property
    def dcp_settings(self) -> DCPSettings:
        options = self._get_options_as_dict('dcp')
        return DCPSettings(options)

    @property
    def index_settings(self) -> IndexSettings:
        options = self._get_options_as_dict('index')
        collection_settings = self.collection
        if collection_settings.collection_map is not None:
            options['collection_map'] = collection_settings.collection_map
        return IndexSettings(options)

    @property
    def n1ql_function_settings(self) -> N1QLFunctionSettings:
        options = self._get_options_as_dict('n1ql_function')
        return N1QLFunctionSettings(options)

    @property
    def n1ql_settings(self) -> N1QLSettings:
        options = self._get_options_as_dict('n1ql')
        return N1QLSettings(options)

    @property
    def backup_settings(self) -> BackupSettings:
        options = self._get_options_as_dict('backup')
        settings = BackupSettings(options)
        settings.env_vars |= self.profiling_settings.get_cbbackupmgr_profiling_env("backup")
        return settings

    @property
    def export_settings(self) -> ExportSettings:
        options = self._get_options_as_dict('export')
        return ExportSettings(options)

    @property
    @_configure_phase_settings
    def access_settings(self) -> AccessSettings:
        options = self._get_options_as_dict('access')
        access = AccessSettings(options)
        return access

    @property
    def mixed_access_settings(self) -> list[AccessSettings]:
        base_settings = self.access_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('access', base_settings)
        return mix

    @property
    @_configure_phase_settings
    def extra_access_settings(self) -> ExtraAccessSettings:
        options = self._get_options_as_dict('extra_access')
        extra_access = ExtraAccessSettings(options)
        return extra_access

    @property
    def mixed_extra_access_settings(self) -> list[ExtraAccessSettings]:
        base_settings = self.extra_access_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('extra_access', base_settings)
        return mix

    @property
    def rebalance_settings(self) -> RebalanceSettings:
        options = self._get_options_as_dict('rebalance')
        return RebalanceSettings(options)

    @property
    def upgrade_settings(self) -> UpgradeSettings:
        options = self._get_options_as_dict("upgrade")
        return UpgradeSettings(options)

    @property
    def stats_settings(self) -> StatsSettings:
        options = self._get_options_as_dict('stats')
        return StatsSettings(options)

    @property
    def profiling_settings(self) -> ProfilingSettings:
        options = self._get_options_as_dict('profiling')
        return ProfilingSettings(options)

    @property
    def internal_settings(self) -> dict:
        return self._get_options_as_dict('internal')

    @property
    def xdcr_cluster_settings(self) -> dict:
        return self._get_options_as_dict('xdcr_cluster')

    @property
    def jts_access_settings(self) -> JTSAccessSettings:
        options = self._get_options_as_dict('jts')
        return JTSAccessSettings(options)

    @property
    def ycsb_settings(self) -> YCSBSettings:
        options = self._get_options_as_dict('ycsb')
        return YCSBSettings(options)

    @property
    def sdktesting_settings(self) -> SDKTestingSettings:
        options = self._get_options_as_dict('sdktesting')
        return SDKTestingSettings(options)

    @property
    def eventing_settings(self) -> EventingSettings:
        options = self._get_options_as_dict('eventing')
        return EventingSettings(options)

    @property
    def magma_settings(self) -> MagmaSettings:
        options = self._get_options_as_dict('magma')
        return MagmaSettings(options)

    @property
    def analytics_settings(self) -> AnalyticsSettings:
        options = self._get_options_as_dict('analytics')
        return AnalyticsSettings(options)

    @property
    def columnar_kafka_links_settings(self) -> ColumnarKafkaLinksSettings:
        options = self._get_options_as_dict("columnar_kafka_links")
        return ColumnarKafkaLinksSettings(options)

    @property
    def columnar_copy_to_settings(self) -> ColumnarCopyToSettings:
        options = self._get_options_as_dict("columnar_copy_to")
        return ColumnarCopyToSettings(options)

    @property
    def columnar_settings(self) -> ColumnarSettings:
        options = self._get_options_as_dict("columnar")
        return ColumnarSettings(options)

    @property
    def audit_settings(self) -> AuditSettings:
        options = self._get_options_as_dict('audit')
        return AuditSettings(options)

    @property
    def sgw_audit_settings(self) -> SGWAuditSettings:
        options = self._get_options_as_dict("sgw-audit")
        return SGWAuditSettings(options)

    def get_n1ql_query_definition(self, query_name: str) -> dict:
        return self._get_options_as_dict('n1ql-{}'.format(query_name))

    def get_sever_group_definition(self, server_group_name: str) -> dict:
        return self._get_options_as_dict('sg-{}'.format(server_group_name))

    @property
    def fio(self) -> dict:
        return self._get_options_as_dict('fio')

    @property
    def java_dcp_settings(self) -> JavaDCPSettings:
        options = self._get_options_as_dict('java_dcp')
        return JavaDCPSettings(options)

    @property
    def dcpdrain_settings(self) -> DCPDrainSettings:
        """
        Return a DCPDrainSettings instance.

        Constructed from the [dcpdrain] section of the .test file.
        """
        options = self._get_options_as_dict('dcpdrain')
        return DCPDrainSettings(options)

    @property
    def client_settings(self) -> ClientSettings:
        options = self._get_options_as_dict('clients')
        return ClientSettings(options)

    @property
    def magma_benchmark_settings(self) -> MagmaBenchmarkSettings:
        options = self._get_options_as_dict('magma_benchmark')
        return MagmaBenchmarkSettings(options)

    @property
    def tpcds_loader_settings(self) -> TPCDSLoaderSettings:
        options = self._get_options_as_dict('TPCDSLoader')
        return TPCDSLoaderSettings(options)

    @property
    def ch2_settings(self) -> CH2:
        options = self._get_options_as_dict('ch2')
        return CH2(options)

    @property
    def ch3_settings(self) -> CH3:
        options = self._get_options_as_dict('ch3')
        return CH3(options)

    @property
    def pytpcc_settings(self) -> PYTPCCSettings:
        options = self._get_options_as_dict('py_tpcc')
        return PYTPCCSettings(options)

    @property
    def autoscaling_setting(self) -> AutoscalingSettings:
        options = self._get_options_as_dict('autoscaling')
        return AutoscalingSettings(options)

    @property
    def tableau_settings(self) -> TableauSettings:
        options = self._get_options_as_dict('tableau')
        return TableauSettings(options)

    @property
    def syncgateway_settings(self) -> SyncgatewaySettings:
        options = self._get_options_as_dict('syncgateway')
        return SyncgatewaySettings(options)

    @property
    def vectordb_settings(self) -> VectorDBBenchSettings:
        options = self._get_options_as_dict("vectordb")
        return VectorDBBenchSettings(options)

    @property
    def migration_settings(self) -> MigrationSettings:
        options = self._get_options_as_dict("migration")
        return MigrationSettings(options)

    def _get_mixed_phase_settings(self, base_section, base_settings):
        settings_cls = type(base_settings)
        mix = []

        for section in base_settings.workload_mix:
            phase_options = self._get_options_as_dict(base_section)
            override_options = self._get_options_as_dict('{}-{}'.format(base_section, section))
            phase_options.update(override_options)
            phase = settings_cls(phase_options)
            phase.configure(self)
            phase.workload_name = section
            mix.append(phase)

        return mix

    @property
    def load_balancer_settings (self) -> LoadBalancerSettings:
        options = self._get_options_as_dict("load_balancer")
        return LoadBalancerSettings(options)

    @property
    def ai_services_settings(self) -> AIServicesSettings:
        options = self._get_options_as_dict("ai_services")
        return AIServicesSettings(options)

    @property
    def aibench_settings(self) -> AIBenchSettings:
        options = self._get_options_as_dict("ai_bench")
        return AIBenchSettings(options)

    @property
    def app_telemetry_settings(self) -> AppTelemetrySettings:
        options = self._get_options_as_dict("telemetry")
        return AppTelemetrySettings(options)


class TargetSettings:

    def __init__(self, host: str, bucket: str, username: str, password: str,
                 prefix: str = None, cloud: dict = {}):
        self.password = password
        self.node = host
        self.bucket = bucket
        self.prefix = prefix
        self.cloud = cloud
        self.username = username

    @property
    def connection_string(self) -> str:
        return 'couchbase://{username}:{password}@{host}/{bucket}'.format(
            username=self.username,
            password=self.password,
            host=self.node,
            bucket=self.bucket,
        )


class AIGatewayTargetSettings(TargetSettings):
    def __init__(
        self,
        host: str,
        bucket: str,
        endpoint: str,
        api_key: str,
        prefix: str = None,
        cloud: dict = {},
    ):
        super().__init__(host, bucket, "", "", prefix, cloud)
        self.gateway_endpoint = endpoint
        self.api_key = api_key


class TargetIterator(Iterable):
    def __init__(
        self,
        cluster_spec: ClusterSpec,
        test_config: TestConfig,
        prefix: str = None,
        buckets: Iterable[str] = None,
        target_svc: Optional[str] = None,
    ):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.prefix = prefix
        self.buckets = list(buckets) if buckets else self.test_config.buckets
        self.target_svc = target_svc
        if not target_svc and len(cluster_spec.clusters_modified_names) > 0:
            self.target_svc = cluster_spec.clusters_modified_names[0]

    def __iter__(self) -> Iterator[TargetSettings]:
        username = self.cluster_spec.rest_credentials[0]
        if self.test_config.client_settings.python_client:
            if self.test_config.client_settings.python_client.split('.')[0] == "2":
                password = self.test_config.bucket.password
            else:
                password = self.cluster_spec.rest_credentials[1]
        else:
            password = self.cluster_spec.rest_credentials[1]

        prefix = self.prefix

        for master in self.cluster_spec.masters:
            if self.prefix is None:
                prefix = target_hash(master)

            for bucket in self.buckets:
                if "perfrunner.tests.views" in self.test_config.test_case.test_module:
                    username = bucket
                    password = self.test_config.bucket.password

                cloud = {}

                if self.cluster_spec.dynamic_infrastructure:
                    cloud = {"cluster_svc": self.target_svc}

                yield TargetSettings(
                    host=master,
                    bucket=bucket,
                    username=username,
                    password=password,
                    prefix=prefix,
                    cloud=cloud,
                )

#!/usr/bin/env python
import json
import os
from argparse import ArgumentParser, Namespace
from collections import Counter
from time import sleep, time
from typing import Any, Dict, Optional, Sequence
from uuid import uuid4

import requests
from capella.columnar.CapellaAPI import CapellaAPI as CapellaAPIColumnar
from capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIDedicated
from capella.serverless.CapellaAPI import CapellaAPI as CapellaAPIServerless
from fabric.api import local
from requests.exceptions import HTTPError

from logger import logger
from perfrunner.helpers.misc import maybe_atoi, pretty_dict, remove_nulls, run_local_shell_command
from perfrunner.settings import TIMING_FILE, ClusterSpec, TestConfig

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
        'aws': {
            'clusters': {
                'x86_64': 'perf-server-x86-ubuntu20-2023-07',  # ami-08d83f4b122efb564
                'arm': 'perf-server-arm-us-east',  # ami-0f249abfe3dd01b30
                'al2': 'perf-server-al_x86-2022-03-us-east',  # ami-060e286353d227c32
            },
            'clients': 'perf-client-x86-ubuntu20-2023-06-v3',  # ami-0d9789eef66732b62
            'utilities': 'perf-broker-us-east',  # ami-0d9e5ee360aa02d94
            'syncgateways': 'perf-server-x86-ubuntu20-2023-07',  # ami-08d83f4b122efb564
            'kafka_brokers': 'perf-client-x86-ubuntu20-2023-06-v3'
        },
        'gcp': {
            'clusters': 'perftest-server-x86-ubuntu20-2023-07',
            'clients': 'perftest-client-x86-ubuntu20-2023-06-v3',
            'utilities': 'perftest-broker-disk-image',
            'syncgateways': 'perftest-server-x86-ubuntu20-2023-07'
        },
        'azure': {
            'clusters': 'perf-server-x86-ubuntu20-image-def',
            'clients': 'perf-client-x86-ubuntu20-image-def',
            'utilities': 'perf-broker-image-def',
            'syncgateways': 'perf-server-x86-ubuntu20-image-def'
        }
    }

    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        self.options = options
        self.cluster_spec = cluster_spec
        self.csp = self.cluster_spec.cloud_provider
        self.uuid = self.cluster_spec.infrastructure_settings.get('uuid', uuid4().hex[:6])
        self.os_arch = self.cluster_spec.infrastructure_settings.get('os_arch', 'x86_64')
        self.all_nodes = {
            'clusters': {
                k: v.strip().split()
                for k, v in self.cluster_spec.vm_clusters_raw.items()
            },
            'clients': {
                k: v.strip().split()
                for k, v in self.cluster_spec.infrastructure_clients.items()
            },
            'utilities': {
                k: v.strip().split()
                for k, v in self.cluster_spec.infrastructure_utilities.items()
            },
            'syncgateways': {
                k: v.strip().split()
                for k, v in self.cluster_spec.infrastructure_syncgateways.items()
            },
            'kafka_brokers': {
                k: v.strip().split()
                for k, v in self.cluster_spec.infrastructure_kafka_clusters.items()
            }
        }

        self.cloud_storage = bool(
            int(self.cluster_spec.infrastructure_settings.get('cloud_storage', 0))
        )

        if self.csp == 'gcp':
            self.zone = self.options.zone
            self.region = self.zone.rsplit('-', 1)[0]
        else:
            self.zone = None
            if self.csp == 'aws':
                self.region = self.options.aws_region
            else:
                self.region = self.options.azure_region

    def deploy(self):
        self.populate_tfvars()
        self.terraform_init(self.csp)

        self.terraform_apply(self.csp)

        output = self.terraform_output(self.csp)
        self.update_spec(output)

    def destroy(self):
        self.terraform_destroy(self.csp)

    def create_tfvar_nodes(self) -> Dict[str, Dict]:
        tfvar_nodes = {
            'clusters': {},
            'clients': {},
            'utilities': {},
            'syncgateways': {},
            'kafka_brokers': {}
        }

        for role, node_lists in self.all_nodes.items():
            i = 0
            for label, nodes in node_lists.items():
                for node in nodes:
                    node_group = node.split(':')[0]
                    params = self.cluster_spec.infrastructure_config()[node_group.split('.')[0]]

                    params['node_group'] = '{}.{}'.format(label, node_group)

                    # Try getting image name from cli option
                    image = getattr(self.options, '{}_image'.format({
                        'clusters': 'cluster',
                        'clients': 'client',
                        'utilities': 'utility',
                        'syncgateways': 'sgw',
                        'kafka_brokers': 'kafka'
                    }[role]))

                    # If image name isn't provided as cli param, use hardcoded defaults
                    if image is None:
                        image = self.IMAGE_MAP[self.csp][role]
                        if self.csp == 'aws' and role == 'clusters':
                            image = image.get(self.os_arch, image['x86_64'])

                    params['image'] = image

                    # Set disk size and type
                    params['storage_size'] = int(params.get('storage_size', 0))

                    if not (storage_type := params.get('storage_type', None)):
                        logger.interrupt('Storage type not specified for node group {}')
                    params['storage_type'] = storage_type

                    # Set CSP-specific options
                    if self.csp == 'azure':
                        params['disk_tier'] = params.get('disk_tier', '')
                    else:
                        # AWS and GCP
                        params['iops'] = int(params.get('iops', 0))
                        if self.csp == 'aws':
                            params['storage_throughput'] = int(params.get('storage_throughput', 0))
                        else:
                            # GCP
                            params['local_nvmes'] = int(params.get('local_nvmes', 0))

                    if 'instance_capacity' in params:
                        del params['instance_capacity']

                    tfvar_nodes[role][str(i := i+1)] = params

        return tfvar_nodes

    def populate_tfvars(self) -> bool:
        logger.info('Setting tfvars')
        global_tag = self.options.tag if self.options.tag else ''
        if self.csp.lower() == 'gcp':
            # GCP doesn't allow uppercase letters in tags
            global_tag = global_tag.lower()

        tfvar_nodes = self.create_tfvar_nodes()

        if not any(tfvar_nodes.values()):
            logger.warn('Nothing to deploy with Terraform.')
            return False

        replacements = {
            '<CLOUD_REGION>': self.region,
            '<CLUSTER_NODES>': tfvar_nodes['clusters'],
            '<CLIENT_NODES>': tfvar_nodes['clients'],
            '<UTILITY_NODES>': tfvar_nodes['utilities'],
            '<SYNCGATEWAY_NODES>': tfvar_nodes['syncgateways'],
            '<KAFKA_NODES>': tfvar_nodes['kafka_brokers'],
            '<CLOUD_STORAGE>': self.cloud_storage,
            '<GLOBAL_TAG>': global_tag,
            '<UUID>': self.uuid
        }

        if self.zone:
            replacements['<CLOUD_ZONE>'] = self.zone

        with open('terraform/{}/terraform.tfvars'.format(self.csp), 'r+') as tfvars:
            file_string = tfvars.read()

            for k, v in replacements.items():
                file_string = file_string.replace(k, json.dumps(v, indent=4))

            tfvars.seek(0)
            tfvars.write(file_string)

        return True

    # Initializes terraform environment.
    def terraform_init(self, csp: str):
        logger.info('Initializating Terraform (terraform init)')
        local('cd terraform/{} && terraform init >> terraform.log'.format(csp))

    # Apply and output terraform deployment.
    def terraform_apply(self, csp: str):
        logger.info('Building and executing Terraform deployment plan '
                    '(terraform plan, terraform apply)')
        local('cd terraform/{} && '
              'terraform plan -out tfplan.out >> terraform.log && '
              'terraform apply -auto-approve tfplan.out'
              .format(csp))

    def terraform_output(self, csp: str) -> dict[str, Any]:
        output = json.loads(
            local('cd terraform/{} && terraform output -json'.format(csp), capture=True)
        )
        return output

    def terraform_destroy(self, csp: str):
        logger.info('Building and executing Terraform destruction plan '
                    '(terraform plan -destroy, terraform apply)')
        _, _, returncode = run_local_shell_command('cd terraform/{} && terraform validate'
                                                   .format(csp), quiet=True)
        if returncode != 0:
            logger.info('No resources to destroy with Terraform.')
            return

        local('cd terraform/{} && '
              'terraform plan -destroy -out tfplan_destroy.out >> terraform.log && '
              'terraform apply -auto-approve tfplan_destroy.out'
              .format(csp))

    # Update spec file with deployed infrastructure.
    def update_spec(self, output: str):
        sections = ['cb.selfmanaged.vm', 'clients', 'utilities', 'syncgateways', 'kafka_clusters']
        cluster_dicts = [
            self.cluster_spec.vm_clusters_raw,
            self.cluster_spec.infrastructure_clients,
            self.cluster_spec.infrastructure_utilities,
            self.cluster_spec.infrastructure_syncgateways,
            self.cluster_spec.infrastructure_kafka_clusters,
        ]
        output_keys = [
            'cluster_instance_ips',
            'client_instance_ips',
            'utility_instance_ips',
            'syncgateway_instance_ips',
            'kafka_instance_ips'
        ]
        private_sections = [
            'cluster_private_ips',
            'client_private_ips',
            'utility_private_ips',
            'syncgateway_private_ips',
            'kafka_private_ips'
        ]

        for section, cluster_dict, output_key, private_section in zip(sections,
                                                                      cluster_dicts,
                                                                      output_keys,
                                                                      private_sections):
            if section not in self.cluster_spec.config:
                continue

            for cluster, nodes in cluster_dict.items():
                node_list = nodes.strip().split()
                node_info = {'public_ips': [None for _ in node_list],
                             'private_ips': [None for _ in node_list]}

                if section == 'kafka_clusters':
                    node_info['subnet_ids'] = [None for _ in node_list]

                for info in output[output_key]['value'].values():
                    node_group = info['node_group']
                    for i, node in enumerate(node_list):
                        hostname, *extras = node.split(':', maxsplit=1)
                        if '{}.{}'.format(cluster, hostname) == node_group:
                            public_ip = info['public_ip']
                            if extras:
                                public_ip += ':{}'.format(*extras)
                            node_info['public_ips'][i] = public_ip

                            if 'private_ip' in info:
                                node_info['private_ips'][i] = info['private_ip']

                            if section == 'kafka_clusters' and 'subnet_id' in info:
                                node_info['subnet_ids'][i] = info['subnet_id']
                            break

                self.cluster_spec.config.set(section, cluster,
                                           '\n' + '\n'.join(node_info['public_ips']))

                if any((private_ips := node_info['private_ips'])):
                    if private_section not in self.cluster_spec.config:
                        self.cluster_spec.config.add_section(private_section)
                    self.cluster_spec.config.set(private_section, cluster,
                                               '\n' + '\n'.join(private_ips))

                if 'subnet_ids' in node_info:
                    if (section := 'kafka_subnet_ids') not in self.cluster_spec.config:
                        self.cluster_spec.config.add_section(section)
                    self.cluster_spec.config.set(section, cluster,
                                               '\n' + '\n'.join(node_info['subnet_ids']))

        if self.cloud_storage:
            cloud_storage_info = output['cloud_storage']['value']
            bucket_url = cloud_storage_info['storage_bucket']
            self.cluster_spec.config.set('storage', 'backup', bucket_url)
            if self.csp == 'azure':
                storage_acc = cloud_storage_info['storage_account']
                self.cluster_spec.config.set('storage', 'storage_acc', storage_acc)

        self.cluster_spec.config.set('infrastructure', 'uuid', self.uuid)

        cloud_network_section = 'cloud_network'
        self.cluster_spec.config.add_section(cloud_network_section)
        for k, v in output['network']['value'].items():
            self.cluster_spec.config.set(cloud_network_section, k, v)

        self.cluster_spec.update_config_file()

        with open('cloud/infrastructure/cloud.ini', 'r+') as f:
            s = f.read()
            s = s.replace("server_list", "\n".join(self.cluster_spec.servers))
            s = s.replace("worker_list", "\n".join(self.cluster_spec.clients))
            if self.cluster_spec.sgw_servers:
                s = s.replace("sgw_list", "\n".join(self.cluster_spec.sgw_servers))
            if self.cluster_spec.kafka_brokers:
                s = s.replace("kafka_broker_list", "\n".join(self.cluster_spec.kafka_brokers))
            f.seek(0)
            f.write(s)


class ControlPlanePreparer:

    def __init__(
        self,
        cluster_spec: ClusterSpec,
        public_api_url: Optional[str] = None,
        org_id: Optional[str] = None,
        project_id: Optional[str] = None,
        deployment_tag: Optional[str] = None
    ):
        logger.info('Control plane preparer initialising...')
        self.cluster_spec = cluster_spec
        self.org_id = org_id
        self.project_id = project_id
        self.tag = deployment_tag
        self.uuid = self.cluster_spec.infrastructure_settings.get('uuid', uuid4().hex[:6])

        if 'controlplane' not in self.cluster_spec.config:
            self.cluster_spec.config.add_section('controlplane')

        if public_api_url is None:
            logger.info('No Capella public API URL provided. '
                        'Looking for Capella environment info in cluster spec.')

            if (env := self.cluster_spec.controlplane_settings.get('env')):
                public_api_url = 'https://cloudapi.{}.nonprod-project-avengers.com'.format(env)
            else:
                logger.interrupt('No Capella environment info specified in cluster spec. '
                                 'Cannot determine Capella public API URL to proceed.')
        else:
            logger.info('Capella public API URL provided. '
                        'Saving Capella environment info to cluster spec.')

            env = public_api_url.removeprefix('https://')\
                                .removesuffix('.nonprod-project-avengers.com')\
                                .split('.', 1)[1]
            self.cluster_spec.config.set('controlplane', 'env', env)
            self.cluster_spec.update_config_file()

        logger.info('Using Capella public API URL: {}'.format(public_api_url))

        self.api_client = CapellaAPIDedicated(
            public_api_url,
            None,
            None,
            os.getenv('CBC_USER'),
            os.getenv('CBC_PWD'),
            os.getenv('CBC_TOKEN_FOR_INTERNAL_SUPPORT')
        )
        logger.info('Control plane preparer initialised.')

    def save_project_id(self, create_project_if_none: bool = True):
        project_created = False

        if self.project_id is None:
            logger.info('No project ID set. Looking for project ID in cluster spec.')
            if (project_id := self.cluster_spec.controlplane_settings.get('project')) is None:
                logger.info('No project ID found in cluster spec.')

                if create_project_if_none:
                    project_name = self.tag or 'perf-{}'.format(self.uuid)
                    project_id = self._create_project(self.org_id, project_name)
                    project_created = True
            else:
                logger.info('Found project ID in cluster spec')

            self.project_id = project_id
        else:
            logger.info('Project ID already set.')

        logger.info('Saving project ID in cluster spec file.')
        self.cluster_spec.config.set('controlplane', 'project', self.project_id)
        if project_created:
            self.cluster_spec.config.set('controlplane', 'project_created', '1')
        self.cluster_spec.update_config_file()

    def save_org_id(self):
        if self.org_id is None:
            logger.info('No organization ID set. Looking for organization ID in cluster spec.')
            if (org := self.cluster_spec.controlplane_settings.get('org')) is None:
                logger.info('No organization ID found in cluster spec.')
                org = self._find_org()
            else:
                logger.info('Found organization ID in cluster spec.')

            self.org_id = org
        else:
            logger.info('Organization ID already set.')

        logger.info('Saving organization ID in cluster spec file.')
        self.cluster_spec.config.set('controlplane', 'org', self.org_id)
        self.cluster_spec.update_config_file()

    @staticmethod
    def api_keys_set_in_file() -> bool:
        if os.path.isfile(CAPELLA_CREDS_FILE):
            with open(CAPELLA_CREDS_FILE, 'r') as f:
                creds = json.load(f)
                access_key = creds.get('access')
                secret_key = creds.get('secret')
                return bool(access_key and secret_key)

        logger.info('No creds file found.')
        return False

    def get_api_keys(self):
        logger.info('Getting control plane API keys...')

        access_key = os.getenv('CBC_ACCESS_KEY')
        secret_key = os.getenv('CBC_SECRET_KEY')

        if not bool(access_key and secret_key):
            logger.info('No control plane API keys set as env vars. '
                        'Looking for API keys in creds file.')

            if not self.api_keys_set_in_file():
                logger.info('No control plane API keys found in creds file.')

                key_name = self.tag or 'perf-{}'.format(self.uuid)
                key_id, access_key, secret_key = self._generate_api_key(self.org_id, key_name)
                creds = {'id': key_id, 'access': access_key, 'secret': secret_key}
                with open(CAPELLA_CREDS_FILE, 'w') as f:
                    json.dump(creds, f)
                logger.info('Saved API keys in creds file.')
            else:
                logger.info('Found control plane API keys in creds file.')
        else:
            logger.info('Found control plane API keys set as env vars.')

    def _find_org(self) -> str:
        logger.info('Finding an accessible organization...')
        resp = self.api_client.list_accessible_tenants()
        raise_for_status(resp)
        org = resp.json()[0]['id']
        logger.info('Found organization ID: {}'.format(org))
        return org

    def _generate_api_key(self, org_id: str, name: str) -> tuple[str, str, str]:
        logger.info('Generating control plane API keys in organization {}...'.format(org_id))
        resp = self.api_client.create_access_secret_key(name, org_id)
        raise_for_status(resp)
        payload = resp.json()
        return payload['id'], payload['access'], payload['secret']

    def _create_project(self, org_id: str, name: str) -> str:
        logger.info('Creating project in organization {}...'.format(org_id))
        resp = self.api_client.create_project(org_id, name)
        raise_for_status(resp)
        project = resp.json()['id']
        return project

    def _destroy_project(self, org_id: str, project_id: str):
        logger.info('Deleting project {} from organization {}...'.format(project_id, org_id))
        resp = self.api_client.delete_project(org_id, project_id)
        if resp.status_code == 400 and resp.json()['errorType'] == 'DeleteProjectsWithDatabases':
            logger.warn('Cannot delete project because there are still databases present.')
            return
        else:
            raise_for_status(resp)
        logger.info('Project successfully queued for deletion.')

    def clean_up_project(self):
        if int(self.cluster_spec.controlplane_settings.get('project_created', 0)):
            self._destroy_project(self.org_id, self.project_id)

    def clean_up_api_keys(self):
        if os.path.isfile(CAPELLA_CREDS_FILE):
            logger.info('Revoking API key...')
            with open(CAPELLA_CREDS_FILE, 'r') as f:
                creds = json.load(f)
                self.api_client.revoke_access_secret_key(self.org_id, creds['id'])
            os.remove(CAPELLA_CREDS_FILE)


class CapellaProvisionedDeployer(CloudVMDeployer):

    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        super().__init__(cluster_spec, options)

        self.all_nodes['clusters'] = {
            k: v.strip().split()
            for k, v in self.cluster_spec.capella_provisioned_clusters_raw.items()
        }

        self.client_network_info = self.cluster_spec.infrastructure_section('cloud_network')

        self.test_config = None
        if options.test_config:
            test_config = TestConfig()
            test_config.parse(options.test_config, override=options.override)
            self.test_config = test_config

        if self.test_config and self.test_config.rebalance_settings.nodes_after:
            self.all_nodes['clusters'] = {
                k: v.strip().split()[:init_num]
                for (k, v), init_num in zip(self.cluster_spec.infrastructure_clusters.items(),
                                            self.test_config.cluster.initial_nodes)
            }

        self.api_client = CapellaAPIDedicated(
            'https://cloudapi.{}.nonprod-project-avengers.com'.format(
                self.cluster_spec.controlplane_settings['env']
            ),
            None,
            None,
            os.getenv('CBC_USER'),
            os.getenv('CBC_PWD'),
            os.getenv('CBC_TOKEN_FOR_INTERNAL_SUPPORT')
        )

        access_key = os.getenv('CBC_ACCESS_KEY')
        secret_key = os.getenv('CBC_SECRET_KEY')

        if not bool(access_key and secret_key):
            with open(CAPELLA_CREDS_FILE, 'r') as f:
                creds = json.load(f)
                access_key = creds.get('access')
                secret_key = creds.get('secret')

        self.api_client.ACCESS = access_key
        self.api_client.SECRET = secret_key

        self.org_id = self.cluster_spec.controlplane_settings['org']
        self.project_id = self.cluster_spec.controlplane_settings['project']

        self.capella_timeout = max(0, self.options.capella_timeout)

        self.cluster_ids = self.cluster_spec.capella_provisioned_cluster_ids

    def deploy(self):
        if self.test_config.cluster.monitor_deployment_time:
            logger.info("Start timing capella cluster deployment")
            t0 = time()

        self.deploy_clusters()

        if self.options.disable_autoscaling:
            self.disable_autoscaling()

        self.update_spec()

        if not self.options.capella_only and self.options.vpc_peering:
            self.peer_vpc(next(iter(self.cluster_ids.values())))

        if self.test_config.cluster.monitor_deployment_time:
            logger.info("Finished timing capella cluster deployment")
            deployment_time = time() - t0
            logger.info("The total capella cluster deployment time is: {}".format(deployment_time))
            with open(TIMING_FILE, 'w') as f:
                l1 = "{}\n".format(str(deployment_time))
                f.writelines([l1])

    def destroy(self):
        self.destroy_peering_connection()

        if not self.options.keep_cluster:
            self.destroy_clusters()
            self.wait_for_cluster_destroy()

    def wait_for_cluster_destroy(self):
        logger.info('Waiting for clusters to be destroyed...')

        pending_clusters = [cluster_id for cluster_id in self.cluster_ids.values()]
        timeout_mins = self.capella_timeout
        interval_secs = 30
        t0 = time()
        while pending_clusters and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            resp = self.api_client.get_clusters({'projectId': self.project_id, 'perPage': 100})
            raise_for_status(resp)

            pending_clusters = []
            for cluster in resp.json()['data'].get('items', []):
                if (cluster_id := cluster['id']) in self.cluster_ids.values():
                    pending_clusters.append(cluster_id)
                    logger.info('Cluster {} not destroyed yet'.format(cluster_id))

        if pending_clusters:
            logger.error('Timed out after {} mins waiting for clusters to delete.'
                         .format(timeout_mins))
            for cluster_id in pending_clusters:
                logger.error('DataDog link for debugging (cluster {}): {}'
                             .format(cluster_id, format_datadog_link(cluster_id=cluster_id)))
        else:
            logger.info('All clusters destroyed.')

    @staticmethod
    def capella_server_group_sizes(node_schemas: Sequence[str]) -> Counter:
        server_groups = []
        for node in node_schemas:
            name, services = node.split(':')[:2]

            node_group, _ = name.split('.')
            services_set = tuple(set(services.split(',')))

            server_groups.append((node_group, services_set))

        return Counter(server_groups)

    @staticmethod
    def construct_capella_server_groups(
        cluster_spec: ClusterSpec,
        node_schemas: Sequence[str],
        enable_disk_autoscaling: bool = True
    ) -> list[dict[str, Any]]:
        """Create server groups for deploying a Provisioned Capella cluster using internal API.

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
        server_group_sizes = CapellaProvisionedDeployer.capella_server_group_sizes(node_schemas)

        server_groups = []
        for (node_group, services), size in server_group_sizes.items():
            node_group_config = cluster_spec.infrastructure_section(node_group)

            storage_class = node_group_config.get('storage_type').lower()

            if cluster_spec.cloud_provider == 'azure' and 'disk_tier' in node_group_config:
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
                    if cluster_spec.cloud_provider != 'azure'
                    else disk_tier,
                    'sizeInGb': int(node_group_config['storage_size']),
                },
                'diskAutoScaling': {
                    "enabled": enable_disk_autoscaling
                }
            }

            if cluster_spec.cloud_provider == 'aws':
                server_group['disk']['iops'] = int(node_group_config.get('iops', 3000))

            server_groups.append(server_group)

        return server_groups

    def deploy_clusters(self):
        cluster_nodes = self.all_nodes['clusters']
        names = ['perf-cluster-{}'.format(self.uuid) for _ in cluster_nodes]
        if len(names) > 1:
            names = ['{}-{}'.format(name, i) for i, name in enumerate(names)]

        self.cluster_spec.config.add_section('cb.capella.provisioned.id')

        for name, (label, nodes_list) in zip(names, cluster_nodes.items()):
            spec = self.construct_capella_server_groups(self.cluster_spec, nodes_list,
                                                        self.options.enable_disk_autoscaling)
            config = {
                "cidr": self.get_available_cidr(),
                "name": name,
                "description": "",
                "projectId": self.project_id,
                "provider": {
                    'aws': 'hostedAWS',
                    'gcp': 'hostedGCP',
                    'azure': 'hostedAzure'
                }[self.cluster_spec.cloud_provider.lower()],
                "region": self.region,
                "singleAZ": not self.options.multi_az,
                "server": self.options.capella_cb_version,
                'configurationType': "multiNode"
                                     if int(spec[0]['count']) != 1
                                     else "singleNode",
                "specs": spec,
                "package": "enterprise"
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

            resp = self.api_client.create_cluster_customAMI(self.org_id, config)
            raise_for_status(resp)
            cluster_id = resp.json().get('id')
            self.cluster_ids[label] = cluster_id

            logger.info('Initialised cluster deployment for cluster {} (label: {})\n  config: {}'.
                        format(cluster_id, label, config))
            logger.info('Saving cluster ID to spec file.')
            self.cluster_spec.config.set('cb.capella.provisioned.id', label, cluster_id)
            self.cluster_spec.update_config_file()

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_clusters = [cluster_id for cluster_id in self.cluster_ids.values()]
        t0 = time()
        while pending_clusters and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            statuses = []
            for cluster_id in pending_clusters:
                status = self.api_client.get_cluster_status(cluster_id).json().get('status')
                logger.info('Cluster state for {}: {}'.format(cluster_id, status))
                if status == 'deploymentFailed':
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

    def destroy_clusters(self):
        for cluster_id in self.cluster_ids.values():
            logger.info('Deleting Capella cluster {}...'.format(cluster_id))
            resp = self.api_client.delete_cluster(cluster_id)
            raise_for_status(resp)
            logger.info('Capella cluster successfully queued for deletion.')

    def get_available_cidr(self):
        resp = self.api_client.get_deployment_options(self.org_id,
                                                      self.cluster_spec.cloud_provider.lower())
        return resp.json().get('suggestedCidr')

    def get_deployed_cidr(self, cluster_id):
        resp = self.api_client.get_cluster_info(cluster_id)
        return resp.json().get('place', {}).get('CIDR')

    def get_hostnames(self, cluster_id):
        resp = self.api_client.get_nodes(tenant_id=self.org_id,
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

    def update_spec(self):
        self.cluster_spec.config.add_section('cb.capella.provisioned.spec')
        for option, value in self.cluster_spec.capella_provisioned_clusters_raw.items():
            self.cluster_spec.config.set('cb.capella.provisioned.spec', option, value)

        for label, cluster_id in self.cluster_ids.items():
            hostnames = self.get_hostnames(cluster_id)
            self.cluster_spec.config.set('cb.capella.provisioned', label,
                                         '\n' + '\n'.join(hostnames))

        self.cluster_spec.update_config_file()

    def disable_autoscaling(self):
        if self.api_client.TOKEN_FOR_INTERNAL_SUPPORT is None:
            logger.error('Cannot create circuit breaker to prevent auto-scaling. No value found '
                         'for CBC_TOKEN_FOR_INTERNAL_SUPPORT, so cannot authenticate with CP API.')
            return

        for cluster_id in self.cluster_ids.values():
            logger.info(
                'Creating deployment circuit breaker to prevent auto-scaling for cluster {}.'
                .format(cluster_id)
            )

            resp = self.api_client.create_circuit_breaker(cluster_id)
            raise_for_status(resp)

            resp = self.api_client.get_circuit_breaker(cluster_id)
            raise_for_status(resp)

            logger.info('Circuit breaker created: {}'.format(pretty_dict(resp.json())))

    def peer_vpc(self, cluster_id: str):
        logger.info('Setting up VPC peering...')
        if self.cluster_spec.cloud_provider == 'aws':
            peering_connection = self._peer_vpc_aws(cluster_id)
        elif self.cluster_spec.cloud_provider == 'gcp':
            peering_connection, dns_managed_zone, client_vpc = self._peer_vpc_gcp(cluster_id)
            self.cluster_spec.config.set('infrastructure', 'dns_managed_zone', dns_managed_zone)
            self.cluster_spec.config.set('infrastructure', 'client_vpc', client_vpc)

        if peering_connection:
            self.cluster_spec.config.set('infrastructure', 'peering_connection', peering_connection)
            self.cluster_spec.update_config_file()
        else:
            exit(1)

    def _peer_vpc_aws(self, cluster_id: str) -> str:
        # Initiate VPC peering
        logger.info('Initiating peering')

        client_vpc = self.client_network_info['vpc_id']
        cidr = self.client_network_info['public_subnet_cidr']
        route_table = self.client_network_info['route_table_id']
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
            resp = self.api_client.create_private_network(
                self.org_id, self.project_id, cluster_id, data)
            private_network_id = resp.json()['id']

            # Get AWS CLI commands that we need to run to complete the peering process
            logger.info('Accepting peering request')
            resp = self.api_client.get_private_network(
                self.org_id, self.project_id, cluster_id, private_network_id)
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

    def _peer_vpc_gcp(self, cluster_id: str) -> tuple[str, str]:
        # Initiate VPC peering
        logger.info('Initiating peering')

        client_vpc = self.client_network_info['vpc_id']
        cidr = self.client_network_info['subnet_cidr']
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
            resp = self.api_client.create_private_network(
                self.org_id, self.project_id, cluster_id, data)
            private_network_id = resp.json()['id']

            # Get gcloud commands that we need to run to complete the peering process
            logger.info('Accepting peering request')
            resp = self.api_client.get_private_network(
                self.org_id, self.project_id, cluster_id, private_network_id)
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
        if self.cluster_spec.cloud_provider == 'aws':
            self._destroy_peering_connection_aws()
        elif self.cluster_spec.cloud_provider == 'gcp':
            self._destroy_peering_connection_gcp()

    def _destroy_peering_connection_aws(self):
        peering_connection = self.cluster_spec.infrastructure_settings.get('peering_connection',
                                                                           None)

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
        peering_connection = self.cluster_spec.infrastructure_settings.get('peering_connection',
                                                                           None)

        if not peering_connection:
            logger.warn('No peering connection ID found in cluster spec; nothing to destroy.')
            return

        dns_managed_zone = self.cluster_spec.infrastructure_settings['dns_managed_zone']
        client_vpc = self.cluster_spec.infrastructure_settings['client_vpc']

        local('gcloud compute networks peerings delete {} --network={}'
              .format(peering_connection, client_vpc))
        local('gcloud dns managed-zones delete {}'.format(dns_managed_zone))


class CapellaServerlessDeployer(CapellaProvisionedDeployer):

    NEBULA_OVERRIDE_ARGS = ['override_count', 'min_count', 'max_count', 'instance_type']

    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        CloudVMDeployer.__init__(self, cluster_spec, options)
        if not options.test_config:
            logger.error('Test config required if deploying serverless infrastructure.')
            exit(1)

        test_config = TestConfig()
        test_config.parse(options.test_config, override=options.override)
        self.test_config = test_config

        for prefix, section in {'dapi': 'data_api', 'nebula': 'direct_nebula'}.items():
            for arg in self.NEBULA_OVERRIDE_ARGS:
                if (value := getattr(options, prefix + '_' + arg)) is not None:
                    self.cluster_spec.config.set(section, arg, str(value))

        self.cluster_spec.update_config_file()

        public_api_url = 'https://cloudapi.{}.nonprod-project-avengers.com'.format(
            self.cluster_spec.controlplane_settings['env']
        )

        self.serverless_client = CapellaAPIServerless(
            public_api_url,
            os.getenv('CBC_USER'),
            os.getenv('CBC_PWD'),
            os.getenv('CBC_TOKEN_FOR_INTERNAL_SUPPORT')
        )

        self.dedicated_client = CapellaAPIDedicated(
            public_api_url,
            None,
            None,
            os.getenv('CBC_USER'),
            os.getenv('CBC_PWD')
        )

        self.org_id = self.cluster_spec.controlplane_settings['org']

        if (dp_id := self.options.capella_dataplane) is None:
            dp_id = next(iter(self.cluster_spec.capella_serverless_dataplane_ids.values()))
        else:
            self.cluster_spec.config.set('infrastructure', 'cbc_dataplane', dp_id)
            self.cluster_spec.update_config_file()

        self.dp_id = dp_id

        self.project_id = self.cluster_spec.controlplane_settings['project']
        self.cluster_id = self.cluster_spec.infrastructure_settings.get('cbc_cluster', None)

    def deploy(self):
        self.deploy_serverless_dataplane()

        if not self.options.no_serverless_dbs:
            self.create_serverless_dbs()
        else:
            logger.info('Skipping deploying serverless dbs as --no-serverless-dbs flag is set')

        self.update_spec()

    def destroy(self):
        dbs_destroyed = False

        # Destroy capella cluster
        if self.dp_id:
            dbs_destroyed = self.destroy_serverless_databases()
            if dbs_destroyed and not self.options.keep_cluster:
                self.destroy_serverless_dataplane()
        else:
            logger.warn('No serverless dataplane ID found. Not destroying serverless dataplane.')

    def deploy_serverless_dataplane(self):
        if not self.dp_id:
            logger.info('Deploying serverless dataplane')
            # If no dataplane ID given (which is normal) then we deploy a new one
            nebula_config = self.cluster_spec.direct_nebula
            dapi_config = self.cluster_spec.data_api

            config = remove_nulls({
                "provider": "aws",
                "region": self.region,
                'overRide': {
                    'couchbase': {
                        'image': self.options.capella_ami,
                        'version': self.options.capella_cb_version,
                        'specs': (
                            specs
                            if (specs := self.construct_capella_server_groups(
                                self.cluster_spec,
                                next(iter(self.all_nodes['clusters'].values())),
                                self.options.enable_disk_autoscaling
                            ))
                            else None
                        )
                    },
                    'nebula': {
                        'image': self.options.nebula_ami,
                        'compute': {
                            'type': nebula_config.get('instance_type', None),
                            'count': {
                                'min': maybe_atoi(nebula_config.get('min_count', '')),
                                'max': maybe_atoi(nebula_config.get('max_count', '')),
                                'overRide': maybe_atoi(nebula_config.get('override_count', ''))
                            }
                        }
                    },
                    'dataApi': {
                        'image': self.options.dapi_ami,
                        'compute': {
                            'type': dapi_config.get('instance_type', None),
                            'count': {
                                'min': maybe_atoi(dapi_config.get('min_count', '')),
                                'max': maybe_atoi(dapi_config.get('max_count', '')),
                                'overRide': maybe_atoi(dapi_config.get('override_count', ''))
                            }
                        }
                    }
                }
            })

            logger.info(pretty_dict(config))

            resp = self.serverless_client.create_serverless_dataplane(config)
            raise_for_status(resp)
            self.dp_id = resp.json().get('dataplaneId')
            logger.info('Initialised deployment for serverless dataplane {}'.format(self.dp_id))
            logger.info('Saving dataplane ID to spec file.')
            self.cluster_spec.config.set('infrastructure', 'cbc_dataplane', self.dp_id)
            self.cluster_spec.update_config_file()
        else:
            logger.info('Skipping serverless dataplane deployment as existing dataplane specified: '
                        '{}'.format(self.dp_id))
            logger.info('Verifying existing dataplane deployment')

        # Whether we have just deployed a dataplane or not, we will confirm the deployment
        # status of the dataplane to check its ready for a test
        resp = self.serverless_client.get_dataplane_deployment_status(self.dp_id)
        raise_for_status(resp)
        status = resp.json()['status']['state']

        self.cluster_id = resp.json()['couchbaseCluster']['id']
        self.cluster_spec.config.set('infrastructure', 'cbc_cluster', self.cluster_id)
        self.cluster_spec.update_config_file()

        if self.options.disable_autoscaling:
            self.disable_autoscaling()

        timeout_mins = self.options.capella_timeout
        interval_secs = 30
        status = None
        t0 = time()
        while (time() - t0) < timeout_mins * 60 and status != 'ready':
            resp = self.serverless_client.get_dataplane_deployment_status(self.dp_id)
            raise_for_status(resp)
            status = resp.json()['status']['state']

            logger.info('Dataplane state: {}'.format(status))
            if status == 'disabled':
                break
            elif status != 'ready':
                sleep(interval_secs)

        if status != 'ready':
            if status == 'disabled':
                logger.error('Deployment failed, dataplane entered disabled state.')
            else:
                logger.error('Deployment timed out after {} mins.'.format(timeout_mins))
            logger.error('DataDog link for debugging (filtering by dataplane ID): {}'
                         .format(format_datadog_link(dataplane_id=self.dp_id)))
            logger.error('DataDog link for debugging (filtering by cluster ID): {}'
                         .format(format_datadog_link(cluster_id=self.cluster_id)))
            exit(1)

        resp = self.serverless_client.get_serverless_dataplane_info(self.dp_id)
        raise_for_status(resp)
        logger.info('Deployed dataplane info: {}'.format(pretty_dict(resp.json())))

    def disable_autoscaling(self):
        logger.info('Creating deployment circuit breaker to prevent auto-scaling.')

        resp = self.serverless_client.create_circuit_breaker(self.cluster_id)
        raise_for_status(resp)

        resp = self.serverless_client.get_circuit_breaker(self.cluster_id)
        raise_for_status(resp)

        logger.info('Circuit breaker created: {}'.format(pretty_dict(resp.json())))

    def _create_db(self, name, width=1, weight=30):
        logger.info('Adding new serverless DB: {}'.format(name))

        data = {
            'name': name,
            'tenantId': self.org_id,
            'projectId': self.project_id,
            'provider': self.csp,
            'region': self.region,
            'overRide': {
                'width': width,
                'weight': weight,
                'dataplaneId': self.dp_id
            },
            'dontImportSampleData': True
        }

        logger.info('DB configuration: {}'.format(pretty_dict(data)))

        resp = self.serverless_client.create_serverless_database_overRide(data)
        raise_for_status(resp)
        return resp.json()

    def _get_db_info(self, db_id):
        resp = self.serverless_client.get_database_debug_info(db_id)
        raise_for_status(resp)
        return resp.json()

    def create_serverless_dbs(self):
        dbs = {}

        if not (init_db_map := self.test_config.serverless_db.init_db_map):
            init_db_map = {
                'bucket-{}'.format(i+1): {'width': 1, 'weight': 30}
                for i in range(self.test_config.cluster.num_buckets)
            }

        for db_name, params in init_db_map.items():
            resp = self._create_db(db_name, params['width'], params['weight'])
            db_id = resp['databaseId']
            logger.info('Database ID for {}: {}'.format(db_name, db_id))
            dbs[db_id] = {
                'name': db_name,
                'width': params['width'],
                'weight': params['weight'],
                'nebula_uri': None,
                'dapi_uri': None,
                'access': None,
                'secret': None
            }
            self.test_config.serverless_db.update_db_map(dbs)

        timeout_mins = self.options.capella_timeout
        interval_secs = 20
        t0 = time()
        db_ids = list(dbs.keys())
        while db_ids and time() - t0 < timeout_mins * 60:
            for db_id in db_ids:
                db_info = self._get_db_info(db_id)['database']
                db_state = db_info['status']['state']
                logger.info('{} state: {}'.format(db_id, db_state))
                if db_state == 'ready':
                    logger.info('Serverless DB deployed: {}'.format(db_id))
                    db_ids.remove(db_id)
                    dbs[db_id]['nebula_uri'] = db_info['connect']['sdk']
                    dbs[db_id]['dapi_uri'] = db_info['connect']['dataApi']

            if db_ids:
                sleep(interval_secs)

        self.test_config.serverless_db.update_db_map(dbs)

        if db_ids:
            logger.error('Serverless DB deployment timed out after {} mins'.format(timeout_mins))
            for db_id in db_ids:
                logger.error('DataDog link for debugging (database {}): {}'
                             .format(db_id, format_datadog_link(database_id=db_id)))
            exit(1)
        else:
            logger.info('All serverless DBs deployed')

    def update_spec(self):
        resp = self.serverless_client.get_serverless_dataplane_info(self.dp_id)
        raise_for_status(resp)
        dp_info = resp.json()

        resp = self.serverless_client.get_access_to_serverless_dataplane_nodes(self.dp_id)
        raise_for_status(resp)
        dp_creds = resp.json()

        hostname = dp_info['couchbase']['nodes'][0]['hostname']
        auth = (
            dp_creds['couchbaseCreds']['username'],
            dp_creds['couchbaseCreds']['password']
        )
        self.cluster_spec.config.set('credentials', 'rest', ':'.join(auth).replace('%', '%%'))

        default_pool = self.get_default_pool(hostname, auth)
        nodes = []
        for node in default_pool['nodes']:
            hostname = node['hostname'].removesuffix(':8091')
            services = sorted(node['services'], key=lambda s: s != 'kv')
            services_str = ','.join(services)
            group = node['serverGroup'].removeprefix('group:')
            nodes.append((hostname, services_str, group))

        nodes = sorted(nodes, key=lambda n: n[2])
        nodes = [':'.join(n) for n in sorted(nodes, key=lambda n: '' if 'kv' in n[1] else n[1])]
        node_string = '\n'.join(nodes)

        self.cluster_spec.config.set('clusters', 'serverless', node_string)

        self.cluster_spec.update_config_file()

    def get_default_pool(self, hostname, auth):
        session = requests.Session()
        resp = session.get('https://{}:18091/pools/default'.format(hostname),
                           auth=auth, verify=False)
        raise_for_status(resp)
        return resp.json()

    def _destroy_db(self, db_id):
        logger.info('Destroying serverless DB {}'.format(db_id))
        resp = self.serverless_client.delete_database(self.org_id, self.project_id, db_id)
        raise_for_status(resp)
        logger.info('Serverless DB queued for deletion: {}'.format(db_id))

    def destroy_serverless_databases(self) -> bool:
        logger.info('Deleting all serverless databases...')

        pending_dbs = []
        for db_id in self.test_config.serverless_db.db_map:
            self._destroy_db(db_id)
            pending_dbs.append(db_id)
        logger.info('All serverless databases queued for deletion.')

        overall_timeout_mins = self.options.capella_timeout
        retries = 2
        timeout_mins = overall_timeout_mins / (retries + 1)
        interval_secs = 20
        while pending_dbs and retries >= 0:
            logger.info('Waiting for databases to be deleted (retries left: {})'.format(retries))
            t0 = time()
            while pending_dbs and time() - t0 < timeout_mins * 60:
                sleep(interval_secs)
                resp = self.serverless_client.list_all_databases(self.org_id, self.project_id)
                raise_for_status(resp)
                pending_dbs = [db['data']['id'] for db in resp.json()['data']]
                logger.info('{} databases remaining: {}'.format(len(pending_dbs), pending_dbs))

            if pending_dbs:
                logger.info('Database deletion timeout reached.'
                            'Retrying deletion of remaining databases.')
                for db_id in pending_dbs:
                    self._destroy_db(db_id)
            else:
                logger.info('All databases successfully deleted.')
                return True

            retries -= 1

        if pending_dbs:
            logger.error('Timed out after {} mins waiting for databases to delete.'
                         .format(overall_timeout_mins))
            for db_id in pending_dbs:
                logger.error('DataDog link for debugging (database {}): {}'
                             .format(db_id, format_datadog_link(database_id=db_id)))
            return False

    def destroy_serverless_dataplane(self):
        logger.info('Deleting serverless dataplane...')
        while (resp := self.serverless_client.delete_dataplane(self.dp_id)).status_code == 422:
            logger.info("Waiting for databases to be fully deleted...")
            sleep(5)
        raise_for_status(resp)
        logger.info('Serverless dataplane successfully queued for deletion.')


class EKSDeployer(CloudVMDeployer):
    pass


class CapellaAppServicesDeployer(CloudVMDeployer):
    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        super().__init__(cluster_spec, options)

        self.test_config = None
        if options.test_config:
            test_config = TestConfig()
            test_config.parse(options.test_config, override=options.override)
            self.test_config = test_config

        self.org_id = self.cluster_spec.controlplane_settings['org']
        self.project_id = self.cluster_spec.controlplane_settings['project']
        self.cluster_id = next(iter(self.cluster_spec.capella_provisioned_cluster_ids.values()))

        logger.info("The org id is: {}".format(self.org_id))
        logger.info("The project id is: {}".format(self.project_id))
        logger.info("The cluster id is: {}".format(self.cluster_id))

        self.api_client = CapellaAPIDedicated(
            'https://cloudapi.{}.nonprod-project-avengers.com'.format(
                self.cluster_spec.controlplane_settings['env']
            ),
            None,
            None,
            os.getenv('CBC_USER'),
            os.getenv('CBC_PWD')
        )

        self.capella_timeout = max(0, self.options.capella_timeout)

    def deploy(self):
        # Configure terraform
        logger.info("Started deploying the AS")
        # Deploy capella cluster
        # Create sgw backend(app services)
        if self.test_config.cluster.monitor_deployment_time:
            logger.info("Started timing the app services deployment")
            t0 = time()
            logger.info("Deploying sgw backend")
        sgw_cluster_id = self.deploy_cluster_internal_api()

        if self.test_config.cluster.monitor_deployment_time:
            logger.info("Finished timing the app services deployment")
            deployment_time = time() - t0
            logger.info("The app services deployment time is: {}".format(deployment_time))
            logger.info("Started timing the app service database creation")
            t0 = time()
        for bucket_name in self.test_config.buckets:
            # Create sgw database(app endpoint)
            logger.info("Deploying sgw database")
            sgw_db_id, sgw_db_name = self.deploy_sgw_db(sgw_cluster_id, bucket_name)

            # Set sync function
            logger.info("Setting sync function")
            sync_function = "function (doc) { channel(doc.channels); }"
            self.api_client.update_sync_function_sgw(self.org_id, self.project_id,
                                                     self.cluster_id, sgw_cluster_id,
                                                     sgw_db_name, sync_function)

            # Resume sgw database
            logger.info("Resuming sgw database")
            self.api_client.resume_sgw_database(self.org_id, self.project_id,
                                                self.cluster_id, sgw_cluster_id,
                                                sgw_db_name)

            # Allow my IP
            logger.info("Whitelisting own IP on sgw backend")
            self.api_client.allow_my_ip_sgw(self.org_id, self.project_id,
                                            self.cluster_id, sgw_cluster_id)

            # Add allowed IPs
            logger.info("Whitelisting IPs")
            client_ips = self.cluster_spec.clients
            logger.info("The client list is: {}".format(client_ips))
            if self.cluster_spec.cloud_provider == 'aws':
                client_ips = [
                    dns.split('.')[0].removeprefix('ec2-').replace('-', '.') for dns in client_ips
                ]
            logger.info("The client list is: {}".format(client_ips))
            for client_ip in client_ips:
                self.api_client.add_allowed_ip_sgw(self.org_id, self.project_id,
                                                   sgw_cluster_id, self.cluster_id,
                                                   client_ip)

            # Add app roles
            logger.info("Adding app roles")
            app_role = {"name": "moderator", "admin_channels": []}
            self.api_client.add_app_role_sgw(self.org_id, self.project_id,
                                             self.cluster_id, sgw_cluster_id,
                                             sgw_db_name, app_role)
            app_role = {"name": "admin", "admin_channels": []}
            self.api_client.add_app_role_sgw(self.org_id, self.project_id,
                                             self.cluster_id, sgw_cluster_id,
                                             sgw_db_name, app_role)

            # Add user
            logger.info("Adding user")
            user = {"email": "", "password": "Password123!", "name": "guest",
                    "disabled": False, "admin_channels": ["*"], "admin_roles": []}
            self.api_client.add_user_sgw(self.org_id, self.project_id,
                                         self.cluster_id, sgw_cluster_id,
                                         sgw_db_name, user)

            # Add admin user
            logger.info("Adding admin user")
            admin_user = {"name": "Administrator", "password": "Password123!"}
            self.api_client.add_admin_user_sgw(self.org_id, self.project_id,
                                               self.cluster_id, sgw_cluster_id,
                                               sgw_db_name, admin_user)

            # Update cluster spec file
            self.update_spec(sgw_cluster_id, sgw_db_name)

        if self.test_config.cluster.monitor_deployment_time:
            logger.info("Finished creating the app services databases")
            db_creation_time = time() - t0
            logger.info("The app services database creation time is: {}".format(db_creation_time))

            with open(TIMING_FILE, 'w') as f:
                l1 = "{}\n".format(str(deployment_time))
                l2 = "{}\n".format(str(db_creation_time))
                f.writelines([l1, l2])

    def destroy(self):
        # Destroy capella cluster
        sgw_cluster_id = self.cluster_spec.infrastructure_settings['app_services_cluster']
        self.destroy_cluster_internal_api(sgw_cluster_id)

    def update_spec(self, sgw_cluster_id, sgw_db_name):
        # Get sgw public and private ips
        resp = self.api_client.get_sgw_links(self.org_id, self.project_id,
                                             self.cluster_id, sgw_cluster_id,
                                             sgw_db_name)

        logger.info("The connect response is: {}".format(resp))
        adminurl = resp.json().get('data').get('adminURL').split(':')[1].split('//')[1]
        logger.info("The admin url is: {}".format(adminurl))
        self.cluster_spec.config.add_section('sgw_schemas')
        for option, value in self.cluster_spec.infrastructure_syncgateways.items():
            self.cluster_spec.config.set('sgw_schemas', option, value)

        sgw_option = self.cluster_spec.config.options('syncgateways')[0]
        sgw_list = []
        for i in range(0, self.test_config.syncgateway_settings.nodes):
            sgw_list.append(adminurl)
        logger.info("the sgw list is: {}".format(sgw_list))
        self.cluster_spec.config.set('syncgateways', sgw_option, '\n' + '\n'.join(sgw_list))
        self.cluster_spec.update_config_file()

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

    def construct_capella_sgw_groups(self, cluster_spec, node_schemas):
        sgw_groups = CapellaAppServicesDeployer.capella_sgw_group_sizes(node_schemas)

        for sgw_groups in sgw_groups.items():

            for (node_group) in sgw_groups.items():
                node_group_config = cluster_spec.infrastructure_section(node_group)

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

        logger.info("The payload is: {}".format(config))

        if self.options.capella_cb_version and self.options.capella_sgw_ami:
            config['overRide'] = {
                'token': os.getenv('CBC_OVERRIDE_TOKEN'),
                'server': self.options.capella_cb_version,
                'image': self.options.capella_sgw_ami
            }
            logger.info('Deploying with custom AMI: {}'.format(self.options.capella_sgw_ami))

        resp = self.api_client.create_sgw_backend(self.org_id, config)
        raise_for_status(resp)
        sgw_cluster_id = resp.json().get('id')
        logger.info('Initialised app services deployment {}'.format(sgw_cluster_id))
        logger.info('Saving app services ID to spec file.')

        self.cluster_spec.config.set('infrastructure', 'app_services_cluster', sgw_cluster_id)
        self.cluster_spec.update_config_file()

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_sgw_cluster = sgw_cluster_id
        t0 = time()
        while pending_sgw_cluster and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            status = self.api_client.get_sgw_info(self.org_id, self.project_id,
                                                  self.cluster_id, sgw_cluster_id).json() \
                                                                                  .get('data') \
                                                                                  .get('status') \
                                                                                  .get('state')
            logger.info('Cluster state for {}: {}'.format(pending_sgw_cluster, status))
            if status == "deploymentFailed":
                logger.error('Deployment failed for cluster {}. DataDog link for debugging: {}'
                             .format(pending_sgw_cluster,
                                     format_datadog_link(cluster_id=pending_sgw_cluster)))
                exit(1)

            if status == "healthy":
                break

        return sgw_cluster_id

    def deploy_sgw_db(self, sgw_cluster_id, bucket_name):
        """Sample config.

        {
            "name": "sgw-1",
            "sync": "",
            "bucket": "bucket-1",
            "delta_sync": false,
            "import_filter": ""
        }
        """
        sgw_db_name = "db-{}".format(bucket_name.split("-")[1])
        logger.info("The sgw db name is: {}".format(sgw_db_name))
        config = {
            "name": sgw_db_name,
            "bucket": bucket_name,
            "delta_sync": False,
        }
        collections_map = self.test_config.collection.collection_map
        logger.info("The collections map is: {}".format(collections_map))
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

            logger.info("The number of scopes is: {}".format(scope_count))

            logger.info("The number of collections is: {}".format(collection_count))
            config["scopes"] = {}
            scopes = {}
            for scope in target_scopes:
                logger.info("The current scope is: {}".format(scope))
                collections = {}
                for collection in target_collections:
                    collections[collection] = {
                        "import_filter": "function(doc) {return true}",
                        "sync": "function (doc) { channel(doc.channels); }"
                    }
                scopes[scope] = collections
            config["scopes"] = scopes
        logger.info("The configuration is: {}".format(config))

        resp = self.api_client.create_sgw_database(self.org_id, self.project_id,
                                                   self.cluster_id, sgw_cluster_id, config)
        # raise_for_status(resp)
        sgw_db_id = resp.json().get('id')
        logger.info('Initialised sgw database deployment {}'.format(sgw_db_id))
        logger.info('Saving sgw db ID to spec file.')

        num_sgw = 0
        count = 0
        while num_sgw != self.test_config.cluster.num_buckets:
            sleep(10)
            resp = self.api_client.get_sgw_databases(self.org_id, self.project_id,
                                                     self.cluster_id, sgw_cluster_id).json()
            logger.info("The response_data is: {}".format(resp))
            new_resp = resp.get('data')
            logger.info("The new resp is: {}".format(new_resp))
            if new_resp is not None:
                num_sgw = 0
                for resp_bit in new_resp:
                    logger.info(resp_bit)
                    num_sgw += 1
            count += 1
            if count > 25:
                break

        logger.info("SGW databases successfully created")
        return sgw_db_id, sgw_db_name

    def destroy_cluster_internal_api(self, sgw_cluster_id):
        logger.info('Deleting Capella App Services cluster...')
        resp = self.api_client.delete_sgw_backend(self.org_id, self.project_id,
                                                  self.cluster_id, sgw_cluster_id)

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_sgw_cluster = sgw_cluster_id
        t0 = time()
        while pending_sgw_cluster and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)
            try:
                status = self.api_client.get_sgw_info(self.org_id, self.project_id,
                                                      self.cluster_id, sgw_cluster_id)
                status = status.json().get('data').get('status').get('state')
                logger.info('Cluster state for {}: {}'.format(pending_sgw_cluster, status))
            except Exception as e:
                logger.info("The exception is: {}".format(e))
                sleep(30)
                break

        raise_for_status(resp)
        logger.info('Capella App Services cluster successfully queued for deletion.')


class CapellaColumnarDeployer(CapellaProvisionedDeployer):

    def __init__(self, cluster_spec: ClusterSpec, options: Namespace):
        CloudVMDeployer.__init__(self, cluster_spec, options)

        if public_api_url := self.options.capella_public_api_url:
            env = public_api_url.removeprefix('https://')\
                                .removesuffix('.nonprod-project-avengers.com')\
                                .split('.', 1)[1]
            self.cluster_spec.config.set('infrastructure', 'cbc_env', env)
            self.cluster_spec.update_config_file()

        self.api_client = CapellaAPIColumnar(
            'https://cloudapi.{}.nonprod-project-avengers.com'.format(
                self.cluster_spec.controlplane_settings['env']
            ),
            None,
            None,
            os.getenv('CBC_USER'),
            os.getenv('CBC_PWD'),
            os.getenv('CBC_TOKEN_FOR_INTERNAL_SUPPORT')
        )

        self.org_id = self.cluster_spec.controlplane_settings['org']
        self.project_id = self.cluster_spec.controlplane_settings['project']

        self.capella_timeout = max(0, self.options.capella_timeout)

        self.instance_ids = self.cluster_spec.capella_columnar_instance_ids

    def deploy(self):
        self.deploy_goldfish_instances()
        self.update_spec()

    def deploy_goldfish_instances(self):
        names = ['perf-goldfish-{}'.format(self.uuid)
                 for _ in self.cluster_spec.capella_columnar_instances]
        if len(names) > 1:
            names = ['{}-{}'.format(name, i) for i, name in enumerate(names)]

        self.cluster_spec.config.add_section('cb.capella.columnar.id')

        for name, (label, nodes) in zip(names,
                                        self.cluster_spec.capella_columnar_instances):
            config = {
                "name": name,
                "description": "",
                "provider": self.csp,
                "region": self.region,
                "nodes": len(nodes)
            }

            logger.info('Deploying Goldfish instance with config: {}'.format(pretty_dict(config)))
            resp = self.api_client.create_columnar_instance(self.org_id, self.project_id,
                                                            **config)
            raise_for_status(resp)

            instance_id = resp.json().get('id')
            self.instance_ids[label] = instance_id

            logger.info('Initialised Goldfish instance deployment {}'.format(instance_id))
            logger.info('Saving Goldfish instance ID to spec file.')
            self.cluster_spec.config.set('cb.capella.columnar.id', label, instance_id)
            self.cluster_spec.update_config_file()

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_instances = [instance_id for instance_id in self.instance_ids.values()]
        logger.info('Waiting for Goldfish instance(s) to be deployed...')
        t0 = time()
        while pending_instances and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            statuses = []
            for instance_id in pending_instances:
                resp = self.api_client.get_specific_columnar_instance(self.org_id,
                                                                      self.project_id,
                                                                      instance_id)
                raise_for_status(resp)
                status = resp.json()['state']
                logger.info('Instance state for {}: {}'.format(instance_id, status))
                if status == 'deploy_failed':
                    logger.error('Deployment failed for Goldfish instance {}.'.format(instance_id))
                    exit(1)
                statuses.append(status)

            pending_instances = [
                pending_instances[i] for i, status in enumerate(statuses) if status != 'healthy'
            ]

        if pending_instances:
            logger.error('Deployment timed out after {} mins'.format(timeout_mins))
            exit(1)

        logger.info('Successfully deployed all Goldfish instances')

    def destroy(self):
        if not self.options.keep_cluster:
            self.destroy_goldfish_instances()
            self.wait_for_goldfish_instance_destroy()

    def destroy_goldfish_instances(self):
        for instance_id in self.instance_ids.values():
            logger.info('Deleting Goldfish instance {}...'.format(instance_id))
            resp = self.api_client.delete_columnar_instance(self.org_id, self.project_id,
                                                            instance_id)
            raise_for_status(resp)
            logger.info('Goldfish instance successfully queued for deletion.')

    def wait_for_goldfish_instance_destroy(self):
        logger.info('Waiting for Goldfish instances to be destroyed...')

        timeout_mins = self.capella_timeout
        interval_secs = 30
        pending_instances = [instance_id for instance_id in self.instance_ids.values()]
        t0 = time()
        while pending_instances and (time() - t0) < timeout_mins * 60:
            sleep(interval_secs)

            resp = self.api_client.get_columnar_instances(self.org_id, self.project_id)
            raise_for_status(resp)

            pending_instances = []
            for instance in resp.json()['data']:
                if (instance_id := instance['data'].get('id')) in self.instance_ids.values():
                    pending_instances.append(instance_id)
                    logger.info('Instance {} not destroyed yet'.format(instance_id))

        if pending_instances:
            logger.error('Timed out after {} mins waiting for instances to delete.'
                         .format(timeout_mins))
        else:
            logger.info('All Goldfish instances destroyed.')

    def update_spec(self):
        """Update the infrastructure spec file with Goldfish instance details.

        This includes:
        - Hostnames for compute nodes
        - Nebula endpoints
        - API keys
        """
        if not self.cluster_spec.config.has_section('goldfish_nebula'):
            self.cluster_spec.config.add_section('goldfish_nebula')

        nebula_creds = []
        cb_versions = []

        for label, instance_id in self.instance_ids.items():
            nodes = self.cluster_spec.capella_columnar_instances_raw[label].strip().split()
            hostnames = [
                'multinodeapi-{}.{}.{}.aws.omnistrate.cloud:kv,cbas'.format(
                    x, instance_id, self.region
                )
                for x, _ in enumerate(nodes)
            ]
            self.cluster_spec.config.set('cb.capella.columnar', label, '\n' + '\n'.join(hostnames))

            resp = self.api_client.get_specific_columnar_instance(self.org_id, self.project_id,
                                                                  instance_id)
            raise_for_status(resp)
            nebula_endpoint = resp.json()['config']['endpoint']
            logger.info('Nebula endpoint for {}: {}'.format(instance_id, nebula_endpoint))
            self.cluster_spec.config.set('goldfish_nebula', label, nebula_endpoint)

            cb_version = resp.json()['config']['version']['server']
            logger.info('Couchbase Server version for {}: {}'.format(instance_id, cb_version))
            cb_versions.append(cb_version)

            logger.info('Generating API keys for {}...'.format(instance_id))
            resp = self.api_client.create_api_keys(self.org_id, self.project_id, instance_id)
            raise_for_status(resp)
            key_id, key_secret = resp.json()['apikeyId'], resp.json()['secret']
            logger.info('API key ID for {}: {}'.format(instance_id, key_id))
            nebula_creds.append('{}:{}'.format(key_id, key_secret))

        self.cluster_spec.config.set('credentials', 'goldfish_nebula',
                                   '\n'.join(nebula_creds).replace('%', '%%'))
        self.cluster_spec.config.set('infrastructure', 'goldfish_cb_versions',
                                     '\n'.join(cb_versions))
        self.cluster_spec.update_config_file()


# CLI args.
def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a infrastructure specification file')
    parser.add_argument('--test-config',
                        required=False,
                        help='the path to the test configuration file')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('--azure-region',
                        choices=[
                            'eastus',
                            'westeurope',
                            'eastus2',
                            'westus2',
                            'uksouth'
                        ],
                        default='eastus',
                        help='the cloud region (Azure)')
    parser.add_argument('-r', '--aws-region',
                        choices=[
                            'us-east-1',
                            'us-east-2',
                            'us-west-2',
                            'ca-central-1',
                            'ap-northeast-1',
                            'ap-northeast-2',
                            'ap-southeast-1',
                            'ap-south-1',
                            'eu-west-1',
                            'eu-west-2',
                            'eu-west-3',
                            'eu-central-1',
                            'sa-east-1'
                        ],
                        default='us-east-1',
                        help='the cloud region (AWS)')
    parser.add_argument('-z', '--gcp-zone',
                        choices=[
                           'us-central1-a',
                           'us-central1-b',
                           'us-central1-c',
                           'us-central1-f',
                           'us-west1-a',
                           'us-west1-b',
                           'us-west1-c'
                        ],
                        default='us-west1-b',
                        dest='zone',
                        help='the cloud zone (GCP)')
    parser.add_argument('--cluster-image',
                        help='Image/AMI name to use for cluster nodes')
    parser.add_argument('--client-image',
                        help='Image/AMI name to use for client nodes')
    parser.add_argument('--utility-image',
                        help='Image/AMI name to use for utility nodes')
    parser.add_argument('--sgw-image',
                        help='Image/AMI name to use for sync gateway nodes')
    parser.add_argument('--kafka-image',
                        help='Image/AMI name to use for Kafka nodes')
    parser.add_argument('--capella-public-api-url',
                        help='public API URL for Capella environment')
    parser.add_argument('--capella-tenant',
                        help='tenant ID for Capella deployment')
    parser.add_argument('--capella-project',
                        help='project ID for Capella deployment')
    parser.add_argument('--capella-dataplane',
                        help='serverless dataplane ID to use for serverless database deployment')
    parser.add_argument('--capella-cb-version',
                        help='cb version to use for Capella deployment')
    parser.add_argument('--capella-ami',
                        help='custom AMI to use for Capella deployment')
    parser.add_argument('--capella-sgw-ami',
                        help='custom AMI to use for App Services Capella Deployment')
    parser.add_argument('--release-id',
                        help='release id for managing releases')
    parser.add_argument('--dapi-ami',
                        help='AMI to use for Data API deployment (serverless)')
    parser.add_argument('--dapi-override-count',
                        type=int,
                        help='number of DAPI nodes to deploy')
    parser.add_argument('--dapi-min-count',
                        type=int,
                        help='minimum number of DAPI nodes in autoscaling group')
    parser.add_argument('--dapi-max-count',
                        type=int,
                        help='maximum number of DAPI nodes in autoscaling group')
    parser.add_argument('--dapi-instance-type',
                        help='instance type to use for DAPI nodes')
    parser.add_argument('--nebula-ami',
                        help='AMI to use for Direct Nebula deployment (serverless)')
    parser.add_argument('--nebula-override-count',
                        type=int,
                        help='number of Direct Nebula nodes to deploy')
    parser.add_argument('--nebula-min-count',
                        type=int,
                        help='minimum number of Direct Nebula nodes in autoscaling group')
    parser.add_argument('--nebula-max-count',
                        type=int,
                        help='maximum number of Direct Nebula nodes in autoscaling group')
    parser.add_argument('--nebula-instance-type',
                        help='instance type to use for Direct Nebula nodes')
    parser.add_argument('--vpc-peering',
                        action='store_true',
                        help='enable VPC peering for Capella deployment')
    parser.add_argument('--capella-timeout',
                        type=int,
                        default=20,
                        help='Timeout (minutes) for Capella deployment when using internal API')
    parser.add_argument('--disable-autoscaling',
                        action='store_true',
                        help='Disable cluster auto-scaling for Capella clusters by creating a '
                             'deployment circuit breaker for the cluster.')
    parser.add_argument('--keep-cluster',
                        action='store_true',
                        help='Don\'t destroy cluster or serverless dataplane, only the clients '
                             'and utilities')
    parser.add_argument('--no-serverless-dbs',
                        action='store_true',
                        help='Don\'t deploy serverless databases, only deploy serverless dataplane')
    parser.add_argument('--capella-only',
                        action='store_true',
                        help='Only deploy Capella resources (provisioned cluster or serverless '
                             'dataplane). Will not deploy perf client or utility nodes.')
    parser.add_argument('-t', '--tag',
                        help='Global tag for launched instances.')
    parser.add_argument('--enable-disk-autoscaling',
                        action='store_true',
                        default=True,
                        help='Enables Capella disk autoscaling')
    parser.add_argument('--multi-az',
                        action="store_true",
                        default=False,
                        help="Deploy Capella cluster to multiple Availability Zones")
    parser.add_argument('override',
                        nargs='*',
                        help='custom cluster and/or test settings')

    return parser.parse_args()


def deployer_sequence(
    cluster_spec: ClusterSpec,
    cli_args: Namespace
) -> list[type[CloudVMDeployer]]:
    if cluster_spec.app_services == 'true':
        return [CapellaAppServicesDeployer]

    deployers = []

    if not (cluster_spec.has_any_capella and cli_args.capella_only):
        deployers.append(CloudVMDeployer)

    if cluster_spec.has_capella_provisioned and cluster_spec.has_capella_serverless:
        logger.interrupt('Having both Capella Provisioned and Capella Serverless databases in the '
                         'same test is not supported.')

    if cluster_spec.has_capella_provisioned:
        deployers.append(CapellaProvisionedDeployer)
    elif cluster_spec.has_capella_serverless:
        deployers.append(CapellaServerlessDeployer)

    if cluster_spec.has_capella_columnar:
        deployers.append(CapellaColumnarDeployer)

    return deployers


def need_capella_project(
    deployer_classes: list[type[CloudVMDeployer]],
    cli_args: Namespace
) -> bool:
    return not (
        CapellaServerlessDeployer in deployer_classes and
        cli_args.no_serverless_dbs and
        CapellaColumnarDeployer not in deployer_classes
    )


def destroy():
    args = get_args()
    cluster_spec = ClusterSpec()
    cluster_spec.parse(fname=args.cluster, override=args.override)

    deployer_classes = deployer_sequence(cluster_spec, args)
    for deployer_cls in deployer_classes:
        logger.info('Destroying with {}'.format(deployer_cls.__name__))
        deployer = deployer_cls(cluster_spec, args)
        deployer.destroy()

    if cluster_spec.has_any_capella:
        cp_prepper = ControlPlanePreparer(
            cluster_spec,
            args.capella_public_api_url,
            args.capella_tenant,
            args.capella_project
        )
        cp_prepper.save_org_id()
        cp_prepper.clean_up_api_keys()
        if not args.keep_cluster:
            cp_prepper.save_project_id(False)
            cp_prepper.clean_up_project()


def main():
    args = get_args()
    cluster_spec = ClusterSpec()
    cluster_spec.parse(fname=args.cluster, override=args.override)

    deployer_classes = deployer_sequence(cluster_spec, args)

    if cluster_spec.has_any_capella:
        cp_prepper = ControlPlanePreparer(
            cluster_spec,
            args.capella_public_api_url,
            args.capella_tenant,
            args.capella_project,
            args.tag
        )
        cp_prepper.save_org_id()
        cp_prepper.get_api_keys()
        cp_prepper.save_project_id(need_capella_project(deployer_classes, args))

    for deployer_cls in deployer_classes:
        logger.info('Deploying with {}'.format(deployer_cls.__name__))
        deployer = deployer_cls(cluster_spec, args)
        deployer.deploy()

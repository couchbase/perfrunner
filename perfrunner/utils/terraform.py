#!/usr/bin/env python
import json
import os
from argparse import ArgumentParser
from collections import Counter
from time import sleep, time
from uuid import uuid4

from capella.dedicated.CapellaAPI import CapellaAPI
from fabric.api import local

from logger import logger
from perfrunner.settings import ClusterSpec


class Terraform:

    # TODO: AWS capacity retry,
    #  Reset TFs function,
    #  Swap find and replace for pipe,
    #  Backup spec update,
    #  Add support for multiple clusters.

    AZURE_IMAGE_URL_PREFIX = '/subscriptions/a5c0936c-5cec-4c8c-85e1-97f5cab644d9/resourceGroups/' \
                             'perf-resources-eastus/providers/Microsoft.Compute/galleries' \
                             '/perf_vm_images/images'

    IMAGE_MAP = {
        'aws': {
            'clusters': {
                'x86_64': 'ami-005bce54f0c4e2248',
                'arm': 'ami-0f249abfe3dd01b30',
                'al2': 'ami-060e286353d227c32'
            },
            'clients': 'ami-01b36cb3330d38ac5',
            'utilities': 'ami-0d9e5ee360aa02d94',
            'sync_gateways': 'ami-005bce54f0c4e2248'
        },
        'gcp': {
            'clusters': 'perftest-server-disk-image-1',
            'clients': 'perf-client-cblite-disk-image-3',
            'utilities': 'perftest-broker-disk-image',
            'sync_gateways': 'perftest-server-disk-image-1'
        },
        'azure': {
            'clusters': '{}/perf-server-image-def'.format(AZURE_IMAGE_URL_PREFIX),
            'clients': '{}/perf-client-image-def'.format(AZURE_IMAGE_URL_PREFIX),
            'utilities': '{}/perf-broker-image-def'.format(AZURE_IMAGE_URL_PREFIX),
            'sync_gateways': '{}/perf-server-image-def'.format(AZURE_IMAGE_URL_PREFIX)
        }
    }

    def __init__(self, options):
        self.options = options
        self.infra_spec = ClusterSpec()
        self.infra_spec.parse(self.options.cluster)
        self.provider = self.infra_spec.cloud_provider
        self.backend = None
        self.uuid = uuid4().hex[0:6] if self.provider != 'aws' else None
        self.os_arch = self.infra_spec.infrastructure_settings.get('os_arch', 'x86_64')
        self.node_list = {
            'clusters': [
                n for nodes in self.infra_spec.infrastructure_clusters.values()
                for n in nodes.strip().split()
            ],
            'clients': self.infra_spec.clients,
            'utilities': self.infra_spec.utilities,
            'sync_gateways': self.infra_spec.sgw_servers
        }

        self.cloud_storage = bool(
            int(self.infra_spec.infrastructure_settings.get('cloud_storage', 0))
        )

        if self.provider == 'gcp' or \
           (self.provider == 'capella' and self.infra_spec.capella_backend == 'gcp'):
            self.zone = self.options.zone
            self.region = self.zone.rsplit('-', 1)[0]
        else:
            self.zone = None
            self.region = self.options.region

    def deploy(self):
        # Configure terraform
        self.populate_tfvars()
        self.terraform_init(self.provider)

        # Deploy resources
        self.terraform_apply(self.provider)

        # Get info about deployed resources and update cluster spec file
        output = self.terraform_output(self.provider)
        self.update_spec(output)

    def destroy(self):
        self.terraform_destroy(self.provider)

    def create_tfvar_nodes(self):
        tfvar_nodes = {
            'clusters': {},
            'clients': {},
            'utilities': {},
            'sync_gateways': {}
        }

        cloud_provider = self.backend if self.provider == 'capella' else self.provider

        for role, nodes in self.node_list.items():
            # If this is a capella test, skip cluster nodes
            if self.provider == 'capella' and role == 'clusters':
                continue

            i = 0
            for node in nodes:
                node_cluster, node_group = node.split(':')[0].split('.', 2)[1:]
                parameters = self.infra_spec.infrastructure_config()[node_group.split('.')[0]]

                parameters['node_group'] = node_group

                image = self.IMAGE_MAP[cloud_provider][role]
                if cloud_provider == 'aws' and role == 'clusters':
                    image = image.get(self.os_arch, image['x86_64'])
                parameters['image'] = image

                parameters['volume_size'] = int(parameters.get('volume_size', 0))

                storage_class = parameters.get('storage_class', parameters.get('volume_type', None))
                if not storage_class:
                    node_cluster_config = self.infra_spec.infrastructure_section(node_cluster)
                    storage_class = node_cluster_config.get('storage_class')
                parameters['storage_class'] = storage_class

                if 'disk_tier' not in parameters and cloud_provider == 'azure':
                    parameters['disk_tier'] = ""

                if cloud_provider in ('aws', 'gcp'):
                    parameters['iops'] = int(parameters.get('iops', 0))
                    if cloud_provider == 'aws':
                        parameters['volume_throughput'] = int(parameters.get('volume_throughput',
                                                                             0))

                del parameters['instance_capacity']

                tfvar_nodes[role][str(i := i+1)] = parameters

        return tfvar_nodes

    def populate_tfvars(self):
        cloud_provider = self.backend if self.provider == 'capella' else self.provider

        tfvar_nodes = self.create_tfvar_nodes()

        replacements = {
            '<CLOUD_REGION>': self.region,
            '<CLUSTER_NODES>': tfvar_nodes['clusters'],
            '<CLIENT_NODES>': tfvar_nodes['clients'],
            '<UTILITY_NODES>': tfvar_nodes['utilities'],
            '<SYNC_GATEWAY_NODES>': tfvar_nodes['sync_gateways'],
            '<CLOUD_STORAGE>': self.cloud_storage,
            '<GLOBAL_TAG>': self.options.tag if self.options.tag else ""
        }

        if self.uuid:
            replacements['<UUID>'] = self.uuid

        if self.zone:
            replacements['<CLOUD_ZONE>'] = self.zone

        with open('terraform/{}/terraform.tfvars'.format(cloud_provider), 'r+') as tfvars:
            file_string = tfvars.read()

            for k, v in replacements.items():
                file_string = file_string.replace(k, json.dumps(v, indent=4))

            tfvars.seek(0)
            tfvars.write(file_string)

    # Initializes terraform environment.
    def terraform_init(self, provider):
        local('cd terraform/{} && terraform init >> terraform.log'.format(provider))

    # Apply and output terraform deployment.
    def terraform_apply(self, provider):
        local('cd terraform/{} && '
              'terraform plan -out tfplan.out >> terraform.log && '
              'terraform apply -auto-approve tfplan.out'
              .format(provider))

    def terraform_output(self, provider):
        output = json.loads(
            local('cd terraform/{} && terraform output -json'.format(provider), capture=True)
        )
        return output

    def terraform_destroy(self, provider):
        local('cd terraform/{} && '
              'terraform plan -destroy -out tfplan_destroy.out >> terraform.log && '
              'terraform apply -auto-approve tfplan_destroy.out'
              .format(provider))

    # Update spec file with deployed infrastructure.
    def update_spec(self, output):
        sections = ['clusters', 'clients', 'utilities', 'sync_gateways']
        cluster_dicts = [
            self.infra_spec.infrastructure_clusters,
            self.infra_spec.infrastructure_clients,
            self.infra_spec.infrastructure_utilities,
            self.infra_spec.infrastructure_sync_gateways
        ]
        output_keys = [
            'cluster_instance_ips',
            'client_instance_ips',
            'utility_instance_ips',
            'sync_gateway_instance_ips'
        ]
        private_sections = [
            'cluster_private_ips',
            'client_private_ips',
            'utility_private_ips',
            'sync_gateway_private_ips'
        ]

        for section, cluster_dict, output_key, private_section in zip(sections,
                                                                      cluster_dicts,
                                                                      output_keys,
                                                                      private_sections):
            if (section not in self.infra_spec.config.sections()) or \
               (self.provider == 'capella' and section == 'clusters'):
                continue

            for cluster, nodes in cluster_dict.items():
                node_list = nodes.strip().split()
                public_ips = [None for _ in node_list]
                private_ips = [None for _ in node_list]

                for _, info in output[output_key]['value'].items():
                    node_group = info['node_group']
                    for i, node in enumerate(node_list):
                        hostname, *extras = node.split(':', maxsplit=1)
                        if hostname.split('.', 2)[-1] == node_group:
                            public_ip = info['public_ip']
                            if extras:
                                public_ip += ':{}'.format(*extras)
                            public_ips[i] = public_ip
                            if 'private_ip' in info:
                                private_ips[i] = info['private_ip']
                            break

                self.infra_spec.config.set(section, cluster, '\n' + '\n'.join(public_ips))
                if any(private_ips):
                    if private_section not in self.infra_spec.config.sections():
                        self.infra_spec.config.add_section(private_section)
                    self.infra_spec.config.set(private_section, cluster,
                                               '\n' + '\n'.join(private_ips))

        if self.cloud_storage:
            bucket_url = output['cloud_storage']['value']['storage_bucket']
            self.infra_spec.config.set('storage', 'backup', bucket_url)
            if self.provider == 'azure':
                storage_acc = output['cloud_storage']['value']['storage_account']
                self.infra_spec.config.set('storage', 'storage_acc', storage_acc)

        self.infra_spec.update_spec_file()

        with open('cloud/infrastructure/cloud.ini', 'r+') as f:
            s = f.read()
            if self.provider != 'capella':
                s = s.replace("server_list", "\n".join(self.infra_spec.servers))
            s = s.replace("worker_list", "\n".join(self.infra_spec.clients))
            if self.infra_spec.sgw_servers:
                s = s.replace("sgw_list", "\n".join(self.infra_spec.sgw_servers))
            f.seek(0)
            f.write(s)


class CapellaTerraform(Terraform):

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

    def __init__(self, options):
        super().__init__(options)
        self.backend = self.infra_spec.infrastructure_settings['backend']

        if public_api_url := self.options.capella_public_api_url:
            env = public_api_url.removeprefix('https://')\
                                .removesuffix('.nonprod-project-avengers.com')\
                                .split('.', 1)[1]
            self.infra_spec.config.set('infrastructure', 'cbc_env', env)

        if tenant := self.options.capella_tenant:
            self.infra_spec.config.set('infrastructure', 'cbc_tenant', tenant)

        if project := self.options.capella_project:
            self.infra_spec.config.set('infrastructure', 'cbc_project', project)

        self.infra_spec.update_spec_file()

        self.tenant_id = self.infra_spec.infrastructure_settings['cbc_tenant']
        self.project_id = self.infra_spec.infrastructure_settings['cbc_project']

        self.api_client = CapellaAPI(
            'https://cloudapi.{}.nonprod-project-avengers.com'.format(
                self.infra_spec.infrastructure_settings['cbc_env']
            ),
            os.getenv('CBC_SECRET_KEY'),
            os.getenv('CBC_ACCESS_KEY'),
            os.getenv('CBC_USER'),
            os.getenv('CBC_PWD')
        )

        self.use_internal_api = (
            (self.options.capella_cb_version and self.options.capella_ami) or self.backend == 'gcp'
        )
        self.capella_timeout = max(0, self.options.capella_timeout)

    def deploy(self):
        # Configure terraform
        self.populate_tfvars()
        self.terraform_init(self.backend)
        self.terraform_init('capella')

        # Deploy non-capella resources
        self.terraform_apply(self.backend)
        non_capella_output = self.terraform_output(self.backend)

        # Deploy capella cluster
        if self.use_internal_api:
            cluster_id = self.deploy_cluster_internal_api()
        else:
            self.terraform_apply('capella')
            capella_output = self.terraform_output('capella')
            cluster_id = capella_output['cluster_id']['value']

        # Update cluster spec file
        self.update_spec(non_capella_output, cluster_id)

        # Do VPC peering
        if self.options.vpc_peering:
            network_info = non_capella_output['network']['value']
            self.peer_vpc(network_info, cluster_id)

    def destroy(self):
        # Tear down VPC peering connection
        self.destroy_peering_connection()

        # Destroy non-capella resources
        self.terraform_destroy(self.backend)

        # Destroy capella cluster
        use_internal_api = self.infra_spec.infrastructure_settings.get('cbc_use_internal_api', 0)

        if int(use_internal_api):
            cluster_id = self.infra_spec.infrastructure_settings['cbc_cluster']
            self.destroy_cluster_internal_api(cluster_id)
        else:
            self.terraform_destroy('capella')

    def populate_tfvars(self):
        super().populate_tfvars()

        replacements = {
            '<UUID>': self.uuid,
            '<CLUSTER_SETTINGS>': {
                'project_id': self.project_id,
                'provider': self.backend,
                'region': self.region,
                'cidr': self.get_available_cidr()
            },
            '<SERVER_GROUPS>': [
                group for groups in self.create_tfvar_server_groups().values()
                for group in groups
            ]
        }

        with open('terraform/capella/terraform.tfvars', 'r+') as tfvars:
            file_string = tfvars.read()

            for k, v in replacements.items():
                file_string = file_string.replace(k, json.dumps(v, indent=4))

            tfvars.seek(0)
            tfvars.write(file_string)

    def capella_server_group_sizes(self) -> dict:
        server_groups = {}
        for node in self.node_list['clusters']:
            name, services = node.split(':')

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

    def template_capella_server_internal_api(self):
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
        server_groups = self.capella_server_group_sizes()

        cluster_list = []
        for cluster, server_groups in server_groups.items():
            server_list = []
            cluster_params = self.infra_spec.infrastructure_section(cluster)

            for (node_group, services), size in server_groups.items():
                node_group_config = self.infra_spec.infrastructure_section(node_group)

                storage_class = node_group_config.get(
                    'volume_type', node_group_config.get(
                        'storage_class', cluster_params.get('storage_class')
                    )
                ).lower()

                server_group = {
                    'count': size,
                    'services': [{'type': svc} for svc in services],
                    'compute': {
                        'type': node_group_config['instance_type'],
                        'cpu': 0,
                        'memoryInGb': 0
                    },
                    'disk': {
                        'type': storage_class,
                        'sizeInGb': int(node_group_config['volume_size']),
                    }
                }

                if self.infra_spec.capella_backend == 'aws':
                    server_group['disk']['iops'] = int(node_group_config.get('iops', 3000))

                server_list.append(server_group)

            cluster_list.append(server_list)

        return cluster_list

    def deploy_cluster_internal_api(self):
        config = {
            "cidr": self.get_available_cidr(),
            "name": "perf-cluster-{}".format(self.uuid),
            "description": "",
            "projectId": self.project_id,
            "provider": 'hosted{}'.format(self.infra_spec.capella_backend.upper()),
            "region": self.region,
            "singleAZ": True,
            "server": None,
            "specs": self.template_capella_server_internal_api()[0],
            "package": "enterprise"
        }

        logger.info(config)

        if self.options.capella_cb_version and self.options.capella_ami:
            config['overRide'] = {
                'token': os.getenv('CBC_OVERRIDE_TOKEN'),
                'server': self.options.capella_cb_version,
                'image': self.options.capella_ami
            }
            logger.info('Deploying with custom AMI: {}'.format(self.options.capella_ami))

        resp = self.api_client.create_cluster_customAMI(self.tenant_id, config)
        resp.raise_for_status()
        cluster_id = resp.json().get('id')
        logger.info('Initialised cluster deployment for cluster {}'.format(cluster_id))
        logger.info('Saving cluster ID to spec file.')

        self.infra_spec.config.set('infrastructure', 'cbc_cluster', cluster_id)
        self.infra_spec.config.set('infrastructure', 'cbc_use_internal_api', "1")
        self.infra_spec.update_spec_file()

        timeout_mins = self.capella_timeout
        interval_secs = 30
        status = None
        t0 = time()
        while (time() - t0) < timeout_mins * 60:
            status = self.api_client.get_cluster_status(cluster_id).json().get('status')
            logger.info('Cluster state: {}'.format(status))
            if status != 'healthy':
                sleep(interval_secs)
            else:
                break

        if status != 'healthy':
            logger.error('Deployment timed out after {} mins'.format(timeout_mins))
            exit(1)

        return cluster_id

    def destroy_cluster_internal_api(self, cluster_id):
        logger.info('Deleting Capella cluster...')
        resp = self.api_client.delete_cluster(cluster_id)
        resp.raise_for_status()
        logger.info('Capella cluster successfully queued for deletion.')

    def create_tfvar_server_groups(self) -> list[dict]:
        server_group_sizes = self.capella_server_group_sizes()
        tfvar_server_groups = {cluster: [] for cluster in server_group_sizes}

        for cluster, server_groups in server_group_sizes.items():
            cluster_params = self.infra_spec.infrastructure_section(cluster)

            for (node_group, services), size in server_groups.items():
                parameters = self.infra_spec.infrastructure_config()[node_group]

                parameters['instance_capacity'] = size
                parameters['services'] = [
                    self.SERVICES_PERFRUNNER_TO_CAPELLA[svc]for svc in services
                ]

                storage_class = parameters.pop(
                    'volume_type', parameters.get(
                        'storage_class', cluster_params.get('storage_class')
                    )
                ).upper()

                parameters['storage_class'] = storage_class
                parameters['volume_size'] = int(parameters['volume_size'])
                parameters['iops'] = int(parameters.get('iops', 0))

                tfvar_server_groups[cluster].append(parameters)

        return tfvar_server_groups

    def get_available_cidr(self):
        resp = self.api_client.get_deployment_options(self.tenant_id)
        return resp.json().get('suggestedCidr')

    def get_deployed_cidr(self, cluster_id):
        resp = self.api_client.get_cluster_info(cluster_id)
        return resp.json().get('place', {}).get('CIDR')

    def get_hostnames(self, cluster_id):
        resp = self.api_client.get_nodes(tenant_id=self.tenant_id,
                                         project_id=self.project_id,
                                         cluster_id=cluster_id)
        nodes = resp.json()['data']
        nodes = [node['data'] for node in nodes]
        services_per_node = {node['hostname']: node['services'] for node in nodes}

        kv_nodes = []
        non_kv_nodes = []
        for hostname, services in services_per_node.items():
            services_string = ','.join(self.SERVICES_CAPELLA_TO_PERFRUNNER[svc] for svc in services)
            if 'kv' in services_string:
                kv_nodes.append("{}:{}".format(hostname, services_string))
            else:
                non_kv_nodes.append("{}:{}".format(hostname, services_string))

        ret_list = kv_nodes + non_kv_nodes
        return ret_list

    def update_spec(self, non_capella_output, cluster_id):
        super().update_spec(non_capella_output)

        hostnames = self.get_hostnames(cluster_id)
        cluster = self.infra_spec.config.options('clusters')[0]
        self.infra_spec.config.set('clusters', cluster, '\n' + '\n'.join(hostnames))
        self.infra_spec.config.set('infrastructure', 'cbc_cluster', cluster_id)
        if self.use_internal_api:
            self.infra_spec.config.set('infrastructure', 'cbc_use_internal_api', "1")
        self.infra_spec.update_spec_file()

    def peer_vpc(self, network_info, cluster_id):
        logger.info('Setting up VPC peering...')
        if self.infra_spec.capella_backend == 'aws':
            peering_connection = self._peer_vpc_aws(network_info, cluster_id)
        elif self.infra_spec.capella_backend == 'gcp':
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
            resp = self.api_client.create_private_network(
                self.tenant_id, self.project_id, cluster_id, data)
            private_network_id = resp.json()['id']

            # Get AWS CLI commands that we need to run to complete the peering process
            logger.info('Accepting peering request')
            resp = self.api_client.get_private_network(
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
            resp = self.api_client.create_private_network(
                self.tenant_id, self.project_id, cluster_id, data)
            private_network_id = resp.json()['id']

            # Get gcloud commands that we need to run to complete the peering process
            logger.info('Accepting peering request')
            resp = self.api_client.get_private_network(
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
        if self.infra_spec.capella_backend == 'aws':
            self._destroy_peering_connection_aws()
        elif self.infra_spec.capella_backend == 'gcp':
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


class EKSTerraform(Terraform):
    pass


# CLI args.
def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a infrastructure specification file')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('-r', '--region',
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
    parser.add_argument('-z', '--zone',
                        choices=[
                           'us-central1-a',
                           'us-central1-b',
                           'us-central1-c'
                           'us-central1-f',
                           'us-west1-a',
                           'us-west1-b',
                           'us-west1-c'
                        ],
                        default='us-west1-b',
                        help='the cloud zone (GCP)')
    parser.add_argument('--capella-public-api-url',
                        help='public API URL for Capella environment')
    parser.add_argument('--capella-tenant',
                        help='tenant ID for Capella deployment')
    parser.add_argument('--capella-project',
                        help='project ID for Capella deployment')
    parser.add_argument('--capella-cb-version',
                        help='cb version to use for Capella deployment')
    parser.add_argument('--capella-ami',
                        help='custom AMI to use for Capella deployment')
    parser.add_argument('--vpc-peering',
                        action='store_true',
                        help='enable VPC peering for Capella deployment')
    parser.add_argument('--capella-timeout',
                        type=int,
                        default=20,
                        help='Timeout (minutes) for Capella deployment when using internal API')
    parser.add_argument('-t', '--tag',
                        help='Global tag for launched instances.')

    return parser.parse_args()


def destroy():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster)
    if infra_spec.cloud_provider != 'capella':
        deployer = Terraform(args)
    else:
        deployer = CapellaTerraform(args)

    deployer.destroy()


def main():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster)
    if infra_spec.cloud_provider != 'capella':
        deployer = Terraform(args)
    else:
        deployer = CapellaTerraform(args)

    deployer.deploy()

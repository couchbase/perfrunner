import json
from argparse import ArgumentParser
from multiprocessing import set_start_method

import boto3
import yaml

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec

set_start_method("fork")


class Deployer:

    def __init__(self, infra_spec, options):
        self.options = options
        self.cluster_path = options.cluster
        self.infra_spec = infra_spec
        self.settings = self.infra_spec.infrastructure_settings
        self.clusters = self.infra_spec.infrastructure_clusters
        self.clients = self.infra_spec.infrastructure_clients
        self.utilities = self.infra_spec.infrastructure_utilities
        self.infra_config = self.infra_spec.infrastructure_config()
        self.generated_cloud_config_path = self.infra_spec.generated_cloud_config_path
        self.region = options.region

    def deploy(self):
        raise NotImplementedError


class AWSDeployer(Deployer):

    def __init__(self, infra_spec, options):
        super().__init__(infra_spec, options)
        self.desired_infra = self.gen_desired_infrastructure_config()
        self.deployed_infra = {}
        self.vpc_int = 0
        self.ec2client = boto3.client('ec2')
        self.ec2 = boto3.resource('ec2')
        self.cloudformation_client = boto3.client('cloudformation')
        self.eksclient = boto3.client('eks')
        self.iamclient = boto3.client('iam')
        self.eks_cluster_role_path = "cloud/infrastructure/aws/eks/eks_cluster_role.yaml"
        self.eks_node_role_path = "cloud/infrastructure/aws/eks/eks_node_role.yaml"
        self.generated_kube_config_dir = "cloud/infrastructure/generated/kube_configs"
        self.ebs_csi_iam_policy_path = "cloud/infrastructure/aws/eks/ebs-csi-iam-policy.json"
        self.cloud_ini = "cloud/infrastructure/cloud.ini"
        self.os_arch = self.infra_spec.infrastructure_settings.get('os_arch', 'x86_64')

    def gen_desired_infrastructure_config(self):
        desired_infra = {'k8s': {}, 'ec2': {}}
        k8s = self.infra_spec.infrastructure_section('k8s')
        if 'clusters' in list(k8s.keys()):
            desired_k8s_clusters = k8s['clusters'].split(',')
            for desired_k8s_cluster in desired_k8s_clusters:
                k8s_cluster_config = self.infra_spec.infrastructure_section(desired_k8s_cluster)
                for desired_node_group in k8s_cluster_config['node_groups'].split(','):
                    node_group_config = self.infra_spec.infrastructure_section(desired_node_group)
                    k8s_cluster_config[desired_node_group] = node_group_config
                desired_infra['k8s'][desired_k8s_cluster] = k8s_cluster_config
        ec2 = self.infra_spec.infrastructure_section('ec2')
        if 'clusters' in list(ec2.keys()):
            desired_ec2_clusters = ec2['clusters'].split(',')
            for desired_ec2_cluster in desired_ec2_clusters:
                ec2_cluster_config = self.infra_spec.infrastructure_section(desired_ec2_cluster)
                for desired_node_group in ec2_cluster_config['node_groups'].split(','):
                    node_group_config = self.infra_spec.infrastructure_section(desired_node_group)
                    ec2_cluster_config[desired_node_group] = node_group_config
                desired_infra['ec2'][desired_ec2_cluster] = ec2_cluster_config
        return desired_infra

    def write_infra_file(self):
        with open(self.generated_cloud_config_path, 'w+') as fp:
            json.dump(self.deployed_infra, fp, indent=4, sort_keys=True, default=str)

    def create_vpc(self):
        logger.info("Creating VPC...")
        vpc_available = False
        for i in range(0, 5):
            response = self.ec2client.describe_vpcs(
                Filters=[
                    {
                        'Name': 'cidr-block-association.cidr-block',
                        'Values': [
                            '10.{}.0.0/16'.format(i)
                        ]
                    }
                ]
            )
            resp = response['Vpcs']
            if resp:
                continue
            else:
                self.vpc_int = i
                vpc_available = True
                break

        if not vpc_available:
            raise Exception("vpc cidr block already in use")
        response = self.ec2client.create_vpc(
            CidrBlock='10.{}.0.0/16'.format(self.vpc_int),
            AmazonProvidedIpv6CidrBlock=False,
            DryRun=False,
            InstanceTenancy='dedicated',
            TagSpecifications=[
                {'ResourceType': 'vpc',
                 'Tags': [{'Key': 'Use', 'Value': 'CloudPerfTesting'}]}])
        self.deployed_infra['vpc'] = response['Vpc']
        self.write_infra_file()
        waiter = self.ec2client.get_waiter('vpc_available')
        waiter.wait(VpcIds=[self.deployed_infra['vpc']['VpcId']],
                    WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.ec2client.modify_vpc_attribute(
            EnableDnsSupport={
                'Value': True
            },
            VpcId=self.deployed_infra['vpc']['VpcId']
        )
        response = self.ec2client.modify_vpc_attribute(
            EnableDnsHostnames={
                'Value': True
            },
            VpcId=self.deployed_infra['vpc']['VpcId']
        )
        response = self.ec2client.describe_vpcs(
            VpcIds=[self.deployed_infra['vpc']['VpcId']], DryRun=False)
        self.deployed_infra['vpc'] = response['Vpcs'][0]
        self.write_infra_file()

    def create_subnets(self):
        logger.info("Creating subnets...")
        subnets = 0
        self.deployed_infra['vpc']['subnets'] = {}
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = 'k8s_cluster_{}'.format(i)
            if self.region == 'us-east-1':
                availability_zones = ['us-east-1a', 'us-east-1b']
            else:
                availability_zones = ['us-west-2a', 'us-west-2b']
            for az in availability_zones:
                response = self.ec2client.create_subnet(
                    TagSpecifications=[
                        {'ResourceType': 'subnet',
                         'Tags': [
                             {'Key': 'Use',
                              'Value': 'CloudPerfTesting'},
                             {'Key': 'Role',
                              'Value': cluster_name},
                             {'Key': 'kubernetes.io/cluster/{}'.format(cluster_name),
                              'Value': 'shared'}]}],
                    AvailabilityZone=az,
                    CidrBlock='10.{}.{}.0/24'.format(self.vpc_int, subnets+1),
                    VpcId=self.deployed_infra['vpc']['VpcId'],
                    DryRun=False)
                subnets += 1
                subnet_id = response['Subnet']['SubnetId']
                self.deployed_infra['vpc']['subnets'][subnet_id] = response['Subnet']
                self.write_infra_file()

        if len(self.desired_infra['ec2'].keys()) > 0:
            if self.region == 'us-east-1':
                az = 'us-east-1b'
            else:
                az = 'us-west-2b'
            response = self.ec2client.create_subnet(
                TagSpecifications=[
                    {'ResourceType': 'subnet',
                     'Tags': [
                         {'Key': 'Use',
                          'Value': 'CloudPerfTesting'},
                         {'Key': 'Role',
                          'Value': 'ec2'}]}],
                AvailabilityZone=az,
                CidrBlock='10.{}.{}.0/24'.format(self.vpc_int, subnets+1),
                VpcId=self.deployed_infra['vpc']['VpcId'],
                DryRun=False)
            subnets += 1
            subnet_id = response['Subnet']['SubnetId']
            self.deployed_infra['vpc']['subnets'][subnet_id] = response['Subnet']
            self.write_infra_file()

        waiter = self.ec2client.get_waiter('subnet_available')
        waiter.wait(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()),
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.ec2client.describe_subnets(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()), DryRun=False)
        for subnet in response['Subnets']:
            self.deployed_infra['vpc']['subnets'][subnet['SubnetId']] = subnet
        self.write_infra_file()

    def map_public_ip(self):
        logger.info("Mapping public IPs...")
        for subnet in list(self.deployed_infra['vpc']['subnets'].keys()):
            self.ec2client.modify_subnet_attribute(
                MapPublicIpOnLaunch={'Value': True},
                SubnetId=subnet)
        waiter = self.ec2client.get_waiter('subnet_available')
        waiter.wait(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()),
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.ec2client.describe_subnets(
            SubnetIds=list(self.deployed_infra['vpc']['subnets'].keys()), DryRun=False)
        for subnet in response['Subnets']:
            self.deployed_infra['vpc']['subnets'][subnet['SubnetId']] = subnet
        self.write_infra_file()

    def create_internet_gateway(self):
        logger.info("Creating internet gateway...")
        spec = {
            'ResourceType': 'internet-gateway',
            'Tags': [{'Key': 'Use', 'Value': 'CloudPerfTesting'}]}
        response = self.ec2client.create_internet_gateway(
            TagSpecifications=[spec],
            DryRun=False
        )
        self.deployed_infra['vpc']['internet_gateway'] = response['InternetGateway']
        self.write_infra_file()

    def attach_internet_gateway(self):
        logger.info("Attaching internet gateway...")
        self.ec2client.attach_internet_gateway(
            DryRun=False,
            InternetGatewayId=self.deployed_infra['vpc']['internet_gateway']['InternetGatewayId'],
            VpcId=self.deployed_infra['vpc']['VpcId'])
        response = self.ec2client.describe_internet_gateways(
            DryRun=False,
            InternetGatewayIds=[
                self.deployed_infra['vpc']['internet_gateway']['InternetGatewayId']])
        self.deployed_infra['vpc']['internet_gateway'] = response['InternetGateways'][0]
        self.write_infra_file()

    def create_public_routes(self):
        logger.info("Creating public routes...")
        response = self.ec2client.describe_route_tables(
            Filters=[{'Name': 'vpc-id',
                      'Values': [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['vpc']['route_tables'] = response['RouteTables']
        rt_updated = False
        for rt in self.deployed_infra['vpc']['route_tables']:
            if rt['VpcId'] == self.deployed_infra['vpc']['VpcId']:
                response = self.ec2client.create_route(
                    DestinationCidrBlock='0.0.0.0/0',
                    GatewayId=self.deployed_infra['vpc']['internet_gateway']['InternetGatewayId'],
                    RouteTableId=rt['RouteTableId'])
                rt_updated = bool(response['Return'])
        if not rt_updated:
            raise Exception("Failed to update route table")
        response = self.ec2client.describe_route_tables(
            Filters=[{'Name': 'vpc-id',
                      'Values': [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['vpc']['route_tables'] = response['RouteTables']
        self.write_infra_file()

    def create_eks_roles(self):
        if not self.desired_infra['k8s']:
            return
        logger.info("Creating cloudformation eks roles...")
        with open(self.eks_cluster_role_path, 'r') as cf_file:
            cft_template = cf_file.read()
            response = self.cloudformation_client.create_stack(
                StackName='CloudPerfTestingEKSClusterRole',
                TemplateBody=cft_template,
                Capabilities=['CAPABILITY_IAM'],
                DisableRollback=True,
                EnableTerminationProtection=False)
            self.deployed_infra['vpc']['eks_cluster_role_stack_arn'] = response['StackId']
            self.write_infra_file()
        with open(self.eks_node_role_path, 'r') as cf_file:
            cft_template = cf_file.read()
            response = self.cloudformation_client.create_stack(
                StackName='CloudPerfTestingEKSNodeRole',
                TemplateBody=cft_template,
                Capabilities=['CAPABILITY_IAM'],
                DisableRollback=True,
                EnableTerminationProtection=False)
            self.deployed_infra['vpc']['eks_node_role_stack_arn'] = response['StackId']
            self.write_infra_file()
        waiter = self.cloudformation_client.get_waiter('stack_create_complete')
        waiter.wait(
            StackName='CloudPerfTestingEKSClusterRole',
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        waiter.wait(
            StackName='CloudPerfTestingEKSNodeRole',
            WaiterConfig={'Delay': 10, 'MaxAttempts': 120})
        response = self.cloudformation_client.describe_stacks(
            StackName='CloudPerfTestingEKSClusterRole')
        self.deployed_infra['vpc']['eks_cluster_role_iam_arn'] = \
            response['Stacks'][0]['Outputs'][0]['OutputValue']
        self.write_infra_file()
        response = self.cloudformation_client.describe_stacks(
            StackName='CloudPerfTestingEKSNodeRole')
        self.deployed_infra['vpc']['eks_node_role_iam_arn'] = \
            response['Stacks'][0]['Outputs'][0]['OutputValue']
        self.write_infra_file()

    def create_eks_clusters(self):
        if not self.desired_infra['k8s']:
            return
        logger.info("Creating eks clusters...")
        self.deployed_infra['vpc']['eks_clusters'] = {}
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = 'k8s_cluster_{}'.format(i)
            cluster_version = self.infra_spec.kubernetes_version(cluster_name)
            eks_subnets = []
            for subnet_id, subnet_info in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_info['Tags']:
                    if tag['Key'] == 'Role' and tag['Value'] == cluster_name:
                        eks_subnets.append(subnet_id)
            if len(eks_subnets) < 2:
                raise Exception("EKS requires 2 or more subnets")
            response = self.eksclient.create_cluster(
                name=cluster_name,
                version=cluster_version,
                roleArn=self.deployed_infra['vpc']['eks_cluster_role_iam_arn'],
                resourcesVpcConfig={
                    'subnetIds': eks_subnets,
                    'endpointPublicAccess': True,
                    'endpointPrivateAccess': False},
                kubernetesNetworkConfig={'serviceIpv4Cidr': '172.{}.0.0/16'.format(20+i)},
                tags={'Use': 'CloudPerfTesting', 'Role': cluster_name})
            self.deployed_infra['vpc']['eks_clusters'][response['cluster']['name']] = \
                response['cluster']
            self.write_infra_file()
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = 'k8s_cluster_{}'.format(i)
            waiter = self.eksclient.get_waiter('cluster_active')
            waiter.wait(name=cluster_name,
                        WaiterConfig={'Delay': 10, 'MaxAttempts': 600})
            self.deployed_infra['vpc']['eks_clusters'][response['cluster']['name']] = \
                response['cluster']
            self.write_infra_file()
            self.create_kubeconfig(cluster_name)

    def create_kubeconfig(self, cluster_name):
        if not self.desired_infra['k8s']:
            return
        cluster = self.eksclient.describe_cluster(name=cluster_name)
        cluster_cert = cluster["cluster"]["certificateAuthority"]["data"]
        cluster_ep = cluster["cluster"]["endpoint"]
        cluster_arn = cluster["cluster"]["arn"]
        cluster_config = {
            "apiVersion": "v1",
            "clusters": [
                {"cluster": {"server": str(cluster_ep),
                             "certificate-authority-data": str(cluster_cert)},
                 "name": str(cluster_arn)}],
            "users":
                [{"name": str(cluster_arn),
                  "user":
                      {"exec":
                          {"apiVersion": "client.authentication.k8s.io/v1alpha1",
                           "command": "aws",
                           "args":
                               ["--region",
                                "us-west-2",
                                "eks",
                                "get-token",
                                "--cluster-name",
                                cluster_name]}}}],
            "contexts":
                [{"context":
                    {"cluster": str(cluster_arn),
                     "user": str(cluster_arn)},
                  "name": str(cluster_arn)}],
            "current-context": str(cluster_arn)}
        config_path = '{}/{}'.format(self.generated_kube_config_dir, cluster_name)
        with open(config_path, 'w+') as fp:
            yaml.dump(cluster_config, fp, default_flow_style=False)
        self.deployed_infra['vpc']['eks_clusters'][cluster_name]['kube_config'] = cluster_config
        self.deployed_infra['vpc']['eks_clusters'][cluster_name]['kube_config_path'] = config_path
        self.write_infra_file()

    def create_eks_node_groups(self):
        if not self.desired_infra['k8s']:
            return
        logger.info("Creating eks node groups...")
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra['k8s'].items():
            cluster_infra = self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name]
            cluster_infra['node_groups'] = {}
            eks_subnets = []
            for subnet_id, subnet_info in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_info['Tags']:
                    if tag['Key'] == 'Role' and tag['Value'] == k8s_cluster_name:
                        eks_subnets.append(subnet_id)
            if len(eks_subnets) < 2:
                raise Exception("EKS requires 2 or more subnets")
            for node_group in k8s_cluster_spec['node_groups'].split(','):
                resource_path = 'k8s.{}.{}'.format(k8s_cluster_name, node_group)
                labels = {'NodeRoles': None}
                for k, v in self.clusters.items():
                    if 'couchbase' in k:
                        for host in v.split():
                            host_resource, services = host.split(":")
                            if resource_path in host_resource:
                                labels['NodeRoles'] = k
                                for service in services.split(","):
                                    labels['{}_enabled'.format(service)] = 'true'
                for k, v in self.clients.items():
                    if 'workers' in k and resource_path in v:
                        labels['NodeRoles'] = k
                        break
                    if 'backups' in k and resource_path in v:
                        labels['NodeRoles'] = k
                        break
                for k, v in self.utilities.items():
                    if ('brokers' in k or 'operators' in k) and resource_path in v:
                        labels['NodeRoles'] = 'utilities'
                node_group_spec = k8s_cluster_spec[node_group]
                response = self.eksclient.create_nodegroup(
                    clusterName=k8s_cluster_name,
                    nodegroupName=node_group,
                    scalingConfig={
                        'minSize': int(node_group_spec['instance_capacity']),
                        'maxSize': int(node_group_spec['instance_capacity']),
                        'desiredSize': int(node_group_spec['instance_capacity'])
                    },
                    diskSize=int(node_group_spec['volume_size']),
                    subnets=eks_subnets,
                    instanceTypes=[node_group_spec['instance_type']],
                    amiType='AL2_x86_64',
                    remoteAccess={'ec2SshKey': self.infra_spec.aws_key_name},
                    nodeRole=self.deployed_infra['vpc']['eks_node_role_iam_arn'],
                    labels=labels,
                    tags={'Use': 'CloudPerfTesting',
                          'Role': k8s_cluster_name,
                          'SubRole': node_group})
                cluster_infra['node_groups'][node_group] = response['nodegroup']
                self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name] = cluster_infra
                self.write_infra_file()
        waiter = self.eksclient.get_waiter('nodegroup_active')
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra['k8s'].items():
            for node_group in k8s_cluster_spec['node_groups'].split(','):
                waiter.wait(
                    clusterName=k8s_cluster_name,
                    nodegroupName=node_group,
                    WaiterConfig={'Delay': 10, 'MaxAttempts': 600})
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra['k8s'].items():
            cluster_infra = self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name]
            for node_group in k8s_cluster_spec['node_groups'].split(','):
                response = self.eksclient.describe_nodegroup(
                    clusterName=k8s_cluster_name,
                    nodegroupName=node_group)
                cluster_infra['node_groups'][node_group] = response['nodegroup']
                self.deployed_infra['vpc']['eks_clusters'][k8s_cluster_name] = cluster_infra
                self.write_infra_file()

    def create_ec2s(self):
        logger.info("Creating ec2s...")
        self.deployed_infra['vpc']['ec2'] = {}
        if len(list(self.desired_infra['ec2'].keys())) > 0:
            ec2_subnet = None
            for subnet_name, subnet_config in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_config['Tags']:
                    if tag['Key'] == 'Role' and tag['Value'] == 'ec2':
                        ec2_subnet = subnet_name
                        break
                if ec2_subnet is not None:
                    break
            if ec2_subnet is None:
                raise Exception("need at least one subnet with tag ec2 to deploy instances")
            for ec2_cluster_name, ec2_cluster_config in self.desired_infra['ec2'].items():
                for node_group in ec2_cluster_config['node_groups'].split(','):
                    resource_path = 'ec2.{}.{}'.format(ec2_cluster_name, node_group)
                    tags = [{'Key': 'Use', 'Value': 'CloudPerfTesting'},
                            {'Key': 'Role', 'Value': node_group}]
                    node_role = None
                    for k, v in self.clusters.items():
                        if 'couchbase' in k:
                            for host in v.split():
                                host_resource, services = host.split(":")
                                if resource_path in host_resource:
                                    node_role = k
                                    tags.append({'Key': 'NodeRoles', 'Value': k})
                                    break
                            if node_role:
                                break
                    if not node_role:
                        for k, v in self.clients.items():
                            if 'workers' in k and resource_path in v:
                                node_role = k
                                tags.append({'Key': 'NodeRoles', 'Value': k})
                                break
                            if 'backups' in k and resource_path in v:
                                node_role = k
                                tags.append({'Key': 'NodeRoles', 'Value': k})
                                break
                    if not node_role:
                        for k, v in self.utilities.items():
                            if ('brokers' in k or 'operators' in k) and resource_path in v:
                                node_role = 'utilities'
                                tags.append({'Key': 'NodeRoles', 'Value': 'utilities'})
                                break
                    node_group_spec = ec2_cluster_config[node_group]
                    block_device = '/dev/sda1'
                    if "workers" in node_role:  # perf client ami
                        if self.region == 'us-east-1':
                            ami = 'ami-09dcb69fe2852d6cd'
                        else:
                            ami = 'ami-0045ddecdcfa4a45c'
                    elif "couchbase" in node_role:  # perf server ami
                        if self.region == 'us-east-1':
                            if self.os_arch == 'arm':
                                ami = 'ami-0f249abfe3dd01b30'
                                block_device = '/dev/xvda'
                            elif self.os_arch == 'al2':
                                ami = 'ami-09416c5c736392e2c'
                                block_device = '/dev/xvda'
                            else:
                                ami = 'ami-0c6f86d5c61063ccd'
                        else:
                            ami = 'ami-83b400fb'
                    elif "utilities" in node_role:  # perf client ami
                        if self.region == 'us-east-1':
                            ami = 'ami-0d9e5ee360aa02d94'
                        else:
                            ami = 'ami-0c7ae1c909fa076e9'
                    else:
                        raise Exception("ec2 group must include one of: client, server, broker")
                    volume_type = self.os_arch = node_group_spec.get('volume_type', 'gp2')
                    iops = self.os_arch = node_group_spec.get('iops', 0)
                    if int(iops):
                        block_device_mappings = [
                            {'DeviceName': block_device,
                             'Ebs':
                                 {'DeleteOnTermination': True,
                                  'VolumeSize': int(node_group_spec['volume_size']),
                                  'VolumeType': volume_type,
                                  'Encrypted': False,
                                  'Iops': int(iops)}}]
                    else:
                        block_device_mappings = [
                            {'DeviceName': block_device,
                             'Ebs':
                                 {'DeleteOnTermination': True,
                                  'VolumeSize': int(node_group_spec['volume_size']),
                                  'VolumeType': volume_type,
                                  'Encrypted': False}}]
                    if node_group_spec['instance_type'][0] == "t":
                        response = self.ec2.create_instances(
                            BlockDeviceMappings=block_device_mappings,
                            CreditSpecification={'CpuCredits': 'standard'},
                            ImageId=ami,
                            InstanceType=node_group_spec['instance_type'],
                            KeyName=self.infra_spec.aws_key_name,
                            MaxCount=int(node_group_spec['instance_capacity']),
                            MinCount=int(node_group_spec['instance_capacity']),
                            Monitoring={'Enabled': False},
                            SubnetId=ec2_subnet,
                            DisableApiTermination=False,
                            DryRun=False,
                            EbsOptimized=False,
                            InstanceInitiatedShutdownBehavior='terminate',
                            TagSpecifications=[
                                {'ResourceType': 'instance',
                                 'Tags': tags}]
                        )
                    else:
                        response = self.ec2.create_instances(
                            BlockDeviceMappings=block_device_mappings,
                            ImageId=ami,
                            InstanceType=node_group_spec['instance_type'],
                            KeyName=self.infra_spec.aws_key_name,
                            MaxCount=int(node_group_spec['instance_capacity']),
                            MinCount=int(node_group_spec['instance_capacity']),
                            Monitoring={'Enabled': False},
                            SubnetId=ec2_subnet,
                            DisableApiTermination=False,
                            DryRun=False,
                            EbsOptimized=False,
                            InstanceInitiatedShutdownBehavior='terminate',
                            TagSpecifications=[
                                {'ResourceType': 'instance',
                                 'Tags': tags}]
                        )
                    ec2_group = self.deployed_infra['vpc']['ec2'].get(node_group, {})
                    for node in response:
                        ec2_group[node.id] = {
                            "private_ip": ""
                        }
                    self.deployed_infra['vpc']['ec2'][node_group] = ec2_group
                    self.write_infra_file()

            for ec2_group_name, ec2_dict in self.deployed_infra['vpc']['ec2'].items():
                waiter = self.ec2client.get_waiter('instance_status_ok')
                ec2_list = list(ec2_dict.keys())
                waiter.wait(
                    InstanceIds=list(ec2_dict.keys()),
                    DryRun=False,
                    WaiterConfig={'Delay': 10, 'MaxAttempts': 600})
                for ec2_id in ec2_list:
                    instance = self.ec2.Instance(ec2_id)
                    ec2_dict[ec2_id]["public_ip"] = instance.public_ip_address
                    ec2_dict[ec2_id]["public_dns"] = instance.public_dns_name
                    ec2_dict[ec2_id]["private_ip"] = instance.private_ip_address
                self.deployed_infra['vpc']['ec2'][ec2_group_name] = ec2_dict
                self.write_infra_file()

    def open_security_groups(self):
        logger.info("Opening security groups...")
        response = self.ec2client.describe_security_groups(
            Filters=[
                {'Name': 'vpc-id',
                 'Values':
                     [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['security_groups'] = response['SecurityGroups']
        self.write_infra_file()
        for sg in self.deployed_infra['security_groups']:
            self.ec2client.authorize_security_group_ingress(
                GroupId=sg['GroupId'],
                IpPermissions=[
                    {'FromPort': -1,
                     'IpProtocol': '-1',
                     'IpRanges':
                         [{'CidrIp': '0.0.0.0/0'}],
                     'ToPort': -1}])
        response = self.ec2client.describe_security_groups(
            Filters=[
                {'Name': 'vpc-id',
                 'Values':
                     [self.deployed_infra['vpc']['VpcId']]}],
            DryRun=False)
        self.deployed_infra['security_groups'] = response['SecurityGroups']
        self.write_infra_file()

    def setup_eks_csi_driver_iam_policy(self):
        if not self.desired_infra['k8s']:
            return
        logger.info("Attaching EBS CSI Driver policy ARN...")
        with open(self.ebs_csi_iam_policy_path) as f:
            self.iam_policy = json.load(f)
        self.iam_policy = json.dumps(self.iam_policy)
        response = self.iamclient.create_policy(
            PolicyName='CloudPerfTesting-Amazon_EBS_CSI_Driver',
            PolicyDocument=self.iam_policy,
            Description='Cloud Perf Testing IAM Policy to enable EKS EBS Persistent Volumes'
        )
        self.deployed_infra['vpc']['ebs_csi_policy_arn'] = response['Policy']['Arn']
        self.write_infra_file()
        self.iamclient.attach_role_policy(
            RoleName=self.deployed_infra['vpc']['eks_node_role_iam_arn'].split("/")[1],
            PolicyArn=self.deployed_infra['vpc']['ebs_csi_policy_arn']
        )

    def update_infrastructure_spec(self):
        if self.infra_spec.infrastructure_settings['type'] == 'kubernetes':
            remote = RemoteHelper(self.infra_spec)

            with open(self.generated_cloud_config_path) as f:
                self.deployed_infra = json.load(f)

            k8_nodes = {
                node_dict['metadata']['name']:
                    {
                        "labels": node_dict['metadata']['labels'],
                        "addresses": node_dict['status']['addresses']
                    }
                for node_dict in remote.get_nodes()}

            address_replace_list = []
            clusters = self.infra_spec.infrastructure_clusters
            for cluster, hosts in clusters.items():
                for host in hosts.split():
                    address, services = host.split(":")
                    node_group = address.split(".")[2]
                    matching_node = None
                    for node_name, node_spec in k8_nodes.items():
                        if node_spec['labels']['NodeRoles'] != cluster:
                            continue
                        if node_spec['labels']['eks.amazonaws.com/nodegroup'] != node_group:
                            continue

                        has_all_services = True
                        for service in services.split(","):
                            service_enabled = node_spec['labels'].get("{}_enabled"
                                                                      .format(service), 'false')
                            if service_enabled != 'true':
                                has_all_services = False

                        if has_all_services:
                            replace_addr = None
                            for node_addr_dict in node_spec['addresses']:
                                if node_addr_dict['type'] == "ExternalIP":
                                    replace_addr = node_addr_dict['address']
                            if not replace_addr:
                                raise Exception("no replace address found")
                            address_replace_list.append((address, replace_addr))
                            del k8_nodes[node_name]
                            matching_node = node_name
                            break
                    if not matching_node:
                        raise Exception("no matching node found")

                print("cluster: {}, hosts: {}".format(cluster, str(address_replace_list)))

                with open(self.cluster_path) as f:
                    s = f.read()
                with open(self.cluster_path, 'w') as f:
                    for replace_pair in address_replace_list:
                        s = s.replace(replace_pair[0], replace_pair[1])
                    f.write(s)
        else:
            with open(self.generated_cloud_config_path) as f:
                self.deployed_infra = json.load(f)
            clusters = self.infra_spec.infrastructure_clusters
            clients = self.infra_spec.infrastructure_clients
            utilities = self.infra_spec.infrastructure_utilities
            node_group_ips = {}
            for node_group_name, ec2_dict in self.deployed_infra['vpc']['ec2'].items():
                ips = []
                for instance, instance_ips in ec2_dict.items():
                    ips.append(instance_ips['public_dns'])
                node_group_ips[node_group_name] = ips
            for cluster, hosts in clusters.items():
                address_replace_list = []
                for host in hosts.split():
                    address, services = host.split(":")
                    node_group = address.split(".")[2]
                    ip_list = node_group_ips[node_group]
                    next_ip = ip_list.pop(0)
                    node_group_ips[node_group] = ip_list
                    address_replace_list.append((address, next_ip))

                print("cluster: {}, hosts: {}".format(cluster, str(address_replace_list)))

                server_list = ""
                for server_tuple in address_replace_list:
                    print(server_tuple[1])
                    server_list += "{}\n".format(server_tuple[1])
                    print(server_list)
                server_list = server_list.rstrip()

                with open(self.cloud_ini) as f:
                    s = f.read()
                with open(self.cloud_ini, 'w') as f:
                    s = s.replace("server_list", server_list)
                    f.write(s)

                with open(self.cluster_path) as f:
                    s = f.read()
                with open(self.cluster_path, 'w') as f:
                    for replace_pair in address_replace_list:
                        s = s.replace(replace_pair[0], replace_pair[1])
                    f.write(s)

            for cluster, hosts in clients.items():
                address_replace_list = []
                for host in hosts.split():
                    node_group = host.split(".")[2]
                    ip_list = node_group_ips[node_group]
                    next_ip = ip_list.pop(0)
                    node_group_ips[node_group] = ip_list
                    address_replace_list.append((host, next_ip))

                print("clients: {}, hosts: {}".format(cluster, str(address_replace_list)))

                worker_list = ""
                for worker_tuple in address_replace_list:
                    print(worker_tuple[1])
                    worker_list += "{}\n".format(worker_tuple[1])
                    print(worker_list)

                worker_list = worker_list.rstrip()

                with open(self.cloud_ini) as f:
                    s = f.read()
                with open(self.cloud_ini, 'w') as f:
                    s = s.replace("worker_list", worker_list)
                    f.write(s)

                with open(self.cluster_path) as f:
                    s = f.read()
                with open(self.cluster_path, 'w') as f:
                    for replace_pair in address_replace_list:
                        s = s.replace(replace_pair[0], replace_pair[1])
                    f.write(s)

            for cluster, hosts in utilities.items():
                address_replace_list = []
                for host in hosts.split():
                    node_group = host.split(".")[2]
                    ip_list = node_group_ips[node_group]
                    next_ip = ip_list.pop(0)
                    node_group_ips[node_group] = ip_list
                    address_replace_list.append((host, next_ip))

                print("utilities: {}, hosts: {}".format(cluster, str(address_replace_list)))

                with open(self.cluster_path) as f:
                    s = f.read()
                with open(self.cluster_path, 'w') as f:
                    for replace_pair in address_replace_list:
                        s = s.replace(replace_pair[0], replace_pair[1])
                    f.write(s)

    def deploy(self):
        logger.info("Deploying infrastructure...")
        self.create_vpc()
        self.create_subnets()
        self.map_public_ip()
        self.create_internet_gateway()
        self.attach_internet_gateway()
        self.create_public_routes()
        self.create_eks_roles()
        self.create_eks_clusters()
        self.create_eks_node_groups()
        self.create_ec2s()
        self.open_security_groups()
        self.update_infrastructure_spec()
        if self.deployed_infra['vpc'].get('eks_clusters', None) is not None:
            for k, v in self.deployed_infra['vpc']['eks_clusters'].items():
                logger.info("eks cluster {} kube_config available at: {}"
                            .format(k, v['kube_config_path']))
        logger.info("Infrastructure deployment complete")


class AzureDeployer(Deployer):

    def deploy(self):
        pass


class GCPDeployer(Deployer):

    def deploy(self):
        pass


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a infrastructure specification file')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('-r', '--region',
                        choices=['us-east-1', 'us-west-2'],
                        default='us-east-1',
                        help='the cloud region')

    return parser.parse_args()


def main():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster)
    if infra_spec.cloud_infrastructure:
        infra_provider = infra_spec.infrastructure_settings['provider']
        if infra_provider == 'aws':
            deployer = AWSDeployer(infra_spec, args)
        elif infra_provider == 'azure':
            deployer = AzureDeployer(infra_spec, args)
        elif infra_provider == 'gcp':
            deployer = GCPDeployer(infra_spec, args)
        else:
            raise Exception("{} is not a valid infrastructure provider".format(infra_provider))
        try:
            deployer.deploy()
        except Exception as ex:
            with open(infra_spec.generated_cloud_config_path) as f:
                logger.info("infrastructure dump:\n{}".format(pretty_dict(json.load(f))))
            raise ex


if __name__ == '__main__':
    main()

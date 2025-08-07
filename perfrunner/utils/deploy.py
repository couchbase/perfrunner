import json
import os
import time
from argparse import ArgumentParser
from copy import deepcopy
from multiprocessing import set_start_method
from uuid import uuid4

import boto3
import google.auth
import yaml
from google.cloud import compute_v1 as compute
from google.cloud import storage
from google.protobuf.json_format import MessageToDict

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec
from perfrunner.utils.terraform import CloudVMDeployer

set_start_method("fork")


class Deployer:
    def __init__(self, infra_spec: ClusterSpec, options):
        self.options = options
        self.cluster_path = options.cluster
        self.infra_spec = infra_spec
        self.settings = self.infra_spec.infrastructure_settings
        self.clusters = self.infra_spec.infrastructure_clusters
        self.clients = self.infra_spec.infrastructure_clients
        self.syncgateways = self.infra_spec.infrastructure_syncgateways
        self.utilities = self.infra_spec.infrastructure_utilities
        self.infra_config = self.infra_spec.infrastructure_config()
        self.generated_cloud_config_path = self.infra_spec.generated_cloud_config_path
        self.region = options.region
        self.zone = options.zone

    def deploy(self):
        raise NotImplementedError


class AWSDeployer(Deployer):
    UTILITY_NODE_GROUPS = {
        "ec2": "ec2_node_group_utilities",
        "k8s": "k8s_node_group_utilities",
    }

    def __init__(self, infra_spec: ClusterSpec, options):
        super().__init__(infra_spec, options)
        self.utility_node_group_config = {
            "instance_type": CloudVMDeployer.UTILITY_NODE_TYPES["aws"].get(
                self.infra_spec.utility_profile
            ),
            "instance_capacity": 1,
            "volume_size": 20,
        }
        self.desired_infra = self.gen_desired_infrastructure_config()
        self.deployed_infra = {}
        self.vpc_int = 0
        self.ec2client = boto3.client('ec2')
        self.ec2 = boto3.resource('ec2')
        self.s3 = boto3.resource('s3', region_name=self.region)
        self.cloudformation_client = boto3.client('cloudformation')
        self.eksclient = boto3.client('eks')
        self.iamclient = boto3.client('iam')
        self.eks_cluster_role_path = "cloud/infrastructure/aws/eks/eks_cluster_role.yaml"
        self.eks_node_role_path = "cloud/infrastructure/aws/eks/eks_node_role.yaml"
        self.generated_kube_config_dir = "cloud/infrastructure/generated/kube_configs"
        self.ebs_csi_iam_policy_path = "cloud/infrastructure/aws/eks/ebs-csi-iam-policy.json"
        self.cloud_ini = self.settings.get('cloud_ini', 'cloud/infrastructure/cloud.ini')
        self.os_arch = self.settings.get('os_arch', 'x86_64')
        self.deployment_id = self.infra_spec.get_or_create_infrastructure_uuid()
        name_prefix = options.tag.replace("jenkins-", "").lower()
        self.k8s_cluster_name = f"{name_prefix}-{self.deployment_id}"
        self.deployed_infra["kubeconfigs"] = []
        self.deployed_infra["cluster_map"] = {}

    def gen_desired_infrastructure_config(self) -> dict:
        desired_infra = {"k8s": {}, "ec2": {}}

        for infra_type in desired_infra:
            infra_settings = self.infra_spec.infrastructure_section(infra_type)
            if "clusters" in infra_settings:
                desired_clusters = infra_settings["clusters"].split(",")
                for cluster in desired_clusters:
                    cluster_config = self.infra_spec.infrastructure_section(cluster)
                    for node_group in cluster_config["node_groups"].split(","):
                        node_group_config = self.infra_spec.infrastructure_section(node_group)
                        cluster_config[node_group] = node_group_config

                    if self.utilities:
                        utility_node_group = self.UTILITY_NODE_GROUPS[infra_type]
                        cluster_config["node_groups"] += f",{utility_node_group}"
                        cluster_config[utility_node_group] = self.utility_node_group_config

                    desired_infra[infra_type][cluster] = cluster_config

        logger.info(f"Desired infrastructure: {pretty_dict(desired_infra)}")
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
        desired_tenancy = 'dedicated'
        if self.desired_infra['k8s']:
            desired_tenancy = 'default'
        response = self.ec2client.create_vpc(
            CidrBlock='10.{}.0.0/16'.format(self.vpc_int),
            AmazonProvidedIpv6CidrBlock=False,
            DryRun=False,
            InstanceTenancy=desired_tenancy,
            TagSpecifications=[
                {'ResourceType': 'vpc',
                 'Tags': [{'Key': 'Use', 'Value': 'CloudPerfTesting'},
                          {'Key': 'Name', 'Value': self.options.tag}]}])
        self.deployed_infra['vpc'] = response['Vpc']
        self.write_infra_file()
        time.sleep(5)
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
        # When using external clients, deploy two extra subnets for LB (one in each AZ)
        needed_subnets = 2 if self.infra_spec.external_client else 1
        for i in range(1, len(self.desired_infra["k8s"].keys()) + 1):
            cluster_name = f"{self.k8s_cluster_name}_{i}"
            if self.region == 'us-east-1':
                availability_zones = ['us-east-1a', 'us-east-1b']
            else:
                availability_zones = ['us-west-2a', 'us-west-2b']

            tags = [
                {"Key": "Use", "Value": "CloudPerfTesting"},
                {"Key": "Role", "Value": cluster_name},
                {"Key": f"kubernetes.io/cluster/{cluster_name}", "Value": "shared"},
                {"Key": "Name", "Value": self.options.tag},
            ]
            if self.infra_spec.external_client:
                # Make the subnets available for both internal and external load balancers
                tags.append({'Key': 'kubernetes.io/role/internal-elb', 'Value': '1'})
                tags.append({"Key": "kubernetes.io/role/elb", "Value": "1"})
            for az in availability_zones * needed_subnets:
                response = self.ec2client.create_subnet(
                    TagSpecifications=[
                        {'ResourceType': 'subnet',
                         'Tags': tags}],
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
                          'Value': 'ec2'},
                         {'Key': 'Name', 'Value': self.options.tag}]}],
                AvailabilityZone=az,
                CidrBlock='10.{}.{}.0/24'.format(self.vpc_int, subnets+1),
                VpcId=self.deployed_infra['vpc']['VpcId'],
                DryRun=False)
            subnets += 1
            subnet_id = response['Subnet']['SubnetId']
            self.deployed_infra['vpc']['subnets'][subnet_id] = response['Subnet']
            self.write_infra_file()
        time.sleep(5)
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
        time.sleep(5)
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
            'Tags': [{'Key': 'Use', 'Value': 'CloudPerfTesting'},
                     {'Key': 'Name', 'Value': self.options.tag}]}
        response = self.ec2client.create_internet_gateway(
            TagSpecifications=[spec],
            DryRun=False
        )
        time.sleep(5)
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
        time.sleep(5)
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
        time.sleep(5)
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
        node_role_stack_name = f"CloudPerfTestingEKSNodeRole-{self.deployment_id}"
        cluster_role_stack_name = f"CloudPerfTestingEKSClusterRole-{self.deployment_id}"
        with open(self.eks_cluster_role_path, 'r') as cf_file:
            cft_template = cf_file.read()
            response = self.cloudformation_client.create_stack(
                StackName=cluster_role_stack_name,
                TemplateBody=cft_template,
                Capabilities=["CAPABILITY_IAM"],
                DisableRollback=True,
                EnableTerminationProtection=False,
            )
            self.deployed_infra['vpc']['eks_cluster_role_stack_arn'] = response['StackId']
            self.write_infra_file()
        with open(self.eks_node_role_path, 'r') as cf_file:
            cft_template = cf_file.read()
            response = self.cloudformation_client.create_stack(
                StackName=node_role_stack_name,
                TemplateBody=cft_template,
                Capabilities=["CAPABILITY_IAM"],
                DisableRollback=True,
                EnableTerminationProtection=False,
            )
            self.deployed_infra['vpc']['eks_node_role_stack_arn'] = response['StackId']
            self.write_infra_file()
        waiter = self.cloudformation_client.get_waiter('stack_create_complete')
        waiter.wait(
            StackName=cluster_role_stack_name,
            WaiterConfig={"Delay": 10, "MaxAttempts": 120},
        )
        waiter.wait(
            StackName=node_role_stack_name,
            WaiterConfig={"Delay": 10, "MaxAttempts": 120},
        )
        response = self.cloudformation_client.describe_stacks(StackName=cluster_role_stack_name)
        self.deployed_infra['vpc']['eks_cluster_role_iam_arn'] = \
            response['Stacks'][0]['Outputs'][0]['OutputValue']
        self.write_infra_file()
        response = self.cloudformation_client.describe_stacks(StackName=node_role_stack_name)
        self.deployed_infra['vpc']['eks_node_role_iam_arn'] = \
            response['Stacks'][0]['Outputs'][0]['OutputValue']
        self.deployed_infra["node_role_stack"] = node_role_stack_name
        self.deployed_infra["cluster_role_stack"] = cluster_role_stack_name
        self.write_infra_file()

    def create_eks_clusters(self):
        if not self.desired_infra['k8s']:
            return
        logger.info("Creating eks clusters...")
        self.deployed_infra['vpc']['eks_clusters'] = {}
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = f"{self.k8s_cluster_name}_{i}"
            desired_name = f"k8s_cluster_{i}"
            cluster_version = self.infra_spec.kubernetes_version(desired_name)
            eks_subnets = []
            for subnet_id, subnet_info in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_info['Tags']:
                    if tag["Key"] == "Role" and tag["Value"] == cluster_name:
                        eks_subnets.append(subnet_id)
            if len(eks_subnets) < 2:
                raise Exception("EKS requires 2 or more subnets")
            response = self.eksclient.create_cluster(
                name=cluster_name,
                version=cluster_version,
                roleArn=self.deployed_infra["vpc"]["eks_cluster_role_iam_arn"],
                resourcesVpcConfig={
                    "subnetIds": eks_subnets,
                    "endpointPublicAccess": True,
                    "endpointPrivateAccess": False,
                },
                kubernetesNetworkConfig={"serviceIpv4Cidr": "172.{}.0.0/16".format(20 + i)},
                tags={"Use": "CloudPerfTesting", "Role": cluster_name},
            )
            self.deployed_infra['vpc']['eks_clusters'][response['cluster']['name']] = \
                response['cluster']
            self.deployed_infra["cluster_map"].update({desired_name: cluster_name})
            self.write_infra_file()
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = f"{self.k8s_cluster_name}_{i}"
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
                          {"apiVersion": "client.authentication.k8s.io/v1beta1",
                           "command": "aws",
                           "args":
                               ["--region",
                                self.region,
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
        self.deployed_infra["kubeconfigs"].append(config_path)
        self.write_infra_file()

    def _get_ami_type_from_instance_name(self, instance_type):
        # 'AL2_x86_64' for 'x86_64' and 'AL2_ARM_64' for graviton
        name_part = instance_type.split(".")[0]
        if len(name_part) >= 3 and name_part[2] == 'g':
            return 'AL2_ARM_64'
        else:
            return 'AL2_x86_64'

    def create_eks_node_groups(self):
        if not self.desired_infra['k8s']:
            return
        logger.info("Creating eks node groups...")
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra['k8s'].items():
            deployed_cluster_name = self.deployed_infra["cluster_map"].get(k8s_cluster_name)
            cluster_infra = self.deployed_infra["vpc"]["eks_clusters"][deployed_cluster_name]
            cluster_infra['node_groups'] = {}
            eks_subnets = []

            for subnet_id, subnet_info in self.deployed_infra['vpc']['subnets'].items():
                for tag in subnet_info['Tags']:
                    if tag["Key"] == "Role" and tag["Value"] == deployed_cluster_name:
                        eks_subnets.append(subnet_id)
            if len(eks_subnets) < 2:
                raise Exception("EKS requires 2 or more subnets")
            for node_group in k8s_cluster_spec['node_groups'].split(','):
                resource_path = f"k8s.{k8s_cluster_name}.{node_group}"

                if node_group.endswith("utilities"):
                    labels = {"NodeRoles": "utilities"}
                else:
                    labels = {"NodeRoles": None}
                    for k, v in self.clusters.items():
                        if "couchbase" in k and resource_path in v:
                            labels["NodeRoles"] = k
                            break
                    for k, v in self.clients.items():
                        if ("workers" in k or "backups" in k) and resource_path in v:
                            labels["NodeRoles"] = k
                            break
                    for k, v in self.syncgateways.items():
                        if resource_path in v:
                            labels["NodeRoles"] = k
                            break

                node_group_spec = k8s_cluster_spec[node_group]
                response = self.eksclient.create_nodegroup(
                    clusterName=deployed_cluster_name,
                    nodegroupName=node_group,
                    scalingConfig={
                        "minSize": int(node_group_spec["instance_capacity"]),
                        "maxSize": int(node_group_spec["instance_capacity"]),
                        "desiredSize": int(node_group_spec["instance_capacity"]),
                    },
                    diskSize=int(node_group_spec["volume_size"]),
                    subnets=eks_subnets,
                    instanceTypes=[node_group_spec["instance_type"]],
                    amiType=self._get_ami_type_from_instance_name(node_group_spec["instance_type"]),
                    remoteAccess={"ec2SshKey": self.infra_spec.aws_key_name},
                    nodeRole=self.deployed_infra["vpc"]["eks_node_role_iam_arn"],
                    labels=labels,
                    tags={
                        "Use": "CloudPerfTesting",
                        "Role": deployed_cluster_name,
                        "SubRole": node_group,
                    },
                )
                cluster_infra["node_groups"][node_group] = response["nodegroup"]
                self.deployed_infra["vpc"]["eks_clusters"][deployed_cluster_name] = cluster_infra
                self.write_infra_file()
        waiter = self.eksclient.get_waiter("nodegroup_active")
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra["k8s"].items():
            deployed_cluster_name = self.deployed_infra["cluster_map"].get(k8s_cluster_name)
            for node_group in k8s_cluster_spec["node_groups"].split(","):
                waiter.wait(
                    clusterName=deployed_cluster_name,
                    nodegroupName=node_group,
                    WaiterConfig={"Delay": 10, "MaxAttempts": 600},
                )
        for k8s_cluster_name, k8s_cluster_spec in self.desired_infra["k8s"].items():
            deployed_cluster_name = self.deployed_infra["cluster_map"].get(k8s_cluster_name)
            cluster_infra = self.deployed_infra["vpc"]["eks_clusters"][deployed_cluster_name]
            for node_group in k8s_cluster_spec["node_groups"].split(","):
                response = self.eksclient.describe_nodegroup(
                    clusterName=deployed_cluster_name, nodegroupName=node_group
                )
                cluster_infra["node_groups"][node_group] = response["nodegroup"]
                self.deployed_infra["vpc"]["eks_clusters"][deployed_cluster_name] = cluster_infra
                self.write_infra_file()

        # Tag all instances created by the cluster node groups
        self._tag_eks_node_group_instances()

    def _tag_eks_node_group_instances(self):
        filters = [{"Name": "vpc-id", "Values": ["{}".format(self.deployed_infra["vpc"]["VpcId"])]}]
        instances = self.ec2.instances.filter(Filters=filters)
        instance_ids = [instance.instance_id for instance in instances]
        self.ec2client.create_tags(
            Resources=instance_ids, Tags=[{"Key": "Name", "Value": self.options.tag}]
        )

    def create_ec2s(self):
        logger.info("Creating ec2s...")
        self.deployed_infra["vpc"]["ec2"] = {}
        if len(list(self.desired_infra["ec2"].keys())) > 0:
            ec2_subnet = None
            for subnet_name, subnet_config in self.deployed_infra["vpc"]["subnets"].items():
                for tag in subnet_config["Tags"]:
                    if tag["Key"] == "Role" and tag["Value"] == "ec2":
                        ec2_subnet = subnet_name
                        break
                if ec2_subnet is not None:
                    break
            if ec2_subnet is None:
                raise Exception("need at least one subnet with tag ec2 to deploy instances")
            for ec2_cluster_name, ec2_cluster_config in self.desired_infra["ec2"].items():
                for node_group in ec2_cluster_config["node_groups"].split(","):
                    resource_path = "ec2.{}.{}".format(ec2_cluster_name, node_group)
                    tags = [
                        {"Key": "Use", "Value": "CloudPerfTesting"},
                        {"Key": "Role", "Value": node_group},
                        {"Key": "Jenkins Tag", "Value": self.options.tag},
                        {"Key": "Name", "Value": self.options.tag},
                    ]

                    if node_group.endswith("utilities"):
                        node_role = "utilities"
                        tags.append({"Key": "NodeRoles", "Value": "utilities"})
                    else:
                        node_role = None
                        for k, v in self.clusters.items():
                            if "couchbase" in k:
                                for host in v.split():
                                    host_resource, services = host.split(":")
                                    if resource_path in host_resource:
                                        node_role = k
                                        tags.append({"Key": "NodeRoles", "Value": k})
                                        break
                                if node_role:
                                    break
                        if not node_role:
                            for k, v in self.clients.items():
                                if "workers" in k and resource_path in v:
                                    node_role = k
                                    tags.append({"Key": "NodeRoles", "Value": k})
                                    break
                                if "backups" in k and resource_path in v:
                                    node_role = k
                                    tags.append({"Key": "NodeRoles", "Value": k})
                                    break
                        if not node_role:
                            for k, v in self.syncgateways.items():
                                if "syncgateways" in k and resource_path in v:
                                    node_role = k
                                    tags.append({"Key": "NodeRoles", "Value": k})
                                    break

                    node_group_spec = ec2_cluster_config[node_group]
                    block_device = '/dev/sda1'
                    if "workers" in node_role:  # perf client ami
                        if self.region == 'us-east-1':
                            ami = 'ami-0d9789eef66732b62'
                            logger.info("Client AMI: " + str(ami))
                        else:
                            ami = 'ami-0045ddecdcfa4a45c'
                            logger.info("Client AMI: " + str(ami))
                    elif "couchbase" in node_role:  # perf server ami
                        if self.region == 'us-east-1':
                            if self.os_arch == 'arm':
                                ami = 'ami-0f249abfe3dd01b30'
                                logger.info("Server AMI: " + str(ami))
                                block_device = '/dev/xvda'
                            elif self.os_arch == 'al2':
                                ami = 'ami-060e286353d227c32'
                                logger.info("Server AMI: " + str(ami))
                                block_device = '/dev/xvda'
                            else:
                                ami = 'ami-005bce54f0c4e2248'
                        else:
                            logger.info("Server AMI: " + str(ami))
                            ami = 'ami-83b400fb'
                    elif "syncgateway" in node_role:  # perf server ami
                        if self.region == 'us-east-1':
                            ami = 'ami-005bce54f0c4e2248'
                        else:
                            ami = 'ami-83b400fb'
                    elif "utilities" in node_role:  # perf broker ami
                        if self.region == 'us-east-1':
                            ami = 'ami-0d9e5ee360aa02d94'
                            logger.info("Broker AMI: " + str(ami))
                        else:
                            ami = 'ami-0c7ae1c909fa076e9'
                            logger.info("Broker AMI: " + str(ami))
                    else:
                        raise Exception("ec2 group must include one of: client, server, broker")
                    volume_type = node_group_spec.get('volume_type', 'gp2')
                    iops = node_group_spec.get('iops', 0)
                    thoughput = node_group_spec.get('volume_throughput', 0)
                    data_volume = node_group_spec.get('data_volume_size', 0)
                    data_volume_type = node_group_spec.get('data_volume_type', 'gp2')
                    data_iops = node_group_spec.get('data_volume_iops', 3000)
                    data_throughput = node_group_spec.get('data_volume_throughput', 125)
                    if int(thoughput) and int(iops):
                        if int(iops):
                            block_device_mappings = [
                                {'DeviceName': block_device,
                                 'Ebs':
                                     {'DeleteOnTermination': True,
                                      'VolumeSize': int(node_group_spec['volume_size']),
                                      'VolumeType': volume_type,
                                      'Encrypted': False,
                                      'Throughput': int(thoughput),
                                      'Iops': int(iops)}}]
                    elif int(iops):
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
                    if data_volume:
                        block_device_mappings.append(
                            {'DeviceName': '/dev/sdb',
                             'Ebs':
                                 {'DeleteOnTermination': True,
                                  'VolumeSize': int(node_group_spec['data_volume_size']),
                                  'VolumeType': data_volume_type,
                                  'Throughput': int(data_throughput),
                                  'Iops': int(data_iops),
                                  'Encrypted': False}})
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

    def create_s3bucket(self):
        bucket_name = self.infra_spec.backup
        if bucket_name and bucket_name.startswith('s3'):
            bucket_name = "{}-{}".format(bucket_name.split("/")[-1], self.deployment_id)
            logger.info('Creating S3 bucket: {}'.format(bucket_name))
            if self.region == 'us-east-1':
                self.s3.create_bucket(Bucket=bucket_name)
            else:
                self.s3.create_bucket(Bucket=bucket_name,
                                      CreateBucketConfiguration={
                                          'LocationConstraint': self.region})

            self.deployed_infra['storage_bucket'] = bucket_name
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
        logger.info("The security groups are: {}".format(self.deployed_infra['security_groups']))
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
        logger.info("The security groups are: {}".format(self.deployed_infra['security_groups']))
        self.write_infra_file()

    def setup_eks_csi_driver_iam_policy(self):
        if not self.desired_infra['k8s']:
            return
        logger.info("Attaching EBS CSI Driver policy ARN...")
        with open(self.ebs_csi_iam_policy_path) as f:
            self.iam_policy = json.load(f)
        self.iam_policy = json.dumps(self.iam_policy)
        response = self.iamclient.create_policy(
            PolicyName=f"CloudPerfTesting-Amazon_EBS_CSI_Driver-{self.deployment_id}",
            PolicyDocument=self.iam_policy,
            Description="Cloud Perf Testing IAM Policy to enable EKS EBS Persistent Volumes",
        )
        self.deployed_infra['vpc']['ebs_csi_policy_arn'] = response['Policy']['Arn']
        self.write_infra_file()
        self.iamclient.attach_role_policy(
            RoleName=self.deployed_infra['vpc']['eks_node_role_iam_arn'].split("/")[1],
            PolicyArn=self.deployed_infra['vpc']['ebs_csi_policy_arn']
        )

    def add_ebs_csi_driver(self):
        self.setup_eks_csi_driver_iam_policy()
        ebs_csi_driver_addon = 'aws-ebs-csi-driver'
        logger.info('Adding EBS CSI driver addon')
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = f"{self.k8s_cluster_name}_{i}"
            self.eksclient.create_addon(
                clusterName=cluster_name,
                addonName=ebs_csi_driver_addon,
                tags={
                    'Name': self.options.tag
                }
            )
            response = self.eksclient.describe_addon(
                clusterName=cluster_name,
                addonName=ebs_csi_driver_addon
            )
            logger.info('Deployed addon: {}'.format(response.get('addon')))

        waiter = self.eksclient.get_waiter('addon_active')
        for i in range(1, len(self.desired_infra['k8s'].keys()) + 1):
            cluster_name = f"{self.k8s_cluster_name}_{i}"
            waiter.wait(clusterName=cluster_name,
                        addonName=ebs_csi_driver_addon,
                        WaiterConfig={'Delay': 10, 'MaxAttempts': 120})

    def update_infrastructure_spec(self):
        clusters = self.infra_spec.infrastructure_clusters
        clients = self.infra_spec.infrastructure_clients
        sgws = self.infra_spec.infrastructure_syncgateways
        utilities = self.infra_spec.infrastructure_utilities
        with open(self.generated_cloud_config_path) as f:
            self.deployed_infra = json.load(f)

        if self.infra_spec.dynamic_infrastructure:
            remote = RemoteHelper(self.infra_spec)

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
                    address, _ = host.split(":")
                    node_group = address.split(".")[2]
                    for node_name, node_spec in k8_nodes.items():
                        if node_spec['labels']['NodeRoles'] != cluster:
                            continue
                        if node_spec['labels']['eks.amazonaws.com/nodegroup'] != node_group:
                            continue

                        replace_addr = None
                        for node_addr_dict in node_spec["addresses"]:
                            if node_addr_dict["type"] == "ExternalIP":
                                replace_addr = node_addr_dict["address"]
                        if not replace_addr:
                            raise Exception("no replace address found")
                        address_replace_list.append((address, replace_addr))
                        del k8_nodes[node_name]
                        break

                logger.info("cluster: {}, hosts: {}".format(cluster, str(address_replace_list)))

                with open(self.cluster_path) as f:
                    s = f.read()
                with open(self.cluster_path, 'w') as f:
                    for replace_pair in address_replace_list:
                        s = s.replace(replace_pair[0], replace_pair[1], 1)
                    f.write(s)

            for cluster, hosts in sgws.items():
                address_replace_list = []
                for host in hosts.split():
                    node_group = host.split(".")[2]
                    for node_name, node_spec in k8_nodes.items():
                        if node_spec["labels"]["NodeRoles"] != cluster:
                            continue
                        if node_spec["labels"]["eks.amazonaws.com/nodegroup"] != node_group:
                            continue

                        replace_addr = None
                        for node_addr_dict in node_spec["addresses"]:
                            if node_addr_dict["type"] == "ExternalIP":
                                replace_addr = node_addr_dict["address"]
                        address_replace_list.append((host, replace_addr))
                        del k8_nodes[node_name]
                        break

                logger.info(f"sgws: {cluster}, hosts: {str(address_replace_list)}")

                sgw_list = ""
                for sgw_tuple in address_replace_list:
                    sgw_list += f"{sgw_tuple[1]}\n"

                sgw_list = sgw_list.rstrip()

                with open(self.cloud_ini) as f:
                    s = f.read()
                with open(self.cloud_ini, "w") as f:
                    s = s.replace("sgw_list", sgw_list, 1)
                    f.write(s)

                with open(self.cluster_path) as f:
                    s = f.read()
                with open(self.cluster_path, "w") as f:
                    for replace_pair in address_replace_list:
                        s = s.replace(replace_pair[0], replace_pair[1], 1)
                    f.write(s)

        if self.infra_spec.external_client or not self.infra_spec.dynamic_infrastructure:
            backup = self.infra_spec.backup
            node_group_ips = {}
            for node_group_name, ec2_dict in self.deployed_infra['vpc']['ec2'].items():
                ips = []
                for _, instance_ips in ec2_dict.items():
                    ips.append(instance_ips['public_dns'])
                node_group_ips[node_group_name] = ips

            for cluster, hosts in clusters.items():
                if self.infra_spec.external_client:
                    break  # If external client, it is a k8s cluster
                address_replace_list = []
                for host in hosts.split():
                    address, services = host.split(":")
                    node_group = address.split(".")[2]
                    ip_list = node_group_ips[node_group]
                    next_ip = ip_list.pop(0)
                    node_group_ips[node_group] = ip_list
                    address_replace_list.append((address, next_ip))

                logger.info("cluster: {}, hosts: {}".format(cluster, str(address_replace_list)))

                server_list = ""
                for server_tuple in address_replace_list:
                    server_list += "{}\n".format(server_tuple[1])
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
                        s = s.replace(replace_pair[0], replace_pair[1], 1)
                    f.write(s)

            for cluster, hosts in clients.items():
                address_replace_list = []
                for host in hosts.split():
                    node_group = host.split(".")[2]
                    ip_list = node_group_ips[node_group]
                    next_ip = ip_list.pop(0)
                    node_group_ips[node_group] = ip_list
                    address_replace_list.append((host, next_ip))

                logger.info("clients: {}, hosts: {}".format(cluster, str(address_replace_list)))

                worker_list = ""
                for worker_tuple in address_replace_list:
                    worker_list += "{}\n".format(worker_tuple[1])
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
                        s = s.replace(replace_pair[0], replace_pair[1], 1)
                    f.write(s)

            for cluster, hosts in sgws.items():
                address_replace_list = []
                for host in hosts.split():
                    node_group = host.split(".")[2]
                    ip_list = node_group_ips[node_group]
                    next_ip = ip_list.pop(0)
                    node_group_ips[node_group] = ip_list
                    address_replace_list.append((host, next_ip))

                logger.info("sgws: {}, hosts: {}".format(cluster, str(address_replace_list)))

                sgw_list = ""
                for sgw_tuple in address_replace_list:
                    sgw_list += "{}\n".format(sgw_tuple[1])

                sgw_list = sgw_list.rstrip()

                with open(self.cloud_ini) as f:
                    s = f.read()
                with open(self.cloud_ini, 'w') as f:
                    s = s.replace("sgw_list", sgw_list, 1)
                    f.write(s)

                with open(self.cluster_path) as f:
                    s = f.read()
                with open(self.cluster_path, 'w') as f:
                    for replace_pair in address_replace_list:
                        s = s.replace(replace_pair[0], replace_pair[1], 1)
                    f.write(s)

            if utilities:
                utility_ips = node_group_ips[self.UTILITY_NODE_GROUPS["ec2"]]
                logger.info(f"utilities: hosts, hosts: {utility_ips}")
                utility_hosts_line = "hosts = {}\n".format("\n".join(utility_ips))

                with open(self.cluster_path, "r+") as f:
                    lines = f.readlines()
                    in_section = False
                    insert_idx = -1
                    for i, line in enumerate(lines):
                        if line.startswith("[utilities]"):
                            in_section = True

                        if in_section and line.strip() == "":
                            in_section = False
                            insert_idx = i
                            break

                    if insert_idx < 0:
                        logger.interrupt(
                            "Failed to update spec file with utility hosts. "
                            "Could not find utilities section."
                        )
                    lines.insert(insert_idx, utility_hosts_line)
                    f.seek(0)
                    f.writelines(lines)

            # Replace backup storage bucket name in infra spec (if exists)
            with open(self.cluster_path) as f:
                s = f.read()
            with open(self.cluster_path, 'w') as f:
                if storage_bucket := self.deployed_infra.get('storage_bucket', None):
                    s = s.replace(backup, 's3://{}'.format(storage_bucket))
                f.write(s)

    def deploy(self):
        logger.info("Deploying infrastructure...")
        self.create_vpc()
        time.sleep(120)
        self.create_subnets()
        time.sleep(120)
        self.map_public_ip()
        self.create_internet_gateway()
        time.sleep(120)
        self.attach_internet_gateway()
        time.sleep(120)
        self.create_public_routes()
        self.create_eks_roles()
        self.create_eks_clusters()
        self.create_eks_node_groups()
        self.create_ec2s()
        self.create_s3bucket()
        self.open_security_groups()
        self.add_ebs_csi_driver()
        self.update_infrastructure_spec()
        if self.deployed_infra['vpc'].get('eks_clusters', None) is not None:
            for k, v in self.deployed_infra['vpc']['eks_clusters'].items():
                logger.info("eks cluster {} kube_config available at: {}"
                            .format(k, v['kube_config_path']))
        logger.info("Infrastructure deployment complete")


class OpenshiftDeployer(AWSDeployer):

    OPENSHIFT_PATH = 'cloud/infrastructure/generated/openshift'

    def __init__(self, infra_spec: ClusterSpec, options):
        super().__init__(infra_spec, options)
        self.yaml_path = '{}/oc.yaml'.format(self.OPENSHIFT_PATH)
        self.pull_secret = os.environ['OPENSHIFT_PULL_SECRET']
        # make the base url a bit shorter which are in the form of *.cluster_name.domain
        self.cluster_name = options.tag.replace('jenkins-', '').lower()
        self.desired_infra['region'] = options.region
        self.openshift_version = self.infra_spec.infrastructure_section('k8s_cluster_1') \
            .get('version', '4.11')
        self.remote = RemoteHelper(self.infra_spec)

    def deploy(self):
        self.get_oc_binary()
        self.get_desired_infra()
        self.populate_yaml()
        self.create_cluster()
        self.update_labels()

        # Deploy non Openshift resources
        if self.infra_spec.external_client:
            logger.info("Deploying external resources")
            # Purge any k8s resources from desired infra
            self.desired_infra['k8s'] = {}
            logger.info(self.infra_spec)
            super().deploy()

    def get_oc_binary(self):
        os.system('wget -q https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable-{}'
                  '/openshift-install-linux.tar.gz && '
                  'tar xfv openshift-install-linux.tar.gz'.format(self.openshift_version))

    def populate_yaml(self):
        self.find_and_replace(self.yaml_path, 'CLUSTER_NAME', self.cluster_name)
        self.find_and_replace(self.yaml_path, 'INSTANCE_TYPE', self.desired_infra['instance_type'])
        self.find_and_replace(self.yaml_path, 'NODE_COUNT',
                              str(self.desired_infra['instance_count']))
        self.find_and_replace(self.yaml_path, 'REGION', self.desired_infra['region'])
        self.find_and_replace(self.yaml_path, 'PULL_SECRET', '\'{}\''.format(self.pull_secret))

    def update_labels(self):
        nodes = iter(self.remote.get_nodes(selector='node-role.kubernetes.io/worker'))
        # cluster machines will be labelled with cluster and service labels
        for k, v in self.infra_spec.infrastructure_clusters.items():
            if 'couchbase' in k:
                for host in v.split():
                    labels = 'NodeRoles={} '.format(k)
                    node = next(nodes)['metadata']['name']
                    _, services = host.split(":")
                    for service in services.split(','):
                        labels = '{} {}_enabled=true'.format(labels, service)
                    self.remote.add_node_label(node, labels)

        # client machines are either 'workers' or 'backups' machines
        if not self.infra_spec.external_client:
            for k, v in self.infra_spec.infrastructure_clients.items():
                if 'workers' in k or 'backups' in k:
                    for host in v.split():
                        node = next(nodes)['metadata']['name']
                        labels = 'NodeRoles={} '.format(k)
                        self.remote.add_node_label(node, labels)

        # utilities we only need broker nodes
        if not self.infra_spec.external_client:
            if self.infra_spec.infrastructure_utilities:
                node = next(nodes)["metadata"]["name"]
                self.remote.add_node_label(node, "NodeRoles=utilities ")

    def find_and_replace(self, path, find, replace):
        with open(path, 'r') as file:
            file_data = file.read()
        file_data = file_data.replace(find, replace)
        with open(path, 'w') as file:
            file.write(file_data)

    def get_desired_infra(self):
        # For external clients, only deploy server infrastructure in the Openshift cluster
        instance_count = len(self.infra_spec.servers)
        if not self.infra_spec.external_client:
            instance_count = instance_count + len(self.infra_spec.clients) + 1  # 1 broker node
        self.desired_infra['instance_count'] = instance_count
        # Node instance types can't be varied in openshift.
        # Will use k8s_node_group_1 type for everything when we work with internal clients
        self.desired_infra['instance_type'] = self.infra_spec.config.get(
            'k8s_node_group_1', 'instance_type')

    def create_cluster(self):
        os.system('cp {} {}/install-config.yaml'.format(self.yaml_path, self.OPENSHIFT_PATH))
        os.system('./openshift-install create cluster --dir={}'.format(self.OPENSHIFT_PATH))
        os.system(
            'cp {}/auth/kubeconfig cloud/infrastructure/generated/kube_configs/k8s_cluster_1'
            .format(self.OPENSHIFT_PATH))



class AzureDeployer(Deployer):

    def deploy(self):
        pass


class GCPDeployer(Deployer):

    def __init__(self, infra_spec: ClusterSpec, options):
        super().__init__(infra_spec, options)
        self.desired_infra = self.gen_desired_infrastructure_config()
        self.deployed_infra = {'zone': self.zone}
        self.vpc_int = 0
        self.cloud_ini = "cloud/infrastructure/cloud.ini"
        self.project = 'couchbase-qe'
        self.region = self.zone.rsplit('-', 1)[0]
        self.credentials, _ = google.auth.default()
        if hasattr(self.credentials, '_service_account_email'):
            self.service_account = self.credentials._service_account_email
        else:
            raise Exception('The GCP credentials provided do not belong to a service account.')
        self.storage_client = storage.Client(project=self.project, credentials=self.credentials)
        self.instance_client = compute.InstancesClient()
        self.image_client = compute.ImagesClient()
        self.network_client = compute.NetworksClient()
        self.subnet_client = compute.SubnetworksClient()
        self.firewall_client = compute.FirewallsClient()
        self.zone_ops_client = compute.ZoneOperationsClient()
        self.region_ops_client = compute.RegionOperationsClient()
        self.global_ops_client = compute.GlobalOperationsClient()
        self.deployment_id = uuid4().hex

    def gen_desired_infrastructure_config(self):
        desired_infra = {'gce': {}}
        gce = self.infra_spec.infrastructure_section('gce')
        if 'clusters' in list(gce.keys()):
            desired_gce_clusters = gce['clusters'].split(',')
            for desired_gce_cluster in desired_gce_clusters:
                gce_cluster_config = self.infra_spec.infrastructure_section(desired_gce_cluster)
                for desired_node_group in gce_cluster_config['node_groups'].split(','):
                    node_group_config = self.infra_spec.infrastructure_section(desired_node_group)
                    gce_cluster_config[desired_node_group] = node_group_config
                desired_infra['gce'][desired_gce_cluster] = gce_cluster_config
        return desired_infra

    def write_infra_file(self):
        with open(self.generated_cloud_config_path, 'w+') as fp:
            json.dump(self.deployed_infra, fp, indent=4, sort_keys=True, default=str)

    def create_vpc(self):
        logger.info('Creating VPC...')

        vpc_name = "perf-vpc-{}".format(self.deployment_id)
        vpc = compute.Network(
            name=vpc_name,
            auto_create_subnetworks=False,
            routing_config=compute.NetworkRoutingConfig(
                routing_mode='REGIONAL'
            )
        )

        try:
            op = self.network_client.insert_unary(project=self.project, network_resource=vpc)
            self._wait_for_operations([op])
        finally:
            deployed = self.network_client.get(project=self.project, network=vpc_name)
            self.deployed_infra['vpc'] = MessageToDict(deployed._pb)
            self.write_infra_file()

        logger.info('VPC created.')

    def create_subnets(self):
        logger.info('Creating subnet...')

        ip_cidr_range = "10.0.0.0/16"
        vpc_name = self.deployed_infra['vpc']['name']
        subnet_name = "perf-subnet-{}".format(self.deployment_id)

        subnet = compute.Subnetwork(
            name=subnet_name,
            ip_cidr_range=ip_cidr_range,
            network="projects/{}/global/networks/{}".format(self.project, vpc_name),
            private_ip_google_access=True
        )

        try:
            op = self.subnet_client.insert_unary(
                project=self.project,
                region=self.region,
                subnetwork_resource=subnet
            )
            self._wait_for_operations([op])
        finally:
            deployed = self.subnet_client.get(
                project=self.project,
                region=self.region,
                subnetwork=subnet_name
            )
            self.deployed_infra['vpc']['primary_subnet'] = MessageToDict(deployed._pb)
            self.write_infra_file()

        logger.info('All subnets created.')

    def create_firewall_rules(self):
        logger.info('Creating firewall rules...')

        vpc_name = self.deployed_infra['vpc']['name']
        network = "projects/{}/global/networks/{}".format(self.project, vpc_name)
        firewall_configs = [
            {   # Allow all network traffic going from instances in our subnet to any other
                # instance in our VPC, effectively allowing unrestricted network traffic between
                # our instances
                "name": "{}-allow-custom".format(vpc_name),
                "network": network,
                "direction": "INGRESS",
                "allowed": [
                    compute.Allowed(
                        I_p_protocol="all"
                    )
                ],
                "source_ranges": [
                    self.deployed_infra['vpc']['primary_subnet']['ipCidrRange']
                ]
            },
            {   # Allow SSH connections from any IP address (external or internal) to any instance
                # in our VPC
                "name": "{}-allow-ssh".format(vpc_name),
                "network": network,
                "direction": "INGRESS",
                "allowed": [
                    compute.Allowed(
                        I_p_protocol="tcp",
                        ports=[
                            "22"
                        ]
                    )
                ],
                "source_ranges": [
                    "0.0.0.0/0"
                ]
            },
            {   # Allow connections from any IP address to the RabbitMQ ports of broker instances
                "name": "{}-allow-broker".format(vpc_name),
                "network": network,
                "direction": "INGRESS",
                "target_tags": [
                    "broker"
                ],
                "allowed": [
                    compute.Allowed(
                        I_p_protocol="tcp",
                        ports=[
                            "5672"
                        ]
                    )
                ],
                "source_ranges": [
                    "0.0.0.0/0"
                ]
            },
            {
                # Allow connections from any IP address to the couchbase ports
                # (for stats collectors)
                "name": "{}-allow-couchbase".format(vpc_name),
                "network": network,
                "direction": "INGRESS",
                "target_tags": [
                    "server",
                ],
                "allowed": [
                    compute.Allowed(
                        I_p_protocol="tcp",
                        ports=[
                            "4984",
                            "4985",
                            "4988",
                            "8000",
                            "8091-8096",
                            "9998-9999",
                            "18091-18096",
                            "11210"
                        ]
                    )
                ],
                "source_ranges": [
                    "0.0.0.0/0"
                ]
            },
            {
                # Allow connections from any IP address to the sgw ports
                "name": "{}-allow-sgw".format(vpc_name),
                "network": network,
                "direction": "INGRESS",
                "target_tags": [
                    "sgws"
                ],
                "allowed": [
                    compute.Allowed(
                        I_p_protocol="tcp",
                        ports=[
                            "4984",
                            "4985",
                            "4988",
                        ]
                    )
                ],
                "source_ranges": [
                    "0.0.0.0/0"
                ]
            },
            {
                # Allow connections from any IP address to the memcached and cblite ports
                "name": "{}-allow-client".format(vpc_name),
                "network": network,
                "direction": "INGRESS",
                "target_tags": [
                    "client"
                ],
                "allowed": [
                    compute.Allowed(
                        I_p_protocol="tcp",
                        ports=[
                            "4980-5500",
                            "7990-8010"
                        ]
                    )
                ],
                "source_ranges": [
                    "0.0.0.0/0"
                ]
            }
        ]

        try:
            ops = []
            for config in firewall_configs:
                firewall = compute.Firewall(**config)
                op = self.firewall_client.insert_unary(
                    project=self.project,
                    firewall_resource=firewall
                )
                ops.append(op)

            self._wait_for_operations(ops)
        finally:
            request = compute.ListFirewallsRequest(
                project=self.project,
                filter='network = "https://www.googleapis.com/compute/v1/{}"'.format(network)
            )
            deployed = self.firewall_client.list(request=request)
            deployed_dicts = [MessageToDict(firewall._pb) for firewall in deployed]
            self.deployed_infra['vpc']['firewalls'] = deployed_dicts
            self.write_infra_file()

        logger.info('All firewall rules created.')

    def create_gce_instances(self):
        logger.info('Creating instances...')

        self.deployed_infra['vpc']['gce'] = {}
        if len(list(self.desired_infra['gce'].keys())) > 0:
            for gce_cluster_name, gce_cluster_config in self.desired_infra['gce'].items():
                for node_group in gce_cluster_config['node_groups'].split(','):
                    node_group_spec = gce_cluster_config[node_group]
                    num_nodes = int(node_group_spec['instance_capacity'])
                    resource_path = 'gce.{}.{}'.format(gce_cluster_name, node_group)

                    disk_params = []
                    volume_size = int(node_group_spec.get('volume_size', 0))
                    if volume_size:
                        volume_type = node_group_spec.get('volume_type', 'pd-balanced')
                        if volume_type == 'pd-extreme':
                            iops = int(node_group_spec.get('iops', 0))
                        else:
                            iops = 0

                        params = {
                            'disk_size_gb': volume_size,
                            'disk_type': 'zones/{}/diskTypes/{}'.format(self.zone, volume_type),
                        }

                        if iops:
                            params['provisioned_iops'] = iops

                        disk_params.append(params)

                    # A template instance config
                    instance_template = {
                        'name': None,
                        'project': self.project,
                        'zone': self.zone,
                        'machine_type': node_group_spec['instance_type'],
                        'boot_disk_image': None,
                        'subnet': self.deployed_infra['vpc']['primary_subnet']['name'],
                        'network_tier': 'PREMIUM',
                        'labels': {
                            'use': 'cloud_perf_testing',
                            'role': node_group,
                            'node_roles': None
                        },
                        'tags': [],
                        'extra_disk_params': disk_params
                    }

                    # List that will contain configs for all instances in the current node group
                    instances = []

                    # Find all the servers in the current node group,
                    # and add them to the instance list
                    for k, v in self.clusters.items():
                        if 'couchbase' in k:
                            i = 0
                            for host in v.split():
                                host_resource, services = host.split(":")
                                if resource_path in host_resource:
                                    instance_conf = deepcopy(instance_template)
                                    instance_conf['name'] = "perf-{}-{}-{}".format(
                                        k, (i := i+1), self.deployment_id
                                    )
                                    instance_conf['labels']['node_roles'] = k
                                    instance_conf['tags'] = ['server'] + services.split(',')
                                    instance_conf['boot_disk_image'] = \
                                        'perftest-server-disk-image-1'
                                    instances.append(instance_conf)

                    # Find all the clients in the current node group,
                    # and add them to the instance list
                    for k, v in self.clients.items():
                        if 'workers' in k:
                            i = 0
                            for host in v.split():
                                if resource_path in host:
                                    instance_conf = deepcopy(instance_template)
                                    instance_conf['name'] = "perf-{}-{}-{}".format(
                                        k, (i := i+1), self.deployment_id
                                    )
                                    instance_conf['labels']['node_roles'] = k
                                    instance_conf['tags'] = ['client']
                                    instance_conf['boot_disk_image'] = \
                                        'perf-client-cblite-disk-image-3'
                                    instances.append(instance_conf)

                    # Find all the brokers in the current node group,
                    # and add them to the instance list
                    for k, v in self.utilities.items():
                        if 'brokers' in k:
                            i = 0
                            for host in v.split():
                                if resource_path in host:
                                    instance_conf = deepcopy(instance_template)
                                    instance_conf['name'] = "perf-{}-{}-{}".format(
                                        k, (i := i+1), self.deployment_id
                                    )
                                    instance_conf['labels']['node_roles'] = k
                                    instance_conf['tags'] = ['broker']
                                    instance_conf['boot_disk_image'] = \
                                        'perftest-broker-disk-image'
                                    instances.append(instance_conf)

                    # Find all the sync gateways in the current node group,
                    # and add them to the instance list
                    for k, v in self.syncgateways.items():
                        if 'syncgateways' in k:
                            i = 0
                            for host in v.split():
                                if resource_path in host:
                                    instance_conf = deepcopy(instance_template)
                                    instance_conf['name'] = "perf-{}-{}-{}".format(
                                        k, (i := i+1), self.deployment_id
                                    )
                                    instance_conf['labels']['node_roles'] = k
                                    instance_conf['tags'] = ['sgws']
                                    instance_conf['boot_disk_image'] = \
                                        'perftest-server-disk-image-1'
                                    instances.append(instance_conf)

                    if num_nodes != len(instances):
                        raise Exception('Node number mismatch in {}. '
                                        'Found {} nodes but instance capacity is {}.'
                                        .format(node_group, len(instances), num_nodes))

                    logger.info('Launching instances for {}...'.format(node_group))

                    try:
                        ops = []
                        for instance_conf in instances:
                            instance = self._configure_instance(**instance_conf)
                            op = self.instance_client.insert_unary(
                                project=self.project,
                                zone=self.zone,
                                instance_resource=instance
                            )
                            ops.append(op)

                        self._wait_for_operations(ops)
                    finally:
                        instance_names = [conf['name'] for conf in instances]
                        deployed = self._get_deployed_gce_instances(instance_names)

                        gce_group = self.deployed_infra['vpc']['gce'].get(node_group, {})
                        for instance in deployed:
                            network_interface = instance.network_interfaces[0]
                            gce_group[instance.name] = {
                                "private_ip": network_interface.network_i_p,
                                "public_ip": network_interface.access_configs[0].nat_i_p
                            }

                        self.deployed_infra['vpc']['gce'][node_group] = gce_group
                        self.write_infra_file()

                    logger.info('All instances created for {}.'.format(node_group))

        logger.info('All instances created.')

    def _configure_instance(self, name: str, project: str, zone: str, machine_type: str,
                            boot_disk_image: str, subnet: str, network_tier: str, labels: dict,
                            tags: list[str], extra_disk_params: list[dict] = []):
        disks = []

        # Configure the boot disk from the given "disk image" (equivalent to AWS AMI)
        custom_image = self.image_client.get(project=project, image=boot_disk_image)
        boot_init_params = compute.AttachedDiskInitializeParams(
            source_image="projects/{}/global/images/{}".format(project, boot_disk_image),
            disk_size_gb=custom_image.disk_size_gb,
            disk_type="zones/{}/diskTypes/pd-balanced".format(zone)
        )
        boot_disk = compute.AttachedDisk(
            initialize_params=boot_init_params,
            auto_delete=True,
            boot=True
        )

        disks.append(boot_disk)

        # Configure any extra disks
        for params in extra_disk_params:
            init_params = compute.AttachedDiskInitializeParams(**params)
            disk = compute.AttachedDisk(
                initialize_params=init_params,
                auto_delete=True,
                boot=False
            )
            disks.append(disk)

        # This is the account the instance will authenticate with
        service_account = compute.ServiceAccount(
            email=self.service_account,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        # Configure the network interface - the instance will belong to the subnet we created
        access_config = compute.AccessConfig(
            network_tier=network_tier
        )
        network_interface = compute.NetworkInterface(
            access_configs=[access_config],
            subnetwork="projects/{}/regions/{}/subnetworks/{}".format(
                            project, zone.rsplit('-', 1)[0], subnet
                        )
        )

        # Assemble the full instance configuration
        instance = compute.Instance(
            name=name,
            machine_type="projects/{}/zones/{}/machineTypes/{}".format(
                project, zone, machine_type
            ),
            disks=disks,
            service_accounts=[service_account],
            network_interfaces=[network_interface],
            labels=labels,
            tags=compute.Tags(items=tags)
        )

        return instance

    def create_storage_bucket(self):
        bucket_name = self.infra_spec.backup
        prefix = 'gs://'
        if bucket_name and bucket_name.startswith(prefix):
            logger.info('Creating Cloud Storage bucket...')

            bucket_name = "{}-{}".format(bucket_name.split(prefix)[1], self.deployment_id)

            bucket = self.storage_client.bucket(bucket_name)
            bucket.storage_class = 'STANDARD'
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True

            self.storage_client.create_bucket(bucket, location=self.region)
            logger.info('Cloud Storage bucket created.')
            self.deployed_infra['storage_bucket'] = bucket_name
            self.write_infra_file()

    def _wait_for_operations(self, pending_ops: list[compute.Operation]):
        while pending_ops:
            new_ops = []

            for op in pending_ops:
                kwargs = {'project': self.project, 'operation': op.name}
                if op.zone:
                    client = self.zone_ops_client
                    kwargs['zone'] = self.zone
                elif op.region:
                    client = self.region_ops_client
                    kwargs['region'] = self.region
                else:
                    client = self.global_ops_client

                new_op = client.wait(**kwargs)

                if new_op.error:
                    raise Exception('ERROR: ', new_op.error)

                if new_op.warnings:
                    logger.warning('Warnings for operation {}: {}'
                                   .format(new_op.id, new_op.warnings))

                if new_op.status == compute.Operation.Status.DONE:
                    logger.info('Operation {} completed successfully.'.format(new_op.id))
                else:
                    new_ops.append(new_op)

            pending_ops = new_ops

    def _get_deployed_gce_instances(self, instance_names: list[str]):
        instance_filter = ' OR '.join('(name = {})'.format(name) for name in instance_names)
        request = compute.ListInstancesRequest(
            project=self.project,
            zone=self.zone,
            filter=instance_filter
        )
        instance_list = self.instance_client.list(request=request)
        return instance_list

    def update_infrastructure_spec(self):
        with open(self.generated_cloud_config_path) as f:
            self.deployed_infra = json.load(f)

        clusters = self.infra_spec.infrastructure_clusters
        clients = self.infra_spec.infrastructure_clients
        utilities = self.infra_spec.infrastructure_utilities
        sgws = self.infra_spec.infrastructure_syncgateways
        backup = self.infra_spec.backup

        # Build up dictionary of node identifiers and their public and private IP addresses
        node_group_ips = {}
        for node_group_name, gce_dict in self.deployed_infra['vpc']['gce'].items():
            ips = {}
            for instance, instance_ips in gce_dict.items():
                node_num = instance.split('-')[2]
                ips[node_num] = {
                    'public': instance_ips['public_ip'],
                    'private': instance_ips['private_ip']
                }
            node_group_ips[node_group_name] = ips

        internal_ip_section = "\n[cluster_private_ips]\n"

        # Iterate through server clusters and replace node identifiers with IPs in the infra spec
        server_list = ""
        for cluster, hosts in clusters.items():
            public_address_replace_list = []
            private_address_replace_list = []
            internal_ip_section += "{} =\n".format(cluster)
            for host in hosts.split():
                address = host.split(":")[0]
                node_group, node_num = address.split(".")[2:4]
                public_ip = node_group_ips[node_group][node_num]['public']
                private_ip = node_group_ips[node_group][node_num]['private']
                public_address_replace_list.append((address, public_ip))
                private_address_replace_list.append((address, private_ip))
                server_list += "{}\n".format(public_ip)
                internal_ip_section += "        {}\n".format(private_ip)

            logger.info("cluster: {}, hosts: {}".format(cluster, str(public_address_replace_list)))

            # Perform the IP replacement in the infra spec
            with open(self.cluster_path) as f:
                s = f.read()
            with open(self.cluster_path, 'w') as f:
                for replace_pair in public_address_replace_list:
                    s = s.replace(replace_pair[0], replace_pair[1], 1)
                f.write(s)

        # Append the internal IP section to the infra spec
        with open(self.cluster_path, 'a') as f:
            f.write(internal_ip_section)

        # Add all of the servers to the cloud.ini file
        with open(self.cloud_ini) as f:
            s = f.read()
        with open(self.cloud_ini, 'w') as f:
            s = s.replace("server_list", server_list)
            f.write(s)

        # Iterate through client clusters and replace node identifiers with IPs in the infra spec
        worker_list = ""
        for cluster, hosts in clients.items():
            address_replace_list = []
            for host in hosts.split():
                node_group, node_num = host.split(".")[2:4]
                public_ip = node_group_ips[node_group][node_num]['public']
                address_replace_list.append((host, public_ip))
                worker_list += "{}\n".format(public_ip)
            worker_list = worker_list.rstrip()

            logger.info("clients: {}, hosts: {}".format(cluster, str(address_replace_list)))

            with open(self.cluster_path) as f:
                s = f.read()
            with open(self.cluster_path, 'w') as f:
                for replace_pair in address_replace_list:
                    s = s.replace(replace_pair[0], replace_pair[1], 1)
                f.write(s)

        # Add all of the clients to the cloud.ini file
        with open(self.cloud_ini) as f:
            s = f.read()
        with open(self.cloud_ini, 'w') as f:
            s = s.replace("worker_list", worker_list)
            f.write(s)

        # Iterate through sgw clusters and replace node identifiers with IPs in the infra spec
        sgw_list = ""
        for cluster, hosts in sgws.items():
            address_replace_list = []
            for host in hosts.split():
                node_group, node_num = host.split(".")[2:4]
                public_ip = node_group_ips[node_group][node_num]['public']
                address_replace_list.append((host, public_ip))
                sgw_list += "{}\n".format(public_ip)
            sgw_list = sgw_list.rstrip()

            logger.info("sgw: {}, hosts: {}".format(cluster, str(address_replace_list)))

            with open(self.cluster_path) as f:
                s = f.read()
            with open(self.cluster_path, 'w') as f:
                for replace_pair in address_replace_list:
                    s = s.replace(replace_pair[0], replace_pair[1], 1)
                f.write(s)

        # Add all of the clients to the cloud.ini file
        with open(self.cloud_ini) as f:
            s = f.read()
        with open(self.cloud_ini, 'w') as f:
            s = s.replace("sgw_list", sgw_list)
            f.write(s)

        # Iterate through broker clusters and replace node identifiers with IPs in the infra spec
        for cluster, hosts in utilities.items():
            address_replace_list = []
            for host in hosts.split():
                node_group, node_num = host.split(".")[2:4]
                address_replace_list.append(
                    (host, node_group_ips[node_group][node_num]['public'])
                )

            logger.info("utilities: {}, hosts: {}".format(cluster, str(address_replace_list)))

            with open(self.cluster_path) as f:
                s = f.read()
            with open(self.cluster_path, 'w') as f:
                for replace_pair in address_replace_list:
                    s = s.replace(replace_pair[0], replace_pair[1], 1)
                f.write(s)

        # Replace backup storage bucket name in infra spec (if exists)
        with open(self.cluster_path) as f:
            s = f.read()
        with open(self.cluster_path, 'w') as f:
            if storage_bucket := self.deployed_infra.get('storage_bucket', None):
                s = s.replace(backup, 'gs://{}'.format(storage_bucket))
            f.write(s)

    def deploy(self):
        logger.info("Deploying infrastructure...")
        self.create_vpc()
        self.create_subnets()
        self.create_firewall_rules()
        self.create_gce_instances()
        self.create_storage_bucket()
        self.update_infrastructure_spec()
        logger.info("Infrastructure deployment complete.")


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
                        help='the cloud region (AWS)')
    parser.add_argument('-z', '--zone',
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
                        help='the cloud zone (GCP)')
    parser.add_argument('-t', '--tag',
                        default='<None>',
                        help='Global tag for launched instances.')
    parser.add_argument('override',
                        nargs='*',
                        help='custom cluster and/or test settings')

    return parser.parse_args()


def main():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster, override=args.override)
    if infra_spec.cloud_infrastructure:
        infra_provider = infra_spec.cloud_provider
        if infra_provider == 'aws':
            deployer = AWSDeployer(infra_spec, args)
        elif infra_provider == 'openshift':
            deployer = OpenshiftDeployer(infra_spec, args)
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

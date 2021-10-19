import datetime
import json
from argparse import ArgumentParser
from multiprocessing import set_start_method

import boto3

from logger import logger
from perfrunner.settings import ClusterSpec

set_start_method("fork")


class Destroyer:

    def __init__(self, infra_spec, options):
        self.options = options
        self.infra_spec = infra_spec
        self.settings = self.infra_spec.infrastructure_settings
        self.clusters = self.infra_spec.infrastructure_clusters
        self.clients = self.infra_spec.infrastructure_clients
        self.utilities = self.infra_spec.infrastructure_utilities
        self.infra_config = self.infra_spec.infrastructure_config()
        self.generated_cloud_config_path = self.infra_spec.generated_cloud_config_path

    def destroy(self):
        raise NotImplementedError


class AWSDestroyer(Destroyer):

    def __init__(self, infra_spec, options):
        super().__init__(infra_spec, options)
        self.ec2client = boto3.client('ec2')
        self.cloudformation_client = boto3.client('cloudformation')
        self.eksclient = boto3.client('eks')
        self.iamclient = boto3.client('iam')
        self.elbclient = boto3.client('elb')
        with open(self.generated_cloud_config_path) as f:
            self.deployed_infra = json.load(f)

    def delete_eks_node_groups(self):
        logger.info("Deleting eks node groups...")
        eks_clusters = self.deployed_infra['vpc'].get('eks_clusters', {})
        for eks_cluster_name, eks_cluster_config in eks_clusters.items():
            node_groups = eks_cluster_config.get('node_groups', {})
            for node_group_name, node_group_config in node_groups.items():
                try:
                    self.eksclient.delete_nodegroup(
                        clusterName=node_group_config['clusterName'],
                        nodegroupName=node_group_config['nodegroupName'])
                except Exception as ex:
                    logger.info(ex)

    def wait_node_groups_deleted(self):
        logger.info("Waiting for eks node groups deletion to complete...")
        eks_clusters = self.deployed_infra['vpc'].get('eks_clusters', {})
        for eks_cluster_name, eks_cluster_config in eks_clusters.items():
            node_groups = eks_cluster_config.get('node_groups', {})
            for node_group_name, node_group_config in node_groups.items():
                waiter = self.eksclient.get_waiter('nodegroup_deleted')
                try:
                    waiter.wait(
                        clusterName=node_group_config['clusterName'],
                        nodegroupName=node_group_config['nodegroupName'],
                        WaiterConfig={'Delay': 10, 'MaxAttempts': 600})
                except Exception as ex:
                    logger.info(ex)

    def delete_eks_clusters(self):
        logger.info("Deleting eks clusters...")
        eks_clusters = self.deployed_infra['vpc'].get('eks_clusters', {})
        for eks_cluster_name, _ in eks_clusters.items():
            try:
                self.eksclient.delete_cluster(name=eks_cluster_name)
            except Exception as ex:
                logger.info(ex)

    def wait_eks_clusters_deleted(self):
        logger.info("Waiting for eks clusters deletion...")
        eks_clusters = self.deployed_infra['vpc'].get('eks_clusters', {})
        for eks_cluster_name, _ in eks_clusters.items():
            waiter = self.eksclient.get_waiter('cluster_deleted')
            try:
                waiter.wait(name=eks_cluster_name,
                            WaiterConfig={'Delay': 10, 'MaxAttempts': 600})
            except Exception as ex:
                logger.info(ex)

    def delete_ec2s(self):
        logger.info("Deleting ec2s...")
        instance_list = []
        for ec2_group_name, ec2_dict in self.deployed_infra['vpc'].get('ec2', {}).items():
            instance_list += list(ec2_dict.keys())
        if len(instance_list) > 0:
            self.ec2client.terminate_instances(
                InstanceIds=instance_list, DryRun=False)

    def wait_ec2s_deleted(self):
        logger.info("Waiting for ec2s deletion...")
        instance_list = []
        for ec2_group_name, ec2_dict in self.deployed_infra['vpc'].get('ec2', {}).items():
            instance_list += list(ec2_dict.keys())
        if len(instance_list) > 0:
            waiter = self.ec2client.get_waiter('instance_terminated')
            waiter.wait(
                InstanceIds=instance_list,
                DryRun=False,
                WaiterConfig={'Delay': 10, 'MaxAttempts': 600})

    def delete_vpc(self):
        logger.info("Deleting VPC...")
        try:
            self.ec2client.delete_vpc(
                VpcId=self.deployed_infra['vpc']['VpcId'], DryRun=False)
        except Exception as ex:
            logger.info(ex)

    def delete_subnets(self):
        logger.info('Deleting subnets...')
        if not self.deployed_infra['vpc'].get('subnets', None):
            return
        for subnet in self.deployed_infra['vpc']['subnets'].keys():
            try:
                self.ec2client.delete_subnet(
                    SubnetId=subnet, DryRun=False)
            except Exception as ex:
                logger.info(ex)

    def delete_internet_gateway(self):
        logger.info('Deleting internet gateway')
        vpc_id = self.deployed_infra['vpc']['VpcId']
        if not self.deployed_infra['vpc'].get('internet_gateway', None):
            return
        igw_id = self.deployed_infra['vpc']['internet_gateway']['InternetGatewayId']
        try:
            self.ec2client.detach_internet_gateway(
                DryRun=False,
                InternetGatewayId=igw_id,
                VpcId=vpc_id)
        except Exception as ex:
            logger.info(ex)

        try:
            self.ec2client.delete_internet_gateway(
                DryRun=False,
                InternetGatewayId=igw_id)
        except Exception as ex:
            logger.info(ex)

    def delete_eks_roles(self):
        logger.info("Deleting EKS roles")
        try:
            self.cloudformation_client.delete_stack(
                StackName='CloudPerfTestingEKSNodeRole')
        except Exception as ex:
            logger.info(ex)

        try:
            self.cloudformation_client.delete_stack(
                StackName='CloudPerfTestingEKSClusterRole')
        except Exception as ex:
            logger.info(ex)

    def delete_eks_csi_driver_iam_policy(self):
        logger.info("Deleting EKS CSI Driver IAM Policy")

        try:
            self.iamclient.detach_role_policy(
                RoleName=self.deployed_infra['vpc']['eks_node_role_iam_arn'].split("/")[1],
                PolicyArn=self.deployed_infra['vpc']['ebs_csi_policy_arn']
            )
        except Exception as ex:
            logger.info(ex)
        try:
            self.iamclient.delete_policy(
                PolicyArn=self.deployed_infra['vpc']['ebs_csi_policy_arn']
            )
        except Exception as ex:
            logger.info(ex)

    def delete_volumes(self):
        logger.info("Deleting persistent volumes...")
        eks_clusters = self.deployed_infra['vpc'].get('eks_clusters', {})
        response = self.ec2client.describe_volumes(
            Filters=[{'Name': 'tag-key',
                      'Values':
                          ['kubernetes.io/cluster/{}'.format(eks_cluster_name)
                           for eks_cluster_name in eks_clusters.keys()]}],
            DryRun=False,
        )
        volumes = response['Volumes']
        deleted_volumes = []
        for volume in volumes:
            volume_id = volume['VolumeId']
            attachements = volume['Attachments']
            state = volume['State']
            created_date = volume['CreateTime']
            created_date = datetime.datetime(year=created_date.year,
                                             month=created_date.month,
                                             day=created_date.day)
            two_days_ago = datetime.datetime.today() - datetime.timedelta(days=2)
            two_days_ago = datetime.datetime(year=two_days_ago.year,
                                             month=two_days_ago.month,
                                             day=two_days_ago.day)

            if state == 'available' and attachements == [] and two_days_ago < created_date:
                self.ec2client.delete_volume(VolumeId=volume_id, DryRun=False)
                deleted_volumes.append(volume_id)
        if deleted_volumes:
            waiter = self.ec2client.get_waiter('volume_deleted')
            waiter.wait(
                VolumeIds=deleted_volumes,
                DryRun=False,
                WaiterConfig={'Delay': 10, 'MaxAttempts': 600}
            )

    def destroy(self):
        logger.info("Deleting deployed infrastructure: {}"
                    .format(self.generated_cloud_config_path))

        self.delete_eks_node_groups()
        self.delete_ec2s()

        self.wait_node_groups_deleted()
        self.wait_ec2s_deleted()

        self.delete_eks_clusters()

        self.wait_eks_clusters_deleted()

        self.delete_volumes()
        self.delete_eks_csi_driver_iam_policy()
        self.delete_eks_roles()
        self.delete_internet_gateway()
        self.delete_subnets()
        self.delete_vpc()

        logger.info("Destroy complete")


class AzureDestroyer(Destroyer):

    def destroy(self):
        pass


class GCPDestroyer(Destroyer):

    def destroy(self):
        pass


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a infrastructure specification file')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')

    return parser.parse_args()


def main():
    args = get_args()
    infra_spec = ClusterSpec()
    infra_spec.parse(fname=args.cluster)
    infra_provider = infra_spec.infrastructure_settings['provider']
    if infra_spec.cloud_infrastructure:
        if infra_provider == 'aws':
            destroyer = AWSDestroyer(infra_spec, args)
        elif infra_provider == 'azure':
            destroyer = AzureDestroyer(infra_spec, args)
        elif infra_provider == 'gcp':
            destroyer = GCPDestroyer(infra_spec, args)
        else:
            raise Exception("{} is not a valid infrastructure provider".format(infra_provider))
        destroyer.destroy()


if __name__ == '__main__':
    main()

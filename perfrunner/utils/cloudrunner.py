import argparse
import copy
import time
from multiprocessing import set_start_method
from typing import Dict, Iterator, List

import boto3
import yaml

from logger import logger

set_start_method("fork")


class CloudRunner:

    AMI = {
        #  perf-client-2021-03, pyenv python 3.6.12
        'clients': 'ami-04f9b8bb1ea4c3ef6',
        'servers': 'ami-83b400fb',
    }

    AWS_REGION = 'us-west-2'

    EC2_META = 'ec2.yaml'

    EC2_SETTINGS = {
        'KeyName': 'perf-jenkins',
        'Placement': {
            'AvailabilityZone': 'us-west-2a',
        },
        'SecurityGroupIds': ['sg-1aed4460'],  # internal-couchbase
        'SubnetId': 'subnet-40406509',  # PerfVPC
    }

    DEVICE_SETTINGS_IO = {
        'BlockDeviceMappings': [
            {
                'DeviceName': '/dev/sdb',
                'Ebs': {
                    'Encrypted': False,
                    'DeleteOnTermination': True,
                    'Iops': 20000,
                    'VolumeSize': 1000,
                    'VolumeType': 'io1',
                }
            },
        ],
    }

    DEVICE_SETTINGS_GP = {
        'BlockDeviceMappings': [
            {
                'DeviceName': '/dev/sdb',
                'Ebs': {
                    'Encrypted': False,
                    'DeleteOnTermination': True,
                    'VolumeSize': 1000,
                    'VolumeType': 'gp2',
                }
            },
        ],
    }

    MONITORING_INTERVAL = 2  # seconds

    def __init__(self):
        self.ec2 = boto3.resource('ec2', region_name=self.AWS_REGION)
        self.s3 = boto3.resource('s3', region_name=self.AWS_REGION)

    def launch(self, count: int, group: str, instance_type: str, ebs_type: str) -> List[str]:
        instance_settings = copy.deepcopy(self.EC2_SETTINGS)
        if group == 'servers':
            if ebs_type == "io":
                instance_settings.update(**self.DEVICE_SETTINGS_IO)
            elif ebs_type == "gp":
                instance_settings.update(**self.DEVICE_SETTINGS_GP)
            else:
                raise Exception("ebs_type {} not supported".format(ebs_type))

        instances = self.ec2.create_instances(
            ImageId=self.AMI[group],
            InstanceType=instance_type,
            MaxCount=count,
            MinCount=count,
            **instance_settings,
        )
        return [instance.instance_id for instance in instances]

    def initiate_s3(self):
        logger.info('Creating S3 bucket')
        self.s3.create_bucket(Bucket='cb-backup-to-s3-perftest',
                              CreateBucketConfiguration={
                                  'LocationConstraint': self.AWS_REGION})

    def monitor_instances(self, instance_ids: list):
        logger.info('Waiting for all instances to be running')
        time.sleep(self.MONITORING_INTERVAL)
        for instance_id in instance_ids:
            while True:
                instance = self.ec2.Instance(instance_id)
                state = instance.state
                logger.info('{} is in state "{}"'.format(instance_id,
                                                         state['Name']))
                if state['Code']:  # 0 : pending, 16 : running.
                    break
                time.sleep(self.MONITORING_INTERVAL)

    def get_ips(self, instance_ids: List[str]) -> Dict[str, str]:
        ips = {}
        for instance_id in instance_ids:
            instance = self.ec2.Instance(instance_id)
            ips[instance_id] = instance.private_ip_address
        return ips

    def store_ips(self, ips: Dict[str, str], group: str):
        logger.info('Storing information in {}'.format(self.EC2_META))
        with open(self.EC2_META, 'a') as fp:
            meta = {group: ips}
            yaml.dump(meta, fp)

    def read_ids(self) -> List[str]:
        logger.info('Reading information from {}'.format(self.EC2_META))
        ids = []
        with open(self.EC2_META) as fp:
            meta = yaml.load(fp, Loader=yaml.FullLoader)
            for instances in meta.values():
                ids += list(instances)
        return ids

    def terminate(self, instance_ids: Iterator[str]):
        for instance_id in instance_ids:
            logger.info('Terminating: {}'.format(instance_id))
            instance = self.ec2.Instance(instance_id)
            instance.terminate()
        try:
            s3_bucket = self.s3.Bucket('cb-backup-to-s3-perftest')
            s3_bucket.objects.all().delete()
            s3_bucket.delete()
            logger.info('S3 bucket Deleted')
        except Exception:
            pass


def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--num-clients',
                        default=0,
                        type=int)
    parser.add_argument('--num-servers',
                        default=0,
                        type=int)
    parser.add_argument('--client-type',
                        default='c3.4xlarge',
                        choices=['c3.4xlarge',
                                 'c3.8xlarge',
                                 'm5dn.8xlarge',
                                 'c5.24xlarge'])
    parser.add_argument('--server-type',
                        default='m4.2xlarge',
                        choices=['c4.4xlarge',
                                 'c4.8xlarge',
                                 'c5.4xlarge',
                                 'c5.9xlarge',
                                 'm4.2xlarge',
                                 'm4.4xlarge',
                                 'm4.10xlarge',
                                 'm4.16xlarge',
                                 'r4.2xlarge',
                                 'r4.4xlarge',
                                 'r4.8xlarge',
                                 'i3.8xlarge',
                                 'i3.4xlarge',
                                 'i3en.3xlarge',
                                 'c5d.12xlarge',
                                 'r5.2xlarge',
                                 'r5.4xlarge',
                                 'm5ad.4xlarge',
                                 'c5.24xlarge'])
    parser.add_argument('--ebs-type',
                        choices=['io', 'gp'],
                        default='io')
    parser.add_argument('action',
                        choices=['launch', 'terminate'])
    parser.add_argument('--enable-s3',
                        default=0,
                        type=int)

    return parser.parse_args()


def main():
    args = get_args()

    cr = CloudRunner()

    if args.action == 'launch':
        if args.num_servers:
            instance_ids = cr.launch(count=args.num_servers,
                                     group='servers',
                                     instance_type=args.server_type,
                                     ebs_type=args.ebs_type)
            cr.monitor_instances(instance_ids)
            ips = cr.get_ips(instance_ids)
            cr.store_ips(ips, group='servers')
        if args.num_clients:
            instance_ids = cr.launch(count=args.num_clients,
                                     group='clients',
                                     instance_type=args.client_type,
                                     ebs_type=args.ebs_type)
            cr.monitor_instances(instance_ids)
            ips = cr.get_ips(instance_ids)
            cr.store_ips(ips, group='clients')
        if args.enable_s3:
            cr.initiate_s3()
    else:
        instance_ids = cr.read_ids()
        cr.terminate(instance_ids)


if __name__ == '__main__':
    main()

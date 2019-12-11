import argparse
import copy
import time
from typing import Dict, Iterator, List

import boto3
import yaml

from logger import logger


class CloudRunner:

    AMI = {
        'clients': 'ami-dbf9baa3',
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

    DEVICE_SETTINGS = {
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

    MONITORING_INTERVAL = 2  # seconds

    def __init__(self):
        self.ec2 = boto3.resource('ec2', region_name=self.AWS_REGION)

    def launch(self, count: int, group: str, instance_type: str) -> List[str]:
        instance_settings = copy.deepcopy(self.EC2_SETTINGS)
        if group == 'servers':
            instance_settings.update(**self.DEVICE_SETTINGS)

        instances = self.ec2.create_instances(
            ImageId=self.AMI[group],
            InstanceType=instance_type,
            MaxCount=count,
            MinCount=count,
            **instance_settings,
        )
        return [instance.instance_id for instance in instances]

    def monitor_instances(self, instance_ids: list):
        logger.info('Waiting for all instances to be running')

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
            meta = yaml.load(fp)
            for instances in meta.values():
                ids += list(instances)
        return ids

    def terminate(self, instance_ids: Iterator[str]):
        for instance_id in instance_ids:
            logger.info('Terminating: {}'.format(instance_id))
            instance = self.ec2.Instance(instance_id)
            instance.terminate()


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
                                 'c3.8xlarge'])
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
                                 'c5d.12xlarge'])
    parser.add_argument('action',
                        choices=['launch', 'terminate'])

    return parser.parse_args()


def main():
    args = get_args()

    cr = CloudRunner()

    if args.action == 'launch':
        if args.num_servers:
            instance_ids = cr.launch(count=args.num_servers,
                                     group='servers',
                                     instance_type=args.server_type)
            cr.monitor_instances(instance_ids)
            ips = cr.get_ips(instance_ids)
            cr.store_ips(ips, group='servers')
        if args.num_clients:
            instance_ids = cr.launch(count=args.num_clients,
                                     group='clients',
                                     instance_type=args.client_type,)
            cr.monitor_instances(instance_ids)
            ips = cr.get_ips(instance_ids)
            cr.store_ips(ips, group='clients')
    else:
        instance_ids = cr.read_ids()
        cr.terminate(instance_ids)


if __name__ == '__main__':
    main()

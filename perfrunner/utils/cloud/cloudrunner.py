import abc
import argparse
import csv
import os
import time
from typing import Any, Dict, List

from libcloud.compute.base import NodeImage
from libcloud.compute.providers import get_driver
from libcloud.compute.types import NodeState, Provider
from logger import logger


class CloudBase(object):
    __metaclass__ = abc.ABCMeta
    WAIT_TIME = 900
    LOG_FILE = 'cloud_nodes.csv'
    INTERVAL_TIME = 60

    def list_all_nodes(self, driver: get_driver) -> List:
        """ List all nodes present with the account

        :param It takes the driver as input
        """
        return driver.list_nodes()

    @abc.abstractmethod
    def launch_all_nodes(self) -> None:
        """
        Launch all nodes required by the user
        """
        return None

    @abc.abstractmethod
    def destroy_all_nodes(self) -> None:
        """
        Destroy list of nodes present with the Account
        """
        return None

    @abc.abstractmethod
    def status_check(self) -> bool:
        """
        Check status of the cluster, if the nodes are in RUNNING/PENDING state
        """
        return False

    def scheduler(self) -> bool:
        """
        This code will start a scheduler to check instance status in every 30 seconds,
        The event will run until all instance are up or max waiting time is exceeded.
        """
        start_time = time.time()
        retry = 0
        while True:
            if retry % 3:
                status = self.status_check(renew=True)
            else:
                status = self.status_check()
            if status:
                break
            diff_time = time.time() - start_time
            if diff_time >= self.WAIT_TIME:
                break
            time.sleep(self.INTERVAL_TIME)
            retry += 1

        return self.status_check()

    @abc.abstractmethod
    def start_phase(self) -> None:
        """It is the start phase of the test.
        After start phase it will write instance details to a file

        """
        return None

    @abc.abstractmethod
    def end_phase(self) -> None:
        """End of tests all steps.
        End phase will use the file created by start phase and parse for instanceID.
        The instanceID will be used to terminate the nodes. point to be noted here is
        nodes will be terminated, not shut down.

        """
        return None

    @abc.abstractmethod
    def log_nodes(self) -> None:
        """
        Get all nodes and log into a CSV file
        """
        return None


class AwsProvider(CloudBase):

    Cluster = {'server': {'size': 'm4.2xlarge', 'ami': 'ami-d22628ab', 'instance': 1,
                          'name': 'cb_server', 'instance_id': set(), 'security_group': ['perf-server']},
               'client': {'size': 'c3.2xlarge', 'ami': 'ami-09c3cc70', 'instance': 1,
                          'name': 'cb_client', 'instance_id': set(), 'security_group': ['perf-clients']}}

    def __init__(self, args: argparse.Namespace):
        self.instance_id_list = set()
        self.Cluster['client']['instance'] = args.client_instance
        self.Cluster['server']['instance'] = args.server_instance
        self.Cluster['server']['size'] = args.server_size
        self.key = args.key
        self.expected_nodes = 0
        self.driver = self.get_local_driver()
        self.nodes = []

    def get_local_driver(self) -> get_driver:
        cls = get_driver(Provider.EC2)
        return cls(os.getenv('AWS_ACCESS_KEY_ID'),
                   os.getenv('AWS_SECRET_ACCESS_KEY'),
                   region='us-west-2'
                   )

    def is_my_node(self, ec2, state=NodeState.RUNNING) -> bool:
        return ec2.state == state

    def parse_nodes(self, ec2_nodes: Any, v: Dict) -> None:
        if isinstance(ec2_nodes, list):
            v['instance_id'] = set([s.id for s in ec2_nodes])
        else:
            v['instance_id'].add(ec2_nodes.id)

    def get_running_nodes(self) -> List:
        return list(filter(lambda x: x.id in self.instance_id_list,
                           self.list_all_nodes(self.driver)))

    def launch_all_nodes(self) -> None:
        sizes = self.driver.list_sizes()
        for k, v in AwsProvider.Cluster.items():
            image = NodeImage(id=v['ami'], name=None, driver=self.driver)
            logger.info('launching {} nodes'.format(k))
            size = [s for s in sizes if s.id == v['size']][0]
            ec2_nodes = self.driver.create_node(name=v['name'], image=image, size=size,
                                                ex_maxcount=v['instance'], ex_keyname=self.key,
                                                ex_security_groups=v['security_group'],
                                                ex_assign_public_ip=False)
            self.expected_nodes += v['instance']
            self.parse_nodes(ec2_nodes, v)
            self.instance_id_list |= v['instance_id']

    def destroy_all_nodes(self) -> None:
        with open(self.LOG_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.instance_id_list.add(row['instance_id'])
        self.nodes = self.get_running_nodes()
        for ec2 in self.nodes:
            self.driver.destroy_node(ec2)

    def status_check(self, renew=False) -> bool:
        count = 0
        if renew:
            self.driver = self.get_local_driver()
            self.nodes = self.get_running_nodes()
        for ec2 in self.nodes:
            if self.is_my_node(ec2):
                count += 1
        return count == self.expected_nodes

    def log_nodes(self) -> None:
        fieldnames = ('type', 'private_ip', 'instance_id')
        with open(self.LOG_FILE, "w") as f:
            writer = csv.writer(f)
            writer.writerow(fieldnames)
            for ec2 in self.nodes:
                if ec2.id in AwsProvider.Cluster['server']['instance_id']:
                    node_type = 'server'
                else:
                    node_type = 'client'
                writer.writerow((node_type, ec2.private_ips[0], ec2.id))

    def start_phase(self) -> None:
        self.launch_all_nodes()
        if not self.scheduler():
            raise RuntimeError("All nodes created during "
                               "the test are not up after {} seconds".format(self.WAIT_TIME))
        self.log_nodes()

    def end_phase(self) -> None:
        self.destroy_all_nodes()
        if not self.scheduler():
            raise RuntimeError("All nodes created during the test "
                               "are not terminated after {} seconds".format(self.WAIT_TIME))


def get_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', type=str, default='AWS', dest='provider',
                        choices=['AWS'], help='Provider for cloud example: AWS')
    parser.add_argument('-p', type=str, dest='phase', choices=['start', 'end'],
                        help='Phase of test example: start/end')
    parser.add_argument('-n', default=1, type=int,
                        dest='client_instance', help='Number of client instances')
    parser.add_argument('-m', default=1, type=int,
                        dest='server_instance', help='Number of server instances')
    parser.add_argument('-k', type=str,
                        dest='key', help='key to login instances')
    parser.add_argument('-s', type=str, default='m4.2xlarge', dest='server_size',
                        choices=['m4.2xlarge', 'c3.4xlarge'], help='size of server instance')
    return parser.parse_args()


def main():
    options = get_options()
    cloud = None
    if options.provider == 'AWS':
        cloud = AwsProvider(options)
    if options.phase == 'start':
        cloud.start_phase()
    if options.phase == 'end':
        cloud.end_phase()


if __name__ == '__main__':
    main()

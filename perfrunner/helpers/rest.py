import time

import requests

from perfrunner.logger import logger
from perfrunner.helpers import Helper


class RestHelper(Helper):

    def __init__(self, cluster_spec):
        super(RestHelper, self).__init__(cluster_spec)
        self.auth = (self.rest_username, self.rest_password)

    def get(self, **kwargs):
        return requests.get(auth=self.auth, **kwargs)

    def post(self, **kwargs):
        return requests.post(auth=self.auth, **kwargs)

    def set_data_path(self, host_port):
        logger.info('Configuring data paths: {0}'.format(host_port))

        API = 'http://{0}/nodes/self/controller/settings'.format(host_port)
        data = {'path': self.data_path, 'index_path': self.index_path}
        self.post(url=API, data=data)

    def set_auth(self, host_port):
        logger.info('Configuring cluster authentication: {0}'.format(host_port))

        API = 'http://{0}/settings/web'.format(host_port)
        data = {'username': self.rest_username, 'password': self.rest_password,
                'port': 'SAME'}
        self.post(url=API, data=data)

    def set_mem_quota(self, host_port, mem_quota):
        logger.info('Configuring memory quota: {0}'.format(host_port))

        API = 'http://{0}/pools/default'.format(host_port)
        data = {'memoryQuota': mem_quota}
        self.post(url=API, data=data)

    def add_node(self, host_port, new_host):
        logger.info('Adding new node: {0}'.format(new_host))

        API = 'http://{0}/controller/addNode'.format(host_port)
        data = {'hostname': new_host,
                'user': self.rest_username, 'password': self.rest_password}
        self.post(url=API, data=data)

    @staticmethod
    def ns_1(host_port):
        return 'ns_1@{0}'.format(host_port.split(':')[0])

    def rebalance(self, host_port, known_nodes, ejected_nodes):
        logger.info('Starting rebalance')

        API = 'http://{0}/controller/rebalance'.format(host_port)
        known_nodes = ','.join(map(self.ns_1, known_nodes))
        ejected_nodes = ','.join(ejected_nodes)
        data = {'knownNodes': known_nodes, 'ejectedNodes': ejected_nodes}
        self.post(url=API, data=data)

    def get_tasks(self, host_port):
        API = 'http://{0}/pools/default/tasks'.format(host_port)
        return self.get(url=API).json()

    def get_rebalance_status(self, host_port):
        for task in self.get_tasks(host_port):
            if task['type'] == 'rebalance':
                is_running = bool(task['status'] == 'running')
                progress = task.get('progress')
                return is_running, progress

    def monitor_rebalance(self, host_port):
        logger.info('Monitoring rebalance status')
        while True:
            is_running, progress = self.get_rebalance_status(host_port)
            if is_running:
                logger.info('Rebalance progress: {0}'.format(progress))
                time.sleep(10)
            else:
                break
        logger.info('Rebalance successfully completed')

    def create_bucket(self, host_port, name, ram_quota, replica_number=1,
                      replica_index=0):
        logger.info('Adding new bucket: {0}'.format(name))

        API = 'http://{0}/pools/default/buckets'.format(host_port)
        data = {'name': name, 'bucketType': 'membase', 'ramQuotaMB': ram_quota,
                'replicaNumber': replica_number, 'replicaIndex': replica_index,
                'authType': 'sasl', 'saslPassword': ''}
        self.post(url=API, data=data)

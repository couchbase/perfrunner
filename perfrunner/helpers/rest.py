import json
import time
from collections import namedtuple

import requests
from decorator import decorator
from logger import logger
from requests.exceptions import ConnectionError


MAX_RETRY = 5
RETRY_DELAY = 5


@decorator
def retry(method, *args, **kwargs):
    r = namedtuple('request', ['url'])('')
    for _ in range(MAX_RETRY):
        try:
            r = method(*args, **kwargs)
        except ConnectionError:
            time.sleep(RETRY_DELAY * 2)
            continue
        if r.status_code in range(200, 203):
            return r
        else:
            logger.warn(r.text)
            logger.warn('Retrying {}'.format(r.url))
            time.sleep(RETRY_DELAY)
    logger.interrupt('Request {} failed after {} attempts'.format(
        r.url, MAX_RETRY
    ))


class RestHelper(object):

    def __init__(self, cluster_spec):
        self.rest_username, self.rest_password = \
            cluster_spec.get_rest_credentials()
        self.auth = (self.rest_username, self.rest_password)

    @retry
    def get(self, **kwargs):
        return requests.get(auth=self.auth, **kwargs)

    @retry
    def post(self, **kwargs):
        return requests.post(auth=self.auth, **kwargs)

    @retry
    def put(self, **kwargs):
        return requests.put(auth=self.auth, **kwargs)

    def set_data_path(self, host_port, data_path, index_path):
        logger.info('Configuring data paths: {}'.format(host_port))

        api = 'http://{}/nodes/self/controller/settings'.format(host_port)
        data = {
            'path': data_path, 'index_path': index_path
        }
        self.post(url=api, data=data)

    def set_auth(self, host_port):
        logger.info('Configuring cluster authentication: {}'.format(host_port))

        api = 'http://{}/settings/web'.format(host_port)
        data = {
            'username': self.rest_username, 'password': self.rest_password,
            'port': 'SAME'
        }
        self.post(url=api, data=data)

    def set_mem_quota(self, host_port, mem_quota):
        logger.info('Configuring memory quota: {}'.format(host_port))

        api = 'http://{}/pools/default'.format(host_port)
        data = {
            'memoryQuota': mem_quota
        }
        self.post(url=api, data=data)

    def add_node(self, host_port, new_host, uri=None):
        logger.info('Adding new node: {}'.format(new_host))

        if uri:
            api = 'http://{}{}'.format(host_port, uri)
        else:
            api = 'http://{}/controller/addNode'.format(host_port)
        data = {
            'hostname': new_host,
            'user': self.rest_username, 'password': self.rest_password
        }
        self.post(url=api, data=data)

    @staticmethod
    def ns_1(host_port):
        return 'ns_1@{}'.format(host_port.split(':')[0])

    def rebalance(self, host_port, known_nodes, ejected_nodes):
        logger.info('Starting rebalance')

        api = 'http://{}/controller/rebalance'.format(host_port)
        known_nodes = ','.join(map(self.ns_1, known_nodes))
        ejected_nodes = ','.join(map(self.ns_1, ejected_nodes))
        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes
        }
        self.post(url=api, data=data)

    def is_balanced(self, host_port):
        api = 'http://{}/pools/rebalanceStatuses'.format(host_port)
        counters = self.get(url=api).json()['counters']
        return counters['rebalance_start'] == counters['rebalance_success']

    def get_failover_counter(self, host_port):
        api = 'http://{}/pools/rebalanceStatuses'.format(host_port)
        counters = self.get(url=api).json()['counters']
        return counters.get('failover_node')

    def get_tasks(self, host_port):
        api = 'http://{}/pools/default/tasks'.format(host_port)
        return self.get(url=api).json()

    def get_rebalance_status(self, host_port):
        for task in self.get_tasks(host_port):
            if task['type'] == 'rebalance':
                is_running = bool(task['status'] == 'running')
                progress = task.get('progress')
                return is_running, progress

    def create_bucket(self, host_port, name, ram_quota, replica_number=1,
                      replica_index=0, threads_number=None):
        logger.info('Adding new bucket: {}'.format(name))

        api = 'http://{}/pools/default/buckets'.format(host_port)
        data = {
            'name': name, 'bucketType': 'membase', 'ramQuotaMB': ram_quota,
            'replicaNumber': replica_number, 'replicaIndex': replica_index,
            'authType': 'sasl', 'saslPassword': self.rest_password
        }
        if threads_number:
            data['threadsNumber'] = threads_number
        self.post(url=api, data=data)

    def configure_auto_compaction(self, host_port, settings):
        logger.info('Applying auto-compaction settings: {}'.format(settings))

        api = 'http://{}/controller/setAutoCompaction'.format(host_port)
        data = {
            'databaseFragmentationThreshold[percentage]': settings.db_percentage,
            'viewFragmentationThreshold[percentage]': settings.view_percentage,
            'parallelDBAndViewCompaction': str(settings.parallel).lower()
        }
        self.post(url=api, data=data)

    def get_bucket_stats(self, host_port, bucket):
        api = 'http://{}/pools/default/buckets/{}/stats'.format(host_port,
                                                                bucket)
        return self.get(url=api).json()

    def add_remote_cluster(self, host_port, remote_host_port, name):
        logger.info('Adding remote cluster {} with reference {}'.format(
            remote_host_port, name
        ))

        api = 'http://{}/pools/default/remoteClusters'.format(host_port)
        data = {
            'hostname': remote_host_port, 'name': name,
            'username': self.rest_username, 'password': self.rest_password
        }
        self.post(url=api, data=data)

    def start_replication(self, host_port, params):
        logger.info('Starting replication with parameters {}'.format(params))

        api = 'http://{}/controller/createReplication'.format(host_port)
        self.post(url=api, data=params)

    def trigger_bucket_compaction(self, host_port, bucket):
        logger.info('Triggering bucket {} compaction'.format(bucket))

        api = 'http://{}/pools/default/buckets/{}/controller/compactBucket'\
            .format(host_port, bucket)
        self.post(url=api)

    def trigger_index_compaction(self, host_port, ddoc, bucket):
        logger.info('Triggering ddoc {} compaction, bucket {}'.format(
            ddoc, bucket
        ))

        api = 'http://{}/pools/default/buckets/{}/ddocs/_design%2F{}/controller/compactView'\
            .format(host_port, bucket, ddoc)
        self.post(url=api)

    def create_ddoc(self, host_port, bucket, ddoc_name, ddoc):
        logger.info('Creating new ddoc {}, bucket {}'.format(
            ddoc_name, bucket
        ))

        api = 'http://{}/couchBase/{}/_design/{}'.format(
            host_port, bucket, ddoc_name)
        data = json.dumps(ddoc)
        headers = {'Content-type': 'application/json'}
        self.put(url=api, data=data,  headers=headers)

    def query_view(self, host_port, bucket, ddoc_name, view_name, params):
        logger.info('Querying view: {}/_design/{}/_view/{}'.format(
            bucket, ddoc_name, view_name
        ))

        api = 'http://{}/couchBase/{}/_design/{}/_view/{}'.format(
            host_port, bucket, ddoc_name, view_name)
        self.get(url=api, params=params)

    def get_version(self, host_port):
        logger.info('Getting server version')

        api = 'http://{}/pools/'.format(host_port)
        r = self.get(url=api).json()
        return r['implementationVersion']\
            .replace('-rel-enterprise', '').replace('-community', '')

    def get_master_events(self, host_port):
        logger.info('Getting master events from {}'.format(host_port))

        api = 'http://{}/diag/masterEvents?o=1'.format(host_port)
        return self.get(url=api).text

    def get_logs(self, host_port):
        logger.info('Getting web logs from {}'.format(host_port))

        api = 'http://{}/logs'.format(host_port)
        return self.get(url=api).json()

    def set_internal_settings(self, host_port, data):
        logger.info('Updating internal settings: {}'.format(data))

        api = 'http://{}/internalSettings'.format(host_port)
        return self.post(url=api, data=data)

    def run_diag_eval(self, host_port, cmd):
        api = 'http://{}/diag/eval'.format(host_port)
        return self.post(url=api, data=cmd).text

    def enable_auto_failover(self, host_port, timeout=45):
        logger.info('Enabling auto-failover')

        api = 'http://{}/settings/autoFailover'.format(host_port)
        data = {'enabled': 'true', 'timeout': timeout}
        self.post(url=api, data=data)

    def create_server_group(self, host_port, name):
        logger.info('Creating server group: {}'.format(name))

        api = 'http://{}/pools/default/serverGroups'.format(host_port)
        data = {'name': name}
        self.post(url=api, data=data)

    def get_server_groups(self, host_port):
        logger.info('Getting server groups')

        api = 'http://{}/pools/default/serverGroups'.format(host_port)
        return {
            g["name"]: g["addNodeURI"] for g in self.get(url=api).json()["groups"]
        }

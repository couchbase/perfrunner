import json
import time
from collections import namedtuple
from decorator import decorator

import requests
from requests.exceptions import ConnectionError
from logger import logger


@decorator
def retry(method, *args, **kwargs):

    MAX_RETRY = 5
    RETRY_DELAY = 5

    r = namedtuple('request', ['url'])('')
    for retry in range(MAX_RETRY):
        try:
            r = method(*args, **kwargs)
        except ConnectionError:
            time.sleep(RETRY_DELAY * 2)
            continue
        if r.status_code in range(200, 203):
            return r
        else:
            logger.warn(r.text)
            logger.warn('Retrying {0}'.format(r.url))
            time.sleep(RETRY_DELAY)
    logger.interrupt('Request {0} failed after {1} attempts'.format(
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
        logger.info('Configuring data paths: {0}'.format(host_port))

        API = 'http://{0}/nodes/self/controller/settings'.format(host_port)
        data = {
            'path': data_path, 'index_path': index_path
        }
        self.post(url=API, data=data)

    def set_auth(self, host_port):
        logger.info('Configuring cluster authentication: {0}'.format(host_port))

        API = 'http://{0}/settings/web'.format(host_port)
        data = {
            'username': self.rest_username, 'password': self.rest_password,
            'port': 'SAME'
        }
        self.post(url=API, data=data)

    def set_mem_quota(self, host_port, mem_quota):
        logger.info('Configuring memory quota: {0}'.format(host_port))

        API = 'http://{0}/pools/default'.format(host_port)
        data = {
            'memoryQuota': mem_quota
        }
        self.post(url=API, data=data)

    def add_node(self, host_port, new_host):
        logger.info('Adding new node: {0}'.format(new_host))

        API = 'http://{0}/controller/addNode'.format(host_port)
        data = {
            'hostname': new_host,
            'user': self.rest_username, 'password': self.rest_password
        }
        self.post(url=API, data=data)

    @staticmethod
    def ns_1(host_port):
        return 'ns_1@{0}'.format(host_port.split(':')[0])

    def rebalance(self, host_port, known_nodes, ejected_nodes):
        logger.info('Starting rebalance')

        API = 'http://{0}/controller/rebalance'.format(host_port)
        known_nodes = ','.join(map(self.ns_1, known_nodes))
        ejected_nodes = ','.join(map(self.ns_1, ejected_nodes))
        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes
        }
        self.post(url=API, data=data)

    def is_balanced(self, host_port):
        API = 'http://{0}/pools/rebalanceStatuses'.format(host_port)
        try:
            return self.get(url=API).json()['balanced']
        except KeyError:
            logger.warn('Deprecated "balanced"')
            return True

    def get_tasks(self, host_port):
        API = 'http://{0}/pools/default/tasks'.format(host_port)
        return self.get(url=API).json()

    def get_rebalance_status(self, host_port):
        for task in self.get_tasks(host_port):
            if task['type'] == 'rebalance':
                is_running = bool(task['status'] == 'running')
                progress = task.get('progress')
                return is_running, progress

    def create_bucket(self, host_port, name, ram_quota, replica_number=1,
                      replica_index=0, threads_number=None):
        logger.info('Adding new bucket: {0}'.format(name))

        API = 'http://{0}/pools/default/buckets'.format(host_port)
        data = {
            'name': name, 'bucketType': 'membase', 'ramQuotaMB': ram_quota,
            'replicaNumber': replica_number, 'replicaIndex': replica_index,
            'authType': 'sasl', 'saslPassword': self.rest_password
        }
        if threads_number:
            data['threadsNumber'] = threads_number
        self.post(url=API, data=data)

    def configure_auto_compaction(self, host_port, settings):
        logger.info('Applying auto-compaction settings: {0}'.format(settings))

        API = 'http://{0}/controller/setAutoCompaction'.format(host_port)
        data = {
            'databaseFragmentationThreshold[percentage]': settings.db_percentage,
            'viewFragmentationThreshold[percentage]': settings.view_percentage,
            'parallelDBAndViewCompaction': str(settings.parallel).lower()
        }
        self.post(url=API, data=data)

    def get_bucket_stats(self, host_port, bucket):
        API = 'http://{0}/pools/default/buckets/{1}/stats'.format(host_port,
                                                                  bucket)
        return self.get(url=API).json()

    def add_remote_cluster(self, host_port, remote_host_port, name):
        logger.info('Adding remote cluster {0} with reference {1}'.format(
            remote_host_port, name
        ))

        API = 'http://{0}/pools/default/remoteClusters'.format(host_port)
        data = {
            'hostname': remote_host_port, 'name': name,
            'username': self.rest_username, 'password': self.rest_password
        }
        self.post(url=API, data=data)

    def start_replication(self, host_port, params):
        logger.info('Starting replication with parameters {0}'.format(params))

        API = 'http://{0}/controller/createReplication'.format(host_port)
        self.post(url=API, data=params)

    def trigger_bucket_compaction(self, host_port, bucket):
        logger.info('Triggering bucket {0} compaction'.format(bucket))

        API = 'http://{0}/pools/default/buckets/{1}/controller/compactBucket'\
            .format(host_port, bucket)
        self.post(url=API)

    def trigger_index_compaction(self, host_port, ddoc, bucket):
        logger.info('Triggering ddoc {0} compaction, bucket {1}'.format(
            ddoc, bucket
        ))

        API = 'http://{0}/pools/default/buckets/{1}/ddocs/_design%2F{2}/controller/compactView'\
            .format(host_port, bucket, ddoc)
        self.post(url=API)

    def create_ddoc(self, host_port, bucket, ddoc_name, ddoc):
        logger.info('Creating new ddoc {0}, bucket {1}'.format(
            ddoc_name, bucket
        ))

        API = 'http://{0}/couchBase/{1}/_design/{2}'.format(
            host_port, bucket, ddoc_name)
        data = json.dumps(ddoc)
        headers = {'Content-type': 'application/json'}
        self.put(url=API, data=data,  headers=headers)

    def query_view(self, host_port, bucket, ddoc_name, view_name, params):
        logger.info('Querying view: {0}/_design/{1}/_view/{2}'.format(
            bucket, ddoc_name, view_name
        ))

        API = 'http://{0}/couchBase/{1}/_design/{2}/_view/{3}'.format(
            host_port, bucket, ddoc_name, view_name)
        self.get(url=API, params=params)

    def get_version(self, host_port):
        logger.info('Getting server version')

        API = 'http://{0}/pools/'.format(host_port)
        r = self.get(url=API).json()
        return r['implementationVersion'].replace('-rel-enterprise', '')

    def get_master_events(self, host_port):
        logger.info('Getting master events from {0}'.format(host_port))

        API = 'http://{0}/diag/masterEvents?o=1'.format(host_port)
        return self.get(url=API).text

    def get_logs(self, host_port):
        logger.info('Getting web logs from {0}'.format(host_port))

        API = 'http://{0}/logs'.format(host_port)
        return self.get(url=API).json()

    def set_internal_settings(self, host_port, data):
        logger.info('Updating internal settings: {0}'.format(data))

        API = 'http://{0}/internalSettings'.format(host_port)
        return self.post(url=API, data=data)

class TuqRestHelper(RestHelper):

    def __init__(self, cluster_spec, tuq):
        super(TuqRestHelper, self).__init__(cluster_spec)
        clusters = cluster_spec.get_clusters().values()
        self.master = clusters[0][0]
        self.tuq = tuq
        self.API = 'http://%s/query' % self.tuq.server_addr
        logger.debug('Initializing tuq API %s' % self.API)

    def create_index(self, name, path, bucket):
        logger.info('Creating tuq index %s on %s(%s)' % (name, bucket, path))

        return self.post(url=self.API,
                         data='CREATE INDEX %s ON %s(%s)' % (name, bucket, path))
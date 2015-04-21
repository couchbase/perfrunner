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
            cluster_spec.rest_credentials
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

    def delete(self, **kwargs):
        return requests.delete(auth=self.auth, **kwargs)

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
        data = {'memoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_services(self, host_port, services):
        logger.info('Configuring services on master node: {}'.format(host_port))

        api = 'http://{}/node/controller/setupServices'.format(host_port)
        data = {'services': services}
        self.post(url=api, data=data)

    def add_node(self, host_port, new_host, services=None, uri=None):
        logger.info('Adding new node: {}'.format(new_host))

        if uri:
            api = 'http://{}{}'.format(host_port, uri)
        else:
            api = 'http://{}/controller/addNode'.format(host_port)
        data = {
            'hostname': new_host,
            'user': self.rest_username,
            'password': self.rest_password,
            'services': services
        }
        self.post(url=api, data=data)

    def rebalance(self, host_port, known_nodes, ejected_nodes):
        logger.info('Starting rebalance')

        api = 'http://{}/controller/rebalance'.format(host_port)
        known_nodes = ','.join(map(self.get_otp_node_name, known_nodes))
        ejected_nodes = ','.join(map(self.get_otp_node_name, ejected_nodes))
        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes
        }
        self.post(url=api, data=data)

    def is_balanced(self, host_port):
        api = 'http://{}/pools/default'.format(host_port)
        counters = self.get(url=api).json()['counters']
        return counters.get('rebalance_start') == counters.get('rebalance_success')

    def get_failover_counter(self, host_port):
        api = 'http://{}/pools/default'.format(host_port)
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

    def create_bucket(self, host_port, name, ram_quota, replica_number,
                      replica_index, eviction_policy, threads_number,
                      password):
        logger.info('Adding new bucket: {}'.format(name))

        api = 'http://{}/pools/default/buckets'.format(host_port)
        data = {
            'name': name,
            'bucketType': 'membase',
            'ramQuotaMB': ram_quota,
            'evictionPolicy': eviction_policy,
            'replicaNumber': replica_number,
            'replicaIndex': replica_index,
            'authType': 'sasl',
            'saslPassword': password,
        }

        logger.info('bucket specification: {}'.format(data))

        if threads_number:
            data.update({'threadsNumber': threads_number})
        self.post(url=api, data=data)

    def delete_bucket(self, host_port, name):
        logger.info('Removing bucket: {}'.format(name))

        api = 'http://{}/pools/default/buckets/{}'.format(host_port, name)
        self.delete(url=api)

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

    def add_remote_cluster(self, host_port, remote_host_port, name,
                           certificate=None):
        logger.info('Adding remote cluster {} with reference {}'.format(
            remote_host_port, name
        ))

        api = 'http://{}/pools/default/remoteClusters'.format(host_port)
        data = {
            'hostname': remote_host_port, 'name': name,
            'username': self.rest_username, 'password': self.rest_password
        }
        if certificate:
            data.update({
                'demandEncryption': 1, 'certificate': certificate
            })
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

    def trigger_index_compaction(self, host_port, bucket, ddoc):
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
        self.put(url=api, data=data, headers=headers)

    def query_view(self, host_port, bucket, ddoc_name, view_name, params):
        logger.info('Querying view: {}/_design/{}/_view/{}'.format(
            bucket, ddoc_name, view_name
        ))

        api = 'http://{}/couchBase/{}/_design/{}/_view/{}'.format(
            host_port, bucket, ddoc_name, view_name)
        self.get(url=api, params=params)

    def get_version(self, host_port):
        logger.info('Getting Couchbase Server version')

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

    def get_memcached_port(self, host_port):
        logger.info('Getting memcached port from {}'.format(host_port))

        api = 'http://{}/nodes/self'.format(host_port)
        r = self.get(url=api).json()
        return r['ports']['direct']

    def get_otp_node_name(self, host_port):
        logger.info('Getting OTP node name from {}'.format(host_port))

        api = 'http://{}/nodes/self'.format(host_port)
        r = self.get(url=api).json()
        return r['otpNode']

    def set_internal_settings(self, host_port, data):
        logger.info('Updating internal settings: {}'.format(data))

        api = 'http://{}/internalSettings'.format(host_port)
        return self.post(url=api, data=data)

    def run_diag_eval(self, host_port, cmd):
        api = 'http://{}/diag/eval'.format(host_port)
        return self.post(url=api, data=cmd).text

    def enable_auto_failover(self, host_port, timeout=120):
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
            g['name']: g['addNodeURI'] for g in self.get(url=api).json()['groups']
        }

    def get_certificate(self, host_port):
        logger.info('Getting remote certificate')

        api = 'http://{}/pools/default/certificate'.format(host_port)
        return self.get(url=api).text

    def fail_over(self, host_port, node):
        logger.info('Failing over node: {}'.format(node))

        api = 'http://{}/controller/failOver'.format(host_port)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def graceful_fail_over(self, host_port, node):
        logger.info('Failing over node: {}'.format(node))

        api = 'http://{}/controller/startGracefulFailover'.format(host_port)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def add_back(self, host_port, node):
        logger.info('Adding node back: {}'.format(node))

        api = 'http://{}/controller/reAddNode'.format(host_port)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def set_delta_recovery_type(self, host_port, node):
        logger.info('Enabling delta recovery: {}'.format(node))

        api = 'http://{}/controller/setRecoveryType'.format(host_port)
        data = {
            'otpNode': self.get_otp_node_name(node),
            'recoveryType': 'delta'  # alt: full
        }
        self.post(url=api, data=data)

    def node_statuses(self, host_port):
        api = 'http://{}/nodeStatuses'.format(host_port)
        data = self.get(url=api).json()
        return {node: info['status'] for node, info in data.items()}

    def node_statuses_v2(self, host_port):
        api = 'http://{}/pools/default'.format(host_port)
        data = self.get(url=api).json()
        return {node['hostname']: node['status'] for node in data['nodes']}

    def get_node_stats(self, host_port, bucket):
        api = 'http://{}/pools/default/buckets/{}/nodes'.format(host_port,
                                                                bucket)
        data = self.get(url=api).json()
        for server in data['servers']:
            api = 'http://{}{}'.format(host_port, server['stats']['uri'])
            data = self.get(url=api).json()
            yield data['hostname'], data['op']['samples']

    def exec_n1ql_stmnt(self, host, stmnt):
        logger.info('Executing: {}'.format(stmnt))
        api = 'http://{}:8093/query/service'.format(host)
        data = {
            'statement': '{0}'.format(stmnt)
        }
        self.post(url=api, data=data)

    def n1ql_query(self, host, stmnt):
        logger.info('Executing: {}'.format(stmnt))
        api = 'http://{}:8093/query'.format(host)
        headers = {'content-type': 'text/plain'}
        self.post(url=api, data=stmnt, headers=headers)


class SyncGatewayRequestHelper(RestHelper):

    def __init__(self):
        self.auth = ()

    def wait_for_gateway_to_start(self, idx, gateway_ip):
        logger.info(
            'Checking that Sync Gateway is running on {}'.format(gateway_ip)
        )
        self.get(url='http://{}:4985/'.format(gateway_ip))
        logger.info('Sync Gateway - {} - successfully started'.format(idx))

    def wait_for_gateload_to_start(self, idx, gateload_ip):
        logger.info(
            'Checking that Gateload is running on {}'.format(gateload_ip)
        )
        self.get(url='http://{}:9876/debug/vars'.format(gateload_ip))
        logger.info('Gateload - {} - successfully started'.format(idx))

    def wait_for_seriesly_to_start(self, seriesly_ip):
        logger.info(
            'Checking that Seriesly is running on {}'.format(seriesly_ip)
        )
        self.get(url='http://{}:3133/'.format(seriesly_ip))
        logger.info('Seriesly successfully started')

    def turn_off_gateway_logging(self, gateway_ip):
        logger.info(
            'Turning off Sync Gateway logging on {}'.format(gateway_ip)
        )
        api = 'http://{}:4985/_logging'.format(gateway_ip)

        resp = self.get(url=api).json()
        logger.info('Before - {}'.format(resp))

        self.put(url=api, data='{}')
        resp = self.get(url=api).json()
        logger.info('After - {}'.format(resp))

    def collect_expvar(self, gateway_ip):
        logger.info('Collecting expvar for gateway {} - http://{}:4985/_expvar'.format(gateway_ip, gateway_ip))

        api = 'http://{}:4985/_expvar'.format(gateway_ip)
        return self.get(url=api).json()

    def get_version(self, gateway_ip):
        logger.info('Getting Sync Gateway version')

        api = 'http://{}:4985/'.format(gateway_ip)
        meta = self.get(url=api).json()
        return meta['version'].split('(')[0].split('/')[-1]

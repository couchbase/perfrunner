import json
import time
import urllib2
import base64
from collections import namedtuple

import requests
from decorator import decorator
from logger import logger
from requests.exceptions import ConnectionError

import perfrunner.helpers.misc as misc

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

    def get_data_path(self, host_port):
        logger.info('Getting data paths: {}'.format(host_port))

        api = 'http://{}/nodes/self'.format(host_port)
        hdd = self.get(url=api).json()['storage']['hdd'][0]
        return hdd['path'], hdd['index_path']

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

    def set_index_mem_quota(self, host_port, mem_quota):
        logger.info('Configuring indexer memory quota: {} to {} MB'.format(host_port, mem_quota))

        api = 'http://{}/pools/default'.format(host_port)
        data = {'indexMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_query_settings(self, host_port, override_settings):
        host = host_port.replace('8091', '8093')
        api = 'http://{}/admin/settings'.format(host)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        settings = self.get(url=api).json()

        settings = self.get(url=api).json()
        for override, value in override_settings.items():
            if override not in settings:
                logger.error('Cannot change query setting {} to {}, setting invalid'
                             .format(override, value))
                continue
            settings[override] = value
            logger.info('Changing query setting {} to {}'.format(override, value))
        self.post(url=api, data=json.dumps(settings), headers=headers)

    def set_index_settings(self, host_port, override_settings):
        host = host_port.replace('8091', '9102')
        api = 'http://{}/settings'.format(host)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        settings = self.get(url=api).json()
        for override, value in override_settings.items():
            if override not in settings:
                logger.error('Cannot change 2i setting {} to {}, setting invalid'
                             .format(override, value))
                continue
            settings[override] = value
            logger.info('Changing 2i setting {} to {}'.format(override, value))

        self.post(url=api, data=json.dumps(settings), headers=headers)

    def set_services(self, host_port, services):
        logger.info('Configuring services on master node: {}'.format(host_port))

        api = 'http://{}/node/controller/setupServices'.format(host_port)
        data = {'services': services}
        resp = requests.Session().post(url=api, data=data)

        # This post request would return a <NOT FOUND>
        # if in case of a pre-sherlock server
        if resp.status_code != 404:
            if resp.status_code not in range(200, 203):
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

    def check_rest_endpoint_exists(self, api):
        """Check that a REST endpoint exists, to allow for different endpoints to
           be monitored dependant on version. """
        for _ in range(MAX_RETRY):
            try:
                r = requests.get(url=api, auth=self.auth)
            except ConnectionError:
                time.sleep(RETRY_DELAY * 2)
                continue
            if r.status_code in range(200, 203):
                return True
            else:
                return False

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

    def get_goxdcr_stats(self, host_port, bucket):
        api = 'http://{}/pools/default/buckets/@xdcr-{}/stats'.format(host_port,
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

    def query_spatial(self, host_port, bucket, ddoc_name, view_name, params):
        logger.info('Querying view: {}/_design/{}/_spatial/{}'.format(
            bucket, ddoc_name, view_name
        ))

        api = 'http://{}/couchBase/{}/_design/{}/_spatial/{}'.format(
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
        api = 'http://{}:8093/query/service'.format(host)
        headers = {'content-type': 'text/plain'}
        resp = self.post(url=api, data=stmnt, headers=headers)
        return resp.json()

    def wait_for_indexes_to_become_online(self, host, index_name=None):
        # POLL to ensure the indexes become online
        url = 'http://{}:8093/query/service'.format(host)
        data = {
            'statement': 'SELECT * FROM system:indexes'
        }
        if index_name is not None:
            data = {
                'statement': 'SELECT * FROM system:indexes WHERE name = "{}"'.format(index_name)
            }

        ready = False
        while not ready:
            time.sleep(10)
            resp = requests.Session().post(url=url, data=data)
            if resp.json()['status'] == 'success':
                results = resp.json()['results']
                for result in results:
                    if result['indexes']['state'] == 'online':
                        ready = True
                    else:
                        ready = False
                        break
            else:
                logger.error('Query:{} => Did not return a success!'.format(data['statement']))

        if index_name is None:
            logger.info('All Indexes: ONLINE')
        else:
            logger.info('Index:{} is ONLINE'.format(index_name))

    def wait_for_secindex_init_build(self, host, indexes, rest_username, rest_password):
        # POLL until initial index build is complete
        init_ts = time.time()

        logger.info(
            "Waiting for the following indexes to be ready: {}".format(indexes))

        IndexesReady = [0 for index in indexes]
        url = 'http://{}:9102/getIndexStatus'.format(host)
        request = urllib2.Request(url)
        base64string = base64.encodestring('%s:%s' % (rest_username, rest_password)).replace('\n', '')
        request.add_header("Authorization", "Basic %s" % base64string)

        def get_index_status(json2i, index):
            """
            Return json2i["status"][k]["status"] if json2i["status"][k]["name"]
            matches the desired index.
            """
            for d in json2i["status"]:
                if d["name"] == index:
                    return d["status"]
            return None

        @misc.retry(catch=(KeyError,))
        def check_indexes_ready():
            response = urllib2.urlopen(request)
            data = str(response.read())
            json2i = json.loads(data)
            for i, index in enumerate(indexes):
                status = get_index_status(json2i, index)
                if(status == 'Ready'):
                    IndexesReady[i] = 1

        while True:
            time.sleep(1)
            check_indexes_ready()
            if(sum(IndexesReady) == len(indexes)):
                break

        finish_ts = time.time()
        logger.info('secondary index build time: {}'.format(finish_ts - init_ts))
        time_elapsed = round(finish_ts - init_ts)
        return time_elapsed

    def wait_for_secindex_incr_build(self, index_nodes, bucket, indexes, numitems):
        # POLL until incremenal index build is complete
        logger.info('expecting {} num_docs_indexed for indexes {}'.format(numitems, indexes))

        # collect num_docs_indexed information globally from all index nodes
        hosts = [node.split(':')[0] for node in index_nodes]

        def get_num_indexed():
            data = {}
            API_TEMP = 'http://{}:9102/stats'
            for host in hosts:
                host_data = self.get(url=API_TEMP.format(host)).json()
                data.update(host_data)

            num_indexed = []
            for index in indexes:
                key = "" + bucket + ":" + index + ":num_docs_indexed"
                val = data[key]
                num_indexed.append(val)
            return num_indexed

        expected_num_indexed = [numitems] * len(indexes)
        prev_num_indexed = [None] * len(indexes)
        steady_count = 0

        while True:
            time.sleep(1)
            curr_num_indexed = get_num_indexed()

            if curr_num_indexed == expected_num_indexed:
                break

            # True only if every index's num_docs_indexed is within threshold
            # of the expected value
            THRESHOLD = 0.02
            all_tolerated = all(
                [
                    actual >= (1 - THRESHOLD) * expected
                    for actual, expected in
                    zip(curr_num_indexed, expected_num_indexed)
                ])

            if not all_tolerated:
                continue

            if prev_num_indexed == curr_num_indexed:
                steady_count += 1
            else:
                prev_num_indexed = curr_num_indexed
                steady_count = 0

            STEADY_COUNT = 20
            if steady_count == STEADY_COUNT:
                break

        logger.info("Actually indexed {}".format(curr_num_indexed))


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

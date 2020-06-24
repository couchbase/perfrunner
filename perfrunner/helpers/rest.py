import json
import time
from collections import namedtuple
from typing import Callable, Dict, Iterator, List

import requests
from decorator import decorator
from requests.exceptions import ConnectionError

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.settings import BucketSettings, ClusterSpec

MAX_RETRY = 20
RETRY_DELAY = 10
EVENTING_PORT = 8096


@decorator
def retry(method: Callable, *args, **kwargs):
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


class RestHelper:

    def __init__(self, cluster_spec: ClusterSpec):
        self.rest_username, self.rest_password = cluster_spec.rest_credentials
        self.auth = self.rest_username, self.rest_password

    @retry
    def get(self, **kwargs):
        return requests.get(auth=self.auth, **kwargs)

    def _post(self, **kwargs):
        return requests.post(auth=self.auth, **kwargs)

    @retry
    def post(self, **kwargs):
        return self._post(**kwargs)

    def _put(self, **kwargs):
        return requests.put(auth=self.auth, **kwargs)

    @retry
    def put(self, **kwargs):
        return self._put(**kwargs)

    def _delete(self, **kwargs):
        return requests.delete(auth=self.auth, **kwargs)

    def delete(self, **kwargs):
        return self._delete(**kwargs)

    def set_data_path(self, host: str, data_path: str, index_path: str):
        logger.info('Configuring data paths: {}'.format(host))

        api = 'http://{}:8091/nodes/self/controller/settings'.format(host)
        data = {
            'path': data_path, 'index_path': index_path
        }
        self.post(url=api, data=data)

    def set_auth(self, host: str):
        logger.info('Configuring cluster authentication: {}'.format(host))

        api = 'http://{}:8091/settings/web'.format(host)
        data = {
            'username': self.rest_username, 'password': self.rest_password,
            'port': 'SAME'
        }
        self.post(url=api, data=data)

    def rename(self, host: str):
        logger.info('Changing server name: {}'.format(host))

        api = 'http://{}:8091/node/controller/rename'.format(host)
        data = {'hostname': host}

        self.post(url=api, data=data)

    def set_mem_quota(self, host: str, mem_quota: str):
        logger.info('Configuring data RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'memoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_index_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring index RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'indexMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_fts_index_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring FTS RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'ftsMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_analytics_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring CBAS RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'cbasMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_query_settings(self, host: str, override_settings: dict):
        api = 'http://{}:8093/admin/settings'.format(host)
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

        settings = self.get(url=api).json()
        for override, value in override_settings.items():
            if override not in settings:
                logger.error('Cannot change query setting {} to {}, setting invalid'
                             .format(override, value))
                continue
            settings[override] = value
            logger.info('Changing query setting {} to {}'.format(override, value))
        self.post(url=api, data=json.dumps(settings), headers=headers)

    def set_index_settings(self, host: str, settings: dict):
        logger.info('Changing indexer settings for {}'.format(host))

        api = 'http://{}:9102/settings'.format(host)

        curr_settings = self.get(url=api).json()
        for option, value in settings.items():
            if option in curr_settings:
                logger.info('Changing {} to {}'.format(option, value))
                self.post(url=api, data=json.dumps({option: value}))
            else:
                logger.warn('Skipping unknown option: {}'.format(option))

    def get_index_settings(self, host: str) -> dict:
        api = 'http://{}:9102/settings?internal=ok'.format(host)

        return self.get(url=api).json()

    def get_gsi_stats(self, host: str) -> dict:
        api = 'http://{}:9102/stats'.format(host)

        return self.get(url=api).json()

    def create_index(self, host: str, bucket: str, name: str, field: str,
                     storage: str = 'memdb'):
        api = 'http://{}:9102/createIndex'.format(host)
        data = {
            'index': {
                'bucket': bucket,
                'using': storage,
                'name': name,
                'secExprs': ['`{}`'.format(field)],
                'exprType': 'N1QL',
                'isPrimary': False,
                'where': '',
                'deferred': False,
                'partitionKey': '',
                'partitionScheme': 'SINGLE',
            },
            'type': 'create',
            'version': 1,
        }
        logger.info('Creating index {}'.format(pretty_dict(data)))
        self.post(url=api, data=json.dumps(data))

    def set_services(self, host: str, services: str):
        logger.info('Configuring services on {}: {}'
                    .format(host, pretty_dict(services)))

        api = 'http://{}:8091/node/controller/setupServices'.format(host)
        data = {'services': services}
        self.post(url=api, data=data)

    def add_node(self, host: str, new_host: str, services: str = None):
        logger.info('Adding new node: {}'.format(new_host))

        api = 'http://{}:8091/controller/addNode'.format(host)
        data = {
            'hostname': new_host,
            'user': self.rest_username,
            'password': self.rest_password,
            'services': services,
        }
        self.post(url=api, data=data)

    def rebalance(self, host: str, known_nodes: List[str],
                  ejected_nodes: List[str]):
        logger.info('Starting rebalance')

        api = 'http://{}:8091/controller/rebalance'.format(host)
        known_nodes = ','.join(map(self.get_otp_node_name, known_nodes))
        ejected_nodes = ','.join(map(self.get_otp_node_name, ejected_nodes))
        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes
        }
        self.post(url=api, data=data)

    def get_counters(self, host: str) -> dict:
        api = 'http://{}:8091/pools/default'.format(host)
        return self.get(url=api).json()['counters']

    def is_not_balanced(self, host: str) -> int:
        counters = self.get_counters(host)
        return counters.get('rebalance_start') - counters.get('rebalance_success')

    def get_failover_counter(self, host: str) -> int:
        counters = self.get_counters(host)
        return counters.get('failover_node')

    def get_tasks(self, host: str) -> dict:
        api = 'http://{}:8091/pools/default/tasks'.format(host)
        return self.get(url=api).json()

    def get_task_status(self, host: str, task_type: str) -> [bool, float]:
        for task in self.get_tasks(host):
            if task['type'] == task_type:
                is_running = task['status'] == 'running'
                progress = task.get('progress')
                return is_running, progress
        return False, 0

    def create_bucket(self, host: str, name: str, password: str,
                      ram_quota: int, replica_number: int, replica_index: int,
                      eviction_policy: str, bucket_type: str,
                      conflict_resolution_type: str = None):
        logger.info('Adding new bucket: {}'.format(name))

        api = 'http://{}:8091/pools/default/buckets'.format(host)

        data = {
            'name': name,
            'bucketType': bucket_type,
            'ramQuotaMB': ram_quota,
            'evictionPolicy': eviction_policy,
            'flushEnabled': 1,
            'replicaNumber': replica_number,
            'authType': 'sasl',
            'saslPassword': password,
        }

        if bucket_type == BucketSettings.BUCKET_TYPE:
            data['replicaIndex'] = replica_index

        if conflict_resolution_type:
            data['conflictResolutionType'] = conflict_resolution_type

        logger.info('Bucket configuration: {}'.format(pretty_dict(data)))

        self.post(url=api, data=data)

    def flush_bucket(self, host: str, bucket: str):
        logger.info('Flushing bucket: {}'.format(bucket))

        api = 'http://{}:8091/pools/default/buckets/{}/controller/doFlush'.format(host, bucket)
        self.post(url=api)

    def configure_auto_compaction(self, host, settings):
        logger.info('Applying auto-compaction settings: {}'.format(settings))

        api = 'http://{}:8091/controller/setAutoCompaction'.format(host)
        data = {
            'databaseFragmentationThreshold[percentage]': settings.db_percentage,
            'viewFragmentationThreshold[percentage]': settings.view_percentage,
            'parallelDBAndViewCompaction': str(settings.parallel).lower()
        }
        self.post(url=api, data=data)

    def get_auto_compaction_settings(self, host: str) -> dict:
        api = 'http://{}:8091/settings/autoCompaction'.format(host)
        return self.get(url=api).json()

    def get_bucket_stats(self, host: str, bucket: str) -> dict:
        api = 'http://{}:8091/pools/default/buckets/{}/stats'.format(host,
                                                                     bucket)
        return self.get(url=api).json()

    def get_xdcr_stats(self, host: str, bucket: str) -> dict:
        api = 'http://{}:8091/pools/default/buckets/@xdcr-{}/stats'.format(host,
                                                                           bucket)
        return self.get(url=api).json()

    def add_remote_cluster(self, host: str, remote_host: str,
                           name: str, certificate: str = None):
        logger.info('Adding remote cluster {} with reference {}'.format(
            remote_host, name
        ))

        api = 'http://{}:8091/pools/default/remoteClusters'.format(host)
        data = {
            'hostname': remote_host, 'name': name,
            'username': self.rest_username, 'password': self.rest_password
        }
        if certificate:
            data.update({
                'demandEncryption': 1, 'certificate': certificate
            })
        self.post(url=api, data=data)

    def get_remote_clusters(self, host: str) -> List[Dict]:
        logger.info('Getting remote clusters')

        api = 'http://{}:8091/pools/default/remoteClusters'.format(host)
        return self.get(url=api).json()

    def start_replication(self, host: str, params: dict):
        logger.info('Starting replication with parameters {}'.format(params))

        api = 'http://{}:8091/controller/createReplication'.format(host)
        self.post(url=api, data=params)

    def trigger_bucket_compaction(self, host: str, bucket: str):
        logger.info('Triggering bucket {} compaction'.format(bucket))

        api = 'http://{}:8091/pools/default/buckets/{}/controller/compactBucket'\
            .format(host, bucket)
        self.post(url=api)

    def trigger_index_compaction(self, host: str, bucket: str, ddoc: str):
        logger.info('Triggering ddoc {} compaction, bucket {}'.format(
            ddoc, bucket
        ))

        api = 'http://{}:8091/pools/default/buckets/{}/ddocs/_design%2F{}/controller/compactView'\
            .format(host, bucket, ddoc)
        self.post(url=api)

    def create_ddoc(self, host: str, bucket: str, ddoc_name: str, ddoc: dict):
        logger.info('Creating new ddoc {}, bucket {}'.format(
            ddoc_name, bucket
        ))

        api = 'http://{}:8091/couchBase/{}/_design/{}'.format(
            host, bucket, ddoc_name)
        data = json.dumps(ddoc)
        headers = {'Content-type': 'application/json'}
        self.put(url=api, data=data, headers=headers)

    def query_view(self, host: str, bucket: str, ddoc_name: str,
                   view_name: str, params: dict):
        logger.info('Querying view: {}/_design/{}/_view/{}'.format(
            bucket, ddoc_name, view_name
        ))

        api = 'http://{}:8091/couchBase/{}/_design/{}/_view/{}'.format(
            host, bucket, ddoc_name, view_name)
        self.get(url=api, params=params)

    def get_version(self, host: str) -> str:
        logger.info('Getting Couchbase Server version')

        api = 'http://{}:8091/pools/'.format(host)
        r = self.get(url=api).json()
        return r['implementationVersion'] \
            .replace('-rel-enterprise', '') \
            .replace('-enterprise', '') \
            .replace('-community', '')

    def get_sgversion(self, host: str) -> str:
        logger.info('Getting SG  Server version')

        api = 'http://{}:4985'.format(host)
        r = self.get(url=api).json()
        return r['version'] \
            .replace('Couchbase Sync Gateway/', '') \
            .replace(') EE', '').replace('(', '-').split(';')[0]

    def is_community(self, host: str) -> bool:
        logger.info('Getting Couchbase Server edition')

        api = 'http://{}:8091/pools/'.format(host)
        r = self.get(url=api).json()
        return 'community' in r['implementationVersion']

    def get_memcached_port(self, host: str) -> int:
        logger.info('Getting memcached port from {}'.format(host))

        api = 'http://{}:8091/nodes/self'.format(host)
        r = self.get(url=api).json()
        return r['ports']['direct']

    def get_otp_node_name(self, host: str) -> str:
        logger.info('Getting OTP node name from {}'.format(host))

        api = 'http://{}:8091/nodes/self'.format(host)
        r = self.get(url=api).json()
        return r['otpNode']

    def set_internal_settings(self, host: str, data: dict):
        logger.info('Updating internal settings: {}'.format(data))

        api = 'http://{}:8091/internalSettings'.format(host)
        self.post(url=api, data=data)

    def set_xdcr_cluster_settings(self, host: str, data: dict):
        logger.info('Updating xdcr cluster settings: {}'.format(data))

        api = 'http://{}:8091/settings/replications'.format(host)
        self.post(url=api, data=data)

    def run_diag_eval(self, host: str, cmd: str):
        api = 'http://{}:8091/diag/eval'.format(host)
        self.post(url=api, data=cmd)

    def enable_auto_failover(self, host: str):
        logger.info('Enabling auto-failover with the minimum timeout')

        api = 'http://{}:8091/settings/autoFailover'.format(host)
        for timeout in 5, 30:
            data = {'enabled': 'true', 'timeout': timeout}
            r = self._post(url=api, data=data)
            if r.status_code == 200:
                break

    def get_certificate(self, host: str) -> str:
        logger.info('Getting remote certificate')

        api = 'http://{}:8091/pools/default/certificate'.format(host)
        return self.get(url=api).text

    def fail_over(self, host: str, node: str):
        logger.info('Failing over node: {}'.format(node))

        api = 'http://{}:8091/controller/failOver'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def graceful_fail_over(self, host: str, node: str):
        logger.info('Gracefully failing over node: {}'.format(node))

        api = 'http://{}:8091/controller/startGracefulFailover'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def add_back(self, host: str, node: str):
        logger.info('Adding node back: {}'.format(node))

        api = 'http://{}:8091/controller/reAddNode'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def set_delta_recovery_type(self, host: str, node: str):
        logger.info('Enabling delta recovery: {}'.format(node))

        api = 'http://{}:8091/controller/setRecoveryType'.format(host)
        data = {
            'otpNode': self.get_otp_node_name(node),
            'recoveryType': 'delta'  # alt: full
        }
        self.post(url=api, data=data)

    def node_statuses(self, host: str) -> dict:
        api = 'http://{}:8091/nodeStatuses'.format(host)
        data = self.get(url=api).json()
        return {node: info['status'] for node, info in data.items()}

    def node_statuses_v2(self, host: str) -> dict:
        api = 'http://{}:8091/pools/default'.format(host)
        data = self.get(url=api).json()
        return {node['hostname']: node['status'] for node in data['nodes']}

    def get_node_stats(self, host: str, bucket: str) -> Iterator:
        api = 'http://{}:8091/pools/default/buckets/{}/nodes'.format(host,
                                                                     bucket)
        data = self.get(url=api).json()
        for server in data['servers']:
            api = 'http://{}:8091{}'.format(host, server['stats']['uri'])
            data = self.get(url=api).json()
            yield data['hostname'], data['op']['samples']

    def get_vbmap(self, host: str, bucket: str) -> dict:
        logger.info('Reading vbucket map: {}/{}'.format(host, bucket))
        api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        data = self.get(url=api).json()

        return data['vBucketServerMap']['vBucketMap']

    def get_server_list(self, host: str, bucket: str) -> List[str]:
        api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        data = self.get(url=api).json()

        return [server.split(':')[0]
                for server in data['vBucketServerMap']['serverList']]

    def exec_n1ql_statement(self, host: str, statement: str) -> dict:
        logger.info('Executing N1QL statement: {}'.format(statement))

        api = 'http://{}:8093/query/service'.format(host)
        data = {
            'statement': statement,
        }

        response = self.post(url=api, data=data)
        return response.json()

    def get_query_stats(self, host: str) -> dict:
        logger.info('Getting query engine stats')

        api = 'http://{}:8093/admin/stats'.format(host)

        response = self.get(url=api)
        return response.json()

    def delete_fts_index(self, host: str, index: str):
        logger.info('Deleting FTS index: {}'.format(index))

        api = 'http://{}:8094/api/index/{}'.format(host, index)

        self.delete(url=api)

    def create_fts_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new FTS index: {}'.format(index))
        api = 'http://{}:8094/api/index/{}'.format(host, index)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)
        self.put(url=api, data=data, headers=headers)

    def get_fts_doc_count(self, host: str, index: str) -> int:
        api = 'http://{}:8094/api/index/{}/count'.format(host, index)
        response = self.get(url=api).json()
        return response['count']

    def get_fts_stats(self, host: str) -> dict:
        api = 'http://{}:8094/api/nsstats'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_sg_stats(self, host: str) -> dict:
        api = 'http://{}:4985/_expvar'.format(host)
        response = self.get(url=api)
        return response.json()

    def start_sg_replication(self, host, payload):
        logger.info('Start sg replication.')
        logger.info('Payload: {}'.format(payload))

        api = 'http://{}:4985/_replicate'.format(host)
        self.post(url=api, data=json.dumps(payload))

    def start_sg_replication2(self, host, payload):
        logger.info('Start sg replication.')
        logger.info('Payload: {}'.format(payload))

        api = 'http://{}:4985/db/_replication'.format(host)
        self.post(url=api, data=json.dumps(payload))

    def get_sgreplicate_stats(self, host: str, version: int) -> dict:
        if version == 1:
            api = 'http://{}:4985/_active_tasks'.format(host)
        elif version == 2:
            api = 'http://{}:4985/db/_replicationStatus'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_elastic_stats(self, host: str) -> dict:
        api = "http://{}:9200/_stats".format(host)
        response = self.get(url=api)
        return response.json()

    def delete_elastic_index(self, host: str, index: str):
        logger.info('Deleting Elasticsearch index: {}'.format(index))
        api = 'http://{}:9200/{}'.format(host, index)
        self.delete(url=api)

    def create_elastic_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new Elasticsearch index: {}'.format(index))
        api = 'http://{}:9200/{}'.format(host, index)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)
        self.put(url=api, data=data, headers=headers)

    def get_elastic_doc_count(self, host: str, index: str) -> int:
        api = "http://{}:9200/{}/_count".format(host, index)
        response = self.get(url=api).json()
        return response['count']

    def get_index_status(self, host: str) -> dict:
        api = 'http://{}:9102/getIndexStatus'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_index_stats(self, hosts: List[str]) -> dict:
        api = 'http://{}:9102/stats'
        data = {}
        for host in hosts:
            host_data = self.get(url=api.format(host))
            data.update(host_data.json())
        return data

    def get_index_num_connections(self, host: str) -> int:
        api = 'http://{}:9102/stats'.format(host)
        response = self.get(url=api).json()
        return response['num_connections']

    def get_index_storage_stats(self, host: str) -> str:
        api = 'http://{}:9102/stats/storage'.format(host)
        return self.get(url=api).text

    def get_index_storage_stats_mm(self, host: str) -> str:
        api = 'http://{}:9102/stats/storage/mm'.format(host)
        return self.get(url=api).text

    def enable_audit(self, host: str):
        logger.info('Enabling audit')

        api = 'http://{}:8091/settings/audit'.format(host)
        data = {
            'auditdEnabled': 'true',
        }
        self.post(url=api, data=data)

    def get_rbac_roles(self, host: str) -> List[dict]:
        logger.info('Getting the existing RBAC roles')

        api = 'http://{}:8091/settings/rbac/roles'.format(host)

        return self.get(url=api).json()

    def add_rbac_user(self, host: str, bucket: str, password: str,
                      roles: List[str]):
        logger.info('Adding an RBAC user: {}, roles: {}'.format(bucket,
                                                                roles))
        data = {
            'password': password,
            'roles': ','.join(roles),
        }

        for domain in 'local', 'builtin':
            api = 'http://{}:8091/settings/rbac/users/{}/{}'.format(host,
                                                                    domain,
                                                                    bucket)
            r = self._put(url=api, data=data)
            if r.status_code == 200:
                break

    def analytics_node_active(self, host: str) -> bool:
        logger.info('Check if analytics node is active')

        api = 'http://{}:8095/analytics/cluster'.format(host)

        status = self.get(url=api).json()
        return status["state"] == "ACTIVE"

    def set_analytics_loglevel(self, analytics_node: str, analytics_log_level: str):
        logger.info('Set analytics node {} loglevel to {}'.format(analytics_node,
                                                                  analytics_log_level))

        api = 'http://{}:8095/analytics/node/config'.format(analytics_node)
        data = {
            'logLevel': analytics_log_level,
        }
        self.put(url=api, data=data)

    def restart_analytics(self, analytics_node: str):
        logger.info('Restart analytics node {}'.format(analytics_node))

        api = 'http://{}:8095/analytics/cluster/restart'.format(analytics_node)
        self.post(url=api)

    def create_function(self, node: str, payload: str, name: str):
        logger.info('Set function code {} \n on node {}'.format(payload, node))

        api = 'http://{}:8091/_p/event/saveAppTempStore/?name={}'\
            .format(node, name)
        self.post(url=api, data=payload)

    def deploy_function(self, node: str, payload: str, name: str):
        logger.info('Set function code {} \n on node {}'.format(payload, node))

        api = 'http://{}:8091/_p/event/setApplication/?name={}'\
            .format(node, name)
        self.post(url=api, data=payload)

    def enable_function(self, node: str, payload: str, name: str):
        logger.info('Enable function code {} \n on node {}'.format(payload, node))

        api = 'http://{}:8091/_p/event/setSettings/?name={}'\
            .format(node, name)
        self.post(url=api, data=payload)

    def get_num_events_processed(self, node: str, name: str):
        logger.info('get stats on node {} for {}'.format(node, name))

        api = 'http://{}:{}/getEventProcessingStats?name={}'\
            .format(node, EVENTING_PORT, name)
        data = self.get(url=api).json()
        logger.info(data)
        return data["DCP_MUTATION"]

    def get_deployed_apps(self, node: str):
        logger.info('get deployed apps on node {}'.format(node))

        api = 'http://{}:{}/getDeployedApps'.format(node, EVENTING_PORT)
        return self.get(url=api).json()

import base64
import json
import os
import time
from collections import namedtuple
from contextlib import contextmanager
from json import JSONDecodeError
from typing import Callable, Dict, Iterator, List, Optional

import requests
from capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIDedicated
from capella.serverless.CapellaAPI import CapellaAPI as CapellaAPIServerless
from decorator import decorator
from fabric.api import local
from requests.exceptions import ConnectionError

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import BucketSettings, ClusterSpec, TestConfig
from perfrunner.utils.terraform import (
    CAPELLA_CREDS_FILE,
    SERVICES_CAPELLA_TO_PERFRUNNER,
)

MAX_RETRY = 20
RETRY_DELAY = 10

ANALYTICS_PORT = 8095
ANALYTICS_PORT_SSH = 18095
EVENTING_PORT = 8096
EVENTING_PORT_SSH = 18096


@decorator
def retry(method: Callable, *args, **kwargs):
    r = namedtuple('request', ['url'])('')
    for _ in range(MAX_RETRY):
        try:
            r = method(*args, **kwargs)
            r.raise_for_status()
            return r
        except ConnectionError:
            time.sleep(RETRY_DELAY * 2)
            continue
        except requests.exceptions.HTTPError as e:
            logger.warn(e)
            logger.warn(r.text)
            logger.warn('Retrying {}'.format(r.url))
            time.sleep(RETRY_DELAY)
    logger.interrupt('Request {} failed after {} attempts'.format(
        r.url, MAX_RETRY
    ))


class RestHelper:
    def __new__(cls,
                cluster_spec: ClusterSpec,
                test_config: TestConfig,
                verbose: bool = False
                ):
        if cluster_spec.dynamic_infrastructure:
            return KubernetesRestHelper(cluster_spec, test_config)
        elif cluster_spec.serverless_infrastructure:
            return ServerlessRestHelper(cluster_spec, test_config)
        elif cluster_spec.capella_infrastructure:
            return ProvisionedCapellaRestHelper(cluster_spec, test_config)
        else:
            return DefaultRestHelper(cluster_spec, test_config)


class RestBase:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        self.rest_username, self.rest_password = cluster_spec.rest_credentials
        self.auth = self.rest_username, self.rest_password
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    @retry
    def get(self, **kwargs) -> requests.Response:
        return requests.get(auth=self.auth, verify=False, **kwargs)

    def _post(self, **kwargs) -> requests.Response:
        return requests.post(auth=self.auth, verify=False, **kwargs)

    @retry
    def post(self, **kwargs) -> requests.Response:
        return self._post(**kwargs)

    def _put(self, **kwargs) -> requests.Response:
        return requests.put(auth=self.auth, verify=False, **kwargs)

    @retry
    def put(self, **kwargs) -> requests.Response:
        return self._put(**kwargs)

    def _delete(self, **kwargs) -> requests.Response:
        return requests.delete(auth=self.auth, verify=False, **kwargs)

    def delete(self, **kwargs) -> requests.Response:
        return self._delete(**kwargs)

    @retry
    def cblite_post(self, **kwargs) -> requests.Response:
        return requests.post(**kwargs)

    @retry
    def cblite_get(self, **kwargs) -> requests.Response:
        return requests.get(**kwargs)


class DefaultRestHelper(RestBase):

    def __init__(self, cluster_spec, test_config):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)

    def set_data_path(self, host: str, path: str):
        logger.info('Configuring data path on {}'.format(host))

        api = 'http://{}:8091/nodes/self/controller/settings'.format(host)
        data = {
            'path': path,
        }
        self.post(url=api, data=data)

    def set_index_path(self, host: str, path: str):
        logger.info('Configuring index path on {}'.format(host))

        api = 'http://{}:8091/nodes/self/controller/settings'.format(host)
        data = {
            'index_path': path,
        }
        self.post(url=api, data=data)

    def set_analytics_paths(self, host: str, paths: List[str]):
        logger.info('Configuring analytics path on {}: {}'.format(host, paths))

        api = 'http://{}:8091/nodes/self/controller/settings'.format(host)
        data = {
            'cbas_path': paths,
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

    def rename(self, host: str, new_host: str = None):
        if not new_host:
            new_host = host

        logger.info('Changing server name: {} -> {}'.format(host, new_host))

        api = 'http://{}:8091/node/controller/rename'.format(host)
        data = {'hostname': new_host}

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
        logger.info('Configuring Analytics RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'cbasMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_eventing_mem_quota(self, host: str, mem_quota: int):
        logger.info('Configuring eventing RAM quota: {} MB'.format(mem_quota))

        api = 'http://{}:8091/pools/default'.format(host)
        data = {'eventingMemoryQuota': mem_quota}
        self.post(url=api, data=data)

    def set_query_settings(self, host: str, override_settings: dict):
        api = 'http://{}:8093/admin/settings'.format(host)

        settings = self.get(url=api).json()
        for override, value in override_settings.items():
            if override not in settings:
                logger.error('Cannot change query setting {} to {}, setting invalid'
                             .format(override, value))
                continue
            settings[override] = value
            logger.info('Changing {} to {}'.format(override, value))
        self.post(url=api, data=json.dumps(settings))

    def get_query_settings(self, host: str):
        api = 'http://{}:8093/admin/settings'.format(host)

        return self.get(url=api).json()

    def set_index_settings(self, host: str, settings: dict):
        api = 'http://{}:9102/settings'.format(host)
        count = 0
        curr_settings = self.get_index_settings(host)
        for option, value in settings.items():
            if option in curr_settings:
                if "enableInMemoryCompression" in option and not value:
                    while count <= 10:
                        compression = self.get_index_settings(host)[option]
                        if compression:
                            logger.info("current compression settings {}".format(compression))
                            break
                        else:
                            time.sleep(30)
                            compression = self.get_index_settings(host)[option]
                        count += 1
                    if count == 10:
                        raise Exception("Unable to set compression disabled after 5 min")
                logger.info('Changing {} to {}'.format(option, value))
                self.post(url=api, data=json.dumps({option: value}))
            else:
                logger.warn('Skipping unknown option: {}'.format(option))

    def set_planner_settings(self, host: str, settings: dict):
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/settings/planner'.format(host)
        else:
            api = 'http://{}:9102/settings/planner'.format(host)
        logger.info('Changing host {} to {}'.format(host, settings))
        self.post(url=api, data=settings)

    def get_index_metadata(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/getLocalIndexMetadata'.format(host)
        else:
            api = 'http://{}:9102/getLocalIndexMetadata'.format(host)

        return self.get(url=api).json()

    def get_index_settings(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/settings?internal=ok'.format(host)
        else:
            api = 'http://{}:9102/settings?internal=ok'.format(host)
        return self.get(url=api).json()

    def get_gsi_stats(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/stats'.format(host)
        else:
            api = 'http://{}:9102/stats'.format(host)

        return self.get(url=api).json()

    def create_index(self, host: str, bucket: str, name: str, field: str,
                     storage: str = 'memdb', scope: str = '_default',
                     collection: str = '_default'):
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/createIndex'.format(host)
        else:
            api = 'http://{}:9102/createIndex'.format(host)
        data = {
            'index': {
                'bucket': bucket,
                'scope': scope,
                'collection': collection,
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
        logger.info('Configuring services on {}: {}'.format(host, services))

        api = 'http://{}:8091/node/controller/setupServices'.format(host)
        data = {'services': services}
        self.post(url=api, data=data)

    def add_node(self, host: str, new_host: str, services: str = None):
        logger.info('Adding new node: {}'.format(new_host))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/addNode'.format(host)
        else:
            api = 'http://{}:8091/controller/addNode'.format(host)
        data = {
            'hostname': new_host,
            'user': self.rest_username,
            'password': self.rest_password,
            'services': services,
        }
        self.post(url=api, data=data)

    def add_node_to_group(self, host: str, new_host: str, services: str = None, group: str = None):
        logger.info('Adding new node: {} to group {}'.format(new_host, group))
        server_group_info = self.get_server_group_info(host)["groups"]
        for group_info in server_group_info:
            if group_info['name'] == group:
                add_node_uri = group_info['addNodeURI']
                break
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091{}'.format(host, add_node_uri)
        else:
            api = 'http://{}:8091{}'.format(host, add_node_uri)
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
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/rebalance'.format(host)
        else:
            api = 'http://{}:8091/controller/rebalance'.format(host)
        known_nodes = ','.join(map(self.get_otp_node_name, known_nodes))
        ejected_nodes = ','.join(map(self.get_otp_node_name, ejected_nodes))
        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes
        }
        self.post(url=api, data=data)

    def increase_bucket_limit(self, host: str, num_buckets: int):
        logger.info('increasing bucket limit to {}'.format(num_buckets))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/internalSettings'.format(host)
        else:
            api = 'http://{}:8091/internalSettings'.format(host)
        data = {
            'maxBucketCount': num_buckets
        }
        self.post(url=api, data=data)

    def get_counters(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default'.format(host)
        else:
            api = 'http://{}:8091/pools/default'.format(host)
        return self.get(url=api).json()['counters']

    def is_not_balanced(self, host: str) -> int:
        counters = self.get_counters(host)
        return counters.get('rebalance_start') - counters.get('rebalance_success')

    def get_failover_counter(self, host: str) -> int:
        counters = self.get_counters(host)
        return counters.get('failover_node')

    def get_tasks(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/tasks'.format(host)
        else:
            api = 'http://{}:8091/pools/default/tasks'.format(host)
        return self.get(url=api).json()

    def get_task_status(self, host: str, task_type: str) -> tuple[bool, float]:
        for task in self.get_tasks(host):
            if task['type'] == task_type:
                is_running = task['status'] == 'running'
                progress = task.get('progress')
                return is_running, progress
        return False, 0

    def get_xdcrlink_status(self, host: str, task_type: str, uuid: str) -> tuple[bool, float]:
        for task in self.get_tasks(host):
            if task['type'] == task_type and uuid in task['target']:
                is_running = task['status'] == 'running'
                progress = task.get('progress')
                return is_running, progress
        return False, 0

    def get_xdcr_replication_id(self, host: str):
        replication_id = ''
        for task in self.get_tasks(host):
            if 'settingsURI' in task:
                replication_id = task['settingsURI']
        return replication_id.split('/')[-1]

    def delete_bucket(self, host: str, name: str):
        logger.info('Deleting new bucket: {}'.format(name))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{host}:18091/pools/default/buckets/{bucket}'. \
                format(host=host, bucket=name)
        else:
            api = 'http://{host}:8091/pools/default/buckets/{bucket}'. \
                format(host=host, bucket=name)
        self.delete(url=api)

    def create_bucket(self,
                      host: str,
                      name: str,
                      password: str,
                      ram_quota: int,
                      replica_number: int,
                      replica_index: int,
                      eviction_policy: str,
                      bucket_type: str,
                      max_ttl: int,
                      magma_seq_tree_data_block_size: int = 0,
                      backend_storage: Optional[str] = None,
                      conflict_resolution_type: Optional[str] = None,
                      compression_mode: Optional[str] = None,
                      history_seconds: int = 0,
                      history_bytes: int = 0) -> requests.Response:
        logger.info('Adding new bucket: {}'.format(name))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets'.format(host)
        else:
            api = 'http://{}:8091/pools/default/buckets'.format(host)

        data = {
            'name': name,
            'bucketType': bucket_type,
            'ramQuotaMB': ram_quota,
            'evictionPolicy': eviction_policy,
            'flushEnabled': 1,
            'replicaNumber': replica_number,
        }

        if bucket_type == BucketSettings.BUCKET_TYPE:
            data['replicaIndex'] = replica_index

        if conflict_resolution_type:
            data['conflictResolutionType'] = conflict_resolution_type

        if compression_mode:
            data['compressionMode'] = compression_mode

        if backend_storage:
            data['storageBackend'] = backend_storage

        if magma_seq_tree_data_block_size:
            data['magmaSeqTreeDataBlockSize'] = magma_seq_tree_data_block_size

        if history_seconds:
            data['historyRetentionSeconds'] = history_seconds

        if history_bytes:
            data['historyRetentionBytes'] = history_bytes

        if max_ttl:
            data['maxTTL'] = max_ttl

        logger.info('Bucket configuration: {}'.format(pretty_dict(data)))

        self.post(url=api, data=data)

    def flush_bucket(self, host: str, bucket: str):
        logger.info('Flushing bucket: {}'.format(bucket))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}/' \
                  'controller/doFlush'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}/controller/doFlush'.format(host, bucket)
        self.post(url=api)

    def configure_auto_compaction(self, host, settings):
        logger.info('Applying auto-compaction settings: {}'.format(settings))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/setAutoCompaction'.format(host)
        else:
            api = 'http://{}:8091/controller/setAutoCompaction'.format(host)
        data = {
            'databaseFragmentationThreshold[percentage]': settings.db_percentage,
            'viewFragmentationThreshold[percentage]': settings.view_percentage,
            'parallelDBAndViewCompaction': str(settings.parallel).lower(),
            'magmaFragmentationPercentage': settings.magma_fragmentation_percentage
        }
        self.post(url=api, data=data)

    def get_auto_compaction_settings(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/settings/autoCompaction'.format(host)
        else:
            api = 'http://{}:8091/settings/autoCompaction'.format(host)
        return self.get(url=api).json()

    def get_bucket_stats(self, host: str, bucket: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}/stats'. \
                format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}/stats'. \
                format(host, bucket)

        return self.get(url=api).json()

    def get_scope_and_collection_items(self, host: str, bucket: str, scope: str,
                                       coll: str = None) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/stats/range/kv_collection_item_count?bucket={}&' \
                  'scope={}&start=-1'.format(host, bucket, scope)
        else:
            api = 'http://{}:8091/pools/default/stats/range/kv_collection_item_count?bucket={}&' \
                  'scope={}&start=-1'.format(host, bucket, scope)

        if coll:
            api += '&collection={}'.format(coll)

        return self.get(url=api).json()

    def get_dcp_replication_items(self, host: str, bucket: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/stats/range/kv_dcp_items_remaining?bucket={}&' \
                  'connection_type=replication&aggregationFunction=sum'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/stats/range/kv_dcp_items_remaining?bucket={}&' \
                  'connection_type=replication&aggregationFunction=sum'.format(host, bucket)
        return self.get(url=api).json()

    def get_dcp_replication_items_v2(self, host: str, bucket: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/stats/range/kv_dcp_items_remaining?bucket={}' \
                  '&connection_type=replication&nodesAggregation=sum'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/stats/range/kv_dcp_items_remaining?bucket={}' \
                  '&connection_type=replication&nodesAggregation=sum'.format(host, bucket)
        return self.get(url=api).json()

    def get_xdcr_stats(self, host: str, bucket: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/@xdcr-{}/stats'. \
                format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/@xdcr-{}/stats'. \
                format(host, bucket)
        return self.get(url=api).json()

    def add_remote_cluster(self,
                           local_host: str,
                           remote_host: str,
                           name: str,
                           secure_type: str,
                           certificate: str):
        logger.info('Adding a remote cluster: {}'.format(remote_host))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/remoteClusters'.format(local_host)
        else:
            api = 'http://{}:8091/pools/default/remoteClusters'.format(local_host)
        payload = {
            'name': name,
            'hostname': remote_host,
            'username': self.rest_username,
            'password': self.rest_password,
        }
        if secure_type:
            payload['secureType'] = secure_type
        if certificate:
            payload['demandEncryption'] = 1
            payload['certificate'] = certificate

        self.post(url=api, data=payload)

    def get_remote_clusters(self, host: str) -> List[Dict]:
        logger.info('Getting remote clusters')
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/remoteClusters'.format(host)
        else:
            api = 'http://{}:8091/pools/default/remoteClusters'.format(host)
        return self.get(url=api).json()

    def create_replication(self, host: str, params: dict):
        logger.info('Starting replication with parameters {}'.format(params))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/createReplication'.format(host)
        else:
            api = 'http://{}:8091/controller/createReplication'.format(host)
        self.post(url=api, data=params)

    def edit_replication(self, host: str, params: dict, replicationid: str):
        logger.info('Editing replication {} with parameters {}'.format(replicationid, params))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/settings/replications/{}'.format(host, replicationid)
        else:
            api = 'http://{}:8091/settings/replications/{}'.format(host, replicationid)
        self.post(url=api, data=params)

    def trigger_bucket_compaction(self, host: str, bucket: str):
        logger.info('Triggering bucket {} compaction'.format(bucket))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}/controller/compactBucket' \
                .format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}/controller/compactBucket' \
                .format(host, bucket)
        self.post(url=api)

    def trigger_index_compaction(self, host: str, bucket: str, ddoc: str):
        logger.info('Triggering ddoc {} compaction, bucket {}'.format(ddoc, bucket))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}/ddocs/_design%2F{}/' \
                  'controller/compactView'.format(host, bucket, ddoc)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}/ddocs/_design%2F{}/' \
                  'controller/compactView'.format(host, bucket, ddoc)
        self.post(url=api)

    def create_ddoc(self, host: str, bucket: str, ddoc_name: str, ddoc: dict):
        logger.info('Creating new ddoc {}, bucket {}'.format(
            ddoc_name, bucket
        ))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/couchBase/{}/_design/{}'.format(
                host, bucket, ddoc_name)
        else:
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
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/couchBase/{}/_design/{}/_view/{}'.format(
                host, bucket, ddoc_name, view_name)
        else:
            api = 'http://{}:8091/couchBase/{}/_design/{}/_view/{}'.format(
                host, bucket, ddoc_name, view_name)
        self.get(url=api, params=params)

    def get_version(self, host: str) -> str:
        logger.info('Getting Couchbase Server version on server {}'.format(host))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/'.format(host)
        else:
            api = 'http://{}:8091/pools/'.format(host)
        r = self.get(url=api).json()
        return r['implementationVersion'] \
            .replace('-rel-enterprise', '') \
            .replace('-enterprise', '') \
            .replace('-community', '')

    def supports_rbac(self, host: str) -> bool:
        """Return true if the cluster supports RBAC."""
        # if self.test_config.cluster.enable_n2n_encryption:
        #     rbac_url = 'https://{}:18091/settings/rbac/roles'.format(host)
        # else:
        rbac_url = 'http://{}:8091/settings/rbac/roles'.format(host)
        r = requests.get(auth=self.auth, url=rbac_url)
        return r.status_code == requests.codes.ok

    def is_community(self, host: str) -> bool:
        logger.info('Getting Couchbase Server edition')
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/'.format(host)
        else:
            api = 'http://{}:8091/pools/'.format(host)
        r = self.get(url=api).json()
        return 'community' in r['implementationVersion']

    def get_memcached_port(self, host: str) -> int:
        logger.info('Getting memcached port from {}'.format(host))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/nodes/self'.format(host)
        else:
            api = 'http://{}:8091/nodes/self'.format(host)
        r = self.get(url=api).json()
        return r['ports']['direct']

    def get_otp_node_name(self, host: str) -> str:
        logger.info('Getting OTP node name from {}'.format(host))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/nodes/self'.format(host)
        else:
            api = 'http://{}:8091/nodes/self'.format(host)
        r = self.get(url=api).json()
        return r['otpNode']

    def set_internal_settings(self, host: str, data: dict):
        logger.info('Updating internal settings: {}'.format(data))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/internalSettings'.format(host)
        else:
            api = 'http://{}:8091/internalSettings'.format(host)
        self.post(url=api, data=data)

    def set_xdcr_cluster_settings(self, host: str, data: dict):
        logger.info('Updating xdcr cluster settings: {}'.format(data))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/settings/replications'.format(host)
        else:
            api = 'http://{}:8091/settings/replications'.format(host)
        self.post(url=api, data=data)

    def run_diag_eval(self, host: str, cmd: str):
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/diag/eval'.format(host)
        else:
            api = 'http://{}:8091/diag/eval'.format(host)
        self.post(url=api, data=cmd)

    def set_auto_failover(self, host: str, enabled: str,
                          failover_min: int, failover_max: int):
        logger.info('Setting auto-failover to: {}'.format(enabled))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/settings/autoFailover'.format(host)
        else:
            api = 'http://{}:8091/settings/autoFailover'.format(host)

        for timeout in failover_min, failover_max:
            data = {'enabled': enabled,
                    'timeout': timeout,
                    'failoverOnDataDiskIssues[enabled]': enabled,
                    'failoverOnDataDiskIssues[timePeriod]': 10
                    }
            r = self._post(url=api, data=data)
            if r.status_code == 200:
                break

    def get_certificate(self, host: str) -> str:
        logger.info('Getting remote certificate')

        build = self.get_version(host)
        version, _ = build.split('-')
        version_number = tuple(map(int, version.split('.')))

        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/'.format(host)
        else:
            api = 'http://{}:8091/pools/default/'.format(host)

        if version_number < (7, 1, 0):
            api += 'certificate'
            return self.get(url=api).text

        api += 'trustedCAs'
        r = self.get(url=api)
        certs = ''.join(cert['pem'] for cert in r.json())
        return certs

    def fail_over(self, host: str, node: str):
        logger.info('Failing over node: {}'.format(node))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/failOver'.format(host)
        else:
            api = 'http://{}:8091/controller/failOver'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def graceful_fail_over(self, host: str, node: str):
        logger.info('Gracefully failing over node: {}'.format(node))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/startGracefulFailover'.format(host)
        else:
            api = 'http://{}:8091/controller/startGracefulFailover'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def add_back(self, host: str, node: str):
        logger.info('Adding node back: {}'.format(node))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/reAddNode'.format(host)
        else:
            api = 'http://{}:8091/controller/reAddNode'.format(host)
        data = {'otpNode': self.get_otp_node_name(node)}
        self.post(url=api, data=data)

    def set_delta_recovery_type(self, host: str, node: str):
        logger.info('Enabling delta recovery: {}'.format(node))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'http://{}:18091/controller/setRecoveryType'.format(host)
        else:
            api = 'http://{}:8091/controller/setRecoveryType'.format(host)
        data = {
            'otpNode': self.get_otp_node_name(node),
            'recoveryType': 'delta'  # alt: full
        }
        self.post(url=api, data=data)

    def node_statuses(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/nodeStatuses'.format(host)
        else:
            api = 'http://{}:8091/nodeStatuses'.format(host)
        data = self.get(url=api).json()
        return {node: info['status'] for node, info in data.items()}

    def node_statuses_v2(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default'.format(host)
        else:
            api = 'http://{}:8091/pools/default'.format(host)
        data = self.get(url=api).json()
        return {node['hostname']: node['status'] for node in data['nodes']}

    def get_node_stats(self, host: str, bucket: str) -> Iterator:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}/nodes'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}/nodes'.format(host, bucket)
        data = self.get(url=api).json()
        for server in data['servers']:
            if self.test_config.cluster.enable_n2n_encryption:
                api = 'https://{}:18091{}'.format(host, server['stats']['uri'])
            else:
                api = 'http://{}:8091{}'.format(host, server['stats']['uri'])
            data = self.get(url=api).json()
            yield data['hostname'], data['op']['samples']

    def get_vbmap(self, host: str, bucket: str) -> dict:
        logger.info('Reading vbucket map: {}/{}'.format(host, bucket))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        data = self.get(url=api).json()

        return data['vBucketServerMap']['vBucketMap']

    def get_server_list(self, host: str, bucket: str) -> List[str]:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        data = self.get(url=api).json()

        return [server.split(':')[0]
                for server in data['vBucketServerMap']['serverList']]

    def get_bucket_info(self, host: str, bucket: str) -> List[str]:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        return self.get(url=api).json()

    def set_bucket_history(self, host: str, bucket: str, history_bytes: int = 0,
                           history_seconds: int = 0) -> requests.Response:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/buckets/{}'.format(host, bucket)
        else:
            api = 'http://{}:8091/pools/default/buckets/{}'.format(host, bucket)
        data = {
            'historyRetentionBytes': history_bytes,
            'historyRetentionSeconds': history_seconds
        }
        return self.post(url=api, data=data)

    def exec_n1ql_statement(self, host: str, statement: str, query_context: str = None) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18093/query/service'.format(host)
        else:
            api = 'http://{}:8093/query/service'.format(host)
        data = {
            'statement': statement,
        }
        if query_context:
            data['query_context'] = query_context
        response = self.post(url=api, data=data)
        return response.json()

    def set_serverless_throttle(self, node, values):
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/internalSettings'.format(node)
        else:
            api = 'http://{}:8091/internalSettings'.format(node)
        for key in values:
            limit = values[key]
            if limit != 0:
                data = {key: limit}
                logger.info('Setting throttle limit: {}'.format(data))
                self.post(url=api, data=data)

    def reset_serverless_throttle(self, node):
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/internalSettings'.format(node)
        else:
            api = 'http://{}:8091/internalSettings'.format(node)
        serverless_throttle = {'dataThrottleLimit': 5000,
                               'indexThrottleLimit': 5000,
                               'searchThrottleLimit': 5000,
                               'queryThrottleLimit': 5000}
        for service, limit in serverless_throttle.items():
            data = {service: limit}
            logger.info('Setting throttle limit: {}'.format(data))
            self.post(url=api, data=data)

    def explain_n1ql_statement(self, host: str, statement: str, query_context: str = None):
        statement = 'EXPLAIN {}'.format(statement)
        return self.exec_n1ql_statement(host, statement, query_context)

    def get_query_stats(self, host: str) -> dict:
        logger.info('Getting query engine stats')
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18093/admin/stats'.format(host)
        else:
            api = 'http://{}:8093/admin/stats'.format(host)

        response = self.get(url=api)
        return response.json()

    def delete_fts_index(self, host: str, index: str):
        logger.info('Deleting FTS index: {}'.format(index))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/index/{}'.format(host, index)
        else:
            api = 'http://{}:8094/api/index/{}'.format(host, index)

        self.delete(url=api)

    def create_fts_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new FTS index: {}'.format(index))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/index/{}'.format(host, index)
        else:
            api = 'http://{}:8094/api/index/{}'.format(host, index)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)

        self.put(url=api, data=data, headers=headers)

    def get_fts_doc_count(self, host: str, index: str, bucket: str) -> int:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/index/{}/count'.format(host, index)
        else:
            api = 'http://{}:8094/api/index/{}/count'.format(host, index)

        response = self.get(url=api).json()
        return response['count']

    def get_fts_stats(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/nsstats'.format(host)
        else:
            api = 'http://{}:8094/api/nsstats'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_elastic_stats(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = "https://{}:19200/_stats".format(host)
        else:
            api = "http://{}:9200/_stats".format(host)
        response = self.get(url=api)
        return response.json()

    def delete_elastic_index(self, host: str, index: str):
        logger.info('Deleting Elasticsearch index: {}'.format(index))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19200/{}'.format(host, index)
        else:
            api = 'http://{}:9200/{}'.format(host, index)

        self.delete(url=api)

    def create_elastic_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new Elasticsearch index: {}'.format(index))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19200/{}'.format(host, index)
        else:
            api = 'http://{}:9200/{}'.format(host, index)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)

        self.put(url=api, data=data, headers=headers)

    def get_elastic_doc_count(self, host: str, index: str) -> int:
        if self.test_config.cluster.enable_n2n_encryption:
            api = "https://{}:19200/{}/_count".format(host, index)
        else:
            api = "http://{}:9200/{}/_count".format(host, index)
        response = self.get(url=api).json()
        return response['count']

    def get_index_status(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/getIndexStatus'.format(host)
        else:
            api = 'http://{}:9102/getIndexStatus'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_index_stats(self, hosts: List[str]) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/stats'
        else:
            api = 'http://{}:9102/stats'
        data = {}
        for host in hosts:
            host_data = self.get(url=api.format(host))
            data.update(host_data.json())
        return data

    def get_index_num_connections(self, host: str) -> int:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/stats'.format(host)
        else:
            api = 'http://{}:9102/stats'.format(host)
        response = self.get(url=api).json()
        return response['num_connections']

    def get_index_storage_stats(self, host: str) -> str:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/stats/storage'.format(host)
        else:
            api = 'http://{}:9102/stats/storage'.format(host)
        return self.get(url=api)

    def get_index_storage_stats_mm(self, host: str) -> str:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19102/stats/storage/mm'.format(host)
        else:
            api = 'http://{}:9102/stats/storage/mm'.format(host)
        return self.get(url=api).text

    def get_audit_settings(self, host: str) -> dict:
        logger.info('Getting current audit settings')
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/settings/audit'.format(host)
        else:
            api = 'http://{}:8091/settings/audit'.format(host)
        return self.get(url=api).json()

    def enable_audit(self, host: str, disabled: List[str]):
        logger.info('Enabling audit')
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/settings/audit'.format(host)
        else:
            api = 'http://{}:8091/settings/audit'.format(host)
        data = {
            'auditdEnabled': 'true',
        }
        if disabled:
            data['disabled'] = ','.join(disabled)
        self.post(url=api, data=data)

    def get_rbac_roles(self, host: str) -> List[dict]:
        logger.info('Getting the existing RBAC roles')
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/settings/rbac/roles'.format(host)
        else:
            api = 'http://{}:8091/settings/rbac/roles'.format(host)

        return self.get(url=api).json()

    def delete_rbac_user(self, host: str, bucket: str):
        logger.info('Deleting an RBAC user: {}'.format(bucket))
        for domain in 'local', 'builtin':
            if self.test_config.cluster.enable_n2n_encryption:
                api = 'https://{}:18091/settings/rbac/users/{}/{}'.format(host, domain, bucket)
            else:
                api = 'http://{}:8091/settings/rbac/users/{}/{}'.format(host, domain, bucket)
            r = self._delete(url=api)
            if r.status_code == 200:
                break

    def add_rbac_user(self, host: str, user: str, password: str,
                      roles: List[str]):
        logger.info('Adding an RBAC user: {}, roles: {}'.format(user, roles))
        data = {
            'password': password,
            'roles': ','.join(roles),
        }

        for domain in 'local', 'builtin':
            if self.test_config.cluster.enable_n2n_encryption:
                api = 'https://{}:18091/settings/rbac/users/{}/{}'.format(host, domain, user)
            else:
                api = 'http://{}:8091/settings/rbac/users/{}/{}'.format(host, domain, user)
            r = self._put(url=api, data=data)
            if r.status_code == 200:
                break

    def analytics_node_active(self, host: str) -> bool:
        logger.info('Checking if analytics node is active: {}'.format(host))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:{}/analytics/cluster'.format(host, ANALYTICS_PORT_SSH)
        else:
            api = 'http://{}:{}/analytics/cluster'.format(host, ANALYTICS_PORT)

        status = self.get(url=api).json()
        return status["state"] == "ACTIVE"

    def exec_analytics_statement(self, analytics_node: str,
                                 statement: str) -> requests.Response:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:{}/analytics/service'.format(analytics_node, ANALYTICS_PORT_SSH)
        else:
            api = 'http://{}:{}/analytics/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'statement': statement
        }
        return self.post(url=api, data=data)

    def exec_analytics_statement_curl(self, analytics_node: str,
                                      statement: str) -> requests.Response:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:{}/analytics/service'.format(analytics_node, ANALYTICS_PORT_SSH)
        else:
            api = 'http://{}:{}/analytics/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'statement': statement
        }
        data_json = json.dumps(data)
        cmd = "curl -v -u {}:{} -H \"Content-Type: application/json\" -d '{}' {}".\
            format(self.rest_username, self.rest_password, data_json, api)
        return local(cmd, capture=True)

    def exec_analytics_query(self, analytics_node: str,
                             statement: str) -> requests.Response:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:{}/analytics/service'.format(analytics_node, ANALYTICS_PORT_SSH)
        else:
            api = 'http://{}:{}/analytics/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'statement': statement
        }
        return self.post(url=api, data=data).json()

    def get_analytics_stats(self, analytics_node: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:19110/analytics/node/stats'.format(analytics_node)
        else:
            api = 'http://{}:9110/analytics/node/stats'.format(analytics_node)
        return self.get(url=api).json()

    def get_pending_mutations(self, analytics_node: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18095/analytics/node/agg/stats/remaining'.format(analytics_node)
        else:
            api = 'http://{}:8095/analytics/node/agg/stats/remaining'.format(analytics_node)
        return self.get(url=api).json()

    def get_pending_mutations_v2(self, analytics_node: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18095/analytics/status/ingestion'.format(analytics_node)
        else:
            api = 'http://{}:8095/analytics/status/ingestion'.format(analytics_node)
        return self.get(url=api).json()

    def get_ingestion_v2(self, analytics_node: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18095/analytics/status/ingestion/v2'.format(analytics_node)
        else:
            api = 'http://{}:8095/analytics/status/ingestion/v2'.format(analytics_node)
        return self.get(url=api).json()

    def set_analytics_logging_level(self, analytics_node: str, log_level: str):
        logger.info('Setting log level \"{}\" for analytics'.format(log_level))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'logLevel': log_level
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def set_analytics_page_size(self, analytics_node: str, page_size: str):
        logger.info('Setting buffer cache page size \"{}\" for analytics'.format(page_size))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'storageBuffercachePagesize': page_size
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def set_analytics_storage_compression_block(self, analytics_node: str,
                                                storage_compression_block: str):
        logger.info('Setting storage compression block \"{}\" for analytics'
                    .format(storage_compression_block))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'storageCompressionBlock': storage_compression_block
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def set_analytics_max_active_writable_datasets(
            self,
            analytics_node: str,
            max_writable: int):
        logger.info('Setting max active writable datasets \"{}\" for analytics'
                    .format(str(max_writable)))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        data = {
            'storageMaxActiveWritableDatasets': str(max_writable)
        }
        r = self.put(url=api, data=data)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def restart_analytics_cluster(self, analytics_node: str):
        logger.info('Restarting analytics cluster')
        api = 'http://{}:{}/analytics/cluster/restart'.format(analytics_node, ANALYTICS_PORT)
        r = self.post(url=api)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def validate_analytics_logging_level(self, analytics_node: str, log_level: str):
        logger.info('Checking that analytics log level is set to {}'.format(log_level))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        response = self.get(url=api).json()
        if "logLevel" in response:
            return response["logLevel"] == log_level
        return False

    def validate_analytics_setting(self, analytics_node: str, setting: str, value: str):
        logger.info('Checking that analytics {} is set to {}'.format(setting, value))
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        response = self.get(url=api).json()
        assert (str(response[setting]) == str(value))

    def get_analytics_service_config(self, analytics_node: str):
        logger.info('Grabbing analytics service config')
        api = 'http://{}:{}/analytics/config/service'.format(analytics_node, ANALYTICS_PORT)
        response = self.get(url=api).json()
        return response

    def get_cbas_incoming_records_count(self, host: str) -> dict:
        api = 'http://{}:8091/pools/default/stats/range/cbas_incoming_records_count?' \
              'aggregationFunction=sum'.format(host)
        return self.get(url=api).json()

    def get_cbas_incoming_records_count_v2(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/stats/range/cbas_incoming_records_count?' \
                  'nodesAggregation=sum'.format(host)
        else:
            api = 'http://{}:8091/pools/default/stats/range/cbas_incoming_records_count?' \
                  'nodesAggregation=sum'.format(host)
        return self.get(url=api).json()

    def deploy_function(self, node: str, func: dict, name: str):
        logger.info('Deploying function on node {}: {}'.format(node, pretty_dict(func)))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18096/api/v1/functions/{}'.format(node, name)
        else:
            api = 'http://{}:8096/api/v1/functions/{}'.format(node, name)
        self.post(url=api, data=json.dumps(func))

    def get_functions(self, node: str):
        logger.info('Getting all functions')
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18096/api/v1/functions'.format(node)
        else:
            api = 'http://{}:8096/api/v1/functions'.format(node)
        return self.get(url=api).json()

    def change_function_settings(self, node: str, func: dict, name: str, func_scope: dict):
        logger.info('Changing function settings on node {}: {}'.format(node,
                    pretty_dict(func)))
        logger.info('function scope for {} is {}'.format(name, pretty_dict(func_scope)))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18096/api/v1/functions/{}/settings/'.format(node, name)
            if func_scope:
                api = 'https://{}:18096/api/v1/functions/{}/settings?bucket={}&scope={}'.\
                    format(node, name, func_scope['bucket'], func_scope['scope'])
        else:
            api = 'http://{}:8096/api/v1/functions/{}/settings/'.format(node, name)
            if func_scope:
                api = 'http://{}:8096/api/v1/functions/{}/settings?bucket={}&scope={}'.\
                    format(node, name, func_scope['bucket'], func_scope['scope'])
        logger.info("Api path {}".format(api))
        self.post(url=api, data=func)

    def get_num_events_processed(self, event: str, node: str, name: str):
        logger.info('get stats on node {} for {}'.format(node, name))

        data = {}
        all_stats = self.get_eventing_stats(node=node)
        for stat in all_stats:
            if name == stat["function_name"]:
                data = stat["event_processing_stats"]
                break

        logger.info(data)
        if event == "ALL":
            return data
        if event in data:
            return data[event]

        return 0

    def get_apps_with_status(self, node: str, status: str):
        logger.info('get apps with status {} on node {}'.format(status, node))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:{}//api/v1/status'.format(node, EVENTING_PORT_SSH)
        else:
            api = 'http://{}:{}//api/v1/status'.format(node, EVENTING_PORT)
        data = self.get(url=api).json()
        apps = []
        for app in data["apps"]:
            if app["composite_status"] == status:
                apps.append(app["name"])
        return apps

    def get_eventing_stats(self, node: str, full_stats: bool = False) -> dict:
        logger.info('get eventing stats on node {}'.format(node))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:{}/api/v1/stats'.format(node, EVENTING_PORT_SSH)
        else:
            api = 'http://{}:{}/api/v1/stats'.format(node, EVENTING_PORT)
        if full_stats:
            api += "?type=full"

        return self.get(url=api).json()

    def get_active_nodes_by_role(self, master_node: str, role: str) -> List[str]:
        active_nodes = self.node_statuses(master_node)
        active_nodes_by_role = []

        if use_private_ips := self.cluster_spec.using_private_cluster_ips:
            ip_map = self.cluster_spec.servers_public_to_private_ip

        for node in self.cluster_spec.servers_by_role(role):
            lookup = ip_map[node] + ":8091" if use_private_ips else node + ":8091"
            if lookup in active_nodes:
                active_nodes_by_role.append(node)
        return active_nodes_by_role

    def fts_set_node_level_parameters(self, parameter: dict, host: str):
        logger.info("Adding in the parameter {} ".format(parameter))
        api = "http://{}:8094/api/managerOptions".format(host)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(parameter, ensure_ascii=False)
        self.put(url=api, data=data, headers=headers)

    def upload_cluster_certificate(self, node: str):
        logger.info("Uploading cluster certificate to {}".format(node))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/controller/uploadClusterCA'.format(node)
        else:
            api = 'http://{}:8091/controller/uploadClusterCA'.format(node)
        data = open('./certificates/inbox/ca.pem', 'rb').read()
        self.post(url=api, data=data)

    def reload_cluster_certificate(self, node: str):
        logger.info("Reloading certificate on {}".format(node))
        api = 'http://{}:8091/node/controller/reloadCertificate'.format(node)
        self.post(url=api)

    def enable_certificate_auth(self, node: str):
        logger.info("Enabling certificate-based client auth on {}".format(node))
        api = 'http://{}:8091/settings/clientCertAuth'.format(node)
        data = open('./certificates/inbox/config.json', 'rb').read()
        self.post(url=api, data=data)

    def get_minimum_tls_version(self, node: str):
        # Because tlsv1.3 cannot be the global tls min version (unlike previous tls versions),
        # we must add additional checks to ensure that version is correctly identified
        logger.info("Getting TLS version of {}".format(node))
        api = 'http://{}:8091/settings/security'.format(node)
        global_tls_min_version = self.get(url=api).json()['tlsMinVersion']
        try:
            api = 'http://{}:8091/settings/security/data'.format(node)
            local_tls_min_version = self.get(url=api).json()['tlsMinVersion']
            if local_tls_min_version == global_tls_min_version:
                return global_tls_min_version
            else:
                return local_tls_min_version
        except KeyError:
            return global_tls_min_version

    def set_num_threads(self, node: str, thread_type: str, thread: int):
        logger.info('Setting {} to {}'.format(thread_type, thread))
        api = 'http://{}:8091/pools/default/settings/memcached/global'.format(node)
        data = {
            thread_type: thread
        }
        self.post(url=api, data=data)

    def get_supported_ciphers(self, node: str, service: str):
        logger.info("Getting the supported ciphers for the {} service".format(service))
        api = api = 'http://{}:8091/settings/security/{}'.format(node, service)
        return self.get(url=api).json()['supportedCipherSuites']

    def get_cipher_suite(self, node: str):
        logger.info("Getting cipher suites of {}".format(node))
        api = 'http://{}:8091/settings/security'.format(node)
        return self.get(url=api).json()['cipherSuites']

    def set_cipher_suite(self, node: str, cipher_list: list):
        # There are around 200 different supported ciphers, but only 10 of them are supported by all
        # services. Only these 10 ciphers can be set globally. They are:
        # "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_GCM_SHA256",
        # "TLS_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        # "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        # "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        # "TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256". If we want to set any other cipher,
        # it needs to be set individually for each service that supports the cipher.
        logger.info("Setting cipher suite of {}".format(cipher_list))
        try:
            api = 'http://{}:8091/settings/security'.format(node)
            data = {
                'cipherSuites': json.dumps(cipher_list)
            }
            self.post(url=api, data=data)
            logger.info("The entire cipher suite has been set globally")
        except Exception:
            logger.info("The cipher suite cannot be set globally, will be set for each service")
            service_list = ['data', 'fullTextSearch', 'index', 'query', 'eventing', 'analytics',
                            'backup', 'clusterManager']
            for service in service_list:
                api = 'http://{}:8091/settings/security/{}'.format(node, service)
                try:
                    data = {
                        'cipherSuites': json.dumps(cipher_list)
                    }
                    self.post(url=api, data=data)
                    logger.info("The entire cipher suite has been set for the {} service"
                                .format(service))
                except Exception:
                    valid_cipher_list = []
                    supported_cipher_list = self.get_supported_ciphers(node, service)
                    for cipher in cipher_list:
                        if cipher in supported_cipher_list:
                            valid_cipher_list.append(cipher)
                    data = {
                        'cipherSuites': json.dumps(valid_cipher_list)
                    }
                    self.post(url=api, data=data)
                    logger.info("The following cipher suite: {} has been set for the {} service"
                                .format(valid_cipher_list, service))

    def set_minimum_tls_version(self, node: str, tls_version: str):
        logger.info("Setting minimum TLS version of {}".format(tls_version))
        # ClusterManager does not support tlsv1.3, so tlsv1.3 cannot be set as the global
        # minimum tls version. As a result, for all the services that support tlsv1.3
        # (Data, Search, Index, Query, Eventing, Analytics, Backup),
        # the minimum tls version must be set separately.
        if tls_version != 'tlsv1.3':
            api = 'http://{}:8091/settings/security'.format(node)
            data = {
                'tlsMinVersion': tls_version
            }
            self.post(url=api, data=data)
        else:
            build = self.get_version(node)
            version, build_number = build.split('-')
            build_version_number = tuple(map(int, version.split('.'))) + (int(build_number),)
            if build_version_number < (7, 1, 0, 0):
                logger.info("TLSv1.3 is not supported by this version of couchbase server")
                logger.info("Reverting to TLSv1.2")
                api = 'http://{}:8091/settings/security'.format(node)
                data = {
                    'tlsMinVersion': 'tlsv1.2'
                }
                self.post(url=api, data=data)
            else:
                data = {
                    'tlsMinVersion': tls_version
                }
                # Data
                api = 'http://{}:8091/settings/security/data'.format(node)
                self.post(url=api, data=data)
                # Search
                api = 'http://{}:8091/settings/security/fullTextSearch'.format(node)
                self.post(url=api, data=data)
                # Index
                api = 'http://{}:8091/settings/security/index'.format(node)
                self.post(url=api, data=data)
                # Query
                api = 'http://{}:8091/settings/security/query'.format(node)
                self.post(url=api, data=data)
                # Eventing
                api = 'http://{}:8091/settings/security/eventing'.format(node)
                self.post(url=api, data=data)
                # Analytics
                api = 'http://{}:8091/settings/security/analytics'.format(node)
                self.post(url=api, data=data)
                # Backup
                api = 'http://{}:8091/settings/security/backup'.format(node)
                self.post(url=api, data=data)

    def create_scope(self, host, bucket, scope):
        logger.info("Creating scope {}:{}".format(bucket, scope))
        if self.test_config.cluster.enable_n2n_encryption:
            protocol, port = 'https', 18091
        else:
            protocol, port = 'http', 8091
        api = '{}://{}:{}/pools/default/buckets/{}/scopes' \
            .format(protocol, host, port, bucket)
        data = {
            'name': scope
        }
        self.post(url=api, data=data)

    def delete_scope(self, host, bucket, scope):
        logger.info("Deleting scope {}:{}".format(bucket, scope))
        if self.test_config.cluster.enable_n2n_encryption:
            protocol, port = 'https', 18091
        else:
            protocol, port = 'http', 8091
        api = '{}://{}:{}/pools/default/buckets/{}/scopes' \
            .format(protocol, host, port, bucket)
        self.delete(url=api)

    def create_collection(self, host: str, bucket: str, scope: str, collection: str,
                          history: Optional[bool] = None) -> requests.Response:
        logger.info("Creating collection {}:{}.{} (history={})"
                    .format(bucket, scope, collection, history))
        if self.test_config.cluster.enable_n2n_encryption:
            protocol, port = 'https', 18091
        else:
            protocol, port = 'http', 8091
        api = '{}://{}:{}/pools/default/buckets/{}/scopes/{}/collections' \
            .format(protocol, host, port, bucket, scope)
        data = {
            'name': collection,
        }
        if history is not None:
            data['history'] = str(history).lower()
        self.post(url=api, data=data)

    def delete_collection(self, host, bucket, scope, collection):
        logger.info("Dropping collection {}:{}.{}".format(bucket, scope, collection))
        if self.test_config.cluster.enable_n2n_encryption:
            protocol, port = 'https', 18091
        else:
            protocol, port = 'http', 8091
        api = '{}://{}:{}/pools/default/buckets/{}/scopes/{}/collections/{}' \
            .format(protocol, host, port, bucket, scope, collection)
        self.delete(url=api)

    def set_collection_map(self, host, bucket, collection_map):
        logger.info("Setting collection map on {} via bulk api".format(bucket))
        if self.test_config.cluster.enable_n2n_encryption:
            protocol, port = 'https', 18091
        else:
            protocol, port = 'http', 8091
        api = '{}://{}:{}/pools/default/buckets/{}/scopes' \
            .format(protocol, host, port, bucket)
        self.put(url=api, data=json.dumps(collection_map))

    def create_server_group(self, host, server_group):
        logger.info('Creating Server Group {}'.format(server_group))
        api = 'http://{}:8091/pools/default/serverGroups'.format(host)
        data = {
            'name': server_group
        }
        return self.post(url=api, data=data)

    def get_server_group_info(self, host):
        api = 'http://{}:8091/pools/default/serverGroups'.format(host)
        return self.get(url=api).json()

    def change_group_membership(self, host, uri, node_json):
        api = 'http://{}:8091{}'.format(host, uri)
        self.put(url=api, data=json.dumps(node_json))

    def delete_server_group(self, host, uri):
        api = 'http://{}:8091{}'.format(host, uri)
        self.delete(url=api)

    def indexes_per_node(self, host: str):
        api = 'http://{}:{}/stats'.format(host, '9102')
        return self.get(url=api).json()['num_indexes']

    def indexes_instances_per_node(self, host: str):
        api = 'http://{}:{}/stats'.format(host, '9102')
        return self.get(url=api).json()['num_storage_instances']

    def backup_index(self, host, bucket):
        logger.info("Backing up index metadata on host {} bucket {}"
                    .format(host, bucket))
        api = 'http://{}:{}/api/v1/bucket/{}/backup?keyspace={}' \
            .format(host, '9102', bucket, bucket)
        return self.get(url=api).json()["result"]

    def restore_index(self, host, bucket, metadata, from_keyspace, to_keyspace):
        logger.info("Restoring indexes on host {} bucket {}".format(host, bucket))
        api = 'http://{}:{}/api/v1/bucket/{}/backup?keyspace={}:{}'. \
            format(host, '9102', bucket, from_keyspace, to_keyspace)
        return self.post(url=api, data=json.dumps(metadata))

    def set_plasma_diag(self, host: str, cmd: str, arg: str):
        logger.info('setting plasma diag to {} for {}'.format(cmd, arg))
        api = 'http://{}:{}/plasmaDiag'.format(host, '9102')
        data = {
            'Cmd': cmd, "Args": [arg]
        }
        r = self.post(url=api, data=json.dumps(data))
        if r.status_code not in (200, 202):
            logger.warning('Unexpected request status code {}'.format(r.status_code))

    def get_plasma_dbs(self, host: str):
        api = 'http://{}:{}/plasmaDiag'.format(host, '9102')
        data = {
            'Cmd': 'listDBs'
        }
        r = self.post(url=api, data=json.dumps(data))
        if r.status_code not in (200, 202):
            logger.warning('Unexpected request status code {}'.format(r.status_code))
        ids = [line.split()[-1] for line in r.text.split("\n")[1:-1]]
        logger.info('Plasma db list {}'.format(ids))
        return ids

    def set_analytics_replica(self, host: str, analytics_replica: int):
        logger.info('Setting analytics replica to: {}'.format(analytics_replica))
        api = 'http://{}:8091/settings/analytics'.format(host)
        data = {
            'numReplicas': analytics_replica,
        }
        self.post(url=api, data=data)

    def get_analytics_replica(self, host: str):
        api = 'http://{}:8091/settings/analytics'.format(host)
        return self.get(url=api).json()

    def get_sgversion(self, host: str) -> str:
        logger.info('Getting SG Server version on server: {}'.format(host))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:4985'.format(host)
        else:
            api = 'http://{}:4985'.format(host)
        r = self.get(url=api).json()
        return r['version'] \
            .replace('Couchbase Sync Gateway/', '') \
            .replace(') EE', '').replace('(', '-').split(';')[0]

    def get_sg_stats(self, host: str) -> dict:
        if self.cluster_spec.capella_infrastructure:
            api = 'https://{}:4988/metrics'.format(host)
        elif self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:4985/_expvar'.format(host)
        else:
            api = 'http://{}:4985/_expvar'.format(host)
        response = self.get(url=api)
        if self.cluster_spec.capella_infrastructure:
            return response.text
        return response.json()

    def start_sg_replication(self, host, payload):
        logger.info('Start sg replication.')
        logger.info('Payload: {}'.format(payload))

        if self.cluster_spec.capella_infrastructure or \
           self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:4985/_replicate'.format(host)
        else:
            api = 'http://{}:4985/_replicate'.format(host)
        logger.info('api: {}'.format(api))
        self.post(url=api, data=json.dumps(payload))

    def start_sg_replication2(self, host, payload):
        logger.info('Start sg replication.')
        logger.info('Payload: {}'.format(payload))

        if self.cluster_spec.capella_infrastructure or \
           self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:4985/db-1/_replication/'.format(host)
        else:
            api = 'http://{}:4985/db-1/_replication/'.format(host)
        logger.info('api: {}'.format(api))
        self.post(url=api, data=json.dumps(payload))

    def get_sgreplicate_stats(self, host: str, version: int) -> dict:
        if version == 1:
            if self.cluster_spec.capella_infrastructure or \
               self.test_config.cluster.enable_n2n_encryption:
                api = 'https://{}:4985/_active_tasks'.format(host)
            else:
                api = 'http://{}:4985/_active_tasks'.format(host)
        elif version == 2:
            if self.cluster_spec.capella_infrastructure or \
               self.test_config.cluster.enable_n2n_encryption:
                api = 'https://{}:4985/db-1/_replicationStatus'.format(host)
            else:
                api = 'http://{}:4985/db-1/_replicationStatus'.format(host)
        response = self.get(url=api)
        return response.json()

    def get_expvar_stats(self, host: str) -> dict:
        try:
            if self.cluster_spec.capella_infrastructure:
                api = 'https://{}:4988/metrics'.format(host)
            elif self.test_config.cluster.enable_n2n_encryption:
                api = 'https://{}:4985/_expvar'.format(host)
            else:
                api = 'http://{}:4985/_expvar'.format(host)
            response = self.get(url=api)
        except Exception as ex:
            logger.info("Expvar request failed: {}".format(ex))
            raise ex
        if self.cluster_spec.capella_infrastructure:
            return response
        return response.json()

    def start_cblite_replication_push(
            self,
            cblite_host,
            cblite_port,
            cblite_db,
            sgw_host,
            sgw_port=4984,
            user="guest",
            password="guest"):
        api = 'http://{}:{}/_replicate'.format(cblite_host, cblite_port)
        if self.cluster_spec.capella_infrastructure or \
           self.test_config.cluster.enable_n2n_encryption:
            ws_prefix = "wss://"
        else:
            ws_prefix = "ws://"
        if user and password:
            data = {
                "source": "{}".format(cblite_db),
                "target": "{}{}:{}/db-1".format(ws_prefix, sgw_host, sgw_port),
                "continuous": 1,
                "user": user,
                "password": password,
                "bidi": 0
            }
        else:
            data = {
                "source": "{}".format(cblite_db),
                "target": "{}{}:{}/db-1".format(ws_prefix, sgw_host, sgw_port),
                "continuous": 1,
                "bidi": 0
            }
        logger.info("The sgw host is: {}".format(sgw_host))
        logger.info("The url is: {}".format(api))
        logger.info("The json data is: {}".format(data))
        self.cblite_post(url=api, json=data, headers={'content-type': 'application/json'})
        logger.info("Successfully posted")

    def start_cblite_replication_pull(
            self,
            cblite_host,
            cblite_port,
            cblite_db,
            sgw_host,
            sgw_port=4984,
            user="guest",
            password="guest"):
        api = 'http://{}:{}/_replicate'.format(cblite_host, cblite_port)
        if self.cluster_spec.capella_infrastructure or \
           self.test_config.cluster.enable_n2n_encryption:
            ws_prefix = "wss://"
        else:
            ws_prefix = "ws://"
        if user and password:
            data = {
                "source": "{}{}:{}/db-1".format(ws_prefix, sgw_host, sgw_port),
                "target": "{}".format(cblite_db),
                "continuous": 1,
                "user": user,
                "password": password,
                "bidi": 0
            }
        else:
            data = {
                "source": "{}{}:{}/db-1".format(ws_prefix, sgw_host, sgw_port),
                "target": "{}".format(cblite_db),
                "continuous": 1,
                "bidi": 0
            }
        logger.info("The sgw host is: {}".format(sgw_host))
        logger.info("The url is: {}".format(api))
        logger.info("The json data is: {}".format(data))
        self.cblite_post(url=api, json=data, headers={'content-type': 'application/json'})
        logger.info("Successfully posted")

    def start_cblite_replication_bidi(
            self,
            cblite_host,
            cblite_port,
            cblite_db,
            sgw_host,
            sgw_port=4984,
            user="guest",
            password="guest"):
        api = 'http://{}:{}/_replicate'.format(cblite_host, cblite_port)
        if self.cluster_spec.capella_infrastructure or \
           self.test_config.cluster.enable_n2n_encryption:
            ws_prefix = "wss://"
        else:
            ws_prefix = "ws://"
        if user and password:
            data = {
                "source": "{}".format(cblite_db),
                "target": "{}{}:{}/db-1".format(ws_prefix, sgw_host, sgw_port),
                "continuous": 1,
                "user": user,
                "password": password,
                "bidi": 1
            }
        else:
            data = {
                "source": "{}".format(cblite_db),
                "target": "{}{}:{}/db-1".format(ws_prefix, sgw_host, sgw_port),
                "continuous": 1,
                "bidi": 1
            }
        logger.info("The sgw host is: {}".format(sgw_host))
        logger.info("The url is: {}".format(api))
        logger.info("The json data is: {}".format(data))
        self.cblite_post(url=api, json=data, headers={'content-type': 'application/json'})
        logger.info("Successfully posted the current sgw")

    def get_cblite_info(self, cblite_host, cblite_port, cblite_db):
        api = 'http://{}:{}/{}'.format(cblite_host, cblite_port, cblite_db)
        return self.cblite_get(url=api).json()

    def get_kv_ops(self, host):
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/stats/range/kv_ops?start=-1'.format(host)
        else:
            api = 'http://{}:8091/pools/default/stats/range/kv_ops?start=-1'.format(host)

        resp = self.get(url=api)
        return resp.json()

    def get_metering_stats(self, host, apply_functions=[]) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18091/pools/default/stats/range'.format(host)
        else:
            api = 'http://{}:8091/pools/default/stats/range'.format(host)

        data = [
            {
                "metric": [
                    {"label": "name", "value": metric}
                ],
                "applyFunctions": apply_functions,
                "step": 1,
                "start": -1
            }
            for metric in ['meter_ru_total', 'meter_wu_total', 'meter_cu_total']
        ]

        resp = self.post(url=api, json=data)
        return resp.json()


class KubernetesRestHelper(RestBase):

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.remote = RemoteHelper(cluster_spec)
        self.ip_table, self.port_translation = self.remote.get_ip_port_mapping()

    def translate_host_and_port(self, host, port):
        trans_host = self.ip_table.get(host)
        trans_port = self.port_translation.get(trans_host).get(str(port))
        return trans_host, trans_port

    def exec_n1ql_statement(self, host: str, statement: str, query_context: str = None) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            host, port = self.translate_host_and_port(host, '18093')
            api = 'https://{}:{}/query/service' \
                .format(host, port)
        else:
            host, port = self.translate_host_and_port(host, '8093')
            api = 'http://{}:{}/query/service' \
                .format(host, port)

        data = {
            'statement': statement,
        }
        if query_context:
            data['query_context'] = query_context
        response = self.post(url=api, data=data)
        return response.json()

    # indexer endpoints not yet exposed by operator
    def get_index_status(self, host: str) -> dict:
        return {'status': [{'status': 'Ready'}]}

    # indexer endpoints not yet exposed by operator
    def get_gsi_stats(self, host: str) -> dict:
        return {'num_docs_queued': 0, 'num_docs_pending': 0}

    def get_active_nodes_by_role(self, master_node: str, role: str) -> List[str]:
        active_nodes_by_role = []
        for node in self.cluster_spec.servers_by_role(role):
            active_nodes_by_role.append(node)
        return active_nodes_by_role

    def node_statuses(self, host: str) -> dict:
        host, port = self.translate_host_and_port(host, '8091')
        api = 'http://{}:{}/nodeStatuses' \
            .format(host, port)
        data = self.get(url=api).json()
        return {node: info['status'] for node, info in data.items()}

    def get_version(self, host: str) -> str:
        logger.info('Getting Couchbase Server version')
        if self.test_config.cluster.enable_n2n_encryption:
            host, port = self.translate_host_and_port(host, '18091')
            api = 'https://{}:{}/pools/' \
                .format(host, port)
        else:
            host, port = self.translate_host_and_port(host, '8091')
            api = 'http://{}:{}/pools/' \
                .format(host, port)
        r = self.get(url=api).json()
        return r['implementationVersion'] \
            .replace('-rel-enterprise', '') \
            .replace('-enterprise', '') \
            .replace('-community', '')

    def get_bucket_stats(self, host: str, bucket: str) -> dict:
        host, port = self.translate_host_and_port(host, '8091')
        api = 'http://{}:{}/pools/default/buckets/{}/stats' \
            .format(host, port, bucket)
        return self.get(url=api).json()

    def get_bucket_info(self, host: str, bucket: str) -> List[str]:
        host, port = self.translate_host_and_port(host, '8091')
        api = 'http://{}:{}/pools/default/buckets/{}' \
            .format(host, port, bucket)
        return self.get(url=api).json()

    def flush_bucket(self, host: str, bucket: str):
        logger.info('Flushing bucket: {}'.format(bucket))
        host, port = self.translate_host_and_port(host, '8091')
        api = 'http://{}:{}/pools/default/buckets/{}/controller/doFlush'.format(host, port, bucket)
        self.post(url=api)


class CapellaRestBase(DefaultRestHelper):

    def __init__(self, cluster_spec, test_config):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.base_url = 'https://cloudapi.{}.nonprod-project-avengers.com'.format(
            self.cluster_spec.infrastructure_settings['cbc_env']
        )
        self.tenant_id = self.cluster_spec.infrastructure_settings['cbc_tenant']
        self.project_id = self.cluster_spec.infrastructure_settings['cbc_project']
        self.cluster_ids = self.cluster_spec.infrastructure_settings['cbc_cluster'].split()

        access, secret = os.getenv('CBC_ACCESS_KEY'), os.getenv('CBC_SECRET_KEY')
        if (not access or not secret) and os.path.isfile(CAPELLA_CREDS_FILE):
            with open(CAPELLA_CREDS_FILE, 'r') as f:
                creds = json.load(f)
            access, secret = creds.get('access'), creds.get('secret')

        self.dedicated_client = CapellaAPIDedicated(
            self.base_url, secret, access, self._cbc_user, self._cbc_pwd, self._cbc_token
        )
        self.admin_credentials = self.cluster_spec.capella_admin_credentials

    @contextmanager
    def _admin_creds(self, host: str):
        for i, (_, hostnames) in enumerate(self.cluster_spec.clusters):
            if host in hostnames:
                username, password = self.admin_credentials[i]
                self.auth = username, password

        try:
            yield
        finally:
            self.auth = self.rest_username, self.rest_password

    @property
    def _cbc_user(self):
        return os.getenv('CBC_USER')

    @property
    def _cbc_pwd(self):
        return os.getenv('CBC_PWD')

    @property
    def _cbc_token(self):
        return os.getenv('CBC_TOKEN_FOR_INTERNAL_SUPPORT')

    @retry
    def trigger_log_collection(self, cluster_id: str):
        resp = self.dedicated_client.trigger_log_collection(cluster_id)
        return resp

    def trigger_all_cluster_log_collection(self):
        for cluster_id in self.cluster_ids:
            logger.info('Triggering log collection for clusterId: {}'.format(cluster_id))
            resp = self.trigger_log_collection(cluster_id)
        return resp

    @retry
    def get_cluster_tasks(self, cluster_id: str):
        resp = self.dedicated_client.get_cluster_tasks(cluster_id)
        return resp

    def get_log_information(self, cluster_id: str):
        resp = self.get_cluster_tasks(cluster_id)
        for i in resp.json():
            if i['type'] == 'clusterLogsCollection':
                return i

    def _check_if_given_clusters_are_uploaded(self, cluster_ids_not_uploaded: list):
        clusters_not_uploaded = []
        waiting_states = ['running', 'pending']

        for cluster_id in cluster_ids_not_uploaded:
            log_info = self.get_log_information(cluster_id)
            if log_info['status'] in waiting_states:
                logger.info('Progress {} for cluster {}'.format(log_info['progress'], cluster_id))
                clusters_not_uploaded.append(cluster_id)
            elif log_info['status'] == 'completed':
                logger.info('Cluster {} logs uploaded'.format(cluster_id))
            else:
                logger.interrupt('Failed to upload logs for cluster {}'.format(cluster_id))

        return clusters_not_uploaded

    def wait_until_all_logs_uploaded(self):
        cluster_ids_not_uploaded = [cluster_id for cluster_id in self.cluster_ids]
        t0 = time.time()
        timeout_mins = 20

        while (time.time() - t0) < timeout_mins * 60:
            cluster_ids_not_uploaded = \
                 self._check_if_given_clusters_are_uploaded(cluster_ids_not_uploaded)
            if not cluster_ids_not_uploaded:
                logger.info('All cluster logs have been successfully uploaded')
                return
            logger.info('Waiting for cluster logs to be uploaded')
            time.sleep(60)

        logger.interrupt(
            'Waiting for logs to upload has timed out after {} mins'.format(timeout_mins))

    def get_node_log_info(self, cluster_id: str) -> dict[list]:
        log_nodes = {}
        log_info = self.get_log_information(cluster_id)
        for node in log_info['perNode']:
            log_nodes[node] = (log_info['perNode'][node]['path'],
                               log_info['perNode'][node]['url'])
        return log_nodes

    def get_all_cluster_node_logs(self):
        log_nodes = {}
        for cluster_id in self.cluster_ids:
            logger.info('Getting node information from the cluster id: {}'.format(cluster_id))
            log_nodes = log_nodes | self.get_node_log_info(cluster_id)
        return log_nodes


class ProvisionedCapellaRestHelper(CapellaRestBase):

    def hostname_to_cluster_id(self, host: str):
        if len(self.cluster_ids) == 1:
            return self.cluster_ids[0]

        for i, (_, hostnames) in enumerate(self.cluster_spec.clusters):
            if host in hostnames:
                return self.cluster_ids[i]
        return None

    def get_remote_clusters(self, host: str) -> List[Dict]:
        logger.info('Getting remote clusters')
        api = 'https://{}:18091/pools/default/remoteClusters'.format(host)
        with self._admin_creds(host):
            response = self.get(url=api)
        return response.json()

    def get_active_nodes_by_role(self, master_node: str, role: str) -> List[str]:
        cluster_idx = self.cluster_ids.index(self.hostname_to_cluster_id(master_node))
        return self.cluster_spec.servers_by_cluster_and_role(role)[cluster_idx]

    def create_db_user_all_clusters(self, username: str, password: str):
        for cluster_id in self.cluster_ids:
            self.create_db_user(cluster_id, username, password)

    @retry
    def create_db_user(self, cluster_id: str, username: str, password: str):
        logger.info('Adding DB credential')
        resp = self.dedicated_client.create_db_user(self.tenant_id, self.project_id, cluster_id,
                                                    username, password)
        return resp

    @retry
    def create_bucket(self, host: str,
                      name: str,
                      ram_quota: int,
                      replica_number: int = 1,
                      conflict_resolution_type: str = "seqno",
                      flush: bool = True,
                      durability: str = "none",
                      backend_storage: str = "couchstore",
                      eviction_policy: str = "nruEviction",
                      ttl_value: int = 0,
                      ttl_unit: str = None):
        cluster_id = self.hostname_to_cluster_id(host)

        logger.info('Adding new bucket on cluster {}: {}'.format(cluster_id, name))

        bucket_type = 'couchbase'

        data = {
            'name': name,
            'bucketConflictResolution': conflict_resolution_type,
            'memoryAllocationInMb': ram_quota,
            'flush': flush,
            'replicas': replica_number,
            'durabilityLevel': durability,
            'storageBackend': backend_storage,
            'timeToLive': {
                'unit': ttl_unit,
                'value': ttl_value
            },
            'type': bucket_type
        }
        if self.test_config.bucket.bucket_type == 'ephemeral':
            data['type'] = 'ephemeral'
            data['storageBackend'] = None
            data['evictionPolicy'] = eviction_policy

        logger.info('Bucket configuration: {}'.format(pretty_dict(data)))

        resp = self.dedicated_client.create_bucket(
            self.tenant_id, self.project_id, cluster_id, data
        )
        return resp

    def allow_my_ip_all_clusters(self):
        for cluster_id in self.cluster_ids:
            self.allow_my_ip(cluster_id)

    def add_allowed_ips_all_clusters(self, ips: list[str]):
        for cluster_id in self.cluster_ids:
            self.add_allowed_ips(cluster_id, ips)

    @retry
    def allow_my_ip(self, cluster_id):
        logger.info('Whitelisting own IP on cluster {}'.format(cluster_id))
        resp = self.dedicated_client.allow_my_ip(self.tenant_id, self.project_id, cluster_id)
        return resp

    @retry
    def add_allowed_ips(self, cluster_id, ips: list[str]):
        logger.info('Whitelisting IPs on cluster {}: {}'.format(cluster_id, ips))
        resp = self.dedicated_client.add_allowed_ips(
            self.tenant_id, self.project_id, cluster_id, ips)
        return resp

    @retry
    def flush_bucket(self, host: str, bucket: str):
        cluster_id = self.hostname_to_cluster_id(host)
        bucket_id = base64.urlsafe_b64encode(bucket.encode()).decode()
        logger.info('Flushing bucket on cluster {}: {}'.format(cluster_id, bucket_id))
        resp = self.dedicated_client.flush_bucket(
            self.tenant_id, self.project_id, cluster_id, bucket_id)
        return resp

    @retry
    def delete_bucket(self, host: str, bucket: str):
        cluster_id = self.hostname_to_cluster_id(host)
        logger.info('Deleting bucket on cluster {}: {}'.format(cluster_id, bucket))
        resp = self.dedicated_client.delete_bucket(
            self.tenant_id, self.project_id, cluster_id, bucket)
        return resp

    @retry
    def update_cluster_configuration(self, host: str, new_cluster_config: dict):
        cluster_id = self.hostname_to_cluster_id(host)
        logger.info('Updating cluster config for cluster {}. New config: {}'
                    .format(cluster_id, pretty_dict(new_cluster_config)))
        resp = self.dedicated_client.update_specs(self.tenant_id, self.project_id, cluster_id,
                                                  new_cluster_config)
        return resp

    def create_replication(self, host: str, params: dict):
        logger.info('Creating XDCR replication with parameters: {}'.format(pretty_dict(params)))
        cluster_id = self.hostname_to_cluster_id(host)
        resp = self.dedicated_client.create_xdcr_replication(self.tenant_id, self.project_id,
                                                             cluster_id, params)
        logger.info('XDCR replication created.')
        return resp

    def get_all_cluster_nodes(self):
        cluster_nodes = {}
        for i, cluster_id in enumerate(self.cluster_ids):
            resp = self.dedicated_client.get_nodes(tenant_id=self.tenant_id,
                                                   project_id=self.project_id,
                                                   cluster_id=cluster_id)
            nodes = resp.json()['data']
            nodes = [node['data'] for node in nodes]
            services_per_node = {node['hostname']: node['services'] for node in nodes}

            kv_nodes = []
            non_kv_nodes = []
            for hostname, services in services_per_node.items():
                services_string = ','.join(SERVICES_CAPELLA_TO_PERFRUNNER[svc] for svc in services)
                if 'kv' in services_string:
                    kv_nodes.append("{}:{}".format(hostname, services_string))
                else:
                    non_kv_nodes.append("{}:{}".format(hostname, services_string))

            cluster_name = self.cluster_spec.config.options('clusters')[i]
            cluster_nodes[cluster_name] = kv_nodes + non_kv_nodes
        return cluster_nodes

    @retry
    def backup(self, host: str, bucket: str):
        cluster_id = self.hostname_to_cluster_id(host)
        logger.info('Triggering backup.')
        resp = self.dedicated_client.backup_now(self.tenant_id, self.project_id, cluster_id,
                                                bucket)
        return resp

    def wait_for_backup(self, host: str):
        cluster_id = self.hostname_to_cluster_id(host)
        while True:
            time.sleep(30)
            resp = self.dedicated_client.get_backups(self.tenant_id, self.project_id, cluster_id)
            if resp is not None:
                logger.info(str(resp.json()))

                if resp.json()['data']:
                    if 'elapsedTimeInSeconds' in resp.json()['data'][0]['data']:
                        if resp.json()['data'][0]['data']['status'] != 'pending':
                            logger.info('Backup complete.')
                            return resp.json()['data'][0]['data']['elapsedTimeInSeconds']

    def restore(self, host: str, bucket: str):
        cluster_id = self.hostname_to_cluster_id(host)
        logger.info('Triggering restore.')
        self.dedicated_client.restore_from_backup(self.tenant_id, self.project_id, cluster_id,
                                                  bucket)

    def wait_for_restore_initialize(self, host: str, bucket):
        cluster_id = self.hostname_to_cluster_id(host)
        while True:
            try:
                resp = self.dedicated_client.get_restores(self.tenant_id, self.project_id,
                                                          cluster_id, bucket)
                if resp.json()['status']:
                    break
            except JSONDecodeError:
                if resp.status_code == 204:
                    continue
                else:
                    logger.info('Restore trigger failed, error code: {}'.format(resp.status_code))

    def wait_for_restore(self, host: str, bucket):
        cluster_id = self.hostname_to_cluster_id(host)
        while True:
            resp = self.dedicated_client.get_restores(self.tenant_id, self.project_id, cluster_id,
                                                      bucket)
            if resp.json()['status'] == 'complete':
                break

    def get_sgversion(self, host: str) -> str:
        logger.info('Getting SG Server version on server: {}'.format(host))
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        build = \
            self.dedicated_client.get_sgw_info(self.tenant_id,
                                               self.project_id,
                                               cluster_id,
                                               sgw_cluster_id).json() \
                                                              .get('data') \
                                                              .get('config') \
                                                              .get('version') \
                                                              .get('sync_gateway_version')
        return build


class ServerlessRestHelper(CapellaRestBase):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        DefaultRestHelper.__init__(self, cluster_spec=cluster_spec, test_config=test_config)
        self.base_url = 'https://cloudapi.{}.nonprod-project-avengers.com'.format(
            self.cluster_spec.infrastructure_settings['cbc_env']
        )
        self.tenant_id = self.cluster_spec.infrastructure_settings['cbc_tenant']
        self.project_id = self.cluster_spec.infrastructure_settings['cbc_project']
        self.dp_id = self.cluster_spec.infrastructure_settings['cbc_dataplane']
        self.cluster_ids = self.cluster_spec.infrastructure_settings['cbc_cluster'].split()
        self.dedicated_client = CapellaAPIDedicated(
            self.base_url, None, None, self._cbc_user, self._cbc_pwd, self._cbc_token
        )
        self.serverless_client = CapellaAPIServerless(
            self.base_url, self._cbc_user, self._cbc_pwd, self._cbc_token
        )
        self.admin_credentials = self.cluster_spec.capella_admin_credentials

    def get_db_info(self, db_id):
        logger.info('Getting debug info for DB {}'.format(db_id))
        resp = self.serverless_client.get_database_debug_info(db_id)
        resp.raise_for_status()
        return resp.json()

    def get_dataplane_info(self) -> dict:
        resp = self.serverless_client.get_serverless_dataplane_info(self.dp_id)
        resp.raise_for_status()
        return resp.json()

    @retry
    def allow_my_ip(self, db_id):
        logger.info('Whitelisting own IP on DB {}'.format(db_id))
        resp = self.serverless_client.allow_my_ip(self.tenant_id, self.project_id, db_id)
        return resp

    @retry
    def add_allowed_ips(self, db_id, ips: list[str]):
        logger.info('Whitelisting IPs on DB {}: {}'.format(db_id, ips))

        data = {
            "create": [
                {"cidr": "{}/32".format(ip), "comment": ""} for ip in ips
            ]
        }

        resp = self.serverless_client.add_ip_allowlists(self.tenant_id, db_id, self.project_id,
                                                        data)
        return resp

    @retry
    def bypass_nebula(self, client_ip):
        logger.info('Bypassing Nebula for {}'.format(client_ip))

        url = "{}/internal/support/serverless-dataplanes/{}/bypass"\
              .format(self.base_url.replace('cloudapi', 'api'), self.dp_id)

        body = {"allowCIDR": "{}/32".format(client_ip)}

        resp = self.serverless_client.request(url, "POST", params=json.dumps(body))
        resp.raise_for_status()
        return resp

    @retry
    def get_db_api_key(self, db_id):
        logger.info('Getting API key for serverless DB {}'.format(db_id))
        resp = self.serverless_client.generate_keys(self.tenant_id, self.project_id, db_id)
        return resp

    @retry
    def _update_db(self, db_id, width, weight):
        data = {
            'overRide': {
                'width': width,
                'weight': weight
            }
        }
        resp = self.serverless_client.update_db(db_id, data)
        return resp

    def update_db(self, db_id, width=None, weight=None):
        if not (weight or width):
            return

        self._update_db(db_id, width, weight)

        db_map = self.test_config.serverless_db.db_map
        db_map[db_id]['width'] = width
        db_map[db_id]['weight'] = weight
        self.test_config.serverless_db.update_db_map(db_map)

    def _get_dataplane_node_configs(self):
        resp = self.serverless_client.get_serverless_dataplane_node_configs(self.dp_id)
        resp.raise_for_status()
        return resp.json()

    def get_nebula_certificate(self):
        dp_node_configs = self._get_dataplane_node_configs()
        for node in dp_node_configs['state']:
            if node['kind'] == 'nebula':
                certs = node['config']['gateway']['certificate']
                first_cert = certs['certificate']
                intermediates = certs['intermediates']
                return first_cert + intermediates

    def get_dapi_certificate(self):
        dp_node_configs = self._get_dataplane_node_configs()
        for node in dp_node_configs['state']:
            if node['kind'] == 'dataApi':
                certs = node['config']['gateway']['certificate']
                first_cert = certs['certificate']
                intermediates = certs['intermediates']
                return first_cert + intermediates

    @contextmanager
    def _bucket_creds(self, bucket: str):
        access = self.test_config.serverless_db.db_map[bucket]['access']
        secret = self.test_config.serverless_db.db_map[bucket]['secret']
        self.auth = access, secret

        try:
            yield
        finally:
            self.auth = self.rest_username, self.rest_password

    def create_fts_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new FTS index: {}'.format(index))
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/index/{}'.format(host, index)
        else:
            api = 'http://{}:8094/api/index/{}'.format(host, index)
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)
        bucket = definition['sourceName']
        with self._bucket_creds(bucket):
            self.put(url=api, data=data, headers=headers)

    def get_fts_doc_count(self, host: str, index: str, bucket: str) -> int:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/index/{}/count'.format(host, index)
        else:
            api = 'http://{}:8094/api/index/{}/count'.format(host, index)
        with self._bucket_creds(bucket):
            response = self.get(url=api).json()
        return response['count']

    def exec_n1ql_statement(self, host: str, statement: str, query_context: str) -> dict:
        api = 'https://{}:18091/_p/query/query/service'.format(host)

        data = {
            'statement': statement,
            'query_context': query_context
        }

        bucket = query_context.removeprefix('default:').split('.')[0].strip('`')

        with self._bucket_creds(bucket):
            response = self.post(url=api, data=data)

        return response.json()

    def get_active_nodes_by_role(self, master_node: str, role: str) -> List[str]:
        resp = self.get_dataplane_info()
        node_list = resp['couchbase']['nodes']
        has_role = []
        for node_config in node_list:
            if role in node_config['services'][0]['type']:
                has_role.append(node_config['hostname'])
        return has_role

    def get_bucket_fts_stats(self, host: str, bucket, index) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/nsstats/index/{}'.format(host, index)
        else:
            api = 'http://{}:8094/api/nsstats/index/{}'.format(host, index)
        with self._bucket_creds(bucket):
            response = self.get(url=api)
        return response.json()

    @contextmanager
    def _admin_creds(self, host: str):
        for i, (_, hostnames) in enumerate(self.cluster_spec.clusters):
            if host in hostnames:
                username, password = self.admin_credentials[i]
                self.auth = username, password

        try:
            yield
        finally:
            self.auth = self.rest_username, self.rest_password

    def get_fts_stats(self, host: str) -> dict:
        if self.test_config.cluster.enable_n2n_encryption:
            api = 'https://{}:18094/api/nsstats'.format(host)
        else:
            api = 'http://{}:8094/api/nsstats'.format(host)
        with self._admin_creds(host):
            response = self.get(url=api)
        return response.json()

import base64
import json
import os
import time
from collections import namedtuple
from json import JSONDecodeError
from typing import Callable, Iterator, Literal, Optional, Union
from urllib.parse import urlparse

import requests
from capella.columnar.CapellaAPI import CapellaAPI as CapellaAPIColumnar
from capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIDedicated
from capella.serverless.CapellaAPI import CapellaAPI as CapellaAPIServerless
from decorator import decorator
from fabric.api import local
from requests.exceptions import ConnectionError

from logger import logger
from perfrunner.helpers.misc import create_build_tuple, pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import BucketSettings, ClusterSpec, TestConfig
from perfrunner.utils.terraform import (
    SERVICES_CAPELLA_TO_PERFRUNNER,
    ControlPlaneManager,
)

MAX_RETRY = 20
RETRY_DELAY = 10

# Used ports, named to match doc:
# https://docs.couchbase.com/server/current/install/install-ports.html#detailed-port-description
REST_PORT = 8091
REST_PORT_SSL = 18091
ANALYTICS_PORT = 8095
ANALYTICS_PORT_SSL = 18095
ANALYTICS_ADMIN_PORT = 9110
ANALYTICS_ADMIN_PORT_SSL = 19110
EVENTING_PORT = 8096
EVENTING_PORT_SSL = 18096
FTS_PORT = 8094
FTS_PORT_SSL = 18094
INDEXING_PORT = 9102
INDEXING_PORT_SSL = 19102
QUERY_PORT = 8093
QUERY_PORT_SSL = 18093
SGW_ADMIN_PORT = 4985
SGW_PUBLIC_PORT = 4984
SGW_APPSERVICE_METRICS_PORT = 4988

# Elasticsearch
ELASTICSEARCH_REST_PORT = 9200
ELASTICSEARCH_REST_PORT_SSL = 19200


@decorator
def retry(method: Callable, *args, **kwargs):
    r = namedtuple('request', ['url'])('')
    url = kwargs.get('url')
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
        url, MAX_RETRY
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
            return CapellaServerlessRestHelper(cluster_spec, test_config)
        elif cluster_spec.capella_infrastructure:
            if cluster_spec.columnar_infrastructure:
                return CapellaColumnarRestHelper(cluster_spec, test_config)
            return CapellaProvisionedRestHelper(cluster_spec, test_config)
        else:
            return DefaultRestHelper(cluster_spec, test_config)


class RestBase:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        self.rest_username, self.rest_password = cluster_spec.rest_credentials
        self.auth = self.rest_username, self.rest_password
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.use_tls = test_config.cluster.enable_n2n_encryption or \
            self.cluster_spec.capella_infrastructure

    def _set_auth(self, **kwargs) -> tuple[str, str]:
        return self.auth

    @retry
    def get(self, **kwargs) -> requests.Response:
        kwargs.setdefault("auth", self._set_auth(**kwargs))
        return requests.get(verify=False, **kwargs)

    def _post(self, **kwargs) -> requests.Response:
        kwargs.setdefault("auth", self._set_auth(**kwargs))
        return requests.post(verify=False, **kwargs)

    @retry
    def post(self, **kwargs) -> requests.Response:
        return self._post(**kwargs)

    @retry
    def session_post(self, session: requests.Session, **kwargs) -> requests.Response:
        kwargs.setdefault("auth", self._set_auth(**kwargs))
        return session.post(verify=False, **kwargs)

    def _put(self, **kwargs) -> requests.Response:
        kwargs.setdefault("auth", self._set_auth(**kwargs))
        return requests.put(verify=False, **kwargs)

    @retry
    def put(self, **kwargs) -> requests.Response:
        return self._put(**kwargs)

    def _delete(self, **kwargs) -> requests.Response:
        kwargs.setdefault("auth", self._set_auth(**kwargs))
        return requests.delete(verify=False, **kwargs)

    def delete(self, **kwargs) -> requests.Response:
        return self._delete(**kwargs)

    @retry
    def cblite_post(self, **kwargs) -> requests.Response:
        return requests.post(**kwargs)

    @retry
    def cblite_get(self, **kwargs) -> requests.Response:
        return requests.get(**kwargs)

    def _get_api_url(self, host: str, path: str,
                     plain_port: str = REST_PORT, ssl_port: str = REST_PORT_SSL) -> str:
        """Return a url in the form of `{proto}://{host}:{port}/{path}`."""
        port, proto = (ssl_port, 'https') if self.use_tls else (plain_port, 'http')

        return f"{proto}://{host}:{port}/{path.lstrip('/')}"


class DefaultRestHelper(RestBase):

    def __init__(self, cluster_spec, test_config):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)

    def _set_service_path(
        self, host: str, service: Literal["data", "index", "cbas"], path: Union[str, list[str]]
    ):
        logger.info(f"Configuring {service} path on {host}: {path}")

        api = self._get_api_url(host=host, path="nodes/self/controller/settings")
        setting = f"{service}_path" if service != "data" else "path"
        data = {setting: path}
        self.post(url=api, data=data)

    def set_data_path(self, host: str, path: str):
        self._set_service_path(host, "data", path)

    def set_index_path(self, host: str, path: str):
        self._set_service_path(host, "index", path)

    def set_analytics_paths(self, host: str, paths: list[str]):
        self._set_service_path(host, "cbas", paths)

    def set_auth(self, host: str):
        logger.info(f"Configuring cluster authentication: {host}")

        api = self._get_api_url(host=host, path="settings/web")
        data = {"username": self.rest_username, "password": self.rest_password, "port": "SAME"}
        self.post(url=api, data=data)

    def rename(self, host: str, new_host: str = None):
        new_host = new_host or host
        logger.info(f"Changing server name: {host} -> {new_host}")

        api = self._get_api_url(host=host, path="node/controller/rename")
        data = {"hostname": new_host}
        self.post(url=api, data=data)

    def _set_service_mem_quota(
        self,
        host: str,
        service: Literal["data", "index", "fts", "cbas", "eventing"],
        mem_quota: int,
    ):
        logger.info(f"Configuring {service} RAM quota: {mem_quota} MB")

        api = self._get_api_url(host=host, path="pools/default")
        setting = f"{service}MemoryQuota" if service != "data" else "memoryQuota"
        data = {setting: mem_quota}
        self.post(url=api, data=data)

    def set_kv_mem_quota(self, host: str, mem_quota: str):
        self._set_service_mem_quota(host, "data", mem_quota)

    def set_index_mem_quota(self, host: str, mem_quota: int):
        self._set_service_mem_quota(host, "index", mem_quota)

    def set_fts_index_mem_quota(self, host: str, mem_quota: int):
        self._set_service_mem_quota(host, "fts", mem_quota)

    def set_analytics_mem_quota(self, host: str, mem_quota: int):
        self._set_service_mem_quota(host, "cbas", mem_quota)

    def set_eventing_mem_quota(self, host: str, mem_quota: int):
        self._set_service_mem_quota(host, "eventing", mem_quota)

    def set_query_settings(self, host: str, override_settings: dict):
        api = self._get_api_url(
            host=host, path="admin/settings", plain_port=QUERY_PORT, ssl_port=QUERY_PORT_SSL
        )

        settings = self.get(url=api).json()
        for override, value in override_settings.items():
            if override not in settings:
                logger.error(f"Cannot change query setting {override} to {value}, setting invalid")
                continue
            settings[override] = value
            logger.info(f"Changing {override} to {value}")
        self.post(url=api, data=json.dumps(settings))

    def get_query_settings(self, host: str) -> dict:
        api = self._get_api_url(
            host=host, path="admin/settings", plain_port=QUERY_PORT, ssl_port=QUERY_PORT_SSL
        )

        return self.get(url=api).json()

    def set_index_settings(self, host: str, settings: dict):
        api = self._get_api_url(
            host=host, path="settings", plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL
        )
        count = 0
        curr_settings = self.get_index_settings(host)
        for option, value in settings.items():
            if option in curr_settings:
                if "enableInMemoryCompression" in option and not value:
                    while count <= 10:
                        compression = self.get_index_settings(host)[option]
                        if compression:
                            logger.info(f"current compression settings {compression}")
                            break
                        else:
                            time.sleep(30)
                            compression = self.get_index_settings(host)[option]
                        count += 1
                    if count == 10:
                        raise Exception("Unable to set compression disabled after 5 min")
                logger.info(f"Changing {option} to {value}")
                self.post(url=api, data=json.dumps({option: value}))
            else:
                logger.warn(f"Skipping unknown option: {option}")

    def set_planner_settings(self, host: str, settings: dict):
        logger.info('Changing host {} to {}'.format(host, settings))
        url = self._get_api_url(host=host, path='settings/planner', plain_port=INDEXING_PORT,
                                ssl_port=INDEXING_PORT_SSL)
        self.post(url=url, data=settings)

    def get_index_metadata(self, host: str) -> dict:
        url = self._get_api_url(host=host, path='getLocalIndexMetadata',
                                plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL)
        return self.get(url=url).json()

    def get_index_settings(self, host: str) -> dict:
        url = self._get_api_url(host=host, path='settings?internal=ok',
                                plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL)
        return self.get(url=url).json()

    def get_gsi_stats(self, host: str) -> dict:
        url = self._get_api_url(host=host, path='stats',
                                plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL)
        return self.get(url=url).json()

    def create_index(self, host: str, bucket: str, name: str, field: str,
                     storage: str = 'memdb', scope: str = '_default',
                     collection: str = '_default'):
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
        url = self._get_api_url(host=host, path='createIndex', plain_port=INDEXING_PORT,
                                ssl_port=INDEXING_PORT_SSL)
        self.post(url=url, data=json.dumps(data))

    def set_services(self, host: str, services: str):
        logger.info(f"Configuring services on {host}: {services}")

        api = self._get_api_url(host=host, path="node/controller/setupServices")
        data = {'services': services}
        self.post(url=api, data=data)

    def add_node(self, host: str, new_host: str, services: str = None):
        logger.info('Adding new node: {}'.format(new_host))
        data = {
            'hostname': new_host,
            'user': self.rest_username,
            'password': self.rest_password,
            'services': services,
        }

        url = self._get_api_url(host=host, path='controller/addNode')
        self.post(url=url, data=data)

    def add_node_to_group(self, host: str, new_host: str, services: str = None, group: str = None):
        logger.info('Adding new node: {} to group {}'.format(new_host, group))
        server_group_info = self.get_server_group_info(host)["groups"]
        for group_info in server_group_info:
            if group_info['name'] == group:
                add_node_uri = group_info['addNodeURI']
                break

        data = {
            'hostname': new_host,
            'user': self.rest_username,
            'password': self.rest_password,
            'services': services,
        }

        url = self._get_api_url(host=host, path=add_node_uri[1:])
        self.post(url=url, data=data)

    def rebalance(self, host: str, known_nodes: list[str], ejected_nodes: list[str]):
        logger.info('Starting rebalance')
        known_nodes = ','.join(map(self.get_otp_node_name, known_nodes))
        ejected_nodes = ','.join(map(self.get_otp_node_name, ejected_nodes))
        data = {
            'knownNodes': known_nodes,
            'ejectedNodes': ejected_nodes
        }

        url = self._get_api_url(host=host, path='controller/rebalance')
        self.post(url=url, data=data)

    def increase_bucket_limit(self, host: str, num_buckets: int):
        logger.info('increasing bucket limit to {}'.format(num_buckets))

        data = {
            'maxBucketCount': num_buckets
        }

        url = self._get_api_url(host=host, path='internalSettings')
        self.post(url=url, data=data)

    def get_counters(self, host: str) -> dict:
        return self.get(url=self._get_api_url(host=host, path='pools/default')).json()['counters']

    def is_balanced(self, host: str) -> bool:
        logger.info(f"Checking if cluster is balanced using rebalance counters ({host})")
        counters = self.get_counters(host)
        logger.info(f"counters : {counters}")
        return counters.get("rebalance_start", 0) == counters.get("rebalance_success", 0)

    def get_failover_counter(self, host: str) -> int:
        logger.info('Checking failover counter ({})'.format(host))
        counters = self.get_counters(host)
        return counters.get('failover_node')

    def get_tasks(self, host: str) -> dict:
        return self.get(url=self._get_api_url(host=host, path='pools/default/tasks')).json()

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
        self.delete(url=self._get_api_url(host=host, path='pools/default/buckets/{}'.format(name)))

    def create_bucket(
        self,
        host: str,
        name: str,
        ram_quota: int,
        replica_number: int,
        replica_index: int,
        eviction_policy: str,
        bucket_type: str,
        magma_seq_tree_data_block_size: int = 0,
        backend_storage: Optional[str] = None,
        conflict_resolution_type: Optional[str] = None,
        compression_mode: Optional[str] = None,
        history_seconds: int = 0,
        history_bytes: int = 0,
        max_ttl: int = 0,
    ) -> requests.Response:
        logger.info('Adding new bucket: {}'.format(name))

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

        url = self._get_api_url(host=host, path='pools/default/buckets')
        self.post(url=url, data=data)

    def flush_bucket(self, host: str, bucket: str):
        logger.info('Flushing bucket: {}'.format(bucket))
        url = self._get_api_url(host=host,
                                path='pools/default/buckets/{}/controller/doFlush'.format(bucket))
        self.post(url=url)

    def configure_auto_compaction(self, host, settings):
        logger.info('Applying auto-compaction settings: {}'.format(settings))
        data = {
            'databaseFragmentationThreshold[percentage]': settings.db_percentage,
            'viewFragmentationThreshold[percentage]': settings.view_percentage,
            'parallelDBAndViewCompaction': str(settings.parallel).lower(),
            'magmaFragmentationPercentage': settings.magma_fragmentation_percentage
        }
        url = self._get_api_url(host=host, path='controller/setAutoCompaction')
        self.post(url=url, data=data)

    def get_auto_compaction_settings(self, host: str) -> dict:
        return self.get(url=self._get_api_url(host=host, path='settings/autoCompaction')).json()

    def get_bucket_stats(self, host: str, bucket: str) -> dict:
        url = self._get_api_url(host=host, path='pools/default/buckets/{}/stats'.format(bucket))
        return self.get(url=url).json()

    def get_scope_and_collection_items(self, host: str, bucket: str, scope: str,
                                       coll: str = None) -> dict:
        api = 'pools/default/stats/range/kv_collection_item_count?bucket={}&' \
            'scope={}&start=-1'.format(bucket, scope)
        if coll:
            api += '&collection={}'.format(coll)

        return self.get(url=self._get_api_url(host=host, path=api)).json()

    def get_dcp_replication_items(self, host: str, bucket: str) -> dict:
        api = 'pools/default/stats/range/kv_dcp_items_remaining?bucket={}&' \
            'connection_type=replication&aggregationFunction=sum'.format(bucket)
        return self.get(url=self._get_api_url(host=host, path=api)).json()

    def get_dcp_replication_items_v2(self, host: str, bucket: str) -> dict:
        api = 'pools/default/stats/range/kv_dcp_items_remaining?bucket={}' \
            '&connection_type=replication&nodesAggregation=sum'.format(bucket)
        return self.get(url=self._get_api_url(host=host, path=api)).json()

    def get_xdcr_stats(self, host: str, bucket: str) -> dict:
        api = f'pools/default/buckets/@xdcr-{bucket}/stats'
        return self.get(url=self._get_api_url(host=host, path=api)).json()

    def get_xdcr_changes_left_total(self, host: str, bucket: str) -> int:
        api = 'pools/default/stats/range/xdcr_changes_left_total?' \
            f'sourceBucketName={bucket}&pipelineType=Main'
        resp = self.get(url=self._get_api_url(host=host, path=api)).json()
        if resp["data"] != []:
            return int(resp["data"][0]["values"][-1][1])
        return -1

    def get_xdcr_docs_written_total(self, host: str, bucket: str) -> int:
        api = 'pools/default/stats/range/xdcr_docs_written_total?' \
            f'sourceBucketName={bucket}&pipelineType=Main'
        resp = self.get(url=self._get_api_url(host=host, path=api)).json()
        return int(resp["data"][0]["values"][-1][1])

    def xdcr_mobile_docs_filtered_total(self, host: str, bucket: str) -> int:
        api = 'pools/default/stats/range/xdcr_mobile_docs_filtered_total?' \
            f'sourceBucketName={bucket}&pipelineType=Main'
        resp = self.get(url=self._get_api_url(host=host, path=api)).json()
        return int(resp["data"][0]["values"][-1][1])

    def get_xdcr_completeness(self, host: str, bucket: str) -> int:
        api = 'pools/default/stats/range/xdcr_percent_completeness?' \
            f'sourceBucketName={bucket}&pipelineType=Main'
        resp = self.get(url=self._get_api_url(host=host, path=api)).json()
        return resp["data"]

    def get_xdcr_items(self, host: str, bucket: str) -> int:
        api = 'pools/default/stats/range/xdcr_docs_written_total?' \
            f'sourceBucketName={bucket}&pipelineType=Main'
        resp = self.get(url=self._get_api_url(host=host, path=api)).json()
        return resp["data"]

    def add_remote_cluster(self,
                           local_host: str,
                           remote_host: str,
                           name: str,
                           secure_type: str,
                           certificate: str):
        logger.info('Adding a remote cluster: {}'.format(remote_host))
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

        url = self._get_api_url(host=local_host, path='pools/default/remoteClusters')
        self.post(url=url, data=payload)

    def get_remote_clusters(self, host: str) -> list[dict]:
        logger.info('Getting remote clusters')
        url = self._get_api_url(host=host, path='pools/default/remoteClusters')
        return self.get(url=url).json()

    def create_replication(self, host: str, params: dict):
        logger.info('Starting replication with parameters {}'.format(params))
        url = self._get_api_url(host=host, path='controller/createReplication')
        self.post(url=url, data=params)

    def edit_replication(self, host: str, params: dict, replicationid: str):
        logger.info('Editing replication {} with parameters {}'.format(replicationid, params))
        url = self._get_api_url(host=host, path='settings/replications/{}'.format(replicationid))
        self.post(url=url, data=params)

    def delete_replication(self, host: str, replicationid: str):
        logger.info(f"Deleting replication {replicationid}")
        url = self._get_api_url(host=host, path=f'controller/cancelXDCR/{replicationid}')
        self.delete(url=url)

    def trigger_bucket_compaction(self, host: str, bucket: str):
        logger.info('Triggering bucket {} compaction'.format(bucket))
        url = self._get_api_url(host=host,
                                path='pools/default/buckets/{}/controller/compactBucket'.format(bucket))
        self.post(url=url)

    def trigger_index_compaction(self, host: str, bucket: str, ddoc: str):
        logger.info('Triggering ddoc {} compaction, bucket {}'.format(ddoc, bucket))
        api = 'pools/default/buckets/{}/ddocs/_design%2F{}/' \
            'controller/compactView'.format(bucket, ddoc)
        self.post(url=self._get_api_url(host=host, path=api))

    def create_ddoc(self, host: str, bucket: str, ddoc_name: str, ddoc: dict):
        logger.info('Creating new ddoc {}, bucket {}'.format(ddoc_name, bucket))
        data = json.dumps(ddoc)
        headers = {'Content-type': 'application/json'}
        url = self._get_api_url(host=host,
                                path='couchBase/{}/_design/{}'.format(bucket, ddoc_name))
        self.put(url=url, data=data, headers=headers)

    def query_view(self, host: str, bucket: str, ddoc_name: str,
                   view_name: str, params: dict):
        logger.info('Querying view: {}/_design/{}/_view/{}'.format(
            bucket, ddoc_name, view_name
        ))
        url = self._get_api_url(host=host,
                                path='couchBase/{}/_design/{}/_view/{}'.format(bucket, ddoc_name,
                                                                               view_name))
        self.get(url=url, params=params)

    def _get_version_raw(self, host: str) -> str:
        r = self.get(url=self._get_api_url(host=host, path="pools")).json()
        return r["implementationVersion"]

    def get_version(self, host: str) -> str:
        logger.info(f"Getting Couchbase Server version on server {host}")
        version = (
            self._get_version_raw(host)
            .replace("-rel-enterprise", "")
            .replace("-enterprise", "")
            .replace("-community", "")
            .replace("-columnar", "")
        )
        logger.info(f"Couchbase Server version: {version}")
        return version

    def supports_rbac(self, host: str) -> bool:
        """Return true if the cluster supports RBAC."""
        rbac_url = self._get_api_url(host=host, path="settings/rbac/roles")
        r = self.get(url=rbac_url)
        return r.status_code == requests.codes.ok

    def is_columnar(self, host: str) -> bool:
        logger.info("Checking if using a Couchbase Columnar build")
        return "columnar" in self._get_version_raw(host)

    def is_community(self, host: str) -> bool:
        logger.info('Getting Couchbase Server edition')
        return "community" in self._get_version_raw(host)

    def get_memcached_port(self, host: str) -> int:
        logger.info('Getting memcached port from {}'.format(host))
        r = self.get(url=self._get_api_url(host=host, path='nodes/self')).json()
        return r['ports']['direct']

    def get_otp_node_name(self, host: str) -> str:
        logger.info('Getting OTP node name from {}'.format(host))
        r = self.get(url=self._get_api_url(host=host, path='nodes/self')).json()
        return r['otpNode']

    def set_internal_settings(self, host: str, data: dict):
        logger.info('Updating internal settings: {}'.format(data))
        url = self._get_api_url(host=host, path='internalSettings')
        self.post(url=url, data=data)

    def set_xdcr_cluster_settings(self, host: str, data: dict):
        logger.info('Updating xdcr cluster settings: {}'.format(data))
        url = self._get_api_url(host=host, path='settings/replications')
        self.post(url=url, data=data)

    def enable_cross_clustering_versioning(self, host: str, bucket: str):
        logger.info(f"Enabling cross clustering versioning on node: {host}")
        api = self._get_api_url(host=host, path=f"pools/default/buckets/{bucket}")
        data = {
            'enableCrossClusterVersioning': 'true'
        }
        self.post(url=api, data=data)

    def disable_cross_clustering_versioning(self, host: str, bucket: str):
        logger.info(f"Enabling cross clustering versioning on node: {host}")
        api = self._get_api_url(host=host, path=f"pools/default/buckets/{bucket}")
        data = {
            'enableCrossClusterVersioning': 'false'
        }
        self.post(url=api, data=data)

    def run_diag_eval(self, host: str, cmd: str):
        url = self._get_api_url(host=host, path='diag/eval')
        self.post(url=url, data=cmd)

    def set_auto_failover(self, host: str, enabled: str, failover_timeouts: list[int],
                          disk_failover_timeout: int):
        logger.info('Setting auto-failover to: {}'.format(enabled))

        for timeout in failover_timeouts:
            data = {'enabled': enabled,
                    'timeout': timeout,
                    'failoverOnDataDiskIssues[enabled]': enabled,
                    'failoverOnDataDiskIssues[timePeriod]': disk_failover_timeout
                    }
            url = self._get_api_url(host=host, path='settings/autoFailover')
            r = self._post(url=url, data=data)
            if r.status_code == 200:
                return
            else:
                logger.warn('Auto-failover settings rejected: {}, Data: {}'.format(r.reason, data))

        raise Exception('Autofailover setting combinations rejected')

    def get_certificate(self, host: str) -> str:
        logger.info('Getting remote certificate')

        build = self.get_version(host)
        if create_build_tuple(build) < (7, 1, 0, 0) and not self.is_columnar(host):
            return self.get(url=self._get_api_url(host=host, path='pools/default/certificate')).text
        else:
            r = self.get(url=self._get_api_url(host=host, path='pools/default/trustedCAs'))
            certs = ''.join(cert['pem'] for cert in r.json())
            return certs

    def fail_over(self, host: str, node: str):
        logger.info('Failing over node: {}'.format(node))
        data = {'otpNode': self.get_otp_node_name(node)}
        url = self._get_api_url(host=host, path='controller/failOver')
        self.post(url=url, data=data)

    def graceful_fail_over(self, host: str, node: str):
        logger.info('Gracefully failing over node: {}'.format(node))
        data = {'otpNode': self.get_otp_node_name(node)}
        url = self._get_api_url(host=host, path='controller/startGracefulFailover')
        self.post(url=url, data=data)

    def add_back(self, host: str, node: str):
        logger.info('Adding node back: {}'.format(node))
        data = {'otpNode': self.get_otp_node_name(node)}
        url = self._get_api_url(host=host, path='controller/reAddNode')
        self.post(url=url, data=data)

    def set_delta_recovery_type(self, host: str, node: str):
        logger.info('Enabling delta recovery: {}'.format(node))
        data = {
            'otpNode': self.get_otp_node_name(node),
            'recoveryType': 'delta'  # alt: full
        }
        url = self._get_api_url(host=host, path='controller/setRecoveryType')
        self.post(url=url, data=data)

    def node_statuses(self, host: str) -> dict:
        data = self.get(url=self._get_api_url(host=host, path='nodeStatuses')).json()
        return {node: info['status'] for node, info in data.items()}

    def node_statuses_v2(self, host: str) -> dict:
        data = self.get(url=self._get_api_url(host=host, path='pools/default')).json()
        return {node['hostname']: node['status'] for node in data['nodes']}

    def get_node_stats(self, host: str, bucket: str) -> Iterator:
        api = 'pools/default/buckets/{}/nodes'.format(bucket)
        data = self.get(url=self._get_api_url(host=host, path=api)).json()
        for server in data['servers']:
            url = self._get_api_url(host=host, path=server['stats']['uri'][1:])
            node_data = self.get(url=url).json()
            yield node_data['hostname'], node_data['op']['samples']

    def get_vbmap(self, host: str, bucket: str) -> dict:
        logger.info('Reading vbucket map: {}/{}'.format(host, bucket))
        data = self.get_bucket_info(host, bucket)

        return data['vBucketServerMap']['vBucketMap']

    def get_server_list(self, host: str, bucket: str) -> list[str]:
        data = self.get_bucket_info(host, bucket)

        return [server.split(':')[0]
                for server in data['vBucketServerMap']['serverList']]

    def get_bucket_info(self, host: str, bucket: str) -> dict:
        url = self._get_api_url(host=host, path='pools/default/buckets/{}'.format(bucket))
        return self.get(url=url).json()

    def set_bucket_history(self, host: str, bucket: str, history_bytes: int = 0,
                           history_seconds: int = 0) -> requests.Response:
        data = {
            'historyRetentionBytes': history_bytes,
            'historyRetentionSeconds': history_seconds
        }
        url = self._get_api_url(host=host, path='pools/default/buckets/{}'.format(bucket))
        return self.post(url=url, data=data)

    def update_bucket_storage_backend(self, host: str, bucket: str, mode: str):
        data = {
            'storageBackend': mode
        }
        url = self._get_api_url(host=host, path='pools/default/buckets/{}'.format(bucket))
        response = self.post(url=url, data=data)
        response.raise_for_status()

    def get_bucket_storage_backend_info(self, host: str, bucket: str) -> dict:
        bucket_info = self.get_bucket_info(host, bucket)
        storage_backend_info = {
            'storageBackend': bucket_info['storageBackend'],
            'nodes': {node['otpNode']: node.get('storageBackend') for node in bucket_info['nodes']}
        }
        return storage_backend_info

    def exec_n1ql_statement(self, host: str, statement: str, query_context: str = None) -> dict:
        data = {
            'statement': statement,
        }
        if query_context:
            data['query_context'] = query_context

        url = self._get_api_url(host=host, path='query/service', plain_port=QUERY_PORT,
                                ssl_port=QUERY_PORT_SSL)
        response = self.post(url=url, data=data)
        resp_data = response.json()
        if errors := resp_data.get("errors"):
            logger.warn(f"Query failed: {errors}")
        return resp_data

    def set_serverless_throttle(self, node, values):
        api = self._get_api_url(host=node, path='settings/serverless')
        for key in values:
            limit = values[key]
            if limit != 0:
                data = {key: limit}
                logger.info('Setting throttle limit: {}'.format(data))
                self.post(url=api, data=data)

    def reset_serverless_throttle(self, node):
        api = self._get_api_url(host=node, path='settings/serverless')
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
        url = self._get_api_url(host=host, path='admin/stats', plain_port=QUERY_PORT,
                                ssl_port=QUERY_PORT_SSL)
        response = self.get(url=url)
        return response.json()

    def delete_fts_index(self, host: str, index: str):
        logger.info('Deleting FTS index: {}'.format(index))
        url = self._get_api_url(host=host, path='api/index/{}'.format(index),
                                plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL)
        self.delete(url=url)

    def create_fts_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new FTS index: {}'.format(index))
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)
        url = self._get_api_url(host=host, path='api/index/{}'.format(index),
                                plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL)
        self.put(url=url, data=data, headers=headers)

    def get_fts_doc_count(self, host: str, index: str, bucket: str) -> int:
        url = self._get_api_url(host=host, path='api/index/{}/count'.format(index),
                                plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL)
        response = self.get(url=url).json()
        return response['count']

    def get_fts_stats(self, host: str) -> dict:
        url = self._get_api_url(host=host, path='api/nsstats',
                                plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL)
        response = self.get(url=url)
        return response.json()

    def get_elastic_stats(self, host: str) -> dict:
        url = self._get_api_url(host=host, path='_stats', plain_port=ELASTICSEARCH_REST_PORT,
                                ssl_port=ELASTICSEARCH_REST_PORT_SSL)
        response = self.get(url=url)
        return response.json()

    def delete_elastic_index(self, host: str, index: str):
        logger.info('Deleting Elasticsearch index: {}'.format(index))
        url = self._get_api_url(host=host, path=index, plain_port=ELASTICSEARCH_REST_PORT,
                                ssl_port=ELASTICSEARCH_REST_PORT_SSL)
        self.delete(url=url)

    def create_elastic_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new Elasticsearch index: {}'.format(index))
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)

        url = self._get_api_url(host=host, path=index, plain_port=ELASTICSEARCH_REST_PORT,
                                ssl_port=ELASTICSEARCH_REST_PORT_SSL)
        self.put(url=url, data=data, headers=headers)

    def get_elastic_doc_count(self, host: str, index: str) -> int:
        url = self._get_api_url(host=host, path='{}/_count'.format(index),
                                plain_port=ELASTICSEARCH_REST_PORT,
                                ssl_port=ELASTICSEARCH_REST_PORT_SSL)
        response = self.get(url=url).json()
        return response['count']

    def get_index_status(self, host: str) -> dict:
        url = self._get_api_url(host=host, path='getIndexStatus', plain_port=INDEXING_PORT,
                                ssl_port=INDEXING_PORT_SSL)
        response = self.get(url=url)
        return response.json()

    def get_index_stats(self, hosts: list[str]) -> dict:
        data = {}
        for host in hosts:
            url = self._get_api_url(host=host, path='stats', plain_port=INDEXING_PORT,
                                    ssl_port=INDEXING_PORT_SSL)
            host_data = self.get(url=url)
            data.update(host_data.json())
        return data

    def get_index_num_connections(self, host: str) -> int:
        url = self._get_api_url(host=host, path='stats', plain_port=INDEXING_PORT,
                                ssl_port=INDEXING_PORT_SSL)
        response = self.get(url=url).json()
        return response['num_connections']

    def get_index_storage_stats(self, host: str) -> requests.Response:
        url = self._get_api_url(host=host, path='stats/storage', plain_port=INDEXING_PORT,
                                ssl_port=INDEXING_PORT_SSL)
        return self.get(url=url)

    def get_index_storage_stats_mm(self, host: str) -> str:
        url = self._get_api_url(host=host, path='stats/storage/mm', plain_port=INDEXING_PORT,
                                ssl_port=INDEXING_PORT_SSL)
        return self.get(url=url).text

    def get_audit_settings(self, host: str) -> dict:
        logger.info('Getting current audit settings')
        return self.get(url=self._get_api_url(host=host, path='settings/audit')).json()

    def enable_audit(self, host: str, disabled: list[str]):
        logger.info('Enabling audit')
        data = {
            'auditdEnabled': 'true',
        }
        if disabled:
            data['disabled'] = ','.join(disabled)

        url = self._get_api_url(host=host, path='settings/audit')
        self.post(url=url, data=data)

    def get_rbac_roles(self, host: str) -> list[dict]:
        logger.info('Getting the existing RBAC roles')
        return self.get(url=self._get_api_url(host=host, path='settings/rbac/roles')).json()

    def delete_rbac_user(self, host: str, bucket: str):
        logger.info('Deleting an RBAC user: {}'.format(bucket))
        for domain in 'local', 'builtin':
            api = 'settings/rbac/users/{}/{}'.format(domain, bucket)
            r = self._delete(url=self._get_api_url(host=host, path=api))
            if r.status_code == 200:
                break

    def add_rbac_user(self, host: str, user: str, password: str, roles: list[str]):
        logger.info('Adding an RBAC user: {}, roles: {}'.format(user, roles))
        data = {
            'password': password,
            'roles': ','.join(roles),
        }

        for domain in 'local', 'builtin':
            url = self._get_api_url(host=host,
                                    path='settings/rbac/users/{}/{}'.format(domain, user))
            r = self._put(url=url, data=data)
            if r.status_code == 200:
                break

    def get_analytics_cluster_info(self, host: str) -> dict:
        url = self._get_api_url(host=host, path='analytics/cluster', plain_port=ANALYTICS_PORT,
                                ssl_port=ANALYTICS_PORT_SSL)
        return self.get(url=url).json()

    def analytics_node_active(self, host: str) -> bool:
        logger.info('Checking if analytics node is active: {}'.format(host))
        cluster_info = self.get_analytics_cluster_info(host)
        return cluster_info["state"] == "ACTIVE"

    def exec_analytics_statement(self, analytics_node: str, statement: str) -> requests.Response:
        data = {'statement': statement}
        url = self._get_api_url(host=analytics_node, path='analytics/service',
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)
        return self.post(url=url, data=data)

    def exec_analytics_statement_curl(self, analytics_node: str, statement: str) -> str:
        url = self._get_api_url(host=analytics_node, path='analytics/service',
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)
        data = {'statement': statement}
        data_json = json.dumps(data)
        cmd = (
            f"curl -v -k -u {self.rest_username}:{self.rest_password} "
            f"-H \"Content-Type: application/json\" -d '{data_json}' {url}"
        )
        return local(cmd, capture=True)

    def get_analytics_stats(self, analytics_node: str) -> dict:
        url = self._get_api_url(host=analytics_node, path='analytics/node/stats',
                                plain_port=ANALYTICS_ADMIN_PORT, ssl_port=ANALYTICS_ADMIN_PORT_SSL)
        return self.get(url=url).json()

    def get_pending_mutations(self, analytics_node: str) -> dict:
        url = self._get_api_url(host=analytics_node, path='analytics/node/agg/stats/remaining',
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)
        return self.get(url=url).json()

    def get_pending_mutations_v2(self, analytics_node: str) -> dict:
        url = self._get_api_url(host=analytics_node, path='analytics/status/ingestion',
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)
        return self.get(url=url).json()

    def get_ingestion_v2(self, analytics_node: str) -> dict:
        url = self._get_api_url(host=analytics_node, path='analytics/status/ingestion/v2',
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)
        return self.get(url=url).json()

    def set_analytics_config_settings(self, analytics_node: str, level: Literal["service", "node"],
                                      settings: dict[str, str]):
        logger.info(f'Updating analytics {level}-level parameters on {analytics_node}:\n'
                    f'{pretty_dict(settings)}')
        api = self._get_api_url(
            host=analytics_node,
            path=f"analytics/config/{level}",
            plain_port=ANALYTICS_PORT,
            ssl_port=ANALYTICS_PORT_SSL,
        )
        r = self.put(url=api, data=settings)
        if r.status_code not in (200, 202):
            logger.warning(f'Unexpected request status code {r.status_code}')

    def restart_analytics_cluster(self, analytics_node: str):
        logger.info('Restarting analytics cluster')
        api = self._get_api_url(
            host=analytics_node,
            path="analytics/cluster/restart",
            plain_port=ANALYTICS_PORT,
            ssl_port=ANALYTICS_PORT_SSL,
        )
        r = self.post(url=api)
        if r.status_code not in (200, 202,):
            logger.warning('Unexpected request status code {}'.
                           format(r.status_code))

    def validate_analytics_settings(self, analytics_node: str, level: Literal["service", "node"],
                                    settings: dict[str, str]):
        logger.info(f'Verifying analytics {level}-level parameters on {analytics_node}:\n'
                    f'{pretty_dict(settings)}')
        api = self._get_api_url(
            host=analytics_node,
            path=f"analytics/config/{level}",
            plain_port=ANALYTICS_PORT,
            ssl_port=ANALYTICS_PORT_SSL,
        )
        response = self.get(url=api).json()
        for setting, value in settings.items():
            assert (str(response[setting]) == str(value))

    def get_analytics_config(self, analytics_node: str, level: Literal["service", "node"]) -> dict:
        logger.info(f'Grabbing analytics {level} config from {analytics_node}')
        api = self._get_api_url(
            host=analytics_node,
            path=f"analytics/config/{level}",
            plain_port=ANALYTICS_PORT,
            ssl_port=ANALYTICS_PORT_SSL,
        )
        config = self.get(url=api).json()
        return config

    def get_cbas_incoming_records_count(
        self,
        host: str,
        metric: str = "cbas_incoming_records_total",
    ) -> dict:
        api = self._get_api_url(
            host=host,
            path=f"pools/default/stats/range/{metric}?nodesAggregation=sum",
        )
        return self.get(url=api).json()

    def pause_analytics_cluster(self, analytics_node: str):
        url = self._get_api_url(host=analytics_node, path='analytics/cluster/pause',
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)
        self.post(url=url)

    def get_analytics_pause_status(self, analytics_node: str) -> dict:
        url = self._get_api_url(host=analytics_node, path='analytics/cluster/pause/status',
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)
        return self.get(url=url).json()

    def get_analytics_link_info(self, analytics_node: str, link_name: str,
                                link_scope: str = 'Default') -> dict:
        path = 'analytics/link/{}/{}'.format(link_scope, link_name)
        url = self._get_api_url(host=analytics_node, path=path,
                                plain_port=ANALYTICS_PORT, ssl_port=ANALYTICS_PORT_SSL)

        resp = self.get(url=url)

        # The Analytics Links API returns a list, even though there is only one link returned
        # So we just return the single list element
        return resp.json()[0]

    def create_remote_link(
        self,
        analytics_node: str,
        data_node: str,
        link_name: str,
        data_node_username: Optional[str] = None,
        data_node_password: Optional[str] = None,
    ):
        logger.info(
            f"Creating analytics remote link '{link_name}' "
            f"between {analytics_node} (analytics) and {data_node} (data)"
        )

        url = self._get_api_url(
            host=analytics_node,
            path="analytics/link",
            plain_port=ANALYTICS_PORT,
            ssl_port=ANALYTICS_PORT_SSL,
        )

        default_auth = self._set_auth()

        params = {
            "dataverse": "Default",
            "name": link_name,
            "type": "couchbase",
            "hostname": f"{data_node}:{REST_PORT_SSL if self.use_tls else REST_PORT}",
            "username": data_node_username or default_auth[0],
            "password": data_node_password or default_auth[1],
            "encryption": "none",
        }

        if url.startswith("https"):
            with open("root.pem", "r") as f:
                params |= {
                    "encryption": "full",
                    "certificate": f.read(),
                }

        resp = self.post(url=url, data=params)
        resp.raise_for_status()

    def deploy_function(self, node: str, func: dict, name: str):
        logger.info('Deploying function on node {}: {}'.format(node, pretty_dict(func)))
        url = self._get_api_url(host=node, path='api/v1/functions/{}'.format(name),
                                plain_port=EVENTING_PORT, ssl_port=EVENTING_PORT_SSL)
        self.post(url=url, data=json.dumps(func))

    def get_functions(self, node: str):
        logger.info('Getting all functions')
        url = self._get_api_url(host=node, path='api/v1/functions', plain_port=EVENTING_PORT,
                                ssl_port=EVENTING_PORT_SSL)
        return self.get(url=url).json()

    def change_function_settings(self, node: str, func: dict, name: str, func_scope: dict):
        logger.info('Changing function settings on node {}: {}'.format(node, pretty_dict(func)))
        logger.info('function scope for {} is {}'.format(name, pretty_dict(func_scope)))
        api = 'api/v1/functions/{}/settings'.format(name)
        if func_scope:
            api += '?bucket={}&scope={}'.format(func_scope['bucket'], func_scope['scope'])
        logger.info("API path {}".format(api))
        url = self._get_api_url(host=node, path=api, plain_port=EVENTING_PORT,
                                ssl_port=EVENTING_PORT_SSL)
        self.post(url=url, data=func)

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
        url = self._get_api_url(host=node, path='api/v1/status',
                                plain_port=EVENTING_PORT, ssl_port=EVENTING_PORT_SSL)
        data = self.get(url=url).json()
        apps = []
        for app in data["apps"]:
            if app["composite_status"] == status:
                apps.append(app["name"])
        return apps

    def get_eventing_stats(self, node: str, full_stats: bool = False) -> dict:
        logger.info('get eventing stats on node {}'.format(node))
        api = 'api/v1/stats'
        if full_stats:
            api += "?type=full"

        url = self._get_api_url(host=node, path=api, plain_port=EVENTING_PORT,
                                ssl_port=EVENTING_PORT_SSL)
        return self.get(url=url).json()

    def get_active_nodes_by_role(self, master_node: str, role: str) -> list[str]:
        active_nodes = self.node_statuses(master_node)
        active_nodes_by_role = []

        if use_private_ips := self.cluster_spec.using_private_cluster_ips:
            ip_map = self.cluster_spec.servers_public_to_private_ip

        for node in self.cluster_spec.servers_by_role(role):
            lookup = ip_map[node] if use_private_ips else node
            lookup = lookup + ':{}'.format(REST_PORT)
            if lookup in active_nodes:
                active_nodes_by_role.append(node)
        return active_nodes_by_role

    def fts_set_node_level_parameters(self, parameter: dict, host: str):
        logger.info(f"Adding in the parameter {parameter}")
        api = self._get_api_url(
            host=host, path="api/managerOptions", plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL
        )
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(parameter, ensure_ascii=False)
        self.put(url=api, data=data, headers=headers)

    def upload_cluster_certificate(self, node: str):
        logger.info("Uploading cluster certificate to {}".format(node))
        data = open('./certificates/inbox/ca.pem', 'rb').read()
        url = self._get_api_url(host=node, path='controller/uploadClusterCA')
        self.post(url=url, data=data)

    def reload_cluster_certificate(self, node: str):
        logger.info(f"Reloading certificate on {node}")
        api = self._get_api_url(host=node, path="node/controller/reloadCertificate")
        self.post(url=api)

    def enable_certificate_auth(self, node: str):
        logger.info(f"Enabling certificate-based client auth on {node}")
        api = self._get_api_url(host=node, path="settings/clientCertAuth")
        data = {
            'state': 'enable',
            'prefixes': [
                {'path': 'subject.cn', 'prefix': '', 'delimiter': ''}
            ]
        }
        self.post(url=api, json=data)

    def get_minimum_tls_version(self, node: str) -> str:
        # Because tlsv1.3 cannot be the global tls min version (unlike previous tls versions),
        # we must add additional checks to ensure that version is correctly identified
        logger.info(f"Getting TLS version of {node}")
        api = self._get_api_url(host=node, path="settings/security")
        global_tls_min_version = self.get(url=api).json()['tlsMinVersion']
        try:
            api = self._get_api_url(host=node, path="settings/security/data")
            local_tls_min_version = self.get(url=api).json()['tlsMinVersion']
            if local_tls_min_version == global_tls_min_version:
                return global_tls_min_version
            else:
                return local_tls_min_version
        except KeyError:
            return global_tls_min_version

    def set_num_threads(self, node: str, thread_type: str, thread: int):
        logger.info(f"Setting {thread_type} to {thread}")
        api = self._get_api_url(host=node, path="pools/default/settings/memcached/global")
        data = {
            thread_type: thread
        }
        self.post(url=api, data=data)

    def get_supported_ciphers(self, node: str, service: str) -> list[str]:
        logger.info(f"Getting the supported ciphers for the {service} service")
        api = self._get_api_url(host=node, path=f"settings/security/{service}")
        return self.get(url=api).json()['supportedCipherSuites']

    def get_cipher_suite(self, node: str) -> list[str]:
        logger.info(f"Getting cipher suites of {node}")
        api = self._get_api_url(host=node, path="settings/security")
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
        logger.info(f"Setting cipher suite of {cipher_list}")
        try:
            api = self._get_api_url(host=node, path="settings/security")
            data = {
                'cipherSuites': json.dumps(cipher_list)
            }
            self.post(url=api, data=data)
            logger.info("The entire cipher suite has been set globally")
        except Exception:
            logger.info("The cipher suite cannot be set globally, will be set for each service")
            service_list = [
                "data",
                "fullTextSearch",
                "index",
                "query",
                "eventing",
                "analytics",
                "backup",
                "clusterManager",
            ]
            for service in service_list:
                api = self._get_api_url(host=node, path=f"settings/security/{service}")
                try:
                    data = {"cipherSuites": json.dumps(cipher_list)}
                    self.post(url=api, data=data)
                    logger.info(f"The entire cipher suite has been set for the {service} service")
                except Exception:
                    valid_cipher_list = []
                    supported_cipher_list = self.get_supported_ciphers(node, service)
                    for cipher in cipher_list:
                        if cipher in supported_cipher_list:
                            valid_cipher_list.append(cipher)
                    data = {"cipherSuites": json.dumps(valid_cipher_list)}
                    self.post(url=api, data=data)
                    logger.info(
                        f"The following cipher suite: {valid_cipher_list} "
                        f"has been set for the {service} service"
                    )

    def set_minimum_tls_version(self, node: str, tls_version: str):
        logger.info(f'Setting minimum TLS version of {tls_version}')
        # ClusterManager does not support tlsv1.3, so tlsv1.3 cannot be set as the global
        # minimum tls version. As a result, for all the services that support tlsv1.3
        # (Data, Search, Index, Query, Eventing, Analytics, Backup),
        # the minimum tls version must be set separately.
        url_prefix = f'http://{node}:{REST_PORT}/settings/security'
        if tls_version != 'tlsv1.3':
            api = url_prefix
            data = {'tlsMinVersion': tls_version}
            self.post(url=api, data=data)
        else:
            build = self.get_version(node)
            if create_build_tuple(build) < (7, 1, 0, 0) and not self.is_columnar(node):
                logger.info("TLSv1.3 is not supported by this version of couchbase server")
                logger.info("Reverting to TLSv1.2")
                api = url_prefix
                data = {'tlsMinVersion': 'tlsv1.2'}
                self.post(url=api, data=data)
            else:
                data = {'tlsMinVersion': tls_version}

                for suffix in [
                    'data', 'fullTextSearch', 'index', 'query', 'eventing', 'analytics', 'backup'
                ]:
                    api = f'{url_prefix}/{suffix}'
                    self.post(url=api, data=data)

    def create_scope(self, host, bucket, scope):
        logger.info("Creating scope {}:{}".format(bucket, scope))
        data = {
            'name': scope
        }
        url = self._get_api_url(host=host, path='pools/default/buckets/{}/scopes'.format(bucket))
        self.post(url=url, data=data)

    def delete_scope(self, host, bucket, scope):
        logger.info("Deleting scope {}:{}".format(bucket, scope))
        path = 'pools/default/buckets/{}/scopes/{}'.format(bucket, scope)
        self.delete(url=self._get_api_url(host=host, path=path))

    def create_collection(self, host: str, bucket: str, scope: str, collection: str,
                          history: Optional[bool] = None) -> requests.Response:
        logger.info("Creating collection {}:{}.{} (history={})"
                    .format(bucket, scope, collection, history))
        data = {
            'name': collection,
        }
        if history is not None:
            data['history'] = str(history).lower()

        path = 'pools/default/buckets/{}/scopes/{}/collections'.format(bucket, scope)
        url = self._get_api_url(host=host, path=path)
        self.post(url=url, data=data)

    def delete_collection(self, host, bucket, scope, collection):
        logger.info("Dropping collection {}:{}.{}".format(bucket, scope, collection))
        api = 'pools/default/buckets/{}/scopes/{}/collections/{}'.format(bucket, scope, collection)
        self.delete(url=self._get_api_url(host=host, path=api))

    def set_collection_map(self, host, bucket, collection_map):
        logger.info("Setting collection map on {} via bulk api".format(bucket))
        url = self._get_api_url(host=host, path='pools/default/buckets/{}/scopes'.format(bucket))
        self.put(url=url, data=json.dumps(collection_map))

    def create_server_group(self, host: str, server_group: str) -> requests.Response:
        logger.info(f"Creating Server Group {server_group}")
        api = self._get_api_url(host=host, path="pools/default/serverGroups")
        data = {
            'name': server_group
        }
        return self.post(url=api, data=data)

    def get_server_group_info(self, host: str) -> dict:
        api = self._get_api_url(host=host, path="pools/default/serverGroups")
        return self.get(url=api).json()

    def change_group_membership(self, host: str, uri: str, node_json: dict):
        api = self._get_api_url(host=host, path=uri)
        self.put(url=api, data=json.dumps(node_json))

    def delete_server_group(self, host: str, uri: str):
        api = self._get_api_url(host=host, path=uri)
        self.delete(url=api)

    def indexes_per_node(self, host: str) -> int:
        api = self._get_api_url(
            host=host, path="stats", plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL
        )
        return self.get(url=api).json()['num_indexes']

    def indexes_instances_per_node(self, host: str) -> int:
        api = self._get_api_url(
            host=host, path="stats", plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL
        )
        return self.get(url=api).json()['num_storage_instances']

    def backup_index(self, host: str, bucket: str) -> dict:
        logger.info(f"Backing up index metadata on host {host} bucket {bucket}")
        api = self._get_api_url(
            host=host,
            path=f"api/v1/bucket/{bucket}/backup?keyspace={bucket}",
            plain_port=INDEXING_PORT,
            ssl_port=INDEXING_PORT_SSL,
        )
        return self.get(url=api).json()["result"]

    def restore_index(
        self, host: str, bucket: str, metadata: dict, from_keyspace: str, to_keyspace: str
    ) -> requests.Response:
        logger.info(f"Restoring indexes on host {host} bucket {bucket}")
        api = self._get_api_url(
            host=host,
            path=f"api/v1/bucket/{bucket}/backup?keyspace={from_keyspace}:{to_keyspace}",
            plain_port=INDEXING_PORT,
            ssl_port=INDEXING_PORT_SSL,
        )
        return self.post(url=api, data=json.dumps(metadata))

    def set_plasma_diag(self, host: str, cmd: str, arg: str):
        logger.info(f"setting plasma diag to {cmd} for {arg}")
        api = self._get_api_url(
            host=host, path="plasmaDiag", plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL
        )
        data = {
            'Cmd': cmd, "Args": [arg]
        }
        r = self.post(url=api, data=json.dumps(data))
        if r.status_code not in (200, 202):
            logger.warning(f"Unexpected request status code {r.status_code}")

    def get_plasma_dbs(self, host: str) -> list[str]:
        api = self._get_api_url(
            host=host, path="plasmaDiag", plain_port=INDEXING_PORT, ssl_port=INDEXING_PORT_SSL
        )
        data = {
            'Cmd': 'listDBs'
        }
        r = self.post(url=api, data=json.dumps(data))
        if r.status_code not in (200, 202):
            logger.warning(f"Unexpected request status code {r.status_code}")
        ids = [line.split()[-1] for line in r.text.split("\n")[1:-1]]
        logger.info(f"Plasma db list {ids}")
        return ids

    def set_analytics_replica(self, host: str, analytics_replica: int):
        logger.info(f"Setting analytics replica to: {analytics_replica}")
        api = self._get_api_url(host=host, path="settings/analytics")
        data = {
            'numReplicas': analytics_replica,
        }
        self.post(url=api, data=data)

    def get_analytics_settings(self, host: str) -> dict:
        api = self._get_api_url(host=host, path="settings/analytics")
        return self.get(url=api).json()

    def get_analytics_prometheus_stats(self, host: str) -> dict[str, float]:
        url = self._get_api_url(
            host=host,
            path="_prometheusMetrics",
            plain_port=ANALYTICS_PORT,
            ssl_port=ANALYTICS_PORT_SSL,
        )
        resp = self.get(url=url)
        stats = {
            s[0]: float(s[1])
            for line in resp.text.splitlines()
            if not line.startswith("#") and (s := line.split())
        }
        return stats

    def get_sgversion(self, host: str) -> str:
        logger.info('Getting SG Server version on server: {}'.format(host))
        url = self._get_api_url(host=host, path='', plain_port=SGW_ADMIN_PORT,
                                ssl_port=SGW_ADMIN_PORT)
        r = self.get(url=url).json()
        return r['version'] \
            .replace('Couchbase Sync Gateway/', '') \
            .replace(') EE', '').replace('(', '-').split(';')[0]

    def get_sg_stats(self, host: str) -> dict:
        if self.cluster_spec.capella_infrastructure:
            url = 'https://{}:{}/metrics'.format(host, SGW_APPSERVICE_METRICS_PORT)
        else:
            url = self._get_api_url(host=host, path='_expvar', plain_port=SGW_ADMIN_PORT,
                                    ssl_port=SGW_ADMIN_PORT)
        response = self.get(url=url)
        if self.cluster_spec.capella_infrastructure:
            return response.text
        return response.json()

    def start_sg_replication(self, host, payload):
        logger.info('Start sg replication. Payload: {}'.format(payload))
        url = self._get_api_url(host=host, path='_replicate', plain_port=SGW_ADMIN_PORT,
                                ssl_port=SGW_ADMIN_PORT)
        logger.info('api: {}'.format(url))
        self.post(url=url, data=json.dumps(payload))

    def start_sg_replication2(self, host, payload):
        logger.info('Start sg replication. Payload: {}'.format(payload))
        url = self._get_api_url(host=host, path='db-1/_replication/', plain_port=SGW_ADMIN_PORT,
                                ssl_port=SGW_ADMIN_PORT)
        logger.info('api: {}'.format(url))
        self.post(url=url, data=json.dumps(payload))

    def get_sgreplicate_stats(self, host: str, version: int) -> dict:
        if version == 1:
            url = self._get_api_url(host=host, path='_active_tasks', plain_port=SGW_ADMIN_PORT,
                                    ssl_port=SGW_ADMIN_PORT)
        elif version == 2:
            url = self._get_api_url(host=host, path='db-1/_replicationStatus',
                                    plain_port=SGW_ADMIN_PORT, ssl_port=SGW_ADMIN_PORT)
        response = self.get(url=url)
        return response.json()

    def get_expvar_stats(self, host: str) -> dict:
        try:
            if self.cluster_spec.capella_infrastructure:
                url = 'https://{}:{}/metrics'.format(host, SGW_APPSERVICE_METRICS_PORT)
            else:
                url = self._get_api_url(host=host, path='_expvar', plain_port=SGW_ADMIN_PORT,
                                        ssl_port=SGW_ADMIN_PORT)
            response = self.get(url=url)
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
            password="guest",
            collections=None):
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
        if collections:
            data['collections'] = collections

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
            password="guest",
            collections=None):
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
        if collections:
            data['collections'] = collections

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
            password="guest",
            collections=None):
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
        if collections:
            data['collections'] = collections

        logger.info("The sgw host is: {}".format(sgw_host))
        logger.info("The url is: {}".format(api))
        logger.info("The json data is: {}".format(data))
        self.cblite_post(url=api, json=data, headers={'content-type': 'application/json'})
        logger.info("Successfully posted the current sgw")

    def get_cblite_info(self, cblite_host, cblite_port, cblite_db):
        api = 'http://{}:{}/{}'.format(cblite_host, cblite_port, cblite_db)
        return self.cblite_get(url=api).json()

    def sgw_create_db(self, db_name: str, db_config: dict, host: str):
        logger.info(f'Creating database: {db_name} using {host}')
        api = self._get_api_url(
            host=host,
            path=f"{db_name}/",
            plain_port=SGW_ADMIN_PORT,
            ssl_port=SGW_ADMIN_PORT,
        )
        resp = self.put(url=api, data=db_config)
        resp.raise_for_status()

    def sgw_update_sync_function(self, host: str, keyspace: str, new_sync: str):
        logger.info("Updating sync function")
        api = self._get_api_url(
            host=host,
            path=f"{keyspace}/_config/sync",
            plain_port=SGW_ADMIN_PORT,
            ssl_port=SGW_ADMIN_PORT,
        )
        resp = self.put(url=api, data=new_sync)
        resp.raise_for_status()

    def sgw_set_db_offline(self, host: str, db: str):
        logger.info(f"Setting database {db} offline")
        api = self._get_api_url(
            host=host,
            path=f"{db}/_offline",
            plain_port=SGW_ADMIN_PORT,
            ssl_port=SGW_ADMIN_PORT,
        )
        resp = self.post(url=api)
        resp.raise_for_status()

    def sgw_set_db_online(self, host: str, db: str):
        logger.info(f"Setting database {db} online")
        api = self._get_api_url(
            host=host,
            path=f"{db}/_online",
            plain_port=SGW_ADMIN_PORT,
            ssl_port=SGW_ADMIN_PORT,
        )
        resp = self.post(url=api)
        resp.raise_for_status()

    def sgw_start_resync(self, host: str, db: str):
        logger.info(f"Starting resync for {db} on {host}")
        api = self._get_api_url(
            host=host,
            path=f"{db}/_resync",
            plain_port=SGW_ADMIN_PORT,
            ssl_port=SGW_ADMIN_PORT,
        )
        data = {"data": "start"}
        resp = self.post(url=api, json=data)
        resp.raise_for_status()

    def sgw_get_resync_status(self, host: str, db: str) -> dict:
        api = self._get_api_url(
            host=host,
            path=f"{db}/_resync",
            plain_port=SGW_ADMIN_PORT,
            ssl_port=SGW_ADMIN_PORT,
        )
        resp = self.get(url=api)
        return resp.json()

    def get_kv_ops(self, host):
        url = self._get_api_url(host=host, path='pools/default/stats/range/kv_ops?start=-1')
        resp = self.get(url=url)
        return resp.json()

    def get_metering_stats(self, host, apply_functions=[]) -> dict:
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

        url = self._get_api_url(host=host, path='pools/default/stats/range')
        resp = self.post(url=url, json=data)
        return resp.json()

    def get_rebalance_report(self, host: str) -> dict:
        resp = self.get(url=self._get_api_url(host=host, path='logs/rebalanceReport'))
        return resp.json()

    def fts_search_query(self, host: str, indexName: str,  data: dict) -> dict:
        url = self._get_api_url(host=host, path=f"api/index/{indexName}/query",
                                plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL)
        resp = self.post(url=url, data= json.dumps(data))
        return resp.json()

    def is_persistence_active(self, host: str) -> str:
        url = self._get_api_url(host=host, path='isPersistanceActive', plain_port=INDEXING_PORT,
                                ssl_port=INDEXING_PORT_SSL)
        resp = self.get(url=url)
        return resp.text

    def get_orchestrator_node(self, host: str) -> str:
        """Return the address to the ns-server selected orchestrator node.

        If the call fails (Unauthorised, unknown pool), return host.
        """
        resp = self.get(url=self._get_api_url(host=host, path='pools/default/terseClusterInfo'))
        try:
            return resp.json().get('orchestrator').split('@')[-1]
        except Exception:
            return host

    def get_random_local_key(self, host: str, bucket: str,
                             scope: str = '_default', collection: str = '_default',
                             keys_count: int = 1) -> list[str]:
        """Get a list of random key(s) local to the vBuckets in this node."""
        keys = []
        path = 'pools/default/buckets/{}/scopes/{}/collections/' \
            '{}/localRandomKey'.format(bucket,scope, collection)
        url = self._get_api_url(host=host,path=path)
        for _ in range(0, keys_count):
            resp = self.get(url=url)
            keys.append(resp.json().get('key'))
        return keys


class KubernetesRestHelper(DefaultRestHelper):
    SERVICES_PERFRUNNER_TO_CAO = {
        "kv": "data",
        "cbas": "analytics",
        "n1ql": "query",
        "index": "index",
        "fts": "search",
        "eventing": "eventing",
    }

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.remote = RemoteHelper(cluster_spec)
        self.ip_table, self.port_translation = self.remote.get_ip_port_mapping()

    def translate_host_and_port(self, host, port):
        trans_host = self.ip_table.get(host)
        trans_port = self.port_translation.get(trans_host).get(str(port))
        return trans_host, trans_port

    def _get_api_url(self, host: str, path: str,
                     plain_port: str = REST_PORT, ssl_port: str = REST_PORT_SSL) -> str:
        if self.use_tls:
            host, ssl_port = self.translate_host_and_port(host, ssl_port)
        else:
            host, plain_port = self.translate_host_and_port(host, plain_port)

        return super()._get_api_url(host, path, plain_port, ssl_port)

    # indexer endpoints not yet exposed by operator
    def get_index_status(self, host: str) -> dict:
        return {'status': [{'status': 'Ready'}]}

    # indexer endpoints not yet exposed by operator
    def get_gsi_stats(self, host: str) -> dict:
        return {'num_docs_queued': 0, 'num_docs_pending': 0}

    def get_active_nodes_by_role(self, master_node: str, role: str) -> list[str]:
        return self.remote.get_nodes_with_service(self.SERVICES_PERFRUNNER_TO_CAO.get(role, role))


class CapellaRestBase(DefaultRestHelper):

    def __init__(self, cluster_spec, test_config):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.base_url = self.cluster_spec.controlplane_settings["public_api_url"]
        self.tenant_id = self.cluster_spec.controlplane_settings["org"]
        self.project_id = self.cluster_spec.controlplane_settings["project"]

        self.cluster_ids = self.cluster_spec.capella_cluster_ids

        access, secret = None, None
        if api_keys := ControlPlaneManager.get_api_keys():
            access, secret = api_keys

        self.dedicated_client = CapellaAPIDedicated(
            self.base_url, secret, access, self._cbc_user, self._cbc_pwd, self._cbc_token
        )

    def _set_auth(self, **kwargs) -> tuple[str, str]:
        hostname = urlparse(kwargs["url"]).netloc.split(":")[0]
        for (_, nodes), creds in zip(
            self.cluster_spec.clusters, self.cluster_spec.capella_admin_credentials
        ):
            if hostname in nodes:
                return creds

        # Backup is to use non-admin credentials
        return self.auth

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

    def get_cp_version(self) -> str:
        api = f"{self.dedicated_client.internal_url}/status"
        response = self.get(url=api)
        return response.json()["commit"]

    def is_balanced(self, host: str) -> bool:
        logger.info(f"Getting rebalance counters ({host})")
        counters = self.get_counters(host)
        logger.info(f"counters : {counters}")

        # Don't return balanced status based on counters, due to issues such as
        # AV-78172 and AV-79807
        logger.info(f"Checking if cluster is balanced without counters ({host})")
        return self.get(url=self._get_api_url(host=host, path="pools/default")).json()["balanced"]


class CapellaProvisionedRestHelper(CapellaRestBase):
    def hostname_to_cluster_id(self, host: str):
        if len(self.cluster_ids) == 1:
            return self.cluster_ids[0]

        for i, (_, hostnames) in enumerate(self.cluster_spec.clusters):
            if host in hostnames:
                return self.cluster_ids[i]
        return None

    def get_active_nodes_by_role(self, master_node: str, role: str) -> list[str]:
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

    def get_bucket_mem_available(self, host: str):
        cluster_id = self.hostname_to_cluster_id(host)
        resp = self.dedicated_client.get_buckets(self.tenant_id, self.project_id, cluster_id)
        return {
            'free': resp.json()['freeMemoryInMb'],
            'total': resp.json()['totalMemoryInMb']
        }

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

    def create_or_override_log_streaming_config(self, config: dict):
        logger.info("Creating log streaming: {}".format(config))
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        resp = self.dedicated_client \
            .create_or_override_log_streaming_config(self.tenant_id, self.project_id,
                                                     cluster_id, sgw_cluster_id, config)
        resp.raise_for_status()

    def enable_log_streaming(self):
        logger.info("Enabling log streaming")
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        resp = self.dedicated_client.enable_sgw_logstreaming(self.tenant_id, self.project_id,
                                                             cluster_id, sgw_cluster_id)
        resp.raise_for_status()

    def disable_log_streaming(self):
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        resp = self.dedicated_client.disable_sgw_logstreaming(self.tenant_id, self.project_id,
                                                              cluster_id, sgw_cluster_id)
        resp.raise_for_status()

    def create_log_streaming_config(self, config: dict):
        logger.info("Creating log streaming config")
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        resp = self.dedicated_client.create_sgw_logstreaming_config(self.tenant_id, self.project_id,
                                                                    cluster_id, sgw_cluster_id,
                                                                    config)
        resp.raise_for_status()

    def get_log_streaming_config(self) -> dict:
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        return self.dedicated_client.get_sgw_logstreaming_config(self.tenant_id,
                                                                 self.project_id,
                                                                 cluster_id,
                                                                 sgw_cluster_id).json()

    def get_log_streaming_options(self) -> dict:
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        return self.dedicated_client.get_sgw_logstreaming_collector_options(self.tenant_id,
                                                                            self.project_id,
                                                                            cluster_id,
                                                                            sgw_cluster_id).json()

    def get_log_streaming_selected_options(self) -> dict:
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        return self.dedicated_client \
            .get_sgw_logstreaming_collector_option_selected(self.tenant_id, self.project_id,
                                                            cluster_id, sgw_cluster_id).json()

    def get_logging_options(self) -> dict:
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        return self.dedicated_client.get_logging_options(self.tenant_id,
                                                         self.project_id,
                                                         cluster_id,
                                                         sgw_cluster_id).json()

    def get_logging_config(self) -> dict:
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        return self.dedicated_client.get_logging_config(self.tenant_id,
                                                        self.project_id,
                                                        cluster_id,
                                                        sgw_cluster_id,
                                                        "db-1").json()

    def update_logging_config(self, config: dict):
        logger.info("Updating logging config")
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        resp = self.dedicated_client.update_logging_config(self.tenant_id, self.project_id,
                                                           cluster_id, sgw_cluster_id,
                                                           "db-1", config)
        resp.raise_for_status()

    def sgw_update_db(self, sgw_db_name: str, config: dict):
        logger.info(f'Updating db: {sgw_db_name}')
        cluster_id = self.cluster_ids[0]
        sgw_cluster_id = \
            self.cluster_spec.infrastructure_settings['app_services_cluster']
        resp = self.dedicated_client.update_sgw_database(self.tenant_id, self.project_id,
                                                         cluster_id, sgw_cluster_id,
                                                         sgw_db_name, config)
        logger.info(f'The resp is: {resp}')
        resp.raise_for_status()


class CapellaServerlessRestHelper(CapellaRestBase):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.serverless_client = CapellaAPIServerless(
            self.base_url, self._cbc_user, self._cbc_pwd, self._cbc_token
        )
        self.dp_id = self.cluster_spec.controlplane_settings["dataplane_id"]

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

    def create_fts_index(self, host: str, index: str, definition: dict):
        logger.info('Creating a new FTS index: {}'.format(index))
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(definition, ensure_ascii=False)
        bucket = definition['sourceName']
        auth = self.test_config.serverless_db.bucket_creds(bucket)
        url = self._get_api_url(host=host, path='api/index/{}'.format(index), plain_port=FTS_PORT,
                                ssl_port=FTS_PORT_SSL)
        self.put(url=url, data=data, headers=headers, auth=auth)

    def get_fts_doc_count(self, host: str, index: str, bucket: str) -> int:
        url = self._get_api_url(host=host, path='api/index/{}/count'.format(index),
                                plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL)
        auth = self.test_config.serverless_db.bucket_creds(bucket)
        response = self.get(url=url, auth=auth).json()
        return response['count']

    def exec_n1ql_statement(self, host: str, statement: str, query_context: str) -> dict:
        api = 'https://{}:{}/_p/query/query/service'.format(host, REST_PORT_SSL)

        data = {
            'statement': statement,
            'query_context': query_context
        }

        bucket = query_context.removeprefix('default:').split('.')[0].strip('`')
        auth = self.test_config.serverless_db.bucket_creds(bucket)
        response = self.post(url=api, data=data, auth=auth)

        return response.json()

    def get_active_nodes_by_role(self, master_node: str, role: str) -> list[str]:
        resp = self.get_dataplane_info()
        node_list = resp['couchbase']['nodes']
        has_role = []
        for node_config in node_list:
            if role in node_config['services'][0]['type']:
                has_role.append(node_config['hostname'])
        return has_role

    def get_bucket_fts_stats(self, host: str, bucket, index) -> dict:
        url = self._get_api_url(host=host, path='api/nsstats/index/{}'.format(index),
                                plain_port=FTS_PORT, ssl_port=FTS_PORT_SSL)
        auth = self.test_config.serverless_db.bucket_creds(bucket)
        response = self.get(url=url, auth=auth)
        return response.json()


class CapellaColumnarRestHelper(CapellaRestBase):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.columnar_client = CapellaAPIColumnar(
            self.base_url,
            self.dedicated_client.SECRET,
            self.dedicated_client.ACCESS,
            self._cbc_user,
            self._cbc_pwd,
            self._cbc_token,
        )
        self.instance_ids = self.cluster_spec.controlplane_settings["columnar_ids"].split()

    def get_instance_info(self, instance_id: str) -> dict:
        resp = self.columnar_client.get_specific_columnar_instance(
            self.tenant_id, self.project_id, instance_id
        )
        resp.raise_for_status()
        return resp.json()

    def turn_on_instance(self, instance_id: str):
        logger.info(f"Turning on columnar instance {instance_id}")
        resp = self.columnar_client.turn_on_instance(self.tenant_id, self.project_id, instance_id)
        resp.raise_for_status()

    def turn_off_instance(self, instance_id: str):
        logger.info(f"Turning off columnar instance {instance_id}")
        resp = self.columnar_client.turn_off_instance(self.tenant_id, self.project_id, instance_id)
        resp.raise_for_status()

    def create_capella_remote_link(self, instance_id: str, link_name: str, prov_cluster_id: str):
        logger.info(
            f'Creating remote link "{link_name}" for instance {instance_id} '
            f"to cluster {prov_cluster_id}"
        )
        resp = self.columnar_client.create_link(
            self.tenant_id, self.project_id, instance_id, link_name, prov_cluster_id
        )
        resp.raise_for_status()


# For type hinting rest classes
RestType = Union[
    DefaultRestHelper,
    KubernetesRestHelper,
    CapellaProvisionedRestHelper,
    CapellaServerlessRestHelper,
    CapellaColumnarRestHelper,
]

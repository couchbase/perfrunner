from urllib.request import HTTPBasicAuthHandler

import requests
from couchbase.cluster import QueryOptions

from spring.cbgen_helpers import time_all, timeit


class DAPIGen:

    TIMEOUT = 600  # seconds
    N1QL_TIMEOUT = 600
    DURABILITY = ['majority', 'majorityPersistActive', 'persistMajority']

    def __init__(self, **kwargs):
        self.base_url = kwargs['host']
        self.auth = HTTPBasicAuthHandler(kwargs['username'], kwargs['password'])
        self.bucket_name = kwargs['bucket']
        self.n1ql_timeout = kwargs.get('n1ql_timeout', self.N1QL_TIMEOUT)
        self.meta = 'true' if kwargs['meta'] else 'false'
        self.logs = 'true' if kwargs['logs'] else 'false'
        self.session = None

    def connect_collections(self, scope_collection_list):
        self.session = requests.Session()

    @time_all
    def create(self, target: str, key: str, doc: dict, persist_to: int = 0, replicate_to: int = 0,
               ttl: int = 0):
        scope, collection = target.split(':')
        api = 'https://{}/v1/scopes/{}/collections/{}/docs/{}'.format(
            self.base_url,
            scope,
            collection,
            key
        )
        api += '?meta={}&logs={}&timeout={}s&expiry={}s'.format(
            self.meta,
            self.logs,
            self.TIMEOUT,
            ttl
        )
        headers = {'Content-Type': 'application/json'}
        resp = self.session.post(url=api, auth=self.auth, headers=headers, json=doc)
        resp.raise_for_status()
        return resp

    @time_all
    def create_durable(self, target: str, key: str, doc: dict, durability: int = None,
                       ttl: int = 0):
        scope, collection = target.split(':')
        api = 'https://{}/v1/scopes/{}/collections/{}/docs/{}'.format(
            self.base_url,
            scope,
            collection,
            key
        )
        api += '?meta={}&logs={}&timeout={}s&expiry={}s'.format(
            self.meta,
            self.logs,
            self.TIMEOUT,
            ttl
        )
        if durability is not None and durability > 0:
            api += '&durability={}'.format(self.DURABILITY[durability])
        headers = {'Content-Type': 'application/json'}
        resp = self.session.post(url=api, auth=self.auth, headers=headers, json=doc)
        resp.raise_for_status()
        return resp

    @time_all
    def read(self, target: str, key: str):
        scope, collection = target.split(':')
        api = 'https://{}/v1/scopes/{}/collections/{}/docs/{}'.format(
            self.base_url,
            scope,
            collection,
            key
        )
        api += '?meta={}&logs={}&timeout={}s'.format(
            self.meta,
            self.logs,
            self.TIMEOUT
        )
        headers = {'Content-Type': 'application/json'}
        resp = self.session.get(url=api, auth=self.auth, headers=headers)
        resp.raise_for_status()
        return resp

    @time_all
    def update(self, target: str, key: str, doc: dict, persist_to: int = 0, replicate_to: int = 0,
               ttl: int = 0):
        scope, collection = target.split(':')
        api = 'https://{}/v1/scopes/{}/collections/{}/docs/{}'.format(
            self.base_url,
            scope,
            collection,
            key
        )
        api += '?meta={}&logs={}&timeout={}s&expiry={}s&upsert=true'.format(
            self.meta,
            self.logs,
            self.TIMEOUT,
            ttl
        )
        headers = {'Content-Type': 'application/json'}
        resp = self.session.put(url=api, auth=self.auth, headers=headers, json=doc)
        resp.raise_for_status()
        return resp

    @time_all
    def update_durable(self, target: str, key: str, doc: dict, durability: int = None,
                       ttl: int = 0):
        scope, collection = target.split(':')
        api = 'https://{}/v1/scopes/{}/collections/{}/docs/{}'.format(
            self.base_url,
            scope,
            collection,
            key
        )
        api += '?meta={}&logs={}&timeout={}s&expiry={}s&upsert=true'.format(
            self.meta,
            self.logs,
            self.TIMEOUT,
            ttl
        )
        if durability is not None and durability > 0:
            api += '&durability={}'.format(self.DURABILITY[durability])
        headers = {'Content-Type': 'application/json'}
        resp = self.session.put(url=api, auth=self.auth, headers=headers, json=doc)
        resp.raise_for_status()
        return resp

    @time_all
    def delete(self, target: str, key: str):
        scope, collection = target.split(':')
        api = 'https://{}/v1/scopes/{}/collections/{}/docs/{}'.format(
            self.base_url,
            scope,
            collection,
            key
        )
        api += '?meta={}&logs={}&timeout={}s'.format(
            self.meta,
            self.logs,
            self.TIMEOUT
        )
        headers = {'Content-Type': 'application/json'}
        resp = self.session.delete(url=api, auth=self.auth, headers=headers)
        resp.raise_for_status()
        return resp

    @timeit
    def n1ql_query(self, n1ql_query: str, options: QueryOptions):
        scope = options['query_context'].split(':')[1]
        api = 'https://{}/v1/scopes/{}/query'.format(self.base_url, scope)

        params = [
            'meta={}'.format(self.meta),
            'logs={}'.format(self.logs),
            'timeout={}s'.format(self.n1ql_timeout),
            (
                'preserveExpiry={}'.format(str(v).lower())
                if (v := options.get('preserve_expiry', False)) else None
            ),
            (
                'readonly={}'.format(str(v).lower())
                if (v := options.get('read_only', False)) else None
            ),
            (
                'adhoc={}'.format(str(v).lower())
                if (v := options.get('adhoc', False)) else None
            ),
            (
                'flexIndex={}'.format(str(v).lower())
                if (v := options.get('flex_index', False)) else None
            ),
            (
                'scanConsistency={}'.format(v.value[0] + v.value.title().replace('_', '')[1:])
                if (v := options.get('scan_consistency', False)) else None
            ),
            (
                'scanWait={}'.format(str(v))
                if (v := options.get('scan_wait', False)) else None
            ),
            (
                'maxParallelism={}'.format(v)
                if (v := options.get('max_parallelism', False)) else None
            ),
            (
                'pipelineBatch={}'.format(v)
                if (v := options.get('pipeline_batch', False)) else None
            ),
            (
                'pipelineCap={}'.format(v)
                if (v := options.get('pipeline_cap', False)) else None
            ),
            (
                'scanCap={}'.format(v)
                if (v := options.get('scan_cap', False)) else None
            ),
        ]

        api += '?' + '&'.join([p for p in params if p is not None])

        headers = {'Content-Type': 'application/json'}
        body = {
            'query': n1ql_query,
            'parameters': options.get('positional_parameters', options.get('named_parameters', {}))
        }
        resp = self.session.post(url=api, auth=self.auth, headers=headers, json=body)
        resp.raise_for_status()
        return resp

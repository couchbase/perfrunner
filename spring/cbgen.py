import itertools
import json
from random import choice, randint, shuffle
from threading import Thread
from time import sleep, time

import requests
import urllib3
from couchbase import experimental, subdocument
from couchbase.bucket import Bucket
from couchbase.exceptions import (
    ConnectError,
    CouchbaseError,
    HTTPError,
    KeyExistsError,
    NotFoundError,
    TemporaryFailError,
    TimeoutError,
)
from decorator import decorator
from txcouchbase.connection import Connection as TxConnection

from logger import logger

experimental.enable()


@decorator
def quiet(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except (ConnectError, CouchbaseError, HTTPError, KeyExistsError,
            NotFoundError, TimeoutError) as e:
        logger.warn('Function: {}, error: {}'.format(method.__name__, e))


@decorator
def backoff(method, *args, **kwargs):
    while True:
        try:
            return method(*args, **kwargs)
        except TemporaryFailError:
            sleep(1)


@decorator
def timeit(method, *args, **kwargs) -> int:
    t0 = time()
    method(*args, **kwargs)
    return time() - t0


class CBAsyncGen:

    TIMEOUT = 60  # seconds

    def __init__(self, use_ssl=False, **kwargs):
        self.client = TxConnection(quiet=True, **kwargs)
        self.client.timeout = self.TIMEOUT

    def create(self, key: str, doc: dict):
        return self.client.set(key, doc)

    def read(self, key: str):
        return self.client.get(key)

    def update(self, key: str, doc: dict):
        return self.client.set(key, doc)

    def delete(self, key: str):
        return self.client.delete(key)


class CBGen(CBAsyncGen):

    NODES_UPDATE_INTERVAL = 15

    TIMEOUT = 10  # seconds

    def __init__(self, use_ssl=False, **kwargs):
        connection_string = 'couchbase://{}/{}?password={}'

        if use_ssl:
            connection_string = connection_string.replace('couchbase',
                                                          'couchbases')
            connection_string += '&certpath=root.pem'

        connection_string = connection_string.format(kwargs['host'],
                                                     kwargs['bucket'],
                                                     kwargs['password'])

        self.client = Bucket(connection_string=connection_string)
        self.client.timeout = self.TIMEOUT

        self.session = requests.Session()
        self.session.auth = (kwargs['username'], kwargs['password'])
        self.server_nodes = ['{}:{}'.format(kwargs['host'],
                                            kwargs.get('port', 8091))]
        self.nodes_url = 'http://{}:{}/pools/default/buckets/{}/nodes'.format(
            kwargs['host'],
            kwargs.get('port', 8091),
            kwargs['bucket'],
        )

    def start_updater(self):
        self.t = Thread(target=self._get_list_of_servers)
        self.t.daemon = True
        self.t.start()

    def _get_list_of_servers(self):
        while True:
            try:
                nodes = self.session.get(self.nodes_url).json()
            except Exception as e:
                logger.warn('Failed to get list of servers: {}'.format(e))
                continue
            self.server_nodes = [n['hostname'] for n in nodes['servers']]
            sleep(self.NODES_UPDATE_INTERVAL)

    @quiet
    @backoff
    def create(self, *args, **kwargs):
        super().create(*args, **kwargs)

    @quiet
    @timeit
    def read(self, *args, **kwargs):
        super().read(*args, **kwargs)

    @quiet
    @backoff
    @timeit
    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)

    @quiet
    def delete(self, *args, **kwargs):
        super().delete(*args, **kwargs)

    @timeit
    def view_query(self, ddoc, view, query):
        node = choice(self.server_nodes).replace('8091', '8092')
        url = 'http://{}/{}/_design/{}/_view/{}?{}'.format(
            node, self.client.bucket, ddoc, view, query.encoded
        )
        self.session.get(url=url)

    @quiet
    @timeit
    def n1ql_query(self, query):
        tuple(self.client.n1ql_query(query))


class SubDocGen(CBGen):

    @quiet
    @timeit
    def read(self, key: str, field: str):
        self.client.lookup_in(key, subdocument.get(path=field))

    @quiet
    @timeit
    def update(self, key: str, field: str, doc: dict):
        new_field_value = doc[field]
        self.client.mutate_in(key, subdocument.upsert(path=field,
                                                      value=new_field_value))

    @quiet
    @timeit
    def read_xattr(self, key: str, field: str):
        self.client.lookup_in(key, subdocument.get(path=field,
                                                   xattr=True))

    @quiet
    @timeit
    def update_xattr(self, key: str, field: str, doc: dict):
        self.client.mutate_in(key, subdocument.upsert(path=field,
                                                      value=doc,
                                                      xattr=True,
                                                      create_parents=True))


class FtsGen(CBGen):

    QUERY_TEMPLATE = {"ctl": {"timeout": 0}, "query": {}, "size": 10}

    def __init__(self, master_node, settings, auth=None):

        self.master_node = master_node
        self.query_port = settings.port
        self.auth = auth
        self.requests = requests.Session()
        self.http_pool = self.new_http_pool()

        self.settings = settings
        self.query_nodes = self.get_nodes()
        self.nodes_list_size = len(self.query_nodes)
        self.query_list = []
        self.header = {'Content-Type': 'application/json'}
        self.bool_map = {'conjuncts': 'must', 'disjuncts': 'should'}
        self.query_list_size = 0

        self.prepare_query_list()

    def new_http_pool(self):
        basic_auth = '{}:{}'.format(self.auth.username, self.auth.password)
        headers = urllib3.make_headers(keep_alive=True, basic_auth=basic_auth)
        headers.update({'Content-Type': 'application/json'})

        return urllib3.PoolManager(headers=headers)

    @property
    def query_template(self):
        return self.QUERY_TEMPLATE

    def get_nodes(self):
        nodes = []
        cluster_map = requests.get(url='http://{}:8091/pools/default'.format(self.master_node),
                                   auth=self.auth).json()
        for node in cluster_map['nodes']:
            if 'fts' in node['services']:
                url = node['hostname'].split(":")[0]
                nodes.append(url)
        return nodes

    def form_url(self, full_query):
        url = "http://{}:{}/api/index/{}/query".format(self.next_node(),
                                                       self.query_port,
                                                       self.settings.name)
        return {'url': url,
                'auth': self.auth,
                'headers': self.header,
                'data': json.dumps(full_query)
                }

    @staticmethod
    def process_lines(line):
        if len(line) == 0:
            raise Exception('Empty line')
        value = line.strip().split()
        if len(value) == 2:
            return line.strip().split()
        else:
            return line.strip(), None

    @staticmethod
    def process_conj_disj(ttypes):
        index = 0
        keytypes = []
        while index < len(ttypes):
            count = int(ttypes[index])
            keytypes += count * [ttypes[index + 1]]
            index += 2
        return itertools.cycle(keytypes)

    def prepare_query_list(self):
        if self.settings.query_file:
            with open(self.settings.query_file, 'r') as tfile:
                for line in tfile:
                    temp_query = {}
                    tosearch, freq = FtsGen.process_lines(line.strip())
                    query_type = self.settings.type
                    if query_type in ['2_conjuncts', '2_disjuncts', '1_conjuncts_2_disjuncts']:
                        from collections import defaultdict
                        keytypes = FtsGen.process_conj_disj(query_type.split('_'))
                        temp_query = defaultdict(list)
                        tbool = {v: {k: None} for k, v in self.bool_map.items()}

                        for terms in line.split():

                            tmp_key = next(keytypes)
                            temp_query[tmp_key].append({"field": self.settings.field,
                                                        "term": terms})

                        if query_type == '1_conjuncts_2_disjuncts':
                            for k, v in self.bool_map.items():
                                tbool[v][k] = temp_query[k]
                            temp_query = tbool

                    elif query_type == 'fuzzy':
                        temp_query['fuzziness'] = int(freq)
                        temp_query['term'] = tosearch
                        temp_query['field'] = self.settings.field

                    elif query_type == 'numeric':
                        if freq.strip() == 'max_min':
                            temp_query['max'], temp_query['min'] = \
                                [float(k) for k in tosearch.split(':')]
                        elif freq.strip() == 'max':
                            temp_query['max'] = float(tosearch)
                        else:
                            temp_query['min'] = float(tosearch)
                        temp_query['inclusive_max'] = False
                        temp_query['inclusive_min'] = False
                        temp_query['field'] = self.settings.field

                    elif query_type in ['match', 'match_phrase']:
                        tosearch = line.strip()
                        temp_query[query_type] = tosearch
                        temp_query['field'] = self.settings.field

                    elif query_type == 'ids':
                        tosearch = [tosearch]
                        temp_query[query_type] = tosearch
                        temp_query['field'] = self.settings.field

                    elif query_type == "facet":

                        start_date, end_date = freq.split(':')
                        temp_query["query"] = "text:{}".format(tosearch)
                        temp_query["boost"] = 1
                        self.query_template['fields'] = ["*"]
                        self.query_template["facets"] = {self.settings.field:
                                                         {"size": self.settings.query_size,
                                                          "field": self.settings.field,
                                                          "date_ranges": [{"name": "end",
                                                                           "end": end_date},
                                                                          {"name": "start",
                                                                           "start": start_date}]}}
                    else:
                        temp_query[query_type] = tosearch
                        temp_query['field'] = self.settings.field

                    self.query_template['query'] = temp_query
                    self.query_template['size'] = self.settings.query_size
                    self.query_list.append(self.form_url(self.query_template))

            self.query_list_size = len(self.query_list)
            shuffle(self.query_list)

    def next(self):
        return self.requests.post, self.query_list[randint(0, self.query_list_size - 1)]

    def next_node(self):
        return self.query_nodes[randint(0, self.nodes_list_size - 1)]

    def execute_query(self, query):
        return self.http_pool.request('POST', query['url'], body=query['data'])


class ElasticGen(FtsGen):

    QUERY_TEMPLATE = {"query": {}, "size": 10}

    def form_url(self, full_query):
        url = "http://{}:9200/{}/_search".format(self.next_node(), self.settings.name)
        return {'url': url,
                'auth': None,
                'headers': self.header,
                'data': json.dumps(full_query)
                }

    def get_nodes(self):
        nodes = []
        cluster_map = requests.get(url='http://{}:9200/_nodes'.format(self.master_node)).json()
        for node in cluster_map['nodes'].values():
            url = node["ip"]
            nodes.append(url)
        return nodes

    def prepare_query_list(self):
        if self.settings.query_file:
            with open(self.settings.query_file, 'r') as tfile:
                for line in tfile:
                    term, freq = ElasticGen.process_lines(line.strip())
                    tmp_query = {}
                    tmp_query_txt = {}
                    query_type = self.settings.type
                    if query_type == 'fuzzy':
                        tmp_fuzzy = {
                            'fuzziness': int(freq),
                            'value': term,
                        }
                        tmp_query_txt[self.settings.field] = tmp_fuzzy
                        tmp_query[query_type] = tmp_query_txt

                    elif query_type == 'ids':
                        tmp_query_txt['values'] = [term]
                        tmp_query[query_type] = tmp_query_txt

                    elif query_type in ['match', 'match_phrase']:
                        tmp_query_txt[self.settings.field] = line.strip()
                        tmp_query[query_type] = tmp_query_txt

                    elif query_type == 'range':
                        trange = {}
                        if freq.strip() == 'max_min':
                            trange['gte'], trange['lte'] = [float(k) for k in term.split(':')]
                        elif freq.strip() == 'max':
                            trange['gte'] = float(term)
                        else:
                            trange['lte'] = float(term)
                        tmp_query_txt[self.settings.field] = trange
                        tmp_query[query_type] = tmp_query_txt

                    elif query_type in ['2_conjuncts', '2_disjuncts', '1_conjuncts_2_disjuncts']:
                        tbool = {v: [] for k, v in self.bool_map.items()}
                        keytypes = ElasticGen.process_conj_disj(query_type.split('_'))
                        for term in line.strip().split():
                            key = self.bool_map[next(keytypes)]
                            tbool[key].append({'term': {self.settings.field: term}})
                        tmp_query_txt = tbool
                        tmp_query['bool'] = tmp_query_txt

                    elif query_type == 'facet':
                        start_date, end_date = freq.split(':')
                        tmp_query = {"term": {"text": term}}
                        self.query_template['size'] = self.settings.query_size
                        self.query_template['aggs'] = {
                            "perf_elastic_index": {
                                "date_range": {
                                    "field": self.settings.field,
                                    "format": "YYYY-MM-DD",
                                    "ranges": [
                                        {
                                            "from": start_date,
                                            "to": end_date,
                                        },
                                    ]
                                },
                                "aggs": {
                                    "terms_count": {"terms": {"field": "text"}},
                                },
                            },
                        }

                    else:
                        tmp_query_txt[self.settings.field] = term
                        tmp_query[query_type] = tmp_query_txt

                    self.query_template['query'] = tmp_query
                    self.query_list.append(self.form_url(self.query_template))
            self.query_list_size = len(self.query_list)
            shuffle(self.query_list)

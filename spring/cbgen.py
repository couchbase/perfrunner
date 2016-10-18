import copy
import itertools
import json
import logging
from random import choice, shuffle
from threading import Thread
from time import sleep, time

import couchbase.subdocument as SD
import requests
import urllib3
from couchbase import experimental
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
from logger import logger
from requests.auth import HTTPBasicAuth
from txcouchbase.connection import Connection as TxConnection

from spring.docgen import Document

experimental.enable()

logging.getLogger("urllib3").setLevel(logging.WARNING)


@decorator
def quiet(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except (ConnectError, CouchbaseError, HTTPError, KeyExistsError,
            NotFoundError, TemporaryFailError, TimeoutError) as e:
        logger.warn('{}: {}'.format(method, e))


class CBAsyncGen(object):

    def __init__(self, **kwargs):
        self.client = TxConnection(quiet=True, timeout=60, **kwargs)

    def create(self, key, doc, ttl=None):
        extra_params = {}
        if ttl is None:
            extra_params['ttl'] = ttl
        return self.client.set(key, doc, **extra_params)

    def read(self, key):
        return self.client.get(key)

    def update(self, key, doc):
        return self.client.set(key, doc)

    def cas(self, key, doc):
        cas = self.client.get(key).cas
        return self.client.set(key, doc, cas=cas)

    def delete(self, key):
        return self.client.delete(key)


class CBGen(CBAsyncGen):

    NODES_UPDATE_INTERVAL = 15

    TIMEOUT = 10  # seconds

    def __init__(self, **kwargs):
        self.client = Bucket(
            'couchbase://{}:{}/{}'.format(kwargs['host'],
                                          kwargs.get('port', 8091),
                                          kwargs['bucket']),
            password=kwargs['password'],
            timeout=self.TIMEOUT,
        )
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
    def create(self, *args, **kwargs):
        super(CBGen, self).create(*args, **kwargs)

    @quiet
    def read(self, *args, **kwargs):
        super(CBGen, self).read(*args, **kwargs)

    @quiet
    def update(self, *args, **kwargs):
        super(CBGen, self).update(*args, **kwargs)

    @quiet
    def cas(self, *args, **kwargs):
        super(CBGen, self).cas(*args, **kwargs)

    @quiet
    def delete(self, *args, **kwargs):
        super(CBGen, self).delete(*args, **kwargs)

    def query(self, ddoc, view, query):
        node = choice(self.server_nodes).replace('8091', '8092')
        url = 'http://{}/{}/_design/{}/_view/{}?{}'.format(
            node, self.client.bucket, ddoc, view, query.encoded
        )
        t0 = time()
        resp = self.session.get(url=url)
        latency = time() - t0
        return resp.text, latency

    @quiet
    def lcb_query(self, ddoc, view, query):
        return tuple(self.client.query(ddoc, view, query=query))


class SubDocGen(CBGen):

    def read(self, key, subdoc_fields):
        for field in subdoc_fields.split(','):
            self.client.lookup_in(key, SD.get(field))

    def update(self, key, subdoc_fields, size):
        newdoc = Document(size)
        alphabet = newdoc._build_alphabet(key)
        for field in subdoc_fields.split(','):
            new_field_value = getattr(newdoc, '_build_' + field)(alphabet)
            self.client.mutate_in(key, SD.upsert(field, new_field_value))

    def counter(self, key, subdoc_counter_fields):
        for field in subdoc_counter_fields.split(','):
            self.client.counter_in(key, field, delta=50)

    def delete(self, key, subdoc_delete_fields):
        for field in subdoc_delete_fields.split(','):
            self.client.remove_in(key, field)

    def multipath(self):
        raise NotImplementedError


class N1QLGen(CBGen):

    def __init__(self, bucket, password, host, port=8091):
        self.bucket = bucket
        self.password = password

        self.connections = self._get_query_connections(host, port)

    def _get_query_connections(self, host, port):
        nodes = requests.get(url='http://{}:{}/pools/default'.format(host, port),
                             auth=(self.bucket, self.password)).json()

        connections = []
        for node in nodes['nodes']:
            if 'n1ql' in node['services']:
                url = node['hostname'].replace('8091', '8093')
                connections.append(urllib3.connection_from_url(url))

        return connections

    def query(self, query, *args):
        creds = '[{{"user":"local:{}","pass":"{}"}}]'.format(self.bucket,
                                                             self.password)
        query['creds'] = creds

        if len(self.connections) > 1:
            connection = choice(self.connections)
        else:
            connection = self.connections[0]  # Faster than redundant choice

        t0 = time()
        response = connection.request('POST', '/query/service', fields=query,
                                      encode_multipart=False)
        response.read(cache_content=False)
        latency = time() - t0

        return None, latency


class FtsGen(CBGen):
    '''
      FTSGen and ElasticGen classes are fts and ES query generators.
      This part of code can be generalised and work complex way.
      generators.
    '''
    __FTS_QUERY = {"ctl": {"timeout": 0}, "query": {}, "size": 10}

    def __init__(self, cb_url, settings):
        self.fts_query = "http://{}:8094/api/".format(cb_url)
        self.auth = HTTPBasicAuth('Administrator', 'password')
        self.header = {'Content-Type': 'application/json'}
        self.requests = requests.Session()
        '''
              keep-alive is used false to avoid any cache.
              Using keep-alive false
              makes it a  real life scenario
            '''
        self.requests.keep_alive = False
        self.query = self.__FTS_QUERY
        self.query_list = []
        self.query_iterator = None
        self.settings = settings
        self.fts_copy = copy.deepcopy(self.fts_query)
        self.bool_map = {'conjuncts': 'must', 'disjuncts': 'should'}

    def form_url(self, cquery):
        return {'url': cquery,
                'auth': self.auth or None,
                'headers': self.header,
                'data': json.dumps(self.query)
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
            '''
            index incremented by 2 becuase the type is mentioned
            as '1_conjuncts_2_disjuncts', 'number_typeofsearch' ]
            Here we are creating a list of types to search
            '''
            index += 2
        return itertools.cycle(keytypes)

    def prepare_query_list(self, type='query'):
            with open(self.settings.query_file, 'r') as tfile:
                '''
                  Open file using WITH command , it take care of close
                '''
                for line in tfile:
                    temp_query = {}
                    tosearch, freq = FtsGen.process_lines(line.strip())
                    if self.settings.type in ['2_conjuncts', '2_disjuncts', '1_conjuncts_2_disjuncts']:
                        '''
                         For And, OR queries we create the list.
                         Example looks like
                         {"field": "title", "match": "1988"}
                         Elasticsearch things are simple , for us it conjuncts, disjunct are
                         two seperate.
                         Defaultdict is used only here, I dont want to import elesewhere
                         to minimise the import overload
                         reference: https://wiki.python.org/moin/PythonSpeed/PerformanceTips
                                    https://blog.newrelic.com/2015/01/21/python-performance-tips/
                        '''
                        from collections import defaultdict
                        keytypes = FtsGen.process_conj_disj(self.settings.type.split('_'))
                        temp_query = defaultdict(list)
                        tbool = {v: {k: None} for k, v in self.bool_map.iteritems()}

                        for terms in line.split():
                            '''
                             Term query is used for And/Or queries
                            '''
                            tmp_key = keytypes.next()
                            temp_query[tmp_key].append({"field": self.settings.field, "term": terms})

                        if self.settings.type == '1_conjuncts_2_disjuncts':
                            for k, v in self.bool_map.iteritems():
                                tbool[v][k] = temp_query[k]
                            temp_query = tbool

                    elif self.settings.type == 'fuzzy':
                        '''
                        Fuzzy query uses term as search type unlike
                        elasticsearch where they do use word fuzzy
                        '''
                        temp_query['fuzziness'] = int(freq)
                        temp_query['term'] = tosearch
                        temp_query['field'] = self.settings.field

                    elif self.settings.type == 'numeric':
                        '''
                        Creating numeric range query Gen
                        '''
                        if freq.strip() == 'max_min':
                            temp_query['max'], temp_query['min'] = [float(k) for k in tosearch.split(':')]
                        elif freq.strip() == 'max':
                            temp_query['max'] = float(tosearch)
                        else:
                            temp_query['min'] = float(tosearch)
                        temp_query['inclusive_max'] = False
                        temp_query['inclusive_min'] = False
                        temp_query['field'] = self.settings.field

                    elif self.settings.type == 'match':
                        '''
                        Just reassign the whole line to it
                        '''
                        tosearch = line.strip()
                        temp_query[self.settings.type] = tosearch
                        temp_query['field'] = self.settings.field

                    elif self.settings.type == 'ids':
                        '''
                        The ids are from document ids , generated through
                        the N1QL query
                        '''
                        tosearch = [tosearch]
                        temp_query[self.settings.type] = tosearch
                        temp_query['field'] = self.settings.field

                    elif self.settings.type == "facet":
                        '''
                        https://gist.github.com/mschoch/9bc5f508bb25c94ed9af
                        "query": {
                            "boost": 1,
                            "query": "beer"
                          },
                          "fields": ["*"],
                          "facets": {
                            "updated": {
                              "size": 3,
                              "field": "updated",
                              "date_ranges": [
                                      {
                                              "name": "old",
                                              "end": "2010-07-27"
                                      },
                                      {
                                              "name": "new",
                                              "start": "2010-07-27"
                                      }
                              ]
                            }
                        '''
                        start_date, end_date = freq.split(':')
                        temp_query["query"] = tosearch
                        temp_query["boost"] = 1
                        self.query['fields'] = ["*"]
                        self.query["facets"] = {self.settings.field: {"size": 5, "field": self.settings.field,
                                                "date_ranges": [{"name": "end", "end": end_date},
                                                                {"name": "start", "start": start_date}]}}
                    else:
                        temp_query[self.settings.type] = tosearch
                        temp_query['field'] = self.settings.field

                    self.query['query'] = temp_query
                    self.query['size'] = self.settings.query_size
                    self.fts_query += 'index/' + self.settings.name + '/' + type
                    self.query_list.append(self.form_url(self.fts_query))
                    self.fts_query = self.fts_copy
            shuffle(self.query_list)
            self.query_iterator = itertools.cycle(self.query_list)

    def next(self):
        return self.requests.post, self.query_iterator.next()

    def prepare_query(self, type='query'):
        self.fts_query = self.fts_copy
        if type == 'query':
            self.prepare_query_list()
        elif type == 'count':
            self.fts_query += 'index/' + self.settings.name + '/' + type
            return self.requests.get, self.form_url(self.fts_query)
        elif type == 'nsstats':
            self.fts_query += type
            return self.requests.get, self.form_url(self.fts_query)


class ElasticGen(FtsGen):

    __ELASTIC_QUERY = {"query": {}, "size": 10}

    def __init__(self, elastic_url, settings):
        super(ElasticGen, self).__init__(elastic_url, settings)
        self.query = self.__ELASTIC_QUERY
        self.elastic_query = "http://{}:9200/".format(elastic_url)
        self.elastic_copy = copy.deepcopy(self.elastic_query)
        self.auth = None

    def prepare_query(self, type='query'):
        if type == 'query':
            try:
                with open(self.settings.query_file, 'r') as tfile:
                    for line in tfile:
                        self.elastic_query = self.elastic_copy
                        term, freq = ElasticGen.process_lines(line.strip())
                        self.query['size'] = self.settings.query_size
                        self.elastic_query += self.settings.name + '/_search'
                        tmp_query = {}
                        tmp_query_txt = {}
                        if self.settings.type == 'fuzzy':
                            '''
                            fuzziness is extra parameter for fuzzy
                            Source: https://www.elastic.co/guide/en/elasticsearch/reference/current/
                            query-dsl-fuzzy-query.html
                            '''
                            tmp_fuzzy = {}
                            tmp_fuzzy['fuzziness'] = int(freq)
                            tmp_fuzzy['value'] = term
                            tmp_query_txt[self.settings.field] = tmp_fuzzy
                            tmp_query[self.settings.type] = tmp_query_txt

                        elif self.settings.type == 'ids':
                            '''
                            values is extra parameter for Docid
                            source: https://www.elastic.co/guide/en/elasticsearch/reference/
                            current/query-dsl-ids-query.html
                            '''
                            tmp_query_txt['values'] = [term]
                            tmp_query[self.settings.type] = tmp_query_txt

                        elif self.settings.type == 'match':
                            '''
                            Just reassign the whole line to it
                            Source: https://www.elastic.co/guide/en/elasticsearch/reference/
                            current/query-dsl-fuzzy-query.html
                            '''
                            tmp_query_txt[self.settings.field] = line.strip()
                            tmp_query[self.settings.type] = tmp_query_txt

                        elif self.settings.type == 'range':
                            trange = {}
                            if freq.strip() == 'max_min':
                                trange['gte'], trange['lte'] = [float(k) for k in term.split(':')]
                            elif freq.strip() == 'max':
                                trange['gte'] = float(term)
                            else:
                                trange['lte'] = float(term)
                            tmp_query_txt[self.settings.field] = trange
                            tmp_query[self.settings.type] = tmp_query_txt

                        elif self.settings.type in ['2_conjuncts', '2_disjuncts', '1_conjuncts_2_disjuncts']:
                            '''
                            For mix queries the name is like a map '1_conjuncts_2_disjuncts'
                            => 1 conjuncts and 2 disjuncts
                            '''
                            tbool = {v: [] for k, v in self.bool_map.iteritems()}
                            keytypes = ElasticGen.process_conj_disj(self.settings.type.split('_'))
                            for term in line.strip().split():
                                key = self.bool_map[keytypes.next()]
                                tbool[key].append({'term': {self.settings.field: term}})
                            tmp_query_txt = tbool
                            tmp_query['bool'] = tmp_query_txt

                        elif self.settings.type == 'facet':
                            start_date, end_date = freq.split(':')
                            tmp_query = {"term": {"text": term}}
                            self.query['size'] = self.settings.query_size
                            self.query['aggs'] = {"perf_elastic_index": {"date_range": {
                                                  "field": self.settings.field,
                                                  "format": "YYYY-MM-DD",
                                                  "ranges": [{"from": start_date, "to": end_date}]
                                                  }, "aggs": {"terms_count": {"terms": {"field": "text"}}}}}

                        else:
                            tmp_query_txt[self.settings.field] = term
                            tmp_query[self.settings.type] = tmp_query_txt

                        self.query['query'] = tmp_query
                        self.query_list.append(self.form_url(self.elastic_query))
                shuffle(self.query_list)
                self.query_iterator = itertools.cycle(self.query_list)
            except OSError as err:
                logger.info("OS error: {0}".format(err))
            except Exception as err:
                logger.info("Error: {0}".format(err))
        elif type == 'stats':
            self.elastic_query += self.settings.name + '/_stats/'
            return self.requests.get, self.form_url(self.elastic_query)

import copy
import itertools
import json
from random import choice, shuffle
from threading import Thread
from time import sleep, time

import couchbase.subdocument as SD
import requests
import urllib3
from couchbase import experimental
from couchbase.bucket import Bucket
from couchbase.exceptions import (ConnectError, CouchbaseError, HTTPError,
                                  KeyExistsError, NotFoundError,
                                  TemporaryFailError, TimeoutError)
from decorator import decorator
from logger import logger
from requests.auth import HTTPBasicAuth
from txcouchbase.connection import Connection as TxConnection

from spring.docgen import NewDocument

experimental.enable()


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

    def __init__(self, **kwargs):
        self.client = Bucket(
            'couchbase://{}:{}/{}'.format(kwargs['host'],
                                          kwargs.get('port', 8091),
                                          kwargs['bucket']),
            password=kwargs['password'],
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
        newdoc = NewDocument(size)
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

    def __init__(self, **kwargs):
        super(N1QLGen, self).__init__(**kwargs)
        self.bucket = kwargs['username']
        self.password = kwargs['password']

        self.query_url = 'http://{}:{}/pools/default'.format(
            kwargs['host'],
            kwargs.get('port', 8091),
        )
        self.query_conns = self._get_query_connections()

    def start_updater(self):
        pass

    def _get_query_connections(self):
        conns = []
        try:
            nodes = self.session.get(self.query_url).json()
            for node in nodes['nodes']:
                if 'n1ql' in node['services']:
                    url = node['hostname'].replace('8091', '8093')
                    conns.append(urllib3.connection_from_url(url))
        except Exception as e:
            logger.warn('Failed to get list of servers: {}'.format(e))
            raise

        return conns

    def query(self, ddoc_name, view_name, query):
        creds = '[{{"user":"local:{}","pass":"{}"}}]'.format(self.bucket,
                                                             self.password)

        query['creds'] = creds
        node = choice(self.query_conns)

        t0 = time()
        response = node.request('POST', '/query/service', fields=query,
                                encode_multipart=False)
        response.read(cache_content=False)
        latency = time() - t0
        return None, latency


class FtsGen(CBGen):
    __FTS_QUERY = {"ctl": {"timeout": 0, "consistency": {"vectors": {}, "level": ""}},
                   "query": {}, "size": 10}

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

    def form_url(self, cquery):
        return {'url': cquery,
                'auth': self.auth or None,
                'headers': self.header,
                'data': json.dumps(self.query)
                }

    def process_lines(self, line):
        if len(line) == 0:
            raise Exception('Empty line')
        if ' ' in line:
            return line.strip().split()
        else:
            return line.strip(), None

    def prepare_query_list(self, type='query'):
        try:
            file = open(self.settings.query_file, 'r')
            for line in file:
                temp_query = {}
                tosearch, freq = self.process_lines(line.strip())
                if self.settings.type in ['conjuncts', 'disjuncts']:
                    '''
                     For And, OR queries we create the list.
                     Example looks like
                     {"field": "title", "match": "1988"}
                    '''
                    tosearch = []
                    for terms in line.split():
                        '''
                         Term query is used for And/Or queries
                        '''
                        tosearch.append({"field": "text", "term": terms})
                    temp_query[self.settings.type] = tosearch

                elif self.settings.type == 'fuzzy':
                    temp_query['fuzziness'] = int(freq)
                    temp_query['term'] = tosearch

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

                elif self.settings.type == 'match':
                    '''
                    Just reassign the whole line to it
                    '''
                    tosearch = line.strip()
                    temp_query[self.settings.type] = tosearch

                elif self.settings.type == 'ids':
                    tosearch = [tosearch]
                    temp_query[self.settings.type] = tosearch
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
        except OSError as err:
            logger.info("OS error: {0}".format(err))
        except Exception:
            pass

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
                file = open(self.settings.query_file, 'r')
                for line in file:
                    self.elastic_query = self.elastic_copy
                    term, freq = self.process_lines(line.strip())
                    self.query['size'] = self.settings.query_size
                    self.elastic_query += self.settings.name + '/_search?pretty'
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
                    elif self.settings.type == 'ids':
                        '''
                        values is extra parameter for Docid
                        source: https://www.elastic.co/guide/en/elasticsearch/reference/
                        current/query-dsl-ids-query.html
                        '''
                        tmp_query_txt['values'] = [term]

                    elif self.settings.type == 'match':
                        '''
                        Just reassign the whole line to it
                        Source: https://www.elastic.co/guide/en/elasticsearch/reference/
                        current/query-dsl-fuzzy-query.html
                        '''
                        tmp_query_txt[self.settings.field] = line.strip()

                    elif self.settings.type == 'range':
                        trange = {}
                        if freq.strip() == 'max_min':
                            trange['gte'], trange['lte'] = [float(k) for k in term.split(':')]
                        elif freq.strip() == 'max':
                            trange['gte'] = float(term)
                        else:
                            trange['lte'] = float(term)
                        tmp_query_txt[self.settings.field] = trange

                    else:
                        tmp_query_txt[self.settings.field] = term

                    tmp_query[self.settings.type] = tmp_query_txt
                    self.query['query'] = tmp_query
                    self.query_list.append(self.form_url(self.elastic_query))
                shuffle(self.query_list)
                self.query_iterator = itertools.cycle(self.query_list)
            except OSError as err:
                logger.info("OS error: {0}".format(err))
            except Exception:
                pass
        elif type == 'stats':
            self.elastic_query += self.settings.name + '/_stats/'
            return self.requests.get, self.form_url(self.elastic_query)

from random import choice
from threading import Thread
from time import sleep, time

import requests
from couchbase import experimental, subdocument
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError, TemporaryFailError
from decorator import decorator
from txcouchbase.connection import Connection as TxConnection

from logger import logger

experimental.enable()


@decorator
def quiet(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except CouchbaseError as e:
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

    def __init__(self, use_ssl=False, n1ql_timeout=None, **kwargs):
        connection_string = 'couchbase://{}/{}?ipv6=allow&password={}'

        if use_ssl:
            connection_string = connection_string.replace('couchbase',
                                                          'couchbases')
            connection_string += '&certpath=root.pem'

        connection_string = connection_string.format(kwargs['host'],
                                                     kwargs['bucket'],
                                                     kwargs['password'])

        self.client = Bucket(connection_string=connection_string)
        self.client.timeout = self.TIMEOUT
        if n1ql_timeout:
            self.client.n1ql_timeout = n1ql_timeout

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
    @backoff
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

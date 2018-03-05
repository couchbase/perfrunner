from collections import defaultdict
from random import choice
from threading import Thread, Timer
from time import sleep, time
from typing import Callable

import requests
from couchbase import experimental, subdocument
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError, TemporaryFailError
from decorator import decorator
from txcouchbase.connection import Connection as TxConnection

from logger import logger

experimental.enable()


class ErrorTracker:

    MSG = 'Function: {}, error: {}'

    MSG_REPEATED = 'Function: {}, error: {}, repeated {} times'

    QUIET_PERIOD = 10  # 10 seconds

    def __init__(self):
        self.errors = defaultdict(int)

    def track(self, method: str, exc: CouchbaseError):
        if type(exc) not in self.errors:
            self.warn(method, exc)  # Always warn upon the first occurrence
            self.check_later(method, exc)
        self.incr(exc)

    def incr(self, exc: CouchbaseError):
        self.errors[type(exc)] += 1

    def reset(self, exc: CouchbaseError):
        self.errors[type(exc)] = 0

    def check_later(self, method: str, exc: CouchbaseError):
        Timer(self.QUIET_PERIOD, self.maybe_warn, args=(method, exc)).start()

    def warn(self, method: str, exc: CouchbaseError, count: int = 0):
        if count:
            logger.warn(self.MSG_REPEATED.format(method, exc, count))
        else:
            logger.warn(self.MSG.format(method, exc))

    def maybe_warn(self, method: str, exc: CouchbaseError):
        count = self.errors[type(exc)]
        if count > 1:
            self.reset(exc)
            self.warn(method, exc, count)
            self.check_later(method, exc)
        else:  # Not repeated, hence stop tracking it
            self.errors.pop(type(exc))


error_tracker = ErrorTracker()


@decorator
def quiet(method: Callable, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except CouchbaseError as e:
        error_tracker.track(method.__name__, e)


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

    def __init__(self, ssl_mode='none', **kwargs):
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

    def __init__(self, ssl_mode='none', n1ql_timeout=None, **kwargs):
        connection_string = 'couchbase://{}/{}?ipv6=allow&password={}'

        if ssl_mode == 'data':
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

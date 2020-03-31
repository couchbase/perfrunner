from urllib import parse

import pkg_resources

from logger import logger
from spring.cbgen_helpers import backoff, quiet, timeit

cb_version = pkg_resources.get_distribution("couchbase").version
if cb_version[0] == '2':
    from couchbase import experimental, subdocument
    from couchbase.bucket import Bucket
    from couchbase.n1ql import N1QLQuery
    from couchbase.views.params import ViewQuery
    from txcouchbase.connection import Connection as TxConnection
    experimental.enable()
elif cb_version == '3.0.0b3':
    from couchbase_v2 import experimental, subdocument
    from couchbase_v2.bucket import Bucket
    from couchbase_v2.n1ql import N1QLQuery
    from couchbase_v2.views.params import ViewQuery
    from txcouchbase.connection import Connection as TxConnection
    experimental.enable()


class CBAsyncGen:

    TIMEOUT = 60  # seconds

    def __init__(self, **kwargs):
        self.client = TxConnection(quiet=True, **kwargs)
        self.client.timeout = self.TIMEOUT

    def create(self, key: str, doc: dict, persist_to: int = 0,
               replicate_to: int = 0, ttl: int = 0):
        return self.client.upsert(key, doc,
                                  persist_to=persist_to,
                                  replicate_to=replicate_to,
                                  ttl=ttl)

    def create_durable(self, key: str, doc: dict, durability: int = None, ttl: int = 0):
        return self.client.upsert(key, doc,
                                  durability_level=durability,
                                  ttl=ttl)

    def read(self, key: str):
        return self.client.get(key)

    def update(self, key: str, doc: dict, persist_to: int = 0,
               replicate_to: int = 0, ttl: int = 0):
        return self.client.upsert(key, doc,
                                  persist_to=persist_to,
                                  replicate_to=replicate_to,
                                  ttl=ttl)

    def update_durable(self, key: str, doc: dict, durability: int = None, ttl: int = 0):
        return self.client.upsert(key, doc,
                                  durability_level=durability,
                                  ttl=ttl)

    def delete(self, key: str):
        return self.client.remove(key)


class CBGen(CBAsyncGen):

    TIMEOUT = 10  # seconds

    def __init__(self, ssl_mode: str = 'none', n1ql_timeout: int = None, **kwargs):

        connection_string = 'couchbase://{host}/{bucket}?password={password}&{params}'
        connstr_params = parse.urlencode(kwargs["connstr_params"])

        if ssl_mode == 'data':
            connection_string = connection_string.replace('couchbase',
                                                          'couchbases')
            connection_string += '&certpath=root.pem'

        connection_string = connection_string.format(host=kwargs['host'],
                                                     bucket=kwargs['bucket'],
                                                     password=kwargs['password'],
                                                     params=connstr_params)

        self.client = Bucket(connection_string=connection_string)
        self.client.timeout = self.TIMEOUT
        if n1ql_timeout:
            self.client.n1ql_timeout = n1ql_timeout
        logger.info("Connection string: {}".format(connection_string))

    @quiet
    @backoff
    def create(self, *args, **kwargs):
        super().create(*args, **kwargs)

    @quiet
    @backoff
    def create_durable(self, *args, **kwargs):
        super().create_durable(*args, **kwargs)

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
    @backoff
    @timeit
    def update_durable(self, *args, **kwargs):
        super().update_durable(*args, **kwargs)

    @quiet
    def delete(self, *args, **kwargs):
        super().delete(*args, **kwargs)

    @timeit
    def view_query(self, ddoc: str, view: str, view_query: ViewQuery):
        tuple(self.client.query(ddoc, view, query=view_query))

    @quiet
    @timeit
    def n1ql_query(self, n1ql_query: N1QLQuery):
        tuple(self.client.n1ql_query(n1ql_query))


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

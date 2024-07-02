import time
from ctypes import CDLL
from datetime import timedelta

from couchbase.cluster import (
    Cluster,
    ClusterOptions,
    ClusterTimeoutOptions,
    QueryOptions,
)
from couchbase.management.collections import CollectionSpec
from couchbase.management.users import User
from couchbase_core.cluster import PasswordAuthenticator
from couchbase_core.views.params import ViewQuery
from txcouchbase.cluster import TxCluster

from spring.cbgen_helpers import backoff, get_connection, quiet, time_all, timeit


class CBAsyncGen3:

    TIMEOUT = 120  # seconds

    def __init__(self, **kwargs):
        connection_string, cert_path = get_connection(**kwargs)
        timeout = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=self.TIMEOUT))
        options = ClusterOptions(
            authenticator=PasswordAuthenticator(
                kwargs["username"], kwargs["password"], cert_path=cert_path
            ),
            timeout_options=timeout,
        )
        self.cluster = TxCluster(connection_string=connection_string, options=options)
        self.bucket_name = kwargs['bucket']
        self.collections = dict()
        self.collection = None

    def connect_collections(self, scope_collection_list):
        self.bucket = self.cluster.bucket(self.bucket_name)
        for scope_collection in scope_collection_list:
            scope, collection = scope_collection.split(":")
            if scope == "_default" and collection == "_default":
                self.collections[scope_collection] = \
                    self.bucket.default_collection()
            else:
                self.collections[scope_collection] = \
                    self.bucket.scope(scope).collection(collection)

    def create(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_create(*args[1:], **kwargs)

    def do_create(self, key: str, doc: dict, persist_to: int = 0,
                  replicate_to: int = 0, ttl: int = 0):
        return self.collection.upsert(key, doc,
                                      persist_to=persist_to,
                                      replicate_to=replicate_to,
                                      ttl=ttl)

    def create_durable(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_create_durable(*args[1:], **kwargs)

    def do_create_durable(self, key: str, doc: dict, durability: int = None, ttl: int = 0):
        return self.collection.upsert(key, doc,
                                      durability_level=durability,
                                      ttl=ttl)

    def read(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_read(*args[1:], **kwargs)

    def do_read(self, key: str):
        return self.collection.get(key)

    def update(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_update(*args[1:], **kwargs)

    def do_update(self, key: str, doc: dict, persist_to: int = 0,
                  replicate_to: int = 0, ttl: int = 0):
        return self.collection.upsert(key,
                                      doc,
                                      persist_to=persist_to,
                                      replicate_to=replicate_to,
                                      ttl=ttl)

    def update_durable(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_update_durable(*args[1:], **kwargs)

    def do_update_durable(self, key: str, doc: dict, durability: int = None, ttl: int = 0):
        return self.collection.upsert(key, doc,
                                      durability_level=durability,
                                      ttl=ttl)

    def delete(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_delete(*args[1:], **kwargs)

    def do_delete(self, key: str):
        return self.collection.remove(key)


class CBGen3(CBAsyncGen3):

    TIMEOUT = 600  # seconds
    N1QL_TIMEOUT = 600

    def __init__(self, **kwargs):
        connection_string, cert_path = get_connection(**kwargs)

        timeout = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=self.TIMEOUT),
            query_timeout=timedelta(seconds=kwargs.get("n1ql_timeout") or self.N1QL_TIMEOUT),
        )
        options = ClusterOptions(
            authenticator=PasswordAuthenticator(
                kwargs["username"], kwargs["password"], cert_path=cert_path
            ),
            timeout_options=timeout,
        )
        self.cluster = Cluster(connection_string=connection_string, options=options)
        self.bucket_name = kwargs['bucket']
        self.bucket = None
        self.collections = dict()
        self.collection = None

    def create(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_create(*args[1:], **kwargs)

    @quiet
    @backoff
    def do_create(self, *args, **kwargs):
        super().do_create(*args, **kwargs)

    def create_durable(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_create_durable(*args[1:], **kwargs)

    @quiet
    @backoff
    def do_create_durable(self, *args, **kwargs):
        super().do_create_durable(*args, **kwargs)

    def read(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_read(*args[1:], **kwargs)

    def get(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_get(*args[1:], **kwargs)

    def do_get(self, *args, **kwargs):
        return super().do_read(*args, **kwargs)

    @time_all
    def do_read(self, *args, **kwargs):
        super().do_read(*args, **kwargs)

    def set(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_update(*args[1:], **kwargs)

    def do_set(self, *args, **kwargs):
        return super().do_update(*args, **kwargs)

    @time_all
    def do_update(self, *args, **kwargs):
        super().do_update(*args, **kwargs)

    def update_durable(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_update_durable(*args[1:], **kwargs)

    @time_all
    def do_update_durable(self, *args, **kwargs):
        super().do_update_durable(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self.collection = self.collections[args[0]]
        return self.do_delete(*args[1:], **kwargs)

    @quiet
    def do_delete(self, *args, **kwargs):
        super().do_delete(*args, **kwargs)

    @timeit
    def view_query(self, ddoc: str, view: str, query: ViewQuery):
        tuple(self.cluster.view_query(ddoc, view, query=query))

    @quiet
    @timeit
    def n1ql_query(self, n1ql_query: str, options: QueryOptions):
        libc = CDLL("libc.so.6")
        libc.srand(int(time.time_ns()))
        tuple(self.cluster.query(n1ql_query, options))

    def create_user_manager(self):
        self.user_manager = self.cluster.users()

    def create_collection_manager(self):
        self.collection_manager = self.cluster.bucket(self.bucket_name).collections()

    @quiet
    @backoff
    def do_upsert_user(self, *args, **kwargs):
        return self.user_manager.upsert_user(User(username=args[0],
                                                  roles=args[1],
                                                  password=args[2]))

    def get_roles(self):
        return self.user_manager.get_roles()

    def do_collection_create(self, *args, **kwargs):
        self.collection_manager.create_collection(
            CollectionSpec(scope_name=args[0],
                           collection_name=args[1]))

    def do_collection_drop(self, *args, **kwargs):
        self.collection_manager.drop_collection(
            CollectionSpec(scope_name=args[0],
                           collection_name=args[1]))

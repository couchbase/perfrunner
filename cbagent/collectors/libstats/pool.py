from queue import Empty, Queue
from threading import Lock
from time import time

import pkg_resources

sdk_major_version = int(pkg_resources.get_distribution("couchbase").version[0])
if sdk_major_version == 3:
    from datetime import timedelta

    from couchbase.cluster import (
        Cluster,
        ClusterOptions,
        ClusterTimeoutOptions,
    )
    from couchbase_core.cluster import PasswordAuthenticator
elif sdk_major_version == 4:
    from datetime import timedelta

    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions, ClusterTimeoutOptions
else:
    from couchbase.bucket import Bucket


class ClientUnavailableError(Exception):
    pass


class BucketWrapper:

    TIMEOUT = 120

    def __init__(self, host, bucket, password, quiet=True, port=8091):
        connection_string = 'couchbase://{}:{}/{}?password={}'\
            .format(host, port, bucket, password)
        self.client = Bucket(connection_string=connection_string, quiet=quiet)
        self.client.timeout = self.TIMEOUT
        self.use_count = 0
        self.use_time = 0
        self.last_use_time = 0

    def start_using(self):
        self.last_use_time = time()

    def stop_using(self):
        self.use_time += time() - self.last_use_time
        self.use_count += 1

    def query(self, ddoc, view, key):
        return self.client.query(ddoc, view, key=key)

    def set(self, key, doc):
        self.client.set(key, doc)

    def delete(self, key):
        self.client.delete(key)

    def upsert(self, key, doc):
        self.client.upsert(key, doc)

    def endure(self, key, persist_to, replicate_to, interval, timeout=120):
        self.client.endure(key,
                           persist_to=persist_to,
                           replicate_to=replicate_to,
                           interval=interval,
                           timeout=timeout)


class CollectionsWrapper:

    TIMEOUT = 120

    def __init__(self, host, bucket, username, password, quiet=True, port=8091):
        connection_string = 'couchbase://{}?password={}'.format(host, password)
        pass_auth = PasswordAuthenticator(username, password)
        timeout = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=self.TIMEOUT))
        options = ClusterOptions(authenticator=pass_auth, timeout_options=timeout)
        self.cluster = Cluster(connection_string=connection_string, options=options)
        self.bucket = self.cluster.bucket(bucket)
        self.client = self.bucket.default_collection()
        self.use_count = 0
        self.use_time = 0
        self.last_use_time = 0

    def start_using(self):
        self.last_use_time = time()

    def stop_using(self):
        self.use_time += time() - self.last_use_time
        self.use_count += 1

    def query(self, ddoc, view, key):
        return self.cluster.view_query(ddoc, view, key=key)

    def set(self, key, doc):
        self.client.insert(key, doc)

    def delete(self, key):
        self.client.remove(key)


class Pool:

    def __init__(self, bucket, host, username, password, collections=None,
                 initial=10, max_clients=20, quiet=True, port=8091):
        self.host = host
        self.port = port
        self.bucket = bucket
        self.collections = collections
        self.username = username
        self.password = password
        self.quiet = quiet
        self._q = Queue()
        self._l = []
        self._cur_clients = 0
        self._max_clients = max_clients
        self._lock = Lock()

        for x in range(initial):
            self._q.put(self._make_client())
            self._cur_clients += 1

    def _make_client(self):
        if self.collections or sdk_major_version >= 3:
            client = CollectionsWrapper(
                self.host, self.bucket, self.username,
                self.password, self.quiet, self.port
            )
        else:
            client = BucketWrapper(
                self.host, self.bucket, self.password,
                self.quiet, self.port
            )
        self._l.append(client)
        return client

    def get_client(self, initial_timeout=0.05, next_timeout=200):
        try:
            return self._q.get(True, initial_timeout)
        except Empty:
            try:
                self._lock.acquire()
                if self._cur_clients == self._max_clients:
                    raise ClientUnavailableError("Too many clients in use")
                cb = self._make_client()
                self._cur_clients += 1
                cb.start_using()
                return cb
            except ClientUnavailableError as ex:
                try:
                    return self._q.get(True, next_timeout)
                except Empty:
                    raise ex
            finally:
                self._lock.release()

    def release_client(self, cb):
        cb.stop_using()
        self._q.put(cb, True)

from threading import Lock
from time import time
from Queue import Queue, Empty

from couchbase.connection import Connection


class ClientUnavailableError(Exception):
    pass


class ConnectionWrapper(Connection):

    def __init__(self, **kwargs):
        super(ConnectionWrapper, self).__init__(**kwargs)
        self.use_count = 0
        self.use_time = 0
        self.last_use_time = 0

    def start_using(self):
        self.last_use_time = time()

    def stop_using(self):
        self.use_time += time() - self.last_use_time
        self.use_count += 1


class Pool(object):

    def __init__(self, initial=10, max_clients=20, **connargs):
        self._q = Queue()
        self._l = []
        self._connargs = connargs
        self._cur_clients = 0
        self._max_clients = max_clients
        self._lock = Lock()

        for x in range(initial):
            self._q.put(self._make_client())
            self._cur_clients += 1

    def _make_client(self):
        ret = ConnectionWrapper(**self._connargs)
        self._l.append(ret)
        return ret

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

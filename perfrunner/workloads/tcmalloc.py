import random
from hashlib import md5

import pkg_resources

from logger import logger

sdk_major_version = int(pkg_resources.get_distribution("couchbase").version[0])
if sdk_major_version == 2:
    from couchbase import experimental
    from twisted.internet import reactor
    from txcouchbase.connection import Connection
    experimental.enable()
elif sdk_major_version == 3:
    from couchbase.cluster import ClusterOptions
    from couchbase_core.cluster import PasswordAuthenticator
    from twisted.internet import reactor
    from txcouchbase.cluster import TxCluster
elif sdk_major_version == 4:
    import txcouchbase  # noqa: F401
    from couchbase.auth import PasswordAuthenticator
    from couchbase.options import ClusterOptions
    from twisted.internet import reactor
    from txcouchbase.cluster import TxCluster


class SmallIterator:

    FIXED_KEY_WIDTH = 12
    RND_FIELD_SIZE = 16

    def __iter__(self):
        return self

    def _id(self, i):
        return '{}'.format(i).zfill(self.FIXED_KEY_WIDTH)

    def _key(self, _id):
        return 'AB_{}_0'.format(_id)

    def _field(self, _id):
        _id = _id.encode('utf-8')
        data = md5(_id).hexdigest()[:16]
        return {'pn': str(_id), 'nam': 'ViberPhone_{}'.format(data)}


class KeyValueIterator(SmallIterator):

    MIN_FIELDS = 11
    MAX_FIELDS = 22
    BATCH_SIZE = 100

    def __init__(self, num_items):
        self.num_items = num_items

    def _value(self, _id):
        return [
            self._field(_id)
            for _ in range(random.randint(self.MIN_FIELDS, self.MAX_FIELDS))
        ]

    def next(self):
        if self.num_items > 0:
            batch = []
            for _ in range(self.BATCH_SIZE):
                _id = self._id(self.num_items)
                self.num_items -= 1
                batch.append((self._key(_id), self._value(_id)))
            return batch
        else:
            raise StopIteration


class NewFieldIterator(SmallIterator):

    ACTIVE_RECORDS = 0.80
    APPEND_SET = 0.75
    BATCH_SIZE = 100

    def __init__(self, num_items):
        self.rnd_num_items = int(self.APPEND_SET * num_items)
        self.num_items = int(self.ACTIVE_RECORDS * num_items)

    def next(self):
        if self.rnd_num_items > 0:
            batch = []
            for _ in range(self.BATCH_SIZE):
                _id = self._id(random.randint(1, self.num_items))
                field = self._field(_id)
                self.rnd_num_items -= 1
                batch.append((self._key(_id), field))
            return batch
        else:
            raise StopIteration


class LargeIterator(SmallIterator):

    FIELD_SIZE = 102400

    def _key(self, _id):
        return '{}'.format(_id)

    def _field(self, _id):
        rev_id = _id[::-1].encode('utf-8')
        _id = _id.encode('utf-8')
        alphabet = md5(_id).hexdigest() + md5(rev_id).hexdigest()  # 64 bytes
        field = [alphabet for _ in range(int(self.FIELD_SIZE / len(alphabet)))]
        return {'f': ''.join(field)}


class KeyLargeValueIterator(LargeIterator, KeyValueIterator):

    MIN_FIELDS = 1
    MAX_FIELDS = 4


class NewLargeFieldIterator(LargeIterator, NewFieldIterator):

    pass


class WorkloadGen:

    NUM_ITERATIONS = 5

    def __init__(self, num_items, host, bucket, password, collections=None, small=True):
        if collections:
            self.use_collection = True
        else:
            self.use_collection = False
        self.num_items = num_items
        if small:
            self.kv_cls = KeyValueIterator
            self.field_cls = NewFieldIterator
        else:
            self.kv_cls = KeyLargeValueIterator
            self.field_cls = NewLargeFieldIterator
        self.kv_iterator = self.kv_cls(self.num_items)
        self.field_iterator = self.field_cls(self.num_items)

        sdk_major_version = int(pkg_resources.get_distribution("couchbase").version[0])
        if sdk_major_version == 2:
            self.cb = Connection(bucket=bucket, host=host, password=password)
        elif sdk_major_version >= 3:
            connection_string = 'couchbase://{host}?password={password}'
            connection_string = connection_string.format(host=host,
                                                         password=password)
            pass_auth = PasswordAuthenticator(bucket, password)
            self.cluster = TxCluster(connection_string, ClusterOptions(pass_auth))
            self.bucket = self.cluster.bucket(bucket)
            self.collection = self.bucket.scope("scope-1").collection("collection-1")

        self.fraction = 1
        self.iteration = 0

    def _interrupt(self, err):
        logger.interrupt(err.value)

    def _on_set(self, *args):
        self.counter += 1
        if self.counter == self.kv_cls.BATCH_SIZE:
            self._set()

    def _set(self, *args):
        self.counter = 0
        try:
            for k, v in self.kv_iterator.next():
                if self.use_collection:
                    d = self.collection.upsert(k, v)
                    d.addCallback(self._on_set)
                    d.addErrback(self._interrupt)
                else:
                    d = self.cb.set(k, v)
                    d.addCallback(self._on_set)
                    d.addErrback(self._interrupt)
        except StopIteration:
            logger.info('Started iteration: {}-{}'.format(self.iteration,
                                                          self.fraction))
            self._append()

    def run(self):
        if self.use_collection:
            logger.info('Running initial load: {} items per collection'.format(self.num_items))
            d = self.bucket.on_connect()
            d.addCallback(self._set)
            d.addErrback(self._interrupt)
        else:
            logger.info('Running initial load: {} items'.format(self.num_items))
            d = self.cb.connect()
            d.addCallback(self._set)
            d.addErrback(self._interrupt)

        reactor.run()

    def _on_append(self, *args):
        self.counter += 1
        if self.counter == self.field_cls.BATCH_SIZE:
            self._append()

    def _on_get(self, rv, f, key=None):
        if self.use_collection:
            if sdk_major_version == 4:
                v = rv.content_as[lambda x: x]
            else:
                v = rv.content
            v.append(f)
            d = self.collection.upsert(key, v)
            d.addCallback(self._on_append)
            d.addErrback(self._interrupt)
        else:
            v = rv.value
            v.append(f)
            d = self.cb.set(rv.key, v)
            d.addCallback(self._on_append)
            d.addErrback(self._interrupt)

    def _append(self, *args):
        self.counter = 0
        try:
            for k, f in self.field_iterator.next():
                if self.use_collection:
                    d = self.collection.get(k)
                    d.addCallback(self._on_get, f, k)
                    d.addErrback(self._interrupt)
                else:
                    d = self.cb.get(k)
                    d.addCallback(self._on_get, f)
                    d.addErrback(self._interrupt)
        except StopIteration:
            logger.info('Finished iteration: {}-{}'.format(self.iteration,
                                                           self.fraction))
            if self.fraction == 4:
                num_items = self.num_items
                self.fraction = 1
                self.iteration += 1
                if self.iteration == self.NUM_ITERATIONS:
                    reactor.stop()
            else:
                self.fraction *= 2
                num_items = self.num_items / self.fraction
            self.field_iterator = self.field_cls(num_items)
            logger.info('Started iteration: {}-{}'.format(self.iteration,
                                                          self.fraction))
            self._append()

import random
import string

from couchbase import Couchbase
from logger import logger


class ViberIterator(object):

    FIXED_KEY_WIDTH = 12
    RND_FIELD_SIZE = 16
    ALPHABET = string.letters + string.digits

    def _id(self, i):
        return '{}'.format(i).zfill(self.FIXED_KEY_WIDTH)

    def _key(self, _id):
        return 'AB_{0}_0'.format(_id)

    def _field(self, _id):
        data = ''.join(random.sample(self.ALPHABET, self.RND_FIELD_SIZE))
        return {'pn': _id, 'nam': 'ViberPhone_{}'.format(data)}


class KeyValueIterator(ViberIterator):

    MIN_FIELDS = 11
    MAX_FIELDS = 22

    def __init__(self, num_items):
        self.num_items = num_items

    def _value(self, _id):
        return [
            self._field(_id)
            for _ in range(random.randint(self.MIN_FIELDS, self.MAX_FIELDS))
        ]

    def __iter__(self):
        for i in range(self.num_items):
            _id = self._id(i)
            yield self._key(_id), self._value(_id)


class NewFieldIterator(ViberIterator):

    WORKING_SET = 0.5  # 50%
    APPEND_SET = 0.15  # 15%

    def __init__(self, num_items):
        self.rnd_num_items = int(self.APPEND_SET * num_items)
        self.num_items = int(self.WORKING_SET * num_items)

    def __iter__(self):
        for _ in range(self.rnd_num_items):
            _id = self._id(random.randint(0, self.num_items))
            field = self._field(_id)
            yield self._key(_id), field


class WorkloadGen(object):

    NUM_ITERATIONS = 30

    def __init__(self, num_items, host, bucket, password):
        self.c = Couchbase.connect(bucket=bucket, host=host, password=password)
        self.num_items = num_items

    def initial_load(self):
        logger.info('Running initial load: {} items'.format(self.num_items))
        for k, v in KeyValueIterator(self.num_items):
            self.c.set(k, v)

    def fragmentation(self):
        for i in range(self.NUM_ITERATIONS):
            logger.info('Running append iteration: {}'.format(i))
            for k, f in NewFieldIterator(self.num_items):
                v = self.c.get(k).value
                v.append(f)
                self.c.set(k, v)

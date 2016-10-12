import math
import random
import time
from hashlib import md5
from itertools import cycle

import numpy as np

from fastdocgen import build_achievements
from spring.states import NUM_STATES, STATES

ASCII_A_OFFSET = 97


class Iterator(object):

    def __init__(self):
        self.prefix = None

    def __iter__(self):
        return self

    def add_prefix(self, key):
        if self.prefix:
            return '%s-%s' % (self.prefix, key)
        else:
            return key


class ExistingKey(Iterator):

    def __init__(self, working_set, working_set_access, prefix):
        self.working_set = working_set
        self.working_set_access = working_set_access
        self.prefix = prefix

    def next(self, curr_items, curr_deletes):
        num_existing_items = curr_items - curr_deletes
        num_hot_items = int(num_existing_items * self.working_set / 100.0)
        num_cold_items = num_existing_items - num_hot_items

        left_limit = 1 + curr_deletes
        if self.working_set_access == 100 or \
                random.randint(0, 100) <= self.working_set_access:
            left_limit += num_cold_items
            right_limit = curr_items
        else:
            right_limit = left_limit + num_cold_items
        key = np.random.random_integers(left_limit, right_limit)
        key = '%012d' % key
        return self.add_prefix(key)


class SequentialHotKey(Iterator):

    def __init__(self, sid, ws, prefix):
        self.sid = sid
        self.ws = ws
        self.prefix = prefix

    def __iter__(self):
        num_hot_keys = int(self.ws.items * self.ws.working_set / 100.0)
        num_cold_items = self.ws.items - num_hot_keys

        for seq_id in xrange(1 + num_cold_items + self.sid,
                             1 + self.ws.items,
                             self.ws.workers):
            key = '%012d' % seq_id
            key = self.add_prefix(key)
            yield key


class NewKey(Iterator):

    def __init__(self, prefix, expiration):
        self.prefix = prefix
        self.expiration = expiration
        self.ttls = cycle(range(150, 450, 30))

    def next(self, curr_items):
        key = '%012d' % curr_items
        key = self.add_prefix(key)
        ttl = None
        if self.expiration and random.randint(1, 100) <= self.expiration:
            ttl = self.ttls.next()
        return key, ttl


class KeyForRemoval(Iterator):

    def __init__(self, prefix):
        self.prefix = prefix

    def next(self, curr_deletes):
        key = '%012d' % curr_deletes
        return self.add_prefix(key)


class KeyForCASUpdate(Iterator):

    def __init__(self, total_workers, working_set, working_set_access, prefix):
        self.n1ql_workers = total_workers
        self.working_set = working_set
        self.working_set_access = working_set_access
        self.prefix = prefix

    def next(self, sid, curr_items, curr_deletes):
        num_existing_items = curr_items - curr_deletes
        num_hot_items = int(num_existing_items * self.working_set / 100.0)
        num_cold_items = num_existing_items - num_hot_items

        left_limit = 1 + curr_deletes
        if self.working_set_access == 100 or \
                random.randint(0, 100) <= self.working_set_access:
            left_limit += num_cold_items
            right_limit = curr_items
        else:
            right_limit = left_limit + num_cold_items
        limit_step = (right_limit - left_limit) / self.n1ql_workers
        left_limit = left_limit + (limit_step) * sid
        right_limit = left_limit + limit_step - 1
        key = np.random.random_integers(left_limit, right_limit)
        key = '%012d' % key
        return self.add_prefix(key)


class Document(Iterator):

    SIZE_VARIATION = 0.25  # 25%

    OVERHEAD = 225  # Minimum size due to static fields, body size is variable

    def __init__(self, avg_size):
        self.avg_size = avg_size

    @classmethod
    def _get_variation_coeff(cls):
        return np.random.uniform(1 - cls.SIZE_VARIATION, 1 + cls.SIZE_VARIATION)

    @staticmethod
    def _build_alphabet(key):
        return md5(key).hexdigest() + md5(key[::-1]).hexdigest()

    @staticmethod
    def _build_name(alphabet):
        return '%s %s' % (alphabet[:6], alphabet[6:12])  # % is faster than format()

    @staticmethod
    def _build_email(alphabet, *args):
        return '%s@%s.com' % (alphabet[12:18], alphabet[18:24])

    @staticmethod
    def _build_alt_email(alphabet):
        name = random.randint(1, 9)
        domain = random.randint(12, 18)
        return '%s@%s.com' % (alphabet[name:name + 6], alphabet[domain:domain + 6])

    @staticmethod
    def _build_city(alphabet):
        return alphabet[24:30]

    @staticmethod
    def _build_realm(alphabet):
        return alphabet[30:36]

    @staticmethod
    def _build_country(alphabet):
        return alphabet[42:48]

    @staticmethod
    def _build_county(alphabet):
        return alphabet[48:54]

    @staticmethod
    def _build_street(alphabet):
        return alphabet[54:62]

    @staticmethod
    def _build_coins(alphabet):
        return max(0.1, int(alphabet[36:40], 16) / 100.0)

    @staticmethod
    def _build_gmtime(alphabet):
        seconds = 396 * 24 * 3600 * (int(alphabet[63], 16) % 12)
        return tuple(time.gmtime(seconds))

    @staticmethod
    def _build_year(alphabet):
        return 1985 + int(alphabet[62], 16)

    @staticmethod
    def _build_state(alphabet):
        idx = alphabet.find('7') % NUM_STATES
        return STATES[idx][0]

    @staticmethod
    def _build_full_state(alphabet):
        idx = alphabet.find('8') % NUM_STATES
        return STATES[idx][1]

    @staticmethod
    def _build_category(alphabet):
        return int(alphabet[41], 16) % 3

    @staticmethod
    def _build_achievements(alphabet, key=None):
        return build_achievements(alphabet) or [0]

    @staticmethod
    def _build_body(alphabet, length):
        length_int = int(length)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        return body[:length_int]

    def _size(self):
        if self.avg_size <= self.OVERHEAD:
            return 0
        return self._get_variation_coeff() * (self.avg_size - self.OVERHEAD)

    def next(self, key):
        alphabet = self._build_alphabet(key)
        size = self._size()
        return {
            'name': self._build_name(alphabet),
            'email': self._build_email(alphabet),
            'alt_email': self._build_alt_email(alphabet),
            'city': self._build_city(alphabet),
            'realm': self._build_realm(alphabet),
            'coins': self._build_coins(alphabet),
            'category': self._build_category(alphabet),
            'achievements': self._build_achievements(alphabet, key),
            'body': self._build_body(alphabet, size),
        }


class NestedDocument(Document):

    OVERHEAD = 450  # Minimum size due to static fields, body size is variable

    def __init__(self, avg_size):
        super(NestedDocument, self).__init__(avg_size)
        self.capped_field_value = {}

    def _size(self):
        if self.avg_size <= self.OVERHEAD:
            return 0
        if random.random() < 0.975:  # Normal distribution, mean=self.avg_size
            normal = np.random.normal(loc=1.0, scale=0.17)
            return (self.avg_size - self.OVERHEAD) * normal
        else:  # Outliers - beta distribution, 2KB-2MB range
            return 2048 / np.random.beta(a=2.2, b=1.0)

    def next(self, key):
        alphabet = self._build_alphabet(key)
        size = self._size()
        return {
            'name': {'f': {'f': {'f': self._build_name(alphabet)}}},
            'email': {'f': {'f': self._build_email(alphabet)}},
            'alt_email': {'f': {'f': self._build_alt_email(alphabet)}},
            'street': {'f': {'f': self._build_street(alphabet)}},
            'city': {'f': {'f': self._build_city(alphabet)}},
            'county': {'f': {'f': self._build_county(alphabet)}},
            'state': {'f': self._build_state(alphabet)},
            'full_state': {'f': self._build_full_state(alphabet)},
            'country': {'f': self._build_country(alphabet)},
            'realm': {'f': self._build_realm(alphabet)},
            'coins': {'f': self._build_coins(alphabet)},
            'category': self._build_category(alphabet),
            'achievements': self._build_achievements(alphabet),
            'gmtime': self._build_gmtime(alphabet),
            'year': self._build_year(alphabet),
            'body': self._build_body(alphabet, size),
        }


class LargeDocument(NestedDocument):

    def next(self, key):
        alphabet = self._build_alphabet(key)
        return {
            'nest1': super(LargeDocument, self).next(key),
            'nest2': super(NestedDocument, self).next(key),
            'name': self._build_name(alphabet),
            'email': self._build_email(alphabet),
            'alt_email': self._build_alt_email(alphabet),
            'city': self._build_city(alphabet),
            'realm': self._build_realm(alphabet),
            'coins': self._build_coins(alphabet),
            'category': self._build_category(alphabet),
            'achievements': self._build_achievements(alphabet),
        }


class ReverseLookupDocument(NestedDocument):

    def __init__(self, avg_size, partitions, is_random=True):
        super(ReverseLookupDocument, self).__init__(avg_size)
        self.partitions = partitions
        self.is_random = is_random

    def build_email(self, alphabet):
        if self.is_random:
            return self._build_alt_email(alphabet)
        else:
            return self._build_email(alphabet)

    def _build_partition(self, alphabet, seq_id):
        return seq_id % self.partitions

    def _capped_field(self, alphabet, prefix, seq_id, num_unique):
        if self.is_random:
            offset = random.randint(1, 9)
            return '%s' % alphabet[offset:offset + 6]

        index = (seq_id % self.partitions) + \
            self.partitions * (seq_id / (self.partitions * num_unique))
        return '%s_%s_%s' % (prefix, num_unique, index)

    def next(self, key):
        seq_id = int(key[-12:]) + 1
        prefix = key[:-12]
        alphabet = self._build_alphabet(key)
        size = self._size()
        return {
            'name': self._build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self._build_alt_email(alphabet),
            'street': self._build_street(alphabet),
            'city': self._build_city(alphabet),
            'county': self._build_county(alphabet),
            'state': self._build_state(alphabet),
            'full_state': self._build_full_state(alphabet),
            'country': self._build_country(alphabet),
            'realm': self._build_realm(alphabet),
            'coins': self._build_coins(alphabet),
            'category': self._build_category(alphabet),
            'achievements': self._build_achievements(alphabet, key),
            'gmtime': self._build_gmtime(alphabet),
            'year': self._build_year(alphabet),
            'body': self._build_body(alphabet, size),
            'capped_small': self._capped_field(alphabet, prefix, seq_id, 100),
            'partition_id': self._build_partition(alphabet, seq_id),
        }


class ArrayIndexingDocument(ReverseLookupDocument):

    num_docs = 0
    delta = 0

    def __init__(self, avg_size, partitions, num_docs, delta=0):
        super(ArrayIndexingDocument, self).__init__(avg_size, partitions)
        ArrayIndexingDocument.num_docs = num_docs
        ArrayIndexingDocument.delta = delta

    @staticmethod
    def _build_achievements1(alphabet, key):
        spl = key.split('-')
        if spl[0] == 'n1ql':
            # these docs are never updated
            return [((int(spl[1].lstrip('0')) - 1) * 10 + i +
                     ArrayIndexingDocument.delta) for i in range(10)]
        else:
            # these docs are involved in updating
            return [((int(spl[1].lstrip('0')) - 1) * 10 + i +
                     ArrayIndexingDocument.num_docs * 10 +
                     ArrayIndexingDocument.delta) for i in range(10)]

    @staticmethod
    def _build_achievements2(alphabet, key):
        spl = key.split('-')
        if spl[0] == 'n1ql':
            # these docs are never updated
            return [((int(spl[1].lstrip('0')) // 100) * 10 + i +
                     ArrayIndexingDocument.delta) for i in range(10)]
        else:
            # these docs are involved in updating
            return [((int(spl[1].lstrip('0')) // 100) * 10 + i +
                     ArrayIndexingDocument.num_docs * 10 +
                     ArrayIndexingDocument.delta) for i in range(10)]

    def next(self, key):
        seq_id = int(key[-12:]) + 1
        prefix = key[:-12]
        alphabet = self._build_alphabet(key)
        size = self._size()

        return {
            'name': self._build_name(alphabet),
            'email': self._build_email(alphabet),
            'alt_email': self._build_alt_email(alphabet),
            'street': self._build_street(alphabet),
            'city': self._build_city(alphabet),
            'county': self._build_county(alphabet),
            'state': self._build_state(alphabet),
            'full_state': self._build_full_state(alphabet),
            'country': self._build_country(alphabet),
            'realm': self._build_realm(alphabet),
            'coins': self._build_coins(alphabet),
            'category': self._build_category(alphabet),
            'achievements1': self._build_achievements1(alphabet, key),
            'achievements2': self._build_achievements2(alphabet, key),
            'gmtime': self._build_gmtime(alphabet),
            'year': self._build_year(alphabet),
            'body': self._build_body(alphabet, size),
            'capped_small': self._capped_field(alphabet, prefix, seq_id, 100),
            'partition_id': self._build_partition(alphabet, seq_id),
        }

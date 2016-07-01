import array
import math
import time
from hashlib import md5
from itertools import cycle
import random
import json

import numpy as np

from spring.states import STATES, NUM_STATES
from fastdocgen import build_achievements

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
        limit_step = (right_limit - left_limit)/self.n1ql_workers
        left_limit = left_limit +(limit_step)*sid
        right_limit = left_limit + limit_step -1
        key = np.random.random_integers(left_limit, right_limit)
        key = '%012d' % key
        return self.add_prefix(key)


class NewDocument(Iterator):

    SIZE_VARIATION = 0.25  # 25%
    STATIC_PART_SIZE = None

    def __init__(self, avg_size, extra_fields=False):
        self.avg_size = avg_size
        self.extra_fields = extra_fields

    @classmethod
    def _get_variation_coeff(cls):
        return np.random.uniform(1 - cls.SIZE_VARIATION,
                                 1 + cls.SIZE_VARIATION)

    @staticmethod
    def _build_alphabet(key):
        return md5(key).hexdigest() + md5(key[::-1]).hexdigest()

    @staticmethod
    def _build_name(alphabet):
        return '%s %s' % (alphabet[:6], alphabet[6:12])

    @staticmethod
    def _build_email(alphabet):
        return '%s@%s.com' % (alphabet[12:18], alphabet[18:24])

    @staticmethod
    def _build_alt_email(alphabet):
        name = random.randint(1,9)
        domain = random.randint(12,18)
        return '%s@%s.com' % (alphabet[name:name+6], alphabet[domain:domain+6])

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

    @staticmethod
    def _build_extras(alphabet, length):
        return alphabet[0:length]

    def _build_doc(self, alphabet, body_length, key=None):
        if not self.extra_fields:
            return {
                'name': self._build_name(alphabet),
                'email': self._build_email(alphabet),
                'alt_email': self._build_alt_email(alphabet),
                'city': self._build_city(alphabet),
                'realm': self._build_realm(alphabet),
                'coins': self._build_coins(alphabet),
                'category': self._build_category(alphabet),
                'achievements': self._build_achievements(alphabet, key),
                'body': self._build_body(alphabet, body_length)
            }
        else:
            return {
                'name': self._build_name(alphabet),
                'email': self._build_email(alphabet),
                'alt_email': self._build_alt_email(alphabet),
                'city': self._build_city(alphabet),
                'realm': self._build_realm(alphabet),
                'coins': self._build_coins(alphabet),
                'category': self._build_category(alphabet),
                'achievements': self._build_achievements(alphabet),
                'body': self._build_body(alphabet, body_length),
                'extras1': self._build_extras(alphabet, 50),
                'extras2': self._build_extras(alphabet, 60),
                'extras3': self._build_extras(alphabet, 70),
                'extras4': self._build_extras(alphabet, 80),
                'extras5': self._build_extras(alphabet, 90)
            }

    def next(self, key):
        if self.STATIC_PART_SIZE is None:
            alphabet = self._build_alphabet(key)
            self.STATIC_PART_SIZE = len(
                json.dumps(self._build_doc(alphabet, 0)))
        # numpy.random.uniform includes low, but excludes high [low, high)
        # that's why we use '- 2' to calc size. It works for the cases
        # when the number of docs rather large > 10^5
        body_field_length = self._get_variation_coeff() * (
            self.avg_size - self.STATIC_PART_SIZE - 2)
        alphabet = self._build_alphabet(key)
        return self._build_doc(alphabet, body_field_length)


class NewNestedDocument(NewDocument):

    OVERHEAD = 450  # Minimum size due to fixed fields, body size is variable

    def __init__(self, avg_size):
        super(NewNestedDocument, self).__init__(avg_size)
        self.capped_field_value = {}

    def _size(self):
        if self.avg_size <= self.OVERHEAD:
            return 0
        if random.random() < 0.975:
            # Normal distribution with mean=self.avg_size
            normal = np.random.normal(loc=1.0, scale=0.17)
            return (self.avg_size - self.OVERHEAD) * normal
        else:
            # Beta distribution, 2KB-2MB range
            return 2048 / np.random.beta(a=2.2, b=1.0)

    def _capped_field(self, key, num_unique):
        # Assumes the last 12 characters are digits and
        # monotonically increasing
        try:
            index = (int(key[-12:]) + 1) / num_unique
            return '{}_{}'.format(num_unique, index)
        except Exception:
            return 'Invalid Key for capped field'

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
            'capped_small': self._capped_field(key, 100),
            'capped_large': self._capped_field(key, 3000),
        }


class NewLargeDocument(NewNestedDocument):
    def next(self, key):
        alphabet = self._build_alphabet(key)
        nest1 = super(NewLargeDocument, self).next(key)
        nest2 = super(NewNestedDocument, self).next(key)
        return {'nest1': nest1,
                'nest2': nest2,
                'name': self._build_name(alphabet),
                'email': self._build_email(alphabet),
                'alt_email': self._build_alt_email(alphabet),
                'city': self._build_city(alphabet),
                'realm': self._build_realm(alphabet),
                'coins': self._build_coins(alphabet),
                'category': self._build_category(alphabet),
                'achievements': self._build_achievements(alphabet)
                }


class NewDocumentFromSpatialFile(object):
    """The documents will contain one property per dimension.

    The first dimension are names with characters starting with `a`.

    The loader needs to know how many workers there are, so that every
    n-th entry (depending on the number of workers) can be read. The
    ID of the current worker will be set when the worker is started.
    """
    # The size (in byte) one dimension takes. It's min and max, both 64-bit
    # floats
    DIM_SIZE = 16

    def __init__(self, filename, dim):
        self.file = open(filename, 'rb')
        self.dim = dim
        self.record_size = dim * self.DIM_SIZE
        # The offset (number of records) the items should be read from
        # It is set by the worker
        self.offset = 0

    def __del__(self):
        self.file.close()

    def next(self, key):
        self.file.seek(self.record_size * self.offset)
        mbb = array.array('d')
        mbb.fromfile(self.file, self.dim * 2)
        self.offset += 1
        doc = {}
        for i in range(self.dim):
            doc[chr(ASCII_A_OFFSET + i)] = [mbb[i * 2], mbb[i * 2 + 1]]
        return doc


class ReverseLookupDocument(NewNestedDocument):

    def __init__(self, avg_size, partitions, isRandom=True):
        super(ReverseLookupDocument, self).__init__(avg_size)
        self.partitions = partitions
        self.isRandom = isRandom

    def _build_email(self, alphabet):
        if self.isRandom:
            name = random.randint(1,9)
            domain = random.randint(12,18)
            return '%s@%s.com' % (alphabet[name:name+6], alphabet[domain:domain+6])

        return '%s@%s.com' % (alphabet[12:18], alphabet[18:24])

    def _build_partition(self, alphabet, id):
        return id % self.partitions

    def _capped_field(self, alphabet, prefix, id, num_unique):
        if self.isRandom:
            seed = random.randint(1,9)
            return '%s' % (alphabet[seed:seed+6])

        # Assumes the last 12 characters are digits and
        # monotonically increasing
        try:
            parts = self.partitions
            index = (id % parts) + parts * (id / (parts * num_unique))
            return '{}_{}_{}'.format(prefix, num_unique, index)
        except Exception:
            return 'Invalid Key for capped field'

    def next(self, key):
        id = int(key[-12:]) + 1
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
            'achievements': self._build_achievements(alphabet, key),
            'gmtime': self._build_gmtime(alphabet),
            'year': self._build_year(alphabet),
            'body': self._build_body(alphabet, size),
            'capped_small': self._capped_field(alphabet, prefix, id, 100),
            'partition_id': self._build_partition(alphabet, id)
        }


class ReverseLookupDocumentArrayIndexing(ReverseLookupDocument):
    num_docs = 0
    delta = 0

    def __init__(self, avg_size, partitions, num_docs, delta=0):
        super(ReverseLookupDocumentArrayIndexing, self).__init__(avg_size, partitions)
        ReverseLookupDocumentArrayIndexing.num_docs = num_docs
        ReverseLookupDocumentArrayIndexing.delta = delta

    @staticmethod
    def _build_achievements1(alphabet, key):
        spl = key.split('-')
        if spl[0] == 'n1ql':
            # these docs are never updated
            return [((int(spl[1].lstrip('0')) - 1)*10 + i +
                     ReverseLookupDocumentArrayIndexing.delta) for i in xrange(10)]
        else:
            # these docs are involved in updating
            return [((int(spl[1].lstrip('0')) - 1)*10 + i +
                     ReverseLookupDocumentArrayIndexing.num_docs*10 +
                     ReverseLookupDocumentArrayIndexing.delta) for i in xrange(10)]

    @staticmethod
    def _build_achievements2(alphabet, key):
        spl = key.split('-')
        if spl[0] == 'n1ql':
            # these docs are never updated
            return [((int(spl[1].lstrip('0'))//100)*10 + i +
                     ReverseLookupDocumentArrayIndexing.delta) for i in xrange(10)]
        else:
            # these docs are involved in updating
            return [((int(spl[1].lstrip('0'))//100)*10 + i +
                     ReverseLookupDocumentArrayIndexing.num_docs*10 +
                     ReverseLookupDocumentArrayIndexing.delta) for i in xrange(10)]

    def next(self, key):
        id = int(key[-12:]) + 1
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
            'capped_small': self._capped_field(alphabet, prefix, id, 100),
            'partition_id': self._build_partition(alphabet, id)
        }


class MergeDocument(ReverseLookupDocument):

    def __init__(self, avg_size, partitions, isRandom=True):
        super(MergeDocument, self).__init__(avg_size, partitions, isRandom)

    def next(self, key):
        id = int(key[-12:]) + 1
        prefix = key[:-12]
        alphabet = self._build_alphabet(key)
        size = self._size()

        if(id%100000 == 0):

            return {
                'extramerge':self._build_country(alphabet),
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
                'achievements': self._build_achievements(alphabet),
                'gmtime': self._build_gmtime(alphabet),
                'year': self._build_year(alphabet),
                'body': self._build_body(alphabet, size),
                'capped_small': self._capped_field(alphabet, prefix, id, 100),
                'partition_id': self._build_partition(alphabet, id)
            }

        else:

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
                'achievements': self._build_achievements(alphabet),
                'gmtime': self._build_gmtime(alphabet),
                'year': self._build_year(alphabet),
                'body': self._build_body(alphabet, size),
                'capped_small': self._capped_field(alphabet, prefix, id, 100),
                'partition_id': self._build_partition(alphabet, id)
            }
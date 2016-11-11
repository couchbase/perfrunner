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


class FTSKey(Iterator):

    def __init__(self, items):
        self.mutate_items = items

    def next(self):
        return hex(random.randint(0, self.mutate_items))[2:]


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
    def _build_achievements(alphabet):
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
            'achievements': self._build_achievements(alphabet),
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

    def __init__(self, avg_size, prefix):
        super(ReverseLookupDocument, self).__init__(avg_size)
        self.prefix = prefix
        self.is_random = prefix != 'n1ql'

    def build_email(self, alphabet):
        if self.is_random:
            return self._build_alt_email(alphabet)
        else:
            return self._build_email(alphabet)

    def _build_capped(self, alphabet, seq_id, num_unique):
        if self.is_random:
            offset = random.randint(1, 9)
            return '%s' % alphabet[offset:offset + 6]

        index = seq_id / num_unique
        return '%s_%s_%s' % (self.prefix, num_unique, index)

    def _build_topics(self, seq_id):
        return []

    def next(self, key):
        alphabet = self._build_alphabet(key)
        size = self._size()
        seq_id = int(key[-12:]) + 1

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
            'achievements': self._build_achievements(alphabet),
            'gmtime': self._build_gmtime(alphabet),
            'year': self._build_year(alphabet),
            'body': self._build_body(alphabet, size),
            'capped_small': self._build_capped(alphabet, seq_id, 100),
            'topics': self._build_topics(seq_id),
        }


class ExtReverseLookupDocument(ReverseLookupDocument):

    OVERHEAD = 650

    def __init__(self, avg_size, prefix, num_docs):
        super(ExtReverseLookupDocument, self).__init__(avg_size, prefix)
        self.num_docs = num_docs

    def _build_topics(self, seq_id):
        """1:4 reference to JoinedDocument keys."""
        return [
            self.add_prefix('%012d' % ((seq_id + 11) % self.num_docs)),
            self.add_prefix('%012d' % ((seq_id + 19) % self.num_docs)),
            self.add_prefix('%012d' % ((seq_id + 23) % self.num_docs)),
            self.add_prefix('%012d' % ((seq_id + 29) % self.num_docs)),
        ]


class JoinedDocument(ReverseLookupDocument):

    def __init__(self, avg_size, prefix, num_docs, num_categories, num_replies):
        super(JoinedDocument, self).__init__(avg_size, prefix)
        self.num_categories = num_categories
        self.num_docs = num_docs
        self.num_replies = num_replies

    def _build_owner(self, seq_id):
        """4:1 reference to ReverseLookupDocument keys."""
        ref_id = seq_id % (self.num_docs / 4)
        return self.add_prefix('%012d' % ref_id)

    def _build_title(self, alphabet):
        return alphabet[:32]

    def _build_categories(self, seq_id):
        """1:4 reference to RefDocument keys."""
        return [
            self.add_prefix('%012d' % ((seq_id + 11) % self.num_categories)),
            self.add_prefix('%012d' % ((seq_id + 19) % self.num_categories)),
            self.add_prefix('%012d' % ((seq_id + 23) % self.num_categories)),
            self.add_prefix('%012d' % ((seq_id + 29) % self.num_categories)),
        ]

    def _build_user(self, seq_id, idx):
        return self.add_prefix('%012d' % ((seq_id + idx + 537) % self.num_docs))

    def _build_replies(self, seq_id):
        """1:N references to ReverseLookupDocument keys."""
        return [
            {'user': self._build_user(seq_id, idx)}
            for idx in range(self.num_replies)
        ]

    def next(self, key):
        alphabet = self._build_alphabet(key)
        seq_id = int(key[-12:])

        return {
            'owner': self._build_owner(seq_id),
            'title': self._build_title(alphabet),
            'capped_large': self._build_capped(alphabet, seq_id, 1000),
            'categories': self._build_categories(seq_id),
            'replies': self._build_replies(seq_id),
        }


class RefDocument(ReverseLookupDocument):

    def _build_ref_name(self, seq_id):
        return self.add_prefix('%012d' % seq_id)

    def next(self, key):
        seq_id = int(key[-12:])

        return {
            'name': self._build_ref_name(seq_id),
        }


class ArrayIndexingDocument(ReverseLookupDocument):

    """ArrayIndexingDocument extends ReverseLookupDocument by adding two new
    fields achievements1 and achievements2.

    achievements1 is a variable-length array (default length is 10). Every
    instance of achievements1 is unique. This field is useful for single lookups.

    achievements2 is a fixed-length array. Each instance of achievements2 is
    repeated 100 times (ARRAY_CAP). This field is useful for range queries.
    """

    ARRAY_CAP = 100

    ARRAY_SIZE = 10

    def __init__(self, avg_size, prefix, array_size, num_docs):
        super(ArrayIndexingDocument, self).__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def _build_achievements1(self, seq_id):
        """Every document reserves a range of numbers that can be used for a
        new array.

        The left side of range is always based on sequential document ID.

        Random arrays make a few additional steps:
        * The range is shifted by the total number of documents so that static (
        non-random) and random documents do not overlap.
        * The range is doubled so that it's possible vary elements in a new
        array.
        * The left side of range is randomly shifted.

        Here is an example of a new random array for seq_id=7, total 100
        documents and 10 elements in array:
            1) offset is set to 1000.
            2) offset is incremented by 140.
            3) offset is incremented by a random number (e.g., 5).
            4) [1145, 1146, 1147, 1148, 1149, 1150, 1151, 1152, 1153, 1154]
        array is generated.

        Steps for seq_id=8 are the following:
            1) offset is set to 1000.
            2) offset is incremented by 160.
            3) offset is incremented by a random number (e.g., 2).
           4) [1162, 1163, 1164, 1165, 1166, 1167, 1168, 1169, 1170, 1171]
        array is generated.
        """
        offset = seq_id * self.array_size
        if self.is_random:
            offset = self.num_docs * self.array_size
            offset += 2 * seq_id * self.array_size
            offset += random.randint(1, self.array_size)

        return [offset + i for i in range(self.array_size)]

    def _build_achievements2(self, seq_id):
        """achievements2 is very similar to achievements1. However, in case of
        achievements2 ranges overlap so that multiple documents case satisfy the
        same queries. Overlapping is achieving by integer division using
        ARRAY_CAP constant.
        """
        offset = seq_id / self.ARRAY_CAP * self.ARRAY_SIZE
        if self.is_random:
            offset = self.num_docs * self.ARRAY_SIZE
            offset += (2 * seq_id) / self.ARRAY_CAP * self.ARRAY_SIZE
            offset += random.randint(1, self.ARRAY_SIZE)

        return [offset + i for i in range(self.ARRAY_SIZE)]

    def next(self, key):
        alphabet = self._build_alphabet(key)
        size = self._size()
        seq_id = int(key[-12:]) + 1

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
            'achievements1': self._build_achievements1(seq_id),
            'achievements2': self._build_achievements2(seq_id),
            'gmtime': self._build_gmtime(alphabet),
            'year': self._build_year(alphabet),
            'body': self._build_body(alphabet, size),
            'capped_small': self._build_capped(alphabet, seq_id, 100),
            'topics': self._build_topics(seq_id),
        }


class ImportExportDocument(ReverseLookupDocument):

    """ImportExportDocument extends ReverseLookupDocument by adding fields with random size.
    TODO: extend for nested json and arrays.
    """
    OVERHEAD = 1022

    def next(self, key):
        seq_id = int(key[-12:]) + 1
        alphabet = self._build_alphabet(key)
        size = self._size()
        return {
            'name': self._build_name(alphabet) * random.randint(0, 5),
            'email': self.build_email(alphabet) * random.randint(0, 5),
            'alt_email': self._build_alt_email(alphabet) * random.randint(0, 5),
            'street': self._build_street(alphabet) * random.randint(0, 9),
            'city': self._build_city(alphabet) * random.randint(0, 9),
            'county': self._build_county(alphabet) * random.randint(0, 5),
            'state': self._build_state(alphabet) * random.randint(0, 5),
            'full_state': self._build_full_state(alphabet) * random.randint(0, 5),
            'country': self._build_country(alphabet) * random.randint(0, 5),
            'realm': self._build_realm(alphabet) * random.randint(0, 9),
            'alt_street': self._build_street(alphabet) * random.randint(0, 9),
            'alt_city': self._build_city(alphabet) * random.randint(0, 9),
            'alt_county': self._build_county(alphabet) * random.randint(0, 5),
            'alt_state': self._build_state(alphabet) * random.randint(0, 5),
            'alt_full_state': self._build_full_state(alphabet) * random.randint(0, 5),
            'alt_country': self._build_country(alphabet) * random.randint(0, 5),
            'alt_realm': self._build_realm(alphabet) * random.randint(0, 9),
            'coins': self._build_coins(alphabet) * random.randint(0, 999),
            'category': self._build_category(alphabet) * random.randint(0, 5),
            'achievements': self._build_achievements(alphabet),
            'gmtime': self._build_gmtime(alphabet) * random.randint(0, 9),
            'year': self._build_year(alphabet) * random.randint(0, 5),
            'body': self._build_body(alphabet, size),
            'capped_small': self._build_capped(alphabet, seq_id, 100) * random.randint(0, 5),
            'alt_capped_small': self._build_capped(alphabet, seq_id, 100) * random.randint(0, 5),
        }

import hashlib
import json
import math
import random
import time
from datetime import date, datetime, timedelta
from typing import Iterator, List, Tuple

import numpy as np
import spooky
from faker import Faker

from fastdocgen import build_achievements
from perfrunner.workloads.bigfun import query_gen
from spring.dictionary import (
    CATEGORIES,
    COUNTIES,
    EDUCATION_STATUSES,
    GARBAGE,
    GENDERS,
    LOREM,
    MARITAL_STATUSES,
    NUM_STATES,
    NUM_STREET_SUFFIXES,
    PACKAGE_STATUSES,
    STATES,
    STREET_SUFFIX,
    YEARS,
    ZIP_CODES,
)
from spring.settings import WorkloadSettings

PRIME = 4889388631

MAX_PRIME = 25191867719

OFFSET = 25000000000

HASH_LENGTH = 16


def hex_digest(key: str) -> str:
    return '%032x' % spooky.hash128(key)


def hex_digest_64(key: str) -> str:
    return '%032x' % spooky.hash64(key)


def decimal_fmtr(key: int, prefix: str) -> str:
    key = '%012d' % key
    if prefix:
        return '%s-%s' % (prefix, key)
    return key


def hash_fmtr(key: int, prefix: str) -> str:
    key = decimal_fmtr(key, prefix)
    key = hex_digest(key)
    return key[:HASH_LENGTH]


def hex_fmtr(key: int, prefix: str) -> str:
    key = OFFSET + (key * PRIME) % MAX_PRIME  # hash
    key = '%036x' % int(key) ** 4  # int() prevents overflow
    if prefix:
        return '%s-%s' % (prefix, key)
    return key


class Key:

    def __init__(self, number: int, prefix: str, fmtr: str, hit: bool = False):
        self.number = number
        self.prefix = prefix
        self.hit = hit
        self.fmtr = fmtr

    @property
    def string(self) -> str:
        if self.fmtr == 'hash':
            return hash_fmtr(self.number, self.prefix)
        if self.fmtr == 'hex':
            return hex_fmtr(self.number, self.prefix)
        return decimal_fmtr(self.number, self.prefix)


class NewOrderedKey:

    """Generate ordered keys with an optional common prefix.

    These keys are usually used for inserting new documents into the database.

    Example: "38d7cd-000072438963"

    The suffix is a 12 or 43 characters long string consisting of decimal or
    hexadecimal digits.

    This key pattern is rather uncommon in real-world scenarios.
    """

    def __init__(self, prefix: str, fmtr: str):
        self.prefix = prefix
        self.fmtr = fmtr

    def next(self, curr_items: int) -> Key:
        return Key(number=curr_items, prefix=self.prefix, fmtr=self.fmtr)


class KeyForRemoval:

    """Pick an existing key at the beginning of the key space."""

    def __init__(self, prefix: str, fmtr: str):
        self.prefix = prefix
        self.fmtr = fmtr

    def next(self, curr_deletes: int) -> Key:
        return Key(number=curr_deletes, prefix=self.prefix, fmtr=self.fmtr)


class UniformKey:

    """Randomly sample an existing key from the entire key space.

    Sampling uses discrete uniform distribution.

    |<-------------------- key space -------------------->|

    |xxxxxxxxx|...........................................|

              ^                                           ^
              |                                           |

          curr_deletes                                curr_items

    This generator should not be used when the key access pattern is important.
    """

    def __init__(self, prefix: str, fmtr: str):
        self.prefix = prefix
        self.fmtr = fmtr

    def next(self, curr_items: int, curr_deletes: int, *args) -> Key:
        number = random.randrange(curr_deletes, curr_items)
        return Key(number=number, prefix=self.prefix, fmtr=self.fmtr)


class WorkingSetKey:

    """Extend UniformKey by sampling keys from the fixed working set.

    Working set is a subset of the entire key space.

    There are two options that characterize the working set:
    * working_set - a percentage (from 0 to 100) of the entire key space that
    should be considered as the working set.
    * working_set_access - a percentage (from 0 to 100) that defines the
    probability at which the keys from the working set are being used. This
    parameter implements deterministic cache miss ratio.

    |<--------------------------- key space ------------------------->|

            |<----------- cold items ---------->|<---- hot items ---->|

    |xxxxxxx|.........................................................|

            ^                                                         ^
            |                                                         |

        curr_deletes                                              curr_items
    """

    def __init__(self, ws: WorkloadSettings, prefix: str):
        self.num_hot_items = int(ws.items * ws.working_set / 100)
        self.working_set_access = ws.working_set_access
        self.prefix = prefix
        self.fmtr = ws.key_fmtr

    def next(self, curr_items: int, curr_deletes: int, *args) -> Key:
        num_cold_items = curr_items - self.num_hot_items

        if random.randint(0, 100) <= self.working_set_access:  # cache hit
            hit = True
            left_boundary = num_cold_items
            right_boundary = curr_items
        else:  # cache miss
            hit = False
            left_boundary = curr_deletes
            right_boundary = num_cold_items

        number = random.randrange(left_boundary, right_boundary)
        return Key(number=number, prefix=self.prefix, fmtr=self.fmtr, hit=hit)


class MovingWorkingSetKey:

    def __init__(self, ws: WorkloadSettings, prefix: str):
        self.working_set = ws.working_set
        self.working_set_access = ws.working_set_access
        self.working_set_moving_docs = ws.working_set_moving_docs
        self.prefix = prefix
        self.fmtr = ws.key_fmtr

    def next(self, curr_items: int, curr_deletes: int,
             current_hot_load_start: int, timer_elapse: int) -> Key:
        num_existing_items = curr_items - curr_deletes
        num_hot_items = int(num_existing_items * self.working_set / 100)

        if timer_elapse.value:
            from logger import logger
            timer_elapse.value = 0
            # Create next hot_load_start, add working_set_move_docs and then
            # modulus to prevent going beyond num_docs
            num_items = num_existing_items - num_hot_items
            offset = current_hot_load_start.value + self.working_set_moving_docs
            current_hot_load_start.value = int(offset % num_items)
            logger.info("New hotload start {} end {}".format(current_hot_load_start.value,
                        current_hot_load_start.value + num_hot_items))

        left_boundary = curr_deletes + current_hot_load_start.value
        right_boundary = left_boundary + num_hot_items
        number = random.randrange(left_boundary, right_boundary)
        return Key(number=number, prefix=self.prefix, fmtr=self.fmtr)


class ContinuousKey:

    def __init__(self, prefix: str, fmtr: str, alpha: float):
        self.prefix = prefix
        self.fmtr = fmtr
        self.alpha = alpha


class ZipfKey(ContinuousKey):

    def next(self, curr_items: int, curr_deletes: int, *args) -> Key:
        number = curr_items - np.random.zipf(a=self.alpha)
        if number <= curr_deletes:
            number = curr_items - 1
        return Key(number=number, prefix=self.prefix, fmtr=self.fmtr)


class PowerKey(ContinuousKey):

    def next(self, curr_items: int, curr_deletes: int, *args) -> Key:
        r = np.random.power(a=self.alpha)
        number = curr_deletes + int(r * (curr_items - curr_deletes - 1))
        return Key(number=number, prefix=self.prefix, fmtr=self.fmtr)


class SequentialKey:

    """Sequentially generate new keys equally divided the workers.

    SequentialKey equally divides the key space between the workers and
    sequentially iterates over a given part of the key space (based on the
    sequential worker identifier).

    This generator is used for loading data.
    """

    def __init__(self, sid: int, ws: WorkloadSettings, prefix: str):
        self.sid = sid
        self.ws = ws
        self.prefix = prefix

    def __iter__(self) -> Iterator[Key]:
        for seq_id in range(self.sid, self.ws.items, self.ws.workers):
            yield Key(number=seq_id, prefix=self.prefix, fmtr=self.ws.key_fmtr)


class HotKey:

    """Generate the existing keys equally divided between the workers.

    HotKey equally divides the working set between the workers and iterates over
    a given part of the working set (based on the sequential worker identifier).

    This generator is used for warming up the working set.
    """

    def __init__(self, sid: int, ws: WorkloadSettings, prefix: str):
        self.sid = sid
        self.ws = ws
        self.prefix = prefix

    def __iter__(self) -> Iterator[Key]:
        num_hot_keys = int(self.ws.items * self.ws.working_set / 100)
        num_cold_items = self.ws.items - num_hot_keys

        for seq_id in range(num_cold_items + self.sid,
                            self.ws.items,
                            self.ws.workers):
            yield Key(number=seq_id, prefix=self.prefix, fmtr=self.ws.key_fmtr)


class KeyForCASUpdate:

    def __init__(self, total_workers: int, prefix: str, fmtr: str):
        self.n1ql_workers = total_workers
        self.prefix = prefix
        self.fmtr = fmtr

    def next(self, sid: int, curr_items: int) -> Key:
        per_worker_items = curr_items // self.n1ql_workers

        left_boundary = sid * per_worker_items
        right_boundary = left_boundary + per_worker_items

        number = np.random.random_integers(low=left_boundary,
                                           high=right_boundary - 1)
        return Key(number=number, prefix=self.prefix, fmtr=self.fmtr)


class String:

    def __init__(self, avg_size: int):
        self.avg_size = avg_size

    @staticmethod
    def build_alphabet(key: str) -> str:
        return hex_digest(key) + hex_digest(key[::-1])

    @staticmethod
    def build_alphabet_md5(key: str) -> str:
        return hashlib.md5(key.encode()).hexdigest() + hashlib.md5(key[::-1].encode()).hexdigest()

    @staticmethod
    def build_string(alphabet: str, length: float) -> str:
        length_int = int(length)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        return body[:length_int]

    def next(self, key: Key) -> str:
        alphabet = self.build_alphabet(key.string)

        return self.build_string(alphabet, self.avg_size)


class IncompressibleString(String):

    @staticmethod
    def build_alphabet(*args) -> str:
        return GARBAGE

    @staticmethod
    def build_string(alphabet: str, length: float):
        length_int = int(length)
        offset = random.randint(a=0, b=len(alphabet) - length_int)
        return alphabet[offset:offset + length_int]


class Document(String):

    SIZE_VARIATION = 0.25  # 25%

    OVERHEAD = 210  # Minimum size due to static fields, body size is variable

    @classmethod
    def _get_variation_coeff(cls) -> float:
        return np.random.uniform(1 - cls.SIZE_VARIATION, 1 + cls.SIZE_VARIATION)

    @staticmethod
    def build_name(alphabet: str) -> str:
        return '%s %s' % (alphabet[:6], alphabet[6:12])  # % is faster than format()

    @staticmethod
    def build_email(alphabet: str) -> str:
        return '%s@%s.com' % (alphabet[12:18], alphabet[18:24])

    @staticmethod
    def build_alt_email(alphabet: str) -> str:
        name = random.randint(1, 9)
        domain = random.randint(12, 18)
        return '%s@%s.com' % (alphabet[name:name + 6], alphabet[domain:domain + 6])

    @staticmethod
    def build_city(alphabet: str) -> str:
        return alphabet[24:30]

    @staticmethod
    def build_realm(alphabet: str) -> str:
        return alphabet[30:36]

    @staticmethod
    def build_country(alphabet: str) -> str:
        return alphabet[42:48]

    @staticmethod
    def build_county(alphabet: str) -> str:
        return alphabet[48:54]

    @staticmethod
    def build_street(alphabet: str) -> str:
        return alphabet[54:62]

    @staticmethod
    def build_coins(alphabet: str) -> float:
        return max(0.1, int(alphabet[36:40], 16) / 100)

    @staticmethod
    def build_gmtime(alphabet: str) -> Tuple[int]:
        seconds = 396 * 24 * 3600 * (int(alphabet[63], 16) % 12)
        return tuple(time.gmtime(seconds))

    @staticmethod
    def build_year(alphabet: str) -> int:
        return 1985 + int(alphabet[62], 16)

    @staticmethod
    def build_state(alphabet: str) -> str:
        idx = alphabet.find('7') % NUM_STATES
        return STATES[idx][0]

    @staticmethod
    def build_full_state(alphabet: str) -> str:
        idx = alphabet.find('8') % NUM_STATES
        return STATES[idx][1]

    @staticmethod
    def build_category(alphabet: str) -> int:
        return int(alphabet[41], 16) % 3

    @staticmethod
    def build_achievements(alphabet: str) -> List[int]:
        return build_achievements(alphabet) or [0]

    def _size(self) -> float:
        if self.avg_size <= self.OVERHEAD:
            return 0
        return self._get_variation_coeff() * (self.avg_size - self.OVERHEAD)

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_city(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'body': self.build_string(alphabet, size),
        }


class SGImportLatencyDocument(Document):

    def next(self, key: str) -> dict:
        alphabet = self.build_alphabet(key)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_city(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'body': self.build_string(alphabet, size),
        }


class GroupedDocument(Document):

    def __init__(self, avg_size: int, groups: int):
        super().__init__(avg_size)
        self.groups = groups

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'doc_group': key.number % self.groups,
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_city(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'street': self.build_street(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
        }


class GroupedDocumentById(Document):

    def __init__(self, avg_size: int, groups: int):
        super().__init__(avg_size)
        self.groups = groups

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'doc_group': key.number % self.groups,
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_city(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'street': self.build_street(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'id': key.string
        }


class LargeItemGroupedDocument(GroupedDocument):
    def __init__(self, avg_size: int, groups: int, item_size: int):
        super().__init__(avg_size, groups)
        self.item_size = item_size

    @staticmethod
    def build_item(alphabet: str, size: int = 64, prefix: str = ""):
        length = size - len(prefix)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        num = random.randint(1, length)
        if prefix:
            return prefix + "-" + body[num:length] + body[0:num]
        return body[num:length] + body[0:num]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'doc_group': key.number % self.groups,
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_item(alphabet=alphabet, size=self.item_size),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'street': self.build_street(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
        }


class LargeItemGroupedDocumentKeySize(GroupedDocument):
    def __init__(self, avg_size: int, groups: int, item_size: int):
        super().__init__(avg_size, groups)
        self.item_size = item_size

    @staticmethod
    def build_item(alphabet: str, size: int = 64, prefix: str = ""):
        length = size - len(prefix)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        num = random.randint(1, length)
        if prefix:
            return prefix + "-" + body[num:length] + body[0:num]
        return body[num:length] + body[0:num]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'doc_group': key.number % self.groups,
            'name': self.build_item(alphabet=alphabet, size=self.item_size),
            'email': self.build_item(alphabet=alphabet, size=self.item_size),
            'alt_email': self.build_item(alphabet=alphabet, size=self.item_size),
            'city': self.build_item(alphabet=alphabet, size=self.item_size),
            'realm': self.build_item(alphabet=alphabet, size=self.item_size),
            'coins': self.build_item(alphabet=alphabet, size=self.item_size),
            'category': self.build_item(alphabet=alphabet, size=self.item_size),
            'street': self.build_item(alphabet=alphabet, size=self.item_size),
            'year': self.build_item(alphabet=alphabet, size=self.item_size),
            'body': self.build_item(alphabet=alphabet, size=self.item_size)
        }


class SingleFieldLargeDoc(GroupedDocument):
    def __init__(self, avg_size: int, groups: int, item_size: int):
        super().__init__(avg_size, groups)
        self.item_size = item_size

    @staticmethod
    def build_item(alphabet: str, size: int = 64, prefix: str = ""):
        length = size - len(prefix)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        num = random.randint(1, length)
        if prefix:
            return prefix + "-" + body[num:length] + body[0:num]
        return body[num:length] + body[0:num]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'name': self.build_item(alphabet=alphabet, size=self.item_size),
            'email': self.build_item(alphabet=alphabet, size=self.item_size)
        }


class LargeDocRandom(GroupedDocument):
    def __init__(self, avg_size: int, groups: int, item_size: int):
        super().__init__(avg_size, groups)
        self.item_size = item_size

    @staticmethod
    def build_alphabet_rand() -> str:
        return '%032x' % random.getrandbits(128) + '%032x' % random.getrandbits(128)

    @staticmethod
    def build_item(alphabet: str, size: int = 64, prefix: str = ""):
        length = size - len(prefix)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = int(num_slices / 2) * alphabet     # generate size/2 length body
        num = random.randint(1, length)
        if prefix:
            return prefix + "-" + body[num:length] + body[0:num]
        return body[num:length] + body[0:num]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        alphabet2 = self.build_alphabet_md5(key.string)
        name = self.build_item(alphabet=alphabet, size=self.item_size) + \
            self.build_item(alphabet=alphabet2, size=self.item_size)
        email = self.build_item(alphabet=alphabet, size=self.item_size) + \
            self.build_item(alphabet=alphabet2, size=self.item_size)

        return {
            'name': name,
            'email': email
        }

# class LargeDocRandom(GroupedDocument):
#     def __init__(self, avg_size: int, groups: int, item_size: int):
#         super().__init__(avg_size, groups)
#         self.item_size = item_size
#
#     @staticmethod
#     def build_item(size: int = 32, prefix: str = ""):
#         length = size - len(prefix)
#         body = ''
#         num_slices = int(math.ceil(length / 32))  # 32 bit hash
#         for _ in range(num_slices):
#             body = body + '%032x' % random.getrandbits(128)
#         if prefix:
#             body = prefix + '-' + body
#         return body
#
#     def next(self, key: Key) -> dict:
#
#         return {
#             'name': self.build_item(size=self.item_size),
#             'email': self.build_item(size=self.item_size)
#         }


class EventingCounterDocument(Document):

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_city(alphabet),
            'realm': self.build_realm(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'body': self.build_string(alphabet, size),
            'count': key.number
        }


class EventingSmallCounterDocument(Document):

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_city(alphabet),
            'count': key.number
        }


class NestedDocument(Document):

    OVERHEAD = 450  # Minimum size due to static fields, body size is variable

    def __init__(self, avg_size: int):
        super().__init__(avg_size)
        self.capped_field_value = {}  # type: dict

    def _size(self) -> float:
        if self.avg_size <= self.OVERHEAD:
            return 0
        if random.random() < 0.975:  # Normal distribution, mean=self.avg_size
            normal = np.random.normal(loc=1.0, scale=0.17)
            return (self.avg_size - self.OVERHEAD) * normal
        else:  # Outliers - beta distribution, 2KB-2MB range
            return 2048 / np.random.beta(a=2.2, b=1.0)

    def next(self, key: Key):
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': {'f': {'f': {'f': self.build_name(alphabet)}}},
            'email': {'f': {'f': self.build_email(alphabet)}},
            'alt_email': {'f': {'f': self.build_alt_email(alphabet)}},
            'street': {'f': {'f': self.build_street(alphabet)}},
            'city': {'f': {'f': self.build_city(alphabet)}},
            'county': {'f': {'f': self.build_county(alphabet)}},
            'state': {'f': self.build_state(alphabet)},
            'full_state': {'f': self.build_full_state(alphabet)},
            'country': {'f': self.build_country(alphabet)},
            'realm': {'f': self.build_realm(alphabet)},
            'coins': {'f': self.build_coins(alphabet)},
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
        }


class LargeDocument(Document):

    OVERHEAD = 680

    TEXT_LENGTH = 128

    @staticmethod
    def build_string(alphabet: str, length: float) -> str:
        length_int = int(length)
        num_slices = int(math.ceil(length / 32))  # 32 == len(alphabet)
        body = ''
        for i in range(num_slices):
            body += hex_digest(alphabet)
        return body[:length_int]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size() / 3
        offset = (PRIME * key.number) % (len(LOREM) - self.TEXT_LENGTH)

        return {
            'id': alphabet,
            'revered_id': alphabet[::-1],
            'code': hex_digest(alphabet),
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'padding': self.build_string(alphabet, size),
            'notes': self.build_string(alphabet[::-1], size),
            'text': self.build_string(alphabet[:16], size),
            'lorem': LOREM[offset:offset + self.TEXT_LENGTH],
        }


class ReverseLookupDocument(NestedDocument):

    OVERHEAD = 420

    def _size(self) -> float:
        return self.avg_size - self.OVERHEAD

    def __init__(self, avg_size: int, prefix: str):
        super().__init__(avg_size)
        self.prefix = prefix
        self.is_random = prefix != 'n1ql'

    def build_email(self, alphabet: str) -> str:
        if self.is_random:
            return self.build_alt_email(alphabet)
        else:
            return super().build_email(alphabet)

    def build_capped(self, alphabet: str, seq_id: int, num_unique: int) -> str:
        if self.is_random:
            offset = random.randint(1, 9)
            return '%s' % alphabet[offset:offset + 6]

        index = seq_id // num_unique
        return '%s_%d_%d' % (self.prefix, num_unique, index)

    def build_topics(self, seq_id: int) -> List[str]:
        return []

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'capped_small': self.build_capped(alphabet, key.number, 100),
            'topics': self.build_topics(key.number),
        }


class ReverseLookupKeySizeDocument(ReverseLookupDocument):

    def __init__(self, avg_size: int, prefix: str, item_size: int):
        super().__init__(avg_size, prefix)
        self.item_size = item_size

    def build_email(self, alphabet: str) -> str:
        num_repeat = int(math.ceil(self.item_size / 6 / 2))
        if self.is_random:
            name = random.randint(1, 9)
            domain = random.randint(12, 18)
            return '%s@%s.com' % (alphabet[name:name + 6] * num_repeat,
                                  alphabet[domain:domain + 6] * num_repeat)
        else:
            return '%s@%s.com' % (alphabet[12:18] * num_repeat, alphabet[18:24] * num_repeat)

    def build_capped(self, alphabet: str, seq_id: int, num_unique: int) -> str:
        if self.is_random:
            num_repeat = int(math.ceil(self.item_size / 6))
            offset = random.randint(1, 9)
            return '%s' % alphabet[offset:offset + 6] * num_repeat

        num_repeat = int(math.ceil((self.item_size - 8) / len(self.prefix)))
        index = seq_id // num_unique
        return '%s_%d_%d' % (self.prefix * num_repeat, num_unique, index)


class ReverseRangeLookupDocument(ReverseLookupDocument):

    OVERHEAD = 480

    def __init__(self, avg_size: int, prefix: str, range_distance: int):
        super().__init__(avg_size, prefix)
        if self.prefix is None:
            self.prefix = ""
        # Keep one extra as query runs from greater than 'x' to less than 'y'
        # both exclusive.
        self.distance = range_distance + 1

    def build_capped(self, alphabet: str, seq_id: int, num_unique: int) -> str:
        if self.is_random:
            offset = random.randint(1, 9)
            return '%s' % alphabet[offset:offset + 6]

        index = seq_id // num_unique
        return '%s_%d_%012d' % (self.prefix, num_unique, index)

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'capped_small': self.build_capped(alphabet, key.number, 100),
            'capped_small_range': self.build_capped(alphabet,
                                                    key.number + (self.distance * 100),
                                                    100),
            'topics': self.build_topics(key.number),
        }


class HashJoinDocument(ReverseRangeLookupDocument):

    OVERHEAD = 415

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()
        capped_range = key.number + self.distance * 100

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'capped_100': self.build_capped(alphabet, key.number,
                                            num_unique=100),
            'capped_100_range': self.build_capped(alphabet, capped_range,
                                                  num_unique=100),
            'capped_1K': self.build_capped(alphabet, key.number,
                                           num_unique=1000),
            'capped_10K': self.build_capped(alphabet, key.number,
                                            num_unique=10000),
        }


class ExtReverseLookupDocument(ReverseLookupDocument):

    OVERHEAD = 500

    def __init__(self, avg_size: int, prefix: str, num_docs: int):
        super().__init__(avg_size, prefix)
        self.num_docs = num_docs

    def build_topics(self, seq_id: int) -> List[str]:
        """1:4 reference to JoinedDocument keys."""
        return [
            decimal_fmtr((seq_id + 11) % self.num_docs, self.prefix),
            decimal_fmtr((seq_id + 19) % self.num_docs, self.prefix),
            decimal_fmtr((seq_id + 23) % self.num_docs, self.prefix),
            decimal_fmtr((seq_id + 29) % self.num_docs, self.prefix),
        ]

    def next(self, key: Key) -> dict:
        doc = super().next(key)
        doc['name'] = key.string
        return doc


class JoinedDocument(ReverseLookupDocument):

    def __init__(self, avg_size: int, prefix: str, num_docs: int,
                 num_categories: int, num_replies: int):
        super().__init__(avg_size, prefix)
        self.num_categories = num_categories
        self.num_docs = num_docs
        self.num_replies = num_replies

    def build_owner(self, seq_id: int) -> str:
        """4:1 reference to ReverseLookupDocument keys."""
        ref_id = seq_id % (self.num_docs // 4)
        return decimal_fmtr(ref_id, self.prefix)

    def build_title(self, alphabet: str) -> str:
        return alphabet[:32]

    def build_categories(self, seq_id: int) -> List[str]:
        """1:4 reference to RefDocument keys."""
        return [
            decimal_fmtr((seq_id + 11) % self.num_categories, self.prefix),
            decimal_fmtr((seq_id + 19) % self.num_categories, self.prefix),
            decimal_fmtr((seq_id + 23) % self.num_categories, self.prefix),
            decimal_fmtr((seq_id + 29) % self.num_categories, self.prefix),
        ]

    def build_user(self, seq_id: int, idx: int) -> str:
        return decimal_fmtr((seq_id + idx + 537) % self.num_docs, self.prefix)

    def build_replies(self, seq_id: int) -> List[dict]:
        """1:N references to ReverseLookupDocument keys."""
        return [
            {'user': self.build_user(seq_id, idx)}
            for idx in range(self.num_replies)
        ]

    def build_sub_capped(self, alphabet: str, seq_id: int, num_unique: int) -> str:
        ref_id = seq_id % (self.num_docs // 4)
        return self.build_capped(alphabet, ref_id, num_unique)

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'owner': self.build_owner(key.number),
            'moderator': self.build_owner(key.number + 1),
            'title': self.build_title(alphabet),
            'capped': self.build_capped(alphabet, key.number, 100),
            'sub_capped': self.build_sub_capped(alphabet, key.number, 100),
            'categories': self.build_categories(key.number),
            'replies': self.build_replies(key.number),
        }


class RefDocument(ReverseLookupDocument):

    def build_ref_name(self, seq_id: int) -> str:
        return decimal_fmtr(seq_id, self.prefix)

    def next(self, key: Key) -> dict:
        return {
            'name': self.build_ref_name(key.number),
        }


class ArrayIndexingDocument(ReverseLookupDocument):

    """Extend ReverseLookupDocument by adding two new fields.

    achievements1 is a variable-length array (default length is 10). Every
    instance of achievements1 is unique. This field is useful for single lookups.

    achievements2 is a fixed-length array. Each instance of achievements2 is
    repeated 100 times (ARRAY_CAP). This field is useful for range queries.
    """

    ARRAY_CAP = 100

    ARRAY_SIZE = 10

    OVERHEAD = 530

    def __init__(self, avg_size: int, prefix: str, array_size: int, num_docs: int):
        super().__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def build_achievements1(self, seq_id: int) -> List[int]:
        """Build an array of integers.

        Every document reserves a range of numbers that can be used for a
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

        return [int(offset + i) for i in range(self.array_size)]

    def build_achievements2(self, seq_id: int) -> List[int]:
        """Build an array of integers.

        achievements2 is very similar to achievements1. However, in case of
        achievements2 ranges overlap so that multiple documents case satisfy the
        same queries. Overlapping is achieving by integer division using
        ARRAY_CAP constant.
        """
        offset = seq_id // self.ARRAY_CAP * self.ARRAY_SIZE
        if self.is_random:
            offset = self.num_docs * self.ARRAY_SIZE
            offset += (2 * seq_id) // self.ARRAY_CAP * self.ARRAY_SIZE
            offset += random.randint(1, self.ARRAY_SIZE)

        return [int(offset + i) for i in range(self.ARRAY_SIZE)]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements1': self.build_achievements1(key.number + 1),
            'achievements2': self.build_achievements2(key.number + 1),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'capped_small': self.build_capped(alphabet, key.number, 100),
            'topics': self.build_topics(key.number),
        }


class ArrayIndexingUniqueDocument(ReverseLookupDocument):

    def __init__(self, avg_size: int, prefix: str, array_size: int, num_docs: int):
        super().__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def build_achievements1(self, seq_id: int) -> List[int]:

        offset = seq_id * self.array_size
        if self.is_random:
            offset = self.num_docs * self.array_size
            offset += 2 * seq_id * self.array_size
            offset += random.randint(1, self.array_size)

        return [int(offset + i) for i in range(self.array_size)]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements1': self.build_achievements1(key.number + 1),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'capped_small': self.build_capped(alphabet, key.number, 100),
            'topics': self.build_topics(key.number),
        }


class ArrayIndexingRangeScanDocument(ReverseLookupDocument):

    ARRAY_CAP = 100

    ARRAY_SIZE = 10

    OVERHEAD = 530

    def __init__(self, avg_size: int, prefix: str, array_size: int, num_docs: int):
        super().__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def build_achievements2(self, seq_id: int) -> List[int]:
        offset = seq_id // self.ARRAY_CAP * self.array_size
        if self.is_random:
            offset = self.num_docs * self.array_size
            offset += (2 * seq_id) // self.ARRAY_CAP * self.array_size
            offset += random.randint(1, self.array_size)

        return [int(offset + i) for i in range(self.array_size)]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements2': self.build_achievements2(key.number + 1),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'capped_small': self.build_capped(alphabet, key.number, 100),
            'topics': self.build_topics(key.number),
        }


class ArrayIndexingCompositeFieldDocument(ReverseLookupDocument):

    ARRAY_CAP = 100

    ARRAY_SIZE = 10

    OVERHEAD = 530

    def __init__(self, avg_size: int, prefix: str, array_size: int, num_docs: int):
        super().__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def build_achievements1(self, seq_id: int):
        offset = seq_id * self.array_size
        if self.is_random:
            offset = self.num_docs * self.array_size
            offset += 2 * seq_id * self.array_size
            offset += random.randint(1, self.array_size)

        return \
            [
                {
                    "atype": int(offset + i),
                    "avalue": int(offset + i)
                }
                for i in range(self.array_size)
            ]

    def build_achievements2(self, seq_id: int):
        offset = seq_id // self.ARRAY_CAP * self.array_size
        if self.is_random:
            offset = self.num_docs * self.array_size
            offset += (2 * seq_id) // self.ARRAY_CAP * self.array_size
            offset += random.randint(1, self.array_size)

        return \
            [
                {
                    "atype": int(offset + i),
                    "avalue": int(offset + i)
                }
                for i in range(self.array_size)
            ]

    def build_achievements3(self, seq_id: int):
        offset_a = seq_id // self.ARRAY_CAP * self.array_size
        offset_b = seq_id // (2 * self.ARRAY_CAP) * self.array_size
        if self.is_random:
            offset_a = self.num_docs * self.array_size
            offset_a += (2 * seq_id) // self.ARRAY_CAP * self.array_size
            offset_a += random.randint(1, self.array_size)
            offset_b = self.num_docs * self.array_size
            offset_b += seq_id // self.ARRAY_CAP * self.array_size
            offset_b += random.randint(1, self.array_size)

        return \
            [
                {
                    "atype": int(offset_a + i),
                    "avalue": int(offset_b + i)
                }
                for i in range(self.array_size)
            ]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements1': self.build_achievements1(key.number + 1),
            'achievements2': self.build_achievements2(key.number + 1),
            'achievements3': self.build_achievements3(key.number + 1),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'topics': self.build_topics(key.number),
        }


class ArrayIndexingCompositeFieldUniqueDocument(ReverseLookupDocument):

    ARRAY_CAP = 100

    ARRAY_SIZE = 10

    OVERHEAD = 530

    def __init__(self, avg_size: int, prefix: str, array_size: int, num_docs: int):
        super().__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def build_achievements1(self, seq_id: int):
        offset = seq_id * self.array_size
        if self.is_random:
            offset = self.num_docs * self.array_size
            offset += 2 * seq_id * self.array_size
            offset += random.randint(1, self.array_size)

        return \
            [
                {
                    "atype": int(offset + i),
                    "avalue": int(offset + i)
                }
                for i in range(self.array_size)
            ]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements1': self.build_achievements1(key.number + 1),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'topics': self.build_topics(key.number),
        }


class ArrayIndexingCompositeFieldRangeScanDocument(ReverseLookupDocument):

    ARRAY_CAP = 100

    ARRAY_SIZE = 10

    OVERHEAD = 530

    def __init__(self, avg_size: int, prefix: str, array_size: int, num_docs: int):
        super().__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def build_achievements2(self, seq_id: int):
        offset = seq_id // self.ARRAY_CAP * self.array_size
        if self.is_random:
            offset = self.num_docs * self.array_size
            offset += (2 * seq_id) // self.ARRAY_CAP * self.array_size
            offset += random.randint(1, self.array_size)

        return \
            [
                {
                    "atype": int(offset + i),
                    "avalue": int(offset + i)
                }
                for i in range(self.array_size)
            ]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements2': self.build_achievements2(key.number + 1),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'topics': self.build_topics(key.number),
        }


class ArrayIndexingCompositeFieldIntersectDocument(ReverseLookupDocument):

    ARRAY_CAP = 100

    ARRAY_SIZE = 10

    OVERHEAD = 530

    def __init__(self, avg_size: int, prefix: str, array_size: int, num_docs: int):
        super().__init__(avg_size, prefix)
        self.array_size = array_size
        self.num_docs = num_docs

    def build_achievements3(self, seq_id: int):
        offset_a = seq_id // self.ARRAY_CAP * self.array_size
        offset_b = seq_id // (2 * self.ARRAY_CAP) * self.array_size
        if self.is_random:
            offset_a = self.num_docs * self.array_size
            offset_a += (2 * seq_id) // self.ARRAY_CAP * self.array_size
            offset_a += random.randint(1, self.array_size)
            offset_b = self.num_docs * self.array_size
            offset_b += seq_id // self.ARRAY_CAP * self.array_size
            offset_b += random.randint(1, self.array_size)

        return \
            [
                {
                    "atype": int(offset_a + i),
                    "avalue": int(offset_b + i)
                }
                for i in range(self.array_size)
            ]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'street': self.build_street(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements3': self.build_achievements3(key.number + 1),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'body': self.build_string(alphabet, size),
            'topics': self.build_topics(key.number),
        }


class ProfileDocument(ReverseLookupDocument):

    OVERHEAD = 390

    def build_capped(self, *args):
        capped = super().build_capped(*args)
        return capped.replace('_', '')

    def build_zip(self, seq_id: int) -> str:
        if self.is_random:
            zip_code = random.randint(70000, 90000)
        else:
            zip_code = 70000 + seq_id % 20000
        return str(zip_code)

    def build_long_street(self, alphabet: str, seq_id: int, capped_small: str,
                          capped_large: str) -> str:
        if self.is_random:
            num = random.randint(0, 1000)
            idx = random.randint(0, NUM_STREET_SUFFIXES - 1)
        else:
            num = seq_id % 5000
            idx = alphabet.find('7') % NUM_STREET_SUFFIXES
        suffix = STREET_SUFFIX[idx]

        return '%d %s %s %s' % (num, capped_small, capped_large, suffix)

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        category = self.build_category(alphabet) + 1
        capped_large = self.build_capped(alphabet, key.number, 1000 * category)
        capped_small = self.build_capped(alphabet, key.number, 10)

        return {
            'first_name': self.build_name(alphabet),
            'last_name': self.build_street(alphabet),
            'email': self.build_email(alphabet),
            'balance': self.build_coins(alphabet),
            'date': {
                'gmtime': self.build_gmtime(alphabet),
                'year': self.build_year(alphabet),
            },
            'capped_large': capped_large,
            'address': {
                'street': self.build_long_street(alphabet,
                                                 key.number,
                                                 capped_small,
                                                 capped_large),
                'city': self.build_city(alphabet),
                'county': self.build_county(alphabet),
                'state': self.build_state(alphabet),
                'zip': self.build_zip(key.number),
                'realm': self.build_realm(alphabet),
            },
            'body': self.build_string(alphabet, size),
        }


class ImportExportDocument(ReverseLookupDocument):

    """Extend ReverseLookupDocument by adding 25 fields with random size."""

    OVERHEAD = 1022

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()
        return {
            'name': self.build_name(alphabet) * random.randint(0, 5),
            'email': self.build_email(alphabet) * random.randint(0, 5),
            'alt_email': self.build_alt_email(
                alphabet) * random.randint(0, 5),
            'street': self.build_street(alphabet) * random.randint(0, 9),
            'city': self.build_city(alphabet) * random.randint(0, 9),
            'county': self.build_county(alphabet) * random.randint(0, 5),
            'state': self.build_state(alphabet) * random.randint(0, 5),
            'full_state': self.build_full_state(
                alphabet) * random.randint(0, 5),
            'country': self.build_country(
                alphabet) * random.randint(0, 5),
            'realm': self.build_realm(
                alphabet) * random.randint(0, 9),
            'alt_street': self.build_street(
                alphabet) * random.randint(0, 9),
            'alt_city': self.build_city(
                alphabet) * random.randint(0, 9),
            'alt_county': self.build_county(
                alphabet) * random.randint(0, 5),
            'alt_state': self.build_state(
                alphabet) * random.randint(0, 5),
            'alt_full_state': self.build_full_state(
                alphabet) * random.randint(0, 5),
            'alt_country': self.build_country(
                alphabet) * random.randint(0, 5),
            'alt_realm': self.build_realm(
                alphabet) * random.randint(0, 9),
            'coins': self.build_coins(
                alphabet) * random.randint(0, 999),
            'category': self.build_category(
                alphabet) * random.randint(0, 5),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet) * random.randint(0, 9),
            'year': self.build_year(alphabet) * random.randint(0, 5),
            'body': self.build_string(alphabet, size),
            'capped_small': self.build_capped(
                alphabet, key.number, 100) * random.randint(0, 5),
            'alt_capped_small': self.build_capped(
                alphabet, key.number, 100) * random.randint(0, 5),
        }


class ImportExportDocumentArray(ImportExportDocument):

    """Extend ImportExportDocument by adding array docs.

    The documents contain 25 top-level fields with variable-size arrays.
    """

    OVERHEAD = 0

    def _random_array(self, value: str, num: int):
        if value == '':
            return []
        if len(value) < num:
            return [value] * 5
        scope = sorted(random.sample(range(len(value)), num))
        result = [value[0 if i == 0 else scope[i - 1]:i + scope[i]] for i in range(num)]
        return result

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        # 25 Fields of random size. Have an array with at least 10 items in five fields.
        return {
            'name': self._random_array(self.build_name(
                alphabet) * random.randint(0, 9), 5),
            'email': self.build_email(
                alphabet) * random.randint(0, 5),
            'alt_email': self.build_alt_email(
                alphabet) * random.randint(0, 9),
            'street': self._random_array(self.build_street(
                alphabet) * random.randint(0, 9), 5),
            'city': self._random_array(self.build_city(
                alphabet) * random.randint(0, 9), 5),
            'county': self._random_array(self.build_county(
                alphabet) * random.randint(0, 9), 5),
            'state': self._random_array(self.build_state(
                alphabet) * random.randint(0, 9), 5),
            'full_state': self._random_array(self.build_full_state(
                alphabet) * random.randint(0, 9), 5),
            'country': self._random_array(self.build_country(
                alphabet) * random.randint(0, 9), 5),
            'realm': self.build_realm(alphabet) * random.randint(0, 9),
            'alt_street': self._random_array(self.build_street(
                alphabet) * random.randint(0, 9), 5),
            'alt_city': self._random_array(self.build_city(
                alphabet) * random.randint(0, 9), 5),
            'alt_county': self.build_county(
                alphabet) * random.randint(0, 9),
            'alt_state': self.build_state(
                alphabet) * random.randint(0, 9),
            'alt_full_state': self.build_full_state(
                alphabet) * random.randint(0, 9),
            'alt_country': self.build_country(
                alphabet) * random.randint(0, 9),
            'alt_realm': self.build_realm(
                alphabet) * random.randint(0, 9),
            'coins': self.build_coins(
                alphabet) * random.randint(0, 999),
            'category': self.build_category(
                alphabet) * random.randint(0, 9),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet) * random.randint(0, 9),
            'year': self.build_year(alphabet) * random.randint(0, 5),
            'body': self._random_array(self.build_string(alphabet, size), 7),
            'capped_small': self.build_capped(
                alphabet, key.number, 100) * random.randint(0, 5),
            'alt_capped_small': self.build_capped(
                alphabet, key.number, 100) * random.randint(0, 5),
        }


class ImportExportDocumentNested(ImportExportDocument):

    """Extend ImportExportDocument by adding nested docs.

    The documents contain 25 top-level fields (5 nested sub-documents).
    """

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': {'n': {'a': {'m': {'e': self.build_name(
                alphabet) * random.randint(0, 3)}}}},
            'email': {'e': {'m': {'a': {'i': self.build_email(
                alphabet) * random.randint(0, 3)}}}},
            'alt_email': {'a': {'l': {'t': {'e': self.build_alt_email(
                alphabet) * random.randint(0, 3)}}}},
            'street': {'s': {'t': {'r': {'e': self.build_street(
                alphabet) * random.randint(0, 3)}}}},
            'city': {'c': {'i': {'t': {'y': self.build_city(
                alphabet) * random.randint(0, 3)}}}},
            'county': {'c': {'o': {'u': {'n': self.build_county(
                alphabet) * random.randint(0, 3)}}}},
            'state': {'s': {'t': {'a': {'t': self.build_state(
                alphabet) * random.randint(0, 3)}}}},
            'full_state': {'f': {'u': {'l': {'l': self.build_full_state(
                alphabet) * random.randint(0, 3)}}}},
            'country': {'c': {'o': {'u': {'n': self.build_country(
                alphabet) * random.randint(0, 3)}}}},
            'realm': {'r': {'e': {'a': {'l': self.build_realm(
                alphabet) * random.randint(0, 3)}}}},
            'alt_street': {'a': {'l': {'t': {'s': self.build_street(
                alphabet) * random.randint(0, 3)}}}},
            'alt_city': {'a': {'l': {'t': {'c': self.build_city(
                alphabet) * random.randint(0, 3)}}}},
            'alt_county': {'e': {'m': {'a': {'i': self.build_county(
                alphabet) * random.randint(0, 3)}}}},
            'alt_state': {'e': {'m': {'a': {'i': self.build_state(
                alphabet) * random.randint(0, 3)}}}},
            'alt_full_state': {'e': {'m': {'a': {'i': self.build_full_state(
                alphabet) * random.randint(0, 2)}}}},
            'alt_country': {'e': {'m': {'a': {'i': self.build_country(
                alphabet) * random.randint(0, 2)}}}},
            'alt_realm': {'e': {'m': {'a': {'i': self.build_realm(
                alphabet) * random.randint(0, 3)}}}},
            'coins': {'e': {'m': {'a': {'i': self.build_coins(
                alphabet) * random.randint(0, 99)}}}},
            'category': {'e': {'m': {'a': {'i': self.build_category(
                alphabet) * random.randint(0, 3)}}}},
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet) * random.randint(0, 2),
            'year': self.build_year(alphabet) * random.randint(0, 2),
            'body': self.build_string(alphabet, size),
            'capped_small': self.build_capped(
                alphabet, key.number, 10) * random.randint(0, 2),
            'alt_capped_small': self.build_capped(
                alphabet, key.number, 10) * random.randint(0, 2),
        }


class GSIMultiIndexDocument(Document):

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_alt_email(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_alt_email(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'body': self.build_string(alphabet, size),
        }


class PlasmaDocument(Document):

    @staticmethod
    def build_item(alphabet: str, size: int = 64, prefix: str = ""):
        length = size - len(prefix)
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        num = random.randint(1, length)
        if prefix:
            return prefix + "-" + body[num:length] + body[0:num]
        return body[num:length] + body[0:num]


class SmallPlasmaDocument(PlasmaDocument):

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'alt_email': self.build_alt_email(alphabet)
        }


class KeyPlasmaDocument(PlasmaDocument):

    def next(self, key: Key) -> dict:
        return {
            'city': key.string
        }


class SmallPlasmaGroupedDocument(PlasmaDocument):

    def __init__(self, avg_size: int, groups: int):
        super().__init__(avg_size)
        self.groups = groups

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'doc_group': key.number % self.groups,
            'alt_email': self.build_alt_email(alphabet),
        }


class SequentialPlasmaDocument(PlasmaDocument):

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        number = key.string[-12:]

        return {
            'city': self.build_item(alphabet=alphabet, size=17, prefix=number)
        }


class LargeItemPlasmaDocument(PlasmaDocument):

    def __init__(self, avg_size: int, item_size: int):
        super().__init__(avg_size)
        self.item_size = item_size

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_item(alphabet=alphabet, size=self.item_size),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'body': self.build_string(alphabet, size),
        }


class VaryingItemSizePlasmaDocument(PlasmaDocument):

    def __init__(self, avg_size: int, size_variation_min: int,
                 size_variation_max: int):
        super().__init__(avg_size)
        self.size_variation_min = size_variation_min
        self.size_variation_max = size_variation_max

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()
        length = random.randint(self.size_variation_min, self.size_variation_max)

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'city': self.build_item(alphabet=alphabet, size=length),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'body': self.build_string(alphabet, size),
        }


class HundredIndexDocument(PlasmaDocument):

    def __init__(self, avg_size: int, size_variation_min: int,
                 size_variation_max: int):
        super().__init__(avg_size)
        self.size_variation_min = size_variation_min
        self.size_variation_max = size_variation_max

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size()
        length = random.randint(self.size_variation_min, self.size_variation_max)

        return {
            'field1': self.build_name(alphabet),
            'field2': self.build_email(alphabet),
            'field3': self.build_alt_email(alphabet),
            'field4': self.build_item(alphabet=alphabet, size=length),
            'field5': self.build_realm(alphabet),
            'field6': self.build_coins(alphabet),
            'field7': self.build_category(alphabet),
            'field8': self.build_achievements(alphabet),
            'field9': self.build_string(alphabet, size),
            'field10': self.build_name(alphabet),
            'field11': self.build_email(alphabet),
            'field12': self.build_alt_email(alphabet),
            'field13': self.build_item(alphabet=alphabet, size=length),
            'field14': self.build_realm(alphabet),
            'field15': self.build_coins(alphabet),
            'field16': self.build_category(alphabet),
            'field17': self.build_achievements(alphabet),
            'field18': self.build_string(alphabet, size),
            'field19': self.build_category(alphabet),
            'field20': self.build_achievements(alphabet),
        }


class EventingSmallDocument(Document):

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'name': self.build_name(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'coins': self.build_coins(alphabet),
        }


class TpcDsDocument:

    @property
    def categories(self) -> List[str]:
        return random.sample(CATEGORIES, 2)

    @property
    def counties(self) -> List[str]:
        return random.sample(COUNTIES, 10)

    @property
    def day_of_year(self) -> int:
        return random.randint(1, 180)

    @property
    def education_status(self) -> str:
        idx = random.randint(0, len(EDUCATION_STATUSES) - 1)
        return EDUCATION_STATUSES[idx]

    @property
    def gender(self) -> str:
        idx = random.randint(0, len(GENDERS) - 1)
        return GENDERS[idx]

    @property
    def manufacturer_id(self) -> int:
        return random.randint(1, 1000)

    @property
    def marital_status(self) -> str:
        idx = random.randint(0, len(MARITAL_STATUSES) - 1)
        return MARITAL_STATUSES[idx]

    @property
    def month(self) -> int:
        return random.randint(1, 7)

    @property
    def sales_price(self) -> int:
        return random.randint(35, 40)

    @property
    def state(self) -> str:
        idx = random.randint(0, NUM_STATES - 1)
        return STATES[idx][0]

    @property
    def quarter(self) -> int:
        return random.randint(1, 4)

    @property
    def year(self) -> int:
        idx = random.randint(0, len(YEARS) - 1)
        return YEARS[idx]

    @property
    def zip_codes(self) -> List[str]:
        return random.sample(ZIP_CODES, 50)

    def next(self, *args) -> dict:
        return {
            'categories': self.categories,
            'counties': self.counties,
            'day_of_year': self.day_of_year,
            'education_status': self.education_status,
            'gender': self.gender,
            'manufacturer_id': self.manufacturer_id,
            'marital_status': self.marital_status,
            'month': self.month,
            'sales_price': self.sales_price,
            'state': self.state,
            'quarter': self.quarter,
            'year': self.year,
            'zip_codes': self.zip_codes,
        }


class PackageDocument(Document):

    @staticmethod
    def build_account_id(key: int, repeated: int) -> str:
        return '%016x' % spooky.hash64(str(key // repeated))

    @property
    def package_status(self) -> str:
        idx = random.randint(0, len(PACKAGE_STATUSES) - 1)
        return PACKAGE_STATUSES[idx]

    @staticmethod
    def build_bcdn_number(key: str) -> str:
        return '%032x' % spooky.hash128(key)

    @staticmethod
    def build_shipping_date(key: int) -> str:
        return datetime.fromtimestamp(key).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def address(key: int, alphabet: str) -> str:
        suffix = STREET_SUFFIX[key % len(STREET_SUFFIX)]
        return '%d %s %s' % (int(alphabet[:4], 16), alphabet[50:], suffix)

    @property
    def postal_code(self) -> str:
        idx = random.randint(0, len(ZIP_CODES) - 1)
        return ZIP_CODES[idx]

    @property
    def weight(self) -> float:
        return round(10 ** 14 * random.random(), 2)

    @property
    def charges(self) -> float:
        return round(10 ** 2 * random.random(), 2)

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'minorAccountId': self.build_account_id(key.number, 10 ** 6),
            'majorAccountId': self.build_account_id(key.number, 10 ** 7),
            'packageStatus': self.package_status,
            'bcdnNumber': self.build_bcdn_number(key.string),
            'shippingDate': self.build_shipping_date(key.number),
            'address': self.address(key.number, alphabet),
            'postalCode': self.postal_code,
            'weight': self.weight,
            'charges': self.charges,
        }


class BigFunDocument:

    def next(self, *args) -> dict:
        return {
            'user_since_small': query_gen.bf03params(num_matches=1e2),
            'user_since_medium': query_gen.bf03params(num_matches=1e4),
            'user_since_large': query_gen.bf03params(num_matches=1e6),
            'send_time_small': query_gen.bf08params(num_matches=1e2),
            'send_time_medium': query_gen.bf08params(num_matches=1e4),
            'send_time_large': query_gen.bf08params(num_matches=1e6),
        }


class MultiBucketDocument(Document):

    def __init__(self, avg_size: int):
        super().__init__(avg_size)
        with open("tests/fts/data/benchmark/phrase.txt") as fp:
            data = fp.read()
        self.words = data.split()

    def get_words(self, num: int):
        factor = random.randint(0, 4)
        num = num - 2 + factor
        return " ".join(random.sample(self.words, num))

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)

        return {
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'alt_email': self.build_alt_email(alphabet),
            'fts_more_words': self.get_words(140),
            'fts_less_words': self.get_words(3),
            'category': self.build_category(alphabet),
        }


class FTSRebalanceDocument(Document):

    def __init__(self, avg_size: int):
        super().__init__(avg_size)
        with open("tests/fts/data/benchmark/phrase.txt") as fp:
            data_phrase = fp.read()
        self.words_phrase = data_phrase.split()

    def get_words(self, num: int):
        factor = random.randint(0, 4)
        num = num - 2 + factor
        return " ".join(random.sample(self.words_phrase, num))

    @staticmethod
    def build_random_date(start_date: date, days_between_dates: int) -> str:
        random_number_of_days = random.randrange(days_between_dates)
        val = str(start_date + timedelta(days=random_number_of_days))
        return val

    def next(self, key: Key) -> dict:
        start_date = date(2020, 1, 1)
        end_date = date(2020, 3, 1)
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days

        return {
            'num1': random.randint(100000, 999999),
            'num2': random.randint(100000, 999999),
            'date1': self.build_random_date(start_date, days_between_dates),
            'date2': self.build_random_date(start_date, days_between_dates),
            'text1': self.get_words(170),
            'text2': self.get_words(170)
        }


class FTSDocument(Document):

    def __init__(self, avg_size: int):
        super().__init__(avg_size)
        with open("tests/fts/data/benchmark/word_prob.txt") as fp:
            data = fp.read().splitlines()
        words = []
        probability = []
        self.average_length = 0

        for line in data:
            line = line.split()
            words.append(line[0])
            probability.append(float(line[1]))
            self.average_length += len(line[0]) * float(line[1])

        self.words = np.array(words)
        self.probability = np.array(probability)

    def get_words(self, num: int):
        return_string = np.random.choice(self.words, size=num, replace=False, p=self.probability)
        random.shuffle(return_string)
        return " ".join(return_string)

    def next(self, key: Key) -> dict:
        num_words = int(self.avg_size/self.average_length/2)
        return {
            'text': self.get_words(num_words),
            'text2': self.get_words(num_words)
        }


class AdvFilterDocument(Document):

    OVERHEAD = 680

    TEXT_LENGTH = 128

    @staticmethod
    def build_string(alphabet: str, length: float) -> str:
        length_int = int(length)
        num_slices = int(math.ceil(length / 32))  # 32 == len(alphabet)
        body = ''
        for i in range(num_slices):
            body += hex_digest(alphabet)
        return body[:length_int]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size() / 3
        offset = (PRIME * key.number) % (len(LOREM) - self.TEXT_LENGTH)
        identifier = key.string.split("-")[1]

        return {
            'id': alphabet,
            'revered_id': alphabet[::-1],
            'code': hex_digest(alphabet),
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'state': self.build_state(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'padding': self.build_string(alphabet, size),
            'notes': self.build_string(alphabet[::-1], size),
            'text': self.build_string(alphabet[:16], size),
            'lorem': LOREM[offset:offset + self.TEXT_LENGTH],
            'identifier1': identifier,
            'identifier2': {'n': {'a': {'m': {'e': {'s': self.build_name(
                       alphabet) * random.randint(0, 3)}}}}},
        }


class AdvFilterXattrBody(AdvFilterDocument):

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size() / 3
        identifier = key.string.split("-")[1]

        return {
            'identifier1': identifier,
            'city': self.build_city(alphabet),
            'text': self.build_string(alphabet[:16], size),
        }


class LargeGroupedDocument(LargeDocument):

    def __init__(self, avg_size: int, groups: int):
        super().__init__(avg_size)
        self.groups = groups

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        size = self._size() / 3
        offset = (PRIME * key.number) % (len(LOREM) - self.TEXT_LENGTH)

        return {
            'state': key.number % self.groups,
            'id': alphabet,
            'revered_id': alphabet[::-1],
            'code': hex_digest(alphabet),
            'name': self.build_name(alphabet),
            'email': self.build_email(alphabet),
            'city': self.build_city(alphabet),
            'county': self.build_county(alphabet),
            'full_state': self.build_full_state(alphabet),
            'country': self.build_country(alphabet),
            'realm': self.build_realm(alphabet),
            'coins': self.build_coins(alphabet),
            'category': self.build_category(alphabet),
            'achievements': self.build_achievements(alphabet),
            'gmtime': self.build_gmtime(alphabet),
            'year': self.build_year(alphabet),
            'padding': self.build_string(alphabet, size),
            'notes': self.build_string(alphabet[::-1], size),
            'text': self.build_string(alphabet[:16], size),
            'lorem': LOREM[offset:offset + self.TEXT_LENGTH],
        }


class UnifiedDocument(Document):

    def __init__(self, avg_size: int, num_replies: int, item_size: int):
        super().__init__(avg_size)
        self.num_replies = num_replies
        self.item_size = item_size
        self.fk = Faker()
        Faker.seed(1000)

    @staticmethod
    def array_dic(batch_group_id: int, qualify_doc_id: int):
        a1 = []
        for i in range(3):
            temp_dic = {"ac0": batch_group_id,
                        "ac1": batch_group_id,
                        "ac2": batch_group_id,
                        "ac3": batch_group_id,
                        "ac4": batch_group_id,
                        "aid": qualify_doc_id,
                        "apos": i}
            a1.append(temp_dic)

        return a1

    @staticmethod
    def group_id(batch_group_id: int):
        return batch_group_id

    @staticmethod
    def doc_id(qualify_doc_id: int):
        return qualify_doc_id

    @staticmethod
    def build_string(alphabet: str, length: int) -> str:
        num_slices = int(math.ceil(length / 64))  # 64 == len(alphabet)
        body = num_slices * alphabet
        offset = random.randint(0, len(body) - length)
        return body[offset:offset + length]

    @staticmethod
    def build_index_md5(key: str, item_size: int) -> str:
        md5_string = hashlib.md5(key.encode()).hexdigest() + \
                     hashlib.md5(key[::-1].encode()).hexdigest()
        num_slices = int(math.ceil(item_size / len(md5_string)))
        body = num_slices * md5_string
        return body[:item_size]

    def next(self, key: Key) -> dict:
        alphabet = self.build_alphabet(key.string)
        current_item = int(key.string.split('-')[1])
        qualify_doc_id = current_item % self.num_replies
        batch_group_id = int(current_item / self.num_replies)
        doc = {
            "a1": self.array_dic(batch_group_id, qualify_doc_id),
            "c0": self.group_id(batch_group_id),
            "c1": self.group_id(batch_group_id),
            "c2": self.group_id(batch_group_id),
            "c3": self.group_id(batch_group_id),
            "c4": self.group_id(batch_group_id),
            "cid": self.doc_id(qualify_doc_id),
            "id": self.doc_id(qualify_doc_id),
            "index_field": self.build_index_md5(key.string, self.item_size),
            "fts_field1": self.fk.sentence(20),
            "num": self.fk.pyint(100000000000, 1000000000000000),
            "location": {"lat": float(self.fk.latlng()[0]),  "lon": float(self.fk.latlng()[1])},
            "date": self.fk.date()
        }
        doc_b = json.dumps(doc).encode("utf-8")
        current_size = len(doc_b)
        doc["comment"] = self.build_string(alphabet, self.avg_size - current_size)
        return doc


class HighCompressibleDocument(Document):

    @staticmethod
    def build_rand_string(prefix: str, start: int, end: int) -> str:
        return prefix + str(random.randint(start, end))

    @staticmethod
    def build_rand_int(start: int, end: int) -> int:
        return random.randint(start, end)

    def next(self, key: Key) -> dict:

        return {
            "first_doc_field": {
                "second_doc_field": 1234,
                "third_doc_field": self.build_rand_int(0, 9),
                "fourth_doc_field": self.build_rand_int(0, 9),
                "fifth_doc_field": 4321,
                "sixth_doc_field": "couchbase-server",
                "seventh_doc_field": "perf.couchbase.com",
                "eighth_doc_field": self.build_rand_int(1000000000, 9999999999),
                "ninth_doc_field": "couchbase-client",
                "tenth_doc_field": "couchbase-component",
                "eleventh_doc_field": self.build_rand_int(1000000000, 9999999999),
                "twelfth_doc_field": "query-context",
                "thirteenth_doc_field": {
                    "fourteenth_doc_field": {
                        "fifteenth_doc_field": "987654321",
                        "sixteenth_doc_field": "CAPELLA-SERVERLESS",
                        "seventeenth_doc_field": {
                            "eighteenth_doc_field": {
                                "nineteenth_doc_field": self.build_rand_int(0, 9)
                            },
                            "twentieth_doc_field": 9999999999,
                            "twenty_first_doc_field": 9999999999,
                            "twenty_second_doc_field": self.build_rand_int(0, 9)
                        }
                    },
                    "twenty_third_doc_field": {
                        "twenty_fourth_doc_field": "123456789"
                    }
                },
                "twenty_fifth_doc_field": "Bucket",
                "twenty_sixth_doc_field":
                    self.build_rand_string("performance-benchmark-test-case", 10000, 99999)
            },
            "twenty_seventh_doc_field": {
                "twenty_eighth_doc_field": [
                    self.build_rand_string("collection-", 100000, 999999),
                    self.build_rand_string("collection-", 100000, 999999),
                    self.build_rand_string("collection-", 100000, 999999),
                    self.build_rand_string("collection-", 100000, 999999),
                    self.build_rand_string("collection-", 100000, 999999)
                ],
                "twenty_ninth_doc_field": {
                    "thirtieth_doc_field": "202302",
                    "thirty_first_doc_field": "2023-02-16"
                }
            }
        }

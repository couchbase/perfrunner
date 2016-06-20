import random
import threading
import time
from collections import defaultdict

from couchbase import FMT_UTF8, Couchbase, exceptions
from couchbase._libcouchbase import (LCB_ETIMEDOUT, LCB_ETMPFAIL,
                                     LCB_KEY_ENOENT, LCB_NETWORK_ERROR,
                                     LCB_NOT_STORED)
from logger import logger

totals_lock = threading.Lock()
totals = defaultdict(int)


def sizeof_fmt(num):
    for x in ('bytes', 'KB', 'MB', 'GB', 'TB'):
        if num < 1024.0:
            return '{:3.1f} {}'.format(num, x)
        num /= 1024.0


class SyncGen(object):

    RESET_FRACTION = 0.07

    def __init__(self, iterator, conn):
        self.size = 0
        self.appends = 0
        self.adds = 0
        self.resets = 0
        self.retries = 0
        self.iterator = iterator
        # Define a thread-load RNG to ensure deterministic sequence.
        self.rng = random.Random(iterator.start)
        self.client = Couchbase.connect(**conn)

    def populate(self):
        for person in self.iterator:
            value = self.iterator.person_to_value(self.rng, person)

            # Build list of append ops for multi_append, or delete.
            d = {}
            for friend in self.iterator.graph[person]:
                key = self.iterator.person_to_key(friend)
                if self.rng.random() < self.RESET_FRACTION:
                    # 'Delete' this one. We actually set to a single-element
                    # list as this is guaranteed to not fail of another thread
                    # has already deleted it; giving more deterministic behaviour.
                    self._reset(key, value)
                else:
                    d[key] = ';' + value

                # Perform the appends.
                if d:
                    self._sync_append(d)

        # Increment the global summary by our per-thread counts.
        with totals_lock:
            for metric in ('size', 'appends', 'adds', 'resets'):
                totals[metric] += getattr(self, metric)

    def report_totals(self):
        for m in (
            '\n\tTotal documents: {0:,}'.format(len(self.iterator.graph_keys)),
            '\tTotal size:      {0}'.format(sizeof_fmt(totals['size'])),
            '\tTotal appends:   {0:,}'.format(totals['appends']),
            '\tTotal adds:      {0:,}'.format(totals['adds']),
            '\tTotal resets:    {0:,}\n'.format(totals['resets']),
        ):
            print m

    def _sync_append(self, d):
        try:
            self.client.append_multi(d, format=FMT_UTF8)
            # Data size accounting.
            for value in d.itervalues():
                self.size += len(value)
            self.appends += len(d)

        except (exceptions.NotStoredError, exceptions.NotFoundError) as e:
            # One or more keys do not yet exist, handle with add
            for k, v in e.all_results.items():
                if v.success:
                    self.appends += 1
                else:
                    if v.rc in (LCB_ETIMEDOUT, LCB_NOT_STORED, LCB_KEY_ENOENT, LCB_ETMPFAIL):
                        # Snip off semicolon for initial value.
                        initial_value = d[k][1:]
                        self._add_with_retry(k, initial_value)
                    else:
                        logger.info('RC1 {}'.format(v.rc))
                        raise

        except (exceptions.TimeoutError, exceptions.TemporaryFailError, exceptions.NetworkError) as e:
            # Similar to above, crack and retry failed.
            for k, v in e.all_results.items():
                if v.success:
                    self.appends += 1
                else:
                    if v.rc in (LCB_ETIMEDOUT, LCB_ETMPFAIL, LCB_NETWORK_ERROR, LCB_KEY_ENOENT):
                        # Snip off semicolon for initial value.
                        value = d[k][1:]
                        self._add_with_retry(k, value, key_exists=True)
                    else:
                        logger.info('RC2 {}'.format(v.rc))
                        raise

    def _reset(self, key, value):
        success = False
        backoff = 0.01
        while not success:
            try:
                self.client.set(key, value)
                self.resets += 1
                success = True
            except exceptions.TimeoutError as e:
                self.retries += 1
                logger.info(
                    'Thread-{}: _reset() sleeping for {}s due to {}'
                    .format(self.iterator.start, backoff, e)
                )
                time.sleep(backoff)
                backoff *= 2

    def _add_with_retry(self, key, value, key_exists=False):
        success = False
        backoff = 0.01
        while not success:
            try:
                if key_exists:
                    self.client.append(key, ';' + value, format=FMT_UTF8)
                    self.appends += 1
                else:
                    self.client.add(key, value, format=FMT_UTF8)
                    self.adds += 1
                    # Set is first time document is created, so increment key size.
                    self.size += len(key)
                self.size += len(value)
                success = True
            except exceptions.KeyExistsError:
                # Swap to using append
                key_exists = True
            except exceptions.NotFoundError:
                # Swap to using add
                key_exists = False
            except (exceptions.TimeoutError,
                    exceptions.TemporaryFailError) as e:
                self.retries += 1
                logger.info(
                    'Thread-{}: Sleeping for {}s due to {}'
                    .format(self.iterator.start, backoff, e)
                )
                time.sleep(backoff)
                backoff *= 2

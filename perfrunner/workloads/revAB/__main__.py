# Simulator for a 'reverse' phone book - keys are ~all phone numbers in the
# world, values are lists of phone books which contain that phone number.

# Approach: Use NetworkX to build a example representative social network.
# Iterate all nodes (i.e. users) in the graph and for each vertex (friend phone
# number in their AB) perform an `append(friend_tel, user_tel)`.


import itertools
import os
import random
import sys
import time
import threading
from multiprocessing import Pool

from couchbase import Couchbase, FMT_UTF8, exceptions
from couchbase._libcouchbase import (LCB_NOT_STORED, LCB_ETIMEDOUT,
                                     LCB_KEY_ENOENT, LCB_NETWORK_ERROR,
                                     LCB_ETMPFAIL)
from couchbase.experimental import enable as enable_experimental
enable_experimental()
from txcouchbase.connection import Connection as TxCouchbase
from twisted.internet import reactor

from perfrunner.workloads.revAB.fittingCode import socialModels

USERS = 10000
ITERATIONS = 3
RESET_FRACTION = 0.07
WORKERS = 8
BUCKET = 'default'
HOST = 'localhost'
PORT = 8091
ENGINE = 'threaded'

graph = None
graph_keys = None

size_lock = threading.Lock()
total_size = 0
total_appends = 0
total_adds = 0
total_resets = 0


def next_person(start, step):
    for k in itertools.islice(graph_keys, start, None, step):
        yield k


def sizeof_fmt(num):
    for x in ('bytes', 'KB', 'MB', 'GB', 'TB'):
        if num < 1024.0:
            return '%3.1f %s' % (num, x)
        num /= 1024.0


def person_to_key(person):
    # Pad p to typical tel number length (12 chars).
    return u'{:012}'.format(person)


def person_to_value(rng, person):
    # Should be same as key, but to speed up memory increase use a random
    # length between 12 and 400 chars.
    width = rng.randint(12, 400)
    return u'{:0{width}}'.format(person, width=width)


def produce_AB(start):
    # Repeatedly:
    # 1. Populate the graph into Couchbase; randomly delete a percentage of all
    # documents during population.
    print 'START:', start
    gen = SyncGen(start)
    for i in range(ITERATIONS):
        if start == 0:
            # Show progress (but just for 1 thread to avoid spamming)
            print '\titeration {}/{}'.format(i + 1, ITERATIONS)
        gen.populate()

    print 'END:', start


class SyncGen():
    def __init__(self, start):
        self.size = 0
        self.appends = 0
        self.adds = 0
        self.resets = 0
        self.retries = 0
        self.start = start
        # Define a thread-load RNG to ensure deterministic sequence.
        self.rng = random.Random(start)
        os.environ['LCB_NO_CCCP'] = '1'
        self.client = Couchbase.connect(bucket=BUCKET, host=HOST, port=PORT)

    def populate(self):

        for person in next_person(self.start, WORKERS):
            value = person_to_value(self.rng, person)

            # Build list of append ops for multi_append, or delete.
            d = {}
            for friend in graph[person]:
                key = person_to_key(friend)
                if self.rng.random() < RESET_FRACTION:
                    # 'Delete' this one. We actually set to a single-element
                    # list as this is guaranteed to not fail of another thread
                    # has already deleted it; giving more deterministic behaviour.
                    self._reset(key, value)
                else:
                    d[key] = ';' + value

                # Perform the appends.
                if d:
                    self._sync_append(d)

        # Increment the global size by our per-thread counts.
        global total_size, total_appends, total_adds, total_resets
        size_lock.acquire()
        total_size += self.size
        total_appends += self.appends
        total_adds += self.adds
        total_resets += self.resets
        size_lock.release()

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
                        print 'RC1', v.rc
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
                        print 'RC2', v.rc
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
                print 'Thread-{}: _reset() sleeping for {}s due to {}'.format(self.start, backoff, e)
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
                print 'Thread-{}: Sleeping for {}s due to {}'.format(self.start, backoff, e)
                time.sleep(backoff)
                backoff *= 2

count = 0


class ABGen(object):

    def __init__(self, iterator):
        self.rng = random.Random(0)
        self.client = TxCouchbase(bucket=BUCKET, host=HOST, port=PORT)
        d = self.client.connect()
        d.addCallback(self.on_connect_success)
        d.addErrback(self.on_connect_error)
        self.person_iter = iterator

    def on_connect_error(self, err):
        print 'Got error', err
        # Handle it, it's a normal Failure object
        self.client._close()
        err.trap()

    def on_connect_success(self, _):
        print 'Couchbase Connected!'
        self.process_next_person()

    def process_next_person(self):
        """Pick the next person from the graph; and add them to CB"""
        try:
            person = self.person_iter.next()
        except StopIteration:
            print 'StopIteration'
            reactor.stop()
            return
        value = person_to_value(self.rng, person)
        # Build list of append ops for multi_append
        ops = {}
        for friend in graph[person]:
            key = person_to_key(friend)
            ops[key] = ';' + value
        # Do the actual work.
        d = self.client.append_multi(ops, format=FMT_UTF8)
        d.addCallback(self._on_append)
        d.addErrback(self._on_multi_fail, ops)

    def _on_append(self, result):
        """Success, schedule next"""
        global count
        count += 1
        print '\r' + 'Processed:', count,
        sys.stdout.flush()
        self.process_next_person()

    def _on_multi_fail(self, err, ops):
        """Multi failed, crack and handle failures with set."""
        err.trap(exceptions.NotStoredError, exceptions.TimeoutError)
        if err.check(exceptions.NotStoredError):
            # One or more keys do not yet exist, handle with set
            for k, v in err.value.all_results.items():
                print 'VAL:', err.value
                if not v.success:
                    if v.rc == LCB_NOT_STORED:
                        # Snip off semicolon for initial value.
                        print 'SET:', k, ops[k][1:]
                        d = self.client.set(k, ops[k][1:], format=FMT_UTF8)
                        d.addCallback(self._on_set)
                        d.addErrback(self._on_set_fail)

        elif err == exceptions.TimeoutError:
            print 'TIMEOUT!', err
            sys.exit(1)
        else:
            print 'Unhandled error:', err
            sys.exit(1)

    def _on_set(self, result):
        pass

    def _on_set_fail(self, err):
        print 'ON_SET_FAIL', err
        sys.exit(1)


def generate_graph():
    print 'Generating graph... ',
    sys.stdout.flush()
    graph = socialModels.nearestNeighbor_mod(USERS, 0.90, 5)
    print 'done. {} nodes, {} edges'.format(graph.number_of_nodes(), graph.number_of_edges())
    return graph


def main():
    global graph
    global graph_keys
    # Seed RNG with a fixed value to give deterministic runs
    random.seed(0)
    graph = generate_graph()
    graph_keys = graph.nodes()
    random.shuffle(graph_keys)

    if ENGINE == 'twisted':
        for start in range(WORKERS):
            ABGen(iterator=next_person(start, WORKERS))

        # Then drive the event loop
        reactor.run()

    elif ENGINE == 'threaded':
        threads = list()
        for start in range(WORKERS):
            t = threading.Thread(target=produce_AB, args=(start,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    elif ENGINE == 'multiprocessing':
        iterators = [i for i in range(WORKERS)]
        pool = Pool()
        pool.map(produce_AB, iterators)

    else:
        produce_AB(0)

    size_lock.acquire()
    print
    print 'Total documents: {0:,}'.format(USERS)
    print 'Total size:      {0}'.format(sizeof_fmt(total_size))
    print 'Total appends:   {0:,}'.format(total_appends)
    print 'Total adds:      {0:,}'.format(total_adds)
    print 'Total resets:   {0:,}'.format(total_resets)
    size_lock.release()

if __name__ == '__main__':
    main()

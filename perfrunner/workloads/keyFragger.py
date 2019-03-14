"""Create pathologically bad workloads for malloc fragmentation.

Rationale
=========

Create a set of documents using a key of length A, then remove add the document
with a key of length B (where B > A).

The sizes of the keys are chosen so that documents move through the various
JEMalloc 'bins' and deliberately force fragmentation of the heap.

Implementation (code is originally a copy of pathoGen.py)
==============

Spawn a number of worker threads, arranged in a ring, with a queue linking each
pair of Workers:


            Worker A --> queue 2 --> Worker B
               ^                        |
               |                        V
     Load -> queue 1                  queue 3
               ^                        |
               |                        V
            Worker D <-- queue 4 <-- Worker C
          (Supervisor)

Queue 1 is initially populated by the Supervisor with n (batches) generator
functions, each of which returns a sequence of ascending key sizes. Each Worker
(including Supervisor) has its own input queue (which is also the output queue
of the previous Worker). A Worker pops a generator off its input queue, performs
m (batch_size) Couchbase.set() of the next size and Couchbase.remove() of the
previous size, and then pushes the generator onto its output queue, for the next
worker to then operate on. This continues, passing generator functions around
the ring until all generators have reached their maximum size.

At this point the Supervisor sleeps for a short period (to allow disk queue to
drain and memory to stabilize), then the whole process is repeated for the given
number of iterations.

Note: The Supervisor pushes 'batches' of generators to keep the total number of
tuples in the queues low, if the Supervisor pushes too many it has been seen
that the queue blocks deadlocking the test. So we generate batch_size keys from
each generator function to allow for a larger workload with smaller queue
lengths.

"""

import multiprocessing
import random
import time

from couchbase import FMT_BYTES, exceptions
from couchbase.cluster import Cluster, PasswordAuthenticator

from logger import logger

# Stored value overhead
SV_SIZE = 56 + 2

# These are the JEMalloc bins to target, the KeyFragger initialisation will use
# this list to generate key-sizes into the SIZES list
JE_MALLOC_SIZES = (64, 80, 96, 112, 128, 160, 192, 224, 256)

# This list will store the actual key sizes to use in operations.
SIZES = []

# The value of all sets is small and fixed, we don't want values hitting the
# bins we're churning with keys
VALUE_SIZE = 8
VALUE = bytearray(121 for _ in range(VALUE_SIZE))


class SequenceIterator:
    def __init__(self, max_size):
        self.pre = None
        self.sizes = list(SIZES[:SIZES.index(max_size) + 1])

    def next(self):
        if self.sizes:
            self.pre = self.sizes.pop(0)
            return self.pre
        else:
            raise StopIteration

    def previous(self):
        return self.pre


class AlwaysPromote:

    """Promotion policy for baseline case - always promote."""

    def __init__(self, max_size):
        self.max_size = max_size

    def build_generator(self, i):
        return SequenceIterator(self.max_size)


class Freeze:

    def __init__(self, batches, iterations, max_size):
        self.batches = batches
        # Initialize deterministic pseudo-RNG for when to freeze docs.
        self.rng = random.Random(0)
        self.lock = multiprocessing.Lock()
        # Aim to have 10% of documents left at the end.
        self.freeze_probability = self._calc_freeze_probability(
            iterations, 0.2)
        self.max_size = max_size

    def build_generator(self, i):
        """Return a sequence of sizes which ramps from minimum to maximum size.

        If 'freeze' is true, then freeze the sequence at a random position, i.e.
        don't ramp all the way up to max_size.
        """
        if self.rng.random() < self.freeze_probability:
            size = self.rng.choice(SIZES[:SIZES.index(self.max_size)])
            return SequenceIterator(size)
        else:
            return SequenceIterator(self.max_size)

    def _calc_freeze_probability(self, num_iterations, final_fraction):
        """Return the freeze probability (per iteration)."""
        return 1.0 - (final_fraction ** (1.0 / num_iterations))


class KeyFragger:

    def __init__(self, batches, batch_size, num_workers, num_iterations,
                 frozen_mode, host, port, bucket, password, sleep_when_done):
        self.batches = batches
        self.num_workers = num_workers
        self.sleep_when_done = sleep_when_done
        self.batch_size = batch_size

        # Create many inner iterations of subtly changing sizes, this is
        # intended to give the test a longer run-time to allow de-fragmenting
        # to take affect
        for i in range(6):
            for s in JE_MALLOC_SIZES:
                SIZES.append(i + (s - SV_SIZE))

        # Explicitly Add the max KV keylen at the end
        SIZES.append(250)

        if frozen_mode:
            logger.info('KeyFragger in frozen mode')
            max_size = SIZES[-1]
            promotion_policy = Freeze(batches, num_iterations, max_size)
        else:
            max_size = SIZES[-1]
            promotion_policy = AlwaysPromote(max_size)

        # Create queues
        self.queues = list()
        for i in range(self.num_workers):
            self.queues.append(multiprocessing.Queue())

        # Create and spin up workers
        self.workers = list()
        for i in range(self.num_workers):
            if i == self.num_workers - 1:
                # Last one is the Supervisor
                t = Supervisor(number=i,
                               host=host, port=port,
                               bucket=bucket, password=password,
                               queues=self.queues,
                               in_queue=self.queues[i],
                               out_queue=self.queues[(
                                   i + 1) % self.num_workers],
                               promotion_policy=promotion_policy,
                               batches=self.batches,
                               num_iterations=num_iterations,
                               max_size=max_size,
                               batch_size=batch_size)
            else:
                t = Worker(number=i,
                           host=host, port=port,
                           bucket=bucket, password=password,
                           in_queue=self.queues[i],
                           out_queue=self.queues[(i + 1) % self.num_workers],
                           promotion_policy=promotion_policy,
                           batch_size=batch_size)
            self.workers.append(t)

    def run(self):
        logger.info('Starting KeyFragger: {} items, {} workers'.format(
            self.batches * self.batch_size, self.num_workers))

        for t in self.workers:
            t.start()
        for t in self.workers:
            t.join()

        if self.sleep_when_done:
            logger.info('Sleeping for {}s to allow capture of'
                        ' defragmentation'.format(self.sleep_when_done))
            time.sleep(self.sleep_when_done)


class Worker(multiprocessing.Process):

    def __init__(self, number,  host, port, bucket, password, in_queue,
                 out_queue, promotion_policy, batch_size):
        super().__init__()
        self.id = number
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.promotion_policy = promotion_policy
        self.bucket = bucket
        self.password = password
        self.host = host
        self.port = port
        self.batch_size = batch_size

    def _connect(self):
        """Establish a connection to the Couchbase cluster."""
        cluster = Cluster('http://{}:{}'.format(self.host, self.port))
        authenticator = PasswordAuthenticator('Administrator', self.password)
        cluster.authenticate(authenticator)
        self.client = cluster.open_bucket(self.bucket)

    def do_work(self, index, doc):
        previous = doc.previous()
        next_size = doc.next()

        for key in range(index*self.batch_size,
                         index*self.batch_size+self.batch_size):
            if previous:
                self._del_with_retry(key, previous)

            self._set_with_retry(key, next_size)

        return next_size

    def run(self):
        """Run a Worker.

        They run essentially forever, taking document size iterators from the
        input queue and adding them to the output queue for the next guy.
        """
        # We defer creating the Couchbase object until we are actually 'in' the
        # separate process here.
        self._connect()
        while True:
            (i, doc, size) = self.in_queue.get()
            # We use a "magic" null generator to terminate the workers
            if not doc:
                # Pass the death on...
                self.out_queue.put((i, doc, size))
                break
            try:
                size = self.do_work(i, doc)
            except StopIteration:
                pass

            self.out_queue.put((i, doc, size))

    def _set_with_retry(self, key_prefix, keylen):
        success = False
        backoff = 0.01
        # Key is padded with the Worker's 'A' + id so we get A, B, C, etc...
        key = '{prefix:{fill}{align}{width}}'.format(
            prefix=key_prefix,
            fill=chr(65),
            align='<',
            width=keylen)
        while not success:
            try:
                self.client.set(key, VALUE, format=FMT_BYTES)
                success = True
            except (exceptions.TimeoutError,
                    exceptions.TemporaryFailError) as e:
                logger.debug('Worker-{0}: Sleeping for {1}s due to {2}'.format(
                    self.start, backoff, e))
                time.sleep(backoff)
                backoff *= 2

    def _del_with_retry(self, key_prefix, keylen):
        success = False
        backoff = 0.01
        key = '{prefix:{fill}{align}{width}}'.format(
            prefix=key_prefix,
            fill=chr(65),
            align='<',
            width=keylen)

        while not success:
            try:
                self.client.remove(key)
                success = True
            except (exceptions.TimeoutError,
                    exceptions.TemporaryFailError) as e:
                logger.debug('Worker-{0}: Sleeping for {1}s due to {2}'.format(
                    self.start, backoff, e))
                time.sleep(backoff)
                backoff *= 2


class Supervisor(Worker):

    def __init__(self, number, host, port, bucket, password, queues, in_queue,
                 out_queue, promotion_policy, batches, num_iterations,
                 max_size, batch_size):
        super().__init__(number, host, port, bucket, password,
                         in_queue, out_queue, promotion_policy, batch_size)
        self.queues = queues
        self.batches = batches
        self.num_iterations = num_iterations
        self.max_size = max_size
        # Configure a sleep time to allow disk queue to drain, 3 min, 10 max
        self.sleep_time = 3 + min(int(batches / 10000), 7)

    def run(self):
        """Run the Supervisor.

        This is similar to Worker, except that completed documents are not
        added back to the output queue. When the last document is seen as
        completed, a new iteration is started.
        """
        logger.info('Starting KeyFragger supervisor')

        # We defer creating the Couchbase object until we are actually
        # 'in' the separate process here.
        self._connect()

        # Create initial list of documents on the 'finished' queue
        finished_items = list()
        for i in range(self.batches):
            finished_items.append((i, 0))

        for iteration in range(self.num_iterations):

            # Create a tuple for each item in the finished queue, of
            # (doc_id, generator, doc_size). For the first iteration
            # this will be all items, for subsequent iterations it may
            # be fewer if some have been frozen.
            # Spread these across the input queues of all workers, to ensure
            # that each worker operates on different sizes.
            expected_items = len(finished_items)
            num_queues = len(self.queues)
            for (i, size) in list(finished_items):
                queue_index = i % num_queues
                self.queues[queue_index].put(
                    (i,
                     self.promotion_policy.build_generator(i),
                     0))
            finished_items = list()

            while expected_items > 0:
                (i, doc, size) = self.in_queue.get()
                try:
                    size = self.do_work(i, doc)
                    self.out_queue.put((i, doc, size))
                except StopIteration:
                    # Note: Items are not put back on out_queue at end of an
                    # iteration (unlike Worker), instead we keep for the next
                    # iteration, to build the new generators.
                    finished_items.append((i, size))
                    if len(finished_items) == expected_items:
                        # Got all items, end of iteration.
                        break

            assert self.in_queue.empty()
            assert self.out_queue.empty()

            # Any finished items which didn't reach max size should be
            # removed from the next iteration - we want to leave them
            # frozen at their last size.
            finished_items = \
                [(i, s) for (i, s) in finished_items if s == self.max_size]

            logger.info(
                'Completed iteration {}/{}'.format(iteration + 1,
                                                   self.num_iterations))
            frozen = (self.batches - len(finished_items)) * self.batch_size
            logger.info(
                'Frozen {}/{} documents'
                ' (aggregate)'.format(frozen,
                                      self.batches * self.batch_size))

            # Sleep to give the disk write queue a chance to drain.
            if iteration < self.num_iterations - 1:
                logger.info('Sleeping for {}s'.format(self.sleep_time))
                time.sleep(self.sleep_time)

        # All iterations complete. Send a special null generator
        # document around the ring - this tells the workers to shutdown.
        self.out_queue.put((-1, None, 0))


if __name__ == '__main__':
    KeyFragger(batches=10000,
               batch_size=2,
               num_workers=16,
               num_iterations=4,
               frozen_mode=True,
               host='localhost',
               port=9000,
               bucket='bucket-1',
               password='asdasd',
               sleep_when_done=None).run()

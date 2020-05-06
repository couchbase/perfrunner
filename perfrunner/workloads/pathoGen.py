"""Create pathologically bad workloads for malloc fragmentation.

Rationale
=========

Create a set of documents and then update their sizes to
each different size class of the underlying allocator. Do this across
different threads (different CouchbaseClient objects), to emulate
different clients accessing them, and to cause malloc/free in memcached
across different threads.

Implementation
==============

Spawn a number of worker threads, arranged in a ring,
with a queue linking each pair of Workers:


            Worker A --> queue 2 --> Worker B
               ^                        |
               |                        V
     Load -> queue 1                  queue 3
               ^                        |
               |                        V
            Worker D <-- queue 4 <-- Worker C
          (Supervisor)

Queue 1 is initially populated with 10,000 generator functions, each
of which returns a sequence of ascending document sizes [8, 16,
32...MAX_SIZE]. Each has it's own input queue (which is also the output
queue of the previous Worker). A worker pops a generator off it's
input queue, performs a Couchbase.set() of the next size, and then
pushes the generator onto it's output queue, for the next worker to
them operate on. This continues, passing documents around the ring
until all documents have reached their maximum size (256KB).

At this point the generator sleeps for a short period (to allow disk
queue to drain and memory to stabilize), then the whole process is
repeated (setting all documents back to 8 bytes) for the given number
of iterations.

Behaviour
=========

This gives a multiple different clients working on the same
keys, which means that memory for the documents in memcached should be
malloc'd and free'd by different memcached threads (to stress the
allocator). Additionally, the workload should be deterministic.

Note (1) To try and keep mem_used approximately constant, a Supervisor
specialization watches for the last document reaching it's maximum
size, and then starts the next iteration - this ensures that all
documents are at their maximum size at the same point in time; before
we reset back to the smallest size again.

Note (2), that num of workers should be co-prime with #documents, to
ensure that each worker sets a given document to different sizes.

Variants
========

"Frozen mode"
----------------

In the baseline case (described above), at the
start of each iteration all documents are back to the same size
(8B). This does not stress how the allocator handles "frozen"
documents - ones which stay at a given size and are not resized (which
means they cannot be re-packed during resize by the allocator).

Frozen mode tries to addresses this by not always resizing all
documents, leaving some 'frozen' and not subsequently changed. At each
iteration, a small percentage of documents are not added to the output
queue, and instead will remain at the last set size:

  AAAAAAAAAAAAAAA  iteration 1: size    8 bytes, 1000 documents
   BBBBBBBBBBBBB   iteration 1: size   16 bytes,  990 documents
    CCCCCCCCCCC    ...
     DDDDDDDDD     iteration 1: size 4096 bytes,  500 documents
      EEEEEEE      iteration 2: size    8 bytes,  495 documents
       FFFFF       iteration 2: size   16 bytes,  490 documents
        GGG        ...
         H         iteration N: size 4096 bytes,   10 documents

The effect on size-class basec allocators (e.g. tcmalloc & jemalloc)
is that they will end up with multiple pages which are assigned to
particular size (e.g. 16 bytes), but only a small fraction of each
page is actually occupied - for example two pages could exist for 16B
allocations, but after running in freeze mode there is just one slot
occupied on each page - so 32/8192 bytes are actually used, with 8160B
"unused" or fragmented.
"""

import multiprocessing
import random
import time

import pkg_resources

from logger import logger

cb_version = pkg_resources.get_distribution("couchbase").version

if cb_version[0] == '2':
    from couchbase import FMT_BYTES
    from couchbase.exceptions import TimeoutError, TemporaryFailError
    from couchbase.cluster import Cluster, PasswordAuthenticator
elif cb_version[0] == '3':
    from couchbase.exceptions import TimeoutException as TimeoutError
    from couchbase.exceptions import TemporaryFailException as TemporaryFailError
    from couchbase_core._libcouchbase import FMT_BYTES
    from couchbase.cluster import Cluster, PasswordAuthenticator


# TCMalloc size classes
SIZES = (8, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192,
         208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480,
         512, 576, 640, 704, 768, 832, 896, 960, 1024, 1152, 1280,
         1408, 1536, 1792, 2048, 2304, 2560, 2816, 3072, 3328,
         4096, 4608, 5120, 6144, 6656, 8192, 9216, 10240, 12288,
         13312, 16384, 20480, 24576, 26624, 32768, 40960, 49152,
         57344, 65536, 73728, 81920, 90112, 98304, 106496, 114688,
         122880, 131072, 139264, 147456, 155648, 163840, 172032,
         180224, 188416, 196608, 204800, 212992, 221184, 229376,
         237568, 245760, 253952, 262144)


class AlwaysPromote:

    """Promotion policy for baseline case - always promote."""

    def __init__(self, num_items, num_iterations, max_size):
        self.max_size = max_size

    def build_generator(self):
        return SequenceIterator(self.max_size)


class Freeze:

    def __init__(self, num_items, num_iterations, max_size):
        self.num_items = num_items
        # Initialize deterministic pseudo-RNG for when to freeze docs.
        self.rng = random.Random(0)
        self.lock = multiprocessing.Lock()
        # Aim to have 10% of documents left at the end.
        self.freeze_probability = self._calc_freeze_probability(num_iterations=num_iterations,
                                                                final_fraction=0.2)
        self.max_size = max_size

    def build_generator(self, i):
        """Return a sequence of sizes which ramps from minimum to maximum size.

        If 'freeze' is true, then freeze the sequence at a random position, i.e.
        don't ramp all the way up to max_size.
        """
        if self.rng.random() < self.freeze_probability:
            return SequenceIterator(self.rng.choice(SIZES[:SIZES.index(self.max_size)]))
        else:
            return SequenceIterator(self.max_size)

    def _calc_freeze_probability(self, num_iterations, final_fraction):
        """Return the freeze probability (per iteration)."""
        return 1.0 - (final_fraction ** (1.0 / num_iterations))


class PathoGen:

    def __init__(self, num_items, num_workers, num_iterations, frozen_mode,
                 host, port, bucket, password):
        self.num_items = num_items
        self.num_workers = num_workers

        if frozen_mode:
            max_size = 8192  # TCMalloc page size.
            promotion_policy = Freeze(num_items, num_iterations, max_size)
        else:
            max_size = 262144
            promotion_policy = AlwaysPromote()

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
                               out_queue=self.queues[(i + 1) % self.num_workers],
                               promotion_policy=promotion_policy,
                               num_items=self.num_items,
                               num_iterations=num_iterations,
                               max_size=max_size)
            else:
                t = Worker(number=i,
                           host=host, port=port,
                           bucket=bucket, password=password,
                           in_queue=self.queues[i],
                           out_queue=self.queues[(i + 1) % self.num_workers],
                           promotion_policy=promotion_policy)
            self.workers.append(t)

    def run(self):
        logger.info('Starting PathoGen: {} items, {} workers'.format(
            self.num_items, self.num_workers))

        for t in self.workers:
            t.start()
        for t in self.workers:
            t.join()


class Worker(multiprocessing.Process):

    def __init__(self, number, host, port, bucket, password, in_queue,
                 out_queue, promotion_policy):
        super().__init__()
        self.id = number
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.promotion_policy = promotion_policy
        self.bucket = bucket
        self.password = password
        self.host = host
        self.port = port

        # Pre-generate a buffer of the maximum size to use for constructing documents.
        self.buffer = bytearray(120 for _ in range(SIZES[-1]))

    def _connect(self):
        """Establish a connection to the Couchbase cluster."""
        cluster = Cluster('http://{}:{}'.format(self.host, self.port))
        authenticator = PasswordAuthenticator('Administrator', self.password)
        cluster.authenticate(authenticator)
        self.client = cluster.open_bucket(self.bucket)

    def run(self):
        """Run a Worker.

        They run essentially forever, taking document size iterators from the
        input queue and adding them to the output queue for the next guy.
        """
        # We defer creating the Couchbase object until we are actually 'in' the
        # separate process here.
        self._connect()

        while True:
            next_size = None
            (i, doc, size) = self.in_queue.get()
            # We use a "magic" null generator to terminate the workers
            if not doc:
                # Pass the death on...
                self.out_queue.put((i, doc, size))
                break
            # Actually perform the set.
            try:
                next_size = doc.next()
                value = self.buffer[:next_size]
                self._set_with_retry('doc_' + str(i), value)
                size = next_size
            except StopIteration:
                pass
            self.out_queue.put((i, doc, size))

    def _set_with_retry(self, key, value):
        success = False
        backoff = 0.01
        while not success:
            try:
                self.client.set(key, value, format=FMT_BYTES)
                success = True
            except (TimeoutError,
                    TemporaryFailError) as e:
                logger.debug('Worker-{0}: Sleeping for {1}s due to {2}'.format(
                    self.start, backoff, e))
                time.sleep(backoff)
                backoff *= 2


class SequenceIterator:
    def __init__(self, max_size):
        self.sizes = list(SIZES[:SIZES.index(max_size) + 1])

    def next(self):
        if self.sizes:
            return self.sizes.pop(0)
        else:
            raise StopIteration


class Supervisor(Worker):

    SLEEP_TIME = 0

    def __init__(self, number, host, port, bucket, password, queues, in_queue,
                 out_queue, promotion_policy, num_items, num_iterations,
                 max_size):
        super().__init__(number, host, port, bucket, password,
                         in_queue, out_queue, promotion_policy)
        self.queues = queues
        self.num_items = num_items
        self.num_iterations = num_iterations
        self.max_size = max_size

    def run(self):
        """Run the Supervisor.

        This is similar to Worker, except that completed documents are not
        added back to the output queue. When the last document is seen as
        completed, a new iteration is started.
        """
        logger.info('Starting PathoGen supervisor')

        # We defer creating the Couchbase object until we are actually
        # 'in' the separate process here.
        self._connect()

        # Create initial list of documents on the 'finished' queue
        finished_items = list()
        for i in range(self.num_items):
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
                    next_size = doc.next()
                    value = self.buffer[:next_size]
                    self._set_with_retry('doc_' + str(i), value)
                    size = next_size
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
            finished_items = [(ii, sz) for (ii, sz) in finished_items if sz == self.max_size]

            logger.info('Completed iteration {}/{}, frozen {}/{} documents (aggregate)'.format(
                iteration + 1, self.num_iterations,
                self.num_items - len(finished_items), self.num_items))
            # Sleep at end of iteration to give disk write queue chance to drain.
            logger.info('Sleeping for {}s'.format(self.SLEEP_TIME))
            time.sleep(self.SLEEP_TIME)

        # All iterations complete. Send a special null generator
        # document around the ring - this tells the workers to shutdown.
        self.out_queue.put((-1, None, 0))

        # Finally, set all remaining documents back to size zero.
        for (i, size) in list(finished_items):
            self._set_with_retry('doc_' + str(i), self.buffer[:0])

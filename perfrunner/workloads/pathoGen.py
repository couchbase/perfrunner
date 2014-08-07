#!/usr/bin/env python

"""PathoGen: Attempt to create a pathologically bad workload for malloc fragmentation.

Rationale: Create a set of documents and then update their sizes to
each different size class of the underlying allocator. Do this across
different threads (different CouchbaseClient objects), to emulate
diferent clients accessing them, and to cause malloc/free in memcached
across different threads.

Implementation: Spawn a number of worker threads, arranged in a ring,
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
32...262144]. Each has it's own input queue (which is also the output
queue of the previous Worker). A worker pops a generator off it's
input queue, performs a Couchbase.set() of the next size, and then
pushes the generator onto it's output queue, for the next worker to
them operate on. This continues, passing documents around the ring
until all documents have reached their maximum size (256KB).

At this point the generator sleeps for a short period (to allow disk
queue to drain and memory to stabilize), then the whole process is
repeated (setting all documents back to 8 bytes) for the given number
of iterations.

Behaviour: This gives a multiple different clients working on the same
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
"""

import itertools
import Queue
import time
import threading

from couchbase import Couchbase, FMT_BYTES, exceptions
from logger import logger


class PathoGen(object):
    """Main generator class, responsible for orchestrating the process."""

    def __init__(self, num_items, num_workers, num_iterations, host,
                 port, bucket):
        self.num_items = num_items
        self.num_workers = num_workers
        # Create queues
        self.queues = list()
        for i in range(self.num_workers):
            self.queues.append(Queue.Queue())

        # Create and spin up threads.
        self.threads = list()
        for i in range(self.num_workers):
            if i == self.num_workers - 1:
                # Last one is the Supervisor
                t = Supervisor(number=i, host=host, port=port, bucket=bucket,
                               in_queue=self.queues[i],
                               out_queue=self.queues[(i + 1) % self.num_workers],
                               num_items=self.num_items,
                               num_iterations=num_iterations)
            else:
                t = Worker(number=i, host=host, port=port, bucket=bucket,
                           in_queue=self.queues[i],
                           out_queue=self.queues[(i + 1) % self.num_workers])
            self.threads.append(t)

    def run(self):
        logger.info('Starting PathoGen: {} items, {} workers'.format(
            self.num_items, self.num_workers))

        for t in self.threads:
            t.start()
        for t in self.threads:
            t.join()


class Worker(threading.Thread):

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

    def __init__(self, number, host, port, bucket, in_queue, out_queue):
        threading.Thread.__init__(self)
        self.id = number
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.client = Couchbase.connect(bucket=bucket, host=host, port=port)

        # Pre-generate a buffer to use for constructing documents.
        self.buffer = bytearray('x' for _ in range(self.SIZES[-1]))

    def run(self):
        """Run a Worker thread. They run essentially forever, taking document
        size iterators from the input queue and adding them to the
        output queue for the next guy.
        """
        while True:
            (i, doc) = self.in_queue.get()
            # We use a "magic" null generator to terminate the workers
            if not doc:
                # Pass the death on...
                self.out_queue.put((i, doc))
                break
            try:
                next_size = doc.next()
                value = self.buffer[:next_size]
                self._set_with_retry('doc_' + str(i), value)
            except StopIteration:
                pass
            self.out_queue.put((i, doc))

    def _set_with_retry(self, key, value):
        success = False
        backoff = 0.01
        while not success:
            try:
                self.client.set(key, value, format=FMT_BYTES)
                success = True
            except (exceptions.TimeoutError,
                    exceptions.TemporaryFailError) as e:
                logger.debug('Thread-{0}: Sleeping for {1}s due to {2}'.format(
                    self.start, backoff, e))
                time.sleep(backoff)
                backoff *= 2


class Supervisor(Worker):

    SLEEP_TIME = 10

    def __init__(self, number, host, port, bucket, in_queue,
                 out_queue, num_items, num_iterations):
        super(Supervisor, self).__init__(number, host, port, bucket,
                                         in_queue, out_queue)
        self.num_items = num_items
        self.num_iterations = num_iterations

    def _build_generator(self, i):
        """Returns a sequence of sizes which ramps from minimum to maximum
        size.
        """
        return itertools.islice(self.SIZES, None)

    def run(self):
        """Run the Supervisor. This is similar to Worker, except that
        completed documents are not added back to the output
        queue. When the last document is seen as completed, a new
        iteration is started.
        """
        logger.info('Starting PathoGen supervisor')
        for iteration in range(self.num_iterations):
            # Add document iterators to our out queue. (i.e. first worker's
            # input queue).
            for i in range(self.num_items):
                self.out_queue.put((i, self._build_generator(i)))

            while True:
                (i, doc) = self.in_queue.get()
                try:
                    next_size = doc.next()
                    value = self.buffer[:next_size]
                    self._set_with_retry('doc_' + str(i), value)
                    self.out_queue.put((i, doc))
                except StopIteration:
                    # Note: Items are not put back on out_queue at end of
                    # iterator (unlike Worker)
                    if i == self.num_items - 1:
                        break

            assert self.in_queue.empty()

            logger.info('Completed iteration {}/{}, sleeping for {}s'.format(
                iteration + 1, self.num_iterations, self.SLEEP_TIME))
            # Sleep at end of iteration to give disk write queue chance to drain.
            time.sleep(self.SLEEP_TIME)

        # All iterations complete. Send a special null generator
        # document around the ring - this tells the workers to shutdown.
        # Finally, set all documents back to size zero.
        for i in range(self.num_items):
            self.out_queue.put((i, None))
            self._set_with_retry('doc_' + str(i), self.buffer[:0])


if __name__ == '__main__':
    # Small smoketest
    PathoGen(num_items=1000, num_workers=5, num_iterations=3,
             host='localhost', port=8091, bucket='default').run()

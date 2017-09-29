import json
import os
import time
from collections import defaultdict
from multiprocessing import Event, Lock, Process, Value
from random import randint

from decorator import decorator
from numpy import random
from psutil import cpu_count
from requests.auth import HTTPBasicAuth
from twisted.internet import reactor

from logger import logger
from perfrunner.helpers.sync import SyncHotWorkload
from spring.cbgen import CBAsyncGen, CBGen, ElasticGen, FtsGen, SubDocGen
from spring.docgen import (
    ArrayIndexingDocument,
    Document,
    ExtReverseLookupDocument,
    FTSKey,
    GSIMultiIndexDocument,
    HashKeys,
    ImportExportDocument,
    ImportExportDocumentArray,
    ImportExportDocumentNested,
    JoinedDocument,
    KeyForCASUpdate,
    KeyForRemoval,
    LargeDocument,
    LargeItemPlasmaDocument,
    MovingWorkingSetKey,
    NestedDocument,
    NewOrderedKey,
    ProfileDocument,
    RefDocument,
    ReverseLookupDocument,
    ReverseRangeLookupDocument,
    SequentialHotKey,
    SequentialKey,
    SequentialPlasmaDocument,
    SmallPlasmaDocument,
    String,
    UniformKey,
    UnorderedKey,
    VaryingItemSizePlasmaDocument,
    WorkingSetKey,
)
from spring.querygen import N1QLQueryGen, ViewQueryGen, ViewQueryGenByType
from spring.reservoir import Reservoir


@decorator
def with_sleep(method, *args):
    self = args[0]
    if self.target_time is None:
        return method(self)
    else:
        t0 = time.time()
        method(self)
        actual_time = time.time() - t0
        delta = self.target_time - actual_time
        if delta > 0:
            time.sleep(self.CORRECTION_FACTOR * delta)


def set_cpu_afinity(sid):
    os.system('taskset -p -c {} {}'.format(sid % cpu_count(), os.getpid()))


class Worker:

    CORRECTION_FACTOR = 0.975  # empiric!

    BATCH_SIZE = 100

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event
        self.sid = 0

        self.next_report = 0.05  # report after every 5% of completion

        self.init_keys()
        self.init_docs()
        self.init_db()

    def init_keys(self):
        self.new_keys = NewOrderedKey(prefix=self.ts.prefix,
                                      expiration=self.ws.expiration)

        if self.ws.working_set_move_time:
            self.existing_keys = MovingWorkingSetKey(self.ws.working_set,
                                                     self.ws.working_set_access,
                                                     self.ts.prefix,
                                                     self.ws.working_set_move_time,
                                                     self.ws.working_set_moving_docs)
        elif self.ws.working_set < 100:
            self.existing_keys = WorkingSetKey(self.ws.working_set,
                                               self.ws.working_set_access,
                                               self.ts.prefix)
        else:
            self.existing_keys = UniformKey(self.ts.prefix)

        self.keys_for_removal = KeyForRemoval(self.ts.prefix)

        self.fts_keys = FTSKey(self.ws)

        self.hash_keys = HashKeys(self.ws)

    def init_docs(self):
        if not hasattr(self.ws, 'doc_gen') or self.ws.doc_gen == 'basic':
            self.docs = Document(self.ws.size)
        elif self.ws.doc_gen == 'string':
            self.docs = String(self.ws.size)
        elif self.ws.doc_gen == 'nested':
            self.docs = NestedDocument(self.ws.size)
        elif self.ws.doc_gen == 'reverse_lookup':
            self.docs = ReverseLookupDocument(self.ws.size,
                                              self.ts.prefix)
        elif self.ws.doc_gen == 'reverse_range_lookup':
            self.docs = ReverseRangeLookupDocument(self.ws.size,
                                                   prefix='n1ql',
                                                   range_distance=self.ws.range_distance)
        elif self.ws.doc_gen == 'ext_reverse_lookup':
            self.docs = ExtReverseLookupDocument(self.ws.size,
                                                 self.ts.prefix,
                                                 self.ws.items)
        elif self.ws.doc_gen == 'join':
            self.docs = JoinedDocument(self.ws.size,
                                       self.ts.prefix,
                                       self.ws.items,
                                       self.ws.num_categories,
                                       self.ws.num_replies)
        elif self.ws.doc_gen == 'ref':
            self.docs = RefDocument(self.ws.size,
                                    self.ts.prefix)
        elif self.ws.doc_gen == 'array_indexing':
            self.docs = ArrayIndexingDocument(self.ws.size,
                                              self.ts.prefix,
                                              self.ws.array_size,
                                              self.ws.items)
        elif self.ws.doc_gen == 'profile':
            self.docs = ProfileDocument(self.ws.size,
                                        self.ts.prefix)
        elif self.ws.doc_gen == 'import_export_simple':
            self.docs = ImportExportDocument(self.ws.size,
                                             self.ts.prefix)
        elif self.ws.doc_gen == 'import_export_array':
            self.docs = ImportExportDocumentArray(self.ws.size,
                                                  self.ts.prefix)
        elif self.ws.doc_gen == 'import_export_nested':
            self.docs = ImportExportDocumentNested(self.ws.size,
                                                   self.ts.prefix)
        elif self.ws.doc_gen == 'large_subdoc':
            self.docs = LargeDocument(self.ws.size)
        elif self.ws.doc_gen == 'gsi_multiindex':
            self.docs = GSIMultiIndexDocument(self.ws.size)
        elif self.ws.doc_gen == 'small_plasma':
            self.docs = SmallPlasmaDocument(self.ws.size)
        elif self.ws.doc_gen == 'sequential_plasma':
            self.docs = SequentialPlasmaDocument(self.ws.size)
        elif self.ws.doc_gen == 'large_item_plasma':
            self.docs = LargeItemPlasmaDocument(self.ws.size,
                                                self.ws.item_size)
        elif self.ws.doc_gen == 'varying_item_plasma':
            self.docs = VaryingItemSizePlasmaDocument(self.ws.size,
                                                      self.ws.size_variation_min,
                                                      self.ws.size_variation_max)

    def init_db(self):
        params = {'bucket': self.ts.bucket, 'host': self.ts.node, 'port': 8091,
                  'username': self.ts.bucket, 'password': self.ts.password,
                  'use_ssl': self.ws.use_ssl}

        try:
            self.cb = CBGen(**params)
        except Exception as e:
            raise SystemExit(e)

    def report_progress(self, curr_ops):  # only first worker
        if not self.sid and self.ws.ops < float('inf') and \
                curr_ops > self.next_report * self.ws.ops:
            progress = 100.0 * curr_ops / self.ws.ops
            self.next_report += 0.05
            logger.info('Current progress: {:.2f} %'.format(progress))

    def time_to_stop(self):
        return (self.shutdown_event is not None and
                self.shutdown_event.is_set())

    def seed(self):
        random.seed(seed=self.sid * 9901)


class KVWorker(Worker):

    NAME = 'kv-worker'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.reservoir = Reservoir(num_workers=self.ws.workers)

    def gen_cmd_sequence(self, cb=None, extras=None) -> list:
        ops = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes + \
            ['fus'] * self.ws.fts_updates_swap + \
            ['fur'] * self.ws.fts_updates_reverse
        random.shuffle(ops)

        curr_items_tmp = curr_items_spot = self.curr_items.value
        if self.ws.creates:
            with self.lock:
                self.curr_items.value += self.ws.creates
                curr_items_tmp = self.curr_items.value - self.ws.creates
            curr_items_spot = (curr_items_tmp -
                               self.ws.creates * self.ws.workers)

        deleted_items_tmp = deleted_spot = 0
        if self.ws.deletes:
            with self.lock:
                self.deleted_items.value += self.ws.deletes
                deleted_items_tmp = self.deleted_items.value - self.ws.deletes
            deleted_spot = (deleted_items_tmp +
                            self.ws.deletes * self.ws.workers)

        if not cb:
            cb = self.cb

        cmds = []
        for op in ops:
            if op == 'c':
                curr_items_tmp += 1
                key, ttl = self.new_keys.next(curr_items_tmp)
                doc = self.docs.next(key)
                key = self.hash_keys.hash_it(key)
                cmds.append((None, cb.create, (key, doc, ttl)))
            elif op == 'r':
                key = self.existing_keys.next(curr_items_spot, deleted_spot)
                key = self.hash_keys.hash_it(key)

                if extras == 'subdoc':
                    cmds.append(('get', cb.read, (key, self.ws.subdoc_field)))
                elif extras == 'xattr':
                    cmds.append(('get', cb.read_xattr, (key, self.ws.xattr_field)))
                else:
                    cmds.append(('get', cb.read, (key, )))
            elif op == 'u':
                key = self.existing_keys.next(curr_items_spot, deleted_spot,
                                              self.current_hot_load_start,
                                              self.timer_elapse)
                doc = self.docs.next(key)
                key = self.hash_keys.hash_it(key)

                if extras == 'subdoc':
                    cmds.append(('set', cb.update, (key, self.ws.subdoc_field, doc)))
                elif extras == 'xattr':
                    cmds.append(('set', cb.update_xattr, (key, self.ws.xattr_field, doc)))
                else:
                    cmds.append(('set', cb.update, (key, doc)))
            elif op == 'd':
                deleted_items_tmp += 1
                key = self.keys_for_removal.next(deleted_items_tmp)
                key = self.hash_keys.hash_it(key)
                cmds.append((None, cb.delete, (key, )))
            elif op == 'fus':
                key = self.fts_keys.next()
                key = self.hash_keys.hash_it(key)
                cmds.append((None, self.do_fts_updates_swap, (key,)))
            elif op == 'fur':
                key = self.fts_keys.next()
                key = self.hash_keys.hash_it(key)
                cmds.append((None, self.do_fts_updates_reverse, (key,)))
        return cmds

    def do_fts_updates_swap(self, key):
        doc = self.cb.client.get(key).value
        if 'text' in doc and 'text2' in doc:
            tmp = doc["text2"]
            doc["text2"] = doc["text"]
            doc["text"] = tmp
        elif 'time' in doc:
            if randint(0, 1):
                doc["time"] = int(doc["time"]) >> 1
            else:
                doc["time"] = int(doc["time"]) << 1
        else:
            return
        self.cb.client.set(key, doc)

    def do_fts_updates_reverse(self, key):
        doc = self.cb.client.get(key).value
        words = doc["name"].split(' ')
        if len(words):
            doc["name"] = ' '.join(words[::-1])
            self.cb.client.set(key, doc)

    @with_sleep
    def do_batch(self, *args, **kwargs):
        for cmd, func, args in self.gen_cmd_sequence():
            latency = func(*args)
            if latency is not None:
                self.reservoir.update(operation=cmd, value=latency)

    def run_condition(self, curr_ops):
        return curr_ops.value < self.ws.ops and not self.time_to_stop()

    def run(self, sid, lock, curr_ops, curr_items, deleted_items,
            current_hot_load_start=None, timer_elapse=None):

        if self.ws.throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.ws.workers / \
                self.ws.throughput
        else:
            self.target_time = None
        self.sid = sid
        self.lock = lock
        self.curr_items = curr_items
        self.deleted_items = deleted_items
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse

        self.seed()

        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
        try:
            while self.run_condition(curr_ops):
                with lock:
                    curr_ops.value += self.BATCH_SIZE
                self.do_batch()
                self.report_progress(curr_ops.value)
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))

        self.reservoir.dump(filename='{}-{}'.format(self.NAME, self.sid))


class SubDocWorker(KVWorker):

    NAME = 'sub-doc-worker'

    def init_db(self):
        params = {'bucket': self.ts.bucket, 'host': self.ts.node, 'port': 8091,
                  'username': self.ts.bucket, 'password': self.ts.password}
        self.cb = SubDocGen(**params)

    def gen_cmd_sequence(self, cb=None, *args):
        return super().gen_cmd_sequence(cb, extras='subdoc')


class XATTRWorker(SubDocWorker):

    NAME = 'xattr-worker'

    def gen_cmd_sequence(self, cb=None, *args):
        return super(SubDocWorker, self).gen_cmd_sequence(cb, extras='xattr')


class AsyncKVWorker(KVWorker):

    NAME = 'async-kv-worker'

    NUM_CONNECTIONS = 8

    def init_db(self):
        params = {'bucket': self.ts.bucket, 'host': self.ts.node, 'port': 8091,
                  'username': self.ts.bucket, 'password': self.ts.password}

        self.cbs = [CBAsyncGen(**params) for _ in range(self.NUM_CONNECTIONS)]
        self.counter = list(range(self.NUM_CONNECTIONS))

    def restart(self, _, cb, i):
        self.counter[i] += 1
        if self.counter[i] == self.BATCH_SIZE:
            actual_time = time.time() - self.time_started
            if self.target_time is not None:
                delta = self.target_time - actual_time
                if delta > 0:
                    time.sleep(self.CORRECTION_FACTOR * delta)

            self.report_progress(self.curr_ops.value)
            if not self.done and (
                    self.curr_ops.value >= self.ws.ops or self.time_to_stop()):
                with self.lock:
                    self.done = True
                logger.info('Finished: {}-{}'.format(self.NAME, self.sid))
                reactor.stop()
            else:
                self.do_batch(_, cb, i)

    def do_batch(self, _, cb, i):
        self.counter[i] = 0
        self.time_started = time.time()

        with self.lock:
            self.curr_ops.value += self.BATCH_SIZE

        for _, func, args in self.gen_cmd_sequence(cb):
            d = func(*args)
            d.addCallback(self.restart, cb, i)
            d.addErrback(self.log_and_restart, cb, i)

    def log_and_restart(self, err, cb, i):
        logger.warn('Request problem with worker-{} thread-{}: {}'.format(
            self.sid, i, err.value)
        )
        self.restart(None, cb, i)

    def error(self, err, cb, i):
        logger.warn('Connection problem with worker-{} thread-{}: {}'.format(
            self.sid, i, err)
        )

        cb.client._close()
        time.sleep(15)
        d = cb.client.connect()
        d.addCallback(self.do_batch, cb, i)
        d.addErrback(self.error, cb, i)

    def run(self, sid, lock, curr_ops, curr_items, deleted_items,
            current_hot_load_start=None, timer_elapse=None):
        set_cpu_afinity(sid)

        if self.ws.throughput < float('inf'):
            self.target_time = (self.BATCH_SIZE * self.ws.workers /
                                float(self.ws.throughput))
        else:
            self.target_time = None

        self.sid = sid
        self.lock = lock
        self.curr_items = curr_items
        self.deleted_items = deleted_items
        self.curr_ops = curr_ops
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse

        self.seed()

        self.done = False
        for i, cb in enumerate(self.cbs):
            d = cb.client.connect()
            d.addCallback(self.do_batch, cb, i)
            d.addErrback(self.error, cb, i)
        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
        reactor.run()


class SeqReadsWorker(Worker):

    def run(self, sid, *args):
        set_cpu_afinity(sid)

        for key in SequentialHotKey(sid, self.ws, self.ts.prefix):
            key = self.hash_keys.hash_it(key)
            self.cb.read(key)


class SeqUpsertsWorker(Worker):

    def run(self, sid, *args):
        for key in SequentialKey(sid, self.ws, self.ts.prefix):
            doc = self.docs.next(key)
            key = self.hash_keys.hash_it(key)
            self.cb.update(key, doc)


class RandUpsertsWorker(Worker):

    def run(self, sid, *args):
        for key in UnorderedKey(sid, self.ws, self.ts.prefix):
            doc = self.docs.next(key)
            key = self.hash_keys.hash_it(key)
            self.cb.update(key, doc)


class SeqXATTRUpdatesWorker(XATTRWorker):

    def run(self, sid, *args):
        for key in SequentialKey(sid, self.ws, self.ts.prefix):
            doc = self.docs.next(key)
            key = self.hash_keys.hash_it(key)
            self.cb.update_xattr(key, self.ws.xattr_field, doc)


class WorkerFactory:

    def __new__(cls, settings):
        if getattr(settings, 'async', None):
            worker = AsyncKVWorker
        elif getattr(settings, 'seq_upserts') and \
                getattr(settings, 'xattr_field', None):
            worker = SeqXATTRUpdatesWorker
        elif getattr(settings, 'rand_upserts', None):
            worker = RandUpsertsWorker
        elif getattr(settings, 'seq_upserts', None):
            worker = SeqUpsertsWorker
        elif getattr(settings, 'seq_reads', None):
            worker = SeqReadsWorker
        elif getattr(settings, 'subdoc_field', None):
            worker = SubDocWorker
        elif getattr(settings, 'xattr_field', None):
            worker = XATTRWorker
        else:
            worker = KVWorker
        return worker, settings.workers


class ViewWorkerFactory:

    def __new__(cls, workload_settings):
        return ViewWorker, workload_settings.query_workers


class ViewWorker(Worker):

    NAME = 'query-worker'

    def __init__(self, workload_settings, target_settings, shutdown_event):
        super().__init__(workload_settings, target_settings, shutdown_event)

        self.total_workers = self.ws.query_workers
        self.throughput = self.ws.query_throughput

        self.reservoir = Reservoir(num_workers=self.ws.query_workers)

        if workload_settings.index_type is None:
            self.new_queries = ViewQueryGen(workload_settings.ddocs,
                                            workload_settings.query_params)
        else:
            self.new_queries = ViewQueryGenByType(workload_settings.index_type,
                                                  workload_settings.query_params)

    @with_sleep
    def do_batch(self):
        curr_items_spot = \
            self.curr_items.value - self.ws.creates * self.ws.workers
        deleted_spot = \
            self.deleted_items.value + self.ws.deletes * self.ws.workers

        for _ in range(self.BATCH_SIZE):
            key = self.existing_keys.next(curr_items_spot, deleted_spot)
            doc = self.docs.next(key)
            ddoc_name, view_name, query = self.new_queries.next(doc)

            latency = self.cb.view_query(ddoc_name, view_name, query=query)

            self.reservoir.update(operation='query', value=latency)

    def run(self, sid, lock, curr_ops, curr_items, deleted_items, *args):
        self.cb.start_updater()

        if self.throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.total_workers / \
                self.throughput
        else:
            self.target_time = None
        self.sid = sid
        self.curr_items = curr_items
        self.deleted_items = deleted_items

        try:
            logger.info('Started: {}-{}'.format(self.NAME, self.sid))
            while not self.time_to_stop():
                self.do_batch()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))

        self.reservoir.dump(filename='{}-{}'.format(self.NAME, self.sid))


class N1QLWorkerFactory:

    def __new__(cls, workload_settings):
        return N1QLWorker, workload_settings.n1ql_workers


class N1QLWorker(Worker):

    NAME = 'query-worker'

    def __init__(self, workload_settings, target_settings, shutdown_event):
        self.new_queries = N1QLQueryGen(workload_settings.n1ql_queries)
        self.total_workers = workload_settings.n1ql_workers
        self.throughput = workload_settings.n1ql_throughput

        self.reservoir = Reservoir(num_workers=workload_settings.n1ql_workers)

        super().__init__(workload_settings, target_settings, shutdown_event)

        self.init_creds()

    def init_keys(self):
        self.new_keys = NewOrderedKey(prefix='n1ql',
                                      expiration=self.ws.expiration)

        self.existing_keys = UniformKey(prefix='n1ql')

        self.keys_for_cas_update = KeyForCASUpdate(self.total_workers,
                                                   prefix='n1ql')

    def init_docs(self):
        if self.ws.doc_gen == 'reverse_lookup':
            self.docs = ReverseLookupDocument(self.ws.size,
                                              prefix='n1ql')
        elif self.ws.doc_gen == 'reverse_range_lookup':
            self.docs = ReverseRangeLookupDocument(self.ws.size,
                                                   prefix='n1ql',
                                                   range_distance=self.ws.range_distance)
        elif self.ws.doc_gen == 'ext_reverse_lookup':
            self.docs = ExtReverseLookupDocument(self.ws.size,
                                                 prefix='n1ql',
                                                 num_docs=self.ws.items)
        elif self.ws.doc_gen == 'join':
            self.docs = JoinedDocument(self.ws.size,
                                       prefix='n1ql',
                                       num_docs=self.ws.items,
                                       num_categories=self.ws.num_categories,
                                       num_replies=self.ws.num_replies)
        elif self.ws.doc_gen == 'ref':
            self.docs = RefDocument(self.ws.size,
                                    prefix='n1ql')
        elif self.ws.doc_gen == 'profile':
            self.docs = ProfileDocument(self.ws.size,
                                        prefix='n1ql')
        elif self.ws.doc_gen == 'array_indexing':
            self.docs = ArrayIndexingDocument(self.ws.size,
                                              prefix='n1ql',
                                              array_size=self.ws.array_size,
                                              num_docs=self.ws.items)

    def init_creds(self):
        for bucket in getattr(self.ws, 'buckets', []):
            self.cb.client.add_bucket_creds(bucket, self.ts.password)

    def read(self):
        curr_items_tmp = self.curr_items.value
        if self.ws.doc_gen == 'ext_reverse_lookup':
            curr_items_tmp //= 4

        for _ in range(self.ws.n1ql_batch_size):
            key = self.existing_keys.next(curr_items=curr_items_tmp,
                                          curr_deletes=0)
            doc = self.docs.next(key)
            query = self.new_queries.next(key, doc)

            latency = self.cb.n1ql_query(query)
            self.reservoir.update(operation='query', value=latency)

    def create(self):
        with self.lock:
            self.curr_items.value += self.ws.n1ql_batch_size
            curr_items_tmp = self.curr_items.value - self.ws.n1ql_batch_size

        for _ in range(self.ws.n1ql_batch_size):
            curr_items_tmp += 1
            key, ttl = self.new_keys.next(curr_items=curr_items_tmp)
            doc = self.docs.next(key)
            query = self.new_queries.next(key, doc)

            latency = self.cb.n1ql_query(query)
            self.reservoir.update(operation='query', value=latency)

    def update(self):
        with self.lock:
            curr_items_tmp = self.curr_items.value

        for _ in range(self.ws.n1ql_batch_size):
            key = self.keys_for_cas_update.next(sid=self.sid,
                                                curr_items=curr_items_tmp)
            doc = self.docs.next(key)
            query = self.new_queries.next(key, doc)

            latency = self.cb.n1ql_query(query)
            self.reservoir.update(operation='query', value=latency)

    @with_sleep
    def do_batch(self):
        if self.ws.n1ql_op == 'read':
            self.read()
        elif self.ws.n1ql_op == 'create':
            self.create()
        elif self.ws.n1ql_op == 'update':
            self.update()

    def run(self, sid, lock, curr_ops, curr_items, *args):
        if self.throughput < float('inf'):
            self.target_time = self.ws.n1ql_batch_size * self.total_workers / \
                float(self.throughput)
        else:
            self.target_time = None
        self.lock = lock
        self.sid = sid
        self.curr_items = curr_items

        try:
            logger.info('Started: {}-{}'.format(self.NAME, self.sid))
            while not self.time_to_stop():
                self.do_batch()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))

        self.reservoir.dump(filename='{}-{}'.format(self.NAME, self.sid))


class FtsWorkerFactory:

    def __new__(cls, workload_settings):
        if workload_settings.fts_config:
            return FtsWorker, workload_settings.fts_config.worker
        return FtsWorker, 0


class FtsWorker(Worker):

    NAME = "fts-es-worker"

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        super().__init__(workload_settings, target_settings, shutdown_event)
        self.query_gen = workload_settings.query_gen
        self.query_list = workload_settings.query_gen.query_list
        self.query_list_size = len(workload_settings.query_gen.query_list)

    def init_keys(self):
        pass

    def init_docs(self):
        pass

    def init_db(self):
        pass

    def do_check_result(self, r):
        result = json.loads(r.data.decode('utf-8'))
        if self.ws.fts_config.elastic:
            return result["hits"]["total"] == 0
        return result['total_hits'] == 0

    def validate_response(self, r, args):
        if not self.ws.fts_config.logfile:
            return

        if r.status not in range(200, 203) or self.do_check_result(r):
            with open(self.ws.fts_config.logfile, 'a') as f:
                f.write(str(args))
                f.write(str(r.status_code))
                f.write(str(r.data))

    def do_batch(self):
        for i in range(self.BATCH_SIZE):
            args = self.query_list[random.randint(self.query_list_size - 1)]
            response = self.query_gen.execute_query(args)
            if self.sid == 0:
                self.validate_response(response, args)

    def run(self, sid, *args):
        logger.info("Started {}".format(self.NAME))
        self.sid = sid
        try:
            logger.info('Started: {}-{}'.format(self.NAME, self.sid))
            while not self.time_to_stop():
                self.do_batch()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))


class WorkloadGen:

    def __init__(self, workload_settings, target_settings, timer=None, *args):
        self.ws = workload_settings
        self.ts = target_settings
        self.timer = timer
        self.shutdown_event = timer and Event() or None
        self.worker_processes = defaultdict(list)

    def start_workers(self,
                      worker_factory,
                      name,
                      curr_items=None,
                      deleted_items=None,
                      current_hot_load_start=None,
                      timer_elapse=None):
        curr_ops = Value('L', 0)
        lock = Lock()
        worker_type, total_workers = worker_factory(self.ws)
        if name == 'fts' and total_workers:
            auth = HTTPBasicAuth(self.ws.fts_config.username, self.ts.password)
            if self.ws.fts_config.elastic:
                self.ws.query_gen = ElasticGen(self.ts.node, self.ws.fts_config, auth)
            else:
                self.ws.query_gen = FtsGen(self.ts.node, self.ws.fts_config, auth)

        for sid in range(total_workers):
            args = (sid, lock, curr_ops, curr_items, deleted_items,
                    current_hot_load_start, timer_elapse)

            worker = worker_type(self.ws, self.ts, self.shutdown_event)

            worker_process = Process(target=worker.run, args=args)
            worker_process.start()
            self.worker_processes[name].append(worker_process)

            if getattr(self.ws, 'async', False):
                time.sleep(2)

    def wait_for_all_workers(self):
        for processes in self.worker_processes.values():
            for process in processes:
                process.join()
                if process.exitcode:
                    logger.interrupt('Worker finished with non-zero exit code')

    def run(self):
        curr_items = Value('L', self.ws.items)
        deleted_items = Value('L', 0)
        current_hot_load_start = Value('L', 0)
        timer_elapse = Value('I', 0)

        if self.ws.working_set_move_time:
            current_hot_load_start.value = int(self.ws.items * self.ws.working_set / 100)

        logger.info('Starting all workers')

        self.start_workers(WorkerFactory,
                           'kv', curr_items, deleted_items,
                           current_hot_load_start, timer_elapse)
        self.start_workers(ViewWorkerFactory,
                           'view', curr_items, deleted_items)
        self.start_workers(N1QLWorkerFactory,
                           'n1ql', curr_items, deleted_items)
        self.start_workers(FtsWorkerFactory, 'fts')

        sync = SyncHotWorkload(current_hot_load_start, timer_elapse)
        sync.start_timer(self.ws)

        if self.timer and self.ws.ops == float('inf'):
            time.sleep(self.timer)
            self.shutdown_event.set()
        self.wait_for_all_workers()

        sync.stop_timer()

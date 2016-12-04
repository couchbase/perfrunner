import os
import time
from collections import defaultdict
from multiprocessing import Event, Lock, Process, Value
import requests

from couchbase.exceptions import ValueFormatError
from decorator import decorator
from logger import logger
from numpy import random
from psutil import cpu_count

from twisted.internet import reactor

from spring.cbgen import (
    CBAsyncGen,
    CBGen,
    ElasticGen,
    FtsGen,
    N1QLGen,
    SubDocGen,
)
from spring.docgen import (
    ArrayIndexingDocument,
    Document,
    ExistingKey,
    ExtReverseLookupDocument,
    FTSKey,
    ImportExportDocument,
    ImportExportDocumentArray,
    ImportExportDocumentNested,
    JoinedDocument,
    KeyForCASUpdate,
    KeyForRemoval,
    LargeDocument,
    NestedDocument,
    NewKey,
    RefDocument,
    ReverseLookupDocument,
    ReverseRangeLookupDocument,
    SequentialHotKey,
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


class Worker(object):

    CORRECTION_FACTOR = 0.975  # empiric!

    BATCH_SIZE = 100

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event

        self.next_report = 0.05  # report after every 5% of completion

        self.init_keys()
        self.init_docs()
        self.init_db()

    def init_keys(self):
        self.existing_keys = ExistingKey(self.ws.working_set,
                                         self.ws.working_set_access,
                                         self.ts.prefix)
        self.new_keys = NewKey(self.ts.prefix, self.ws.expiration)
        self.keys_for_removal = KeyForRemoval(self.ts.prefix)
        self.fts_keys = FTSKey(self.ws)

    def init_docs(self):
        if not hasattr(self.ws, 'doc_gen') or self.ws.doc_gen == 'basic':
            self.docs = Document(self.ws.size)
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

    def init_db(self):
        host, port = self.ts.node.split(':')
        params = {'bucket': self.ts.bucket, 'host': host, 'port': port,
                  'username': self.ts.bucket, 'password': self.ts.password}

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


class KVWorker(Worker):

    NAME = 'kv-worker'

    def gen_cmd_sequence(self, cb=None, cases="cas"):
        ops = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes + \
            ['fu'] * self.ws.fts_updates + \
            [cases] * self.ws.cases
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
                cmds.append((cb.create, (key, doc, ttl)))
            elif op == 'r':
                key = self.existing_keys.next(curr_items_spot, deleted_spot)
                if cases == 'counter':
                    cmds.append((cb.read, (key, self.ws.subdoc_fields)))
                else:
                    cmds.append((cb.read, (key, )))
            elif op == 'u':
                if cases == 'counter':
                    key = self.existing_keys.next(curr_items_spot, deleted_spot)
                    cmds.append((cb.update, (key, self.ws.subdoc_fields, self.ws.size)))
                else:
                    key = self.existing_keys.next(curr_items_spot, deleted_spot)
                    doc = self.docs.next(key)
                    cmds.append((cb.update, (key, doc)))
            elif op == 'd':
                deleted_items_tmp += 1
                key = self.keys_for_removal.next(deleted_items_tmp)
                cmds.append((cb.delete, (key, )))
            elif op == 'cas':
                key = self.existing_keys.next(curr_items_spot, deleted_spot)
                doc = self.docs.next(key)
                cmds.append((cb.cas, (key, doc)))
            elif op == 'counter':
                key = self.existing_keys.next(curr_items_spot, deleted_spot)
                cmds.append((cb.cas, (key, self.ws.subdoc_counter_fields)))
            elif op == 'fu':
                key = self.fts_keys.next()
                cmds.append((cb.fts_update, (key, )))
        return cmds

    @with_sleep
    def do_batch(self, *args, **kwargs):
        for cmd, args in self.gen_cmd_sequence():
            cmd(*args)

    def run_condition(self, curr_ops):
        if self.ws.operations:
            return curr_ops.value < self.ws.items
        else:
            return curr_ops.value < self.ws.ops and not self.time_to_stop()

    def run(self, sid, lock, curr_ops, curr_items, deleted_items):
        if self.ws.throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.ws.workers / \
                self.ws.throughput
        else:
            self.target_time = None
        self.sid = sid
        self.lock = lock
        self.curr_items = curr_items
        self.deleted_items = deleted_items

        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
        try:
            while self.run_condition(curr_ops):
                with lock:
                    curr_ops.value += self.BATCH_SIZE
                self.do_batch()
                self.report_progress(curr_ops.value)
        except (KeyboardInterrupt, ValueFormatError):
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))


class SubDocWorker(KVWorker):

    NAME = 'sub-doc-worker'

    def init_db(self):
        host, port = self.ts.node.split(':')
        params = {'bucket': self.ts.bucket, 'host': host, 'port': port,
                  'username': self.ts.bucket, 'password': self.ts.password}
        self.cb = SubDocGen(**params)

    def gen_cmd_sequence(self, cb=None, *args):
        return super(SubDocWorker, self).gen_cmd_sequence(cb, cases='counter')


class AsyncKVWorker(KVWorker):

    NAME = 'async-kv-worker'

    NUM_CONNECTIONS = 8

    def init_db(self):
        host, port = self.ts.node.split(':')
        params = {'bucket': self.ts.bucket, 'host': host, 'port': port,
                  'username': self.ts.bucket, 'password': self.ts.password}

        self.cbs = [CBAsyncGen(**params) for _ in range(self.NUM_CONNECTIONS)]
        self.counter = range(self.NUM_CONNECTIONS)

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

        for cmd, args in self.gen_cmd_sequence(cb):
            d = cmd(*args)
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

    def run(self, sid, lock, curr_ops, curr_items, deleted_items):
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

        self.done = False
        for i, cb in enumerate(self.cbs):
            d = cb.client.connect()
            d.addCallback(self.do_batch, cb, i)
            d.addErrback(self.error, cb, i)
        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
        reactor.run()


class SeqReadsWorker(Worker):

    def run(self, sid, *args, **kwargs):
        set_cpu_afinity(sid)

        for key in SequentialHotKey(sid, self.ws, self.ts.prefix):
            self.cb.read(key)


class SeqUpdatesWorker(Worker):

    def run(self, sid, *args, **kwargs):
        for key in SequentialHotKey(sid, self.ws, self.ts.prefix):
            doc = self.docs.next(key)
            self.cb.update(key, doc)


class WorkerFactory(object):

    def __new__(cls, workload_settings):
        if getattr(workload_settings, 'async', False):
            worker = AsyncKVWorker
        elif getattr(workload_settings, 'seq_updates', False):
            worker = SeqUpdatesWorker
        elif getattr(workload_settings, 'seq_reads', False):
            worker = SeqReadsWorker
        else:
            worker = KVWorker
        return worker, workload_settings.workers


class SubDocWorkerFactory(object):

    def __new__(cls, workload_settings):
        return SubDocWorker, workload_settings.subdoc_workers


class ViewWorkerFactory(object):

    def __new__(cls, workload_settings):
        return ViewWorker, workload_settings.query_workers


class ViewWorker(Worker):

    NAME = 'view-worker'

    def __init__(self, workload_settings, target_settings, shutdown_event):
        super(ViewWorker, self).__init__(workload_settings, target_settings,
                                         shutdown_event)

        self.total_workers = self.ws.query_workers
        self.throughput = self.ws.query_throughput

        if workload_settings.index_type is None:
            self.new_queries = ViewQueryGen(workload_settings.ddocs,
                                            workload_settings.qparams)
        else:
            self.new_queries = ViewQueryGenByType(workload_settings.index_type,
                                                  workload_settings.qparams)

    @with_sleep
    def do_batch(self):
        curr_items_spot = \
            self.curr_items.value - self.ws.creates * self.ws.workers
        deleted_spot = \
            self.deleted_items.value + self.ws.deletes * self.ws.workers

        for _ in range(self.BATCH_SIZE):
            key = self.existing_keys.next(curr_items_spot, deleted_spot)
            doc = self.docs.next(key)
            doc['key'] = key
            doc['bucket'] = self.ts.bucket
            ddoc_name, view_name, query = self.new_queries.next(doc)
            self.cb.query(ddoc_name, view_name, query=query)

    def run(self, sid, lock, curr_queries, curr_items, deleted_items):
        self.cb.start_updater()

        if self.throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.total_workers / \
                self.throughput
        else:
            self.target_time = None
        self.sid = sid
        self.curr_items = curr_items
        self.deleted_items = deleted_items
        self.curr_queries = curr_queries

        try:
            logger.info('Started: {}-{}'.format(self.NAME, self.sid))
            while curr_queries.value < self.ws.ops and not self.time_to_stop():
                with lock:
                    curr_queries.value += self.BATCH_SIZE
                self.do_batch()
        except (KeyboardInterrupt, ValueFormatError, AttributeError) as e:
            logger.info('Interrupted: {}-{}, {}'.format(self.NAME, self.sid, e))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))


class N1QLWorkerFactory(object):

    def __new__(cls, workload_settings):
        return N1QLWorker, workload_settings.n1ql_workers


class N1QLWorker(Worker):

    NAME = 'n1ql-worker'

    def __init__(self, workload_settings, target_settings, shutdown_event):
        self.new_queries = N1QLQueryGen(workload_settings.n1ql_queries)
        self.total_workers = workload_settings.n1ql_workers
        self.throughput = workload_settings.n1ql_throughput

        self.reservoir = Reservoir(num_workers=workload_settings.n1ql_workers)

        super(N1QLWorker, self).__init__(workload_settings, target_settings,
                                         shutdown_event)

    def init_keys(self):
        self.existing_keys = ExistingKey(self.ws.working_set,
                                         self.ws.working_set_access,
                                         prefix='n1ql')
        self.new_keys = NewKey(prefix='n1ql', expiration=self.ws.expiration)
        self.keys_for_casupdate = KeyForCASUpdate(self.total_workers,
                                                  self.ws.working_set,
                                                  self.ws.working_set_access,
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
        elif self.ws.doc_gen == 'array_indexing':
            self.docs = ArrayIndexingDocument(self.ws.size,
                                              prefix='n1ql',
                                              array_size=self.ws.array_size,
                                              num_docs=self.ws.items)

    def init_db(self):
        host, port = self.ts.node.split(':')
        self.cb = N1QLGen(bucket=self.ts.bucket, password=self.ts.password,
                          host=host, port=port)

    def read(self):
        curr_items_tmp = self.curr_items.value
        if self.ws.doc_gen == 'ext_reverse_lookup':
            curr_items_tmp /= 4

        for _ in range(self.BATCH_SIZE):
            key = self.existing_keys.next(curr_items=curr_items_tmp,
                                          curr_deletes=0)
            doc = self.docs.next(key)
            doc['key'] = key
            doc['bucket'] = self.ts.bucket
            query = self.new_queries.next(doc)

            _, latency = self.cb.query(query)
            self.reservoir.update(latency)

    def create(self):
        with self.lock:
            self.curr_items.value += self.BATCH_SIZE
            curr_items_tmp = self.curr_items.value - self.BATCH_SIZE

        for _ in range(self.BATCH_SIZE):
            curr_items_tmp += 1
            key, ttl = self.new_keys.next(curr_items=curr_items_tmp)
            doc = self.docs.next(key)
            doc['key'] = key
            doc['bucket'] = self.ts.bucket
            query = self.new_queries.next(doc)

            _, latency = self.cb.query(query)
            self.reservoir.update(latency)

    def update(self):
        with self.lock:
            self.cas_updated_items.value += self.BATCH_SIZE
            curr_items_tmp = self.curr_items.value - self.BATCH_SIZE

        for _ in range(self.BATCH_SIZE):
            key = self.keys_for_casupdate.next(self.sid,
                                               curr_items=curr_items_tmp,
                                               curr_deletes=0)
            doc = self.docs.next(key)
            doc['key'] = key
            doc['bucket'] = self.ts.bucket
            query = self.new_queries.next(doc)

            _, latency = self.cb.query(query)
            self.reservoir.update(latency)

    def range_update(self):
        with self.lock:
            self.cas_updated_items.value += self.BATCH_SIZE
            curr_items_tmp = self.curr_items.value - self.BATCH_SIZE

        for _ in range(self.BATCH_SIZE):
            key = self.keys_for_casupdate.next(self.sid,
                                               curr_items=curr_items_tmp,
                                               curr_deletes=0)
            doc = self.docs.next(key)
            doc['key'] = key
            doc['bucket'] = self.ts.bucket
            query = self.new_queries.next(doc)

            _, latency = self.cb.query(query)
            self.reservoir.update(latency)

    @with_sleep
    def do_batch(self):
        if self.ws.n1ql_op == 'read':
            self.read()
        elif self.ws.n1ql_op == 'create':
            self.create()
        elif self.ws.n1ql_op == 'update':
            self.update()
        elif self.ws.n1ql_op == 'rangeupdate':
            self.range_update()

    def run(self, sid, lock, curr_queries, curr_items, deleted_items,
            cas_updated_items):

        if self.throughput < float('inf'):
            self.target_time = float(self.BATCH_SIZE) * self.total_workers / \
                self.throughput
        else:
            self.target_time = None
        self.lock = lock
        self.sid = sid
        self.curr_items = curr_items
        self.cas_updated_items = cas_updated_items
        self.curr_queries = curr_queries

        try:
            logger.info('Started: {}-{}'.format(self.NAME, self.sid))
            while curr_queries.value < self.ws.ops and not self.time_to_stop():
                with self.lock:
                    curr_queries.value += self.BATCH_SIZE
                self.do_batch()
        except (KeyboardInterrupt, ValueFormatError, AttributeError) as e:
            logger.info('Interrupted: {}-{}-{}'.format(self.NAME, self.sid, e))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))

        self.reservoir.dump(filename='{}-{}'.format(self.NAME, self.sid))


class FtsWorkerFactory(object):

    def __new__(cls, workload_settings):
        if workload_settings.fts_config:
            return FtsWorker, workload_settings.fts_config.worker
        return FtsWorker, 0


class FtsWorker(Worker):

    BATCH_SIZE = 100
    NAME = "fts-es-worker"

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        super(FtsWorker, self).__init__(workload_settings, target_settings, shutdown_event)
        self.query_list = workload_settings.fts_query_list
        self.query_list_size = workload_settings.fts_query_list_size
        self.requests = requests.Session()

    def init_keys(self):
        pass

    def init_docs(self):
        pass

    def init_db(self):
        pass

    def do_check_result(self, r):
        if self.ws.fts_config.elastic:
            return r.json()["hits"]["total"] == 0
        return r.json()['total_hits'] == 0

    def do_batch(self):
        for i in range(self.BATCH_SIZE):
            if not self.time_to_stop():
                args = self.query_list[random.randint(self.query_list_size - 1)]
                if self.sid == 0:
                    try:
                        r = self.requests.post(**args)
                        if not self.ws.fts_config.logfile:
                            continue

                        if r.status_code not in range(200, 203) or self.do_check_result(r):
                            with open(self.ws.fts_config.logfile, 'a') as f:
                                f.write(str(args))
                                f.write(str(r.status_code))
                                f.write(str(r.text))
                    except IOError as e:
                        logger.info("I/O error({0}): {1}".format(e.errno, e.strerror))
                else:
                    self.requests.post(**args)

    def run(self, sid, lock):
        logger.info("Started {}".format(self.NAME))
        self.sid = sid
        try:
            logger.info('Started: {}-{}'.format(self.NAME, self.sid))
            while not self.time_to_stop():
                self.do_batch()
        except (KeyboardInterrupt, ValueFormatError, AttributeError) as e:
            logger.info('Interrupted: {}-{}-{}'.format(self.NAME, self.sid, e))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))


class WorkloadGen(object):

    def __init__(self, workload_settings, target_settings, timer=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.timer = timer
        self.shutdown_event = timer and Event() or None
        self.worker_processes = defaultdict(list)

    def start_workers(self, worker_factory, name, curr_items=None,
                      deleted_items=None, cas_updated_items=None):
        curr_ops = Value('L', 0)
        lock = Lock()
        worker_type, total_workers = worker_factory(self.ws)
        if name == 'fts' and total_workers:
            rest_auth = (self.ws.fts_config.username, self.ts.password)
            master_host = self.ts.node.split(":")[0]
            if self.ws.fts_config.elastic:
                self.ws.fts_query_list = ElasticGen(master_host, self.ws.fts_config, rest_auth).query_list
            else:
                self.ws.fts_query_list = FtsGen(master_host, self.ws.fts_config, rest_auth).query_list
            self.ws.fts_query_list_size = len(self.ws.fts_query_list)

        for sid in range(total_workers):
            if curr_items is None and deleted_items is None:
                args = (sid, lock)
            elif cas_updated_items is not None:
                args = (sid, lock, curr_ops, curr_items, deleted_items,
                        cas_updated_items)
            else:
                args = (sid, lock, curr_ops, curr_items, deleted_items)

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
        cas_updated_items = Value('L', 0)

        logger.info('Starting all workers')

        self.start_workers(WorkerFactory,
                           'kv', curr_items, deleted_items)
        self.start_workers(SubDocWorkerFactory,
                           'subdoc', curr_items, deleted_items)
        self.start_workers(ViewWorkerFactory,
                           'view', curr_items, deleted_items)
        self.start_workers(N1QLWorkerFactory,
                           'n1ql', curr_items, deleted_items, cas_updated_items)
        self.start_workers(FtsWorkerFactory, 'fts')

        if self.timer:
            time.sleep(self.timer)
            self.shutdown_event.set()
        self.wait_for_all_workers()

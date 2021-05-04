import copy
import os
import signal
import time
from multiprocessing import Event, Lock, Manager, Process, Value
from threading import Timer
from typing import Callable, List, Tuple, Union

import twisted
from decorator import decorator
from numpy import random
from psutil import cpu_count
from twisted.internet import reactor

from logger import logger
from perfrunner.helpers.sync import SyncHotWorkload
from spring.cbgen3 import CBAsyncGen3, CBGen3
from spring.docgen import (
    AdvFilterDocument,
    AdvFilterXattrBody,
    ArrayIndexingDocument,
    ArrayIndexingRangeScanDocument,
    ArrayIndexingUniqueDocument,
    BigFunDocument,
    Document,
    EventingCounterDocument,
    EventingSmallCounterDocument,
    EventingSmallDocument,
    ExtReverseLookupDocument,
    GroupedDocument,
    GSIMultiIndexDocument,
    HashJoinDocument,
    HotKey,
    HundredIndexDocument,
    ImportExportDocument,
    ImportExportDocumentArray,
    ImportExportDocumentNested,
    IncompressibleString,
    JoinedDocument,
    KeyForCASUpdate,
    KeyForRemoval,
    LargeDocument,
    LargeGroupedDocument,
    LargeItemGroupedDocument,
    LargeItemGroupedDocumentKeySize,
    LargeItemPlasmaDocument,
    MovingWorkingSetKey,
    MultiBucketDocument,
    NestedDocument,
    NewOrderedKey,
    PackageDocument,
    PowerKey,
    ProfileDocument,
    RefDocument,
    ReverseLookupDocument,
    ReverseRangeLookupDocument,
    SequentialKey,
    SequentialPlasmaDocument,
    SmallPlasmaDocument,
    SmallPlasmaGroupedDocument,
    String,
    TpcDsDocument,
    UniformKey,
    VaryingItemSizePlasmaDocument,
    WorkingSetKey,
    ZipfKey,
)
from spring.querygen3 import N1QLQueryGen3, ViewQueryGen3, ViewQueryGenByType3
from spring.reservoir import Reservoir


def err(*args, **kwargs):
    pass


twisted.python.log.err = err


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


Sequence = List[Tuple[str, Callable, Tuple]]
Client = Union[CBAsyncGen3, CBGen3]


class Worker:

    CORRECTION_FACTOR = 0.975  # empiric!

    NAME = 'worker'

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event
        self.sid = 0

        self.next_report = 0.05  # report after every 5% of completion
        self.init_load_targets()
        self.init_access_targets()
        self.init_keys()
        self.init_docs()
        self.init_db()

    def init_load_targets(self):
        self.load_targets = []
        if self.ws.collections is not None:
            target_scope_collections = self.ws.collections[self.ts.bucket]
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    if target_scope_collections[scope][collection]['load'] == 1:
                        self.load_targets += [scope+":"+collection]
        else:
            self.load_targets = ["_default:_default"]
        self.num_load_targets = len(self.load_targets)

    def init_access_targets(self):
        self.access_targets = []
        if self.ws.collections is not None:
            target_scope_collections = self.ws.collections[self.ts.bucket]
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    if target_scope_collections[scope][collection]['load'] == 1 and \
                                    target_scope_collections[scope][collection]['access'] == 1:
                        self.access_targets += [scope+":"+collection]
        else:
            self.access_targets = ["_default:_default"]
        self.num_access_targets = len(self.access_targets)

    def init_keys(self):
        ws = copy.deepcopy(self.ws)
        ws.items = ws.items // self.num_load_targets

        self.new_keys = NewOrderedKey(prefix=self.ts.prefix,
                                      fmtr=ws.key_fmtr)

        if self.ws.working_set_move_time:
            self.existing_keys = MovingWorkingSetKey(ws,
                                                     self.ts.prefix)
        elif self.ws.working_set < 100:
            self.existing_keys = WorkingSetKey(ws,
                                               self.ts.prefix)
        elif self.ws.power_alpha:
            self.existing_keys = PowerKey(self.ts.prefix,
                                          ws.key_fmtr,
                                          ws.power_alpha)
        elif self.ws.zipf_alpha:
            self.existing_keys = ZipfKey(self.ts.prefix,
                                         ws.key_fmtr,
                                         ws.zipf_alpha)
        else:
            self.existing_keys = UniformKey(self.ts.prefix,
                                            ws.key_fmtr)

        self.keys_for_removal = KeyForRemoval(self.ts.prefix,
                                              ws.key_fmtr)

        self.keys_for_cas_update = KeyForCASUpdate(ws.n1ql_workers,
                                                   self.ts.prefix,
                                                   ws.key_fmtr)

    def init_docs(self):
        ws = copy.deepcopy(self.ws)
        ws.items = ws.items // self.num_load_targets
        if not hasattr(ws, 'doc_gen') or ws.doc_gen == 'basic':
            self.docs = Document(ws.size)
        elif self.ws.doc_gen == 'eventing_counter':
            self.docs = EventingCounterDocument(ws.size)
        elif self.ws.doc_gen == 'eventing_small_counter':
            self.docs = EventingSmallCounterDocument(ws.size)
        elif self.ws.doc_gen == 'grouped':
            self.docs = GroupedDocument(ws.size, ws.doc_groups)
        elif self.ws.doc_gen == 'large_item_grouped':
            self.docs = LargeItemGroupedDocument(self.ws.size,
                                                 self.ws.doc_groups,
                                                 self.ws.item_size)
        elif self.ws.doc_gen == 'large_item_grouped_keysize':
            self.docs = LargeItemGroupedDocumentKeySize(self.ws.size,
                                                        self.ws.doc_groups,
                                                        self.ws.item_size)
        elif self.ws.doc_gen == 'small_plasma_grouped':
            self.docs = SmallPlasmaGroupedDocument(self.ws.size, self.ws.doc_groups)
        elif self.ws.doc_gen == 'string':
            self.docs = String(ws.size)
        elif self.ws.doc_gen == 'nested':
            self.docs = NestedDocument(ws.size)
        elif self.ws.doc_gen == 'reverse_lookup':
            self.docs = ReverseLookupDocument(ws.size,
                                              self.ts.prefix)
        elif self.ws.doc_gen == 'reverse_range_lookup':
            self.docs = ReverseRangeLookupDocument(ws.size,
                                                   self.ts.prefix,
                                                   ws.range_distance)
        elif self.ws.doc_gen == 'ext_reverse_lookup':
            self.docs = ExtReverseLookupDocument(ws.size,
                                                 self.ts.prefix,
                                                 ws.items)
        elif self.ws.doc_gen == 'hash_join':
            self.docs = HashJoinDocument(ws.size,
                                         self.ts.prefix,
                                         ws.range_distance)
        elif self.ws.doc_gen == 'join':
            self.docs = JoinedDocument(ws.size,
                                       self.ts.prefix,
                                       ws.items,
                                       ws.num_categories,
                                       ws.num_replies)
        elif self.ws.doc_gen == 'ref':
            self.docs = RefDocument(ws.size,
                                    self.ts.prefix)
        elif self.ws.doc_gen == 'array_indexing':
            self.docs = ArrayIndexingDocument(ws.size,
                                              self.ts.prefix,
                                              ws.array_size,
                                              ws.items)
        elif self.ws.doc_gen == 'array_indexing_unique':
            self.docs = ArrayIndexingUniqueDocument(ws.size,
                                                    self.ts.prefix,
                                                    ws.array_size,
                                                    ws.items)
        elif self.ws.doc_gen == 'array_indexing_range_scan':
            self.docs = ArrayIndexingRangeScanDocument(ws.size,
                                                       self.ts.prefix,
                                                       ws.array_size,
                                                       ws.items)
        elif self.ws.doc_gen == 'profile':
            self.docs = ProfileDocument(ws.size,
                                        self.ts.prefix)
        elif self.ws.doc_gen == 'import_export_simple':
            self.docs = ImportExportDocument(ws.size,
                                             self.ts.prefix)
        elif self.ws.doc_gen == 'import_export_array':
            self.docs = ImportExportDocumentArray(ws.size,
                                                  self.ts.prefix)
        elif self.ws.doc_gen == 'import_export_nested':
            self.docs = ImportExportDocumentNested(ws.size,
                                                   self.ts.prefix)
        elif self.ws.doc_gen == 'large':
            self.docs = LargeDocument(ws.size)
        elif self.ws.doc_gen == 'gsi_multiindex':
            self.docs = GSIMultiIndexDocument(ws.size)
        elif self.ws.doc_gen == 'small_plasma':
            self.docs = SmallPlasmaDocument(ws.size)
        elif self.ws.doc_gen == 'sequential_plasma':
            self.docs = SequentialPlasmaDocument(ws.size)
        elif self.ws.doc_gen == 'large_item_plasma':
            self.docs = LargeItemPlasmaDocument(ws.size,
                                                ws.item_size)
        elif self.ws.doc_gen == 'varying_item_plasma':
            self.docs = VaryingItemSizePlasmaDocument(ws.size,
                                                      ws.size_variation_min,
                                                      ws.size_variation_max)
        elif self.ws.doc_gen == 'eventing_small':
            self.docs = EventingSmallDocument(ws.size)
        elif self.ws.doc_gen == 'tpc_ds':
            self.docs = TpcDsDocument()
        elif self.ws.doc_gen == 'package':
            self.docs = PackageDocument(ws.size)
        elif self.ws.doc_gen == 'incompressible':
            self.docs = IncompressibleString(ws.size)
        elif self.ws.doc_gen == 'big_fun':
            self.docs = BigFunDocument()
        elif self.ws.doc_gen == 'multibucket':
            self.docs = MultiBucketDocument(ws.size)
        elif self.ws.doc_gen == 'advancedfilter':
            self.docs = AdvFilterDocument(ws.size)
        elif self.ws.doc_gen == 'advancedfilterxattr':
            self.docs = AdvFilterXattrBody(ws.size)
        elif self.ws.doc_gen == 'hundred_index_doc':
            self.docs = HundredIndexDocument(ws.size,
                                             ws.size_variation_min,
                                             ws.size_variation_max)
        elif self.ws.doc_gen == 'large_grouped_doc':
            self.docs = LargeGroupedDocument(self.ws.size, self.ws.doc_groups)

    def init_db(self):
        params = {
            'bucket': self.ts.bucket,
            'host': self.ts.node,
            'port': 8091,
            'username': self.ts.bucket,
            'password': self.ts.password,
            'ssl_mode': self.ws.ssl_mode,
            'n1ql_timeout': self.ws.n1ql_timeout,
            'connstr_params': self.ws.connstr_params
        }
        if self.ts.cloud:
            params['host'] = self.ts.cloud['cluster_svc']
        try:
            self.cb = CBGen3(**params)
        except Exception as e:
            raise SystemExit(e)

    def init_creds(self):
        for bucket in getattr(self.ws, 'buckets', []):
            self.cb.client.add_bucket_creds(bucket, self.ts.password)

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

    def dump_stats(self):
        self.reservoir.dump(filename='{}-{}'.format(self.NAME, self.sid))


class KVWorker(Worker):

    NAME = 'kv-worker'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reservoir = Reservoir(num_workers=self.ws.workers)
        self.gen_duration = 0.0
        self.batch_duration = 0.0
        self.delta = 0.0
        self.op_delay = 0.0

    @property
    def random_ops(self) -> List[str]:
        random.shuffle(self.ops_list)
        return self.ops_list

    @property
    def random_targets(self) -> List[str]:
        targets = list(random.choice(self.access_targets, self.num_random_targets))
        return self.q * targets + targets[:self.r]

    def random_target(self) -> str:
        return random.choice(self.access_targets)

    def create_args(self, cb: Client,
                    curr_items: int,
                    target: str) -> Sequence:
        key = self.new_keys.next(curr_items)
        doc = self.docs.next(key)
        if self.ws.durability:
            args = target, key.string, doc, self.ws.durability, self.ws.ttl
            return [('set', cb.update_durable, args)]
        else:
            args = target, key.string, doc, self.ws.persist_to, self.ws.replicate_to, self.ws.ttl
            return [('set', cb.update, args)]

    def read_args(self, cb: Client,
                  curr_items: int,
                  deleted_items: int,
                  target: str) -> Sequence:
        key = self.existing_keys.next(curr_items, deleted_items)
        args = target, key.string

        return [('get', cb.read, args)]

    def update_args(self,
                    cb: Client,
                    curr_items: int,
                    deleted_items: int,
                    target: str) -> Sequence:
        key = self.existing_keys.next(curr_items,
                                      deleted_items,
                                      self.current_hot_load_start,
                                      self.timer_elapse)
        doc = self.docs.next(key)
        if self.ws.durability:
            args = target, key.string, doc, self.ws.durability, self.ws.ttl
            return [('set', cb.update_durable, args)]
        else:
            args = target, key.string, doc, self.ws.persist_to, self.ws.replicate_to, self.ws.ttl
            return [('set', cb.update, args)]

    def delete_args(self, cb: Client,
                    deleted_items: int,
                    target: str) -> Sequence:
        key = self.keys_for_removal.next(deleted_items)
        args = target, key.string

        return [('delete', cb.delete, args)]

    def modify_args(self, cb: Client,
                    curr_items: int, deleted_items: int,
                    target: str) -> Sequence:
        key = self.existing_keys.next(curr_items, deleted_items)
        doc = self.docs.next(key)
        read_args = target, key.string,
        update_args = target, key.string, doc, self.ws.persist_to, self.ws.replicate_to, self.ws.ttl

        return [('get', cb.read, read_args), ('set', cb.update, update_args)]

    def gen_cmd_sequence(self, cb: Client = None) -> Sequence:
        if not cb:
            cb = self.cb
        target = self.random_target()
        curr_items = self.ws.items // self.num_load_targets
        deleted_items = 0
        if self.ws.creates or self.ws.deletes:
            max_batch_deletes_buffer = self.ws.deletes * self.ws.workers
            delete_buffer_diff = self.ws.deletes - max_batch_deletes_buffer
            with self.gen_lock:
                target_info = self.shared_dict[target]
                curr_items = target_info[0]
                deleted_items = target_info[1] + max_batch_deletes_buffer
                self.shared_dict[target] = \
                    [curr_items + self.ws.creates, deleted_items + delete_buffer_diff]
        cmds = []
        for op in self.random_ops:
            if op == 'c':
                cmds += self.create_args(cb, curr_items, target)
                curr_items += 1
            elif op == 'r':
                cmds += self.read_args(cb, curr_items, deleted_items, target)
            elif op == 'u':
                cmds += self.update_args(cb, curr_items, deleted_items, target)
            elif op == 'd':
                cmds += self.delete_args(cb, deleted_items, target)
                deleted_items += 1
            elif op == 'm':
                cmds += self.modify_args(cb, curr_items, deleted_items, target)
        return cmds

    def do_batch(self, *args, **kwargs):
        op_count = 0
        if self.target_time is None:
            cmd_seq = self.gen_cmd_sequence()
            for cmd, func, args in cmd_seq:
                latency = func(*args)
                if latency is not None:
                    self.reservoir.update(operation=cmd, value=latency)
                if not op_count % 5:
                    if self.time_to_stop():
                        return
                op_count += 1
        else:
            t0 = time.time()
            self.op_delay = self.op_delay + (self.delta / self.batch_size)
            cmd_seq = self.gen_cmd_sequence()
            self.gen_duration = time.time() - t0
            for cmd, func, args in cmd_seq:
                latency = func(*args)
                if latency is not None:
                    self.reservoir.update(operation=cmd, value=latency)
                if self.op_delay > 0:
                    time.sleep(self.op_delay * self.CORRECTION_FACTOR)
                if not op_count % 5:
                    if self.time_to_stop():
                        return
                op_count += 1
            self.batch_duration = time.time() - t0
            self.delta = self.target_time - self.batch_duration
            if self.delta > 0:
                time.sleep(self.CORRECTION_FACTOR * self.delta)

    def run_condition(self, curr_ops):
        return curr_ops.value < self.ws.ops and not self.time_to_stop()

    def run(self, sid, locks, curr_ops, shared_dict,
            current_hot_load_start=None, timer_elapse=None):
        self.sid = sid
        self.locks = locks
        self.gen_lock = locks[0]
        self.batch_lock = locks[1]
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse
        self.cb.connect_collections(self.access_targets)
        self.ops_list = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes + \
            ['m'] * (self.ws.reads_and_updates // 2)
        self.batch_size = len(self.ops_list)
        self.num_random_targets = min(self.batch_size,
                                      max(self.num_access_targets//self.ws.workers, 1))
        self.q, self.r = divmod(self.batch_size, self.num_random_targets)

        if self.ws.throughput < float('inf'):
            self.target_time = float(self.batch_size) * self.ws.workers / \
                               self.ws.throughput
        else:
            self.target_time = None
        self.seed()
        try:
            if self.target_time:
                start_delay = random.random_sample() * self.target_time
                time.sleep(start_delay * self.CORRECTION_FACTOR)
            while self.run_condition(curr_ops):
                with self.batch_lock:
                    curr_ops.value += self.batch_size
                self.do_batch()
                self.report_progress(curr_ops.value)
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))
        self.dump_stats()


class AsyncKVWorker(KVWorker):

    NAME = 'async-kv-worker'

    NUM_CONNECTIONS = 8

    def init_db(self):
        params = {'bucket': self.ts.bucket, 'host': self.ts.node, 'port': 8091,
                  'username': self.ts.bucket, 'password': self.ts.password}

        self.cbs = [CBAsyncGen3(**params) for _ in range(self.NUM_CONNECTIONS)]
        self.counter = list(range(self.NUM_CONNECTIONS))

    def restart(self, _, cb, i):
        self.counter[i] += 1
        if self.counter[i] == self.batch_size:
            actual_time = time.time() - self.time_started
            if self.target_time is not None:
                delta = self.target_time - actual_time
                if delta > 0:
                    time.sleep(self.CORRECTION_FACTOR * delta)

            self.report_progress(self.curr_ops.value)
            if not self.done \
                    and (self.curr_ops.value >= self.ws.ops or self.time_to_stop()):
                with self.batch_lock:
                    self.done = True
                logger.info('Finished: {}-{}'.format(self.NAME, self.sid))
                reactor.stop()
            else:
                self.do_batch(_, cb, i)

    def do_batch(self, _, cb, i):
        self.counter[i] = 0
        self.time_started = time.time()

        with self.batch_lock:
            self.curr_ops.value += self.batch_size

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

        cb.bucket._close()
        time.sleep(15)
        d = cb.bucket.on_connect()
        d.addCallback(self.do_batch, cb, i)
        d.addErrback(self.error, cb, i)

    def run(self, sid, locks, curr_ops, shared_dict,
            current_hot_load_start=None, timer_elapse=None):
        set_cpu_afinity(sid)
        self.sid = sid
        self.locks = locks
        self.gen_lock = locks[0]
        self.batch_lock = locks[1]
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse
        self.curr_ops = curr_ops

        for cb in self.cbs:
            cb.connect_collections(self.access_targets)

        self.ops_list = \
            ['c'] * self.ws.creates + \
            ['r'] * self.ws.reads + \
            ['u'] * self.ws.updates + \
            ['d'] * self.ws.deletes + \
            ['m'] * (self.ws.reads_and_updates // 2)
        self.batch_size = len(self.ops_list)
        self.num_random_targets = min(self.batch_size,
                                      max(self.num_access_targets//self.ws.workers, 1))
        self.q, self.r = divmod(self.batch_size, self.num_random_targets)

        if self.ws.throughput < float('inf'):
            self.target_time = float(self.batch_size) * self.ws.workers / \
                               self.ws.throughput
        else:
            self.target_time = None

        self.seed()
        self.done = False

        for i, cb in enumerate(self.cbs):
            d = cb.bucket.on_connect()
            d.addCallback(self.do_batch, cb, i)
            d.addErrback(self.error, cb, i)
        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
        reactor.run()


class HotReadsWorker(Worker):

    def run(self, sid, *args):
        set_cpu_afinity(sid)
        ws = copy.deepcopy(self.ws)
        ws.items = ws.items // self.num_load_targets
        self.cb.connect_collections(self.load_targets)
        for target in self.load_targets:
            for key in HotKey(sid, ws, self.ts.prefix):
                self.cb.read(target, key.string)


class SeqUpsertsWorker(Worker):

    def run(self, sid, *args):
        ws = copy.deepcopy(self.ws)
        ws.items = ws.items // self.num_load_targets
        self.cb.connect_collections(self.load_targets)
        for target in self.load_targets:
            for key in SequentialKey(sid, ws, self.ts.prefix):
                doc = self.docs.next(key)
                self.cb.update(target, key.string, doc)


class FTSDataSpreadWorker(Worker):

    def run(self, sid, *args):
        self.sid = sid
        items_per_collection = self.ws.items // self.num_load_targets
        self.cb.connect_collections(self.load_targets)
        if not self.ws.collections.get(self.ts.bucket, {}) \
                .get("_default", {}) \
                .get("_default", {}) \
                .get('load', 0):
            source = "scope-1:collection-1"
        else:
            source = "_default:_default"

        if self.ws.fts_data_spread_worker_type == "default":
            self.default_spread(source, items_per_collection)
        elif self.ws.fts_data_spread_worker_type == "collection_specific":
            self.collection_specific_spread(source, items_per_collection)
        else:
            raise Exception(
                "invalid fts data spread worker type: {}".format(
                    self.ws.fts_data_spread_worker_type)
            )

    def default_spread(self, source, items_per_collection):
        iteration = 0
        step = self.ws.fts_data_spread_workers
        for target in sorted(self.load_targets):
            if target != source:
                start = self.sid + (items_per_collection * iteration)
                stop = items_per_collection * (iteration + 1)
                new_key = self.sid
                for key in range(start, stop, step):
                    hex_key = format(key, 'x')
                    get_args = source, hex_key
                    doc = self.cb.get(*get_args)
                    new_hex_key = format(new_key, 'x')
                    set_args = target, new_hex_key, doc.content
                    self.cb.set(*set_args)
                    self.cb.delete(*get_args)
                    new_key += step
            iteration += 1

    def collection_specific_spread(self, source, items_per_collection):
        source_key_range = range(0, self.ws.items, self.num_load_targets)
        iteration = 0
        for source_key_index in range(self.sid,
                                      len(source_key_range),
                                      self.ws.fts_data_spread_workers):
            source_key = source_key_range[source_key_index]
            hex_source_key = format(source_key, 'x')

            target_key = self.sid + (iteration * self.ws.fts_data_spread_workers)
            hex_target_key = format(target_key, 'x')

            get_args = source, hex_source_key
            doc = self.cb.get(*get_args)

            for target in sorted(self.load_targets):
                set_args = target, hex_target_key, doc.content
                self.cb.set(*set_args)

            if source_key >= items_per_collection:
                for delete_key in range(source_key, source_key + self.num_load_targets):
                    hex_delete_key = format(delete_key, 'x')
                    del_args = source, hex_delete_key
                    self.cb.delete(*del_args)
            iteration += 1


class AuxillaryWorker:

    CORRECTION_FACTOR = 0.975  # empiric!

    NAME = 'aux-worker'

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        self.ws = workload_settings
        self.ts = target_settings
        self.shutdown_event = shutdown_event
        self.sid = 0
        self.next_report = 0.05  # report after every 5% of completion
        self.init_db()
        self.init_creds()

    def init_db(self):
        params = {
            'bucket': self.ts.bucket,
            'host': self.ts.node,
            'port': 8091,
            'username': self.ts.bucket,
            'password': self.ts.password,
            'ssl_mode': self.ws.ssl_mode,
            'n1ql_timeout': self.ws.n1ql_timeout,
            'connstr_params': self.ws.connstr_params
        }

        try:
            self.cb = CBGen3(**params)
        except Exception as e:
            raise SystemExit(e)

    def init_creds(self):
        for bucket in getattr(self.ws, 'buckets', []):
            self.cb.client.add_bucket_creds(bucket, self.ts.password)

    def time_to_stop(self):
        return (self.shutdown_event is not None and
                self.shutdown_event.is_set())

    def seed(self):
        random.seed(seed=self.sid * 9901)


class UserModWorker(AuxillaryWorker):

    NAME = 'user-mod-worker'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_condition(self, curr_ops):
        return curr_ops.value < self.ws.ops and not self.time_to_stop()

    @with_sleep
    def update_random_user(self, *args, **kwargs):
        random_user = "user"+str(random.randint(1, self.users+1))
        num_random_roles = random.randint(1, self.num_roles+1)
        random_roles = []
        for i in random.choice(self.num_roles,
                               size=num_random_roles,
                               replace=False):
            if self.supported_roles[i].role in self.collection_level_roles:
                for target in self.targets:
                    random_roles.append((self.supported_roles[i].role, target))
            elif self.supported_roles[i].role and self.supported_roles[i].bucket_name:
                random_roles.append((self.supported_roles[i].role,
                                     self.supported_roles[i].bucket_name))
            else:
                random_roles.append(self.supported_roles[i].role)
        password = 'password'
        self.cb.do_upsert_user(random_user, random_roles, password)

    def run(self, sid, lock, curr_ops, shared_dict,
            current_hot_load_start=None, timer_elapse=None):
        self.sid = sid
        self.lock = lock
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse
        self.seed()
        if self.ws.user_mod_throughput < float('inf'):
            self.target_time = self.ws.user_mod_workers / \
                               self.ws.user_mod_throughput
        else:
            self.target_time = None

        self.users = self.ws.users
        self.cb.create_user_manager()
        self.supported_roles = [raw_role for raw_role in self.cb.get_roles()]
        self.targets = []
        for bucket in self.ws.collections.keys():
            for scope in self.ws.collections[bucket].keys():
                for collection in self.ws.collections[bucket][scope].keys():
                    target = bucket+":"+scope+":"+collection
                    self.targets.append(target)

        self.collection_level_roles = ["data_reader", "data_writer", "data_dcp_reader",
                                       "fts_searcher", "query_select", "query_update",
                                       "query_insert", "query_delete", "query_manage_index"]
        self.num_roles = len(self.supported_roles)

        try:
            while self.run_condition(curr_ops) and self.users > 0:
                self.update_random_user()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))


class CollectionModWorker(AuxillaryWorker):

    NAME = 'collection-mod-worker'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run_condition(self, curr_ops):
        return curr_ops.value < self.ws.ops and not self.time_to_stop()

    @with_sleep
    def create_delete_collection(self, *args, **kwargs):
        random_scope = list(random.choice(self.target_scopes, 1))
        target_scope = random_scope[0]
        target_collection = "temp-collection-"+str(self.sid)
        self.cb.do_collection_create(target_scope, target_collection)
        self.cb.do_collection_drop(target_scope, target_collection)

    def run(self, sid, lock, curr_ops, shared_dict,
            current_hot_load_start=None, timer_elapse=None):
        self.sid = sid
        self.lock = lock
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse
        self.seed()
        self.cb.create_collection_manager()

        self.target_scopes = []
        if self.ws.collections is not None:
            target_scope_collections = self.ws.collections[self.ts.bucket]
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    if target_scope_collections[scope][collection]['load'] == 1 and \
                                    target_scope_collections[scope][collection]['access'] == 1:
                        self.target_scopes += [scope]
                        break
        else:
            self.target_scopes = ["_default"]

        if self.ws.collection_mod_throughput < float('inf'):
            self.target_time = self.ws.collection_mod_workers / \
                               self.ws.collection_mod_throughput
        else:
            self.target_time = None

        try:
            while self.run_condition(curr_ops):
                self.create_delete_collection()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))


class N1QLWorker(Worker):

    NAME = 'query-worker'

    def __init__(self, workload_settings, target_settings, shutdown_event=None):
        super().__init__(workload_settings, target_settings, shutdown_event)
        self.new_queries = N1QLQueryGen3(workload_settings.n1ql_queries)
        self.reservoir = Reservoir(num_workers=self.ws.n1ql_workers)
        self.gen_duration = 0.0
        self.batch_duration = 0.0
        self.delta = 0.0
        self.op_delay = 0.0
        self.first = True

    def do_batch_create(self, *args, **kwargs):
        if self.target_time:
            t0 = time.time()
            self.op_delay = self.op_delay + (self.delta / self.ws.n1ql_batch_size)
        target = self.next_target()
        with self.lock:
            target_info = self.shared_dict[target]
            target_curr_items = target_info[0]
            target_deleted_items = target_info[1]
            updated_curr_items = target_curr_items + self.ws.n1ql_batch_size
            self.shared_dict[target] = [updated_curr_items, target_deleted_items]

        for i in range(self.ws.n1ql_batch_size):
            target_curr_items += 1
            key = self.new_keys.next(curr_items=target_curr_items)
            doc = self.docs.next(key)
            query, options = self.new_queries.next(key.string, doc, self.replacement_targets)
            latency = self.cb.n1ql_query(query, options)
            if not self.first:
                self.reservoir.update(operation='query', value=latency)
            else:
                self.first = False
            if self.op_delay > 0 and self.target_time:
                time.sleep(self.op_delay * self.CORRECTION_FACTOR)
            if not i % 5:
                if self.time_to_stop():
                    return
        if self.target_time:
            self.batch_duration = time.time() - t0
            self.delta = self.target_time - self.batch_duration
            if self.delta > 0:
                time.sleep(self.CORRECTION_FACTOR * self.delta)

    def do_batch_update(self, *args, **kwargs):
        if self.target_time:
            t0 = time.time()
            self.op_delay = self.op_delay + (self.delta / self.ws.n1ql_batch_size)

        self.next_target()
        target_curr_items = self.ws.items // self.num_load_targets

        for i in range(self.ws.n1ql_batch_size):

            random_slice = random.choice(self.update_slices)
            key = self.keys_for_cas_update.next(sid=random_slice,
                                                curr_items=target_curr_items)
            doc = self.docs.next(key)
            query, options = self.new_queries.next(key.string, doc, self.replacement_targets)
            latency = self.cb.n1ql_query(query, options)
            if not self.first:
                self.reservoir.update(operation='query', value=latency)
            else:
                self.first = False
            if self.op_delay > 0 and self.target_time:
                time.sleep(self.op_delay * self.CORRECTION_FACTOR)
            if not i % 5:
                if self.time_to_stop():
                    return
        if self.target_time:
            self.batch_duration = time.time() - t0
            self.delta = self.target_time - self.batch_duration
            if self.delta > 0:
                time.sleep(self.CORRECTION_FACTOR * self.delta)

    def do_batch_read(self, *args, **kwargs):
        if self.target_time:
            t0 = time.time()
            self.op_delay = self.op_delay + (self.delta / self.ws.n1ql_batch_size)

        self.next_target()
        target_curr_items = self.ws.items // self.num_load_targets

        if self.ws.doc_gen == 'ext_reverse_lookup':
            target_curr_items //= 4
        for i in range(self.ws.n1ql_batch_size):
            key = self.existing_keys.next(curr_items=target_curr_items,
                                          curr_deletes=0)
            doc = self.docs.next(key)
            query, options = self.new_queries.next(key.string, doc, self.replacement_targets)
            latency = self.cb.n1ql_query(query, options)
            if not self.first:
                self.reservoir.update(operation='query', value=latency)
            else:
                self.first = False
            if self.op_delay > 0 and self.target_time:
                time.sleep(self.op_delay * self.CORRECTION_FACTOR)
            if not i % 5:
                if self.time_to_stop():
                    return
        if self.target_time:
            self.batch_duration = time.time() - t0
            self.delta = self.target_time - self.batch_duration
            if self.delta > 0:
                time.sleep(self.CORRECTION_FACTOR * self.delta)

    def do_batch(self):
        if self.ws.n1ql_op == 'read':
            self.do_batch_read()
        elif self.ws.n1ql_op == 'create':
            self.do_batch_create()
        elif self.ws.n1ql_op == 'update':
            self.do_batch_update()

    def init_access_targets(self):
        self.bucket_targets = dict()
        self.access_targets = dict()
        self.replacement_targets = dict()
        # create bucket_targets
        for bucket in self.ws.bucket_list:
            targets = []
            if self.ws.collections is not None:
                scopes = self.ws.collections[bucket]
                for scope in scopes.keys():
                    collections = scopes[scope]
                    for collection in collections.keys():
                        if collections[collection]['load'] == 1 and \
                                        collections[collection]['access'] == 1:
                            targets += [":".join([scope, collection])]
            else:
                targets = [":".join(['_default', '_default'])]
            self.bucket_targets[bucket] = targets

        # create access targets
        for bucket in self.ws.bucket_list:
            worker_targets = self.bucket_targets[bucket]
            worker_targets.sort()
            num_worker_targets = len(worker_targets)
            max_target_count = 0

            # find max number of times a bucket is used in all queries
            for query in self.ws.n1ql_queries:
                max_target_count = max(query['statement'].count(bucket), max_target_count)
            worker_access_targets = []

            # need to replace
            for i in range(max_target_count):
                target_index_set = set()
                if num_worker_targets <= self.ws.n1ql_workers:
                    target_index_set.add((self.sid + i) % num_worker_targets)
                else:
                    for j in range((self.sid + i) % num_worker_targets,
                                   num_worker_targets,
                                   self.ws.n1ql_workers):
                        target_index_set.add(j)

                target_indexes = list(target_index_set)
                target_indexes.sort()
                if bucket != self.ts.bucket:
                    target_indexes = list(random.choice(target_indexes, 1))
                worker_access_targets.append([worker_targets[i] for i in target_indexes])
            self.access_targets[bucket] = worker_access_targets

        # create replacement targets

        # replacements dictionary:
        # Ex: {"bucket-1": ['collection-1',
        #                   'collection-3']}
        # For target bucket,
        #   Randomly select a collection from access targets.
        # For any non-target bucket,
        #   Select the first collection for that bucket from access targets
        #
        # A query may contain multiple instances of a bucket, ex: joins.
        # The bucket instances are replaced in the same order as they appear in
        # in the replacement list for a bucket.
        #
        # Ex: select * from bucket-1 join bucket-1 on ...
        # The first bucket-1 is replaced with collection-1 and the second bucket-1
        # is replaced with collection-3 (from example dictionary above)
        #
        # Only one target bucket in a query is random selected. For the case where a bucket
        # occurs more than once, the instance is first randomly selected, i.e we randomly
        # choose which occurrence of the bucket to randomly select before we randomly choose
        # a replacement target for that instance of the bucket.
        target = None
        for bucket in self.access_targets.keys():
            bucket_instances = self.access_targets[bucket]
            if bucket == self.ts.bucket:
                num_bucket_instances = len(bucket_instances)
                random_instance = random.randint(num_bucket_instances)
                target_replacements = []
                for j in range(num_bucket_instances):
                    if j == random_instance:
                        random_target = random.choice(bucket_instances[j])
                        target = random_target
                        target_replacements.append(random_target)
                    else:
                        target_replacements.append(bucket_instances[j][0])
                self.replacement_targets[bucket] = target_replacements
            else:
                self.replacement_targets[bucket] = [instance[0] for instance in bucket_instances]
        if not target:
            raise Exception("No target")

    def next_target(self):
        random_instance = random.randint(self.num_bucket_instances)
        target_replacements = []
        target = None
        for j in range(self.num_bucket_instances):
            if j == random_instance:
                random_target = random.choice(self.bucket_instances[j])
                target = random_target
                target_replacements.append(random_target)
            else:
                target_replacements.append(self.bucket_instances[j][0])
        if not target:
            raise Exception("No target")
        self.replacement_targets[self.ts.bucket] = target_replacements
        return target

    def run(self, sid, locks, curr_ops, shared_dict,
            current_hot_load_start=None, timer_elapse=None):
        self.sid = sid
        self.locks = locks
        self.lock = locks[0]
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse
        self.bucket_instances = self.access_targets[self.ts.bucket]
        self.num_bucket_instances = len(self.bucket_instances)
        self.num_worker_targets = len(self.bucket_targets[self.ts.bucket])
        self.max_target_count = 0
        for query in self.ws.n1ql_queries:
            self.max_target_count = \
                max(query['statement'].count(
                    self.ts.bucket),
                    self.max_target_count)

        if self.ws.n1ql_op == 'update':
            if self.num_worker_targets <= self.ws.n1ql_workers:
                self.update_slices = [i % self.ws.n1ql_workers
                                      for i in range(self.sid,
                                                     self.sid + self.num_worker_targets,
                                                     self.max_target_count)]
            else:
                self.update_slices = [i % self.ws.n1ql_workers
                                      for i in range(self.sid,
                                                     self.sid + self.ws.n1ql_workers,
                                                     self.max_target_count)]

        if self.ws.n1ql_throughput < float('inf'):
            self.target_time = self.ws.n1ql_batch_size * self.ws.n1ql_workers / \
                               float(self.ws.n1ql_throughput)
        else:
            self.target_time = None

        try:
            if self.target_time:
                start_delay = random.random_sample() * self.target_time
                time.sleep(start_delay * self.CORRECTION_FACTOR)
            while not self.time_to_stop():
                self.do_batch()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))

        self.dump_stats()


class ViewWorker(Worker):

    NAME = 'query-worker'

    def __init__(self, workload_settings, target_settings, shutdown_event):
        super().__init__(workload_settings, target_settings, shutdown_event)
        self.delta = 0.0
        self.op_delay = 0.0
        self.batch_duration = 0.0
        self.reservoir = Reservoir(num_workers=self.ws.query_workers)
        if workload_settings.index_type is None:
            self.new_queries = ViewQueryGen3(workload_settings.ddocs,
                                             workload_settings.query_params)
        else:
            self.new_queries = ViewQueryGenByType3(workload_settings.index_type,
                                                   workload_settings.query_params)

    def do_batch(self):
        if self.target_time:
            t0 = time.time()
            self.op_delay = self.op_delay + (self.delta / self.ws.n1ql_batch_size)

        target_info = self.shared_dict["_default:_default"]
        curr_items = target_info[0]
        deleted_items = target_info[1]
        curr_items_spot = \
            curr_items - self.ws.creates * self.ws.workers
        deleted_spot = \
            deleted_items + self.ws.deletes * self.ws.workers

        for i in range(self.ws.spring_batch_size):
            key = self.existing_keys.next(curr_items_spot, deleted_spot)
            doc = self.docs.next(key)
            ddoc_name, view_name, query = self.new_queries.next(doc)
            latency = self.cb.view_query(ddoc_name, view_name, query=query)
            self.reservoir.update(operation='query', value=latency)
            if self.op_delay > 0 and self.target_time:
                time.sleep(self.op_delay * self.CORRECTION_FACTOR)
            if not i % 5:
                if self.time_to_stop():
                    return
        if self.target_time:
            self.batch_duration = time.time() - t0
            self.delta = self.target_time - self.batch_duration
            if self.delta > 0:
                time.sleep(self.CORRECTION_FACTOR * self.delta)

    def run(self, sid, locks, curr_ops, shared_dict,
            current_hot_load_start=None, timer_elapse=None):
        if self.ws.query_throughput < float('inf'):
            self.target_time = float(self.ws.spring_batch_size) * self.ws.query_workers / \
                               self.ws.query_throughput
        else:
            self.target_time = None

        self.sid = sid
        self.locks = locks
        self.lock = locks[0]
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse

        try:
            while not self.time_to_stop():
                self.do_batch()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))

        self.dump_stats()


class WorkerFactory:

    def __new__(cls, settings):
        num_workers = settings.workers
        if getattr(settings, 'async', None):
            worker = AsyncKVWorker
        elif getattr(settings, 'seq_upserts', None):
            worker = SeqUpsertsWorker
        elif getattr(settings, 'hot_reads', None):
            worker = HotReadsWorker
        elif getattr(settings, 'fts_data_spread_workers', None):
            worker = FTSDataSpreadWorker
            num_workers = settings.fts_data_spread_workers
        else:
            worker = KVWorker
        return worker, num_workers


class AuxillaryWorkerFactory:

    def __new__(cls, settings):
        if getattr(settings, 'user_mod_workers', None):
            return UserModWorker, settings.user_mod_workers
        elif getattr(settings, 'collection_mod_workers', None):
            return CollectionModWorker, settings.collection_mod_workers
        else:
            return AuxillaryWorker, 0


class N1QLWorkerFactory:

    def __new__(cls, workload_settings):
        return N1QLWorker, workload_settings.n1ql_workers


class ViewWorkerFactory:

    def __new__(cls, workload_settings):
        return ViewWorker, workload_settings.query_workers


class WorkloadGen:

    def __init__(self, workload_settings, target_settings, timer=None, *args):
        self.ws = workload_settings
        self.ts = target_settings
        self.time = timer
        self.timer = self.time and Timer(self.time, self.abort) or None
        self.shutdown_events = []
        self.worker_processes = []

    def start_workers(self,
                      worker_factory,
                      shared_dict,
                      current_hot_load_start=None,
                      timer_elapse=None):
        curr_ops = Value('L', 0)
        batch_lock = Lock()
        gen_lock = Lock()
        locks = [batch_lock, gen_lock]
        worker_type, total_workers = worker_factory(self.ws)
        for sid in range(total_workers):
            shutdown_event = self.time and Event() or None
            self.shutdown_events.append(shutdown_event)
            args = (sid, locks, curr_ops, shared_dict,
                    current_hot_load_start, timer_elapse, worker_type,
                    self.ws, self.ts, shutdown_event)

            def run_worker(sid, locks, curr_ops, shared_dict,
                           current_hot_load_start, timer_elapse, worker_type,
                           ws, ts, shutdown_event):
                worker = worker_type(ws, ts, shutdown_event)
                worker.run(sid, locks, curr_ops, shared_dict, current_hot_load_start, timer_elapse)

            worker_process = Process(target=run_worker, args=args)
            worker_process.daemon = True
            worker_process.start()
            self.worker_processes.append(worker_process)

            if getattr(self.ws, 'async', False):
                time.sleep(2)

    def start_all_workers(self):
        """Start all the workers groups."""
        logger.info('Starting all collections workers')
        self.manager = Manager()
        self.shared_dict = self.manager.dict()
        if self.ws.collections is not None:
            num_load = 0
            target_scope_collections = self.ws.collections[self.ts.bucket]
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    if target_scope_collections[scope][collection]['load'] == 1:
                        num_load += 1

            curr_items = self.ws.items // num_load
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    target = scope+":"+collection
                    if target_scope_collections[scope][collection]['load'] == 1:
                        self.shared_dict[target] = [curr_items, 0]
                    else:
                        self.shared_dict[target] = [0, 0]
        else:
            # version prior to 7.0.0
            target = "_default:_default"
            self.shared_dict[target] = [self.ws.items, 0]

        timer_elapse = Value('I', 0)
        current_hot_load_start = Value('L', 0)

        if self.ws.working_set_move_time:
            current_hot_load_start.value = int(self.ws.items * self.ws.working_set / 100)
            self.sync = SyncHotWorkload(current_hot_load_start, timer_elapse)

        self.start_workers(AuxillaryWorkerFactory,
                           self.shared_dict,
                           current_hot_load_start,
                           timer_elapse)
        self.start_workers(WorkerFactory,
                           self.shared_dict,
                           current_hot_load_start,
                           timer_elapse)
        self.start_workers(N1QLWorkerFactory,
                           self.shared_dict,
                           current_hot_load_start,
                           timer_elapse)
        self.start_workers(ViewWorkerFactory,
                           self.shared_dict,
                           current_hot_load_start,
                           timer_elapse)

    def set_signal_handler(self):
        """Abort the execution upon receiving a signal from perfrunner."""
        signal.signal(signal.SIGTERM, self.abort)

    def abort(self, *args):
        """Triggers the shutdown event."""
        for shutdown_event in self.shutdown_events:
            if shutdown_event:
                shutdown_event.set()

    @staticmethod
    def store_pid():
        """Store PID of the current Celery worker."""
        pid = os.getpid()
        with open('worker.pid', 'w') as f:
            f.write(str(pid))

    def start_timers(self):
        """Start the optional timers."""
        if self.timer is not None and self.ws.ops == float('inf'):
            self.timer.start()

        if self.ws.working_set_move_time:
            self.sync.start_timer(self.ws)

    def stop_timers(self):
        """Cancel all the active timers."""
        if self.timer is not None:
            self.timer.cancel()

        if self.ws.working_set_move_time:
            self.sync.stop_timer()

    def wait_for_completion(self):
        """Wait until the sub-processes terminate."""
        for process in self.worker_processes:
            process.join()

    def run(self):
        self.start_all_workers()

        self.start_timers()

        self.store_pid()

        self.set_signal_handler()

        self.wait_for_completion()

        self.stop_timers()

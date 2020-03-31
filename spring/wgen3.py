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
    EventingSmallDocument,
    ExtReverseLookupDocument,
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
    String,
    TpcDsDocument,
    UniformKey,
    VaryingItemSizePlasmaDocument,
    WorkingSetKey,
    ZipfKey,
)
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
        self.init_keys()
        self.init_docs()
        self.init_db()
        self.init_creds()

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

    @property
    def random_ops(self) -> List[str]:
        random.shuffle(self.ops_list)
        return self.ops_list

    @property
    def random_targets(self) -> List[str]:
        targets = list(random.choice(self.access_targets, self.num_random_targets))
        return self.q * targets + targets[:self.r]

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

        ops = self.random_ops
        targets = self.random_targets
        target_set = set(targets)

        curr_items = dict()
        deleted_items = dict()
        creates = dict()
        deletes = dict()

        for i in range(self.batch_size):
            op = ops[i]
            target = targets[i]
            c = creates.get(target, 0)
            d = deletes.get(target, 0)
            creates[target] = c
            deletes[target] = d
            if op == 'c':
                creates[target] = c + 1
            elif op == 'd':
                deletes[target] = d + 1

        for target in target_set:
            c = creates[target]
            d = deletes[target]

            with self.lock:
                target_info = self.shared_dict[target]
                target_curr_items, target_deleted_items = target_info.split(":")
                if c > 0:
                    update_curr_items = str(int(target_curr_items) + c)
                else:
                    update_curr_items = target_curr_items
                if d > 0:
                    update_deleted_items = str(int(target_deleted_items) + self.ws.deletes)
                else:
                    update_deleted_items = target_deleted_items
                if c + d > 0:
                    self.shared_dict[target] = update_curr_items + ":" + update_deleted_items

            curr_items[target] = int(target_curr_items)
            deleted_items[target] = int(target_deleted_items) + self.ws.deletes * self.ws.workers

        cmds = []
        for i in range(self.batch_size):
            op = ops[i]
            target = targets[i]
            c = curr_items[target]
            d = deleted_items[target]
            if op == 'c':
                cmds += self.create_args(cb, c, target)
                curr_items[target] = c + 1
            elif op == 'r':
                cmds += self.read_args(cb, c, d, target)
            elif op == 'u':
                cmds += self.update_args(cb, c, d, target)
            elif op == 'd':
                cmds += self.delete_args(cb, d, target)
                deleted_items[target] = d + 1
            elif op == 'm':
                cmds += self.modify_args(cb, c, d, target)
        return cmds

    @with_sleep
    def do_batch(self, *args, **kwargs):
        cmd_seq = self.gen_cmd_sequence()
        for cmd, func, args in cmd_seq:
            latency = func(*args)
            if latency is not None:
                self.reservoir.update(operation=cmd, value=latency)

    def run_condition(self, curr_ops):
        return curr_ops.value < self.ws.ops and not self.time_to_stop()

    def run(self, sid, lock, curr_ops, shared_dict, current_hot_load_start=None, timer_elapse=None):
        self.sid = sid
        self.lock = lock
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse
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
        self.cb.connect_collections(self.access_targets)

        self.num_access_targets = len(self.access_targets)
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

        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
        try:
            while self.run_condition(curr_ops):
                with lock:
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

    def run(self, sid, lock, curr_ops, shared_dict, current_hot_load_start=None, timer_elapse=None):
        set_cpu_afinity(sid)

        self.sid = sid
        self.lock = lock
        self.shared_dict = shared_dict
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse
        self.curr_ops = curr_ops

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

        for cb in self.cbs:
            cb.connect_collections(self.access_targets)

        self.num_access_targets = len(self.access_targets)
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

        hot_load_targets = []
        if self.ws.collections is not None:
            target_scope_collections = self.ws.collections[self.ts.bucket]
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    if target_scope_collections[scope][collection]['load'] == 1:
                        hot_load_targets += [scope+":"+collection]
        else:
            hot_load_targets = ["_default:_default"]
        ws = copy.deepcopy(self.ws)
        ws.items = ws.items // len(hot_load_targets)
        self.cb.connect_collections(hot_load_targets)
        for target in hot_load_targets:
            for key in HotKey(sid, ws, self.ts.prefix):
                self.cb.read(target, key.string)


class SeqUpsertsWorker(Worker):

    def run(self, sid, *args):
        load_targets = []
        if self.ws.collections is not None:
            target_scope_collections = self.ws.collections[self.ts.bucket]
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    if target_scope_collections[scope][collection]['load'] == 1:
                        load_targets += [scope+":"+collection]
        else:
            load_targets = ["_default:_default"]
        ws = copy.deepcopy(self.ws)
        ws.items = ws.items // len(load_targets)
        self.cb.connect_collections(load_targets)
        for target in load_targets:
            for key in SequentialKey(sid, ws, self.ts.prefix):
                doc = self.docs.next(key)
                self.cb.update(target, key.string, doc)


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
        random_roles = [(self.supported_roles[i].role, self.supported_roles[i].bucket_name)
                        if self.supported_roles[i].bucket_name is not None
                        else self.supported_roles[i].role
                        for i in random.choice(self.num_roles,
                                               size=num_random_roles,
                                               replace=False)]
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
        self.num_roles = len(self.supported_roles)

        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
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
        print("creating collection: "+target_scope+":"+target_collection)
        self.cb.do_collection_create(target_scope, target_collection)
        print("dropping collection: "+target_scope+":"+target_collection)
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

        print('target scopes: '+str(self.target_scopes))

        if self.ws.collection_mod_throughput < float('inf'):
            self.target_time = self.ws.collection_mod_workers / \
                               self.ws.collection_mod_throughput
        else:
            self.target_time = None

        logger.info('Started: {}-{}'.format(self.NAME, self.sid))
        try:
            while self.run_condition(curr_ops):
                self.create_delete_collection()
        except KeyboardInterrupt:
            logger.info('Interrupted: {}-{}'.format(self.NAME, self.sid))
        else:
            logger.info('Finished: {}-{}'.format(self.NAME, self.sid))


class WorkerFactory:

    def __new__(cls, settings):
        if getattr(settings, 'async', None):
            worker = AsyncKVWorker
        elif getattr(settings, 'seq_upserts', None):
            worker = SeqUpsertsWorker
        elif getattr(settings, 'hot_reads', None):
            worker = HotReadsWorker
        else:
            worker = KVWorker
        return worker, settings.workers


class AuxillaryWorkerFactory:

    def __new__(cls, settings):
        if getattr(settings, 'user_mod_workers', None):
            return UserModWorker, settings.user_mod_workers
        elif getattr(settings, 'collection_mod_workers', None):
            return CollectionModWorker, settings.collection_mod_workers
        else:
            return AuxillaryWorker, 0


class WorkloadGen:

    def __init__(self, workload_settings, target_settings, timer=None, *args):
        self.ws = workload_settings
        self.ts = target_settings
        self.timer = timer and Timer(timer, self.abort) or None
        self.shutdown_event = timer and Event() or None
        self.worker_processes = []

    def start_workers(self,
                      worker_factory,
                      shared_dict,
                      current_hot_load_start=None,
                      timer_elapse=None):
        curr_ops = Value('L', 0)
        lock = Lock()
        worker_type, total_workers = worker_factory(self.ws)

        for sid in range(total_workers):
            args = (sid, lock, curr_ops, shared_dict,
                    current_hot_load_start, timer_elapse, worker_type,
                    self.ws, self.ts, self.shutdown_event)

            def run_worker(sid, lock, curr_ops, shared_dict,
                           current_hot_load_start, timer_elapse, worker_type,
                           ws, ts, shutdown_event):
                worker = worker_type(ws, ts, shutdown_event)
                worker.run(sid, lock, curr_ops, shared_dict, current_hot_load_start, timer_elapse)

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
        num_load = 0

        zero_string = str(0)
        if self.ws.collections is not None:
            target_scope_collections = self.ws.collections[self.ts.bucket]

            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    if target_scope_collections[scope][collection]['load'] == 1:
                        num_load += 1

            curr_items = self.ws.items // num_load
            curr_items_string = str(curr_items)
            zero_zero = zero_string + ":" + zero_string
            curr_items_zero = curr_items_string + ":" + zero_string
            for scope in target_scope_collections.keys():
                for collection in target_scope_collections[scope].keys():
                    target = scope+":"+collection
                    if target_scope_collections[scope][collection]['load'] == 1:
                        self.shared_dict[target] = curr_items_zero
                    else:
                        self.shared_dict[target] = zero_zero
        else:
            # version prior to 7.0.0
            num_load = 1
            target = "_default:_default"
            self.shared_dict[target] = str(self.ws.items) + ":" + zero_string

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

    def set_signal_handler(self):
        """Abort the execution upon receiving a signal from perfrunner."""
        signal.signal(signal.SIGTERM, self.abort)

    def abort(self, *args):
        """Triggers the shutdown event."""
        self.shutdown_event.set()

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

import glob
import importlib.metadata
import json
import os
import tempfile
import threading
import time
from collections import defaultdict, namedtuple
from multiprocessing import Value
from pathlib import Path
from unittest import TestCase

import snappy

from perfrunner.helpers import shell
from perfrunner.helpers.misc import parse_go_duration_ms, pretty_dict
from perfrunner.remote import api, executor
from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.workloads.bigfun.query_gen import new_queries
from perfrunner.workloads.tcmalloc import KeyValueIterator, LargeIterator
from spring import docgen

sdk_major_version = int(importlib.metadata.version("couchbase")[0])
if sdk_major_version == 2:
    from spring.querygen import N1QLQueryGen
elif sdk_major_version >= 3:
    from spring.querygen3 import N1QLQueryGen3 as N1QLQueryGen


class SettingsTest(TestCase):

    def test_stale_update_after(self):
        test_config = TestConfig()
        test_config.parse('tests/query_lat_20M_basic.test')
        query_params = test_config.access_settings.query_params
        self.assertEqual(query_params, {'stale': 'false'})

    def test_cluster_specs(self):
        for file_name in glob.glob("clusters/*.spec") + glob.glob(
            "cloud/infrastructure/**/*.spec", recursive=True
        ):
            cluster_spec = ClusterSpec()
            cluster_spec.parse(file_name, override=None)

    def test_override(self):
        test_config = TestConfig()
        test_config.parse('tests/query_lat_20M_basic.test',
                          override=['cluster.mem_quota.5555'])
        self.assertEqual(test_config.cluster.mem_quota, 5555)

    def test_soe_backup_repo(self):
        for file_name in glob.glob("tests/soe/*.test"):
            test_config = TestConfig()
            test_config.parse(file_name)
            self.assertNotEqual(test_config.restore_settings.backup_repo, '')

    def test_moving_working_set_settings(self):
        for file_name in glob.glob("tests/gsi/plasma/*.test"):
            test_config = TestConfig()
            test_config.parse(file_name)
            if test_config.access_settings.working_set_move_time:
                self.assertNotEqual(test_config.access_settings.working_set,
                                    100)
                self.assertEqual(test_config.access_settings.working_set_access,
                                 100)

    def test_fts_configs(self):
        for file in glob.glob("tests/fts/enduser/tests_dgm/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/enduser/tests_dgm/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/enduser/tests_dgm/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_non_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_non_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_non_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/multi_node/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_3_nodes')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/multi_node/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_3_nodes')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/multi_node/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_3_nodes')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/rebalance/*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Rebalance')

        for file in glob.glob("tests/fts/single_node/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/single_node/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/single_node/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/single_node_kv/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_kv')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/single_node_kv/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_kv')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')


class MiscTest(TestCase):

    def test_parse_go_duration_ms(self):
        self.assertAlmostEqual(parse_go_duration_ms('1.5s'), 1500.0)
        self.assertAlmostEqual(parse_go_duration_ms('1m30s'), 90000.0)
        self.assertAlmostEqual(parse_go_duration_ms('1m40.0s'), 100000.0)
        self.assertAlmostEqual(parse_go_duration_ms('2h3m4.005s'), 7384005.0)
        self.assertAlmostEqual(parse_go_duration_ms('500µs'), 0.5)
        self.assertAlmostEqual(parse_go_duration_ms('500us'), 0.5)
        self.assertAlmostEqual(parse_go_duration_ms(3.0), 3.0)
        self.assertAlmostEqual(parse_go_duration_ms(3), 3.0)
        self.assertEqual(parse_go_duration_ms(None), 0.0)
        self.assertEqual(parse_go_duration_ms('garbage'), 0.0)
        self.assertEqual(parse_go_duration_ms(''), 0.0)


class WorkloadTest(TestCase):

    def test_value_size(self):
        for _ in range(100):
            iterator = KeyValueIterator(10000)
            batch = iterator.next()
            values = [len(str(v)) for k, v in batch]
            mean = sum(values) / len(values)
            self.assertAlmostEqual(mean, 1024, delta=128)

    def test_large_field_size(self):
        field = LargeIterator()._field('000000000001')
        size = len(str(field))
        self.assertAlmostEqual(size, LargeIterator.FIELD_SIZE, delta=16)


WorkloadSettings = namedtuple('WorkloadSettings', ('items',
                                                   'workers',
                                                   'working_set',
                                                   'working_set_access',
                                                   'working_set_moving_docs',
                                                   'key_fmtr'))


class SpringTest(TestCase):

    def test_seq_key_generator(self):
        ws = WorkloadSettings(items=10 ** 5, workers=25, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = []
        for worker in range(ws.workers):
            generator = docgen.SequentialKey(worker, ws, prefix='test')
            keys += [key.string for key in generator]

        expected = [docgen.Key(number=i, prefix='test', fmtr='decimal').string
                    for i in range(ws.items)]

        self.assertEqual(sorted(keys), expected)

    def test_new_ordered_keys(self):
        ws = WorkloadSettings(items=10 ** 4, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key)

        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        for op in range(1, 10 ** 3):
            key = key_gen.next(ws.items + op)
            self.assertNotIn(key, keys)

    def test_zipf_generator(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key, keys)
                keys.add(key.string)
        self.assertEqual(len(keys), ws.items)

        key_gen = docgen.ZipfKey(prefix='test', fmtr='decimal', alpha=1.5)
        for op in range(10 ** 4):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            self.assertIn(key.string, keys)

    def test_power_generator(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key, keys)
                keys.add(key.string)
        self.assertEqual(len(keys), ws.items)

        key_gen = docgen.PowerKey(prefix='test', fmtr=ws.key_fmtr, alpha=100)
        for op in range(10 ** 4):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            self.assertIn(key.string, keys)

    def test_power_generator_cache_miss(self):
        num_ops = 10 ** 5
        ws = WorkloadSettings(items=10 ** 5, workers=40, working_set=1.6,
                              working_set_access=90, working_set_moving_docs=0,
                              key_fmtr='hex')

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)

        key_gen = docgen.PowerKey(prefix='test', fmtr=ws.key_fmtr, alpha=142)
        misses = 0
        for op in range(num_ops):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            if key.string not in hot_keys:
                misses += 1

        hit_rate = 100 * (1 - misses / num_ops)

        self.assertAlmostEqual(hit_rate, ws.working_set_access, delta=0.5)

    def test_zipf_generator_cache_miss(self):
        num_ops = 10 ** 5
        ws = WorkloadSettings(items=10 ** 5, workers=40, working_set=1.6,
                              working_set_access=90, working_set_moving_docs=0,
                              key_fmtr='hex')

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)

        key_gen = docgen.ZipfKey(prefix='test', fmtr=ws.key_fmtr, alpha=1.23)
        misses = 0
        for op in range(num_ops):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            if key.string not in hot_keys:
                misses += 1

        hit_rate = 100 * (1 - misses / num_ops)

        self.assertAlmostEqual(hit_rate, ws.working_set_access, delta=0.5)

    def doc_generators(self, size: int):
        for dg in (
            docgen.ReverseLookupDocument(avg_size=size, prefix='n1ql'),
            docgen.ReverseRangeLookupDocument(avg_size=size, prefix='n1ql',
                                              range_distance=100),
            docgen.ExtReverseLookupDocument(avg_size=size, prefix='n1ql',
                                            num_docs=10 ** 6),
            docgen.HashJoinDocument(avg_size=size, prefix='n1ql',
                                    range_distance=1000),
            docgen.ArrayIndexingDocument(avg_size=size, prefix='n1ql',
                                         array_size=10, num_docs=10 ** 6),
            docgen.ProfileDocument(avg_size=size, prefix='n1ql'),
            docgen.String(avg_size=size)
        ):
            yield dg

    def test_doc_size(self):
        size = 1024
        key_gen = docgen.NewOrderedKey(prefix='n1ql', fmtr='decimal')

        for dg in self.doc_generators(size=size):
            for i in range(10 ** 4):
                key = key_gen.next(i)
                doc = dg.next(key=key)
                actual_size = len(str(doc))
                self.assertAlmostEqual(actual_size, size,
                                       delta=size * 0.05,  # 5% variation
                                       msg=dg.__class__.__name__)

    def test_doc_size_variation(self):
        size = 512
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.Document(avg_size=size)

        for i in range(10 ** 4):
            key = key_gen.next(i)
            doc = doc_gen.next(key=key)
            actual_size = len(str(doc))
            self.assertAlmostEqual(actual_size, size,
                                   delta=size * doc_gen.SIZE_VARIATION)

    def test_small_documents(self):
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.Document(avg_size=150)

        for i in range(10 ** 3):
            key = key_gen.next(i)
            doc = doc_gen.next(key=key)
            size = len(str(doc))

            self.assertEqual(doc["body"], "")
            self.assertAlmostEqual(size, doc_gen.OVERHEAD, delta=100)

    def test_large_documents(self):
        size = 1024
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.LargeDocument(avg_size=size)

        for i in range(10 ** 4):
            key = key_gen.next(i)
            doc = doc_gen.next(key=key)
            value = json.dumps(doc)
            actual_size = len(value)

            self.assertAlmostEqual(actual_size, size,
                                   delta=size * doc_gen.SIZE_VARIATION,
                                   msg=value)

    def test_compression_ratio(self):
        size = 1024
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.LargeDocument(avg_size=size)

        for i in range(10 ** 4):
            key = key_gen.next(i)
            doc = doc_gen.next(key)
            value = json.dumps(doc)

            compressed = snappy.compress(value)
            ratio = len(value) / len(compressed)

            self.assertLess(ratio, 1.75, value)

    def test_hot_keys(self):
        ws = WorkloadSettings(items=10 ** 4, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key.string, keys)
                keys.add(key.string)
        self.assertEqual(len(keys), ws.items)

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key.string, hot_keys)
                self.assertIn(key.string, keys)
                hot_keys.add(key.string)
        self.assertEqual(len(hot_keys), ws.working_set * ws.items // 100)

    def test_uniform_keys(self):
        ws = WorkloadSettings(items=10 ** 3, workers=10, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.UniformKey(prefix='test',
                                    fmtr='decimal')
        for op in range(10 ** 4):
            key = key_gen.next(curr_items=ws.items, curr_deletes=100)
            self.assertIn(key.string, keys)

    def test_working_set_keys(self):
        ws = WorkloadSettings(items=10 ** 3, workers=10, working_set=90,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.WorkingSetKey(ws=ws, prefix='test')
        for op in range(10 ** 4):
            key = key_gen.next(curr_items=ws.items, curr_deletes=0)
            self.assertIn(key.string, keys)

    def test_moving_working_set_keys(self):
        ws = WorkloadSettings(items=10 ** 3, workers=10, working_set=90,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='decimal')
        current_hot_load_start = Value('L', 0)
        timer_elapse = Value('I', 0)

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.MovingWorkingSetKey(ws, prefix='test')
        keys = sorted(keys)

        for op in range(10 ** 4):
            key = key_gen.next(curr_items=ws.items,
                               curr_deletes=0,
                               current_hot_load_start=current_hot_load_start,
                               timer_elapse=timer_elapse)
            self.assertIn(key.string, keys)

    def test_cas_updates(self):
        ws = WorkloadSettings(items=10 ** 3, workers=20, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        cases = defaultdict(set)
        key_gen = docgen.KeyForCASUpdate(total_workers=ws.workers, prefix='test',
                                         fmtr='decimal')
        for sid in 5, 6:
            for op in range(10 ** 3):
                key = key_gen.next(sid=sid, curr_items=ws.items)
                self.assertIn(key.string, keys)
                cases[sid].add(key.string)
        self.assertEqual(cases[5] & cases[6], set())

    def test_key_for_removal(self):
        ws = WorkloadSettings(items=10 ** 3, workers=20, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.KeyForRemoval(prefix='test', fmtr='decimal')
        for op in range(1, 100):
            key = key_gen.next(op)
            self.assertIn(key.string, keys)

    def test_keys_without_prefix(self):
        ws = WorkloadSettings(items=10 ** 3, workers=20, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix=''):
                keys.add(key.string)

        expected = [docgen.Key(number=i, prefix='', fmtr='decimal').string
                    for i in range(ws.items)]

        self.assertEqual(sorted(keys), expected)

    def test_hash_fmtr(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='hash')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key.string, keys)
                self.assertEqual(len(key.string), 16)
                keys.add(key.string)

    def test_new_working_set_hits(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='hex')

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)
        hot_keys = sorted(hot_keys)

        wsk = docgen.WorkingSetKey(ws=ws, prefix='test')
        hits = set()
        news_items = 10
        for op in range(10 ** 5):
            key = wsk.next(curr_items=ws.items + news_items, curr_deletes=100)
            if key.hit:
                hits.add(key.string)

        overlap = set(hot_keys) & hits
        self.assertEqual(len(overlap),
                         ws.items * (ws.working_set / 100) - news_items)

    def test_working_set_hits(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='hex')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)
        keys = sorted(keys)

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)
        hot_keys = sorted(hot_keys)

        wsk = docgen.WorkingSetKey(ws=ws, prefix='test')
        for op in range(10 ** 5):
            key = wsk.next(curr_items=ws.items, curr_deletes=100)
            self.assertIn(key.string, keys)
            if key.hit:
                self.assertIn(key.string, hot_keys)
            else:
                self.assertNotIn(key.string, hot_keys)

    def test_working_set_deletes(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='hex')

        keys_for_removal = docgen.KeyForRemoval(prefix='test',
                                                fmtr=ws.key_fmtr)
        removed_keys = set()
        for i in range(100):
            key = keys_for_removal.next(i)
            removed_keys.add(key.string)
        removed_keys = sorted(removed_keys)

        wsk = docgen.WorkingSetKey(ws=ws, prefix='test')
        for op in range(10 ** 5):
            key = wsk.next(curr_items=ws.items + 100, curr_deletes=100)
            self.assertNotIn(key.string, removed_keys)

    def test_collisions(self):
        ws = WorkloadSettings(items=10 ** 5, workers=25, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = []
        for worker in range(ws.workers):
            generator = docgen.SequentialKey(worker, ws, prefix='test')
            keys += [key.string for key in generator]

        hashes = set()
        for key in keys:
            _hash = docgen.hex_digest(key)
            self.assertNotIn(_hash, hashes)
            hashes.add(_hash)

    def test_package_doc(self):
        ws = WorkloadSettings(items=10 ** 6, workers=100, working_set=15,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='hex')

        generator = docgen.PackageDocument(avg_size=0)
        dates = set()
        for key in docgen.SequentialKey(sid=50, ws=ws, prefix='test'):
            doc = generator.next(key)
            dates.add(doc['shippingDate'])
            self.assertEqual(doc['minorAccountId'], doc['majorAccountId'])
        self.assertEqual(len(dates), ws.items // ws.workers)

    def test_incompressible_docs(self):
        size = 15 * 1024
        generator = docgen.IncompressibleString(avg_size=size)
        doc = generator.next(key=docgen.Key(number=0, prefix='', fmtr=''))
        self.assertEqual(len(doc), size)


class QueryTest(TestCase):

    def test_n1ql_query_gen_q1(self):
        queries = [{
            'statement': 'SELECT * FROM `bucket-1` USE KEYS[$1];',
            'args': '["{key}"]',
        }]

        if sdk_major_version == 2:
            qg = N1QLQueryGen(queries=queries)
        elif sdk_major_version >= 3:
            qg = N1QLQueryGen(queries=queries, query_weight=[1])

        for key in 'n1ql-0123456789', 'n1ql-9876543210':
            if sdk_major_version >= 3:
                stmt, queryopts = qg.next(key, doc={})
                self.assertEqual(queryopts['adhoc'], False)
                self.assertEqual(str(queryopts['scan_consistency']),
                                 'QueryScanConsistency.NOT_BOUNDED')
                self.assertEqual(queryopts['positional_parameters'], [key])
            else:
                query = qg.next(key, doc={})
                self.assertEqual(query.adhoc, False)
                self.assertEqual(query.consistency, 'not_bounded')
                self.assertEqual(query._body['args'], [key])

    def test_n1ql_query_gen_q2(self):
        queries = [{
            'statement': 'SELECT * FROM `bucket-1` WHERE email = $1;',
            'args': '["{email}"]',
            'scan_consistency': 'request_plus',
        }]

        if sdk_major_version == 2:
            qg = N1QLQueryGen(queries=queries)
        elif sdk_major_version >= 3:
            qg = N1QLQueryGen(queries=queries, query_weight=[1])

        for doc in {'email': 'a@a.com'}, {'email': 'b@b.com'}:
            if sdk_major_version >= 3:
                stmt, queryopts = qg.next(key='n1ql-0123456789', doc=doc)
                self.assertEqual(str(queryopts['scan_consistency']),
                                 'QueryScanConsistency.REQUEST_PLUS')
                self.assertEqual(queryopts['positional_parameters'],
                                 [doc['email']])
            else:
                query = qg.next(key='n1ql-0123456789', doc=doc)
                self.assertEqual(query.consistency, 'request_plus')
                self.assertEqual(query._body['args'], [doc['email']])


class BigFunTest(TestCase):

    def test_unique_statements(self):
        queries = "perfrunner/workloads/bigfun/queries_with_index.yaml"
        for query in new_queries(queries):
            statements = set()
            for i in range(10):
                self.assertNotIn(query.statement, statements)
                statements.add(query.statement)


class PipelineTest(TestCase):
    def test_existence_of_test_configs(self):
        """Check if all test configs in the pipelines are present in the tests directory."""
        all_missing_test_configs = {}
        filenames_to_paths = {}

        test_config_keys = ["test_config", "test", "analytics_test_config", "kv_test_config"]

        for root, _, files in os.walk("tests"):
            for file in files:
                if not file.endswith(".test"):
                    continue

                if file not in filenames_to_paths:
                    filenames_to_paths[file] = []
                filenames_to_paths[file].append(root)

        for fn in glob.glob("tests/pipelines/*.json"):
            with open(fn, "r") as f:
                test_cases = json.load(f)

            for stage, stage_tests in test_cases.items():
                test_configs = [
                    t
                    for test in stage_tests
                    for t in [test[k] for k in test_config_keys if k in test]
                ]
                missing_stage_test_configs = []

                for test_config in test_configs:
                    parent_path = str(Path(test_config).parent)
                    name = Path(test_config).name

                    if (paths := filenames_to_paths.get(name, [])) and parent_path == ".":
                        continue
                    elif any(root.endswith(parent_path) for root in paths):
                        continue
                    else:
                        missing_stage_test_configs.append(test_config)

                if missing_stage_test_configs:
                    if fn not in all_missing_test_configs:
                        all_missing_test_configs[fn] = {}
                    all_missing_test_configs[fn][stage] = missing_stage_test_configs

        self.assertDictEqual(
            all_missing_test_configs,
            {},
            "\nTest configs from the following pipeline files are missing: \n"
            + pretty_dict(all_missing_test_configs),
        )

    def test_stages(self):
        stages = {'Analytics', 'Eventing', 'FTS', 'Tools', 'Views',
                  'GSI', 'GSI-DGM',
                  'N1QL', 'N1QL-Windows', 'N1QL-Arke', 'YCSB', 'YCSB-Hebe',
                  'KV', 'KV-DGM', 'KV-Windows', 'KV-Athena', 'KV-Hercules',
                  'Rebalance', 'Rebalance-C1', 'Rebalance-C2', 'Rebalance-Demeter',
                  'Rebalance-Large-Scale', 'Rebalance-Large-Scale-C1', 'Rebalance-Large-Scale-C2',
                  'XDCR', 'XDCR-Windows', 'XDCR-C1', 'XDCR-C2'}
        for pipeline in ('tests/pipelines/weekly-watson.json',
                         'tests/pipelines/weekly-spock.json',
                         'tests/pipelines/weekly-vulcan.json',
                         'tests/pipelines/weekly-alice.json'):
            with open(pipeline) as fh:
                test_cases = json.load(fh)
                self.assertEqual(stages, set(test_cases), pipeline)


class LocalShellTest(TestCase):
    def test_capture_returns_stripped_stdout_with_attributes(self):
        with shell.quiet():
            result = shell.local("echo hello && echo oops >&2", capture=True)
        self.assertEqual(result, "hello")
        self.assertEqual(result.stdout, "hello")
        self.assertEqual(result.stderr, "oops")
        self.assertEqual(result.return_code, 0)
        self.assertTrue(result.succeeded)
        self.assertFalse(result.failed)

    def test_output_state_shared_with_remote_api(self):
        # RemoteHelper sets state.output.stdout/running from its verbose flag; that
        # must control local() too, like fabric.state.output did (Fabric 1 parity).
        self.assertIs(api.output, shell.output)
        self.assertIs(api.state.output, shell.output)

    def test_non_verbose_discards_output_and_echo(self):
        saved_running, saved_stdout = shell.output.running, shell.output.stdout
        try:
            shell.output.running = shell.output.stdout = False
            self.assertTrue(shell._is_hidden("running"))
            self.assertTrue(shell._is_hidden("output"))
            result = shell.local("true")  # passthrough mode routes to devnull, no echo
            self.assertEqual(result.return_code, 0)
        finally:
            shell.output.running, shell.output.stdout = saved_running, saved_stdout
        self.assertFalse(shell._is_hidden("output"))

    def test_stderr_stays_visible_in_non_verbose_mode(self):
        # Fabric 1 keyed the streams separately and RemoteHelper only disables stdout,
        # so error text from local commands must survive non-verbose runs.
        saved = shell.output.stdout
        try:
            shell.output.stdout = False
            self.assertTrue(shell._is_hidden("output"))
            self.assertFalse(shell._is_hidden("stderr"))
        finally:
            shell.output.stdout = saved
        with shell.hide("output"):  # per-call hide("output") covers both streams
            self.assertTrue(shell._is_hidden("stderr"))

    def test_passthrough_mode_inherits_stdio(self):
        # capture=False with nothing hidden: child stdout/stderr inherit from the
        # parent and the result string is empty, but attributes are still populated.
        result = shell.local("true")
        self.assertEqual(result, "")
        self.assertEqual(result.return_code, 0)
        self.assertTrue(result.succeeded)

    def test_failure_aborts_by_default(self):
        with self.assertRaises(SystemExit):
            with shell.hide("everything"):
                shell.local("exit 1", capture=True)

    def test_warn_only_returns_failed_result(self):
        with shell.quiet():
            result = shell.local("exit 7", capture=True)
        self.assertEqual(result.return_code, 7)
        self.assertTrue(result.failed)
        self.assertFalse(result.succeeded)

    def test_settings_with_hide_and_warn_only(self):
        with shell.settings(shell.hide("output", "warnings"), warn_only=True):
            result = shell.local("exit 3")
        self.assertEqual(result.return_code, 3)

    def test_settings_rejects_unknown_keys(self):
        with self.assertRaises(TypeError):
            with shell.settings(host_string="node-1"):
                pass

    def test_lcd_nests_relative_paths(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sub_dir = os.path.join(tmp_dir, "sub")
            os.mkdir(sub_dir)
            with shell.quiet(), shell.lcd(tmp_dir), shell.lcd("sub"):
                result = shell.local("pwd", capture=True)
        self.assertEqual(os.path.realpath(result), os.path.realpath(sub_dir))

    def test_lcd_restores_previous_directory(self):
        with shell.quiet():
            with shell.lcd("/"):
                pass
            result = shell.local("pwd", capture=True)
        self.assertEqual(os.path.realpath(result), os.path.realpath(os.getcwd()))

    def test_shell_env_exports_variables(self):
        with shell.quiet(), shell.shell_env(FOO="bar", BAZ="qux"):
            result = shell.local("echo $FOO-$BAZ", capture=True)
        self.assertEqual(result, "bar-qux")

    def test_shell_executable_override(self):
        with shell.quiet():
            result = shell.local("echo $0", capture=True, shell="/bin/bash")
        self.assertEqual(result, "/bin/bash")

    def test_command_attributes_record_real_command(self):
        with shell.quiet(), shell.lcd("/tmp"), shell.shell_env(FOO="bar"):
            result = shell.local("true", capture=True)
        self.assertEqual(result.command, "true")
        self.assertIn("cd /tmp", result.real_command)
        self.assertIn('export FOO="bar"', result.real_command)


class RemoteApiTest(TestCase):
    def setUp(self):
        self.created = {}

        def factory(host, config, gateway=None):
            session = executor.FakeSession(host=host)
            session.config = config
            session.gateway = gateway
            responses = self.scripted.get(host, {})
            session.responses.update(responses)
            self.created[host] = session
            return session

        self.scripted = {}
        self._saved_pool = api.pool
        api.pool = executor.ConnectionPool(factory)
        self._saved_cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)

    def tearDown(self):
        os.chdir(self._saved_cwd)
        self._tmp.cleanup()
        api.pool = self._saved_pool

    def test_run_wraps_command_in_login_shell(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run("echo hello")
        command, kwargs = self.created["node-1"].commands[0]
        self.assertEqual(command, '/bin/bash -l -c "echo hello"')
        self.assertTrue(kwargs["pty"])

    def test_run_escapes_shell_characters(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run('echo "$HOME" `id`')
            api.run('echo "$HOME"', shell_escape=False)
        escaped, _ = self.created["node-1"].commands[0]
        raw, _ = self.created["node-1"].commands[1]
        self.assertEqual(escaped, '/bin/bash -l -c "echo \\"\\$HOME\\" \\`id\\`"')
        self.assertEqual(raw, '/bin/bash -l -c "echo "$HOME""')

    def test_cd_and_shell_env_prefixes(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            with api.cd("/tmp/perfrunner"), api.cd("worker"), api.shell_env(GOGC="300"):
                api.run("make")
        command, _ = self.created["node-1"].commands[0]
        self.assertEqual(
            command, '/bin/bash -l -c "cd /tmp/perfrunner/worker && export GOGC=\\"300\\" && make"'
        )

    def test_connection_reuse_across_calls(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run("true")
            api.run("true")
        self.assertEqual(len(self.created), 1)
        self.assertEqual(len(self.created["node-1"].commands), 2)

    def test_execute_parallel_returns_dict_by_host(self):
        hosts = ["h1", "h2", "h3"]
        wrapped = '/bin/bash -l -c "hostname"'
        for host in hosts:
            self.scripted[host] = {wrapped: executor.RunResult(f"out-{host}", "", 0)}

        @api.parallel
        def task():
            return str(api.run("hostname", quiet=True))

        results = api.execute(task, hosts=hosts)
        self.assertEqual(results, {host: f"out-{host}" for host in hosts})
        for host in hosts:
            self.assertEqual(len(self.created[host].commands), 1)

    def test_execute_parallel_all_hosts_complete_despite_failure(self):
        # One failing host must not discard the other hosts' work, and the
        # original exception type must surface (not a generic wrapper).
        wrapped = '/bin/bash -l -c "hostname"'
        self.scripted = {
            "h1": {wrapped: executor.RunResult("out-h1", "", 0)},
            "h2": {wrapped: executor.CommandTimeout("timed out")},
            "h3": {wrapped: executor.RunResult("out-h3", "", 0)},
        }

        @api.parallel
        def task():
            return str(api.run("hostname", quiet=True, timeout=5))

        with self.assertRaises(executor.CommandTimeout):
            api.execute(task, hosts=["h1", "h2", "h3"])
        for host in ("h1", "h2", "h3"):
            self.assertEqual(len(self.created[host].commands), 1)

    def test_execute_serial_lambda(self):
        results = api.execute(lambda: api.run("true", quiet=True), hosts=["h1"])
        self.assertIn("h1", results)

    def test_run_failure_aborts_by_default(self):
        self.scripted["node-1"] = {'/bin/bash -l -c "false"': executor.RunResult("", "", 1)}
        with self.assertRaises(SystemExit):
            with api.settings(api.hide("everything"), host_string="node-1"):
                api.run("false")

    def test_run_failure_with_warn_only(self):
        self.scripted["node-1"] = {'/bin/bash -l -c "false"': executor.RunResult("", "", 1)}
        with api.settings(api.hide("everything"), host_string="node-1"):
            result = api.run("false", warn_only=True)
        self.assertEqual(result.return_code, 1)
        self.assertTrue(result.failed)

    def test_run_failure_reports_stderr(self):
        wrapped = '/bin/bash -l -c "systemctl restart couchbase-server"'
        self.scripted["n1"] = {
            wrapped: executor.RunResult("", "Job for couchbase-server failed", 1)
        }
        with self.assertLogs(level="WARNING") as logs:
            with api.settings(api.hide("running", "output"), host_string="n1"):
                result = api.run("systemctl restart couchbase-server", warn_only=True, pty=False)
        self.assertEqual(result.stderr, "Job for couchbase-server failed")
        self.assertTrue(any("Job for couchbase-server failed" in line for line in logs.output))

    def test_command_timeout_propagates(self):
        self.scripted["node-1"] = {
            '/bin/bash -l -c "sleep 100"': executor.CommandTimeout("timed out")
        }
        with self.assertRaises(executor.CommandTimeout):
            with api.settings(api.hide("everything"), host_string="node-1"):
                api.run("sleep 100", timeout=10)

    def test_get_glob_with_default_host_layout(self):
        self.scripted["10.1.1.1"] = {}
        with api.settings(api.hide("everything"), host_string="10.1.1.1"):
            session = api.pool.session("10.1.1.1", executor.SessionConfig())
            session.files = {"/tmp/aaa.zip": "x", "/tmp/bbb.zip": "y", "/tmp/keep.log": "z"}
            downloaded = api.get("/tmp/*.zip")
        self.assertEqual(
            sorted(session.downloads),
            [
                ("/tmp/aaa.zip", os.path.join("10.1.1.1", "tmp", "aaa.zip")),
                ("/tmp/bbb.zip", os.path.join("10.1.1.1", "tmp", "bbb.zip")),
            ],
        )
        self.assertEqual(len(downloaded), 2)

    def test_get_relative_path_uses_cd(self):
        with api.settings(api.hide("everything"), host_string="w1"):
            session = api.pool.session("w1", executor.SessionConfig())
            session.files = {"/worker/perfrunner/worker_1.log": "log"}
            with api.cd("/worker/perfrunner"):
                api.get("worker_*.log", local_path="celery/")
        self.assertEqual(
            session.downloads,
            [("/worker/perfrunner/worker_1.log", os.path.join("celery", "worker_1.log"))],
        )

    def test_get_single_file_default_lands_at_host_slash_basename(self):
        # Contract with the debug flow: a bare get() of one file must land exactly one level deep.
        # Fabric 1 collapsed %(path)s to the basename for single-file downloads.
        with api.settings(api.hide("everything"), host_string="10.1.1.3"):
            session = api.pool.session("10.1.1.3", executor.SessionConfig())
            session.files = {"/tmp/abc123.zip": "z"}
            downloaded = api.get("/tmp/abc123.zip")
        self.assertEqual(downloaded, [os.path.join("10.1.1.3", "abc123.zip")])

    def test_get_glob_default_keeps_full_path(self):
        # Glob downloads keep the full remote path under <host>/ to avoid collisions.
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            session.files = {"/tmp/a.zip": "a", "/tmp/b.zip": "b"}
            downloaded = api.get("/tmp/*.zip")
        self.assertEqual(
            sorted(downloaded),
            [os.path.join("h1", "tmp", "a.zip"), os.path.join("h1", "tmp", "b.zip")],
        )

    def test_put_directory_recursively(self):
        os.makedirs("inbox/sub")
        Path("inbox/chain.pem").write_text("pem")
        Path("inbox/sub/node.key").write_text("key")
        with api.settings(api.hide("everything"), host_string="n1"):
            uploaded = api.put("inbox", "/opt/couchbase/var/lib/couchbase")
        session = self.created["n1"]
        self.assertIn(
            ("inbox/chain.pem", "/opt/couchbase/var/lib/couchbase/inbox/chain.pem"),
            [(os.path.relpath(local), remote) for local, remote in session.uploads],
        )
        self.assertIn("/opt/couchbase/var/lib/couchbase/inbox/sub", session.dirs)
        self.assertEqual(len(uploaded), 2)

    def test_get_directory_downloads_tree_recursively(self):
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            session.dirs = {"/data", "/data/a", "/data/b"}
            session.files = {"/data/f0": "0", "/data/a/f1": "1", "/data/b/f2": "2"}
            downloaded = api.get("/data", local_path="out/")
        self.assertEqual(sorted(session.downloads), [
            ("/data/a/f1", os.path.join("out", "data", "a", "f1")),
            ("/data/b/f2", os.path.join("out", "data", "b", "f2")),
            ("/data/f0", os.path.join("out", "data", "f0")),
        ])
        self.assertEqual(sorted(downloaded), sorted(local for _, local in session.downloads))
        self.assertTrue(os.path.isdir(os.path.join("out", "data", "a")))

    def test_download_tree_depth_guard(self):
        # SFTP has no inode info for cycle detection; a symlink loop must fail fast
        # with a clear error instead of a RecursionError.
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            path = ""
            for level in range(api.MAX_TREE_DEPTH + 2):
                path = f"{path}/d"
                session.dirs.add(path)
            with self.assertRaises(RuntimeError):
                api.get("/d")

    def test_concurrent_channels_capped_per_session(self):
        # Nested parallel decorators stack many threads on one pooled connection;
        # channel opens must be capped below sshd MaxSessions.
        counters = {"current": 0, "max": 0}
        guard = threading.Lock()

        def tracked_run_raw(command, pty=True, timeout=None):
            with guard:
                counters["current"] += 1
                counters["max"] = max(counters["max"], counters["current"])
            time.sleep(0.02)
            with guard:
                counters["current"] -= 1
            return executor.RunResult("", "", 0)

        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
        session.run_raw = tracked_run_raw

        def worker():
            with api.settings(api.hide("everything"), host_string="h1"):
                api.run("true", quiet=True)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertGreater(counters["max"], 1)
        self.assertLessEqual(counters["max"], executor.Session.MAX_CONCURRENT_CHANNELS)

    def test_put_file_into_existing_remote_directory(self):
        # Fabric 1 parity: put("root.pem", "<dir>") with an existing remote directory
        # (no trailing slash) must land <dir>/root.pem, not open the dir for write.
        Path("root.pem").write_text("pem")
        with api.settings(api.hide("everything"), host_string="c1"):
            session = api.pool.session("c1", executor.SessionConfig())
            session.dirs.add("/worker/perfrunner")
            uploaded = api.put("root.pem", "/worker/perfrunner")
        self.assertEqual(uploaded, ["/worker/perfrunner/root.pem"])
        self.assertEqual(session.uploads, [("root.pem", "/worker/perfrunner/root.pem")])

    def test_gateway_creates_jump_session(self):
        with api.settings(api.hide("everything"), host_string="kafka-1", gateway="jump-1"):
            api.run("true")
        self.assertIn("jump-1", self.created)
        self.assertIs(self.created["kafka-1"].gateway, self.created["jump-1"])

    def test_append_is_idempotent_grep(self):
        with api.settings(api.hide("everything"), host_string="n1"):
            api.append("/opt/tomcat/bin/setenv.sh", "export LD_LIBRARY_PATH=/x")
        command, _ = self.created["n1"].commands[0]
        self.assertIn("grep -qF -- 'export LD_LIBRARY_PATH=/x'", command)
        self.assertIn("| tee -a /opt/tomcat/bin/setenv.sh", command)
        self.assertNotIn("sudo", command)

    def test_append_with_use_sudo(self):
        with api.settings(api.hide("everything"), host_string="n1"):
            api.append("/opt/tomcat/bin/setenv.sh", "export LD_LIBRARY_PATH=/x", use_sudo=True)
        command, _ = self.created["n1"].commands[0]
        self.assertIn("sudo grep -qF", command)
        self.assertIn("| sudo tee -a /opt/tomcat/bin/setenv.sh", command)

    def test_state_aliases(self):
        self.assertIs(api.state.env, api.env)
        self.assertIs(api.state.output, api.output)

    def test_settings_rejects_unknown_keys(self):
        with self.assertRaises(TypeError):
            with api.settings(bogus=1):
                pass


class RemoteCharacterisationTest(TestCase):
    """Pin the remote layer's contract: host targeting, command strings, result shapes.

    Uses FakeSession as the executor, so no SSH connection is made. The command strings
    are the interface to remote machines. Any refactor of the execution layer or the
    topology decorators must keep them identical.
    """

    SPEC = (
        "[clusters]\n"
        "test =\n"
        "    10.0.0.1:kv\n"
        "    10.0.0.2:kv\n"
        "    10.0.0.3:index\n"
        "\n"
        "[clients]\n"
        "hosts =\n"
        "    10.0.1.1\n"
        "\n"
        "[storage]\n"
        "data = /data\n"
        "\n"
        "[metadata]\n"
        "cluster = test\n"
    )

    def setUp(self):
        self.created = {}
        self.scripted = {}

        def factory(host, config, gateway=None):
            session = executor.FakeSession(host=host)
            session.responses.update(self.scripted.get(host, {}))
            self.created[host] = session
            return session

        self._saved_pool = api.pool
        api.pool = executor.ConnectionPool(factory)

        spec_file = tempfile.NamedTemporaryFile(mode="w", suffix=".spec", delete=False)
        spec_file.write(self.SPEC)
        spec_file.close()
        self.spec_fname = spec_file.name
        self.cluster_spec = ClusterSpec()
        self.cluster_spec.parse(self.spec_fname, override=None)

    def tearDown(self):
        api.pool = self._saved_pool
        os.unlink(self.spec_fname)

    def _remote(self):
        from perfrunner.remote.linux import RemoteLinux

        return RemoteLinux(self.cluster_spec)

    def test_construction_detects_distro_on_master_only(self):
        self._remote()
        self.assertEqual(list(self.created), ["10.0.0.1"])
        commands = [command for command, _ in self.created["10.0.0.1"].commands]
        self.assertEqual(len(commands), 2)
        self.assertIn("grep ^ID= /etc/os-release", commands[0])
        self.assertIn("grep ^VERSION_ID= /etc/os-release", commands[1])

    def test_reset_swap_runs_on_all_servers(self):
        remote = self._remote()
        remote.reset_swap()
        expected = '/bin/bash -l -c "swapoff --all && swapon --all"'
        for server in ("10.0.0.1", "10.0.0.2", "10.0.0.3"):
            commands = [command for command, _ in self.created[server].commands]
            self.assertIn(expected, commands)

    def test_master_server_decorator_targets_first_server(self):
        remote = self._remote()
        remote.enable_nonlocal_diag_eval()
        command, kwargs = self.created["10.0.0.1"].commands[-1]
        self.assertIn("diag/eval", command)
        self.assertFalse(kwargs["pty"])
        self.assertNotIn("10.0.0.2", self.created)

    def test_detect_core_dumps_returns_dict_per_host(self):
        wrapped = '/bin/bash -l -c "ls /data/core*"'
        self.scripted = {
            "10.0.0.1": {wrapped: executor.RunResult("/data/core-memcached-1", "", 0)},
            "10.0.0.2": {wrapped: executor.RunResult("", "", 2)},
            "10.0.0.3": {wrapped: executor.RunResult("", "", 2)},
        }
        remote = self._remote()
        dumps = remote.detect_core_dumps()
        self.assertEqual(
            dumps, {"10.0.0.1": ["/data/core-memcached-1"], "10.0.0.2": [], "10.0.0.3": []}
        )

    def test_all_clients_decorator_targets_workers(self):
        remote = self._remote()
        remote.terminate_client_processes()
        commands = [command for command, _ in self.created["10.0.1.1"].commands]
        self.assertTrue(any("killall -9" in command for command in commands))


class ConnectionPoolTest(TestCase):
    """Session lifecycle in the pool: reuse, probe-on-idle liveness, and fork safety."""

    def setUp(self):
        self.pool = executor.ConnectionPool(
            lambda host, config, gateway=None: executor.FakeSession(host=host)
        )
        self.config = executor.SessionConfig()

    def test_no_probe_when_recently_used(self):
        first = self.pool.session("h1", self.config)
        second = self.pool.session("h1", self.config)
        self.assertIs(first, second)
        self.assertEqual(first.probes, 0)

    def test_probe_after_idle_reuses_healthy_session(self):
        session = self.pool.session("h1", self.config)
        session.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        again = self.pool.session("h1", self.config)
        self.assertIs(session, again)
        self.assertEqual(session.probes, 1)

    def test_dead_idle_session_is_replaced(self):
        session = self.pool.session("h1", self.config)
        session.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        session.probe_error = executor.NetworkError("dropped by NAT")
        replacement = self.pool.session("h1", self.config)
        self.assertIsNot(session, replacement)
        self.assertTrue(session.closed)
        self.assertEqual(session.probes, 1)

    def test_slow_probe_does_not_block_other_hosts(self):
        # The pool lock only guards its dicts; a dead host's probe (up to the channel
        # open timeout) must not stall parallel checkouts of healthy hosts.
        slow = self.pool.session("slow-host", self.config)
        slow.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        slow.probe_delay = 1.0

        prober = threading.Thread(
            target=self.pool.session, args=("slow-host", self.config))
        prober.start()
        time.sleep(0.1)  # let the probe start and hold slow-host's key lock

        t0 = time.time()
        self.pool.session("healthy-host", self.config)
        elapsed = time.time() - t0
        prober.join()
        self.assertLess(elapsed, 0.5)
        self.assertEqual(slow.probes, 1)

    def test_dead_gateway_session_closed_before_replacement(self):
        first = self.pool.session("kafka-1", self.config, gateway="jump-1")
        gateway_key = ("jump-1", self.config.user, None)
        old_gateway = self.pool._sessions[gateway_key]

        old_gateway.active = False  # gateway died; host session rides it, so it dies too
        first.active = False
        self.pool.session("kafka-1", self.config, gateway="jump-1")

        new_gateway = self.pool._sessions[gateway_key]
        self.assertIsNot(old_gateway, new_gateway)
        self.assertTrue(old_gateway.closed)

    def test_forked_child_discards_inherited_sessions_without_closing(self):
        # Forked children (e.g. cbagent collector processes) must neither reuse nor close the
        # parent's SSH sockets: sharing the encrypted stream corrupts it,
        # and closing sends disconnects on a socket the parent still owns.
        parent_session = self.pool.session("h1", self.config)
        self.pool._pid -= 1  # simulate being in a forked child
        child_session = self.pool.session("h1", self.config)
        self.assertIsNot(parent_session, child_session)
        self.assertFalse(parent_session.closed)


class SSHSessionTest(TestCase):
    """Construction-time behaviour of the Fabric-backed session (no connection made)."""

    def test_host_key_policy_follows_disable_known_hosts(self):
        from paramiko.client import AutoAddPolicy, RejectPolicy

        default = executor.SSHSession("10.9.9.9", executor.SessionConfig())
        self.assertIsInstance(default._conn.client._policy, AutoAddPolicy)

        strict = executor.SSHSession(
            "10.9.9.9", executor.SessionConfig(disable_known_hosts=False)
        )
        self.assertIsInstance(strict._conn.client._policy, RejectPolicy)

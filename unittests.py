import glob
import json
import pkg_resources
from collections import defaultdict, namedtuple
from multiprocessing import Value
from unittest import TestCase

import snappy

from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.workloads.bigfun.query_gen import new_queries
from perfrunner.workloads.tcmalloc import KeyValueIterator, LargeIterator
from spring import docgen

cb_version = pkg_resources.get_distribution("couchbase").version
if cb_version[0] == '2':
    from spring.querygen import N1QLQueryGen
elif cb_version[0] == '3':
    from spring.querygen3 import N1QLQueryGen3 as N1QLQueryGen


class SettingsTest(TestCase):

    def test_stale_update_after(self):
        test_config = TestConfig()
        test_config.parse('tests/query_lat_20M_basic.test')
        query_params = test_config.access_settings.query_params
        self.assertEqual(query_params, {'stale': 'false'})

    def test_cluster_specs(self):
        for file_name in glob.glob("clusters/*.spec"):
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
            self.assertAlmostEqual(size, doc_gen.OVERHEAD, delta=20)

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

        qg = N1QLQueryGen(queries=queries)

        for key in 'n1ql-0123456789', 'n1ql-9876543210':
            if cb_version[0] == '3':
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

        qg = N1QLQueryGen(queries=queries)

        for doc in {'email': 'a@a.com'}, {'email': 'b@b.com'}:
            if cb_version[0] == '3':
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
        QUERIES = 'perfrunner/workloads/bigfun/queries_with_index.json'
        for query in new_queries(QUERIES):
            statements = set()
            for i in range(10):
                self.assertNotIn(query.statement, statements)
                statements.add(query.statement)


class PipelineTest(TestCase):

    def test_stages(self):
        stages = {'Analytics', 'Eventing', 'FTS', 'GSI', 'GSI-DGM', 'KV', 'KV-DGM',
                  'N1QL', 'Rebalance', 'Rebalance-Large-Scale', 'Tools',
                  'Views', 'XDCR', 'YCSB','N1QL-Windows', 'N1QL-Arke',
                  'KV-Windows', 'XDCR-Windows', 'KV-Athena', 'KV-Hercules'}
        for pipeline in ('tests/pipelines/weekly-watson.json',
                         'tests/pipelines/weekly-spock.json',
                         'tests/pipelines/weekly-vulcan.json',
                         'tests/pipelines/weekly-alice.json'):
            with open(pipeline) as fh:
                test_cases = json.load(fh)
                self.assertEqual(stages, set(test_cases), pipeline)

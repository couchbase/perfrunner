import glob
from collections import namedtuple
from unittest import TestCase

from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.workloads.tcmalloc import KeyValueIterator, LargeIterator
from spring import docgen
from spring.wgen import Worker


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
                          override='cluster.mem_quota.5555')
        self.assertEqual(test_config.cluster.mem_quota, 5555)


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


SpringSettings = namedtuple('SprintSettings', ('items', 'workers'))


class SpringTest(TestCase):

    def test_spring_imports(self):
        self.assertEqual(docgen.Document.SIZE_VARIATION, 0.25)
        self.assertEqual(Worker.BATCH_SIZE, 100)

    def test_seq_key_generator(self):
        settings = SpringSettings(items=10 ** 5, workers=25)

        keys = []
        for worker in range(settings.workers):
            generator = docgen.SequentialKey(worker, settings, prefix='test')
            keys += [key for key in generator]

        expected_keys = ['test-%012d' % i for i in range(1, settings.items + 1)]

        self.assertEqual(sorted(keys), expected_keys)

    def test_unordered_key_generator(self):
        settings = SpringSettings(items=10 ** 5, workers=25)

        keys = []
        for worker in range(settings.workers):
            generator = docgen.UnorderedKey(worker, settings, prefix='test')
            keys += [key for key in generator]

        expected_keys = ['test-%012d' % i for i in range(1, settings.items + 1)]

        self.assertEqual(sorted(keys), expected_keys)

    def test_zipf_generator(self):
        num_items = 10 ** 4
        generator = docgen.ZipfKey(prefix='')

        for i in range(10 ** 5):
            key = generator.next(curr_deletes=0, curr_items=num_items)
            key = int(key)
            self.assertLessEqual(key, num_items)
            self.assertGreaterEqual(key, 1)

    def doc_generators(self, size: int):
        for dg in (
            docgen.ReverseLookupDocument(avg_size=size, prefix='n1ql'),
            docgen.ReverseRangeLookupDocument(avg_size=size, prefix='n1ql',
                                              range_distance=100),
            docgen.ExtReverseLookupDocument(avg_size=size, prefix='n1ql',
                                            num_docs=10 ** 6),
            docgen.ArrayIndexingDocument(avg_size=size, prefix='n1ql',
                                         array_size=10, num_docs=10 ** 6),
            docgen.ProfileDocument(avg_size=size, prefix='n1ql'),
        ):
            yield dg

    def test_doc_size(self):
        size = 1024
        key_gen = docgen.NewOrderedKey(prefix='n1ql', expiration=0)

        for dg in self.doc_generators(size=size):
            for i in range(10 ** 4):
                key, _ = key_gen.next(i)
                doc = dg.next(key=key)
                actual_size = len(str(doc))
                self.assertAlmostEqual(actual_size, size,
                                       delta=size * 0.05,  # 5% variation
                                       msg=dg.__class__.__name__)

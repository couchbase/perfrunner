import glob
from unittest import TestCase

from cbagent.settings import Settings
from perfrunner.helpers.misc import server_group
from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.workloads.tcmalloc import KeyValueIterator, LargeIterator
from spring.docgen import Document
from spring.wgen import Worker


class RebalanceTests(TestCase):

    def test_server_group_6_to_8(self):
        servers = range(8)
        initial_nodes = 6
        nodes_after = 8
        group_number = 3

        groups = []
        for i, host in enumerate(servers[initial_nodes:nodes_after],
                                 start=initial_nodes):
            g = server_group(servers[:nodes_after], group_number, i)
            groups.append(g)
        self.assertEqual(groups, ['Group 3', 'Group 3'])

    def test_initial_4_out_of_8(self):
        servers = range(8)
        initial_nodes = 4
        group_number = 2

        groups = []
        for i, host in enumerate(servers[1:initial_nodes], start=1):
            g = server_group(servers[:initial_nodes], group_number, i)
            groups.append(g)
        self.assertEqual(groups, ['Group 1', 'Group 2', 'Group 2'])

    def test_server_group_3_to_4(self):
        servers = range(8)
        initial_nodes = 3
        nodes_after = 4
        group_number = 2

        groups = []
        for i, host in enumerate(servers[initial_nodes:nodes_after],
                                 start=initial_nodes):
            g = server_group(servers[:nodes_after], group_number, i)
            groups.append(g)
        self.assertEqual(groups, ['Group 2'])


class SettingsTest(TestCase):

    def test_stale_update_after(self):
        test_config = TestConfig()
        test_config.parse('tests/query_lat_20M.test', [])
        views_params = test_config.index_settings.params
        self.assertEqual(views_params, {})

    def test_stale_false(self):
        test_config = TestConfig()
        test_config.parse('tests/query_lat_20M_state_false.test', [])
        views_params = test_config.index_settings.params
        self.assertEqual(views_params, {'stale': 'false'})

    def test_cluster_specs(self):
        for file_name in glob.glob("clusters/*.spec"):
            cluster_spec = ClusterSpec()
            cluster_spec.parse(file_name, override=())
            self.assertIn(cluster_spec.parameters['Platform'],
                          ('HW', 'Virtual', 'Localhost'))


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


class CBAgentTest(TestCase):

    def test_settings(self):
        self.assertEqual(Settings.DEFAULT['interval'], 10)


class SpringTest(TestCase):

    def spring_imports(self):
        self.assertEqual(Document.SIZE_VARIATION, 0.25)
        self.assertEqual(Worker.BATCH_SIZE, 100)

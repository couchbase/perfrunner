from unittest import TestCase

from mock import patch

from perfrunner.helpers.misc import target_hash, server_group
from perfrunner.settings import TestConfig
from perfrunner.utils.install import CouchbaseInstaller, Build
from perfrunner.workloads.tcmalloc import (KeyValueIterator,
                                           KeyLargeValueIterator,
                                           LargeIterator)


class InstallTest(TestCase):

    @patch('perfrunner.utils.install.CouchbaseInstaller.__init__')
    def test_normal_pacakge(self, installer_mock):
        installer_mock.return_value = None
        installer = CouchbaseInstaller()
        installer.build = Build('x86_64', 'rpm', '2.0.0-1976', '1.0.0', None)

        filenames = tuple(installer.get_expected_filenames())
        expected = (
            'couchbase-server-enterprise_centos6_x86_64_2.0.0-1976-rel.rpm',
            'couchbase-server-enterprise_x86_64_2.0.0-1976-rel.rpm',
            'couchbase-server-enterprise_2.0.0-1976-rel_x86_64.rpm',
        )
        self.assertEqual(filenames, expected)

    @patch('perfrunner.utils.install.CouchbaseInstaller.__init__')
    def test_toy_pacakge(self, installer_mock):
        installer_mock.return_value = None
        installer = CouchbaseInstaller()
        installer.build = Build('x86_64', 'rpm', '2.0.0-1976', '1.0.0', 'mytoy')

        filenames = tuple(installer.get_expected_filenames())
        expected = (
            'couchbase-server-community_toy-mytoy-x86_64_2.0.0-1976-toy.rpm',
            'couchbase-server-community_toy-mytoy-2.0.0-1976-toy_x86_64.rpm',
            'couchbase-server-community_cent58-2.5.2-toy-mytoy-x86_64_2.0.0-1976-toy.rpm',
            'couchbase-server-community_cent58-3.0.0-toy-mytoy-x86_64_2.0.0-1976-toy.rpm',
            'couchbase-server-community_cent58-master-toy-mytoy-x86_64_2.0.0-1976-toy.rpm',
            'couchbase-server-community_cent54-master-toy-mytoy-x86_64_2.0.0-1976-toy.rpm',
        )
        self.assertEqual(filenames, expected)

    @patch('perfrunner.utils.install.CouchbaseInstaller.__init__')
    def test_openssl_pacakge(self, installer_mock):
        installer_mock.return_value = None
        installer = CouchbaseInstaller()
        installer.build = Build('x86_64', 'rpm', '2.2.0-817', '0.9.8e', None)

        filenames = tuple(installer.get_expected_filenames())
        expected = (
            'couchbase-server-enterprise_x86_64_2.2.0-817-rel.rpm',
            'couchbase-server-enterprise_2.2.0-817-rel_x86_64_openssl098.rpm',
        )
        self.assertEqual(filenames, expected)

    def test_target_hash(self):
        self.assertEqual(target_hash('127.0.0.1'), '3cf55f')


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
        test_config.parse(fname='tests/query_lat_20M.test')
        views_params = test_config.index_settings.params
        self.assertEqual(views_params, {})

    def test_stale_false(self):
        test_config = TestConfig()
        test_config.parse(fname='tests/query_lat_20M_state_false.test')
        views_params = test_config.index_settings.params
        self.assertEqual(views_params, {'stale': 'false'})


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

    def test_large_value_size(self):
        for _ in range(100):
            iterator = KeyLargeValueIterator(10000)
            batch = iterator.next()
            values = [len(str(v)) for k, v in batch]
            mean = sum(values) / len(values)
            self.assertAlmostEqual(mean, 256000, delta=40960)

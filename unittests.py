from unittest import TestCase

from mock import patch

from perfrunner.utils.install import CouchbaseInstaller, Build


class InstallTest(TestCase):

    @patch('perfrunner.utils.install.CouchbaseInstaller.__init__')
    def test_normal_pacakge(self, installer_mock):
        installer_mock.return_value = None
        installer = CouchbaseInstaller()
        installer.build = Build('x86_64', 'rpm', '2.0.0-1976', '1.0.0', None)

        filenames = tuple(installer.get_expected_filenames())
        expected = (
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

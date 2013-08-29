from unittest import TestCase

from mock import patch

from perfrunner.utils.install import Installer, Build


class InstallTest(TestCase):

    @patch('perfrunner.utils.install.Installer.__init__')
    def test_normal_pacakge(self, installer_mock):
        installer_mock.return_value = None
        installer = Installer()
        installer.build = Build('x86_64', 'rpm', '1.0.0', '2.0.0-1976', None)

        filenames = tuple(installer.get_expected_filenames())
        expected = (
            'couchbase-server-enterprise_x86_64_2.0.0-1976-rel.rpm',
            'couchbase-server-enterprise_2.0.0-1976-rel_x86_64.rpm',
        )
        self.assertEqual(filenames, expected)

    @patch('perfrunner.utils.install.Installer.__init__')
    def test_toy_pacakge(self, installer_mock):
        installer_mock.return_value = None
        installer = Installer()
        installer.build = Build('x86_64', 'rpm', '1.0.0', '2.0.0-1976', 'mytoy')

        filenames = tuple(installer.get_expected_filenames())
        expected = (
            'couchbase-server-community_toy-mytoy-x86_64_2.0.0-1976-toy.rpm',
            'couchbase-server-community_toy-mytoy-2.0.0-1976-toy_x86_64.rpm',
        )
        self.assertEqual(filenames, expected)

    @patch('perfrunner.utils.install.Installer.__init__')
    def test_openssl_pacakge(self, installer_mock):
        installer_mock.return_value = None
        installer = Installer()
        installer.build = Build('x86_64', 'rpm', '0.9.8e', '2.2.0-817', None)

        filenames = tuple(installer.get_expected_filenames())
        expected = (
            'couchbase-server-enterprise_x86_64_2.2.0-817-rel.rpm',
            'couchbase-server-enterprise_2.2.0-817-rel_x86_64_openssl098.rpm',
        )
        self.assertEqual(filenames, expected)

from unittest import TestCase, mock

from perfrunner.remote import api, linux
from perfrunner.settings import CBProduct


class SecretsTest(TestCase):
    def test_secrets_never_appear_in_a_logged_command(self):
        class Output(str):
            return_code = 0

        commands = []

        def fake_run(command, **kwargs):
            commands.append((command, dict(api._env_vars.get())))
            return Output("/opt/couchbase/var/x.sock" if command.startswith("ls") else "HTTP 200")

        remote = mock.Mock(UDF_LIBRARY_SOCKETS=linux.RemoteLinux.UDF_LIBRARY_SOCKETS)
        remote.get_product.return_value = CBProduct.OPERATIONAL_INSIGHTS
        with mock.patch.object(linux, "run", fake_run), mock.patch.object(linux, "put"):
            linux.RemoteLinux.docker_registry_login.__wrapped__(remote, "ghcr.io", "u", "tok3n")
            linux.RemoteLinux.upload_analytics_python_library(
                remote, "n1", "pylib.pyz", "Default", "pylib", ("Administrator", "pa55word")
            )

        for command, _ in commands:
            self.assertNotIn("tok3n", command)
            self.assertNotIn("pa55word", command)
        self.assertIn("tok3n", commands[0][1].values())
        self.assertIn("pa55word", commands[-2][1].values())

"""Characterisation of the remote layer's host targeting, command strings and result shapes.

Delete once the remote-layer migration it protects is complete.
"""

import os
import tempfile
from pathlib import Path
from unittest import TestCase

from perfrunner.helpers.misc import SSLCertificate
from perfrunner.remote import api, executor
from perfrunner.settings import ClusterSpec


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

    def test_allow_non_local_ca_upload_runs_on_every_server(self):
        # Spares get their own CA upload, and a spare is a standalone cluster, so it has to
        # allow the upload itself rather than inheriting it from the master.
        remote = self._remote()
        remote.allow_non_local_ca_upload()
        for server in ("10.0.0.1", "10.0.0.2", "10.0.0.3"):
            commands = [command for command, _ in self.created[server].commands]
            self.assertTrue(
                any("allowNonLocalCACertUpload" in command for command in commands), server
            )

    def test_setup_x509_uploads_only_the_node_certificate_and_key(self):
        # The inbox also holds the CA and the client certificate, private keys included. Only
        # `reloadCertificate`'s two files belong on a server, so uploading the directory would
        # hand every node key material it never reads.
        cwd = os.getcwd()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.addCleanup(os.chdir, cwd)
        os.chdir(tmp.name)
        os.makedirs(SSLCertificate.INBOX)
        for name in ("ca.pem", "ca.key", "chain.pem", "pkey.key", "client.pem", "client.key"):
            Path(SSLCertificate.INBOX, name).write_text(name)

        remote = self._remote()
        remote.setup_x509()

        for server in ("10.0.0.1", "10.0.0.2", "10.0.0.3"):
            session = self.created[server]
            uploaded = sorted(os.path.basename(local) for local, _ in session.uploads)
            self.assertEqual(uploaded, ["chain.pem", "pkey.key"], server)
            remote_dir = "/opt/couchbase/var/lib/couchbase/inbox"
            self.assertEqual(
                sorted(remote for _, remote in session.uploads),
                [f"{remote_dir}/chain.pem", f"{remote_dir}/pkey.key"],
            )
            commands = [command for command, _ in session.commands]
            self.assertTrue(any(f"mkdir -p {remote_dir}" in c for c in commands), commands)
            # One chmod for both files, and it grants read: +x would leave an unreadable key
            # unreadable.
            chmods = [c for c in commands if "chmod" in c]
            self.assertEqual(len(chmods), 1, chmods)
            self.assertIn(f"chmod a+r {remote_dir}/chain.pem {remote_dir}/pkey.key", chmods[0])

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

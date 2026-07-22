"""Files the remote layer copies to cluster nodes and to remote workers for x.509 setup."""

import os
import tempfile
from pathlib import Path
from unittest import TestCase

from perfrunner.helpers.x509 import SSLCertificate
from perfrunner.remote import api, executor
from perfrunner.settings import ClusterSpec


class X509FilesTest(TestCase):
    """Run the real `RemoteLinux` methods against FakeSession, so no SSH connection is made."""

    SPEC = (
        "[clusters]\n"
        "test =\n"
        "    10.0.0.1:kv\n"
        "    10.0.0.2:kv\n"
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

    REMOTE_INBOX = "/opt/couchbase/var/lib/couchbase/inbox"

    def setUp(self):
        self.created = {}

        def factory(host, config, gateway=None):
            session = executor.FakeSession(host=host)
            self.created[host] = session
            return session

        self.addCleanup(setattr, api, "pool", api.pool)
        api.pool = executor.ConnectionPool(factory)

        spec_file = tempfile.NamedTemporaryFile(mode="w", suffix=".spec", delete=False)
        spec_file.write(self.SPEC)
        spec_file.close()
        self.addCleanup(os.unlink, spec_file.name)
        cluster_spec = ClusterSpec()
        cluster_spec.parse(spec_file.name, override=None)

        from perfrunner.remote.linux import RemoteLinux

        self.remote = RemoteLinux(cluster_spec)

        # The local inbox is relative to the working directory.
        cwd = os.getcwd()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.addCleanup(os.chdir, cwd)
        os.chdir(tmp.name)
        os.makedirs(SSLCertificate.INBOX)
        for name in ("ca.pem", "ca.key", "ca.crl", "chain.pem", "pkey.key", "client.pem",
                     "client.key"):
            Path(SSLCertificate.INBOX, name).write_text(name)

    def test_inbox_files_land_under_the_nodes_inbox_on_every_server(self):
        self.remote.put_inbox_files(
            {
                SSLCertificate.SERVER_CERT_PATH: "chain.pem",
                SSLCertificate.CRL_PATH: "crls/ca.crl",
            }
        )

        for server in ("10.0.0.1", "10.0.0.2"):
            session = self.created[server]
            self.assertEqual(
                sorted(remote for _, remote in session.uploads),
                [f"{self.REMOTE_INBOX}/chain.pem", f"{self.REMOTE_INBOX}/crls/ca.crl"],
                server,
            )
            commands = [command for command, _ in session.commands]
            self.assertTrue(
                any(f"mkdir -p {self.REMOTE_INBOX} {self.REMOTE_INBOX}/crls" in c
                    for c in commands),
                commands,
            )
            # One chmod for every file, and it grants read: +x would leave an unreadable key
            # unreadable.
            chmods = [c for c in commands if "chmod" in c]
            self.assertEqual(len(chmods), 1, chmods)
            self.assertIn(
                f"chmod a+r {self.REMOTE_INBOX}/chain.pem {self.REMOTE_INBOX}/crls/ca.crl",
                chmods[0],
            )

    def test_client_files_keep_their_relative_paths_on_the_workers(self):
        # A workload reads `certificates/inbox/...` relative to its checkout, wherever it runs.
        self.remote.cloud_put_x509_client_files("/data/worker")

        session = self.created["10.0.1.1"]
        root = "/data/worker/perfrunner/certificates/inbox"
        self.assertEqual(
            sorted(remote for _, remote in session.uploads),
            [f"{root}/ca.pem", f"{root}/client.key", f"{root}/client.pem"],
        )
        commands = [command for command, _ in session.commands]
        self.assertTrue(any(f"chmod 600 {root}/client.key" in c for c in commands), commands)

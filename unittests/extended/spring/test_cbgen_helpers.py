"""The connection string and options spring builds for a Couchbase SDK `Cluster`."""

import subprocess
import sys
import textwrap
from datetime import timedelta
from pathlib import Path
from unittest import TestCase

from perfrunner.helpers.x509 import SSLCertificate
from spring.cbgen_helpers import cluster_args

CREDENTIALS = {"host": "1.2.3.4", "username": "user", "password": "pass"}


def authenticator_of(options) -> dict:
    return options["authenticator"].as_dict()


class ClusterArgsTest(TestCase):

    def test_ssl_mode_auth_presents_this_runs_client_certificate_and_trusts_its_ca(self):
        connstr, options = cluster_args(120, ssl_mode="auth", **CREDENTIALS)

        self.assertTrue(connstr.startswith("couchbases://1.2.3.4"))
        self.assertEqual(
            authenticator_of(options),
            {
                "cert_path": SSLCertificate.CLIENT_CERT_PATH,
                "key_path": SSLCertificate.CLIENT_KEY_PATH,
                "trust_store_path": SSLCertificate.CA_CERT_PATH,
            },
        )

    def test_tls_modes_authenticate_with_a_password_and_trust_the_cluster_ca_bundle(self):
        for ssl_mode in ("data", "n2n"):
            connstr, options = cluster_args(120, ssl_mode=ssl_mode, **CREDENTIALS)

            self.assertTrue(connstr.startswith("couchbases://"), ssl_mode)
            self.assertIn("sasl_mech_force=PLAIN", connstr, ssl_mode)
            authenticator = authenticator_of(options)
            self.assertEqual(authenticator["username"], "user", ssl_mode)
            self.assertEqual(authenticator["trust_store_path"], "root.pem", ssl_mode)

    def test_capella_skips_certificate_verification(self):
        connstr, options = cluster_args(120, ssl_mode="capella", **CREDENTIALS)

        self.assertIn("ssl=no_verify", connstr)
        self.assertNotIn("trust_store_path", authenticator_of(options))

    def test_without_tls_the_connection_is_plain(self):
        connstr, options = cluster_args(120, **CREDENTIALS)

        self.assertEqual(connstr, "couchbase://1.2.3.4?")
        self.assertEqual(options["authenticator"].as_dict()["username"], "user")

    def test_timeouts_are_in_seconds(self):
        _, options = cluster_args(120, 600, **CREDENTIALS)

        self.assertEqual(options["kv_timeout"], timedelta(seconds=120))
        self.assertEqual(options["query_timeout"], timedelta(seconds=600))

    def test_query_timeout_and_tracing_are_left_out_unless_asked_for(self):
        _, options = cluster_args(120, **CREDENTIALS)

        self.assertNotIn("query_timeout", options)
        self.assertNotIn("enable_tracing", options)

    def test_tracing_is_passed_through_as_asked(self):
        for enabled in (True, False):
            with self.subTest(enable_tracing=enabled):
                _, options = cluster_args(120, enable_tracing=enabled, **CREDENTIALS)
                self.assertIs(options["enable_tracing"], enabled)

    def test_connection_string_parameters_reach_the_connection_string(self):
        connstr, _ = cluster_args(120, connstr_params={"kv_pool_size": "4"}, **CREDENTIALS)

        self.assertIn("kv_pool_size=4", connstr)


def import_with_fake_sdk(version: str, modules: str) -> subprocess.CompletedProcess:
    """Import `spring.cbgen_helpers` in a fresh interpreter whose SDK is `version`.

    `modules` is Python source that registers stand-ins for the SDK modules of that version.
    """
    header = textwrap.dedent(
        f"""
        import importlib.metadata as metadata
        import sys
        import types

        real_version = metadata.version
        metadata.version = lambda name: "{version}" if name == "couchbase" else real_version(name)

        def module(name, **attributes):
            sys.modules[name] = types.ModuleType(name)
            vars(sys.modules[name]).update(attributes)

        """
    )
    code = header + textwrap.dedent(modules) + "\nimport spring.cbgen_helpers\n"
    repo_root = Path(__file__).resolve().parents[3]
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=repo_root,
        env={"PYTHONPATH": str(repo_root), "PATH": ""},
        capture_output=True,
        text=True,
    )


class OldSdkImportTest(TestCase):

    def test_the_module_imports_when_the_sdk_is_version_2(self):
        # `spring/cbgen.py` imports this module on SDK 2 runs, where the SDK 3 and 4 names that
        # `cluster_args` is typed with do not exist.
        result = import_with_fake_sdk(
            "2.5.0",
            """
            error = type("E", (Exception,), {})
            module("couchbase.exceptions", CouchbaseError=error, TemporaryFailError=error)
            """,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_the_module_imports_when_the_sdk_is_version_3(self):
        # SDK 3 names its certificate authenticator `CertAuthenticator`.
        result = import_with_fake_sdk(
            "3.2.7",
            """
            error = type("E", (Exception,), {})
            module("couchbase.exceptions", CouchbaseException=error, TemporaryFailException=error)
            module("couchbase.auth", CertAuthenticator=type("CertAuthenticator", (), {}))
            module("couchbase.cluster", ClusterOptions=dict, ClusterTimeoutOptions=dict)
            module("couchbase_core")
            module("couchbase_core.cluster", PasswordAuthenticator=object)
            """,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

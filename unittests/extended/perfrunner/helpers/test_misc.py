"""SSLCertificate generation; shells out to openssl and inspects file modes on disk."""

import os
import stat
import tempfile
from pathlib import Path
from unittest import TestCase

from perfrunner.helpers.misc import SSLCertificate


class SSLCertificateTest(TestCase):
    """Coverage for X.509 node and client certificate generation."""

    def setUp(self):
        # `output_dir` keeps generation out of the repo inbox, so these tests need neither a
        # chdir nor a writable `certificates/inbox`, and stay safe under pytest-xdist.
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.inbox = tmp.name

    def _cert(self, hosts: list = None) -> SSLCertificate:
        return SSLCertificate(hosts, output_dir=self.inbox)

    def _path(self, filename: str) -> str:
        return os.path.join(self.inbox, filename)

    def _load_cert(self, path: str):
        from cryptography.x509 import load_pem_x509_certificate

        with open(path, "rb") as fh:
            return load_pem_x509_certificate(fh.read())

    def _common_name(self, cert) -> str:
        from cryptography.x509.oid import NameOID

        return cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value

    def _read(self, filename: str) -> bytes:
        with open(os.path.join(self.inbox, filename), "rb") as fh:
            return fh.read()

    def test_certificate_file_names(self):
        # Couchbase Server requires the node certificate and key to be named chain.pem and
        # pkey.key in its inbox, so these constants are not free to change.
        self.assertEqual(SSLCertificate.SERVER_CERT_FILENAME, "chain.pem")
        self.assertEqual(SSLCertificate.SERVER_KEY_FILENAME, "pkey.key")
        self.assertEqual(SSLCertificate.CA_CERT_FILENAME, "ca.pem")
        self.assertEqual(SSLCertificate.CLIENT_CERT_FILENAME, "client.pem")
        self.assertEqual(SSLCertificate.CLIENT_KEY_FILENAME, "client.key")

        # Every path constant is that file name inside the inbox.
        for filename, path in (
            (SSLCertificate.CA_CERT_FILENAME, SSLCertificate.CA_CERT_PATH),
            (SSLCertificate.CA_KEY_FILENAME, SSLCertificate.CA_KEY_PATH),
            (SSLCertificate.CRL_FILENAME, SSLCertificate.CRL_PATH),
            (SSLCertificate.SERVER_CERT_FILENAME, SSLCertificate.SERVER_CERT_PATH),
            (SSLCertificate.SERVER_KEY_FILENAME, SSLCertificate.SERVER_KEY_PATH),
            (SSLCertificate.CLIENT_CERT_FILENAME, SSLCertificate.CLIENT_CERT_PATH),
            (SSLCertificate.CLIENT_KEY_FILENAME, SSLCertificate.CLIENT_KEY_PATH),
            (SSLCertificate.CLIENT_BUNDLE_FILENAME, SSLCertificate.CLIENT_BUNDLE_PATH),
        ):
            self.assertEqual(path, os.path.join(SSLCertificate.INBOX, filename))

    def test_generate_server_cert(self):
        from cryptography.x509 import (
            ExtendedKeyUsage,
            SubjectAlternativeName,
        )
        from cryptography.x509.oid import ExtendedKeyUsageOID

        self._cert(["127.0.0.1", "node1.perf.couchbase.com"]).generate_server_cert()

        for name in ("ca.pem", "ca.key", "chain.pem", "pkey.key"):
            self.assertTrue(os.path.exists(self._path(name)), name)

        cert = self._load_cert(self._path(SSLCertificate.SERVER_CERT_FILENAME))
        self.assertEqual(self._common_name(cert), "Couchbase Server")

        eku = cert.extensions.get_extension_for_class(ExtendedKeyUsage).value
        self.assertIn(ExtendedKeyUsageOID.SERVER_AUTH, eku)

        san = cert.extensions.get_extension_for_class(SubjectAlternativeName).value
        self.assertEqual(
            {str(name.value) for name in san},
            {"127.0.0.1", "node1.perf.couchbase.com"},
        )

    def test_generate_client_cert(self):
        from cryptography.x509 import ExtendedKeyUsage, SubjectAlternativeName
        from cryptography.x509.extensions import ExtensionNotFound
        from cryptography.x509.oid import ExtendedKeyUsageOID

        cert_path, key_path = self._cert().generate_client_cert("Administrator")

        self.assertEqual(key_path, self._path(SSLCertificate.CLIENT_KEY_FILENAME))
        self.assertEqual(cert_path, self._path(SSLCertificate.CLIENT_CERT_FILENAME))

        cert = self._load_cert(cert_path)
        # The CN is the Couchbase RBAC username the client authenticates as.
        self.assertEqual(self._common_name(cert), "Administrator")

        eku = cert.extensions.get_extension_for_class(ExtendedKeyUsage).value
        self.assertEqual(list(eku), [ExtendedKeyUsageOID.CLIENT_AUTH])

        # A client cert has no SAN: it identifies a user, not a host.
        with self.assertRaises(ExtensionNotFound):
            cert.extensions.get_extension_for_class(SubjectAlternativeName)

    def test_client_and_server_certs_share_a_ca(self):
        self._cert(["127.0.0.1"]).generate_server_cert()
        ca_key = self._read("ca.key")

        self._cert().generate_client_cert("Administrator")

        # The existing CA must be reused, not regenerated: a new CA would
        # invalidate the node certificates already deployed to the cluster.
        self.assertEqual(self._read("ca.key"), ca_key)

        ca = self._load_cert(self._path(SSLCertificate.CA_CERT_FILENAME))
        for cert_path in (
            self._path(SSLCertificate.SERVER_CERT_FILENAME),
            self._path(SSLCertificate.CLIENT_CERT_FILENAME),
        ):
            cert = self._load_cert(cert_path)
            self.assertEqual(cert.issuer, ca.subject, cert_path)

    def test_private_keys_are_not_world_readable(self):
        ssl_cert = self._cert(["127.0.0.1"])
        ssl_cert.generate_server_cert()
        ssl_cert.generate_client_cert("Administrator")

        for key_name in ("ca.key", "pkey.key", "client.key", "client.p12"):
            mode = os.stat(self._path(key_name)).st_mode
            self.assertEqual(stat.S_IMODE(mode), 0o600, key_name)

    def test_node_cert_allows_client_auth_for_node_to_node(self):
        from cryptography.x509 import ExtendedKeyUsage
        from cryptography.x509.oid import ExtendedKeyUsageOID

        self._cert(["127.0.0.1"]).generate_server_cert()

        cert = self._load_cert(self._path(SSLCertificate.SERVER_CERT_FILENAME))
        eku = cert.extensions.get_extension_for_class(ExtendedKeyUsage).value
        # Under n2n encryption a node is also a TLS client of other nodes, so serverAuth
        # alone is not enough.
        self.assertEqual(
            sorted(oid.dotted_string for oid in eku),
            sorted([
                ExtendedKeyUsageOID.SERVER_AUTH.dotted_string,
                ExtendedKeyUsageOID.CLIENT_AUTH.dotted_string,
            ]),
        )

    def test_client_bundle_is_readable_by_keytool_password(self):
        from cryptography.hazmat.primitives.serialization import pkcs12

        self._cert().generate_client_cert("Administrator", storepass="s3cret")

        with open(self._path(SSLCertificate.CLIENT_BUNDLE_FILENAME), "rb") as fh:
            key, cert, cas = pkcs12.load_key_and_certificates(fh.read(), b"s3cret")

        # The bundle has to carry the same identity as the PEM pair, plus the CA, or the
        # keystore built from it cannot complete the handshake in either direction.
        self.assertEqual(self._common_name(cert), "Administrator")
        self.assertEqual(
            cert.serial_number,
            self._load_cert(self._path(SSLCertificate.CLIENT_CERT_FILENAME)).serial_number,
        )
        self.assertEqual([self._common_name(ca) for ca in cas], ["Couchbase Root CA"])
        self.assertIsNotNone(key)

    def test_ca_can_sign_a_crl(self):
        from cryptography.x509 import KeyUsage, load_pem_x509_crl

        crl_path = self._cert().generate_crl(revoked_serial_numbers=[1234, 5678])

        self.assertEqual(crl_path, self._path(SSLCertificate.CRL_FILENAME))

        ca = self._load_cert(self._path(SSLCertificate.CA_CERT_FILENAME))
        # RFC 5280 4.2.1.3: a CRL issuer's KeyUsage has to assert cRLSign.
        key_usage = ca.extensions.get_extension_for_class(KeyUsage).value
        self.assertTrue(key_usage.crl_sign)

        with open(crl_path, "rb") as fh:
            crl = load_pem_x509_crl(fh.read())
        self.assertEqual(crl.issuer, ca.subject)
        self.assertTrue(crl.is_signature_valid(ca.public_key()))
        self.assertEqual(sorted(revoked.serial_number for revoked in crl), [1234, 5678])

    def test_empty_crl_is_valid(self):
        from cryptography.x509 import load_pem_x509_crl

        crl_path = self._cert().generate_crl()
        with open(crl_path, "rb") as fh:
            crl = load_pem_x509_crl(fh.read())
        self.assertEqual(len(list(crl)), 0)

    def test_expired_ca_is_replaced_instead_of_reused(self):
        from datetime import timedelta

        stale = self._cert(["127.0.0.1"])
        stale.not_valid_before = stale.not_valid_before - timedelta(days=400)
        stale.not_valid_after = stale.not_valid_before + timedelta(days=60)
        stale.generate_ca()
        stale_ca_key = self._read("ca.key")

        # A checkout that is reused rather than made fresh can hold a CA from an earlier
        # run. An expired one must not be reused: everything it signs fails validation.
        self._cert(["127.0.0.1"]).generate_server_cert()

        self.assertNotEqual(self._read("ca.key"), stale_ca_key)
        ca = self._load_cert(self._path(SSLCertificate.CA_CERT_FILENAME))
        cert = self._load_cert(self._path(SSLCertificate.SERVER_CERT_FILENAME))
        self.assertEqual(cert.issuer, ca.subject)
        self.assertGreaterEqual(ca.not_valid_after_utc, cert.not_valid_after_utc)

    def test_ca_close_to_expiry_is_replaced(self):
        from datetime import timedelta

        # Still valid today, but with less than MIN_CA_VALIDITY left, so a test starting now
        # could outlive it.
        short = self._cert(["127.0.0.1"])
        short.not_valid_after = short.not_valid_before + timedelta(days=5)
        short.generate_ca()
        short_ca_key = self._read("ca.key")

        self._cert(["127.0.0.1"]).generate_server_cert()

        self.assertNotEqual(self._read("ca.key"), short_ca_key)

    def test_output_dir_is_self_contained_and_leaves_the_inbox_alone(self):
        # `x509_cert --dest` used to generate into the repo inbox and copy out of it, which
        # omitted a reused CA from the destination and deleted the inbox's staged certificates.
        # A separate output directory gets its own complete set and touches nothing else.
        inbox, dest = self._path("inbox"), self._path("dest")
        SSLCertificate(["127.0.0.1"], output_dir=inbox).generate_server_cert()
        staged = {name: Path(inbox, name).read_bytes() for name in os.listdir(inbox)}

        SSLCertificate(["10.0.0.1"], output_dir=dest).generate_server_cert()

        # Complete, so the node certificate in `dest` can be verified against a CA beside it.
        self.assertEqual(sorted(os.listdir(dest)), ["ca.key", "ca.pem", "chain.pem", "pkey.key"])
        self.assertEqual({name: Path(inbox, name).read_bytes() for name in staged}, staged)
        # A CA in another directory is not a CA to reuse.
        self.assertNotEqual(Path(dest, "ca.pem").read_bytes(), staged["ca.pem"])

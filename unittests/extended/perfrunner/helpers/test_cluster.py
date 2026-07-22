"""REST scope of ClusterManager.set_x509_certificates()."""

import os
import tempfile
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from perfrunner.helpers.x509 import SSLCertificate
from perfrunner.settings import ClientCertAuthState, ClusterSpec, CRLMode


class X509ClusterSetupTest(TestCase):
    """Pin the scope of each REST step in `set_x509_certificates()`.

    Uploading the CA is cluster-wide, reloading is node-local, and enabling client certificate
    auth is cluster-wide but must not reach a standalone spare. A spec with spares and more than
    one cluster is the only shape that tells the three apart.
    """

    SPEC = (
        "[clusters]\n"
        "c1 =\n"
        "    10.0.0.1:kv\n"
        "    10.0.0.2:kv\n"
        "    10.0.0.3:kv\n"
        "c2 =\n"
        "    10.0.1.1:kv\n"
        "    10.0.1.2:kv\n"
        "\n"
        "[clients]\n"
        "hosts =\n"
        "    10.0.2.1\n"
        "\n"
        "[storage]\n"
        "data = /data\n"
        "\n"
        "[metadata]\n"
        "cluster = test\n"
    )

    def setUp(self):
        from perfrunner.helpers import cluster as cluster_module

        spec_file = tempfile.NamedTemporaryFile(mode="w", suffix=".spec", delete=False)
        spec_file.write(self.SPEC)
        spec_file.close()
        self.addCleanup(os.unlink, spec_file.name)
        self.cluster_spec = ClusterSpec()
        self.cluster_spec.parse(spec_file.name, override=None)
        # `rest_credentials` is a cached_property backed by `.secrets.json`, which a unit test
        # has no business reading; seeding the cache stands in for it.
        self.cluster_spec.__dict__["rest_credentials"] = ("rest-user", "rest-password")

        self.calls = []
        self.generated = []
        self.client_certs = []
        self.crls = []
        self.enable_states = []
        self.inbox_uploads = []
        self.crl_posts = []
        self.validations = []

        # `local` writes real certificates to disk; stand in for it so the test only observes
        # which hosts end up in the SAN list, and which CN the client certificate is issued to.
        self.cluster_module = cluster_module
        self.addCleanup(setattr, cluster_module, "local", cluster_module.local)
        self.addCleanup(setattr, cluster_module, "RestHelper", cluster_module.RestHelper)
        cluster_module.local = SimpleNamespace(
            generate_server_x509_cert=self.generated.append,
            generate_client_x509_cert=lambda cn, storepass: self.client_certs.append(
                (cn, storepass)
            ),
            generate_x509_crl=lambda: self.crls.append(True),
        )

    def _manager(
        self,
        initial_nodes,
        buckets=("bucket-1", "bucket-2"),
        client_cert_auth_state="enable",
        ssl_mode="auth",
        load_ssl_mode="none",
        crl_client_auth_mode=None,
        crl_node_to_node_mode=None,
        crl_check_intermediate_certs=None,
        build_tuple=(8, 5, 0, 1000),
    ):
        def record(name):
            return lambda node, *args, **kwargs: self.calls.append((name, node))

        def record_enable(node, state="enable"):
            self.calls.append(("enable", node))
            self.enable_states.append(state)

        # Constructed without __init__, which would build a RestHelper, a RemoteHelper and a
        # Monitor. None of them affect which nodes each step targets.
        cm = object.__new__(self.cluster_module.DefaultClusterManager)
        cm.cluster_spec = self.cluster_spec
        cm.initial_nodes = initial_nodes
        cm.test_config = SimpleNamespace(
            load_settings=SimpleNamespace(ssl_mode=load_ssl_mode),
            access_settings=SimpleNamespace(ssl_mode=ssl_mode, ssl_keystore_password="storepass"),
            xdcr_settings=SimpleNamespace(cng_haproxy=False),
            cluster=SimpleNamespace(
                client_cert_auth_state=ClientCertAuthState(client_cert_auth_state),
                crl_client_auth_mode=crl_client_auth_mode and CRLMode(crl_client_auth_mode),
                crl_node_to_node_mode=crl_node_to_node_mode and CRLMode(crl_node_to_node_mode),
                crl_check_intermediate_certs=crl_check_intermediate_certs,
            ),
            buckets=buckets,
        )
        cm.build_tuple = build_tuple
        # Certificate setup talks to the cluster over its own plain-HTTP helper rather than
        # `cm.rest`, so the factory hands back the recorder and remembers how it was asked.
        cert_rest = SimpleNamespace(
            upload_cluster_certificate=record("upload"),
            reload_cluster_certificate=record("reload"),
            enable_certificate_auth=record_enable,
            reload_crl=lambda node: self.calls.append(("reload_crl", node)) or {},
            set_crl_settings=lambda node, settings: (
                self.calls.append(("set_crl_settings", node)),
                self.crl_posts.append(settings),
            ),
            validate_crl=lambda node, policy, certs: (
                self.calls.append(("validate_crl", node)),
                self.validations.append((policy, certs)),
            )[1]
            or {},
        )

        def rest_helper(cluster_spec, use_tls=True):
            self.rest_helper_use_tls = use_tls
            return cert_rest

        self.cluster_module.RestHelper = rest_helper
        cm.rest = SimpleNamespace(client_cert=None)
        cm.remote = SimpleNamespace(
            allow_non_local_ca_upload=lambda: None,
            put_inbox_files=self.inbox_uploads.append,
        )
        return cm

    def _hosts(self, kind):
        return [node for name, node in self.calls if name == kind]

    def test_each_step_targets_the_right_nodes(self):
        self._manager([2, 1]).set_x509_certificates()

        # The SAN list covers every host in the spec, spares included.
        self.assertEqual(
            self.generated,
            [["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.1.1", "10.0.1.2"]],
        )
        # Cluster-wide: the master of each cluster, plus each standalone spare.
        self.assertEqual(self._hosts("upload"), ["10.0.0.1", "10.0.0.3", "10.0.1.1", "10.0.1.2"])
        # Node-local: everything, so a swapped-in spare presents a certificate.
        self.assertEqual(
            self._hosts("reload"),
            ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.1.1", "10.0.1.2"],
        )
        # Cluster-wide, so once per cluster on its master. A standalone spare with this enabled
        # would demand a client certificate during its own join handshake.
        self.assertEqual(self._hosts("enable"), ["10.0.0.1", "10.0.1.1"])

    def test_a_cluster_without_spares_reloads_only_its_own_nodes(self):
        self._manager([3, 2]).set_x509_certificates()

        self.assertEqual(self._hosts("upload"), ["10.0.0.1", "10.0.1.1"])
        self.assertEqual(
            self._hosts("reload"),
            ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.1.1", "10.0.1.2"],
        )

    def test_client_certificate_is_issued_to_the_specs_rest_user(self):
        # `enable_certificate_auth` maps the whole CN to an RBAC user. `add_rbac_users` creates
        # the spec's REST user with the admin role, so that name is the identity to issue to --
        # not a bucket-named user, which only exists when the test has that bucket.
        self._manager([2, 1]).set_x509_certificates()

        self.assertEqual(self.client_certs, [("rest-user", "storepass")])

    def test_client_certificate_does_not_depend_on_the_test_having_buckets(self):
        # A gateway-only setup reaches this code with no buckets at all.
        self._manager([2, 1], buckets=()).set_x509_certificates()

        self.assertEqual(self.client_certs, [("rest-user", "storepass")])

    def test_certificate_setup_does_not_use_tls(self):
        # Reloading a node's own TLS listener over HTTPS to that same listener can hang.
        self._manager([2, 1]).set_x509_certificates()

        self.assertFalse(self.rest_helper_use_tls)

    def test_mandatory_client_cert_auth_attaches_the_certificate_to_the_deploy_rest_helper(self):
        pair = ("client.pem", "client.key")
        cm = self._manager([2, 1], client_cert_auth_state="mandatory")
        with patch.object(SSLCertificate, "client_cert_pair", return_value=pair):
            cm.set_x509_certificates()

        self.assertEqual(cm.rest.client_cert, pair)

    def test_no_crl_options_leave_the_crl_alone(self):
        self._manager([2, 1]).set_x509_certificates()

        self.assertEqual(self.crls, [])
        self.assertEqual(self.crl_posts, [])
        self.assertEqual(self._hosts("reload_crl"), [])


    # CRL setup: the CRL is shipped to every node, then configured once per cluster on its master.

    def _crl_manager(self, initial_nodes=(2, 1), **kwargs):
        # Validation reads the generated client certificate from the inbox.
        cwd = os.getcwd()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.addCleanup(os.chdir, cwd)
        os.chdir(tmp.name)
        os.makedirs(SSLCertificate.INBOX)
        with open(SSLCertificate.CLIENT_CERT_PATH, "w") as fh:
            fh.write("client-cert-pem")

        kwargs.setdefault("crl_client_auth_mode", "Require")
        return self._manager(list(initial_nodes), **kwargs)

    def test_crl_is_generated_once_and_shipped_to_every_node(self):
        self._crl_manager().set_x509_certificates()

        self.assertEqual(self.crls, [True])
        self.assertEqual(
            self.inbox_uploads[0][SSLCertificate.CRL_PATH], f"crls/{SSLCertificate.CRL_FILENAME}"
        )

    def test_every_node_rescans_including_spares(self):
        self._crl_manager().set_x509_certificates()

        # A spare has the CRL in its inbox from the same upload, and trusts the CA from its own
        # CA upload, so it needs no CRL REST call beyond the rescan.
        self.assertEqual(
            self._hosts("reload_crl"),
            ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.1.1", "10.0.1.2"],
        )

    def test_settings_are_posted_once_per_cluster_on_its_master(self):
        self._crl_manager(crl_check_intermediate_certs=False).set_x509_certificates()

        self.assertEqual(self._hosts("set_crl_settings"), ["10.0.0.1", "10.0.1.1"])
        # False is a setting in its own right; an unset option isn't sent at all.
        expected = {"policyPerScope": {"clientAuth": "Require"}, "checkIntermediateCerts": False}
        self.assertEqual(self.crl_posts, [expected, expected])

    def test_crl_is_reloaded_after_the_ca_is_trusted(self):
        # A CRL is only accepted once its issuer is a trusted CA, and a node does not rescan on
        # its own after a CA upload.
        self._crl_manager().set_x509_certificates()

        names = [name for name, node in self.calls if node.startswith("10.0.0.")]
        last_upload = max(i for i, name in enumerate(names) if name == "upload")
        self.assertLess(last_upload, names.index("reload_crl"))

    def test_a_disabled_client_policy_is_not_validated(self):
        # The validation endpoint ignores the cluster's policy and rejects `Disabled`.
        self._crl_manager(crl_client_auth_mode="Disabled").set_x509_certificates()

        self.assertEqual(self._hosts("validate_crl"), [])
        self.assertEqual(self.crls, [True])

    def test_an_older_build_fails_instead_of_skipping_the_crl(self):
        with self.assertRaises(SystemExit):
            self._crl_manager(build_tuple=(8, 1, 0, 2498)).set_x509_certificates()

        self.assertEqual(self.crls, [])
        self.assertEqual(self.calls, [])

    def test_crl_options_without_client_cert_auth_fail(self):
        with self.assertRaises(SystemExit):
            self._crl_manager(ssl_mode="data").set_x509_certificates()

        self.assertEqual(self.calls, [])

    def test_only_the_node_certificate_and_key_are_copied_to_the_nodes(self):
        # The local inbox also holds the CA key and the client certificate and key.
        self._manager([2, 1]).set_x509_certificates()

        self.assertEqual(
            self.inbox_uploads,
            [
                {
                    SSLCertificate.SERVER_CERT_PATH: SSLCertificate.SERVER_CERT_FILENAME,
                    SSLCertificate.SERVER_KEY_PATH: SSLCertificate.SERVER_KEY_FILENAME,
                }
            ],
        )

    def test_a_load_phase_with_auth_alone_sets_up_the_certificates(self):
        self._manager([2, 1], ssl_mode="none", load_ssl_mode="auth").set_x509_certificates()

        self.assertEqual(self._hosts("enable"), ["10.0.0.1", "10.0.1.1"])

    def test_no_auth_phase_skips_the_certificates(self):
        self._manager([2, 1], ssl_mode="none").set_x509_certificates()

        self.assertEqual(self.calls, [])

    def test_mandatory_client_cert_auth_fails_with_a_password_tls_phase(self):
        for ssl_mode in ("data", "n2n"):
            with self.subTest(ssl_mode=ssl_mode), self.assertRaises(SystemExit):
                self._manager(
                    [2, 1], client_cert_auth_state="mandatory", load_ssl_mode=ssl_mode
                ).set_x509_certificates()

        self.assertEqual(self.calls, [])

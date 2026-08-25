"""REST scope of ClusterManager.set_x509_certificates()."""

import os
import tempfile
from types import SimpleNamespace
from unittest import TestCase

from perfrunner.settings import ClusterSpec


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

        # `local` writes real certificates to disk; stand in for it so the test only observes
        # which hosts end up in the SAN list, and which CN the client certificate is issued to.
        self.cluster_module = cluster_module
        self.addCleanup(setattr, cluster_module, "local", cluster_module.local)
        cluster_module.local = SimpleNamespace(
            generate_server_x509_cert=self.generated.append,
            generate_client_x509_cert=lambda cn, storepass: self.client_certs.append(
                (cn, storepass)
            ),
        )

    def _manager(self, initial_nodes, buckets=("bucket-1", "bucket-2")):
        def record(name):
            return lambda node, *args, **kwargs: self.calls.append((name, node))

        # Constructed without __init__, which would build a RestHelper, a RemoteHelper and a
        # Monitor. None of them affect which nodes each step targets.
        cm = object.__new__(self.cluster_module.DefaultClusterManager)
        cm.cluster_spec = self.cluster_spec
        cm.initial_nodes = initial_nodes
        cm.test_config = SimpleNamespace(
            access_settings=SimpleNamespace(ssl_mode="auth", ssl_keystore_password="storepass"),
            xdcr_settings=SimpleNamespace(cng_haproxy=False),
            buckets=buckets,
        )
        cm.rest = SimpleNamespace(
            upload_cluster_certificate=record("upload"),
            reload_cluster_certificate=record("reload"),
            enable_certificate_auth=record("enable"),
        )
        cm.remote = SimpleNamespace(
            allow_non_local_ca_upload=lambda: None,
            setup_x509=lambda: None,
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
        # Joined nodes only: a standalone spare with this enabled would demand a client
        # certificate during its own join handshake.
        self.assertEqual(self._hosts("enable"), ["10.0.0.1", "10.0.0.2", "10.0.1.1"])

    def test_second_cluster_is_not_read_off_the_first_clusters_spares(self):
        # Walking the flat server list by offset put the second cluster's window inside the
        # first cluster's spare nodes.
        self._manager([2, 1]).set_x509_certificates()

        self.assertIn("10.0.1.1", self._hosts("enable"))
        self.assertNotIn("10.0.0.3", self._hosts("enable"))

    def test_a_cluster_without_spares_reloads_only_its_own_nodes(self):
        self._manager([3, 2]).set_x509_certificates()

        self.assertEqual(self._hosts("upload"), ["10.0.0.1", "10.0.1.1"])
        self.assertEqual(self._hosts("enable"), self._hosts("reload"))

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

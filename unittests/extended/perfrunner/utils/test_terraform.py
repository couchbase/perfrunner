"""App Services deployment on Capella."""

import json
import os
import tempfile
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Optional
from unittest import TestCase
from unittest.mock import patch

import pytest
import requests
from requests.exceptions import HTTPError

from perfrunner.settings import ClusterSpec
from perfrunner.utils import terraform


def make_deployer(
    infra_spec: Optional[ClusterSpec] = None, nodes: int = 2, **api
) -> terraform.AppServicesDeployer:
    """Return a deployer calling the fake Capella API methods in ``api``.

    Constructed without __init__, which would log in to the Capella control plane, so this is the
    one place to add an attribute that a deployer method starts to need from __init__.
    """
    deployer = object.__new__(terraform.AppServicesDeployer)
    deployer.infra_spec = infra_spec or SimpleNamespace(
        config=SimpleNamespace(set=lambda *args: None), update_spec_file=lambda: None
    )
    deployer.cluster_ids = infra_spec.capella_cluster_ids if infra_spec else ["cluster-a"]
    deployer.options = SimpleNamespace(capella_sgw_version=None)
    deployer.test_config = SimpleNamespace(
        syncgateway_settings=SimpleNamespace(nodes=nodes, instance="n2-custom-16-32768")
    )
    deployer.uuid = "00817f"
    deployer.tenant_id = "tenant"
    deployer.project_id = "project"
    deployer.appsvc_ids = []
    deployer.capella_timeout = 20
    deployer.provisioned_api = SimpleNamespace(**api)
    return deployer


def response(status_code: int, body: dict) -> requests.Response:
    resp = requests.Response()
    resp.status_code = status_code
    resp._content = json.dumps(body).encode()
    return resp


def parse_spec(spec: str) -> tuple[ClusterSpec, str]:
    spec_file = tempfile.NamedTemporaryFile(mode="w", suffix=".spec", delete=False)
    spec_file.write(spec)
    spec_file.close()
    cluster_spec = ClusterSpec()
    cluster_spec.parse(spec_file.name, override=None)
    return cluster_spec, spec_file.name


class AppServicesMultiClusterTest(TestCase):
    """Each Capella cluster gets its own App Services backend, and keeps it apart from the others.

    A spec with two clusters, each with its own [syncgateways] option, is the only shape that
    tells "per cluster" apart from "the first cluster, or the last one deployed".
    """

    SPEC = (
        "[infrastructure]\n"
        "provider = capella\n"
        "backend = aws\n"
        "app_services = true\n"
        "\n"
        "[clusters]\n"
        "couchbase1 =\n"
        "    ec2.ec2_cluster_1.ec2_node_group_1.1:kv\n"
        "couchbase2 =\n"
        "    ec2.ec2_cluster_2.ec2_node_group_2.1:kv\n"
        "\n"
        "[syncgateways]\n"
        "syncgateways1 =\n"
        "    ec2.ec2_cluster_1.ec2_node_group_3.1\n"
        "    ec2.ec2_cluster_1.ec2_node_group_3.2\n"
        "syncgateways2 =\n"
        "    ec2.ec2_cluster_2.ec2_node_group_4.1\n"
        "    ec2.ec2_cluster_2.ec2_node_group_4.2\n"
        "\n"
        "[clients]\n"
        "workers1 =\n"
        "    ec2-10-0-2-1.compute-1.amazonaws.com\n"
        "\n"
        "[controlplane]\n"
        "cluster_ids =\n"
        "    cluster-a\n"
        "    cluster-b\n"
    )

    BACKENDS = {"cluster-a": "backend-a", "cluster-b": "backend-b"}
    HOSTS = {("cluster-a", "backend-a"): "a.apps.example.com",
             ("cluster-b", "backend-b"): "b.apps.example.com"}

    def setUp(self):
        self.cluster_spec, self.spec_fname = parse_spec(self.SPEC)
        self.addCleanup(os.unlink, self.spec_fname)
        self.allowed_ips = []

    def _deployer(self) -> terraform.AppServicesDeployer:
        return make_deployer(
            self.cluster_spec,
            get_sgw_links=self._sgw_links,
            add_allowed_ip_sgw=lambda *args: (
                self.allowed_ips.append(args) or SimpleNamespace(ok=True)
            ),
        )

    def _sgw_links(self, tenant_id, project_id, cluster_id, sgw_cluster_id, db_name):
        # A KeyError here means a backend was looked up under another cluster
        host = self.HOSTS[(cluster_id, sgw_cluster_id)]
        data = {"adminURL": f"https://{host}:4985", "publicURL": f"wss://{host}:4984/{db_name}"}
        return SimpleNamespace(json=lambda: {"data": data})

    def _reparsed(self) -> ClusterSpec:
        spec = ClusterSpec()
        spec.parse(self.spec_fname, override=None)
        return spec

    def test_each_cluster_fills_its_own_syncgateways_option(self):
        deployer = self._deployer()

        deployer.update_app_services_spec("cluster-a", "backend-a", "db-1")
        deployer.update_app_services_spec("cluster-b", "backend-b", "db-1")

        self.assertEqual(
            dict(self._reparsed().sgw_clusters),
            {
                "syncgateways1": ["a.apps.example.com"] * 2,
                "syncgateways2": ["b.apps.example.com"] * 2,
            },
        )

    def test_deploy_allows_ips_on_every_backend(self):
        deployer = self._deployer()
        deployer.deploy_cluster_internal_api = self.BACKENDS.get
        deployer.deploy_app_endpoints = lambda cluster_id, sgw_cluster_id: None

        with patch.object(terraform, "record_time", lambda *args: nullcontext()), \
                patch.object(terraform, "my_public_ip", lambda: "203.0.113.7"):
            deployer.deploy()

        # add_allowed_ip_sgw builds its URL with cluster_id and backend_id swapped, so callers
        # pass the backend first
        self.assertEqual(
            self.allowed_ips,
            [
                ("tenant", "project", "backend-a", "cluster-a", "10.0.2.1"),
                ("tenant", "project", "backend-a", "cluster-a", "203.0.113.7"),
                ("tenant", "project", "backend-b", "cluster-b", "10.0.2.1"),
                ("tenant", "project", "backend-b", "cluster-b", "203.0.113.7"),
            ],
        )


class AppServicesGcpLinksTest(TestCase):
    """GCP App Services URLs carry no port: the public one is wss://<id>.apps.../<db>."""

    SPEC = (
        "[infrastructure]\n"
        "provider = capella\n"
        "backend = gcp\n"
        "app_services = true\n"
        "\n"
        "[clusters]\n"
        "couchbase1 =\n"
        "    gcp.gcp_cluster_1.gcp_node_group_1.1:kv\n"
        "\n"
        "[syncgateways]\n"
        "syncgateways1 =\n"
        "    gcp.gcp_cluster_1.gcp_node_group_2.1\n"
        "\n"
        "[controlplane]\n"
        "cluster_ids =\n"
        "    cluster-a\n"
    )

    def test_syncgateways_get_the_public_host(self):
        cluster_spec, spec_fname = parse_spec(self.SPEC)
        self.addCleanup(os.unlink, spec_fname)
        links = {
            "adminURL": "https://abc123-admin.apps.example.com/db-1",
            "publicURL": "wss://abc123.apps.example.com/db-1",
        }
        deployer = make_deployer(
            cluster_spec,
            get_sgw_links=lambda *args: SimpleNamespace(json=lambda: {"data": links}),
        )

        deployer.update_app_services_spec("cluster-a", "backend-a", "db-1")

        spec = ClusterSpec()
        spec.parse(spec_fname, override=None)
        self.assertEqual(
            dict(spec.sgw_clusters), {"syncgateways1": ["abc123.apps.example.com"] * 2}
        )


def _timeout(*args):
    # What the Capella client raises when the request times out
    raise AttributeError("'NoneType' object has no attribute 'status_code'")


@pytest.mark.parametrize(
    "outcomes, calls",
    [
        pytest.param([None, SimpleNamespace(ok=True)], 2, id="no response is retried"),
        pytest.param([_timeout, SimpleNamespace(ok=True)], 2, id="timeout is retried"),
        pytest.param(
            [response(422, {"message": "Duplicate IP 10.0.2.1"})], 1,
            id="duplicate is already allowed",
        ),
    ],
)
def test_allow_ip_succeeds(outcomes, calls):
    deployer, made = _allow_ip_deployer(outcomes)

    with patch.object(terraform, "sleep", lambda secs: None):
        deployer.allow_ip_sgw("cluster-a", "backend-a", "10.0.2.1")

    assert len(made) == calls


def test_allow_ip_gives_up_after_the_last_attempt():
    deployer, made = _allow_ip_deployer([None, _timeout, response(500, {})])

    with patch.object(terraform, "sleep", lambda secs: None), \
            pytest.raises(Exception, match="Could not allowlist 10.0.2.1"):
        deployer.allow_ip_sgw("cluster-a", "backend-a", "10.0.2.1")

    assert len(made) == 3


def _allow_ip_deployer(outcomes: list) -> tuple[terraform.AppServicesDeployer, list]:
    """Return a deployer whose allowlist requests return or raise ``outcomes`` in turn."""
    outcomes = iter(outcomes)
    made = []

    def add_allowed_ip_sgw(*args):
        made.append(args)
        outcome = next(outcomes)
        return outcome(*args) if callable(outcome) else outcome

    return make_deployer(add_allowed_ip_sgw=add_allowed_ip_sgw), made


class AppServicesDeployTimeoutTest(TestCase):
    """A backend or database that never comes up fails the deploy instead of being waited on.

    Otherwise a backend still deploying at the timeout has its database create rejected with a
    422, and the deploy then polls for that database for hours.
    """

    def setUp(self):
        self.now = 0.0

        def fake_sleep(secs):
            self.now += secs

        for name, fake in (("sleep", fake_sleep), ("time", lambda: self.now)):
            patcher = patch.object(terraform, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _deployer(self, **api) -> terraform.AppServicesDeployer:
        return make_deployer(
            nodes=4, create_sgw_backend=lambda *args: response(200, {"id": "backend-a"}), **api
        )

    @staticmethod
    def _backend_states(*states):
        states = iter(states)
        return lambda *args: response(200, {"data": {"status": {"state": next(states)}}})

    def test_backend_that_becomes_healthy_is_returned(self):
        deployer = self._deployer(
            get_sgw_info=self._backend_states("deploying", "deploying", "healthy")
        )

        self.assertEqual(deployer.deploy_cluster_internal_api("cluster-a"), "backend-a")

    def test_backend_still_deploying_at_timeout_fails(self):
        deployer = self._deployer(get_sgw_info=self._backend_states(*["deploying"] * 100))

        with self.assertRaises(SystemExit):
            deployer.deploy_cluster_internal_api("cluster-a")
        self.assertLessEqual(self.now, 20 * 60 + 30)

    def test_rejected_database_create_fails_without_waiting(self):
        rejected = response(
            422,
            {
                "errorType": "EntityStateInvalid",
                "message": "Temporarily unavailable while the App Service is in the Deploying "
                "state.",
            },
        )
        deployer = self._deployer(
            create_sgw_database=lambda *args: rejected,
            get_sgw_databases=lambda *args: self.fail("waited for a database never created"),
        )

        with self.assertRaises(HTTPError):
            deployer._deploy_app_services_db("cluster-a", "backend-a", "db-1", 1, {})

    def test_database_never_online_fails_at_timeout(self):
        deployer = self._deployer(
            create_sgw_database=lambda *args: response(200, {"id": "database-1"}),
            get_sgw_databases=lambda *args: response(
                200, {"data": [{"data": {"state": "Initializing"}}]}
            ),
        )

        with self.assertRaises(SystemExit):
            deployer._deploy_app_services_db("cluster-a", "backend-a", "db-1", 1, {})
        self.assertLessEqual(self.now, 20 * 60 + 10)

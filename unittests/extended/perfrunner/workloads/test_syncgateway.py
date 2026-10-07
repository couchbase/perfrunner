"""YCSB command lines for the Sync Gateway workloads."""

from tempfile import NamedTemporaryFile
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pytest

from perfrunner.settings import ClusterSpec
from perfrunner.workloads import syncgateway


def parse_spec(spec: str) -> ClusterSpec:
    with NamedTemporaryFile("w", suffix=".spec") as spec_file:
        spec_file.write(spec)
        spec_file.flush()
        cluster_spec = ClusterSpec()
        cluster_spec.parse(spec_file.name, override=None)
    return cluster_spec


def sgw_spec(provider: str, backend: str, host: str) -> str:
    return (
        "[infrastructure]\n"
        f"provider = {provider}\n"
        f"backend = {backend}\n"
        "\n"
        "[syncgateways]\n"
        "syncgateways1 =\n"
        f"    {host}\n"
        f"    {host}\n"
        "\n"
        "[client_private_ips]\n"
        "workers1 = 10.1.0.3\n"
    )


@pytest.mark.parametrize(
    "provider, backend, host, api, expected",
    [
        # GCP App Services serves each API from its own host, all on port 443
        ("capella", "gcp", "abc123.apps.example.com", "admin", "abc123-admin.apps.example.com"),
        ("capella", "gcp", "abc123.apps.example.com", "metric", "abc123-metric.apps.example.com"),
        (
            "capella", "gcp", "abc123-admin.apps.example.com", "admin",
            "abc123-admin.apps.example.com",
        ),
        # Elsewhere the APIs share the host and differ by port
        ("capella", "aws", "abc123.apps.example.com", "admin", "abc123.apps.example.com"),
        ("capella", "azure", "abc123.apps.example.com", "admin", "abc123.apps.example.com"),
        ("aws", "aws", "sgw1.example.com", "admin", "sgw1.example.com"),
        ("", "", "172.23.96.1", "admin", "172.23.96.1"),
    ],
)
def test_sgw_api_host(provider: str, backend: str, host: str, api: str, expected: str):
    cluster_spec = parse_spec(sgw_spec(provider, backend, host))

    assert cluster_spec.sgw_api_host(host, api) == expected


class LoadUsersTest(TestCase):
    """Users are created through the admin API, so YCSB must be pointed at whichever host serves it.

    On GCP that is <id>-admin.apps...: the public host times out on the admin port, no user gets
    created, and every later phase fails with a NullPointerException in YCSB.
    """

    def test_gcp_creates_users_on_the_admin_host(self):
        cluster_spec = parse_spec(sgw_spec("capella", "gcp", "abc123.apps.example.com"))
        workload_settings = SimpleNamespace(
            syncgateway_settings=SimpleNamespace(
                log_title="sync_gateway_writes", ycsb_command="syncgateway",
                workload="workloads/syncgateway_blank", users=1000, sg_loader_threads=50,
                channels=100, channels_per_user=10, channels_per_doc=10, insertstart=0,
                starchannel="false", e2e="false", nodes=2, clients=1,
            ),
            collections=None,
            public_port=443,
            admin_port=443,
            memcached_port="8000",
        )
        with patch.object(syncgateway, "run_cmd") as run_cmd:
            syncgateway.syncgateway_load_users(
                workload_settings, SimpleNamespace(bucket="bucket-1"), 0, 0, cluster_spec
            )

        self.assertIn(
            "-p syncgateway.host=abc123-admin.apps.example.com ", run_cmd.call_args.args[2]
        )


class E2ELoadDocsExportTest(TestCase):
    """Each load-docs worker exports its docs where the cblite import later looks for them.

    The test reconstructs the file and db from the worker id, so both must come from the same
    helpers as the YCSB command line.
    """

    def _settings(self, clients: int, instances_per_client: int) -> SimpleNamespace:
        return SimpleNamespace(
            syncgateway_settings=SimpleNamespace(
                log_title="sync_gateway_default", ycsb_command="syncgateway3",
                workload="workloads/syncgateway_blank", cbl_target="127.0.0.1",
                clients=str(clients), instances_per_client=str(instances_per_client),
                documents="1000", delta_sync="false", e2e="true", threads_per_instance="1",
                fieldlength="100", fieldcount="10", cbl_throughput="1000", doctype="basic",
                doc_depth="1", users="100", channels="100", channels_per_user="1",
                channels_per_doc="1", ycsb_retry_count="5", ycsb_retry_interval="1",
                data_integrity="false",
            ),
            collections=None,
            memcached_port="8000",
        )

    def _ycsb_run(self, settings: SimpleNamespace, worker_id: int) -> tuple[str, str]:
        cluster = SimpleNamespace(capella_infrastructure=False)
        with patch.object(syncgateway, "run_cmd") as run_cmd, \
                patch.object(syncgateway, "get_memcached_host", lambda *args: "10.1.0.3"):
            syncgateway.syncgateway_e2e_multi_cbl_load_docs(settings, None, 0, worker_id, cluster)
        path, _, params, _ = run_cmd.call_args.args
        return path, params

    def test_every_worker_exports_to_the_db_and_file_the_import_reads(self):
        settings = self._settings(clients=10, instances_per_client=10)

        for worker_id in range(1, 101):
            path, params = self._ycsb_run(settings, worker_id)
            export = syncgateway.e2e_load_docs_export(settings, worker_id)

            self.assertEqual(export, f"{path}/sync_gateway_default_loaddocs_{worker_id}.json")
            self.assertIn(f"-p syncgateway.exportfile={export.split('/')[-1]} ", params)
            self.assertIn(
                f"-p syncgateway.db={syncgateway.e2e_load_docs_db(settings, worker_id)} ", params
            )

    def test_workers_are_numbered_client_by_client(self):
        settings = self._settings(clients=10, instances_per_client=10)

        # Worker 23 is the 3rd client's worker for the 3rd instance
        self.assertEqual(syncgateway.e2e_load_docs_db(settings, 23), "db_2")
        self.assertEqual(
            syncgateway.e2e_load_docs_export(settings, 23),
            "YCSB_3/sync_gateway_default_loaddocs_23.json",
        )

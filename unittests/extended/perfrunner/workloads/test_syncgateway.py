"""YCSB command lines for the Sync Gateway workloads."""

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from perfrunner.workloads import syncgateway


class LoadUsersTest(TestCase):
    """Users are created through the admin API, so YCSB must be pointed at whichever host serves it.

    On GCP that is <id>-admin.apps...: the public host times out on the admin port, no user gets
    created, and every later phase fails with a NullPointerException in YCSB.
    """

    def test_users_are_created_on_the_admin_host(self):
        cluster = Mock(capella_infrastructure=False)
        cluster.sgw_api_host.return_value = "abc123-admin.apps.example.com"
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
        with patch.object(syncgateway, "run_cmd") as run_cmd, \
                patch.object(syncgateway, "get_hosts", lambda *args: "abc123.apps.example.com"), \
                patch.object(syncgateway, "get_memcached_host", lambda *args: "10.1.0.3"):
            syncgateway.syncgateway_load_users(
                workload_settings, SimpleNamespace(bucket="bucket-1"), 0, 0, cluster
            )

        cluster.sgw_api_host.assert_called_once_with("abc123.apps.example.com", "admin")
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

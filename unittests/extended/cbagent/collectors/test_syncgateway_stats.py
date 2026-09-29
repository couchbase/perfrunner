"""`SyncGatewayStats`: aggregating Sync Gateway's Prometheus metrics into one cluster sample."""

import os
from types import SimpleNamespace
from unittest import TestCase

from perfrunner.remote import api

NODE_1 = """\
# TYPE sgw_database_num_doc_writes counter
sgw_database_num_doc_writes{database="db-1"} 100
sgw_database_num_doc_writes{database="db-2"} 50
sgw_collection_num_doc_writes{collection="scope-1.collection-1",database="db-1"} 100
go_goroutines 300
sgw_resource_utilization_process_cpu_percent_utilization 375
go_sched_gomaxprocs_threads 48
"""
NODE_2 = """\
sgw_database_num_doc_writes{database="db-1"} 25
sgw_resource_utilization_process_cpu_percent_utilization 100
go_sched_gomaxprocs_threads 8
"""


class SyncGatewayStatsTest(TestCase):
    @classmethod
    def setUpClass(cls):
        """Import the collector here, restoring the shared remote env it rewrites.

        See `McstatHistogramCollectorTest.setUpClass` in `test_kvstore_stats.py` for why the
        import is deferred and why a worker type is declared first.
        """
        env_before = vars(api.env).copy()
        cls.addClassCleanup(vars(api.env).update, env_before)
        cls.addClassCleanup(vars(api.env).clear)

        os.environ.setdefault("WORKER_TYPE", "local")

        from cbagent.collectors.syncgateway_stats import SyncGatewayStats

        cls.collector_cls = SyncGatewayStats

    def test_sample_sums_nodes_and_databases_and_registers_only_what_it_stores(self):
        stats_by_host = {"sgw-1": NODE_1, "sgw-2": NODE_2}
        collector = self.collector_cls.__new__(self.collector_cls)
        collector.cluster = "cluster-1"
        collector.hosts = list(stats_by_host)
        collector.rest = SimpleNamespace(get_sg_stats=stats_by_host.__getitem__)
        registered, stored = [], []
        collector.update_metric_metadata = registered.extend
        collector._store = SimpleNamespace(
            append=lambda data, **kwargs: stored.append((data, kwargs))
        )

        collector.sample()

        # Unlisted metrics are dropped, and listed ones SGW does not expose (e.g. delta sync) are
        # absent rather than a flat zero, so cbmonitor never lists a metric without data.
        # vCPUs are 10x-percent of the machine times each node's own cores: 375 * 48 / 1000 = 18
        # on the first node, 100 * 8 / 1000 = 0.8 on the second.
        [(data, kwargs)] = stored
        self.assertEqual(data, {
            "sgw_database_num_doc_writes": 175,
            "sgw_resource_utilization_process_cpu_percent_utilization": 475,
            "sgw_process_vcpus": 18.8,
        })
        self.assertEqual(registered, list(data))
        self.assertEqual(kwargs, {"cluster": "cluster-1", "collector": "syncgateway_cluster_stats"})

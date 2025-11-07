from __future__ import annotations

from typing import TYPE_CHECKING

from perfrunner.helpers.metrics import Metric, MetricHelper

if TYPE_CHECKING:
    from perfrunner.tests import PerfTest


class PrometheusMetricsHelper(MetricHelper):

    def __init__(self, test: PerfTest):
        super().__init__(test)
        self.store = test.collector_agent

    def avg_disk_write_queue(self) -> Metric:
        metric_info = self._metric_info(chirality=-1)
        summary = self.store.get_summary(metric="kv_ep_queue_size")
        avg_disk_write_queue = int(summary.get('avg', 0))
        return avg_disk_write_queue, self._snapshots, metric_info

    def avg_total_queue_age(self) -> Metric:
        metric_info = self._metric_info(chirality=-1)
        summary = self.store.get_summary(metric="kv_vb_queue_age_seconds")
        avg_total_queue_age = int(summary.get('avg', 0))
        return avg_total_queue_age, self._snapshots, metric_info

    def avg_server_process_cpu(self, server_process: str) -> Metric:
        metric_id = f"{self.test_config.name}_avg_{server_process}_cpu"
        metric_id = metric_id.replace(".", "_")
        title = f"Avg. {server_process} CPU utilization (%), {self._title}"
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        summary = self.store.get_summary(
            metric="sysproc_cpu_utilization",
            filters={"proc": server_process},
        )
        avg_server_process_cpu = round(summary.get('avg', 0), 1)
        return avg_server_process_cpu, self._snapshots, metric_info

    def max_memcached_rss(self) -> Metric:
        metric_id = f"{self.test_config.name}_memcached_rss"
        title = f"Max. memcached RSS (MB), {self._title.split(',')[-1]}"
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        summary = self.store.get_summary(
            metric="sysproc_mem_resident",
            filters={"proc": "memcached"},
        )
        max_memcached_rss = round(summary.get('max', 0) / 1024 ** 2)
        return max_memcached_rss, self._snapshots, metric_info

    def avg_memcached_rss(self) -> Metric:
        metric_id = f"{self.test_config.name}_avg_memcached_rss"
        title = f"Avg. memcached RSS (MB), {self._title.split(',')[-1]}"
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        summary = self.store.get_summary(
            metric="sysproc_mem_resident",
            filters={"proc": "memcached"},
        )
        avg_memcached_rss = int(summary.get('avg', 0) / 1024 ** 2)
        return avg_memcached_rss, self._snapshots, metric_info

    def get_percentile_value_of_node_metric(self, collector, metric, server, percentile) -> int:
        # Older version of this function only used to for memcached_rss metric.
        # This is a compatibility placeholder for now.
        summary = self.store.get_summary(
            metric="sysproc_mem_resident",
            percentiles=[percentile],
            filters={"proc": "memcached", "node": server},
        )
        percentile_label = str(float(percentile) / 100)
        return int(summary.get("percentiles", {}).get(percentile_label, 0))

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from perfrunner.helpers.metrics import MetricHelper

if TYPE_CHECKING:
    from perfrunner.tests import PerfTest


class PrometheusMetricsHelper(MetricHelper):
    """MetricHelper backed by the Prometheus metrics store.

    Swaps the store to the PrometheusAgent. MetricHelper methods read the cbagent metrics
    pushed to the store (each read resolves to the same ``perf_<name>`` the collectors push).
    """

    def __init__(self, test: PerfTest):
        super().__init__(test)
        self.store = test.collector_agent

    def _read_values(
        self,
        metric: str,
        collector: str,
        *,
        cluster: Optional[str] = None,
        cluster_idx: int = 0,
        bucket: Optional[str] = None,
        server: Optional[str] = None,
    ) -> list[float]:
        """Read a scoped metric from the Prometheus store using label filters."""
        filters = self._filters(collector, cluster, cluster_idx, bucket, server)
        return self.store.get_values("", metric=metric, filters=filters)

    def _read_timeseries(
        self,
        metric: str,
        collector: str,
        *,
        cluster: Optional[str] = None,
        cluster_idx: int = 0,
        bucket: Optional[str] = None,
        server: Optional[str] = None,
    ) -> list[list[int, float]]:
        """Read a scoped metric's ``[[ts_ms, value], ...]`` series using label filters."""
        filters = self._filters(collector, cluster, cluster_idx, bucket, server)
        return self.store.get_timeseries("", metric=metric, filters=filters)

    def _filters(
        self,
        collector: str,
        cluster: Optional[str],
        cluster_idx: int,
        bucket: Optional[str],
        server: Optional[str],
    ) -> dict[str, str]:
        """Translate a read scope into the snapshot API's label filters.

        ``server`` maps to the ``instance`` label, matching how PromStore pushes.
        """
        if cluster is None:
            cluster = self.test.cbmonitor_clusters[cluster_idx]
        filters = {"cluster": cluster, "collector": collector}
        if bucket:
            filters["bucket"] = bucket
        if server:
            filters["instance"] = server
        return filters

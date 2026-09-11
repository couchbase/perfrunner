"""Collector for metrics only Capella's control plane can answer for.

Cloud volume counters (``disk_iops_*``, ``disk_bytes_*``) come from the provider's own
monitoring, and host NIC throughput has no ns_server equivalent at all - sigar exposes
no network counters. Both are served by the control plane's curated metrics API, a fixed
set of keys each backed by PromQL Capella owns, so its aggregation is taken as given.

That endpoint lags and is scraped about once a minute, so this collector does not sample.
It sits out the phase, then at phase end makes one batched ``query_range`` call for the
phase window and pushes the series with their real timestamps.
"""

import json
import math
import time
from typing import Optional

from cbagent.collectors.collector import Collector
from cbagent.promstore import CAPELLA_METRICS_PREFIX
from cbagent.settings import CbAgentSettings
from logger import logger


class CapellaControlPlaneMetrics(Collector):
    """Pull curated Capella metrics for the phase window and push them to the store.

    Opt in per test with ``stats.extra_collectors = capella_cp_metrics``.
    """

    COLLECTOR = "capella_cp_metrics"
    COLLECTOR_FLAG = "capella_cp_metrics"

    PROMETHEUS_CUSTOM = True
    RECONSTRUCT_ONLY = True
    REQUIRES_CAPELLA = True

    METRICS_PREFIX = CAPELLA_METRICS_PREFIX

    # Curated keys to pull. An unknown key fails only its own entry in the batch.
    #
    # Limited to what ns_server cannot answer: Capella's CPU and memory keys duplicate
    # ``sys_*``, which is already scraped at a finer cadence. Each key here is a
    # ``rate(...[5m])``; the sibling ``*_usage_avg``/``*_rate_avg`` keys wrap that in
    # ``avg_over_time(...[1h:5m])``, whose hour-long window spans the phase boundary.
    METRICS = (
        # Cloud volume IOPS.
        "disk_iops_reads",
        "disk_iops_writes",
        "disk_iops_total",
        # Cloud volume throughput (bytes/sec).
        "disk_throughput_reads",
        "disk_throughput_writes",
        "disk_throughput_total",
        # Host NIC throughput (bytes/sec).
        "node_byte_received_rate",
        "node_byte_transmitted_rate",
    )

    # Every value pushed is a gauge, but Prometheus reserves ``_total`` for counters.
    # Grafana's metrics drilldown honours that suffix and renders such a metric as
    # ``rate(metric[$__rate_interval])``, which is empty at intervals below our 60s
    # sample spacing. Store these under a suffix that does not imply a counter.
    METRIC_RENAMES = {
        "disk_iops_total": "disk_iops_all",
        "disk_throughput_total": "disk_throughput_all",
    }

    # The control plane accepts a finer step, but its scrape cadence is the real floor.
    STEP_SECONDS = 60

    # Mapped onto ``instance`` so these line up with the scraped metrics.
    NODE_LABEL = "couchbaseNode"

    # Samples per push request: a few requests per phase, no single enormous body.
    PUSH_BATCH_SIZE = 2000

    # A failed control-plane hop to the ingestor surfaces as 502 and is worth retrying:
    # the window stops being queryable once the cluster is torn down.
    #
    # FETCH_BUDGET caps how many attempts are started. It cannot interrupt one in flight -
    # the Capella client hardcodes a 300s request timeout with no way to shorten it - so a
    # single client timeout remains the worst case, while fast failures get every attempt.
    FETCH_ATTEMPTS = 4
    FETCH_BACKOFF = 5  # seconds before the first retry, doubled each time
    FETCH_BUDGET = 120  # seconds across all attempts, backoff included

    def __init__(self, settings: CbAgentSettings, test=None):
        super().__init__(settings, test)
        self.cluster = settings.cluster
        self.master_node = settings.master_node
        self.rest = settings.cloud.get("cloud_rest")
        self.phase_start: Optional[float] = None

    def on_phase_start(self):
        self.phase_start = time.time()

    @property
    def cluster_id(self) -> Optional[str]:
        """Capella cluster id for the cluster this instance was created for."""
        if not (self.rest and hasattr(self.rest, "hostname_to_cluster_id")):
            return None
        return self.rest.hostname_to_cluster_id(self.master_node)

    def _batch_url(self, cluster_id: str) -> str:
        return (
            f"{self.rest.dedicated_client.internal_url}"
            f"/v2/organizations/{self.rest.tenant_id}"
            f"/projects/{self.rest.project_id}"
            f"/clusters/{cluster_id}/batch_metrics/query_range"
        )

    def _fetch(self, cluster_id: str, start: int, end: int) -> dict:
        """Return the control plane's ``results`` map for the phase window.

        The batch endpoint takes a JSON array of ``{"metricKey": ...}`` objects and
        answers with one entry per key, so the whole metric set costs one request.
        """
        params = {
            "start": start,
            "end": end,
            "step": str(self.STEP_SECONDS),
            # Must be a JSON array of objects. A bare comma-separated list or an array
            # of plain strings is rejected by the control plane with a 500, not a 422.
            "inputMetricSet": json.dumps([{"metricKey": metric} for metric in self.METRICS]),
        }
        url = self._batch_url(cluster_id)
        backoff = self.FETCH_BACKOFF
        deadline = time.monotonic() + self.FETCH_BUDGET

        for attempt in range(1, self.FETCH_ATTEMPTS + 1):
            try:
                response = self.rest.dedicated_client.do_internal_request(
                    url, method="GET", params=params
                )
            except Exception as e:
                # The Capella client raises rather than returning on a dropped connection:
                # its helper swallows the error and returns None, which it then dereferences.
                logger.warning(f"{self.COLLECTOR}: control plane request raised: {e!r}")
                response = None

            if response is not None and response.ok:
                body = response.json().get("data") or {}
                if summary := body.get("summary"):
                    logger.info(f"{self.COLLECTOR}: control plane returned {summary}")
                return body.get("results") or {}

            if response is None:
                reason, retryable = "no response from the control plane", True
            else:
                reason = f"HTTP {response.status_code}: {response.text[:200]}"
                # A 4xx (unknown key, invisible cluster) fails identically on a retry.
                retryable = response.status_code >= 500

            remaining = deadline - time.monotonic()
            out_of_budget = remaining <= backoff
            if not retryable or attempt == self.FETCH_ATTEMPTS or out_of_budget:
                budget = ""
                if out_of_budget:
                    spent = self.FETCH_BUDGET - remaining
                    budget = f"; gave up after {spent:.0f}s of {self.FETCH_BUDGET}s budget"
                logger.warning(f"{self.COLLECTOR}: batch query failed ({reason}{budget})")
                return {}

            logger.warning(
                f"{self.COLLECTOR}: batch query attempt {attempt}/{self.FETCH_ATTEMPTS} "
                f"failed ({reason}); retrying in {backoff}s"
            )
            time.sleep(backoff)
            backoff *= 2

        return {}

    def _rows_for(self, metric: str, series: list) -> list:
        """Turn one metric's matrix result into store rows."""
        rows = []
        for entry in series:
            # A cluster-wide series carries no node label and is pushed without one.
            server = (entry.get("metric") or {}).get(self.NODE_LABEL)
            for point in entry.get("values") or []:
                # Prometheus matrix points are [<epoch seconds float>, "<value>"].
                timestamp, value = point[0], point[1]
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    continue
                # "NaN" parses as a float, so isfinite is what drops Prometheus gap markers.
                if not math.isfinite(value):
                    continue
                rows.append({
                    "data": {metric: value},
                    "cluster": self.cluster,
                    "server": server,
                    "collector": self.COLLECTOR,
                    "timestamp": int(float(timestamp) * 1000),
                })
        return rows

    def reconstruct(self):
        """Pull the phase window from the control plane and push it to the store.

        Never raises: ``PrometheusAgent.__exit__`` has no guard of its own, and this data
        is supplementary, so a control-plane failure must not abort a completed phase.
        """
        try:
            self._reconstruct()
        except Exception as e:
            logger.warning(f"{self.COLLECTOR}: backfill failed, continuing without it: {e!r}")

    def _reconstruct(self):
        if not self.rest:
            logger.warning(f"{self.COLLECTOR}: no Capella REST helper available, skipping")
            return

        cluster_id = self.cluster_id
        if not cluster_id:
            logger.warning(f"{self.COLLECTOR}: no Capella cluster id for {self.master_node}")
            return

        end = int(time.time())
        # Falls back to a fixed lookback if on_phase_start never ran.
        start = int(self.phase_start) if self.phase_start else end - 300
        if end <= start:
            logger.warning(f"{self.COLLECTOR}: empty phase window, skipping")
            return

        logger.info(
            f"{self.COLLECTOR}: pulling {len(self.METRICS)} curated metric(s) for "
            f"cluster {cluster_id} over {end - start}s"
        )
        results = self._fetch(cluster_id, start, end)

        rows, empty = [], []
        for key, result in results.items():
            # Results are keyed "<metricKey>:<metricOptions>".
            metric = str(result.get("metricName") or key.split(":")[0])
            metric = self.METRIC_RENAMES.get(metric, metric)
            series = ((result.get("data") or {}).get("result")) or []
            metric_rows = self._rows_for(metric, series)
            rows.extend(metric_rows)
            if not metric_rows:
                empty.append(metric)

        if empty:
            logger.info(f"{self.COLLECTOR}: no data returned for {sorted(empty)}")

        for i in range(0, len(rows), self.PUSH_BATCH_SIZE):
            self.store.append_batch(rows[i : i + self.PUSH_BATCH_SIZE])
        logger.info(f"{self.COLLECTOR}: pushed {len(rows)} sample(s)")

    def sample(self):
        """Never called: this collector is RECONSTRUCT_ONLY and runs no process."""
        raise NotImplementedError

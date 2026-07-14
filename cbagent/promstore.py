import re
from typing import Optional

import aiohttp
import requests

from logger import logger
from perfrunner.settings import CONFIG_MANAGER_HOST

# Ensure our custom metrics do not conflict with server and sgw metrics
PERF_METRICS_PREFIX = "perf_"

# Any character outside the Prometheus metric-name charset gets replaced with `_`.
# Prometheus metric names must match [a-zA-Z_:][a-zA-Z0-9_:]*.
_INVALID_METRIC_NAME_CHAR = re.compile(r"[^a-zA-Z0-9_:]")


class PromStore:
    """Store that pushes cbagent collector samples in Prometheus exposition format.

    Exposes ``append`` and ``append_async`` for collectors to record dimensional
    samples, the only entry points cbagent collectors use today. Samples are
    POSTed as Prometheus text exposition to ``/api/v1/import/prometheus``; the
    receiver must support push-based ingestion in that format. A ``snapshot_id``
    is added as the ``job`` label on every sample so they correlate with the
    current test snapshot.

    Note: this class is *not* a drop-in PerfStore replacement. The legacy
    ``push(db, data, ts)`` shape encodes location info in an opaque ``db``
    string; Prometheus uses labels instead, so anyone needing PerfStore-style
    addressing should call ``append`` with explicit ``cluster/server/bucket/
    index/collector`` kwargs.

    Usage:
    ```
        store = PromStore(snapshot_id="abc123")
        store.append(
            data={"xdcr_lag": 12.5, "latency_get": 3.2},
            bucket="bucket-1",
            collector="xdcr_lag",
        )
    ```
    POSTs:
        ```
        perf_xdcr_lag{job="abc123",bucket="bucket-1",collector="xdcr_lag"} 12.5
        perf_latency_get{job="abc123",bucket="bucket-1",collector="xdcr_lag"} 3.2
        ```
    """

    def __init__(self, snapshot_id: str):
        self.snapshot_id = snapshot_id
        self.import_url = f"http://{CONFIG_MANAGER_HOST}/vmagent/api/v1/import/prometheus"
        self.session = requests.Session()
        self.async_session: Optional[aiohttp.ClientSession] = None

    @staticmethod
    def _sanitise_metric_name(name: str) -> str:
        """Sanitise metric name to be Prometheus-compatible.

        Prometheus metric names must match [a-zA-Z_:][a-zA-Z0-9_:]*.
        Any character outside that charset is replaced with `_`, and a leading
        digit is prefixed with `_`.
        """
        sanitised = _INVALID_METRIC_NAME_CHAR.sub("_", name)
        # Prefix with underscore if starts with a digit
        if sanitised and sanitised[0].isdigit():
            sanitised = f"_{sanitised}"
        return f"{PERF_METRICS_PREFIX}{sanitised}"

    @staticmethod
    def _sanitise_label_value(value: str) -> str:
        """Escape special characters in Prometheus label values."""
        return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")

    def _build_labels(
        self,
        cluster: Optional[str] = None,
        server: Optional[str] = None,
        bucket: Optional[str] = None,
        index: Optional[str] = None,
        collector: Optional[str] = None,
    ) -> str:
        """Build Prometheus label string from optional dimensions."""
        labels = {"job": self.snapshot_id}
        if cluster:  # Disambiguates same-named buckets/nodes across clusters (e.g. XDCR)
            labels["cluster"] = cluster
        if server:
            labels["instance"] = server  # Match the label used in other Prometheus metrics
        if bucket:
            labels["bucket"] = bucket
        if index:
            labels["index"] = index
        if collector:  # Intended for cross referencing with the legacy collectors only
            labels["collector"] = collector

        label_parts = [f'{k}="{self._sanitise_label_value(v)}"' for k, v in labels.items()]
        return "{" + ",".join(label_parts) + "}"

    def _format_metrics(
        self,
        data: dict,
        labels_str: str,
        timestamp_ms: Optional[int] = None,
    ) -> str:
        """Format metrics dict into Prometheus exposition text.

        Each key-value pair in data becomes a line: metric_name{labels} value [timestamp_ms]
        """
        lines = []
        for metric_name, value in data.items():
            sanitised_name = self._sanitise_metric_name(metric_name)
            if timestamp_ms is not None:
                lines.append(f"{sanitised_name}{labels_str} {value} {timestamp_ms}")
            else:
                lines.append(f"{sanitised_name}{labels_str} {value}")
        return "\n".join(lines) + "\n"

    def _build_body(
        self,
        data: dict,
        cluster: Optional[str] = None,
        server: Optional[str] = None,
        bucket: Optional[str] = None,
        index: Optional[str] = None,
        collector: Optional[str] = None,
        timestamp: Optional[int] = None,
    ) -> str:
        """Build the Prometheus exposition body for a set of samples.

        ``timestamp`` is epoch milliseconds and passed through unchanged; None lets
        the import endpoint stamp ingestion time.
        """
        labels_str = self._build_labels(cluster, server, bucket, index, collector)
        timestamp_ms = int(timestamp) if timestamp is not None else None
        return self._format_metrics(data, labels_str, timestamp_ms)

    def append(
        self,
        data: dict,
        cluster: Optional[str] = None,
        server: Optional[str] = None,
        bucket: Optional[str] = None,
        index: Optional[str] = None,
        collector: Optional[str] = None,
        timestamp: Optional[int] = None,
    ):
        """Push metrics to Prometheus via import endpoint.

        Args:
            data: Dict of metric_name -> value.
            cluster: Cluster name label (disambiguates clusters within one snapshot).
            server: Server hostname label.
            bucket: Bucket name label.
            index: Index name label.
            collector: Collector name label.
            timestamp: Epoch milliseconds (the unit collectors record and the
                import endpoint expects). If None, Prometheus uses current time.
        """
        body = self._build_body(data, cluster, server, bucket, index, collector, timestamp)

        try:
            resp = self.session.post(
                self.import_url,
                data=body,
                headers={"Content-Type": "text/plain"},
            )
            if resp.status_code not in (200, 204):
                logger.warning(
                    f"PromStore: push failed (HTTP {resp.status_code}): {resp.text[:200]}"
                )
        except requests.ConnectionError as e:
            logger.warning(f"PromStore: connection error pushing metrics: {e}")
        except Exception as e:
            logger.warning(f"PromStore: unexpected error pushing metrics: {e}")

    async def append_async(
        self,
        data: dict,
        cluster: Optional[str] = None,
        server: Optional[str] = None,
        bucket: Optional[str] = None,
        index: Optional[str] = None,
        collector: Optional[str] = None,
        timestamp: Optional[int] = None,
    ):
        """Async version of append(). ``timestamp`` is epoch milliseconds."""
        body = self._build_body(data, cluster, server, bucket, index, collector, timestamp)

        try:
            async with self.async_session.post(
                self.import_url,
                data=body,
                headers={"Content-Type": "text/plain"},
            ) as resp:
                if resp.status not in (200, 204):
                    text = await resp.text()
                    logger.warning(
                        f"PromStore: async push failed (HTTP {resp.status}): {text[:200]}"
                    )
        except Exception as e:
            logger.warning(f"PromStore: async push error: {e}")

    @staticmethod
    def build_dbname(
        cluster: Optional[str] = None,
        server: Optional[str] = None,
        bucket: Optional[str] = None,
        index: Optional[str] = None,
        collector: Optional[str] = None,
    ) -> str:
        """Compatibility method. Returns empty string as Prometheus uses labels, not db names."""
        return ""

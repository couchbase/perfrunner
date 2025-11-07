import os
from threading import Event, Thread
from urllib.parse import urlencode

from logger import logger
from perfrunner.helpers.rest import RestBase
from perfrunner.settings import ClusterSpec, StatsSettings


class PrometheusAgent:
    """Prometheus-based monitoring agent that communicates with the metrics system.

    The agent communicates with the metrics system service which manages Prometheus scrape targets
    registration, snapshot metadata for test runs, and background patching to keep snapshots alive.
    The agent is responsible for:
    - Registering snapshots with the metrics system
    - Patching snapshots to update their timestamp and keep them alive
    - Deleting snapshots scrape configs when the test is finished

    The metrics system supports two types of hostnames for scrape targets:
    - Service Discovery (SD) (default): This is used for Couchbase server nodes.
      For each cluster, a single node is registered to be used for service discovery.
    - Static: This is used for Sync Gateway servers and other services that don't support SD.
      For each cluster, all nodes are registered to be used as static scrape targets.
    """

    REFRESH_INTERVAL = 120  # Refresh every 2 minutes
    # Translation map for old to new metric names
    METRICS_TRANSLATION_MAP = {
        "ops": "kv_ops",
        "memcached_rss": "sysproc_memory_resident",  # Assumes the correct proc filter is applied
    }

    def __init__(self, cluster_spec: ClusterSpec, stats_settings: StatsSettings, rest: RestBase):
        self.cluster_spec = cluster_spec
        self.stats_settings = stats_settings
        self.snapshot_base_url = f"http://{stats_settings.metrics_system_host}/cm/api/v1/snapshot"
        self.rest = rest
        self.snapshot_id = None
        self.phase_name = None
        self.use_tls_ports = rest.use_tls
        # Background patching thread
        self._stop_event = Event()
        self._refresh_thread = None

        self.register_snapshot()
        self.start_background_worker()
        self.cm_current_snapshot_url = f"{self.snapshot_base_url}/{self.snapshot_id}"
        self.snapshot_url = (
            f"http://{stats_settings.metrics_system_host}/a/cbmonitor/cbmonitor"
            f"?snapshotId={self.snapshot_id}"
        )
        self.metrics_store_url = (
            f"http://{stats_settings.metrics_system_host}/api/v1/snapshots/{self.snapshot_id}"
        )
        logger.info(f"Registered Prometheus snapshot: {self.snapshot_url}")

    def __enter__(self):
        """Start a new test phase."""
        self.patch_snapshot(phase_name=self.phase_name, mode="start")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End a test phase and cleanup."""
        self.patch_snapshot(phase_name=self.phase_name, mode="end")

    def register_snapshot(self):
        """Register snapshot with config-manager."""
        try:
            # Collect all server targets as scrape configs
            scheme = "http" if not self.use_tls_ports else "https"
            payload = {
                "configs": [
                    {
                        "hostnames": list(self.cluster_spec.masters),
                        "port": 8091 if not self.use_tls_ports else 18091,
                        "scheme": scheme,
                    }
                ],
                "credentials": {
                    "username": self.cluster_spec.rest_credentials[0],
                    "password": self.cluster_spec.rest_credentials[1],
                },
                "label": os.getenv("BUILD_URL", ""),
            }

            if self.cluster_spec.sgw_servers:
                payload["configs"].append(
                    {
                        "hostnames": list(self.cluster_spec.sgw_masters),
                        "port": 4986,
                        "scheme": scheme,
                        "type": "static",
                    }
                )
            response = self.rest.post(url=self.snapshot_base_url, json=payload)
            response.raise_for_status()
            self.snapshot_id = response.json().get("id")
        except Exception as e:
            logger.warning(f"Could not connect to config-manager: {e}")

    def patch_snapshot(
        self, phase_name: str = None, mode: str = "start", services: list[str] = None
    ):
        """Patch snapshot to update its timestamp and keep it alive."""
        if not self.snapshot_id:
            return

        try:
            payload = {}
            if phase_name:
                # If we have a phase name, we are registering the start or end of a test phase.
                payload = {"phase": phase_name, "mode": mode}

            if services:
                payload["services"] = services
            response = self.rest.patch(url=self.cm_current_snapshot_url, json=payload)
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Could not patch snapshot: {e}")

    def delete_snapshot(self):
        """Delete snapshot from config-manager."""
        if not self.snapshot_id:
            return

        logger.info(f"Removing Scrape target for snapshot: {self.snapshot_url}")
        try:
            response = self.rest.delete(url=self.cm_current_snapshot_url)
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Could not delete snapshot: {e}")

    def start_background_worker(self):
        """Start background thread that periodically patches the snapshot."""
        if not self.snapshot_id:
            return

        def refresh_loop():
            """Background thread that refreshes snapshot."""
            while not self._stop_event.is_set():
                # Wait for an interval or until stop event is set
                if self._stop_event.wait(timeout=self.REFRESH_INTERVAL):
                    break  # Stop event was set
                self.patch_snapshot()

        self._refresh_thread = Thread(target=refresh_loop, daemon=True)
        self._refresh_thread.start()

    def stop_background_worker(self):
        """Stop the background patching thread."""
        if self._refresh_thread and self._refresh_thread.is_alive():
            self._stop_event.set()
            self._refresh_thread.join(timeout=5)

    def set_phase(self, phase_name: str):
        """Set the current test phase name for context."""
        self.phase_name = phase_name

    def add_service(self, services: list[str]):
        """Add a service to the snapshot metadata."""
        self.patch_snapshot(services=services)

    # Store functions - Assume the store is available on the last phase data for the test
    def build_dbname(self, *args, **kwargs):
        # We dont use this in the new store, here for compatibility with the old store
        return ""

    def get_values(self, db: str, metric: str, filters: dict = None) -> list[float]:
        """Get metric values for the last phase run.

        This is used to get the metric values for the last phase run.
        This is intended to be used for compatibility with the old store only
        """
        metric_name = self.METRICS_TRANSLATION_MAP.get(metric, metric)
        phase_url = self._get_phase_url(metric_name, filters)

        response = self.rest.get(url=phase_url)
        response.raise_for_status()
        values = response.json().get("values") or []
        return [float(value.get("value", "0")) for value in values]

    def get_summary(
        self, metric: str, percentiles: list[float] = None, filters: dict = None
    ) -> dict:
        """Get summary of metric values for the last phase run.

        Summary provides a convenient way to get statistical information about the metric values.
        """
        metric_name = self.METRICS_TRANSLATION_MAP.get(metric, metric)
        summary_url = self._get_phase_url(
            metric_name, filters, summary=True, percentiles=percentiles
        )
        response = self.rest.get(url=summary_url)
        response.raise_for_status()
        summary = response.json()
        logger.info(f"Summary: {summary}")
        return summary.get("summary", {})

    def _get_phase_url(
        self,
        metric_name: str,
        filters: dict = None,
        summary: bool = False,
        percentiles: list[float] = None,
    ) -> str:
        query_str = ""
        phase_url = f"{self.metrics_store_url}/metrics/{metric_name}/phases/{self.phase_name}"
        if filters:
            query_str = f"?{urlencode(filters)}"

        if summary:
            summary_url = f"{phase_url}/summary{query_str}"
            if percentiles:
                # Percentile parameters are only used in the summary endpoint and
                # are expected to be in the range of 0 to 1.
                percentiles_decimal = [str(float(p) / 100) for p in percentiles]
                query_str += f"&percentiles={','.join(percentiles_decimal)}"
            return summary_url
        return phase_url + query_str

    def exists(self, db: str, metric: str) -> bool:
        return True  # Always return True for Prometheus metrics

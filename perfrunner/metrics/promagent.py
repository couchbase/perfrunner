import os
import time
from datetime import datetime
from multiprocessing import Process
from threading import Event, Thread
from typing import Optional
from urllib.parse import urlencode

from cbagent.promstore import PromStore
from logger import logger
from perfrunner.helpers.rest import RestBase
from perfrunner.settings import (
    CBMONITOR2_HOST,
    CONFIG_MANAGER_HOST,
    ClusterSpec,
    StatsSettings,
)


class PrometheusAgent:
    """Prometheus-based monitoring agent that communicates with the metrics system.

    The agent communicates with the config-manager, which manages Prometheus scrape targets
    registration, snapshot metadata for test runs, and  keepalive logic.
    It is responsible for:
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
    SNAPSHOT_REGISTER_ATTEMPTS = 5  # Number of attempts to register a snapshot
    SNAPSHOT_REGISTER_BACKOFF = 10  # seconds between retries
    # Ingestion wait: the metrics store ingests asynchronously, so reading right
    # after a reconstruct-push returns partial data (wrong percentiles). After
    # pushing, poll the pushed metrics until their sample count settles before the
    # test reports KPIs.
    INGESTION_POLL_INTERVAL = 5  # seconds between ingestion polls
    INGESTION_POLL_TIMEOUT = 120  # give up waiting after this many seconds
    INGESTION_STABLE_POLLS = 2  # consecutive unchanged counts => ingested

    def __init__(self, cluster_spec: ClusterSpec, stats_settings: StatsSettings, rest: RestBase):
        self.cluster_spec = cluster_spec
        self.stats_settings = stats_settings
        # query_range step for phase reads. The server default (~15s) collapses
        # high-frequency pushed samples (e.g. spring per-op latency) to a handful of
        # points, making percentiles meaningless (p50 == p99.9). Read at the finest
        # configured collection cadence so we don't undersample (e.g. lat_interval=0.5
        # tests push every 500ms). NOTE: this is still stepped (carry-forward) data,
        # not the raw distribution. True percentiles will be implemented in the future.
        self.read_step = self._compute_read_step(stats_settings)
        # Snapshot register/patch/delete go to the config-manager.
        self.snapshot_base_url = f"http://{CONFIG_MANAGER_HOST}/config-manager/api/v1/snapshot"
        self.rest = rest
        self.snapshot_id = None
        self.phase_name = None
        self.use_tls_ports = rest.use_tls
        # Background patching thread
        self._stop_event = Event()
        self._refresh_thread = None

        # Custom collectors infrastructure (for metrics not available via Prometheus scraping)
        self.custom_collectors = []
        self.custom_processes = []

        # register_snapshot raises if all retries fail, the partially-built agent never escapes.
        self.register_snapshot()

        self.cm_current_snapshot_url = f"{self.snapshot_base_url}/{self.snapshot_id}"
        self.snapshot_url = (
            f"https://{CBMONITOR2_HOST}/a/cbmonitor/snapshots/{self.snapshot_id}"
        )
        self.metrics_store_url = f"https://{CBMONITOR2_HOST}/api/v1/snapshots/{self.snapshot_id}"

        # Initialise PromStore for push-based collectors
        self.prom_store = PromStore(snapshot_id=self.snapshot_id)

        self.start_background_worker()
        logger.info(f"Registered Prometheus snapshot: {self.snapshot_url}")

    def __enter__(self):
        """Start a new test phase."""
        self.patch_snapshot(phase_name=self.phase_name, mode="start")
        self.start_custom_collectors()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """End a test phase and cleanup custom collectors."""
        self.stop_custom_collectors()
        self.reconstruct_custom_collectors()
        self.patch_snapshot(phase_name=self.phase_name, mode="end")
        # Block until pushed samples are queryable so the test's report_kpi (which
        # reads right after this) doesn't compute KPIs over partially-ingested data.
        self._wait_for_ingestion()

    # The agent lives on the test object, so it is pickled whenever a test pickles
    # ``self`` into worker subprocesses (e.g. multiprocessing.Pool.map over a bound
    # method, as in SyncGateway's parallel replicate). Drop the unpicklable runtime
    # state, background thread/event, live collector processes, and HTTP sessions
    # (PromStore/collectors). Worker subprocesses don't collect stats, so the copy
    # they receive is intentionally inert; the original in the main process keeps all
    # its state (pickling reads via __getstate__ but doesn't mutate the source).
    _UNPICKLABLE_ATTRS = (
        "_stop_event",
        "_refresh_thread",
        "custom_processes",
        "custom_collectors",
        "prom_store",
    )

    def __getstate__(self):
        return {k: v for k, v in self.__dict__.items() if k not in self._UNPICKLABLE_ATTRS}

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Inert defaults so a pickled-into-a-worker agent doesn't raise if touched.
        self._stop_event = None
        self._refresh_thread = None
        self.custom_processes = []
        self.custom_collectors = []
        self.prom_store = None

    def register_snapshot(self):
        """Register snapshot with config-manager. Retries on failure; raises on exhaustion."""
        scheme = "http" if not self.use_tls_ports else "https"
        payload = {
            "configs": [
                {
                    "hostnames": list(self.cluster_spec.masters),
                    "port": 8091 if not self.use_tls_ports else 18091,
                    "scheme": scheme,
                    "use_alt_addresses": self.cluster_spec.using_private_cluster_ips,
                }
            ],
            "credentials": {
                "username": self.cluster_spec.rest_credentials[0],
                "password": self.cluster_spec.rest_credentials[1],
            },
            "label": os.getenv("BUILD_URL", ""),
        }
        if self.cluster_spec.sgw_servers:
            payload["configs"].append({
                "hostnames": list(self.cluster_spec.sgw_servers),
                "port": 4986,
                "scheme": scheme,
                "type": "static",
                "product": "syncgateway",
            })

        last_exc = None
        for attempt in range(1, self.SNAPSHOT_REGISTER_ATTEMPTS + 1):
            try:
                response = self.rest.post(url=self.snapshot_base_url, json=payload)
                response.raise_for_status()
                self.snapshot_id = response.json().get("id")
                if not self.snapshot_id:
                    raise RuntimeError("config-manager returned no snapshot id")
                return
            except Exception as e:
                last_exc = e
                logger.warning(
                    f"register_snapshot attempt {attempt}/"
                    f"{self.SNAPSHOT_REGISTER_ATTEMPTS} failed: {e}"
                )
                if attempt < self.SNAPSHOT_REGISTER_ATTEMPTS:
                    time.sleep(self.SNAPSHOT_REGISTER_BACKOFF)

        raise RuntimeError(
            f"Could not register Prometheus snapshot after "
            f"{self.SNAPSHOT_REGISTER_ATTEMPTS} attempts"
        ) from last_exc

    def patch_snapshot(
        self,
        phase_name: Optional[str] = None,
        mode: str = "start",
        services: Optional[list[str]] = None,
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

    @classmethod
    def _compute_read_step(cls, stats_settings: StatsSettings) -> str:
        """Return the query_range step as a Prometheus duration string.

        Uses the finest configured collection cadence so reads don't drop samples.
        Prometheus steps must be integer durations, so sub-second cadences are expressed in ms.
        """
        cadence_s = min(stats_settings.interval, stats_settings.lat_interval)
        step_ms = max(int(cadence_s * 1000), 100)
        return f"{step_ms}ms"

    def set_phase(self, phase_name: str):
        """Set the current test phase name for context."""
        self.phase_name = phase_name

    def add_service(self, services: list[str]):
        """Add a service to the snapshot metadata."""
        self.patch_snapshot(services=services)

    # Store functions: Assume the store is available on the last phase data for the test
    def build_dbname(self, *args, **kwargs):
        # We dont use this in the new store, here for compatibility with the old store
        return ""

    def _resolve_metric_name(self, metric: str) -> str:
        """Resolve a caller-supplied metric name to its name in the metrics-store.

        Applies the sanitize/``perf_``-prefix that PromStore owns — the same
        transformation it applies when pushing, so reads line up with writes.
        Every cbagent metric is pushed under its native name, so reads use that
        native name too; a future caller-level flag may opt specific (scraped)
        reads out of the prefix.
        """
        return PromStore._sanitise_metric_name(metric)

    def get_values(self, db: str, metric: str, filters: Optional[dict] = None) -> list[float]:
        """Get metric values for the last phase run.

        This is used to get the metric values for the last phase run.
        This is intended to be used for compatibility with the old store only
        """
        metric_name = self._resolve_metric_name(metric)
        phase_url = self._get_phase_url(metric_name, filters)

        response = self.rest.get(url=phase_url)
        response.raise_for_status()
        values = response.json().get("values") or []
        # ``value`` may be missing or an explicit null (gap/staleness point); treat as 0.
        return [float(value.get("value") or 0) for value in values]

    @staticmethod
    def _iso_to_epoch_ms(ts: str) -> int:
        """Convert an ISO-8601 UTC timestamp (e.g. '2025-11-18T15:00:20Z') to epoch ms.

        ``datetime.fromisoformat`` on Python 3.9 doesn't accept a trailing ``Z``,
        so normalise it to an explicit UTC offset first.
        """
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)

    def get_timeseries(self, db: str, metric: str, filters: Optional[dict] = None) -> list[list]:
        """Return ``[[timestamp_ms, value], ...]`` for a metric over the last phase.

        The metrics-store returns each sample as ``{"time": <ISO-8601>, "value": <num>}``
        (cbmonitor2 snapshot API). ``time`` is converted to epoch milliseconds to match
        the timestamp windowing done by the callers in ``metrics.py``.
        """
        metric_name = self._resolve_metric_name(metric)
        phase_url = self._get_phase_url(metric_name, filters)

        response = self.rest.get(url=phase_url)
        response.raise_for_status()
        values = response.json().get("values") or []
        return [
            [self._iso_to_epoch_ms(entry["time"]), float(entry.get("value") or 0)]
            for entry in values
            if entry.get("time") is not None
        ]

    def bulk_get_timeseries_merged(self, dbs: list[str], metric: str) -> list[list]:
        """Return a timestamp-sorted ``[[timestamp_ms, value], ...]`` for a metric.

        ``db`` carries no addressing in the Prometheus store (``build_dbname`` returns
        ``""``) and the phase query already spans every bucket/series for the metric,
        so a single fetch returns all samples — fetching once per ``db`` would just
        duplicate identical data. Mirrors ``PerfStore.bulk_get_timeseries_merged``.
        """
        if not dbs:
            return []
        return sorted(self.get_timeseries(dbs[0], metric), key=lambda x: x[0])

    def get_summary(
        self, metric: str, percentiles: Optional[list[float]] = None, filters: Optional[dict] = None
    ) -> dict:
        """Get summary of metric values for the last phase run.

        Summary provides a convenient way to get statistical information about the metric values.
        """
        metric_name = self._resolve_metric_name(metric)
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
        filters: Optional[dict] = None,
        summary: bool = False,
        percentiles: Optional[list[float]] = None,
    ) -> str:
        phase_url = f"{self.metrics_store_url}/metrics/{metric_name}/phases/{self.phase_name}"
        params = dict(filters) if filters else {}
        # Request finer resolution than the server default so percentiles aren't computed over a
        # handful of coarsely-stepped points. A caller-supplied ``step`` in ``filters`` still wins.
        params.setdefault("step", self.read_step)

        summary_path = ""
        if summary:
            summary_path = "/summary"
            if percentiles:
                # Percentile parameters are only used in the summary endpoint and
                # are expected to be in the range of 0 to 1.
                percentiles_decimal = [str(float(p) / 100) for p in percentiles]
                params["percentiles"] = ",".join(percentiles_decimal)
        query_str = f"?{urlencode(params)}" if params else ""
        return f"{phase_url}{summary_path}{query_str}"

    def exists(self, db: str, metric: str) -> bool:
        return True  # Always return True for Prometheus metrics

    # ---- Custom Collectors Management ----
    # These methods manage collectors for metrics not available via Prometheus scraping
    # (e.g. xdcr_lag, kv/query latency, observe latency, secondary latency, etc.)
    # Collectors push in Prometheus exposition format.
    #
    # Collectors opt-in by setting the class attribute PROMETHEUS_CUSTOM = True.
    # Activation uses the same ``should_collect()`` / ``COLLECTOR_FLAG`` mechanism
    # as the classic (CbAgent) path, so environment guards are applied consistently.

    def add_custom_collectors(self, test):
        """Add custom collectors for metrics not available via Prometheus scraping.

        Collectors that declare ``PROMETHEUS_CUSTOM = True`` and pass
        ``should_collect()`` are instantiated per cluster via ``create_instances()``
        (the same factory as the classic path) and attached to the shared
        PromStore. Activation flags are read from ``test.COLLECTORS`` inside
        ``get_active_prometheus_collectors``.

        Args:
            test: The PerfTest instance.
        """
        # Imported lazily: cbagent.registry imports perfrunner.tests, which imports this
        # module, a top-level import here would be circular.
        from cbagent.registry import CollectorRegistry

        self.custom_collectors = CollectorRegistry().get_active_prometheus_collectors(test)
        for collector in self.custom_collectors:
            collector.store = self.prom_store

        if self.custom_collectors:
            logger.info(
                f"Added {len(self.custom_collectors)} custom collector(s) "
                f"pushing to Prometheus: "
                f"{[c.__class__.__name__ for c in self.custom_collectors]}"
            )

    def start_custom_collectors(self):
        """Start custom collectors as separate processes."""
        if not self.custom_collectors:
            return

        logger.info(f"Starting {len(self.custom_collectors)} custom collector(s)")
        self.custom_processes = [Process(target=c.collect) for c in self.custom_collectors]
        for p in self.custom_processes:
            p.start()

    def stop_custom_collectors(self):
        """Terminate custom collector processes."""
        if not self.custom_processes:
            return

        logger.info(f"Stopping {len(self.custom_processes)} custom collector(s)")
        for p in self.custom_processes:
            p.terminate()
        self.custom_processes = []

    def reconstruct_custom_collectors(self):
        """Run post-collection reconstruction."""
        for collector in self.custom_collectors:
            if hasattr(collector, "reconstruct"):
                logger.info(f"Reconstructing {collector.__class__.__name__}")
                collector.reconstruct()

    def _pushed_metric_names(self) -> list[str]:
        """Metric names declared by the active custom collectors (deduped)."""
        names, seen = [], set()
        for collector in self.custom_collectors:
            metrics = getattr(collector, "METRICS", ()) or ()
            if isinstance(metrics, str):
                metrics = (metrics,)
            for metric in metrics:
                if metric not in seen:
                    seen.add(metric)
                    names.append(metric)
        return names

    def _count_ingested_samples(self, metrics: list[str]) -> int:
        """Total sample count currently queryable for the given metrics in this phase."""
        total = 0
        for metric in metrics:
            try:
                total += len(self.get_values(db="", metric=metric))
            except Exception as e:
                logger.warning(f"Ingestion poll failed for {metric}: {e}")
        return total

    def _wait_for_ingestion(self):
        """Poll pushed metrics until their queryable sample count stops growing.

        The metrics store ingests asynchronously, so a read immediately after
        reconstruct returns partial data. Wait until the count is unchanged for
        INGESTION_STABLE_POLLS consecutive polls (or a timeout) so KPIs are computed
        over the full data. Only relevant when custom collectors pushed samples.
        """
        metrics = self._pushed_metric_names() if self.custom_collectors else []
        if not metrics:
            return

        logger.info("Waiting for pushed metrics to be ingested before reporting")
        deadline = time.time() + self.INGESTION_POLL_TIMEOUT
        last_count, stable = -1, 0
        while time.time() < deadline:
            count = self._count_ingested_samples(metrics)
            # Require count > 0: an unchanged count of 0 means nothing has been
            # ingested yet, not that ingestion has settled, keep waiting.
            if count > 0 and count == last_count:
                stable += 1
                if stable >= self.INGESTION_STABLE_POLLS:
                    logger.info(f"Ingestion settled at {count} sample(s)")
                    return
            else:
                stable = 0
            last_count = count
            time.sleep(self.INGESTION_POLL_INTERVAL)

        logger.warning(
            f"Ingestion did not settle within {self.INGESTION_POLL_TIMEOUT}s "
            f"(last count={last_count}); reported KPIs may be based on partial data"
        )

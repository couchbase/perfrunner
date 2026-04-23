import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import requests
from capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIDedicated

from logger import logger
from perfrunner.settings import CapellaWebhookSettings
from perfrunner.tests import PerfTest


class WebhookTestBase(PerfTest):
    """Base class for webhook performance tests."""

    MAX_API_RETRIES = 6
    MAX_RETRY_BACKOFF_S = 30
    MAX_INTEGRATION_SLOT_RETRIES = 6
    DELETE_CLEANUP_TIMEOUT_S = 20
    DELETE_CLEANUP_INTERVAL_S = 0.5

    def _get_optional_webhook_auth_token(self, settings: CapellaWebhookSettings) -> str:
        return settings.auth_token or os.getenv("WEBHOOK_AUTH_TOKEN", "")

    def _get_webhook_auth_token(self, settings: CapellaWebhookSettings) -> str:
        token = self._get_optional_webhook_auth_token(settings)
        if not token:
            logger.interrupt("Set webhook_auth_token in test config or WEBHOOK_AUTH_TOKEN env")
        return token

    def _get_capella_client(self) -> CapellaAPIDedicated:
        return CapellaAPIDedicated(
            self.cluster_spec.controlplane_settings["public_api_url"],
            None,
            None,
            os.getenv("CBC_USER", "team-performance@couchbase.com"),
            os.getenv("CBC_PWD"),
        )

    def _get_cluster_id(self) -> str:
        cluster_ids = self.cluster_spec.capella_cluster_ids
        if not cluster_ids:
            logger.interrupt("No Capella cluster IDs found in cluster spec")
        return cluster_ids[0]

    def _get_webhook_settings(self) -> CapellaWebhookSettings:
        return self.test_config.capella_webhook_settings

    def _get_support_token(self) -> str:
        return os.getenv("CBC_TOKEN_FOR_INTERNAL_SUPPORT", "")

    def _get_cloud_url(self, capella_client: CapellaAPIDedicated) -> str:
        return capella_client.internal_url

    def _alert_integrations_base_url(self) -> str:
        cp = self.cluster_spec.controlplane_settings
        return (
            f"{cp['public_api_url']}/v4/organizations/{cp['org']}"
            f"/projects/{cp['project']}/alertIntegrations"
        )

    def _alert_integration_test_url(self) -> str:
        base = self._alert_integrations_base_url()
        return f"{base[:-1]}Test" if base.endswith("s") else f"{base}Test"

    def _init_v4_token_pool(self):
        if hasattr(self, "_v4_tokens"):
            return

        tokens = []
        for env_name in ("CBC_V4_API_TOKENS", "CBC_SECRET_KEYS"):
            raw = os.getenv(env_name, "").strip()
            if raw:
                tokens = [token.strip() for token in raw.split(",") if token.strip()]
                if tokens:
                    break

        if not tokens:
            single_token = os.getenv("CBC_V4_API_TOKEN") or os.getenv("CBC_SECRET_KEY")
            if single_token:
                tokens = [single_token]

        if not tokens:
            logger.interrupt(
                "Set CBC_V4_API_TOKENS (comma-separated) or CBC_V4_API_TOKEN/CBC_SECRET_KEY"
            )

        self._v4_tokens = tokens
        self._v4_token_idx = 0
        self._v4_token_lock = Lock()

        if len(tokens) > 1:
            logger.info(f"Using {len(tokens)} v4 API tokens in round-robin mode")

    def _next_v4_token(self) -> str:
        self._init_v4_token_pool()
        with self._v4_token_lock:
            token = self._v4_tokens[self._v4_token_idx % len(self._v4_tokens)]
            self._v4_token_idx += 1
        return token

    def _v4_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._next_v4_token()}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _retry_after_seconds(
        resp: requests.Response, attempt: int, max_backoff: float
    ) -> float:
        retry_after = resp.headers.get("retryAfter") or resp.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.1, float(retry_after))
            except ValueError:
                pass
        return min(2 ** attempt, max_backoff)

    def _request_with_retry(
        self,
        method: str,
        url: str,
        payload: Optional[dict[str, Any]] = None,
        timeout: int = 60,
    ) -> requests.Response:
        for attempt in range(self.MAX_API_RETRIES + 1):
            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    json=payload,
                    headers=self._v4_headers(),
                    timeout=timeout,
                    verify=False,
                )
            except requests.RequestException as err:
                if attempt >= self.MAX_API_RETRIES:
                    raise
                wait_s = min(2 ** attempt, self.MAX_RETRY_BACKOFF_S)
                logger.warn(
                    f"Request error on {method} {url}: {err}. "
                    f"Retrying in {wait_s}s ({attempt + 1}/{self.MAX_API_RETRIES})"
                )
                time.sleep(wait_s)
                continue

            if resp.status_code not in (429, 500, 502, 503, 504):
                return resp

            if attempt >= self.MAX_API_RETRIES:
                return resp

            wait_s = self._retry_after_seconds(
                resp=resp, attempt=attempt, max_backoff=self.MAX_RETRY_BACKOFF_S
            )
            logger.warn(
                f"Received {resp.status_code} on {method} {url}. "
                f"Retrying in {wait_s}s ({attempt + 1}/{self.MAX_API_RETRIES})"
            )
            time.sleep(wait_s)

    def _create_alert_integration(
        self, base_url: str, payload: dict[str, Any]
    ) -> requests.Response:
        return self._request_with_retry("POST", base_url, payload=payload, timeout=60)

    def _update_alert_integration(
        self, base_url: str, webhook_id: str, payload: dict[str, Any]
    ) -> requests.Response:
        return self._request_with_retry(
            "PUT",
            f"{base_url}/{webhook_id}",
            payload=payload,
            timeout=60,
        )

    def _delete_alert_integration(self, base_url: str, webhook_id: str) -> requests.Response:
        return self._request_with_retry(
            "DELETE",
            f"{base_url}/{webhook_id}",
            timeout=60,
        )

    def _get_alert_integration(self, base_url: str, webhook_id: str) -> requests.Response:
        return self._request_with_retry(
            "GET",
            f"{base_url}/{webhook_id}",
            timeout=30,
        )

    def _list_alert_integrations(self, base_url: str) -> list[dict[str, Any]]:
        resp = self._request_with_retry("GET", base_url, timeout=30)
        if resp.status_code != 200:
            logger.error(f"Failed to list alert integrations: {resp.status_code} {resp.text}")
            return []

        try:
            payload = resp.json()
        except ValueError:
            logger.error("Failed to parse alert integrations list response")
            return []

        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("data", [])
        return []

    def _cleanup_perf_integrations(self, base_url: str):
        for cleanup_pass in range(3):
            integrations = self._list_alert_integrations(base_url)
            target_ids = [
                integration.get("id")
                for integration in integrations
                if integration.get("id")
                and (integration.get("name") or "").startswith("perf")
            ]
            if not target_ids:
                return

            logger.info(
                f"Cleanup pass {cleanup_pass + 1}: deleting {len(target_ids)} "
                "leftover perf integrations"
            )
            for integration_id in target_ids:
                resp = self._delete_alert_integration(base_url, integration_id)
                if resp.status_code not in (200, 204, 404):
                    logger.warn(
                        f"Cleanup failed for integration {integration_id}: "
                        f"{resp.status_code} {resp.text}"
                    )

            time.sleep(self.DELETE_CLEANUP_INTERVAL_S)

    def _ensure_integration_deleted(self, base_url: str, webhook_id: str) -> bool:
        deadline = time.time() + self.DELETE_CLEANUP_TIMEOUT_S
        while time.time() < deadline:
            resp = self._get_alert_integration(base_url, webhook_id)
            if resp.status_code == 404:
                return True

            delete_resp = self._delete_alert_integration(base_url, webhook_id)
            if delete_resp.status_code in (200, 204, 404):
                time.sleep(self.DELETE_CLEANUP_INTERVAL_S)
                continue

            logger.warn(
                f"Delete retry failed for {webhook_id}: "
                f"{delete_resp.status_code} {delete_resp.text}"
            )
            time.sleep(self.DELETE_CLEANUP_INTERVAL_S)

        return False

    def _build_alert_integration_payload(
        self, name: str, webhook_url: str, settings: CapellaWebhookSettings
    ) -> dict[str, Any]:
        payload = self._build_alert_integration_update_payload(name, webhook_url, settings)
        payload["kind"] = "webhook"
        return payload

    def _build_alert_integration_update_payload(
        self, name: str, webhook_url: str, settings: CapellaWebhookSettings
    ) -> dict[str, Any]:
        payload = {
            "name": name,
            "config": {
                "webhook": {
                    "method": "POST",
                    "url": webhook_url,
                }
            },
        }
        if token := self._get_optional_webhook_auth_token(settings):
            payload["config"]["webhook"]["token"] = token

        return payload

    def _load_test_plan(self, settings: CapellaWebhookSettings) -> Optional[Any]:
        """Load the metric simulator test plan JSON from file path or setting."""
        test_plan_path = settings.test_plan
        if test_plan_path and os.path.isfile(test_plan_path):
            with open(test_plan_path) as f:
                return json.load(f)
        return None

    @staticmethod
    def _normalize_epoch_seconds(ts: float) -> float:
        if ts > 1e18:
            return ts / 1e9
        if ts > 1e15:
            return ts / 1e6
        if ts > 1e12:
            return ts / 1e3
        return ts

    @staticmethod
    def _is_plausible_ts(ts: float, ref_ts: float) -> bool:
        return (ref_ts - 30 * 24 * 3600) <= ts <= (ref_ts + 3600)

    @classmethod
    def _parse_event_ts(cls, value: Any) -> Optional[float]:
        if value in (None, "", "0001-01-01T00:00:00Z"):
            return None

        if isinstance(value, (int, float)):
            return cls._normalize_epoch_seconds(float(value))

        if not isinstance(value, str):
            return None

        s = value.strip()
        if not s:
            return None

        if re.fullmatch(r"\d+(\.\d+)?", s):
            return cls._normalize_epoch_seconds(float(s))

        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        if "+" not in s[10:] and "-" not in s[10:]:
            s += "+00:00"

        m = re.match(r"^(.*?\.)(\d+)([+-]\d{2}:\d{2})$", s)
        if m:
            frac = m.group(2)[:6].ljust(6, "0")
            s = f"{m.group(1)}{frac}{m.group(3)}"

        try:
            return datetime.fromisoformat(s).timestamp()
        except (ValueError, TypeError):
            return None

    def _extract_event_ts_from_delivery(self, delivery: dict[str, Any]) -> Optional[float]:
        payload = delivery.get("payload", {})
        if not isinstance(payload, (dict, list)):
            return None

        ref_ts = delivery.get("received_at", time.time())
        target_keys = {
            "detectedat",
            "createdat",
            "timestamp",
            "eventtimestamp",
            "firedat",
            "triggeredat",
            "sentat",
            "occurredat",
            "eventtime",
            "time",
            "ts",
        }

        stack = [payload]
        candidates = []

        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                for k, v in node.items():
                    if isinstance(v, (dict, list)):
                        stack.append(v)

                    if isinstance(k, str):
                        k_norm = re.sub(r"[^a-z0-9]", "", k.lower())
                        if k_norm in target_keys or k_norm.endswith("timestamp"):
                            ts = self._parse_event_ts(v)
                            if ts is not None and self._is_plausible_ts(ts, ref_ts):
                                candidates.append(ts)
            elif isinstance(node, list):
                stack.extend(node)

        return max(candidates) if candidates else None

    def _enable_alert_simulation(
        self, capella_client: CapellaAPIDedicated, cluster_id: str, test_plan: Any
    ) -> bool:
        """Start alert simulation via the metric simulator.

        POST {{cloud_url}}/internal/support/clusters/{cluster_id}/enable-test-observer
        Authorization: Internal support token
        Body: dp-testplan.json
        """
        cloud_url = self._get_cloud_url(capella_client)
        url = f"{cloud_url}/internal/support/clusters/{cluster_id}/enable-test-observer"
        token = self._get_support_token()

        resp = requests.post(
            url,
            json=test_plan,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=30,
            verify=False,
        )
        if resp.status_code not in (200, 201, 202, 204):
            logger.error(
                f"Failed to enable alert simulation: {resp.status_code} {resp.text}"
            )
            return False
        logger.info("Alert simulation enabled successfully")
        return True

    def _disable_alert_simulation(
        self, capella_client: CapellaAPIDedicated, cluster_id: str
    ) -> bool:
        """Stop alert simulation via the metric simulator.

        POST {{cloud_url}}/internal/support/clusters/{cluster_id}/disable-test-observer
        Authorization: Internal support token
        """
        cloud_url = self._get_cloud_url(capella_client)
        url = f"{cloud_url}/internal/support/clusters/{cluster_id}/disable-test-observer"
        token = self._get_support_token()

        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
            verify=False,
        )
        if resp.status_code not in (200, 201, 202, 204):
            logger.error(
                f"Failed to disable alert simulation: {resp.status_code} {resp.text}"
            )
            return False
        logger.info("Alert simulation disabled successfully")
        return True


class WebhookConfigApplyLatencyTest(WebhookTestBase):
    """Test A1: Measure latency of webhook configuration CRUD operations.

    Calls the Capella webhook management API (create/update/delete) and
    measures the time until the configuration change takes effect.
    """

    OPERATIONS = ("create", "update", "delete")

    def _wait_for_condition(
        self, condition: Callable[[], bool], timeout_s: float, poll_interval_s: float
    ) -> bool:
        deadline = time.time() + timeout_s
        while True:
            now = time.time()
            if now >= deadline:
                return False
            try:
                if condition():
                    return True
            except requests.RequestException:
                pass

            remaining = deadline - time.time()
            sleep_time = min(poll_interval_s, max(0, remaining))
            if sleep_time > 0:
                time.sleep(sleep_time)

    @staticmethod
    def _extract_webhook_url(response_json: dict[str, Any]) -> Optional[str]:
        webhook_config = response_json.get("config", {}).get("webhook", {})
        if not webhook_config:
            webhook_config = response_json.get("webhook", {})
        return webhook_config.get("url")

    def _wait_for_create_or_update_effect(
        self,
        base_url: str,
        webhook_id: str,
        expected_name: str,
        expected_url: str,
        timeout_s: float,
        poll_interval_s: float,
    ) -> bool:
        def _condition():
            resp = self._get_alert_integration(base_url, webhook_id)
            if resp.status_code != 200:
                return False
            response_json = resp.json()
            name_matches = response_json.get("name") == expected_name
            url_matches = self._extract_webhook_url(response_json) == expected_url
            return name_matches and url_matches

        return self._wait_for_condition(_condition, timeout_s, poll_interval_s)

    def _wait_for_delete_effect(
        self, base_url: str, webhook_id: str, timeout_s: float, poll_interval_s: float
    ) -> bool:
        def _condition():
            resp = self._get_alert_integration(base_url, webhook_id)
            return resp.status_code == 404

        return self._wait_for_condition(_condition, timeout_s, poll_interval_s)

    def _create_webhook_config(
        self,
        base_url: str,
        payload: dict[str, Any],
        verify_timeout_s: float,
        verify_interval_s: float,
    ) -> tuple[Optional[str], float, bool]:
        t0 = time.time()
        resp = self._create_alert_integration(base_url, payload)
        retries = 0
        while (
            resp.status_code == 422
            and "maximum number of alert integrations" in resp.text
            and retries < self.MAX_INTEGRATION_SLOT_RETRIES
        ):
            wait_s = min(0.5 * (2 ** retries), 5)
            logger.warn(
                "Maximum alert integration limit reached during create; "
                f"retrying in {wait_s}s ({retries + 1}/{self.MAX_INTEGRATION_SLOT_RETRIES})"
            )
            time.sleep(wait_s)
            resp = self._create_alert_integration(base_url, payload)
            retries += 1

        if resp.status_code not in (200, 201):
            latency = time.time() - t0
            logger.error(f"Failed to create webhook config: {resp.status_code} {resp.text}")
            return None, latency, False

        webhook_id = resp.json().get("id")
        if not webhook_id:
            latency = time.time() - t0
            logger.error("Create webhook config did not return an ID")
            return None, latency, False

        effective = self._wait_for_create_or_update_effect(
            base_url=base_url,
            webhook_id=webhook_id,
            expected_name=payload["name"],
            expected_url=payload["config"]["webhook"]["url"],
            timeout_s=verify_timeout_s,
            poll_interval_s=verify_interval_s,
        )
        latency = time.time() - t0
        if not effective:
            logger.error(f"Create webhook config not effective before timeout for id={webhook_id}")

        return webhook_id, latency, effective

    def _update_webhook_config(
        self,
        base_url: str,
        webhook_id: str,
        payload: dict[str, Any],
        verify_timeout_s: float,
        verify_interval_s: float,
    ) -> tuple[float, bool]:
        t0 = time.time()
        resp = self._update_alert_integration(base_url, webhook_id, payload)
        if resp.status_code not in (200, 204):
            latency = time.time() - t0
            logger.error(f"Failed to update webhook config: {resp.status_code} {resp.text}")
            return latency, False

        effective = self._wait_for_create_or_update_effect(
            base_url=base_url,
            webhook_id=webhook_id,
            expected_name=payload["name"],
            expected_url=payload["config"]["webhook"]["url"],
            timeout_s=verify_timeout_s,
            poll_interval_s=verify_interval_s,
        )
        latency = time.time() - t0
        if not effective:
            logger.error(f"Update webhook config not effective before timeout for id={webhook_id}")

        return latency, effective

    def _delete_webhook_config(
        self,
        base_url: str,
        webhook_id: str,
        verify_timeout_s: float,
        verify_interval_s: float,
    ) -> tuple[float, bool]:
        t0 = time.time()
        resp = self._delete_alert_integration(base_url, webhook_id)
        if resp.status_code not in (200, 204, 404):
            latency = time.time() - t0
            logger.error(f"Failed to delete webhook config: {resp.status_code} {resp.text}")
            return latency, False

        deleted = self._wait_for_delete_effect(
            base_url=base_url,
            webhook_id=webhook_id,
            timeout_s=verify_timeout_s,
            poll_interval_s=verify_interval_s,
        )
        latency = time.time() - t0
        if not deleted:
            deleted = self._ensure_integration_deleted(base_url, webhook_id)
            latency = time.time() - t0
        if not deleted:
            logger.error(f"Delete webhook config not effective before timeout for id={webhook_id}")

        return latency, deleted

    def _init_results(self) -> dict[str, dict[str, Any]]:
        return {op: {"latencies": [], "success": 0, "total": 0} for op in self.OPERATIONS}

    @staticmethod
    def _record_result(
        results: dict[str, dict[str, Any]], operation: str, latency: float, success: bool
    ):
        results[operation]["latencies"].append(latency)
        results[operation]["total"] += 1
        if success:
            results[operation]["success"] += 1

    def _merge_results(
        self, destination: dict[str, dict[str, Any]], source: dict[str, dict[str, Any]]
    ):
        for operation in self.OPERATIONS:
            destination[operation]["latencies"].extend(source[operation]["latencies"])
            destination[operation]["success"] += source[operation]["success"]
            destination[operation]["total"] += source[operation]["total"]

    def _run_config_apply_iteration(
        self,
        base_url: str,
        mock_endpoint: str,
        settings: CapellaWebhookSettings,
        verify_timeout_s: float,
        verify_interval_s: float,
    ) -> dict[str, dict[str, Any]]:
        results = self._init_results()
        create_payload = self._build_alert_integration_payload(
            name=f"perf-webhook-{uuid4().hex[:8]}",
            webhook_url=mock_endpoint,
            settings=settings,
        )

        webhook_id, latency, success = self._create_webhook_config(
            base_url=base_url,
            payload=create_payload,
            verify_timeout_s=verify_timeout_s,
            verify_interval_s=verify_interval_s,
        )
        self._record_result(results, "create", latency, success)

        if not webhook_id:
            return results

        update_payload = self._build_alert_integration_update_payload(
            name=f"perf-webhook-updated-{uuid4().hex[:8]}",
            webhook_url=mock_endpoint,
            settings=settings,
        )
        latency, success = self._update_webhook_config(
            base_url=base_url,
            webhook_id=webhook_id,
            payload=update_payload,
            verify_timeout_s=verify_timeout_s,
            verify_interval_s=verify_interval_s,
        )
        self._record_result(results, "update", latency, success)

        latency, success = self._delete_webhook_config(
            base_url=base_url,
            webhook_id=webhook_id,
            verify_timeout_s=verify_timeout_s,
            verify_interval_s=verify_interval_s,
        )
        self._record_result(results, "delete", latency, success)

        return results

    def _measure_for_thread_level(
        self, base_url: str, settings: CapellaWebhookSettings, thread_count: int
    ) -> dict[str, dict[str, Any]]:
        samples = settings.samples
        verify_timeout_s = settings.apply_verify_timeout
        verify_interval_s = settings.apply_verify_interval
        mock_endpoint = settings.mock_endpoint

        results = self._init_results()
        self._cleanup_perf_integrations(base_url)
        try:
            if thread_count == 1:
                for i in range(samples):
                    iteration_results = self._run_config_apply_iteration(
                        base_url=base_url,
                        mock_endpoint=mock_endpoint,
                        settings=settings,
                        verify_timeout_s=verify_timeout_s,
                        verify_interval_s=verify_interval_s,
                    )
                    self._merge_results(results, iteration_results)

                    if (i + 1) % 10 == 0 or i + 1 == samples:
                        logger.info(f"Completed {i + 1}/{samples} iterations")
                return results

            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures = [
                    executor.submit(
                        self._run_config_apply_iteration,
                        base_url=base_url,
                        mock_endpoint=mock_endpoint,
                        settings=settings,
                        verify_timeout_s=verify_timeout_s,
                        verify_interval_s=verify_interval_s,
                    )
                    for _ in range(samples)
                ]
                for i, future in enumerate(as_completed(futures), start=1):
                    try:
                        iteration_results = future.result()
                        self._merge_results(results, iteration_results)
                    except Exception as err:
                        logger.error(
                            "Webhook config iteration failed for "
                            f"thread level {thread_count}: {err}"
                        )

                    if i % 10 == 0 or i == samples:
                        logger.info(
                            f"Completed {i}/{samples} iterations (threads={thread_count})"
                        )
            return results
        finally:
            self._cleanup_perf_integrations(base_url)

    def measure_config_latency(self) -> dict[str, dict[str, dict[str, Any]]]:
        settings = self._get_webhook_settings()
        base_url = self._alert_integrations_base_url()
        thread_levels = settings.apply_threads

        all_results = {}
        for thread_count in thread_levels:
            logger.info(
                "Running webhook config apply latency test: "
                f"{settings.samples} iterations with {thread_count} thread(s)"
            )
            mode = "seq" if thread_count == 1 else f"t{thread_count}"
            all_results[mode] = self._measure_for_thread_level(
                base_url=base_url,
                settings=settings,
                thread_count=thread_count,
            )

        return all_results

    def _report_kpi(self, all_results: dict[str, dict[str, dict[str, Any]]]):
        for mode, mode_results in all_results.items():
            suffix = "" if mode == "seq" else f"_{mode}"
            for operation, data in mode_results.items():
                latencies = data["latencies"]
                if not latencies:
                    continue

                for percentile in [50, 95, 99]:
                    self.reporter.post(
                        *self.metrics.api_latency(
                            f"webhook_config_{operation}{suffix}",
                            latencies,
                            percentile,
                        )
                    )

                self.reporter.post(
                    *self.metrics.webhook_success_rate(
                        data["total"],
                        data["success"],
                        f"config_{operation}{suffix}",
                    )
                )

    def run(self):
        results = self.measure_config_latency()
        self.report_kpi(results)


class WebhookDeliveryLatencyTest(WebhookTestBase):
    """Test B1: Measure end-to-end webhook delivery latency.

    Uses the metric simulator to trigger alerts on the cluster. Measures
    the time from simulation start (T_start) to webhook receipt at the
    mock endpoint (T_receive) for each delivered webhook.

    Flow:
      1. Create a webhook config pointing to mock endpoint
      2. POST enable-test-observer with dp-testplan.json to start alert simulation
      3. Poll mock endpoint for received webhooks and record T_receive
      4. POST disable-test-observer to stop simulation
      5. Compute latencies and report metrics
    """

    def _setup_webhook(
        self, base_url: str, mock_endpoint: str, settings: CapellaWebhookSettings
    ) -> Optional[str]:
        payload = self._build_alert_integration_payload(
            name=f"perf-delivery-test-{uuid4().hex[:8]}",
            webhook_url=mock_endpoint,
            settings=settings,
        )

        resp = self._create_alert_integration(base_url, payload)
        if resp.status_code not in (200, 201):
            logger.interrupt(
                f"Failed to create webhook for delivery test: {resp.status_code} {resp.text}"
            )
        return resp.json().get("id")

    def _get_mock_deliveries(self, mock_endpoint: str) -> list[dict[str, Any]]:
        try:
            resp = requests.get(f"{mock_endpoint}/deliveries", timeout=10)
            if resp.status_code == 200:
                payload = resp.json()
                return payload.get("deliveries", []) if isinstance(payload, dict) else []
            logger.warn(f"Mock deliveries failed: {resp.status_code} {resp.text}")
        except requests.RequestException as err:
            logger.warn(f"Mock deliveries error: {err}")
        return []

    def _collect_deliveries(
        self, mock_endpoint: str, duration: float, poll_interval: float = 0.5
    ) -> list[dict[str, Any]]:
        """Poll the mock endpoint for webhook deliveries over a given duration."""
        deliveries = []
        deadline = time.time() + duration
        seen_ids = set()

        while time.time() < deadline:
            for delivery in self._get_mock_deliveries(mock_endpoint):
                d_id = delivery.get("id")
                if d_id and d_id not in seen_ids:
                    seen_ids.add(d_id)
                    deliveries.append(delivery)
            time.sleep(poll_interval)

        return deliveries

    @staticmethod
    def _rid_from_delivery(delivery: dict[str, Any]) -> Optional[str]:
        path = delivery.get("path", "")
        try:
            return (parse_qs(urlparse(path).query).get("rid") or [None])[0]
        except Exception:
            return None

    def _trigger_one_test_alert(
        self, test_url: str, mock_endpoint: str, token: str, req_id: str
    ) -> tuple[str, float, bool, int, str]:
        sep = "&" if "?" in mock_endpoint else "?"
        target_url = f"{mock_endpoint}{sep}rid={req_id}"
        payload = {
            "kind": "webhook",
            "config": {
                "webhook": {
                    "method": "POST",
                    "url": target_url,
                    "token": token,
                }
            }
        }

        t_sent = time.time()
        resp = self._request_with_retry("POST", test_url, payload=payload, timeout=30)
        ok = resp.status_code in (200, 201, 202, 204)
        return req_id, t_sent, ok, resp.status_code, resp.text

    def _collect_deliveries_from_test_trigger_burst(
        self, mock_endpoint: str, settings: CapellaWebhookSettings
    ) -> tuple[list[dict[str, Any]], int, int, int]:
        token = self._get_webhook_auth_token(settings)

        requested = max(1, settings.samples)
        configured_wave_size = max(1, settings.concurrency)
        strict_burst = settings.strict_burst
        wave_size = requested if strict_burst else min(configured_wave_size, requested)
        wave_sleep = settings.burst_wave_sleep
        if strict_burst:
            wave_sleep = 0
        default_parallelism = wave_size if strict_burst else min(wave_size, 150)
        configured_parallelism = settings.burst_parallelism or default_parallelism
        parallelism = max(1, configured_parallelism)
        parallelism = min(parallelism, wave_size)
        max_attempts = max(1, settings.burst_max_attempts)
        retry_base_sleep = settings.burst_retry_base_sleep
        retry_max_sleep = settings.burst_retry_max_sleep
        retry_jitter = settings.burst_retry_jitter
        inter_chunk_sleep = settings.burst_inter_chunk_sleep
        retryable_statuses = {408, 409, 425, 429, 500, 502, 503, 504}

        test_url = self._alert_integration_test_url()
        sent_at = {}
        accepted = 0
        failed = 0

        logger.info(
            f"Burst config: requested={requested}, wave_size={wave_size}, "
            f"parallelism={parallelism}, wave_sleep={wave_sleep}s, "
            f"max_attempts={max_attempts}, strict_burst={strict_burst}"
        )

        for start in range(0, requested, wave_size):
            n = min(wave_size, requested - start)
            wave_ok = 0
            wave_fail = 0
            wave_retried = 0
            pending = [uuid4().hex for _ in range(n)]

            for attempt in range(max_attempts):
                if not pending:
                    break

                retry_ids = []
                for chunk_start in range(0, len(pending), parallelism):
                    chunk = pending[chunk_start:chunk_start + parallelism]
                    with ThreadPoolExecutor(max_workers=len(chunk)) as executor:
                        future_map = {
                            executor.submit(
                                self._trigger_one_test_alert,
                                test_url,
                                mock_endpoint,
                                token,
                                req_id,
                            ): req_id
                            for req_id in chunk
                        }

                        for future in as_completed(future_map):
                            request_id = future_map[future]
                            try:
                                req_id, t_sent, ok, status, body = future.result()
                            except Exception as err:
                                if attempt + 1 < max_attempts:
                                    retry_ids.append(request_id)
                                    wave_retried += 1
                                else:
                                    failed += 1
                                    wave_fail += 1
                                    logger.warn(
                                        f"Burst trigger exception [{request_id}]: {err}"
                                    )
                                continue

                            if ok:
                                sent_at[req_id] = t_sent
                                accepted += 1
                                wave_ok += 1
                            elif status in retryable_statuses and attempt + 1 < max_attempts:
                                retry_ids.append(request_id)
                                wave_retried += 1
                            else:
                                failed += 1
                                wave_fail += 1
                                logger.warn(
                                    f"Burst trigger failed [{request_id}] "
                                    f"after {attempt + 1} attempt(s): {status} {body}"
                                )

                    if inter_chunk_sleep > 0 and chunk_start + parallelism < len(pending):
                        time.sleep(inter_chunk_sleep)

                pending = retry_ids
                if pending and attempt + 1 < max_attempts:
                    retry_sleep = min(retry_base_sleep * (2 ** attempt), retry_max_sleep)
                    retry_sleep += random.uniform(0, retry_jitter)
                    logger.info(
                        f"Burst retry pass {attempt + 2}/{max_attempts}: "
                        f"{len(pending)} pending requests, sleeping {retry_sleep:.2f}s"
                    )
                    time.sleep(retry_sleep)

            logger.info(
                f"Burst wave done: requested={n}, accepted={wave_ok}, "
                f"failed={wave_fail}, retried={wave_retried}"
            )
            if start + n < requested and wave_sleep > 0:
                time.sleep(wave_sleep)

        mapped = {}
        configured_wait = settings.collection_duration
        extra_wait = max(0, settings.burst_extra_wait_s)
        wait_timeout = max(
            120,
            configured_wait + extra_wait,
            int(60 + (accepted / max(1, parallelism)) * 6),
        )
        wait_timeout = min(wait_timeout, settings.burst_max_wait_s)
        deadline = time.time() + wait_timeout
        last_progress_log = 0
        while time.time() < deadline and len(mapped) < accepted:
            for d in self._get_mock_deliveries(mock_endpoint):
                rid = self._rid_from_delivery(d)
                if rid and rid in sent_at and rid not in mapped:
                    d["sent_at"] = sent_at[rid]
                    mapped[rid] = d

            if len(mapped) - last_progress_log >= 100:
                last_progress_log = len(mapped)
                logger.info(f"Burst receive progress: {len(mapped)}/{accepted}")
            time.sleep(0.2)

        return list(mapped.values()), requested, accepted, failed

    def _cleanup_webhook(self, base_url: str, webhook_id: str):
        self._delete_alert_integration(base_url, webhook_id)

    def measure_delivery_latency(self) -> dict[str, Any]:
        settings = self._get_webhook_settings()
        capella_client = self._get_capella_client()
        base_url = self._alert_integrations_base_url()
        mock_endpoint = settings.mock_endpoint
        cluster_id = self._get_cluster_id()
        collection_duration = settings.collection_duration
        force_burst = settings.force_burst
        burst_only = force_burst or settings.burst_only
        target_samples = max(1, settings.samples)

        test_plan = None
        if not burst_only:
            test_plan = self._load_test_plan(settings)
            if not test_plan:
                logger.interrupt("No valid webhook_test_plan file provided")

        webhook_id = self._setup_webhook(base_url, mock_endpoint, settings)

        try:
            reset_resp = requests.post(f"{mock_endpoint}/reset", timeout=5)
        except requests.RequestException as err:
            self._cleanup_webhook(base_url, webhook_id)
            logger.interrupt(f"Could not reset mock endpoint: {err}")
        if reset_resp.status_code != 200:
            self._cleanup_webhook(base_url, webhook_id)
            logger.interrupt(f"Mock reset failed: {reset_resp.status_code} {reset_resp.text}")

        fallback_requested = None
        fallback_accepted = None
        fallback_failed = None
        deliveries = []
        t_collect_start = time.time()

        if not burst_only:
            sim_ok = self._enable_alert_simulation(capella_client, cluster_id, test_plan)
            if not sim_ok:
                self._cleanup_webhook(base_url, webhook_id)
                logger.interrupt("Failed to start alert simulation")

            logger.info(
                f"Alert simulation started. Collecting deliveries for {collection_duration}s..."
            )

            try:
                deliveries = self._collect_deliveries(mock_endpoint, collection_duration)
            finally:
                self._disable_alert_simulation(capella_client, cluster_id)

        if burst_only or len(deliveries) < target_samples:
            if burst_only:
                logger.info(
                    "B1 burst-only mode enabled; skipping simulator "
                    "and using alertIntegrationTest burst"
                )
            else:
                logger.warn(
                    f"Simulator collected {len(deliveries)} deliveries "
                    f"(< target {target_samples}); "
                    "running burst mode to reach target"
                )

            try:
                reset_resp = requests.post(f"{mock_endpoint}/reset", timeout=5)
            except requests.RequestException as err:
                self._cleanup_webhook(base_url, webhook_id)
                logger.interrupt(f"Could not reset mock endpoint before burst: {err}")

            if reset_resp.status_code != 200:
                self._cleanup_webhook(base_url, webhook_id)
                logger.interrupt(
                    f"Mock reset failed before burst: {reset_resp.status_code} {reset_resp.text}"
                )

            (
                deliveries,
                fallback_requested,
                fallback_accepted,
                fallback_failed,
            ) = self._collect_deliveries_from_test_trigger_burst(
                mock_endpoint=mock_endpoint,
                settings=settings,
            )

            logger.info(
                f"Fallback burst summary: requested={fallback_requested}, "
                f"accepted={fallback_accepted}, failed={fallback_failed}, "
                f"received={len(deliveries)}"
            )

        if not deliveries:
            self._cleanup_webhook(base_url, webhook_id)
            logger.interrupt("No deliveries collected from simulator or burst mode")

        latencies = []
        event_based = 0
        fallback_based = 0
        for delivery in deliveries:
            t_receive = delivery.get("received_at")
            if t_receive is None:
                continue

            t_event = self._extract_event_ts_from_delivery(delivery)
            if t_event is not None and t_receive >= t_event:
                latencies.append(t_receive - t_event)
                event_based += 1
            elif delivery.get("sent_at") is not None and t_receive >= delivery["sent_at"]:
                latencies.append(t_receive - delivery["sent_at"])
                fallback_based += 1
            elif t_collect_start is not None and t_receive >= t_collect_start:
                latencies.append(t_receive - t_collect_start)
                fallback_based += 1

        denominator_mode = settings.success_denominator.strip().lower()
        if fallback_requested is not None:
            if denominator_mode == "requested":
                total = fallback_requested
            else:
                total = fallback_accepted if fallback_accepted is not None else fallback_requested
        else:
            total = len(deliveries)
        successful = len(latencies)

        logger.info(
            f"Collected {successful} valid latencies from {len(deliveries)} deliveries "
            f"(event_based={event_based}, fallback_based={fallback_based}); "
            f"success denominator={total} ({denominator_mode})"
        )

        self._cleanup_webhook(base_url, webhook_id)

        return {
            "latencies": latencies,
            "total": total,
            "successful": successful,
        }

    def _report_kpi(self, results: dict[str, Any]):
        latencies = results["latencies"]
        if latencies:
            for percentile in [50, 95, 99]:
                self.reporter.post(
                    *self.metrics.api_latency("webhook_delivery", latencies, percentile)
                )
        self.reporter.post(
            *self.metrics.webhook_success_rate(
                results["total"], results["successful"], "delivery"
            )
        )

    def run(self):
        results = self.measure_delivery_latency()
        self.report_kpi(results)

import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import parse_duration_to_secs
from perfrunner.tests.tools import ContinuousBackupTest, continuous_backup_retention_minutes

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
UUID_DIR = re.compile(r"[0-9a-fA-F]{32}")
RATE_UNITS_TO_MB = {"GiB": 1024, "MiB": 1, "KiB": 1 / 1024}


class ContinuousBackupPITRTest(ContinuousBackupTest):
    """Restore a bucket to a point in time: a base backup, then a replay of its continuous backup.

    The continuous backup location has to be mounted on the host running perfrunner and on every
    data node.
    """

    COLLECTORS = {
        **ContinuousBackupTest.COLLECTORS,
        "disk": False,
        "latency": False,
        "net": False,
        "secondary_stats": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pitr = self.test_config.pitr_restore_settings
        self.backup_options = self.test_config.backup_settings
        self.bucket = self.test_config.buckets[0]
        self.target_ts = ""

    def _create_kms_credential(self) -> str:
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
        session_token = os.environ.get("AWS_SESSION_TOKEN", "").strip()
        if not access_key or not secret_key:
            raise RuntimeError(
                "An encrypted run needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and "
                "AWS_SESSION_TOKEN for temporary credentials) with access to "
                f"{self.backup_options.km_key_url}"
            )

        fields = {
            "region": self.backup_options.km_region,
            "accessKeyId": access_key,
            "secretAccessKey": secret_key,
        }
        if session_token:
            fields["sessionToken"] = session_token

        # The credential store rejects writes unless n2n encryption is on, or it is overridden
        self.rest.set_credential_store(
            self.master_node, config_encryption_override=True, n2n_encryption_override=True
        )
        cred_id = str(uuid4())
        self.rest.create_credential(self.master_node, cred_id, "aws", fields)
        self.rest.grant_service_credential_consumer(self.master_node, "backup", cred_id)
        return cred_id

    def _enable_continuous_backup(self):
        retention = continuous_backup_retention_minutes(self.test_config.bucket.history_seconds)
        options = {"retention_minutes": retention}

        if self.test_config.bucket.encryption_at_rest:
            self.rest.bypass_encryption_config(self.master_node)
            options |= {
                "km_cred_id": self._create_kms_credential(),
                "km_key_url": self.backup_options.km_key_url,
            }

        self.configure_continuous_backup(True, **options)
        self.wait_for_continuous_backup()

    def _get_total_cmd_set(self) -> int:
        total = 0
        for server in self.data_nodes:
            try:
                stats = self.memcached.get_stats(server, 11210, self.bucket)
                total += int(stats.get(b"cmd_set", stats.get("cmd_set", 0)))
            except Exception as e:
                logger.warning(f"Could not read cmd_set on {server}: {e}")
        return total

    def _get_staging_usage(self) -> float:
        """Return the size (MB) of the continuous backup staging area across the data nodes."""
        if not (bucket_uuid := self.rest.get_bucket_uuid(self.master_node, self.bucket)):
            raise RuntimeError(f"Could not resolve the UUID of bucket {self.bucket}")
        data_path = self.cluster_spec.data_path or "/data"
        candidates = [
            f"{data_path}/data/@continuous_backup/{bucket_uuid}",
            f"{data_path}/@continuous_backup/{bucket_uuid}",
            f"/opt/couchbase/var/lib/couchbase/data/@continuous_backup/{bucket_uuid}",
        ]
        per_node_mb = {
            node: self.remote.get_dir_size_mb(node, *candidates) for node in self.data_nodes
        }
        logger.info(f"Staging area per node (MB): {per_node_mb}")
        return sum(per_node_mb.values())

    @staticmethod
    def _parse_transfer_stats(output: str) -> tuple[Optional[float], Optional[float]]:
        """Return the MB/s and duration (s) of a tool's successful summary row."""
        for line in output.splitlines():
            columns = [col.strip() for col in line.strip().strip("|").split("|")]
            if len(columns) < 3 or columns[0] != "Succeeded":
                continue
            if rate := re.fullmatch(r"([\d.]+)(GiB|MiB|KiB)/s", columns[1]):
                duration = parse_duration_to_secs(columns[-1])
                return float(rate.group(1)) * RATE_UNITS_TO_MB[rate.group(2)], duration
        return None, None

    @staticmethod
    def _clamp_target_to_backup_span(error_text: str, target_ts: str) -> Optional[str]:
        """Return the last backed-up time when cbcontbk rejects a target that is past it.

        The backup only extends to the last mutation, so a target captured just after the
        workload stops can fall a moment beyond it. Returns None for any other failure,
        including a target before the start of the backup.
        """
        match = re.search(
            r"target time does not appear in the backup; backup spans from (\S+) to (\S+)",
            error_text,
        )
        if not match:
            return None
        span_end = datetime.fromisoformat(match.group(2)).astimezone(timezone.utc)
        target = datetime.strptime(target_ts, TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
        if target <= span_end:
            return None
        return span_end.strftime(TIMESTAMP_FORMAT)

    @staticmethod
    def _summarise_growth(samples: list[tuple[int, float]]) -> tuple[float, float, float]:
        """Return the total MB added, the average seconds per flush and the average MB per flush.

        A flush is a run of consecutive samples that grew; the quiet samples between runs are
        not counted as flush time.
        """
        total_mb = 0.0
        flushes: list[tuple[float, float]] = []  # (seconds, MB)
        start = end = chunk_mb = 0.0
        in_flush = False

        for elapsed, delta in samples:
            if delta > 0:
                if not in_flush:
                    start, chunk_mb, in_flush = elapsed, 0.0, True
                end = elapsed
                chunk_mb += delta
                total_mb += delta
            elif in_flush:
                flushes.append((max(0.0, elapsed - start), chunk_mb))
                in_flush = False

        if in_flush:
            flushes.append((max(0.0, end - start), chunk_mb))

        if not flushes:
            return total_mb, 0.0, 0.0
        return total_mb, sum(f[0] for f in flushes) / len(flushes), total_mb / len(flushes)

    def _wait_for_magma_compaction_settle(self, max_wait_secs: int = 600, poll_secs: int = 60):
        data_path = self.cluster_spec.data_path or "/data"
        logger.info(f"Waiting up to {max_wait_secs}s for compaction to settle ({data_path})")
        previous_mb = None
        started = time.time()
        while time.time() - started < max_wait_secs:
            total_mb = sum(
                self.remote.get_dir_size_mb(node, data_path) for node in self.data_nodes
            )
            logger.info(f"Total data disk usage: {total_mb:.1f} MB")
            if previous_mb is not None and abs(total_mb - previous_mb) < 1.0:
                return
            previous_mb = total_mb
            time.sleep(poll_secs)
        logger.warning(f"Compaction did not settle within {max_wait_secs}s; carrying on")

    def _empty_bucket(self):
        """Empty the bucket ahead of the restore.

        An encrypted bucket is flushed so that it keeps its key-management credential. The flush
        request is sent once: a retry would flush again after a transient error response.
        """
        if self.test_config.bucket.encryption_at_rest:
            time.sleep(30)  # Let the disk queues settle
            self.rest.flush_bucket(self.master_node, self.bucket, retry=False)
            self.monitor.monitor_num_items(
                self.master_node, self.bucket, self.test_config.bucket.replica_number, 0
            )
        else:
            self.cluster.delete_buckets()
            self.cluster.create_buckets()
            self.cluster.create_collections()

    @with_stats
    def _run_cdc_window(self) -> dict[str, float]:
        """Run the workload while sampling the continuous backup location's growth."""
        cmd_set_before = self._get_total_cmd_set()
        started = time.time()
        self.access_bg()

        samples = local.sample_dir_growth(
            self.backup_options.continuous_backup_location,
            int(self.test_config.access_settings.time),
        )
        total_mb, avg_flush_secs, avg_flush_mb = self._summarise_growth(samples)
        logger.info(
            f"Continuous backup grew by {total_mb:.1f} MB; a flush averaged {avg_flush_mb:.2f} MB "
            f"in {avg_flush_secs:.1f}s"
        )

        self.worker_manager.wait_for_bg_tasks()

        mutations = self._get_total_cmd_set() - cmd_set_before
        logger.info(f"Applied {mutations} mutations in {time.time() - started:.0f}s")
        if mutations <= 0:
            logger.warning(f"The workload did not write to {self.bucket}")

        self.target_ts = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)
        logger.info(f"Restore target timestamp: {self.target_ts}")

        return {
            "avg_flush_mb": avg_flush_mb,
            "avg_flush_secs": avg_flush_secs,
            "staging_mb": self._get_staging_usage(),
        }

    def _get_continuous_uuid(self) -> str:
        """Return the newest continuous backup directory that has its bucket metadata."""
        location = self.backup_options.continuous_backup_location
        candidates = sorted(
            (
                path
                for path in Path(location).iterdir()
                if UUID_DIR.fullmatch(path.name) and (path / "meta" / "bucket_meta.json").is_file()
            ),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            raise RuntimeError(f"No continuous backup found under {location}")
        return candidates[0].name

    def _wait_for_continuous_uuid(self, timeout_secs: int = 900, poll_secs: int = 30) -> str:
        deadline = time.time() + timeout_secs
        while True:
            try:
                return self._get_continuous_uuid()
            except RuntimeError as e:
                if time.time() > deadline:
                    raise RuntimeError(f"{e} after {timeout_secs}s") from None
            time.sleep(poll_secs)

    @with_stats
    def _run_restore(self, base_backup: str) -> dict[str, float]:
        """Restore the base backup, then replay the continuous backup up to the target time."""
        self._empty_bucket()
        local.cleanup(self.backup_options.continuous_backup_tempdir)

        started = time.time()
        output = self.restore()
        logger.info(f"cbbackupmgr restore output:\n{output}")
        snapshot_mb_s, snapshot_secs = self._parse_transfer_stats(output)
        snapshot_secs = snapshot_secs or time.time() - started

        if self.pitr.wait_after_snapshot_restore:
            # Not recovery work, so it stays out of the restore durations
            self.wait_for_persistence()

        collection = f"{self.bucket}._default._default"
        replay = {
            "start": base_backup,
            "include_data": collection,
            "map_data": f"{collection}={collection}",
            "force_updates": True,
        }
        started = time.time()
        try:
            output = self.run_cbcontbk_restore(self.target_ts, **replay)
        except RuntimeError as e:
            if not (clamped_ts := self._clamp_target_to_backup_span(str(e), self.target_ts)):
                raise
            logger.warning(f"Target {self.target_ts} is past the backup; retrying at {clamped_ts}")
            output = self.run_cbcontbk_restore(clamped_ts, **replay)
        logger.info(f"cbcontbk restore-continuous output:\n{output}")
        replay_mb_s, replay_secs = self._parse_transfer_stats(output)

        return {
            "snapshot_mb_s": snapshot_mb_s or 0.0,
            "replay_mb_s": replay_mb_s or 0.0,
            "replay_secs": replay_secs or time.time() - started,
            "total_secs": snapshot_secs + (replay_secs or time.time() - started),
        }

    def _report_kpi(self, cdc: dict, restore: dict, cdc_snapshots: list):
        kpis = [
            ("cdc_avg_flush_mb", "Avg size of a continuous backup flush (MB)",
             cdc["avg_flush_mb"], -1),
            ("cdc_avg_flush_secs", "Avg time to upload a continuous backup flush (s)",
             cdc["avg_flush_secs"], -1),
            ("cdc_staging_mb", "Staging area utilization after the workload (MB)",
             cdc["staging_mb"], -1),
            ("snapshot_restore_mb_s", "Snapshot restore throughput (MB/s)",
             restore["snapshot_mb_s"], 1),
            ("replay_mb_s", "Restore-continuous throughput (MB/s)", restore["replay_mb_s"], 1),
            ("replay_secs", "Restore-continuous duration (s)", restore["replay_secs"], -1),
            ("restore_total_secs", "Restore total duration (s)", restore["total_secs"], -1),
        ]
        for index, (name, title, value, chirality) in enumerate(kpis):
            # The first three KPIs belong to the workload window, the others to the restore
            with self.snapshots_of(cdc_snapshots if index < 3 else None):
                self.reporter.post(*self.metrics.pitr_kpi(name, title, value, chirality))
            logger.info(f"KPI {name}: {value:.2f}")

    def run(self):
        self.require_cb_tools()
        location = self.backup_options.continuous_backup_location
        if not os.path.ismount(location):
            raise RuntimeError(f"{location} is not a mount of the shared backup location")
        if not self.test_config.bucket.history_seconds:
            raise ValueError("Continuous backup needs [bucket] history_seconds")

        cdc, restore, cdc_snapshots = {}, {}, []
        try:
            self.load()
            self.wait_for_persistence()
            if self.pitr.wait_for_compaction_settle:
                self._wait_for_magma_compaction_settle()

            self._enable_continuous_backup()
            self.backup()
            base_backup = local.get_backup_snapshots(
                self.cluster_spec, self.backup_options.km_key_url, self.backup_options.km_region
            )[0]

            cdc = self._run_cdc_window()
            cdc_snapshots = list(self.cbmonitor_snapshots)
            logger.info(f"Waiting {self.pitr.workload_drain_wait_secs}s for the CDC backup")
            time.sleep(self.pitr.workload_drain_wait_secs)

            self._wait_for_continuous_uuid()
            self.configure_continuous_backup(enable=False)
            restore = self._run_restore(base_backup)
        finally:
            try:
                self.collect_logs()
            except Exception as e:
                logger.warning(f"collect_logs() failed: {e}")

        self.report_kpi(cdc, restore, cdc_snapshots)

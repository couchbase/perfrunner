import glob
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from perfrunner.helpers.local_stats import (
    KV_WORKER_PATTERN,
    QUERY_WORKER_PATTERN,
    SpringLatencySource,
    resolve_spring_latency_files,
    spring_latency_key,
    spring_latency_snapshot_dir,
    spring_latency_target_groups,
)
from spring.wgen3 import AsyncKVWorker, KVWorker, N1QLWorker, SubDocWorker, XATTRWorker


class SpringLatencyPatternTest(TestCase):
    """Pin the glob patterns against the worker names that actually produce the files.

    `MetricHelper` and the latency collectors both select data files by these patterns,
    and both KV and query files are archived into the same dir, so a pattern that is too
    narrow or too broad silently changes which samples a KPI is computed from.
    """

    KV_WORKER_NAMES = [KVWorker.NAME, SubDocWorker.NAME, XATTRWorker.NAME, AsyncKVWorker.NAME]

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _dump(self, *worker_names: str, uuid_suffix: str = "") -> str:
        """Write data files named the way `spring.wgen3.Worker.dump_stats` names them."""
        for name in worker_names:
            Path(self.tmp.name, f"{name}-0-1-bucket-1{uuid_suffix}").touch()
        return self.tmp.name

    def _matches(self, pattern: str) -> list[str]:
        return sorted(os.path.basename(p) for p in glob.glob(f"{self.tmp.name}/{pattern}"))

    def test_kv_pattern_matches_every_kv_worker_variant(self):
        """The leading ``*`` is load-bearing: three of the four KV workers are prefixed."""
        self._dump(*self.KV_WORKER_NAMES)

        self.assertEqual(len(self._matches(KV_WORKER_PATTERN)), len(self.KV_WORKER_NAMES))

    def test_kv_and_query_patterns_do_not_match_each_other(self):
        """Both collectors archive into the same dir, so the patterns must partition it."""
        self._dump(*self.KV_WORKER_NAMES, N1QLWorker.NAME)

        self.assertNotIn(N1QLWorker.NAME, " ".join(self._matches(KV_WORKER_PATTERN)))
        self.assertEqual(len(self._matches(QUERY_WORKER_PATTERN)), 1)

    def test_patterns_survive_the_uuid_the_remote_fetch_appends(self):
        """`Remote.get_spring_data_files` renames files per worker before downloading them."""
        self._dump(KVWorker.NAME, N1QLWorker.NAME, uuid_suffix="-9f3a1c")

        self.assertEqual(len(self._matches(KV_WORKER_PATTERN)), 1)
        self.assertEqual(len(self._matches(QUERY_WORKER_PATTERN)), 1)


class SpringLatencyStatGroupTest(TestCase):
    """Pin the ``scope:collection`` -> stat group mapping shared by both readers.

    `KVLatency` uses it to label the perfdb bucket it pushes to, and `MetricHelper` uses
    it to split KV latency KPIs per group. If they disagree, every group's KPI silently
    reports the same aggregate over all collections.
    """

    CONFIG = "collections/1bucket_2scopes_3collections_history.json"

    def setUp(self):
        with open(self.CONFIG) as fh:
            self.groups = spring_latency_target_groups(json.load(fh))["bucket-1"]

    def test_targets_map_to_their_configured_stat_group(self):
        self.assertEqual(self.groups["scope-1:collection-1"], "history_on")
        self.assertEqual(self.groups["scope-2:collection-1"], "history_off")
        self.assertEqual(self.groups["scope-2:collection-2"], "history_off")

    def test_groups_partition_the_collections(self):
        """Each configured group must select a strict subset, never everything."""
        config_groups = {g for g in self.groups.values() if g}
        self.assertEqual(config_groups, {"history_on", "history_off"})
        for group in config_groups:
            selected = [t for t, g in self.groups.items() if g == group]
            self.assertTrue(0 < len(selected) < len(self.groups))

    def test_collections_without_a_stat_group_are_in_no_named_group(self):
        self.assertEqual(self.groups["_default:_default"], "")

    def test_unknown_and_absent_targets_fall_into_the_unnamed_group(self):
        """Spring records no target unless per-collection latency is on."""
        self.assertEqual(self.groups.get(None, ""), "")
        self.assertEqual(self.groups.get("scope-9:collection-9", ""), "")

    def test_no_collection_map_yields_no_mapping(self):
        self.assertEqual(spring_latency_target_groups(None), {})
        self.assertEqual(spring_latency_target_groups({}), {})


class SpringLatencyFilesTest(TestCase):
    """Cover `resolve_spring_latency_files`, which decides what a latency KPI reads."""

    PATTERN = KV_WORKER_PATTERN
    MASTER = "172.23.100.1"

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.live_dir = os.path.join(self.tmp.name, "live")
        os.makedirs(self.live_dir)

    def _archive(self, label: str, *filenames: str) -> str:
        """Create an archive dir holding ``filenames``, as a collector would."""
        archive_dir = os.path.join(self.tmp.name, label)
        os.makedirs(archive_dir)
        for filename in filenames:
            Path(archive_dir, filename).touch()
        return archive_dir

    def _dump(self, *filenames: str):
        """Create data files in the live dump dir, as spring workers would."""
        for filename in filenames:
            Path(self.live_dir, filename).touch()

    def test_prefers_archive_over_live_dir(self):
        """The archive is scoped to one phase; the live dir is shared by all of them."""
        archive_dir = self._archive("phase_a", "kv-worker-0-1-bucket-1-abc123")
        self._dump("kv-worker-0-1-bucket-1-def456")

        files = resolve_spring_latency_files(self.PATTERN, archive_dir, self.live_dir)

        self.assertEqual(files.source, SpringLatencySource.ARCHIVE)
        self.assertEqual(
            [os.path.basename(p) for p in files.paths], ["kv-worker-0-1-bucket-1-abc123"]
        )

    def test_falls_back_to_live_dir_when_nothing_was_archived(self):
        """KPIs read from inside a phase have no archive yet, and must still resolve."""
        self._dump("kv-worker-0-1-bucket-1-def456")

        files = resolve_spring_latency_files(self.PATTERN, None, self.live_dir)

        self.assertEqual(files.source, SpringLatencySource.LIVE)
        self.assertEqual(
            [os.path.basename(p) for p in files.paths], ["kv-worker-0-1-bucket-1-def456"]
        )

    def test_falls_back_to_live_dir_when_the_archive_holds_no_match(self):
        """An archive dir exists per cluster, not per workload, so it can hold no match."""
        archive_dir = self._archive("phase_a", "query-worker-0-1-bucket-1-abc123")
        self._dump("kv-worker-0-1-bucket-1-def456")

        files = resolve_spring_latency_files(self.PATTERN, archive_dir, self.live_dir)

        self.assertEqual(files.source, SpringLatencySource.LIVE)

    def test_returns_empty_rather_than_raising_when_there_is_no_data(self):
        """`MetricHelper` turns an empty result into a skipped KPI or a logged interrupt."""
        files = resolve_spring_latency_files(self.PATTERN, self._archive("phase_a"), self.live_dir)

        self.assertEqual(files.source, SpringLatencySource.NOWHERE)
        self.assertEqual(files.paths, [])

    def test_key_is_built_only_from_values_that_outlive_a_phase(self):
        """Guard rail: a key holding phase state cannot be rebuilt at report time.

        `MetricHelper` reconstructs this key after the last stats phase, which is often
        not the phase that produced the workload. Anything per-phase or non-deterministic
        in here breaks that lookup - see `spring_latency_key`.
        """
        self.assertEqual(
            spring_latency_key(self.MASTER, self.PATTERN),
            spring_latency_key(self.MASTER, self.PATTERN),
        )
        self.assertNotEqual(
            spring_latency_key(self.MASTER, KV_WORKER_PATTERN),
            spring_latency_key(self.MASTER, QUERY_WORKER_PATTERN),
        )

    def test_snapshot_dirs_do_not_collide_across_phases_or_clusters(self):
        """Two phases, or two clusters, archiving into one dir makes their data unusable."""
        self.assertNotEqual(
            spring_latency_snapshot_dir("cluster_770_access_ab12", self.MASTER),
            spring_latency_snapshot_dir("cluster_770_rebalance_cd34", self.MASTER),
        )
        self.assertNotEqual(
            spring_latency_snapshot_dir("cluster_770_access_ab12", self.MASTER),
            spring_latency_snapshot_dir("cluster_770_access_ab12", "172.23.100.2"),
        )

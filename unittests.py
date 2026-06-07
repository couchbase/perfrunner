import contextlib
import glob
import importlib.metadata
import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time
from collections import defaultdict, namedtuple
from collections.abc import Callable
from multiprocessing import Value
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase

import psutil
import snappy

from perfrunner.helpers import shell
from perfrunner.helpers.local_stats import (
    KV_WORKER_PATTERN,
    QUERY_WORKER_PATTERN,
    SpringLatencySource,
    resolve_spring_latency_files,
    spring_latency_key,
    spring_latency_snapshot_dir,
    spring_latency_target_groups,
)
from perfrunner.helpers.misc import parse_duration_to_secs, pretty_dict
from perfrunner.remote import api, executor
from perfrunner.settings import ClusterSpec, TestConfig
from perfrunner.workloads.analytics.bigfun.query_gen import new_queries
from perfrunner.workloads.tcmalloc import KeyValueIterator, LargeIterator
from spring import docgen
from spring.wgen3 import AsyncKVWorker, KVWorker, N1QLWorker, SubDocWorker, XATTRWorker

# perfrunner.helpers.worker configures celery when it is imported, and refuses to load unless it
# knows which kind of worker it is configuring. Declare one before importing it. Importing only
# updates celery's configuration; it does not connect to a broker or start anything.
os.environ.setdefault("WORKER_TYPE", "local")

from perfrunner.helpers.worker import (  # noqa: E402
    TASK_PIDFILE_DIR,
    LocalWorkerManager,
    RemoteWorkerManager,
    store_pid,
)

sdk_major_version = int(importlib.metadata.version("couchbase")[0])
if sdk_major_version == 2:
    from spring.querygen import N1QLQueryGen
elif sdk_major_version >= 3:
    from spring.querygen3 import N1QLQueryGen3 as N1QLQueryGen


class SettingsTest(TestCase):

    def test_stale_update_after(self):
        test_config = TestConfig()
        test_config.parse('tests/query_lat_20M_basic.test')
        query_params = test_config.access_settings.query_params
        self.assertEqual(query_params, {'stale': 'false'})

    def test_cluster_specs(self):
        for file_name in glob.glob("clusters/*.spec") + glob.glob(
            "cloud/infrastructure/**/*.spec", recursive=True
        ):
            cluster_spec = ClusterSpec()
            cluster_spec.parse(file_name, override=None)

    def test_override(self):
        test_config = TestConfig()
        test_config.parse('tests/query_lat_20M_basic.test',
                          override=['cluster.mem_quota.5555'])
        self.assertEqual(test_config.cluster.mem_quota, 5555)

    def test_soe_backup_repo(self):
        for file_name in glob.glob("tests/soe/*.test"):
            test_config = TestConfig()
            test_config.parse(file_name)
            self.assertNotEqual(test_config.restore_settings.backup_repo, '')

    def test_moving_working_set_settings(self):
        for file_name in glob.glob("tests/gsi/plasma/*.test"):
            test_config = TestConfig()
            test_config.parse(file_name)
            if test_config.access_settings.working_set_move_time:
                self.assertNotEqual(test_config.access_settings.working_set,
                                    100)
                self.assertEqual(test_config.access_settings.working_set_access,
                                 100)

    def test_fts_configs(self):
        for file in glob.glob("tests/fts/enduser/tests_dgm/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/enduser/tests_dgm/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/enduser/tests_dgm/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_non_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_non_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/enduser/tests_nodgm/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'end_user_non_dgm')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/multi_node/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_3_nodes')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/multi_node/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_3_nodes')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/multi_node/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_3_nodes')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/rebalance/*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Rebalance')

        for file in glob.glob("tests/fts/single_node/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/single_node/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')

        for file in glob.glob("tests/fts/single_node/*index*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark')
            self.assertEqual(test_config.showfast.sub_category, 'Index')

        for file in glob.glob("tests/fts/single_node_kv/*latency*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_kv')
            self.assertEqual(test_config.showfast.sub_category, 'Latency')

        for file in glob.glob("tests/fts/single_node_kv/*throughput*.test"):
            test_config = TestConfig()
            test_config.parse(file)
            self.assertEqual(test_config.showfast.category, 'benchmark_kv')
            self.assertEqual(test_config.showfast.sub_category, 'Throughput')


class MiscTest(TestCase):

    def test_parse_duration_to_secs(self):
        self.assertAlmostEqual(parse_duration_to_secs('1.5s'), 1.5)
        self.assertAlmostEqual(parse_duration_to_secs('1m30s'), 90.0)
        self.assertAlmostEqual(parse_duration_to_secs('1m40.0s'), 100.0)
        self.assertAlmostEqual(parse_duration_to_secs('2h3m4.005s'), 7384.005)
        self.assertAlmostEqual(parse_duration_to_secs('500ms'), 0.5)
        self.assertAlmostEqual(parse_duration_to_secs('500µs'), 5e-4)
        self.assertAlmostEqual(parse_duration_to_secs('500μs'), 5e-4)
        self.assertAlmostEqual(parse_duration_to_secs('500us'), 5e-4)
        self.assertAlmostEqual(parse_duration_to_secs('500ns'), 5e-7)
        # A bare number is read as seconds, and surrounding whitespace is tolerated
        self.assertAlmostEqual(parse_duration_to_secs('3'), 3.0)
        self.assertAlmostEqual(parse_duration_to_secs(' 1m 30s '), 90.0)
        # Anything not fully consumed is NaN, never silently read as seconds
        for bad in ('', '   ', 'garbage', '1.5d', '1.5s trailing', 'leading 1.5s', '-1.5s'):
            self.assertTrue(math.isnan(parse_duration_to_secs(bad)), f'expected NaN for {bad!r}')


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


class WorkloadTest(TestCase):

    def test_value_size(self):
        for _ in range(100):
            iterator = KeyValueIterator(10000)
            batch = iterator.next()
            values = [len(str(v)) for k, v in batch]
            mean = sum(values) / len(values)
            self.assertAlmostEqual(mean, 1024, delta=128)

    def test_large_field_size(self):
        field = LargeIterator()._field('000000000001')
        size = len(str(field))
        self.assertAlmostEqual(size, LargeIterator.FIELD_SIZE, delta=16)


WorkloadSettings = namedtuple('WorkloadSettings', ('items',
                                                   'workers',
                                                   'working_set',
                                                   'working_set_access',
                                                   'working_set_moving_docs',
                                                   'key_fmtr'))


class SpringTest(TestCase):

    def test_seq_key_generator(self):
        ws = WorkloadSettings(items=10 ** 5, workers=25, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = []
        for worker in range(ws.workers):
            generator = docgen.SequentialKey(worker, ws, prefix='test')
            keys += [key.string for key in generator]

        expected = [docgen.Key(number=i, prefix='test', fmtr='decimal').string
                    for i in range(ws.items)]

        self.assertEqual(sorted(keys), expected)

    def test_new_ordered_keys(self):
        ws = WorkloadSettings(items=10 ** 4, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key)

        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        for op in range(1, 10 ** 3):
            key = key_gen.next(ws.items + op)
            self.assertNotIn(key, keys)

    def test_zipf_generator(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key, keys)
                keys.add(key.string)
        self.assertEqual(len(keys), ws.items)

        key_gen = docgen.ZipfKey(prefix='test', fmtr='decimal', alpha=1.5)
        for op in range(10 ** 4):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            self.assertIn(key.string, keys)

    def test_power_generator(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key, keys)
                keys.add(key.string)
        self.assertEqual(len(keys), ws.items)

        key_gen = docgen.PowerKey(prefix='test', fmtr=ws.key_fmtr, alpha=100)
        for op in range(10 ** 4):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            self.assertIn(key.string, keys)

    def test_power_generator_cache_miss(self):
        num_ops = 10 ** 5
        ws = WorkloadSettings(items=10 ** 5, workers=40, working_set=1.6,
                              working_set_access=90, working_set_moving_docs=0,
                              key_fmtr='hex')

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)

        key_gen = docgen.PowerKey(prefix='test', fmtr=ws.key_fmtr, alpha=142)
        misses = 0
        for op in range(num_ops):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            if key.string not in hot_keys:
                misses += 1

        hit_rate = 100 * (1 - misses / num_ops)

        self.assertAlmostEqual(hit_rate, ws.working_set_access, delta=0.5)

    def test_zipf_generator_cache_miss(self):
        num_ops = 10 ** 5
        ws = WorkloadSettings(items=10 ** 5, workers=40, working_set=1.6,
                              working_set_access=90, working_set_moving_docs=0,
                              key_fmtr='hex')

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)

        key_gen = docgen.ZipfKey(prefix='test', fmtr=ws.key_fmtr, alpha=1.23)
        misses = 0
        for op in range(num_ops):
            key = key_gen.next(curr_deletes=100, curr_items=ws.items)
            if key.string not in hot_keys:
                misses += 1

        hit_rate = 100 * (1 - misses / num_ops)

        self.assertAlmostEqual(hit_rate, ws.working_set_access, delta=0.5)

    def doc_generators(self, size: int):
        for dg in (
            docgen.ReverseLookupDocument(avg_size=size, prefix='n1ql'),
            docgen.ReverseRangeLookupDocument(avg_size=size, prefix='n1ql',
                                              range_distance=100),
            docgen.ExtReverseLookupDocument(avg_size=size, prefix='n1ql',
                                            num_docs=10 ** 6),
            docgen.HashJoinDocument(avg_size=size, prefix='n1ql',
                                    range_distance=1000),
            docgen.ArrayIndexingDocument(avg_size=size, prefix='n1ql',
                                         array_size=10, num_docs=10 ** 6),
            docgen.ProfileDocument(avg_size=size, prefix='n1ql'),
            docgen.String(avg_size=size)
        ):
            yield dg

    def test_doc_size(self):
        size = 1024
        key_gen = docgen.NewOrderedKey(prefix='n1ql', fmtr='decimal')

        for dg in self.doc_generators(size=size):
            for i in range(10 ** 4):
                key = key_gen.next(i)
                doc = dg.next(key=key)
                actual_size = len(str(doc))
                self.assertAlmostEqual(actual_size, size,
                                       delta=size * 0.05,  # 5% variation
                                       msg=dg.__class__.__name__)

    def test_doc_size_variation(self):
        size = 512
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.Document(avg_size=size)

        for i in range(10 ** 4):
            key = key_gen.next(i)
            doc = doc_gen.next(key=key)
            actual_size = len(str(doc))
            self.assertAlmostEqual(actual_size, size,
                                   delta=size * doc_gen.SIZE_VARIATION)

    def test_small_documents(self):
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.Document(avg_size=150)

        for i in range(10 ** 3):
            key = key_gen.next(i)
            doc = doc_gen.next(key=key)
            size = len(str(doc))

            self.assertEqual(doc["body"], "")
            self.assertAlmostEqual(size, doc_gen.OVERHEAD, delta=100)

    def test_large_documents(self):
        size = 1024
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.LargeDocument(avg_size=size)

        for i in range(10 ** 4):
            key = key_gen.next(i)
            doc = doc_gen.next(key=key)
            value = json.dumps(doc)
            actual_size = len(value)

            self.assertAlmostEqual(actual_size, size,
                                   delta=size * doc_gen.SIZE_VARIATION,
                                   msg=value)

    def test_compression_ratio(self):
        size = 1024
        key_gen = docgen.NewOrderedKey(prefix='test', fmtr='decimal')
        doc_gen = docgen.LargeDocument(avg_size=size)

        for i in range(10 ** 4):
            key = key_gen.next(i)
            doc = doc_gen.next(key)
            value = json.dumps(doc)

            compressed = snappy.compress(value)
            ratio = len(value) / len(compressed)

            self.assertLess(ratio, 1.75, value)

    def test_hot_keys(self):
        ws = WorkloadSettings(items=10 ** 4, workers=40, working_set=10,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key.string, keys)
                keys.add(key.string)
        self.assertEqual(len(keys), ws.items)

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key.string, hot_keys)
                self.assertIn(key.string, keys)
                hot_keys.add(key.string)
        self.assertEqual(len(hot_keys), ws.working_set * ws.items // 100)

    def test_uniform_keys(self):
        ws = WorkloadSettings(items=10 ** 3, workers=10, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.UniformKey(prefix='test',
                                    fmtr='decimal')
        for op in range(10 ** 4):
            key = key_gen.next(curr_items=ws.items, curr_deletes=100)
            self.assertIn(key.string, keys)

    def test_working_set_keys(self):
        ws = WorkloadSettings(items=10 ** 3, workers=10, working_set=90,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.WorkingSetKey(ws=ws, prefix='test')
        for op in range(10 ** 4):
            key = key_gen.next(curr_items=ws.items, curr_deletes=0)
            self.assertIn(key.string, keys)

    def test_moving_working_set_keys(self):
        ws = WorkloadSettings(items=10 ** 3, workers=10, working_set=90,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='decimal')
        current_hot_load_start = Value('L', 0)
        timer_elapse = Value('I', 0)

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.MovingWorkingSetKey(ws, prefix='test')
        keys = sorted(keys)

        for op in range(10 ** 4):
            key = key_gen.next(curr_items=ws.items,
                               curr_deletes=0,
                               current_hot_load_start=current_hot_load_start,
                               timer_elapse=timer_elapse)
            self.assertIn(key.string, keys)

    def test_cas_updates(self):
        ws = WorkloadSettings(items=10 ** 3, workers=20, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        cases = defaultdict(set)
        key_gen = docgen.KeyForCASUpdate(total_workers=ws.workers, prefix='test',
                                         fmtr='decimal')
        for sid in 5, 6:
            for op in range(10 ** 3):
                key = key_gen.next(sid=sid, curr_items=ws.items)
                self.assertIn(key.string, keys)
                cases[sid].add(key.string)
        self.assertEqual(cases[5] & cases[6], set())

    def test_key_for_removal(self):
        ws = WorkloadSettings(items=10 ** 3, workers=20, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)

        key_gen = docgen.KeyForRemoval(prefix='test', fmtr='decimal')
        for op in range(1, 100):
            key = key_gen.next(op)
            self.assertIn(key.string, keys)

    def test_keys_without_prefix(self):
        ws = WorkloadSettings(items=10 ** 3, workers=20, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix=''):
                keys.add(key.string)

        expected = [docgen.Key(number=i, prefix='', fmtr='decimal').string
                    for i in range(ws.items)]

        self.assertEqual(sorted(keys), expected)

    def test_hash_fmtr(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='hash')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                self.assertNotIn(key.string, keys)
                self.assertEqual(len(key.string), 16)
                keys.add(key.string)

    def test_new_working_set_hits(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='hex')

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)
        hot_keys = sorted(hot_keys)

        wsk = docgen.WorkingSetKey(ws=ws, prefix='test')
        hits = set()
        news_items = 10
        for op in range(10 ** 5):
            key = wsk.next(curr_items=ws.items + news_items, curr_deletes=100)
            if key.hit:
                hits.add(key.string)

        overlap = set(hot_keys) & hits
        self.assertEqual(len(overlap),
                         ws.items * (ws.working_set / 100) - news_items)

    def test_working_set_hits(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='hex')

        keys = set()
        for worker in range(ws.workers):
            for key in docgen.SequentialKey(sid=worker, ws=ws, prefix='test'):
                keys.add(key.string)
        keys = sorted(keys)

        hot_keys = set()
        for worker in range(ws.workers):
            for key in docgen.HotKey(sid=worker, ws=ws, prefix='test'):
                hot_keys.add(key.string)
        hot_keys = sorted(hot_keys)

        wsk = docgen.WorkingSetKey(ws=ws, prefix='test')
        for op in range(10 ** 5):
            key = wsk.next(curr_items=ws.items, curr_deletes=100)
            self.assertIn(key.string, keys)
            if key.hit:
                self.assertIn(key.string, hot_keys)
            else:
                self.assertNotIn(key.string, hot_keys)

    def test_working_set_deletes(self):
        ws = WorkloadSettings(items=10 ** 3, workers=40, working_set=20,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='hex')

        keys_for_removal = docgen.KeyForRemoval(prefix='test',
                                                fmtr=ws.key_fmtr)
        removed_keys = set()
        for i in range(100):
            key = keys_for_removal.next(i)
            removed_keys.add(key.string)
        removed_keys = sorted(removed_keys)

        wsk = docgen.WorkingSetKey(ws=ws, prefix='test')
        for op in range(10 ** 5):
            key = wsk.next(curr_items=ws.items + 100, curr_deletes=100)
            self.assertNotIn(key.string, removed_keys)

    def test_collisions(self):
        ws = WorkloadSettings(items=10 ** 5, workers=25, working_set=100,
                              working_set_access=100, working_set_moving_docs=0,
                              key_fmtr='decimal')

        keys = []
        for worker in range(ws.workers):
            generator = docgen.SequentialKey(worker, ws, prefix='test')
            keys += [key.string for key in generator]

        hashes = set()
        for key in keys:
            _hash = docgen.hex_digest(key)
            self.assertNotIn(_hash, hashes)
            hashes.add(_hash)

    def test_package_doc(self):
        ws = WorkloadSettings(items=10 ** 6, workers=100, working_set=15,
                              working_set_access=50, working_set_moving_docs=0,
                              key_fmtr='hex')

        generator = docgen.PackageDocument(avg_size=0)
        dates = set()
        for key in docgen.SequentialKey(sid=50, ws=ws, prefix='test'):
            doc = generator.next(key)
            dates.add(doc['shippingDate'])
            self.assertEqual(doc['minorAccountId'], doc['majorAccountId'])
        self.assertEqual(len(dates), ws.items // ws.workers)

    def test_incompressible_docs(self):
        size = 15 * 1024
        generator = docgen.IncompressibleString(avg_size=size)
        doc = generator.next(key=docgen.Key(number=0, prefix='', fmtr=''))
        self.assertEqual(len(doc), size)


class QueryTest(TestCase):

    def test_n1ql_query_gen_q1(self):
        queries = [{
            'statement': 'SELECT * FROM `bucket-1` USE KEYS[$1];',
            'args': '["{key}"]',
        }]

        if sdk_major_version == 2:
            qg = N1QLQueryGen(queries=queries)
        elif sdk_major_version >= 3:
            qg = N1QLQueryGen(queries=queries, query_weight=[1])

        for key in 'n1ql-0123456789', 'n1ql-9876543210':
            if sdk_major_version >= 3:
                stmt, queryopts = qg.next(key, doc={})
                self.assertEqual(queryopts['adhoc'], False)
                self.assertEqual(str(queryopts['scan_consistency']),
                                 'QueryScanConsistency.NOT_BOUNDED')
                self.assertEqual(queryopts['positional_parameters'], [key])
            else:
                query = qg.next(key, doc={})
                self.assertEqual(query.adhoc, False)
                self.assertEqual(query.consistency, 'not_bounded')
                self.assertEqual(query._body['args'], [key])

    def test_n1ql_query_gen_q2(self):
        queries = [{
            'statement': 'SELECT * FROM `bucket-1` WHERE email = $1;',
            'args': '["{email}"]',
            'scan_consistency': 'request_plus',
        }]

        if sdk_major_version == 2:
            qg = N1QLQueryGen(queries=queries)
        elif sdk_major_version >= 3:
            qg = N1QLQueryGen(queries=queries, query_weight=[1])

        for doc in {'email': 'a@a.com'}, {'email': 'b@b.com'}:
            if sdk_major_version >= 3:
                stmt, queryopts = qg.next(key='n1ql-0123456789', doc=doc)
                self.assertEqual(str(queryopts['scan_consistency']),
                                 'QueryScanConsistency.REQUEST_PLUS')
                self.assertEqual(queryopts['positional_parameters'],
                                 [doc['email']])
            else:
                query = qg.next(key='n1ql-0123456789', doc=doc)
                self.assertEqual(query.consistency, 'request_plus')
                self.assertEqual(query._body['args'], [doc['email']])


class BigFunTest(TestCase):

    def test_unique_statements(self):
        queries = "perfrunner/workloads/analytics/bigfun/queries_with_index.yaml"
        for query in new_queries(queries):
            statements = set()
            for i in range(10):
                self.assertNotIn(query.statement, statements)
                statements.add(query.statement)


class PipelineTest(TestCase):
    def test_existence_of_test_configs(self):
        """Check if all test configs in the pipelines are present in the tests directory."""
        all_missing_test_configs = {}
        filenames_to_paths = {}

        test_config_keys = ["test_config", "test", "analytics_test_config", "kv_test_config"]

        for root, _, files in os.walk("tests"):
            for file in files:
                if not file.endswith(".test"):
                    continue

                if file not in filenames_to_paths:
                    filenames_to_paths[file] = []
                filenames_to_paths[file].append(root)

        for fn in glob.glob("tests/pipelines/*.json"):
            with open(fn, "r") as f:
                test_cases = json.load(f)

            for stage, stage_tests in test_cases.items():
                test_configs = [
                    t
                    for test in stage_tests
                    for t in [test[k] for k in test_config_keys if k in test]
                ]
                missing_stage_test_configs = []

                for test_config in test_configs:
                    parent_path = str(Path(test_config).parent)
                    name = Path(test_config).name

                    if (paths := filenames_to_paths.get(name, [])) and parent_path == ".":
                        continue
                    elif any(root.endswith(parent_path) for root in paths):
                        continue
                    else:
                        missing_stage_test_configs.append(test_config)

                if missing_stage_test_configs:
                    if fn not in all_missing_test_configs:
                        all_missing_test_configs[fn] = {}
                    all_missing_test_configs[fn][stage] = missing_stage_test_configs

        self.assertDictEqual(
            all_missing_test_configs,
            {},
            "\nTest configs from the following pipeline files are missing: \n"
            + pretty_dict(all_missing_test_configs),
        )

    def test_stages(self):
        stages = {'Analytics', 'Eventing', 'FTS', 'Tools', 'Views',
                  'GSI', 'GSI-DGM',
                  'N1QL', 'N1QL-Windows', 'N1QL-Arke', 'YCSB', 'YCSB-Hebe',
                  'KV', 'KV-DGM', 'KV-Windows', 'KV-Athena', 'KV-Hercules',
                  'Rebalance', 'Rebalance-C1', 'Rebalance-C2', 'Rebalance-Demeter',
                  'Rebalance-Large-Scale', 'Rebalance-Large-Scale-C1', 'Rebalance-Large-Scale-C2',
                  'XDCR', 'XDCR-Windows', 'XDCR-C1', 'XDCR-C2'}
        for pipeline in ('tests/pipelines/weekly-watson.json',
                         'tests/pipelines/weekly-spock.json',
                         'tests/pipelines/weekly-vulcan.json',
                         'tests/pipelines/weekly-alice.json'):
            with open(pipeline) as fh:
                test_cases = json.load(fh)
                self.assertEqual(stages, set(test_cases), pipeline)


class LocalShellTest(TestCase):
    def test_capture_returns_stripped_stdout_with_attributes(self):
        with shell.quiet():
            result = shell.local("echo hello && echo oops >&2", capture=True)
        self.assertEqual(result, "hello")
        self.assertEqual(result.stdout, "hello")
        self.assertEqual(result.stderr, "oops")
        self.assertEqual(result.return_code, 0)
        self.assertTrue(result.succeeded)
        self.assertFalse(result.failed)

    def test_output_state_shared_with_remote_api(self):
        # RemoteHelper sets state.output.stdout/running from its verbose flag; that
        # must control local() too, like fabric.state.output did (Fabric 1 parity).
        self.assertIs(api.output, shell.output)
        self.assertIs(api.state.output, shell.output)

    def test_non_verbose_discards_output_and_echo(self):
        saved_running, saved_stdout = shell.output.running, shell.output.stdout
        try:
            shell.output.running = shell.output.stdout = False
            self.assertTrue(shell._is_hidden("running"))
            self.assertTrue(shell._is_hidden("output"))
            result = shell.local("true")  # passthrough mode routes to devnull, no echo
            self.assertEqual(result.return_code, 0)
        finally:
            shell.output.running, shell.output.stdout = saved_running, saved_stdout
        self.assertFalse(shell._is_hidden("output"))

    def test_stderr_stays_visible_in_non_verbose_mode(self):
        # Fabric 1 keyed the streams separately and RemoteHelper only disables stdout,
        # so error text from local commands must survive non-verbose runs.
        saved = shell.output.stdout
        try:
            shell.output.stdout = False
            self.assertTrue(shell._is_hidden("output"))
            self.assertFalse(shell._is_hidden("stderr"))
        finally:
            shell.output.stdout = saved
        with shell.hide("output"):  # per-call hide("output") covers both streams
            self.assertTrue(shell._is_hidden("stderr"))

    def test_passthrough_mode_inherits_stdio(self):
        # capture=False with nothing hidden: child stdout/stderr inherit from the
        # parent and the result string is empty, but attributes are still populated.
        result = shell.local("true")
        self.assertEqual(result, "")
        self.assertEqual(result.return_code, 0)
        self.assertTrue(result.succeeded)

    def test_failure_aborts_by_default(self):
        with self.assertRaises(SystemExit):
            with shell.hide("everything"):
                shell.local("exit 1", capture=True)

    def test_warn_only_returns_failed_result(self):
        with shell.quiet():
            result = shell.local("exit 7", capture=True)
        self.assertEqual(result.return_code, 7)
        self.assertTrue(result.failed)
        self.assertFalse(result.succeeded)

    def test_settings_with_hide_and_warn_only(self):
        with shell.settings(shell.hide("output", "warnings"), warn_only=True):
            result = shell.local("exit 3")
        self.assertEqual(result.return_code, 3)

    def test_settings_rejects_unknown_keys(self):
        with self.assertRaises(TypeError):
            with shell.settings(host_string="node-1"):
                pass

    def test_lcd_nests_relative_paths(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sub_dir = os.path.join(tmp_dir, "sub")
            os.mkdir(sub_dir)
            with shell.quiet(), shell.lcd(tmp_dir), shell.lcd("sub"):
                result = shell.local("pwd", capture=True)
        self.assertEqual(os.path.realpath(result), os.path.realpath(sub_dir))

    def test_lcd_restores_previous_directory(self):
        with shell.quiet():
            with shell.lcd("/"):
                pass
            result = shell.local("pwd", capture=True)
        self.assertEqual(os.path.realpath(result), os.path.realpath(os.getcwd()))

    def test_shell_env_exports_variables(self):
        with shell.quiet(), shell.shell_env(FOO="bar", BAZ="qux"):
            result = shell.local("echo $FOO-$BAZ", capture=True)
        self.assertEqual(result, "bar-qux")

    def test_shell_executable_override(self):
        with shell.quiet():
            result = shell.local("echo $0", capture=True, shell="/bin/bash")
        self.assertEqual(result, "/bin/bash")

    def test_command_attributes_record_real_command(self):
        with shell.quiet(), shell.lcd("/tmp"), shell.shell_env(FOO="bar"):
            result = shell.local("true", capture=True)
        self.assertEqual(result.command, "true")
        self.assertIn("cd /tmp", result.real_command)
        self.assertIn('export FOO="bar"', result.real_command)


class RemoteApiTest(TestCase):
    def setUp(self):
        self.created = {}

        def factory(host, config, gateway=None):
            session = executor.FakeSession(host=host)
            session.config = config
            session.gateway = gateway
            responses = self.scripted.get(host, {})
            session.responses.update(responses)
            self.created[host] = session
            return session

        self.scripted = {}
        self._saved_pool = api.pool
        api.pool = executor.ConnectionPool(factory)
        self._saved_cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)

    def tearDown(self):
        os.chdir(self._saved_cwd)
        self._tmp.cleanup()
        api.pool = self._saved_pool

    def test_run_wraps_command_in_login_shell(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run("echo hello")
        command, kwargs = self.created["node-1"].commands[0]
        self.assertEqual(command, '/bin/bash -l -c "echo hello"')
        self.assertTrue(kwargs["pty"])

    def test_run_escapes_shell_characters(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run('echo "$HOME" `id`')
            api.run('echo "$HOME"', shell_escape=False)
        escaped, _ = self.created["node-1"].commands[0]
        raw, _ = self.created["node-1"].commands[1]
        self.assertEqual(escaped, '/bin/bash -l -c "echo \\"\\$HOME\\" \\`id\\`"')
        self.assertEqual(raw, '/bin/bash -l -c "echo "$HOME""')

    def test_cd_and_shell_env_prefixes(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            with api.cd("/tmp/perfrunner"), api.cd("worker"), api.shell_env(GOGC="300"):
                api.run("make")
        command, _ = self.created["node-1"].commands[0]
        self.assertEqual(
            command, '/bin/bash -l -c "cd /tmp/perfrunner/worker && export GOGC=\\"300\\" && make"'
        )

    def test_connection_reuse_across_calls(self):
        with api.settings(api.hide("everything"), host_string="node-1"):
            api.run("true")
            api.run("true")
        self.assertEqual(len(self.created), 1)
        self.assertEqual(len(self.created["node-1"].commands), 2)

    def test_execute_parallel_returns_dict_by_host(self):
        hosts = ["h1", "h2", "h3"]
        wrapped = '/bin/bash -l -c "hostname"'
        for host in hosts:
            self.scripted[host] = {wrapped: executor.RunResult(f"out-{host}", "", 0)}

        @api.parallel
        def task():
            return str(api.run("hostname", quiet=True))

        results = api.execute(task, hosts=hosts)
        self.assertEqual(results, {host: f"out-{host}" for host in hosts})
        for host in hosts:
            self.assertEqual(len(self.created[host].commands), 1)

    def test_execute_parallel_all_hosts_complete_despite_failure(self):
        # One failing host must not discard the other hosts' work, and the
        # original exception type must surface (not a generic wrapper).
        wrapped = '/bin/bash -l -c "hostname"'
        self.scripted = {
            "h1": {wrapped: executor.RunResult("out-h1", "", 0)},
            "h2": {wrapped: executor.CommandTimeout("timed out")},
            "h3": {wrapped: executor.RunResult("out-h3", "", 0)},
        }

        @api.parallel
        def task():
            return str(api.run("hostname", quiet=True, timeout=5))

        with self.assertRaises(executor.CommandTimeout):
            api.execute(task, hosts=["h1", "h2", "h3"])
        for host in ("h1", "h2", "h3"):
            self.assertEqual(len(self.created[host].commands), 1)

    def test_execute_serial_lambda(self):
        results = api.execute(lambda: api.run("true", quiet=True), hosts=["h1"])
        self.assertIn("h1", results)

    def test_run_failure_aborts_by_default(self):
        self.scripted["node-1"] = {'/bin/bash -l -c "false"': executor.RunResult("", "", 1)}
        with self.assertRaises(SystemExit):
            with api.settings(api.hide("everything"), host_string="node-1"):
                api.run("false")

    def test_run_failure_with_warn_only(self):
        self.scripted["node-1"] = {'/bin/bash -l -c "false"': executor.RunResult("", "", 1)}
        with api.settings(api.hide("everything"), host_string="node-1"):
            result = api.run("false", warn_only=True)
        self.assertEqual(result.return_code, 1)
        self.assertTrue(result.failed)

    def test_run_failure_reports_stderr(self):
        wrapped = '/bin/bash -l -c "systemctl restart couchbase-server"'
        self.scripted["n1"] = {
            wrapped: executor.RunResult("", "Job for couchbase-server failed", 1)
        }
        with self.assertLogs(level="WARNING") as logs:
            with api.settings(api.hide("running", "output"), host_string="n1"):
                result = api.run("systemctl restart couchbase-server", warn_only=True, pty=False)
        self.assertEqual(result.stderr, "Job for couchbase-server failed")
        self.assertTrue(any("Job for couchbase-server failed" in line for line in logs.output))

    def test_command_timeout_propagates(self):
        self.scripted["node-1"] = {
            '/bin/bash -l -c "sleep 100"': executor.CommandTimeout("timed out")
        }
        with self.assertRaises(executor.CommandTimeout):
            with api.settings(api.hide("everything"), host_string="node-1"):
                api.run("sleep 100", timeout=10)

    def test_get_glob_with_default_host_layout(self):
        self.scripted["10.1.1.1"] = {}
        with api.settings(api.hide("everything"), host_string="10.1.1.1"):
            session = api.pool.session("10.1.1.1", executor.SessionConfig())
            session.files = {"/tmp/aaa.zip": "x", "/tmp/bbb.zip": "y", "/tmp/keep.log": "z"}
            downloaded = api.get("/tmp/*.zip")
        self.assertEqual(
            sorted(session.downloads),
            [
                ("/tmp/aaa.zip", os.path.join("10.1.1.1", "tmp", "aaa.zip")),
                ("/tmp/bbb.zip", os.path.join("10.1.1.1", "tmp", "bbb.zip")),
            ],
        )
        self.assertEqual(len(downloaded), 2)

    def test_get_relative_path_uses_cd(self):
        with api.settings(api.hide("everything"), host_string="w1"):
            session = api.pool.session("w1", executor.SessionConfig())
            session.files = {"/worker/perfrunner/worker_1.log": "log"}
            with api.cd("/worker/perfrunner"):
                api.get("worker_*.log", local_path="celery/")
        self.assertEqual(
            session.downloads,
            [("/worker/perfrunner/worker_1.log", os.path.join("celery", "worker_1.log"))],
        )

    def test_get_single_file_default_lands_at_host_slash_basename(self):
        # Contract with the debug flow: a bare get() of one file must land exactly one level deep.
        # Fabric 1 collapsed %(path)s to the basename for single-file downloads.
        with api.settings(api.hide("everything"), host_string="10.1.1.3"):
            session = api.pool.session("10.1.1.3", executor.SessionConfig())
            session.files = {"/tmp/abc123.zip": "z"}
            downloaded = api.get("/tmp/abc123.zip")
        self.assertEqual(downloaded, [os.path.join("10.1.1.3", "abc123.zip")])

    def test_get_glob_default_keeps_full_path(self):
        # Glob downloads keep the full remote path under <host>/ to avoid collisions.
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            session.files = {"/tmp/a.zip": "a", "/tmp/b.zip": "b"}
            downloaded = api.get("/tmp/*.zip")
        self.assertEqual(
            sorted(downloaded),
            [os.path.join("h1", "tmp", "a.zip"), os.path.join("h1", "tmp", "b.zip")],
        )

    def test_put_directory_recursively(self):
        os.makedirs("inbox/sub")
        Path("inbox/chain.pem").write_text("pem")
        Path("inbox/sub/node.key").write_text("key")
        with api.settings(api.hide("everything"), host_string="n1"):
            uploaded = api.put("inbox", "/opt/couchbase/var/lib/couchbase")
        session = self.created["n1"]
        self.assertIn(
            ("inbox/chain.pem", "/opt/couchbase/var/lib/couchbase/inbox/chain.pem"),
            [(os.path.relpath(local), remote) for local, remote in session.uploads],
        )
        self.assertIn("/opt/couchbase/var/lib/couchbase/inbox/sub", session.dirs)
        self.assertEqual(len(uploaded), 2)

    def test_get_directory_downloads_tree_recursively(self):
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            session.dirs = {"/data", "/data/a", "/data/b"}
            session.files = {"/data/f0": "0", "/data/a/f1": "1", "/data/b/f2": "2"}
            downloaded = api.get("/data", local_path="out/")
        self.assertEqual(sorted(session.downloads), [
            ("/data/a/f1", os.path.join("out", "data", "a", "f1")),
            ("/data/b/f2", os.path.join("out", "data", "b", "f2")),
            ("/data/f0", os.path.join("out", "data", "f0")),
        ])
        self.assertEqual(sorted(downloaded), sorted(local for _, local in session.downloads))
        self.assertTrue(os.path.isdir(os.path.join("out", "data", "a")))

    def test_download_tree_depth_guard(self):
        # SFTP has no inode info for cycle detection; a symlink loop must fail fast
        # with a clear error instead of a RecursionError.
        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
            path = ""
            for level in range(api.MAX_TREE_DEPTH + 2):
                path = f"{path}/d"
                session.dirs.add(path)
            with self.assertRaises(RuntimeError):
                api.get("/d")

    def test_concurrent_channels_capped_per_session(self):
        # Nested parallel decorators stack many threads on one pooled connection;
        # channel opens must be capped below sshd MaxSessions.
        counters = {"current": 0, "max": 0}
        guard = threading.Lock()

        def tracked_run_raw(command, pty=True, timeout=None):
            with guard:
                counters["current"] += 1
                counters["max"] = max(counters["max"], counters["current"])
            time.sleep(0.02)
            with guard:
                counters["current"] -= 1
            return executor.RunResult("", "", 0)

        with api.settings(api.hide("everything"), host_string="h1"):
            session = api.pool.session("h1", executor.SessionConfig())
        session.run_raw = tracked_run_raw

        def worker():
            with api.settings(api.hide("everything"), host_string="h1"):
                api.run("true", quiet=True)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertGreater(counters["max"], 1)
        self.assertLessEqual(counters["max"], executor.Session.MAX_CONCURRENT_CHANNELS)

    def test_put_file_into_existing_remote_directory(self):
        # Fabric 1 parity: put("root.pem", "<dir>") with an existing remote directory
        # (no trailing slash) must land <dir>/root.pem, not open the dir for write.
        Path("root.pem").write_text("pem")
        with api.settings(api.hide("everything"), host_string="c1"):
            session = api.pool.session("c1", executor.SessionConfig())
            session.dirs.add("/worker/perfrunner")
            uploaded = api.put("root.pem", "/worker/perfrunner")
        self.assertEqual(uploaded, ["/worker/perfrunner/root.pem"])
        self.assertEqual(session.uploads, [("root.pem", "/worker/perfrunner/root.pem")])

    def test_gateway_creates_jump_session(self):
        with api.settings(api.hide("everything"), host_string="kafka-1", gateway="jump-1"):
            api.run("true")
        self.assertIn("jump-1", self.created)
        self.assertIs(self.created["kafka-1"].gateway, self.created["jump-1"])

    def test_append_is_idempotent_grep(self):
        with api.settings(api.hide("everything"), host_string="n1"):
            api.append("/opt/tomcat/bin/setenv.sh", "export LD_LIBRARY_PATH=/x")
        command, _ = self.created["n1"].commands[0]
        self.assertIn("grep -qF -- 'export LD_LIBRARY_PATH=/x'", command)
        self.assertIn("| tee -a /opt/tomcat/bin/setenv.sh", command)
        self.assertNotIn("sudo", command)

    def test_append_with_use_sudo(self):
        with api.settings(api.hide("everything"), host_string="n1"):
            api.append("/opt/tomcat/bin/setenv.sh", "export LD_LIBRARY_PATH=/x", use_sudo=True)
        command, _ = self.created["n1"].commands[0]
        self.assertIn("sudo grep -qF", command)
        self.assertIn("| sudo tee -a /opt/tomcat/bin/setenv.sh", command)

    def test_state_aliases(self):
        self.assertIs(api.state.env, api.env)
        self.assertIs(api.state.output, api.output)

    def test_settings_rejects_unknown_keys(self):
        with self.assertRaises(TypeError):
            with api.settings(bogus=1):
                pass


class RemoteCharacterisationTest(TestCase):
    """Pin the remote layer's contract: host targeting, command strings, result shapes.

    Uses FakeSession as the executor, so no SSH connection is made. The command strings
    are the interface to remote machines. Any refactor of the execution layer or the
    topology decorators must keep them identical.
    """

    SPEC = (
        "[clusters]\n"
        "test =\n"
        "    10.0.0.1:kv\n"
        "    10.0.0.2:kv\n"
        "    10.0.0.3:index\n"
        "\n"
        "[clients]\n"
        "hosts =\n"
        "    10.0.1.1\n"
        "\n"
        "[storage]\n"
        "data = /data\n"
        "\n"
        "[metadata]\n"
        "cluster = test\n"
    )

    def setUp(self):
        self.created = {}
        self.scripted = {}

        def factory(host, config, gateway=None):
            session = executor.FakeSession(host=host)
            session.responses.update(self.scripted.get(host, {}))
            self.created[host] = session
            return session

        self._saved_pool = api.pool
        api.pool = executor.ConnectionPool(factory)

        spec_file = tempfile.NamedTemporaryFile(mode="w", suffix=".spec", delete=False)
        spec_file.write(self.SPEC)
        spec_file.close()
        self.spec_fname = spec_file.name
        self.cluster_spec = ClusterSpec()
        self.cluster_spec.parse(self.spec_fname, override=None)

    def tearDown(self):
        api.pool = self._saved_pool
        os.unlink(self.spec_fname)

    def _remote(self):
        from perfrunner.remote.linux import RemoteLinux

        return RemoteLinux(self.cluster_spec)

    def test_construction_detects_distro_on_master_only(self):
        self._remote()
        self.assertEqual(list(self.created), ["10.0.0.1"])
        commands = [command for command, _ in self.created["10.0.0.1"].commands]
        self.assertEqual(len(commands), 2)
        self.assertIn("grep ^ID= /etc/os-release", commands[0])
        self.assertIn("grep ^VERSION_ID= /etc/os-release", commands[1])

    def test_reset_swap_runs_on_all_servers(self):
        remote = self._remote()
        remote.reset_swap()
        expected = '/bin/bash -l -c "swapoff --all && swapon --all"'
        for server in ("10.0.0.1", "10.0.0.2", "10.0.0.3"):
            commands = [command for command, _ in self.created[server].commands]
            self.assertIn(expected, commands)

    def test_master_server_decorator_targets_first_server(self):
        remote = self._remote()
        remote.enable_nonlocal_diag_eval()
        command, kwargs = self.created["10.0.0.1"].commands[-1]
        self.assertIn("diag/eval", command)
        self.assertFalse(kwargs["pty"])
        self.assertNotIn("10.0.0.2", self.created)

    def test_detect_core_dumps_returns_dict_per_host(self):
        wrapped = '/bin/bash -l -c "ls /data/core*"'
        self.scripted = {
            "10.0.0.1": {wrapped: executor.RunResult("/data/core-memcached-1", "", 0)},
            "10.0.0.2": {wrapped: executor.RunResult("", "", 2)},
            "10.0.0.3": {wrapped: executor.RunResult("", "", 2)},
        }
        remote = self._remote()
        dumps = remote.detect_core_dumps()
        self.assertEqual(
            dumps, {"10.0.0.1": ["/data/core-memcached-1"], "10.0.0.2": [], "10.0.0.3": []}
        )

    def test_all_clients_decorator_targets_workers(self):
        remote = self._remote()
        remote.terminate_client_processes()
        commands = [command for command, _ in self.created["10.0.1.1"].commands]
        self.assertTrue(any("killall -9" in command for command in commands))


class ConnectionPoolTest(TestCase):
    """Session lifecycle in the pool: reuse, probe-on-idle liveness, and fork safety."""

    def setUp(self):
        self.pool = executor.ConnectionPool(
            lambda host, config, gateway=None: executor.FakeSession(host=host)
        )
        self.config = executor.SessionConfig()

    def test_no_probe_when_recently_used(self):
        first = self.pool.session("h1", self.config)
        second = self.pool.session("h1", self.config)
        self.assertIs(first, second)
        self.assertEqual(first.probes, 0)

    def test_probe_after_idle_reuses_healthy_session(self):
        session = self.pool.session("h1", self.config)
        session.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        again = self.pool.session("h1", self.config)
        self.assertIs(session, again)
        self.assertEqual(session.probes, 1)

    def test_dead_idle_session_is_replaced(self):
        session = self.pool.session("h1", self.config)
        session.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        session.probe_error = executor.NetworkError("dropped by NAT")
        replacement = self.pool.session("h1", self.config)
        self.assertIsNot(session, replacement)
        self.assertTrue(session.closed)
        self.assertEqual(session.probes, 1)

    def test_slow_probe_does_not_block_other_hosts(self):
        # The pool lock only guards its dicts; a dead host's probe (up to the channel
        # open timeout) must not stall parallel checkouts of healthy hosts.
        slow = self.pool.session("slow-host", self.config)
        slow.last_used -= executor.ConnectionPool.PROBE_AFTER_IDLE + 1
        slow.probe_delay = 1.0

        prober = threading.Thread(
            target=self.pool.session, args=("slow-host", self.config))
        prober.start()
        time.sleep(0.1)  # let the probe start and hold slow-host's key lock

        t0 = time.time()
        self.pool.session("healthy-host", self.config)
        elapsed = time.time() - t0
        prober.join()
        self.assertLess(elapsed, 0.5)
        self.assertEqual(slow.probes, 1)

    def test_dead_gateway_session_closed_before_replacement(self):
        first = self.pool.session("kafka-1", self.config, gateway="jump-1")
        gateway_key = ("jump-1", self.config.user, None)
        old_gateway = self.pool._sessions[gateway_key]

        old_gateway.active = False  # gateway died; host session rides it, so it dies too
        first.active = False
        self.pool.session("kafka-1", self.config, gateway="jump-1")

        new_gateway = self.pool._sessions[gateway_key]
        self.assertIsNot(old_gateway, new_gateway)
        self.assertTrue(old_gateway.closed)

    def test_forked_child_discards_inherited_sessions_without_closing(self):
        # Forked children (e.g. cbagent collector processes) must neither reuse nor close the
        # parent's SSH sockets: sharing the encrypted stream corrupts it,
        # and closing sends disconnects on a socket the parent still owns.
        parent_session = self.pool.session("h1", self.config)
        self.pool._pid -= 1  # simulate being in a forked child
        child_session = self.pool.session("h1", self.config)
        self.assertIsNot(parent_session, child_session)
        self.assertFalse(parent_session.closed)


class SSHSessionTest(TestCase):
    """Construction-time behaviour of the Fabric-backed session (no connection made)."""

    def test_host_key_policy_follows_disable_known_hosts(self):
        from paramiko.client import AutoAddPolicy, RejectPolicy

        default = executor.SSHSession("10.9.9.9", executor.SessionConfig())
        self.assertIsInstance(default._conn.client._policy, AutoAddPolicy)

        strict = executor.SSHSession(
            "10.9.9.9", executor.SessionConfig(disable_known_hosts=False)
        )
        self.assertIsInstance(strict._conn.client._policy, RejectPolicy)


def fake_task_result(task_id: str, members: list = None,
                     on_get: Callable = None) -> SimpleNamespace:
    """Build a stand-in for a celery AsyncResult, or for a GroupResult if members are given.

    `on_get` stands in for what waiting on the real result would do, and is passed the timeout it
    was called with. By default it returns at once, as a task that has already finished would.
    """
    def get(timeout=None, propagate=True):
        if on_get is not None:
            return on_get(timeout)

    return SimpleNamespace(id=task_id, results=members, get=get)


# A task process shaped like spring: it traps SIGTERM and asks its worker child to stop, and the
# worker dumps its stats on the way out. The worker leaves SIGTERM at its default disposition,
# exactly as spring's worker processes do, so signalling the worker directly kills it before it can
# dump anything. Whether the stats file gets written is therefore the difference between a graceful
# shutdown and an abrupt kill.
GRACEFUL_TASK = """
import os, signal, sys, time

ready_file, stop_file, stats_file = sys.argv[1], sys.argv[2], sys.argv[3]

if os.fork() == 0:
    deadline = time.time() + 60
    while not os.path.exists(stop_file):
        if time.time() > deadline:
            os._exit(1)
        time.sleep(0.05)
    open(stats_file, "w").write("stats dumped")
    os._exit(0)

def shut_down(signum, frame):
    open(stop_file, "w").write("stop")
    os.wait()
    sys.exit(0)

signal.signal(signal.SIGTERM, shut_down)
open(ready_file, "w").write("ready")
time.sleep(60)
"""

# A child process which ignores SIGTERM entirely, to exercise the escalation path
STUBBORN_CHILD = """
import signal, sys, time

signal.signal(signal.SIGTERM, signal.SIG_IGN)
open(sys.argv[1], "w").write("ready")
time.sleep(60)
"""


class CeleryTaskAbortTest(TestCase):
    """Cover aborting locally running celery tasks.

    The celery plumbing itself (that a task really gets submitted, run and revoked) needs a broker
    and a running worker, so it isn't covered here. What is covered is everything perfrunner does
    around it: recording task PIDs, resolving them back to processes, and terminating those
    processes.
    """

    def setUp(self):
        # Task pidfiles are written relative to the working directory, so run in a scratch one
        self._cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._children = []

    def tearDown(self):
        for child in self._children:
            # Kill the whole tree: the task processes here fork worker children of their own
            try:
                for grandchild in psutil.Process(child.pid).children(recursive=True):
                    with contextlib.suppress(psutil.Error):
                        grandchild.kill()
            except psutil.Error:
                pass
            try:
                child.kill()
                child.wait(timeout=10)
            except Exception:
                pass
        os.chdir(self._cwd)
        self._tmpdir.cleanup()

    def start_child(self, source: str, *args) -> subprocess.Popen:
        """Start a child process from the given source and wait until it is ready to be signalled.

        Waiting matters: signalling before the child has installed its handler would make a
        graceful shutdown look like an abrupt one.
        """
        ready_file = Path(f"ready-{len(self._children)}")
        child = subprocess.Popen([sys.executable, "-c", source, str(ready_file), *args],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self._children.append(child)
        deadline = time.time() + 30
        while not ready_file.exists() and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(ready_file.exists(), "child process did not start")
        return child

    @staticmethod
    def process_stopped(pid: int) -> bool:
        """Whether the process has stopped running.

        A process which has exited but not yet been reaped counts as stopped, since which of us
        reaps it depends on timing we don't control.
        """
        try:
            process = psutil.Process(pid)
            return not process.is_running() or process.status() == psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return True

    @classmethod
    def wait_until_stopped(cls, pid: int, timeout: int = 15) -> bool:
        """Whether the process stops running within the timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if cls.process_stopped(pid):
                return True
            time.sleep(0.05)
        return False

    @staticmethod
    def write_pidfile(task_id: str, pid: int):
        pidfile = Path(TASK_PIDFILE_DIR) / f"{task_id}.pid"
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        pidfile.write_text(str(pid))
        return pidfile

    @staticmethod
    def local_worker_manager(terminate_timeout: int = 5) -> LocalWorkerManager:
        """Build a LocalWorkerManager without the celery worker set up done by __init__."""
        manager = LocalWorkerManager.__new__(LocalWorkerManager)
        manager.fg_async_results = []
        manager.bg_async_results = []
        manager._aborted_results = []
        manager.TASK_TERMINATE_TIMEOUT = terminate_timeout
        return manager

    def test_store_pid_writes_pidfile_while_task_runs(self):
        observed = {}

        @store_pid
        def task():
            pidfile = Path(TASK_PIDFILE_DIR) / "task-1.pid"
            observed["exists"] = pidfile.exists()
            observed["pid"] = pidfile.read_text()

        task(SimpleNamespace(request=SimpleNamespace(id="task-1")))

        self.assertTrue(observed["exists"])
        self.assertEqual(observed["pid"], str(os.getpid()))

    def test_store_pid_removes_pidfile_when_task_finishes(self):
        @store_pid
        def task():
            pass

        task(SimpleNamespace(request=SimpleNamespace(id="task-1")))

        # A leftover pidfile would let a recycled PID be signalled by mistake later on
        self.assertFalse((Path(TASK_PIDFILE_DIR) / "task-1.pid").exists())

    def test_store_pid_removes_pidfile_when_task_raises(self):
        @store_pid
        def task():
            raise RuntimeError("task failed")

        with self.assertRaises(RuntimeError):
            task(SimpleNamespace(request=SimpleNamespace(id="task-1")))

        self.assertFalse((Path(TASK_PIDFILE_DIR) / "task-1.pid").exists())

    def test_task_ids_of_single_result(self):
        self.assertEqual(LocalWorkerManager._task_ids(fake_task_result("task-1")), ["task-1"])

    def test_task_ids_of_group_result(self):
        # A GroupResult's own id is a group id, which no task writes a pidfile for, so the ids of
        # its members are what we need
        group = fake_task_result(
            "group-1", members=[fake_task_result("task-1"), fake_task_result("task-2")]
        )

        self.assertEqual(LocalWorkerManager._task_ids(group), ["task-1", "task-2"])

    def test_task_process_of_task_that_never_started(self):
        # No pidfile at all: the task was queued but never picked up by a worker
        self.assertIsNone(LocalWorkerManager._task_process("task-1"))

    def test_task_process_of_finished_task(self):
        # A pidfile naming a PID that is gone: the task already finished. This must not raise,
        # because tasks are aborted on successful runs too.
        child = self.start_child(STUBBORN_CHILD)
        pid = child.pid
        child.kill()
        child.wait(timeout=10)
        self.write_pidfile("task-1", pid)

        self.assertIsNone(LocalWorkerManager._task_process("task-1"))

    def test_task_process_of_running_task(self):
        child = self.start_child(STUBBORN_CHILD)
        self.write_pidfile("task-1", child.pid)

        process = LocalWorkerManager._task_process("task-1")

        self.assertIsNotNone(process)
        self.assertEqual(process.pid, child.pid)

    def test_abort_task_lets_the_task_shut_down_its_workers_gracefully(self):
        # Spring shuts its worker processes down from its own SIGTERM handler so that they get to
        # dump their stats, so only the task process itself may be signalled. Signalling its
        # workers as well kills them before they can dump anything.
        stats_file = Path("worker-stats")
        child = self.start_child(GRACEFUL_TASK, "stop-file", str(stats_file))
        self.write_pidfile("task-1", child.pid)
        manager = self.local_worker_manager()

        manager._abort_task(fake_task_result("task-1"))

        self.assertTrue(self.wait_until_stopped(child.pid), "task process was left running")
        self.assertTrue(
            stats_file.exists(), "worker process was killed before it could dump its stats"
        )
        self.assertEqual(stats_file.read_text(), "stats dumped")

    def test_abort_all_tasks_clears_the_result_lists(self):
        manager = self.local_worker_manager()
        manager.fg_async_results = [fake_task_result('task-1')]
        manager.bg_async_results = [fake_task_result('task-2')]

        manager.abort_all_tasks()

        # Aborted tasks are no longer tracked, so a later abort can't re-signal them
        self.assertEqual(manager.fg_async_results, [])
        self.assertEqual(manager.bg_async_results, [])

    def test_task_process_removes_the_pidfile(self):
        # A task that is killed outright never runs the `finally` in store_pid, so the reader has
        # to clean up instead - otherwise a recycled PID could be signalled later by mistake
        child = self.start_child(STUBBORN_CHILD)
        pidfile = self.write_pidfile('task-1', child.pid)

        self.assertIsNotNone(LocalWorkerManager._task_process('task-1'))

        self.assertFalse(pidfile.exists())

    def test_abort_all_tasks_waits_for_the_task_results(self):
        # Aborting must not return before the tasks have actually ended, because the caller goes on
        # to reconstruct measurements from the files the workers write while shutting down. The
        # task result is what signals that: the process running the task is a celery pool worker,
        # which is long-lived and does not exit when the task ends.
        waited = []
        manager = self.local_worker_manager()
        manager.fg_async_results = [
            fake_task_result('task-1', on_get=lambda timeout: waited.append('task-1'))
        ]
        manager.bg_async_results = [
            fake_task_result('task-2', on_get=lambda timeout: waited.append('task-2'))
        ]

        manager.abort_all_tasks()

        self.assertEqual(waited, ['task-1', 'task-2'])

    def test_abort_all_tasks_does_not_wait_forever(self):
        # Bounded, so that a task which ignores SIGTERM can't stall teardown indefinitely
        def never_finishes(timeout):
            # Stand in for a task that outlives the wait: celery's `get` blocks for the timeout it
            # was given and then raises, so an unbounded wait would block far longer
            time.sleep(timeout if timeout is not None else 30)
            raise TimeoutError('task is still running')

        manager = self.local_worker_manager(terminate_timeout=1)
        manager.fg_async_results = [fake_task_result('task-1', on_get=never_finishes)]

        started = time.time()
        manager.abort_all_tasks()

        self.assertLess(time.time() - started, 10, 'the wait was not bounded by the timeout')

    def test_abort_all_tasks_does_not_re_raise_task_failures(self):
        # An aborted task ending in failure is expected, and must not replace whatever the test was
        # actually doing (or failing with)
        def task_failed(*args, **kwargs):
            raise RuntimeError('task blew up')

        manager = self.local_worker_manager()
        manager.fg_async_results = [fake_task_result('task-1', on_get=task_failed)]

        manager.abort_all_tasks()  # must not raise

    def test_abort_all_tasks_is_best_effort(self):
        # Aborting runs on successful teardown too, so one task we cannot signal must not stop the
        # others from being signalled
        aborted = []

        class FailingWorkerManager(RemoteWorkerManager):
            def __init__(self):
                self.fg_async_results = [fake_task_result("task-1"), fake_task_result("task-2")]
                self.bg_async_results = [fake_task_result("task-3")]
                self._aborted_results = []

            def _abort_task(self, task_result):
                if task_result.id == "task-2":
                    raise RuntimeError("cannot signal this one")
                aborted.append(task_result.id)

        FailingWorkerManager().abort_all_tasks()

        self.assertEqual(aborted, ["task-1", "task-3"])

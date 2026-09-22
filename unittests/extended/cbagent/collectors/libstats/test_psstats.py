"""`PSStats.get_samples` must survive a malformed `top` sample, not just a missing one.

Seen in practice against a real client node whose `top` cannot read its terminfo
database: it printed a short diagnostic line instead of a process row, and
`stdout.split()[8]` raised `IndexError`. That exception unwound out of `get_samples`
before its `return samples` ran, so the rss/vsize already parsed from `ps` a moment
earlier were thrown away along with the missed CPU reading, and `Collector.collect`
logged a fresh traceback every interval for the rest of the run - see the perf.jenkins
run linked from the change that added this test.
"""

import os
from typing import TYPE_CHECKING
from unittest import TestCase
from unittest.mock import Mock

from perfrunner.remote import api

if TYPE_CHECKING:  # the runtime import is deferred to setUpClass, see below
    from cbagent.collectors.libstats.psstats import PSStats


class PSStatsGetSamplesTest(TestCase):

    PROCESS = "cbbackupmgr"

    @classmethod
    def setUpClass(cls):
        """Import the collector here; see McstatHistogramCollectorTest.setUpClass for why."""
        env_before = vars(api.env).copy()
        cls.addClassCleanup(vars(api.env).update, env_before)
        cls.addClassCleanup(vars(api.env).clear)

        os.environ.setdefault("WORKER_TYPE", "local")

        from cbagent.collectors.libstats.psstats import PSStats

        cls.collector_cls = PSStats

    def _collector(self, ps_stdout: str, top_stdout: str) -> "PSStats":
        """Build a collector whose two `run()` calls return the given `ps` then `top` output."""
        collector = self.collector_cls.__new__(self.collector_cls)
        collector.top_interval = 2
        collector.run = Mock(side_effect=[ps_stdout, top_stdout])
        return collector

    # A real 12-column `top -b` process row for pid 4021, 3.7% CPU.
    TOP_ROW = "4021 root 20 0 118m 21m 3392 R 3.7 0.1 0:00.42 cbbackupmgr"

    def test_a_normal_pair_of_samples_reports_rss_vsize_and_cpu(self):
        collector = self._collector(
            "4021 21504 121856 cbbackupmgr",
            self.TOP_ROW,
        )

        samples = collector.get_samples(self.PROCESS)

        self.assertEqual(
            samples,
            {
                "cbbackupmgr_rss": 21504 * 1024,
                "cbbackupmgr_vsize": 121856 * 1024,
                "cbbackupmgr_cpu": 3.7,
            },
        )

    def test_a_terminfo_error_in_place_of_the_top_row_costs_only_the_cpu_sample(self):
        """The exact shape seen in production: 4 words, none of them numeric at index 8."""
        collector = self._collector(
            "4021 21504 121856 cbbackupmgr",
            "terminals database is inaccessible",
        )

        samples = collector.get_samples(self.PROCESS)

        self.assertEqual(
            samples,
            {
                "cbbackupmgr_rss": 21504 * 1024,
                "cbbackupmgr_vsize": 121856 * 1024,
            },
            "rss/vsize were already parsed from ps and must survive a bad top sample",
        )

    def test_a_top_row_truncated_before_the_cpu_column_costs_only_the_cpu_sample(self):
        collector = self._collector(
            "4021 21504 121856 cbbackupmgr",
            "4021 root 20 0 118m 21m",
        )

        samples = collector.get_samples(self.PROCESS)

        self.assertNotIn("cbbackupmgr_cpu", samples)
        self.assertEqual(samples["cbbackupmgr_rss"], 21504 * 1024)

    def test_a_non_numeric_value_at_the_cpu_column_costs_only_the_cpu_sample(self):
        """Same field count as a real row, but the field `top` would put %CPU in isn't one."""
        collector = self._collector(
            "4021 21504 121856 cbbackupmgr",
            "4021 root 20 0 118m 21m 3392 R N/A 0.1 0:00.42 cbbackupmgr",
        )

        samples = collector.get_samples(self.PROCESS)

        self.assertNotIn("cbbackupmgr_cpu", samples)
        self.assertEqual(samples["cbbackupmgr_rss"], 21504 * 1024)

    def test_no_matching_process_in_ps_returns_no_samples_without_calling_top(self):
        collector = self._collector("", self.TOP_ROW)

        samples = collector.get_samples(self.PROCESS)

        self.assertEqual(samples, {})
        collector.run.assert_called_once()

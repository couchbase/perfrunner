"""Sample sequencing in `McstatHistogramStats`, driven by scripted histogram samples."""

import os
from collections.abc import Sequence
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest import TestCase

from perfrunner.remote import api
from unittests.extended.cbagent.mcstat_fixtures import mcstat_hist

if TYPE_CHECKING:  # the runtime import is deferred to setUpClass, see below
    from cbagent.collectors.kvstore_stats import McstatHistogramStats


class McstatHistogramCollectorTest(TestCase):
    """Cover how `McstatHistogramStats.sample` sequences one sample into the next.

    The collector keeps the previous sample to diff against and replaces it *before* diffing,
    so a pair it cannot diff costs one interval instead of every interval after it. The diff
    loop therefore has to read the saved previous samples rather than the ones just stored -
    read the wrong ones and every interval diffs a sample against itself, which reports
    nothing at all, silently, for the rest of the run.
    """

    NODE = "10.0.0.1"
    OTHER_NODE = "10.0.0.2"
    BUCKET = "bucket-1"

    # What one interval of the fixture below reports, for every percentile in PERCENTILES.
    INTERVAL = {
        "bg_load_p50": 10,
        "bg_load_p90": 18,
        "bg_load_p95": 19,
        "bg_load_p99": 19,
        "bg_load_p99.9": 19,
    }

    @classmethod
    def setUpClass(cls):
        """Import the collector here, and undo the import-time writes to the shared remote env.

        Importing any cbagent collector pulls in `cbagent.collectors.libstats.remotestats`,
        which rewrites `perfrunner.remote.api.env` as it loads - by design, but the
        `RemoteApiTest` in `perfrunner/remote/test_api.py` asserts against the pristine
        defaults. Keeping the import out of module scope and restoring the env means running
        these tests cannot change what those tests see.

        The same import chain reaches `perfrunner.helpers.worker`, which configures celery as
        it loads and refuses to do so unless it knows which kind of worker it is configuring.
        Declare one first. Importing only updates celery's configuration; it does not connect
        to a broker or start anything.
        """
        env_before = vars(api.env).copy()
        cls.addClassCleanup(vars(api.env).update, env_before)
        cls.addClassCleanup(vars(api.env).clear)

        os.environ.setdefault("WORKER_TYPE", "local")

        from cbagent.collectors.kvstore_stats import McstatHistogramStats

        cls.collector_cls = McstatHistogramStats

    def _collector(self, samples: Sequence[dict]) -> "McstatHistogramStats":
        """Build a collector yielding one of `samples` per `sample()`, recording its output."""
        collector = self.collector_cls.__new__(self.collector_cls)
        collector.prev_histograms = {}
        collector._prev_unreachable = set()
        collector._prev_without_stats = set()
        collector._prev_violations = set()
        collector._prev_truncated = set()
        collector._prev_malformed = set()
        collector._retained = set()
        collector.cluster = "cluster-1"
        collector.reported = []

        remaining = iter(samples)
        collector._get_node_list = lambda: [self.NODE]
        collector._get_timing_stats = lambda node: {self.BUCKET: {"bg_load": next(remaining)}}
        collector.update_metric_metadata = lambda *args, **kwargs: None
        collector.store = SimpleNamespace(
            append=lambda stats, **kwargs: collector.reported.append(stats)
        )
        return collector

    def test_the_first_sample_only_establishes_a_baseline(self):
        """One sample is not an interval, so there is nothing to report yet."""
        collector = self._collector([mcstat_hist([(10, 100), (20, 100)])])

        collector.sample()

        self.assertEqual(collector.reported, [])
        self.assertIn(self.BUCKET, collector.prev_histograms)

    def test_the_second_sample_reports_the_interval_between_the_two(self):
        collector = self._collector(
            [mcstat_hist([(10, 100), (20, 100)]), mcstat_hist([(10, 115), (20, 115)])]
        )

        collector.sample()
        collector.sample()

        self.assertEqual(
            collector.reported,
            [self.INTERVAL],
        )

    def test_a_reset_costs_one_interval_and_then_recovers(self):
        """The whole point of re-baselining: a restart must not silence the rest of the run."""
        collector = self._collector(
            [
                mcstat_hist([(10, 100), (20, 100)]),  # baseline
                mcstat_hist([(10, 5), (20, 5)]),  # counters went backwards
                mcstat_hist([(10, 20), (20, 20)]),  # accumulating again
            ]
        )

        collector.sample()
        collector.sample()

        self.assertEqual(collector.reported, [])
        self.assertEqual(collector._prev_violations, {(self.BUCKET, self.NODE, "bg_load")})

        collector.sample()

        self.assertEqual(
            collector.reported,
            [self.INTERVAL],
        )
        self.assertEqual(collector._prev_violations, set())

    def test_a_torn_read_is_reported_apart_from_a_counter_reset(self):
        """The two are rejected the same way but mean entirely different things.

        One says the emitter raced with recording and will be fine next sample, the other
        that something restarted the node or recreated the bucket. Reporting them together
        buries the second in the noise of the first.
        """
        torn = mcstat_hist([(10, 115)])
        torn["total"] = 202  # `data` lost its tail; the counter behind it did not
        collector = self._collector([mcstat_hist([(10, 100), (20, 100)]), torn])

        collector.sample()
        collector.sample()

        self.assertEqual(collector.reported, [])
        self.assertEqual(collector._prev_truncated, {(self.BUCKET, self.NODE, "bg_load")})
        self.assertEqual(collector._prev_violations, set())

    def test_a_torn_sample_does_not_become_the_baseline(self):
        """Baselining on a torn render hands its unshown observations to the next interval.

        They are the slowest ones the histogram holds, so they land where p99.9 is measured:
        ~14% high for that interval, against ~0 for p50 through p99. Keeping the last good
        sample costs a doubled window instead, which is a real interval either way.
        """
        good, torn, recovered = (
            mcstat_hist([(10, 100), (20, 100)]),
            mcstat_hist([(10, 115)]),
            mcstat_hist([(10, 130), (20, 230)]),
        )
        torn["total"] = 231  # `data` lost its tail; the counter behind it did not
        collector = self._collector([good, torn, recovered])

        collector.sample()
        collector.sample()

        self.assertEqual(
            collector.prev_histograms[self.BUCKET][self.NODE]["bg_load"],
            good,
            "the torn sample must not have replaced the baseline",
        )

        collector.sample()

        # The two-interval window the retained baseline gives. Had the torn sample been
        # baselined on instead, its unshown observations would land here, giving p50 14.
        self.assertEqual(
            collector.reported,
            [
                {
                    "bg_load_p50": 13,
                    "bg_load_p90": 18,
                    "bg_load_p95": 19,
                    "bg_load_p99": 19,
                    "bg_load_p99.9": 19,
                }
            ],
        )
        self.assertEqual(collector._retained, set())

    def test_a_baseline_is_held_back_for_one_sample_only(self):
        """A second tear in a row lets the baseline advance.

        A histogram that keeps tearing is unreportable whichever baseline it is measured
        against, so refusing to advance would only widen the window the eventual good
        sample covers, without saving it from anything.
        """
        good = mcstat_hist([(10, 100), (20, 100)])
        torn_pair = []
        for total in (231, 261):
            torn = mcstat_hist([(10, 115)])
            torn["total"] = total
            torn_pair.append(torn)
        collector = self._collector([good, *torn_pair])

        collector.sample()
        collector.sample()

        self.assertEqual(collector._retained, {(self.BUCKET, self.NODE, "bg_load")})

        collector.sample()

        self.assertEqual(collector._retained, set())
        self.assertEqual(
            collector.prev_histograms[self.BUCKET][self.NODE]["bg_load"],
            torn_pair[1],
            "a second tear in a row must let the baseline advance",
        )

    def test_a_histogram_that_is_not_shaped_as_expected_costs_only_its_own_metric(self):
        """A server release that changes the timings format must not take the sample with it.

        Everything `hist_diff` reads is kv-engine's to change, and an exception escaping
        here unwinds into `Collector.collect`, losing every other metric in the sample and
        logging a traceback every interval for the rest of the run.
        """
        for description, break_it in (
            ("a field the arithmetic needs is gone", lambda h: h.pop("overflowed")),
            ("a row no longer carries a count", lambda h: h.update(data=[[10]])),
            ("a row is no longer a sequence", lambda h: h.update(data=[10, 20])),
        ):
            with self.subTest(description):
                curr = mcstat_hist([(10, 115)])
                break_it(curr)
                collector = self._collector([mcstat_hist([(10, 100), (20, 100)]), curr])

                collector.sample()
                collector.sample()

                self.assertEqual(collector.reported, [])
                self.assertEqual(collector._prev_malformed, {(self.BUCKET, self.NODE, "bg_load")})
                self.assertEqual(collector._prev_violations, set())
                self.assertEqual(collector._prev_truncated, set())

    def test_a_node_with_no_histograms_is_not_called_unreachable(self):
        """A metric no operation has produced yet says nothing about the node.

        `bg_load` only appears once something has had to fetch from disk, so early in a run
        a perfectly healthy node has no histogram to report. Counting that as a node we
        could not reach buries the nodes we really could not reach.
        """
        collector = self._collector([])
        collector._get_node_list = lambda: [self.NODE, self.OTHER_NODE]
        collector._get_timing_stats = lambda node: None if node == self.NODE else {}

        collector.sample()

        self.assertEqual(collector._prev_unreachable, {self.NODE})
        self.assertEqual(collector._prev_without_stats, {self.OTHER_NODE})
        self.assertEqual(collector.reported, [])

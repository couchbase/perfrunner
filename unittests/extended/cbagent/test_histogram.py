"""Histogram arithmetic behind the `mcstat timings` collector."""

import random
from unittest import TestCase

from cbagent.histogram import (
    HistogramInvariantError,
    TruncatedHistogramError,
    hist_diff,
    hist_invariant_violation,
    hist_percentiles,
)
from unittests.extended.cbagent.mcstat_fixtures import (
    LATENCY_BOUNDS,
    MAX_TRACKABLE,
    counts,
    drifting_pair,
    latency_peak,
    mcstat_hist,
    percentile_iterator_hist,
)


class HistogramDiffTest(TestCase):
    """Cover `hist_diff`, which turns two cumulative mcstat snapshots into one interval.

    `McstatHistogramStats` samples the same metric every interval and reports percentiles of
    the *delta*, so a wrong diff silently misreports latency for the whole run rather than
    failing. Snapshots share no bucket boundaries (mcstat drops empty buckets), so every case
    below is really about how counts are re-binned onto the newer snapshot's boundaries.
    """

    def test_identical_snapshots_have_no_new_observations(self):
        """The metric was idle for a whole interval - every bucket must diff to zero."""
        hist = mcstat_hist([(8, 10), (64, 20)])

        diff = hist_diff(hist, hist)

        self.assertEqual(counts(diff), [(8, 0), (64, 0)])
        self.assertEqual(diff["total"], 0)

    def test_counts_added_to_shared_buckets_are_subtracted_per_bucket(self):
        """The easy case: both snapshots agree on boundaries, so it is a plain subtraction."""
        prev = mcstat_hist([(8, 10), (64, 20)])
        curr = mcstat_hist([(8, 15), (64, 50)])

        diff = hist_diff(prev, curr)

        self.assertEqual(counts(diff), [(8, 5), (64, 30)])
        self.assertEqual(diff["total"], 35)

    def test_buckets_above_the_previous_range_are_entirely_new(self):
        """A slower observation than anything seen before opens a bucket with no predecessor."""
        prev = mcstat_hist([(8, 10)])
        curr = mcstat_hist([(8, 10), (64, 7)])

        diff = hist_diff(prev, curr)

        self.assertEqual(counts(diff), [(8, 0), (64, 7)])
        self.assertEqual(diff["total"], 7)

    def test_previous_bucket_split_across_finer_new_buckets(self):
        """The newer snapshot resolves a range the older one reported as one bucket.

        The old count cannot be attributed to either half, and `hist_diff` fills the lower
        half first - deliberately over-attributing old observations to the faster bucket so
        the interval's new observations are not under-counted.
        """
        prev = mcstat_hist([(64, 10)])
        curr = mcstat_hist([(16, 4), (64, 20)])

        diff = hist_diff(prev, curr)

        # 4 of the 10 old observations are charged to (0, 16], the remaining 6 to (16, 64]
        self.assertEqual(counts(diff), [(16, 0), (64, 14)])
        self.assertEqual(diff["total"], 14)

    def test_previous_buckets_merged_into_one_coarser_new_bucket(self):
        """Several old buckets fall inside a single new one, so their counts combine."""
        prev = mcstat_hist([(8, 5), (16, 5)])
        curr = mcstat_hist([(16, 12)])

        diff = hist_diff(prev, curr)

        self.assertEqual(counts(diff), [(16, 2)])
        self.assertEqual(diff["total"], 2)

    def test_a_bound_both_snapshots_share_is_never_borrowed_across(self):
        """Where both snapshots delimit a value, the old count below it is exact.

        No old bucket straddles 16 here - (8, 16] ends on it and (16, 64] begins on it - so
        there is nothing to split and no policy to apply: the older snapshot holds exactly
        5 + 5 observations at or below 16. Borrowing from (16, 64] to "fill" (0, 16] would
        move observations down across a boundary the two snapshots agree on, taking count out
        of the faster bucket and leaving it in the slower one, which inflates the reported
        tail percentiles.
        """
        prev = mcstat_hist([(8, 5), (16, 5), (64, 3)])
        curr = mcstat_hist([(16, 12), (64, 4)])

        diff = hist_diff(prev, curr)

        # (0, 16]: 12 in the newer snapshot, 10 in the older.  (16, 64]: 4 and 3.
        self.assertEqual(counts(diff), [(16, 2), (64, 1)])
        self.assertEqual(diff["total"], 3)

    def test_merge_then_borrows_from_a_genuinely_straddling_bucket(self):
        """Merging several old buckets can still end on one that spans the new boundary.

        The older snapshot's (8, 32] really does straddle 16, and 16 is not one of its
        bounds, so its count has to be split. `hist_diff` fills the lower bucket first: all
        10 fit under the new 20, so (0, 16] is charged 5 + 10 and nothing is left for
        (16, 64]. Distinguishes this from the shared-bound case above, which must not borrow.
        """
        prev = mcstat_hist([(8, 5), (32, 10)])
        curr = mcstat_hist([(16, 20), (64, 4)])

        diff = hist_diff(prev, curr)

        self.assertEqual(counts(diff), [(16, 5), (64, 4)])
        self.assertEqual(diff["total"], curr["total"] - prev["total"])

    def test_untrackable_observations_are_differenced_too(self):
        """Observations past `max_trackable` are counted separately and feed the percentiles."""
        prev = mcstat_hist([(8, 10)], overflowed=3, overflowed_sum=9000)
        curr = mcstat_hist([(8, 10)], overflowed=5, overflowed_sum=16000)

        diff = hist_diff(prev, curr)

        self.assertEqual(diff["overflowed"], 2)
        self.assertEqual(diff["overflowed_sum"], 7000)

    def test_diff_total_is_exactly_the_interval_observation_count(self):
        """The property that matters: re-binning must neither invent nor lose observations.

        Randomised over snapshot pairs built the way real ones are - a latency distribution
        that drifts between samples, with counts that only ever grow, run through the real
        percentile-tick emitter. That is what puts every re-binning branch in play: the
        emitter re-chooses bin widths per snapshot, so boundaries differ in both directions.
        """
        rng = random.Random(20260903)
        bounds = LATENCY_BOUNDS

        for _ in range(200):
            centre, width = rng.randrange(len(bounds)), rng.uniform(0.5, 40)
            scale = rng.choice([20, 100, 10**4, 10**6])
            before = latency_peak(centre, 2 * width**2, scale)
            centre, width = rng.randrange(len(bounds)), rng.uniform(0.3, 40)
            added = latency_peak(centre, 2 * width**2, scale * rng.choice([1, 2, 10]))
            prev = percentile_iterator_hist(before, bounds)
            curr = percentile_iterator_hist([b + a for b, a in zip(before, added)], bounds)
            if not prev["data"] or not curr["data"]:
                continue

            diff = hist_diff(prev, curr)

            pair = f"{prev['data']} -> {curr['data']}"
            self.assertEqual(diff["total"], sum(added), pair)
            self.assertTrue(
                all(count >= 0 for _, count in counts(diff)), f"{pair} gave {diff['data']}"
            )

    def test_emitter_reuses_upper_bounds_and_emits_empty_bins(self):
        """Two percentile ticks can land in one bucket, so rows repeat a bound at count 0.

        Real snapshots carry 15-34 such rows out of ~80. The repeated bound is charged
        against an already-advanced cursor, so it must contribute nothing rather than
        double-subtracting the bucket below it.
        """
        prev, curr = drifting_pair()

        repeated = [a for a, b in zip(prev["data"], prev["data"][1:]) if a[0] == b[0]]
        self.assertTrue(repeated, "expected the emitter to repeat an upper bound")
        self.assertTrue([r for r in prev["data"] if r[1] == 0], "expected empty bins")

        diff = hist_diff(prev, curr)

        self.assertEqual(diff["total"], curr["total"] - prev["total"])
        self.assertTrue(all(count >= 0 for _, count in counts(diff)))

    def test_snapshot_boundaries_are_not_nested_between_samples(self):
        """Neither snapshot's bounds are a subset of the other's - both re-bin directions run.

        Pins the assumption the algorithm exists for. Every one of 48 consecutive samples
        collected off a real cluster had 2-18 bounds the next sample did not, so a change
        that only handled hist2 refining hist1 would look correct and quietly misreport.
        """
        prev, curr = drifting_pair()

        prev_bounds = {hi for hi, _ in counts(prev)}
        curr_bounds = {hi for hi, _ in counts(curr)}

        self.assertTrue(prev_bounds - curr_bounds, "expected bounds only the older sample has")
        self.assertTrue(curr_bounds - prev_bounds, "expected bounds only the newer sample has")

    def test_snapshots_of_different_histograms_are_rejected(self):
        """Both samples must start at the same lower bound or the re-binning is meaningless."""
        with self.assertRaises(HistogramInvariantError):
            hist_diff(mcstat_hist([(8, 1)], low=0), mcstat_hist([(8, 2)], low=1))

    def test_a_sample_with_no_buckets_makes_the_whole_next_sample_new(self):
        """An empty earlier sample is not an error: nothing had been recorded yet."""
        diff = hist_diff(mcstat_hist([]), mcstat_hist([(8, 5), (64, 7)]))

        self.assertEqual(counts(diff), [(8, 5), (64, 7)])
        self.assertEqual(diff["total"], 12)

    def test_counter_reset_is_rejected_rather_than_diffed(self):
        """Counts going *down* means the samples are not two views of one histogram.

        memcached restarting, or a bucket being recreated mid-test, resets timings to zero, so
        the newer sample can be smaller than the older one. There is no delta to compute then
        - and no way to tell such a pair from an interval that recorded very little - so it
        must be rejected rather than diffed into whatever the arithmetic happens to produce.
        Each pair below takes a different route through the re-binning.
        """
        for prev, curr in (
            (mcstat_hist([(8, 100)]), mcstat_hist([(8, 5)])),
            (mcstat_hist([(8, 10), (64, 100)]), mcstat_hist([(8, 10), (64, 20)])),
            (mcstat_hist([(8, 100)]), mcstat_hist([(64, 5)])),
            (mcstat_hist([(8, 50), (16, 50)]), mcstat_hist([(16, 20), (64, 30)])),
        ):
            with self.assertRaises(HistogramInvariantError):
                hist_diff(prev, curr)


class HistogramInvariantTest(TestCase):
    """Cover the precondition `hist_diff` relies on: that counters only ever accumulate.

    A pair of samples cannot be diffed once something has reset the histogram, and the
    arithmetic cannot tell that apart from a quiet interval on its own, so the pair is checked
    first and rejected with a reason. The failure this prevents is not a crash but silently
    wrong latency, so the reasons are pinned rather than just their existence.
    """

    def test_consecutive_samples_of_a_live_histogram_are_accepted(self):
        """The check must not cost a legitimate interval - a false positive is a lost sample."""
        prev, curr = drifting_pair()

        self.assertIsNone(hist_invariant_violation(prev, curr))

    def test_the_range_whose_count_fell_is_named(self):
        self.assertEqual(
            str(
                hist_invariant_violation(
                    mcstat_hist([(8, 10), (64, 100)]), mcstat_hist([(8, 10), (64, 20)])
                )
            ),
            "count over (8, 64] fell from 100 to 20",
        )

    def test_counts_lost_where_the_samples_share_no_boundary_are_named(self):
        """With no boundary in common the whole histogram is one comparable range."""
        self.assertEqual(
            str(hist_invariant_violation(mcstat_hist([(8, 100)]), mcstat_hist([(64, 5)]))),
            "count above 0 fell from 100 to 5",
        )

    def test_untrackable_observation_counters_going_backwards_are_rejected(self):
        """Observations too slow to bucket are counted separately and feed the percentiles."""
        prev = mcstat_hist([(8, 10)], overflowed=5, overflowed_sum=9000)

        self.assertIn(
            "count fell",
            str(
                hist_invariant_violation(
                    prev, mcstat_hist([(8, 10)], overflowed=1, overflowed_sum=9000)
                )
            ),
        )
        self.assertIn(
            "sum fell",
            str(
                hist_invariant_violation(
                    prev, mcstat_hist([(8, 10)], overflowed=5, overflowed_sum=10)
                )
            ),
        )

    def test_a_tail_lost_by_a_still_accumulating_counter_is_a_torn_read(self):
        """The emitter stopped rendering below the top bucket; nothing was reset.

        `total` is the accumulating counter rather than anything the emitter's walk
        rendered, so it holding or growing while the tail of `data` disappears is the
        signature of a walk that stopped early - not of the reset, restart or bucket
        recreation the same shortfall would mean if the counter had gone backwards too.
        """
        prev = mcstat_hist([(8, 100), (64, 1)])
        curr = mcstat_hist([(8, 150)])
        curr["total"] = 151  # the observation missing from `data` is still counted here

        violation = hist_invariant_violation(prev, curr)

        self.assertIsInstance(violation, TruncatedHistogramError)
        self.assertEqual(str(violation), "count above 8 fell from 1 to 0")

    def test_the_same_tail_shortfall_on_a_counter_that_fell_is_still_a_reset(self):
        """Only `total` separates the two, so it has to be what the check keys on."""
        prev = mcstat_hist([(8, 100), (64, 1)])
        curr = mcstat_hist([(8, 100)])  # total 100, so the counter itself went backwards

        violation = hist_invariant_violation(prev, curr)

        self.assertEqual(str(violation), "count above 8 fell from 1 to 0")
        self.assertNotIsInstance(violation, TruncatedHistogramError)

    def test_a_range_both_samples_delimit_emptying_is_never_a_torn_read(self):
        """A walk that stops early leaves no shared bound above where it stopped."""
        prev = mcstat_hist([(8, 10), (64, 100)])
        curr = mcstat_hist([(8, 10), (64, 20)])
        curr["total"] = 10_000

        self.assertNotIsInstance(hist_invariant_violation(prev, curr), TruncatedHistogramError)

    def test_the_check_is_necessary_but_not_sufficient(self):
        """Which is why `hist_diff` guards its own arithmetic as well as checking up front.

        The check can only compare counts over ranges that both samples delimit. Here the only
        shared bound is the last, and over (0, 10] the count grew 100 -> 150, so it passes.
        But the earlier sample puts all 100 observations below 3 while the later one has none
        below 7, which no accumulating histogram can do. The re-binning meets the
        contradiction as an old count larger than the new bucket holding it.
        """
        prev = mcstat_hist([(3, 100), (10, 0)])
        curr = mcstat_hist([(7, 0), (10, 150)])

        self.assertIsNone(hist_invariant_violation(prev, curr))

        with self.assertRaises(HistogramInvariantError):
            hist_diff(prev, curr)


class HistogramPercentilesTest(TestCase):
    """Cover `hist_percentiles`, which turns one interval's diff into the reported metrics.

    `McstatHistogramStats` names each metric after the percentile key returned here, so a
    missing or misplaced key silently drops or mislabels a datapoint in cbmonitor.
    """

    PERCENTILES = [50, 90, 95, 99, 99.9]

    def test_percentile_landing_on_a_bucket_boundary_reports_that_bound(self):
        hist = mcstat_hist([(10, 50), (20, 50)])

        self.assertEqual(hist_percentiles(hist, [50]), {50: 10})

    def test_percentile_inside_a_bucket_is_interpolated(self):
        """p90 sits four fifths through (10, 20], not at either end of it."""
        hist = mcstat_hist([(10, 50), (20, 50)])

        self.assertEqual(hist_percentiles(hist, [90]), {90: 18})

    def test_results_are_keyed_by_percentile_not_by_position(self):
        """Keys must survive an unsorted request; the caller builds metric names from them."""
        hist = mcstat_hist([(10, 50), (20, 50)])

        self.assertEqual(hist_percentiles(hist, [99, 50, 90]), {50: 10, 90: 18, 99: 19})

    def test_every_requested_percentile_is_returned_when_the_tail_is_untrackable(self):
        """Percentiles past the tracked range report `max_trackable`, and none are dropped.

        10% of observations were too slow to bucket, so p95 upwards fall in the overflow. The
        caller stores whatever keys come back, so a short result silently loses a metric.
        """
        hist = mcstat_hist([(10, 50), (20, 40)], overflowed=10)

        self.assertEqual(
            hist_percentiles(hist, self.PERCENTILES),
            {50: 10, 90: 20, 95: MAX_TRACKABLE, 99: MAX_TRACKABLE, 99.9: MAX_TRACKABLE},
        )

    def test_all_observations_untrackable(self):
        hist = mcstat_hist([], overflowed=100)

        self.assertEqual(hist_percentiles(hist, [50, 99]), {50: MAX_TRACKABLE, 99: MAX_TRACKABLE})

    def test_nothing_to_count_reports_nothing_rather_than_zero(self):
        """No observations means no latency to report - zeroes would read as a real datapoint.

        A diff whose total came out negative takes the same path. Nothing here validates its
        input: percentiles of a corrupt diff would come back in range and in order, with no
        way for the caller to tell them from real ones, which is why an unusable pair of
        samples is rejected before it is diffed rather than inspected afterwards.
        """
        self.assertEqual(hist_percentiles(mcstat_hist([]), self.PERCENTILES), {})
        self.assertEqual(hist_percentiles(mcstat_hist([(10, 0)]), self.PERCENTILES), {})
        self.assertEqual(hist_percentiles(mcstat_hist([(10, 5)]), []), {})

        negative = mcstat_hist([(10, 5)])
        negative["data"][0][1], negative["total"] = -95, -95

        self.assertEqual(hist_percentiles(negative, self.PERCENTILES), {})

    def test_percentiles_of_a_diff_of_two_snapshots(self):
        """End to end over one interval: only the newly observed 30 count towards percentiles."""
        prev = mcstat_hist([(10, 100), (20, 100)])
        curr = mcstat_hist([(10, 115), (20, 115)])

        diff = hist_diff(prev, curr)

        self.assertEqual(diff["total"], 30)
        self.assertEqual(hist_percentiles(diff, [50, 99]), {50: 10, 99: 19})

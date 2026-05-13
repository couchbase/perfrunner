"""Parsing and decisions of the point-in-time restore test, fed with real tool output."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, mock

os.environ.setdefault("WORKER_TYPE", "local")

from perfrunner.tests import continuous_backup as cb  # noqa: E402

PITR = cb.ContinuousBackupPITRTest

# The summary cbcontbk restore-continuous printed in a Jenkins run
TOOL_OUTPUT = "\n".join(
    [
        "| Transfer",
        "| --------",
        "| Status    | Avg Transfer Rate | Started At | Finished At | Duration |",
        "| Succeeded | 313.12MiB/s       | Thu, 15 Oct 2026 00:11:51 -0700 "
        "| Thu, 15 Oct 2026 00:13:12 -0700 | 1m20s    |",
        "",
        "| Bucket",
        "| ------",
        "| Name     | Status    | Transferred | Avg Transfer Rate | Started At | Finished At "
        "| Duration |",
        "| bucket-1 | Succeeded | 24.46GiB    | 368.37MiB/s       | Thu, 15 Oct 2026 00:12:03 "
        "-0700 | Thu, 15 Oct 2026 00:13:12 -0700 | 1m8s     |",
    ]
)

# What cbcontbk printed when the target was a second past the last mutation
SPAN_ERROR = (
    "cbcontbk restore-continuous failed with code 1:\n"
    "Error restoring cluster: could not create transferables: could not get data ranges: "
    "target time does not appear in the backup; backup spans from "
    "2026-10-14T20:38:56-07:00 to 2026-10-14T21:10:20-07:00"
)


class StubPITR(PITR):
    """The real test class without a cluster behind it."""

    rest = None
    master_node = "10.0.0.1"
    test_config = None


def make_test(location="/cdc"):
    test = object.__new__(StubPITR)
    test.rest = mock.Mock()
    test.backup_options = SimpleNamespace(
        continuous_backup_location=location, continuous_backup_tempdir="/tmp/contbk"
    )
    test.pitr = SimpleNamespace(wait_after_snapshot_restore=False)
    test.bucket = "bucket-1"
    test.target_ts = "2026-10-15T04:10:22Z"
    return test


class ParsingTest(TestCase):
    def test_summary_row_gives_rate_and_duration(self):
        self.assertEqual(PITR._parse_transfer_stats(TOOL_OUTPUT), (313.12, 80.0))

    def test_rates_in_gib_are_converted_to_mb(self):
        summary = "| Succeeded | 2GiB/s | s | e | 10s |"

        self.assertEqual(PITR._parse_transfer_stats(summary), (2048.0, 10.0))

    def test_output_without_a_summary_gives_nothing(self):
        self.assertEqual(PITR._parse_transfer_stats("Error restoring cluster"), (None, None))

    def test_growth_counts_runs_of_growth_as_flushes(self):
        samples = [(1, 0), (2, 10), (3, 20), (4, 0), (5, 0), (6, 30), (7, 0)]

        total_mb, avg_secs, avg_mb = PITR._summarise_growth(samples)

        self.assertEqual((total_mb, avg_secs, avg_mb), (60, 1.5, 30))

    def test_a_flush_still_running_at_the_end_is_counted(self):
        self.assertEqual(PITR._summarise_growth([(1, 0), (2, 5), (3, 5)]), (10, 1.0, 10))

    def test_no_growth_gives_zeros(self):
        self.assertEqual(PITR._summarise_growth([(1, 0), (2, 0)]), (0, 0, 0))


class ClampRestoreTargetTest(TestCase):
    def clamp(self, error, target):
        return PITR._clamp_target_to_backup_span(error, target)

    def test_target_after_the_span_moves_to_its_end_in_utc(self):
        self.assertEqual(self.clamp(SPAN_ERROR, "2026-10-15T04:10:22Z"), "2026-10-15T04:10:20Z")

    def test_target_inside_the_span_is_not_second_guessed(self):
        self.assertIsNone(self.clamp(SPAN_ERROR, "2026-10-15T04:00:00Z"))

    def test_target_before_the_span_is_not_second_guessed(self):
        self.assertIsNone(self.clamp(SPAN_ERROR, "2026-10-15T03:00:00Z"))

    def test_other_failures_are_left_alone(self):
        self.assertIsNone(self.clamp("disk full", "2026-10-15T04:10:22Z"))


class ContinuousUuidTest(TestCase):
    def test_newest_directory_with_bucket_metadata_wins(self):
        with TemporaryDirectory() as location:
            root = Path(location)
            for name, mtime, has_meta in (
                ("a" * 32, 100, True),
                ("b" * 32, 200, True),
                ("c" * 32, 300, False),
                ("not-a-uuid", 400, True),
            ):
                directory = root / name / "meta"
                directory.mkdir(parents=True)
                if has_meta:
                    (directory / "bucket_meta.json").write_text("{}")
                os.utime(root / name, (mtime, mtime))

            self.assertEqual(make_test(location)._get_continuous_uuid(), "b" * 32)

    def test_nothing_to_find_is_an_error(self):
        with TemporaryDirectory() as location, self.assertRaises(RuntimeError):
            make_test(location)._get_continuous_uuid()


class RestoreReplayTest(TestCase):
    def replay(self, outputs):
        test = make_test()
        calls = []

        def run_cbcontbk_restore(target_ts, **kwargs):
            calls.append(target_ts)
            output = next(outputs)
            if isinstance(output, Exception):
                raise output
            return output

        stubs = {
            "_empty_bucket": mock.Mock(),
            "restore": mock.Mock(return_value=TOOL_OUTPUT),
            "run_cbcontbk_restore": mock.Mock(side_effect=run_cbcontbk_restore),
        }
        with (
            mock.patch.multiple(StubPITR, create=True, **stubs),
            mock.patch.object(cb.local, "cleanup"),
            mock.patch.object(cb, "with_stats", lambda method: method, create=True),
        ):
            result = StubPITR._run_restore.__wrapped__(test, "base")
        return result, calls

    def test_target_past_the_backup_is_retried_at_its_end(self):
        result, calls = self.replay(iter([RuntimeError(SPAN_ERROR), TOOL_OUTPUT]))

        self.assertEqual(calls, ["2026-10-15T04:10:22Z", "2026-10-15T04:10:20Z"])
        self.assertEqual(result["replay_mb_s"], 313.12)

    def test_any_other_replay_failure_is_raised(self):
        with self.assertRaises(RuntimeError):
            self.replay(iter([RuntimeError("disk full")]))


class RunTest(TestCase):
    def test_a_failed_run_reports_no_kpis_but_still_collects_logs(self):
        test = make_test()
        test.require_cb_tools = mock.Mock()
        test.report_kpi = mock.Mock()
        test.collect_logs = mock.Mock()
        test.load = mock.Mock(side_effect=RuntimeError("boom"))
        test.test_config = SimpleNamespace(bucket=SimpleNamespace(history_seconds=3600))

        with mock.patch.object(cb.os.path, "ismount", return_value=True), self.assertRaises(
            RuntimeError
        ):
            test.run()

        test.report_kpi.assert_not_called()
        test.collect_logs.assert_called_once()

    def test_an_unmounted_location_fails_before_anything_is_touched(self):
        test = make_test()
        test.require_cb_tools = mock.Mock()
        test.load = mock.Mock()

        with mock.patch.object(cb.os.path, "ismount", return_value=False), self.assertRaises(
            RuntimeError
        ):
            test.run()

        test.load.assert_not_called()

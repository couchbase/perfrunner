import json
import tempfile
from pathlib import Path
from unittest import TestCase

from perfrunner.workloads.analytics.custom.driver import (
    CustomAnalyticsQuery,
    expand_query_configs,
    log_query_to_file,
    plan_check_failures,
    reduce_timings_to_kpis,
)


class ExpandQueryConfigsTest(TestCase):
    def test_set_as_string_emits_one_clause(self):
        queries = expand_query_configs([{"statement": "SELECT 1;", "set": "`a` 1"}])
        self.assertEqual(queries[0].statement, 'SET `a` 1; SELECT 1;')

    def test_set_as_list_emits_clauses_in_order(self):
        queries = expand_query_configs(
            [{"statement": "SELECT 1;", "set": ["`a` 1", "`b` 2"]}]
        )
        self.assertEqual(queries[0].statement, 'SET `a` 1; SET `b` 2; SELECT 1;')

    def test_repeat_is_the_outer_loop_around_named_args(self):
        queries = expand_query_configs(
            [
                {
                    "statement": "SELECT $N;",
                    "named_args": [{"name": "$N", "values": [1, 2]}],
                    "repeat": 2,
                }
            ]
        )
        self.assertEqual([q.named_args["$N"] for q in queries], [1, 2, 1, 2])

    def test_passes_repeats_the_whole_list(self):
        queries = expand_query_configs(
            [{"name": "A", "statement": "SELECT 1;"}, {"name": "B", "statement": "SELECT 2;"}],
            passes=3,
        )
        self.assertEqual([q.name for q in queries], ["A", "B"] * 3)

    def test_once_entries_are_interleaved_in_the_first_pass_only(self):
        queries = expand_query_configs(
            [
                {"name": "warmup", "statement": "SELECT 0;", "once": True},
                {"name": "A", "statement": "SELECT 1;"},
            ],
            passes=3,
        )
        self.assertEqual([q.name for q in queries], ["warmup", "A", "A", "A"])

    def test_once_entry_keeps_its_declared_position(self):
        configs = [
            {"name": "A", "statement": "SELECT 1;"},
            {"name": "B", "statement": "SELECT 2;", "once": True},
            {"name": "C", "statement": "SELECT 3;"},
        ]
        self.assertEqual(
            [q.name for q in expand_query_configs(configs, passes=3)],
            ["A", "B", "C", "A", "C", "A", "C"],
        )
        self.assertEqual(
            [q.name for q in expand_query_configs(configs)],
            ["A", "B", "C"],
        )

    def test_unknown_key_raises(self):
        with self.assertRaises(ValueError):
            expand_query_configs([{"statement": "SELECT 1;", "plan_contain": ["X"]}])


class PlanCheckFailuresTest(TestCase):
    def test_no_checks_configured_never_fails(self):
        query = CustomAnalyticsQuery("stmt", name="Q")
        self.assertEqual(plan_check_failures(query, {}), [])

    def test_plan_contains_satisfied(self):
        query = CustomAnalyticsQuery("stmt", name="Q", plan_contains=["FOO"])
        self.assertEqual(plan_check_failures(query, {"plans": {"x": "FOO"}}), [])

    def test_plan_contains_violated(self):
        query = CustomAnalyticsQuery("stmt", name="Q", plan_contains=["FOO"])
        failures = plan_check_failures(query, {"plans": {"x": "BAR"}})
        self.assertEqual(failures, ["Q (plan does not contain FOO)"])

    def test_plan_excludes_violated(self):
        query = CustomAnalyticsQuery("stmt", name="Q", plan_excludes=["FOO"])
        failures = plan_check_failures(query, {"plans": {"x": "FOO"}})
        self.assertEqual(failures, ["Q (plan contains FOO)"])

    def test_missing_plan_is_itself_a_failure(self):
        query = CustomAnalyticsQuery("stmt", name="Q", plan_excludes=["FOO"])
        failures = plan_check_failures(query, {})
        self.assertEqual(failures, ["Q (no plan in response to check)"])

    def test_matches_an_operator_inside_a_string_format_plan(self):
        # plan-format=STRING: the plan is one big text value, not a nested structure.
        query = CustomAnalyticsQuery("stmt", name="Q", plan_excludes=["PRE_SORTED_DISTINCT_BY"])
        plan = {"optimizedLogicalPlan": "...\nPRE_SORTED_DISTINCT_BY\n..."}
        failures = plan_check_failures(query, {"plans": plan})
        self.assertEqual(failures, ["Q (plan contains PRE_SORTED_DISTINCT_BY)"])

    def test_matches_an_operator_inside_a_json_format_plan(self):
        # plan-format=JSON (used when profiling): the operator is a field value, nested.
        query = CustomAnalyticsQuery("stmt", name="Q", plan_excludes=["PRE_SORTED_DISTINCT_BY"])
        plan = {"optimizedLogicalPlan": {"physical-operator": "PRE_SORTED_DISTINCT_BY"}}
        failures = plan_check_failures(query, {"plans": plan})
        self.assertEqual(failures, ["Q (plan contains PRE_SORTED_DISTINCT_BY)"])

    def test_unnamed_query_reports_as_unnamed(self):
        query = CustomAnalyticsQuery("stmt", plan_contains=["FOO"])
        failures = plan_check_failures(query, {"plans": {"x": "BAR"}})
        self.assertEqual(failures, ["(unnamed) (plan does not contain FOO)"])


class ReduceTimingsToKpisTest(TestCase):
    def test_median_and_empty_label_without_named_args(self):
        rows = reduce_timings_to_kpis({"CD01_baseline": {"no_named_args": [10.1, 9.9, 10.5]}})
        self.assertEqual(rows, [("CD01_baseline", "", "cd01_baseline", 10.1)])

    def test_label_and_sanitised_metric_id_with_named_args(self):
        rows = reduce_timings_to_kpis({"S1": {"$N=0.1,$K=5": [1.0, 2.0]}})
        self.assertEqual(rows, [("S1", " ($N=0.1,$K=5)", "s1_n_0_1_k_5", 1.5)])

    def test_one_row_per_named_args_combination(self):
        rows = reduce_timings_to_kpis(
            {"S1": {"no_named_args": [1.0], "$N=0.1": [2.0]}}
        )
        self.assertEqual(len(rows), 2)
        self.assertIn(("S1", "", "s1", 1.0), rows)
        self.assertIn(("S1", " ($N=0.1)", "s1_n_0_1", 2.0), rows)


class LogQueryToFileTest(TestCase):
    def _log_one(self, query, body):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "log.jsonl"
            log_query_to_file(path, query, 200, body)
            return json.loads(path.read_text().strip())

    def test_results_omitted_by_default(self):
        query = CustomAnalyticsQuery("SELECT 1;", name="Q")
        entry = self._log_one(query, {"results": [{"a": 1}]})
        self.assertNotIn("results", entry)

    def test_results_included_when_log_results_is_set(self):
        query = CustomAnalyticsQuery("SELECT 1;", name="Q", log_results=True)
        entry = self._log_one(query, {"results": [{"a": 1}]})
        self.assertEqual(entry["results"], [{"a": 1}])

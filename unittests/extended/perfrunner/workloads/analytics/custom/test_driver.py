import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock

from perfrunner.workloads.analytics.custom import driver
from perfrunner.workloads.analytics.custom.driver import (
    CustomAnalyticsQuery,
    compute_ratios,
    expand_query_configs,
    log_query_to_file,
    plan_check_failures,
    reduce_timings_to_kpis,
    split_query_conf,
    validate_ratios,
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


class ConcurrencyTest(TestCase):
    def test_concurrency_is_read_from_named_and_unnamed_entries(self):
        queries = expand_query_configs(
            [
                {"name": "A", "statement": "SELECT 1;", "concurrency": 10},
                {"statement": "SELECT 2;", "concurrency": 3},
                {"name": "B", "statement": "SELECT 3;"},
            ]
        )
        self.assertEqual([q.concurrency for q in queries], [10, 3, 1])

    def test_concurrency_below_one_raises(self):
        with self.assertRaises(ValueError):
            expand_query_configs([{"statement": "SELECT 1;", "concurrency": 0}])

    def test_every_copy_is_a_sample_and_is_plan_checked(self):
        body = {"metrics": {"elapsedTime": "1.5s"}, "plans": {"p": "DATASOURCE_SCAN"}}
        response = SimpleNamespace(ok=True, status_code=200, json=lambda: body)
        sessions = []

        def new_session(*args):
            session = SimpleNamespace(post=lambda **kw: response, close=lambda: None)
            sessions.append(session)
            return session

        query = CustomAnalyticsQuery(
            "SELECT 1;", name="Q", concurrency=3, plan_excludes=["DATASOURCE_SCAN"]
        )
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            driver, "_new_session", new_session
        ):
            results = driver.run_custom_analytics_query_task(
                "http://host:8095/analytics/service", ("u", "p"), [query], Path(tmp) / "log"
            )

        self.assertEqual(results["timings"]["Q"]["no_named_args"], [1.5, 1.5, 1.5])
        self.assertEqual(len(results["failures"]), 3)
        self.assertEqual(len(sessions), 4)


class RatiosTest(TestCase):
    CONFIGS = [
        {"name": "py", "statement": "SELECT 1;"},
        {"name": "sql", "statement": "SELECT 2;"},
        {"name": "args", "statement": "SELECT $N;", "named_args": [{"name": "$N", "values": [1]}]},
    ]

    def test_both_top_level_forms_are_accepted(self):
        self.assertEqual(split_query_conf([{"statement": "x"}]), ([{"statement": "x"}], []))
        self.assertEqual(
            split_query_conf({"queries": [{"statement": "x"}], "ratios": [{"name": "r"}]}),
            ([{"statement": "x"}], [{"name": "r"}]),
        )
        with self.assertRaises(ValueError):
            split_query_conf({"queries": [], "ratio": []})

    def test_ratio_must_name_a_timed_entry_without_named_args(self):
        ok = {"name": "r", "title": "t", "numerator": "py", "denominator": "sql"}
        validate_ratios(self.CONFIGS, [ok])
        for bad in (
            dict(ok, numerator="missing"),
            dict(ok, denominator="args"),
            dict(ok, chirality=1, extra=True),
            {k: v for k, v in ok.items() if k != "title"},
        ):
            with self.assertRaises(ValueError):
                validate_ratios(self.CONFIGS, [bad])

    def test_value_is_the_ratio_of_medians(self):
        ratio = {"name": "r", "title": "t", "numerator": "py", "denominator": "sql"}
        timings = {
            "py": {"no_named_args": [3.0, 2.0, 100.0]},
            "sql": {"no_named_args": [0.5, 1.0, 1.0]},
        }
        self.assertEqual(compute_ratios(timings, [ratio]), [(ratio, 3.0)])


class PlanOrderTest(TestCase):
    # Trimmed from a real optimized logical plan: the UDF's assign sits above the join.
    ABOVE = (
        "assign [$$84] <- [score_py($$101, $$113)] project: [$$84]\n"
        "-- ASSIGN_BATCH  |PARTITIONED|\n"
        "  join (and(eq($$89, $$92)))\n"
        "  -- HYBRID_HASH_JOIN [$$92][$$89]  |PARTITIONED|\n"
    )
    BELOW = (
        "join (and(eq($$89, $$92)))\n"
        "-- HYBRID_HASH_JOIN [$$92][$$89]  |PARTITIONED|\n"
        "  assign [$$84] <- [score_py($$101, $$113)]\n"
        "  -- ASSIGN_BATCH  |PARTITIONED|\n"
    )

    def _check(self, plan):
        query = CustomAnalyticsQuery("q", name="Q", plan_order=["score_py(", "_JOIN"])
        return plan_check_failures(query, {"plans": {"optimizedLogicalPlan": plan}})

    def test_udf_above_the_join_passes(self):
        self.assertEqual(self._check(self.ABOVE), [])

    def test_udf_below_the_join_fails(self):
        self.assertEqual(self._check(self.BELOW), ["Q (plan has score_py( below _JOIN)"])

    def test_missing_operator_fails(self):
        self.assertEqual(
            self._check("assign [$$84] <- [score_py($$101)]"),
            ["Q (plan lacks score_py( or _JOIN)"],
        )

    def test_plan_order_is_read_from_the_config(self):
        (query,) = expand_query_configs([{"statement": "q", "plan_order": ["a", "b"]}])
        self.assertEqual(query.plan_order, ["a", "b"])

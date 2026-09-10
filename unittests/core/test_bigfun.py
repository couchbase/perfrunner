"""BigFUN query generation determinism."""

from unittest import TestCase

from perfrunner.workloads.analytics.bigfun.query_gen import new_queries


class BigFunTest(TestCase):
    def test_unique_statements(self):
        queries = "perfrunner/workloads/analytics/bigfun/queries_with_index.yaml"
        for query in new_queries(queries):
            statements = set()
            for _ in range(10):
                self.assertNotIn(query.statement, statements)
                statements.add(query.statement)

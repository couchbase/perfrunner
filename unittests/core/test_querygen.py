import importlib.metadata
from unittest import TestCase

sdk_major_version = int(importlib.metadata.version("couchbase")[0])
if sdk_major_version == 2:
    from spring.querygen import N1QLQueryGen
elif sdk_major_version >= 3:
    from spring.querygen3 import N1QLQueryGen3 as N1QLQueryGen


class QueryTest(TestCase):
    def test_n1ql_query_gen_q1(self):
        queries = [
            {
                "statement": "SELECT * FROM `bucket-1` USE KEYS[$1];",
                "args": '["{key}"]',
            }
        ]

        if sdk_major_version == 2:
            qg = N1QLQueryGen(queries=queries)
        elif sdk_major_version >= 3:
            qg = N1QLQueryGen(queries=queries, query_weight=[1])

        for key in "n1ql-0123456789", "n1ql-9876543210":
            if sdk_major_version >= 3:
                stmt, queryopts = qg.next(key, doc={})
                self.assertEqual(queryopts["adhoc"], False)
                self.assertEqual(
                    str(queryopts["scan_consistency"]), "QueryScanConsistency.NOT_BOUNDED"
                )
                self.assertEqual(queryopts["positional_parameters"], [key])
            else:
                query = qg.next(key, doc={})
                self.assertEqual(query.adhoc, False)
                self.assertEqual(query.consistency, "not_bounded")
                self.assertEqual(query._body["args"], [key])

    def test_n1ql_query_gen_q2(self):
        queries = [
            {
                "statement": "SELECT * FROM `bucket-1` WHERE email = $1;",
                "args": '["{email}"]',
                "scan_consistency": "request_plus",
            }
        ]

        if sdk_major_version == 2:
            qg = N1QLQueryGen(queries=queries)
        elif sdk_major_version >= 3:
            qg = N1QLQueryGen(queries=queries, query_weight=[1])

        for doc in {"email": "a@a.com"}, {"email": "b@b.com"}:
            if sdk_major_version >= 3:
                stmt, queryopts = qg.next(key="n1ql-0123456789", doc=doc)
                self.assertEqual(
                    str(queryopts["scan_consistency"]), "QueryScanConsistency.REQUEST_PLUS"
                )
                self.assertEqual(queryopts["positional_parameters"], [doc["email"]])
            else:
                query = qg.next(key="n1ql-0123456789", doc=doc)
                self.assertEqual(query.consistency, "request_plus")
                self.assertEqual(query._body["args"], [doc["email"]])

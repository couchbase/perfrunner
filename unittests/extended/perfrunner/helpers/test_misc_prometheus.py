"""Prometheus text exposition parsing in `perfrunner.helpers.misc`, as served by Sync Gateway."""

from unittest import TestCase

from perfrunner.helpers.misc import parse_prometheus_stat

SGW_METRICS = """\
# HELP sgw_database_num_doc_writes The total number of documents written.
# TYPE sgw_database_num_doc_writes counter
sgw_database_num_doc_writes{database="db-1"} 1.0036e+06
sgw_database_num_doc_writes{database="db-2"} 400
sgw_database_num_doc_writes_rejected{database="db-1"} 7
sgw_resource_utilization_warn_count 5
sgw_replication_sgr_num_docs_pushed{database="db-1",replication="sgr2_push"} 10
sgw_replication_sgr_num_docs_pushed{database="db-1",replication="sgr2_conflict_resolution"} 20
not a sample
"""


class ParsePrometheusStatTest(TestCase):
    def test_sums_exact_name_matches_whose_labels_include_the_filter(self):
        # A name that prefixes another (`..._rejected`) must not pick it up.
        self.assertEqual(parse_prometheus_stat(SGW_METRICS, "sgw_database_num_doc_writes"),
                         1004000)
        self.assertEqual(
            parse_prometheus_stat(SGW_METRICS, "sgw_resource_utilization_warn_count"), 5
        )
        self.assertEqual(
            parse_prometheus_stat(SGW_METRICS, "sgw_replication_sgr_num_docs_pushed",
                                  replication="sgr2_conflict_resolution"),
            20,
        )
        self.assertEqual(parse_prometheus_stat(SGW_METRICS, "sgw_delta_sync_deltas_sent"), 0)

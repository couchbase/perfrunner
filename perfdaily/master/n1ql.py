from perfrunner.tests.n1ql import N1QLThroughputTest
from perfrunner.tests.n1ql import N1QLLatencyTest
from perfrunner.tests import TargetIterator


class N1QLThroughputTestDaily(N1QLThroughputTest):
    def _report_kpi(self):
        metrics = list()
        metrics.append(self.metric_helper.cal_avg_n1ql_queries_perfdaily())
        self.reporter.post_to_dailyp(metrics)


class N1QLLatencyTestDaily(N1QLLatencyTest):
    def _report_kpi(self):
        metrics = list()
        metrics.append(self.metric_helper.calc_query_latency_perfdaily(percentile=80))
        self.reporter.post_to_dailyp(metrics)

from perfrunner.tests.n1ql import N1QLThroughputTest
from perfrunner.tests import TargetIterator


class N1QLThroughputTestDaily(N1QLThroughputTest):
    def run(self):
        load_settings = self.test_config.load_settings
        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')

        self.load(load_settings, iterator)

        self.wait_for_persistence()
        self.compact_bucket()
        self.build_index()

        self._create_prepared_statements()
        self.workload = self.test_config.access_settings
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',self.workload.n1ql_queries)
        self.access_bg(self.workload)
        self.access(self.workload)

        metrics = list()
        metrics.append(self.metric_helper.cal_avg_n1ql_queries_for_perfdaily())
        self.reporter.post_to_dailyp(metrics)

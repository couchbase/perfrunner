from perfrunner.tests.kv import (
    DurabilityTest,
    MixedLatencyTest,
    PillowfightTest,
)


class PillowfightTestDaily(PillowfightTest):
    def _report_kpi(self):
        metrics = list()
        metrics.append(self.metric_helper.calc_avg_ops_perfdaily())
        self.reporter.post_to_dailyp(metrics)


class MixedLatencyTestDaily(MixedLatencyTest):
    def _report_kpi(self):
        metrics = list()
        for operation in ('get', 'set'):
            metrics.append(self.metric_helper.calc_kv_latency_perfdaily(operation=operation,
                                                                        percentile=99))
        self.reporter.post_to_dailyp(metrics)


class DurabilityTestDaily(DurabilityTest):
    def _report_kpi(self):
        metrics = list()
        for operation in ('replicate_to', 'persist_to'):
            metrics.append(self.metric_helper.calc_kv_latency_perfdaily(operation=operation,
                                                                        percentile=99,
                                                                        dbname='durability'))
        self.reporter.post_to_dailyp(metrics)

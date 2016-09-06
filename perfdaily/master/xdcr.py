from perfrunner.tests.xdcr import XdcrInitTest


class XdcrInitTestDaily(XdcrInitTest):

    def _report_kpi(self):
        metrics = list()
        metrics.append(self.metric_helper.calc_avg_replication_rate_perfdaily(self.time_elapsed))
        self.reporter.post_to_dailyp(metrics)

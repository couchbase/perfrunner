from multiprocessing import Process

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import log_phase
from perfrunner.tests import terminate_bg_process
from perfrunner.tests.index import IndexTest


class QueryTest(IndexTest):

    COLLECTORS = {'latency': True, 'query_latency': True}

    @with_stats
    @terminate_bg_process
    def access(self):
        super(QueryTest, self).timer()

    def access_bg(self):
        access_settings = self.test_config.get_access_settings()
        log_phase('access phase', access_settings)

        self.bg_process = Process(
            target=self.worker_manager.run_workload,
            args=(access_settings, self.target_iterator),
            kwargs={'ddocs': self.ddocs},
        )
        self.bg_process.start()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        self.build_index()

        self.workload = self.test_config.get_access_settings()
        self.access_bg()
        self.access()


class QueryThroughputTest(QueryTest):

    def run(self):
        super(QueryThroughputTest, self).run()
        self.reporter.post_to_sf(
            self.metric_helper.calc_avg_couch_views_ops()
        )


class QueryLatencyTest(QueryTest):

    def run(self):
        super(QueryLatencyTest, self).run()

        self.reporter.post_to_sf(
            *self.metric_helper.calc_query_latency(percentile=80)
        )
        if not self.test_config.get_index_settings().params:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_cpu_utilization()
            )
        if self.remote.os != 'Cygwin':
            self.reporter.post_to_sf(*self.metric_helper.calc_max_beam_rss())

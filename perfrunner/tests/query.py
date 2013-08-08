from multiprocessing import Event, Process

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests.index import IndexTest


class QueryTest(IndexTest):

    @with_stats(query_latency=True)
    def timer(self):
        super(QueryTest, self).timer()

    def access_bg(self):
        access_settings = self.test_config.get_access_settings()
        self.workload = access_settings
        logger.info('Running access phase: {0}'.format(access_settings))
        self.shutdown_event = Event()
        Process(
            target=self.worker_manager.run_workload,
            args=(access_settings, self.target_iterator, self.shutdown_event,
                  self.ddocs)
        ).start()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        self.build_index()

        self.access_bg()
        self.timer()
        self.shutdown_event.set()


class PeakQueryThroughputTest(QueryTest):

    def run(self):
        super(PeakQueryThroughputTest, self).run()
        self.reporter.post_to_sf(self.metric_helper.calc_avg_couch_views_ops())


class QueryLatencyTest(QueryTest):

    def run(self):
        super(QueryLatencyTest, self).run()
        self.reporter.post_to_sf(self.metric_helper.calc_90th_query_latency())

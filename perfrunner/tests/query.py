import time

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests.index import DevIndexTest, IndexTest


class QueryTest(IndexTest):

    """
    The base test which defines workflow for different view query tests. Access
    phase represents mixed KV workload and queries on views.
    """

    COLLECTORS = {'latency': True, 'query_latency': True}

    @with_stats
    def access(self):
        super(QueryTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.define_ddocs()
        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()


class QueryThroughputTest(QueryTest):

    """
    The test adds a simple step to workflow: post-test calculation of average
    query throughput.
    """

    def run(self):
        super(QueryThroughputTest, self).run()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                self.metric_helper.calc_avg_couch_views_ops()
            )


class QueryLatencyTest(QueryTest):

    """The basic test for bulk latency measurements. Bulk means a mix of
    equality, range, group, and etc. The most reasonable test for update_after
    (stale) queries.

    The class itself only adds calculation and posting of query latency.
    """

    def run(self):
        super(QueryLatencyTest, self).run()

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_query_latency(percentile=80)
            )
            if self.test_config.stats_settings.post_rss:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_max_beam_rss()
                )


class IndexLatencyTest(QueryTest):

    """
    Measurement of end-to-end latency which is defined as time it takes for a
    document to appear in view output after it is stored in KV.

    The test only adds calculation phase. See cbagent project for details.
    """

    COLLECTORS = {'index_latency': True, 'query_latency': True}

    def run(self):
        super(IndexLatencyTest, self).run()

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_observe_latency(percentile=95)
            )


class DevQueryLatencyTest(DevIndexTest, QueryLatencyTest):

    """
    Per query type latency measurements.
    """

    pass


class QueryManualCompactionTest(QueryTest):

    @with_stats
    def access(self):
        access_settings = self.test_config.access_settings
        logger.info('Running phase for {} seconds'.format(access_settings.time))
        t0 = time.time()
        while time.time() - t0 < access_settings.time:
            self.compact_index()

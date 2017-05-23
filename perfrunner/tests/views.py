from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest
from perfrunner.workloads.viewgen import ViewGen, ViewGenDev


class IndexTest(PerfTest):

    """
    The test measures time it takes to build index (views). This is just a base
    class, actual measurements happen in initial and incremental indexing tests.

    It doesn't differentiate index types and basically benchmarks dumb/bulk
    indexing.
    """

    def __init__(self, *args):
        super().__init__(*args)

        index_settings = self.test_config.index_settings
        if index_settings.disabled_updates:
            options = {'updateMinChanges': 0, 'replicaUpdateMinChanges': 0}
        else:
            options = None

        self.ddocs = ViewGen().generate_ddocs(index_settings.views, options)

    def define_ddocs(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.items():
                    self.rest.create_ddoc(master, bucket, ddoc_name, ddoc)

    def build_index(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.items():
                    for view_name in ddoc['views']:
                        self.rest.query_view(master, bucket,
                                             ddoc_name, view_name,
                                             params={'limit': 10})

        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_task(master, 'indexer')


class InitialIndexTest(IndexTest):

    """
    The test measures time it takes to build index for the first time. Scenario
    is pretty straightforward, there are only two phases:
    -- Initial data load
    -- Index building
    """

    @with_stats
    def init_index(self):
        super().build_index()

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.elapsed_time(time_elapsed)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.define_ddocs()
        time_elapsed = self.init_index()

        self.report_kpi(time_elapsed)


class InitialAndIncrementalIndexTest(InitialIndexTest):

    """
    Extended version of initial indexing test which also has access phase for
    data/index mutation. It is critical to disable automatic index updates so
    that we can control index building.
    """

    @with_stats
    @timeit
    def init_index(self):
        super(InitialIndexTest, self).build_index()

    @with_stats
    @timeit
    def incr_index(self):
        super(InitialIndexTest, self).build_index()

    def _report_kpi(self, time_elapsed, index_type='Initial'):
        self.reporter.post(
            *self.metrics.get_indexing_meta(time_elapsed, index_type)
        )

    def run(self):
        super().run()

        self.access()
        self.wait_for_persistence()

        time_elapsed = self.incr_index()

        self.report_kpi(time_elapsed, index_type='Incremental')


class IndexByTypeTest(IndexTest):

    """
    Unlike base test this one introduces measurements per different index type.
    It only used as a base class for view query tests (in order to get separate
    measurements for different types of queries).
    """

    def __init__(self, *args):
        super().__init__(*args)

        index_type = self.test_config.index_settings.index_type
        self.ddocs = ViewGenDev().generate_ddocs(index_type)


class QueryTest(IndexTest):

    """
    The base test which defines workflow for different view query tests. Access
    phase represents mixed KV workload and queries on views.
    """

    COLLECTORS = {'query_latency': True}

    @with_stats
    def access(self, *args):
        super().sleep()

        self.worker_manager.wait_for_workers()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.define_ddocs()
        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()

        self.report_kpi()


class QueryThroughputTest(QueryTest):

    """
    The test adds a simple step to the workflow: post-test calculation of
    average query throughput.
    """

    COLLECTORS = {}

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_couch_views_ops()
        )


class QueryLatencyTest(QueryTest):

    """The basic test for bulk latency measurements. Bulk means a mix of
    equality, range, group, and etc. The most reasonable test for update_after
    (stale) queries.

    The class itself only adds calculation and posting of query latency.
    """

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.query_latency(percentile=80)
        )


class IndexLatencyTest(QueryTest):

    """
    Measurement of end-to-end latency which is defined as time it takes for a
    document to appear in view output after it is stored in KV.

    The test only adds calculation phase. See cbagent project for details.
    """

    COLLECTORS = {'index_latency': True}

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.observe_latency(percentile=95)
        )


class QueryLatencyByTypeTest(IndexByTypeTest, QueryLatencyTest):

    """
    Per query type latency measurements.
    """

    pass

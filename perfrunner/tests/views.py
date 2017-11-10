from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest
from perfrunner.workloads.viewgen import ViewGen, ViewGenDev


class IndexTest(PerfTest):

    """Measures time it takes to build index (views).

    This is just a base class, actual measurements happen in initial and
    incremental indexing tests.

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
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.items():
                    self.rest.create_ddoc(master, bucket, ddoc_name, ddoc)

    def build_index(self):
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.items():
                    for view_name in ddoc['views']:
                        self.rest.query_view(master, bucket,
                                             ddoc_name, view_name,
                                             params={'limit': 10})

        for master in self.cluster_spec.masters:
            self.monitor.monitor_task(master, 'indexer')


class InitialIndexTest(IndexTest):

    """Measure time it takes to build index for the first time.

    Scenario is pretty straightforward, there are only two phases:
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

    """Extend the initial indexing test by adding a phase with data mutations.

    It is critical to disable automatic index updates so that we can control
    index building.
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

    """Introduces measurements per different index type.

    It only used as a base class for view query tests (in order to get separate
    measurements for different types of queries).
    """

    def __init__(self, *args):
        super().__init__(*args)

        index_type = self.test_config.index_settings.index_type
        self.ddocs = ViewGenDev().generate_ddocs(index_type)


class QueryTest(IndexTest):

    """Defines workflow for different view query tests.

    Access phase represents mixed KV workload and queries on views.
    """

    COLLECTORS = {'query_latency': True}

    @with_stats
    def access(self, *args):
        settings = self.test_config.access_settings
        settings.index_type = self.test_config.index_settings.index_type
        settings.ddocs = self.ddocs

        super().access(settings=settings)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.define_ddocs()
        self.build_index()

        self.access()

        self.report_kpi()


class QueryThroughputTest(QueryTest):

    """Add post-test calculation of average query throughput."""

    COLLECTORS = {}

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.avg_couch_views_ops()
        )


class QueryLatencyTest(QueryTest):

    """Measure bulk view query latency.

    Bulk means a mix of
    equality, range, group, and etc. The most reasonable test for update_after
    (stale) queries.

    The class itself only adds calculation and posting of query latency.
    """

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.query_latency(percentile=80)
        )


class IndexLatencyTest(QueryTest):

    """Measurement end-to-end indexing latency.

    Indexing latency is time it takes for a document to appear in view output
    after it is stored in KV.

    The test only adds calculation phase. See cbagent project for details.
    """

    COLLECTORS = {'index_latency': True}

    def _report_kpi(self):
        self.reporter.post(
            *self.metrics.observe_latency(percentile=95)
        )


class QueryLatencyByTypeTest(IndexByTypeTest, QueryLatencyTest):

    pass

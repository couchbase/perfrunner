from time import sleep

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
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
        super(IndexTest, self).__init__(*args)

        index_settings = self.test_config.index_settings
        if index_settings.disabled_updates:
            options = {'updateMinChanges': 0, 'replicaUpdateMinChanges': 0}
        else:
            options = None

        self.ddocs = ViewGen().generate_ddocs(index_settings.views, options)

    def define_ddocs(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.iteritems():
                    self.rest.create_ddoc(master, bucket, ddoc_name, ddoc)

    def build_index(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.iteritems():
                    for view_name in ddoc['views']:
                        self.rest.query_view(master, bucket,
                                             ddoc_name, view_name,
                                             params={'limit': 10})
        sleep(self.MONITORING_DELAY)
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_task(master, 'indexer')

    def compact_index(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name in self.ddocs:
                    self.rest.trigger_index_compaction(master, bucket,
                                                       ddoc_name)
        for master in self.cluster_spec.yield_masters():
            self.monitor.monitor_task(master, 'view_compaction')


class InitialIndexTest(IndexTest):

    """
    The test measures time it takes to build index for the first time. Scenario
    is pretty straightforward, there are only two phases:
    -- Initial data load
    -- Index building
    """

    @with_stats
    def build_index(self):
        super(InitialIndexTest, self).build_index()

    def _report_kpi(self, time_elapsed):
        self.reporter.post_to_sf(time_elapsed)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        from_ts, to_ts = self.build_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Initial index', time_elapsed)
        self.report_kpi(time_elapsed)


class InitialAndIncrementalIndexTest(IndexTest):

    """
    Extended version of initial indexing test which also has access phase for
    data/index mutation. It is critical to disable automatic index updates so
    that we can control index building.
    """

    @with_stats
    def build_init_index(self):
        return super(InitialAndIncrementalIndexTest, self).build_index()

    @with_stats
    def build_incr_index(self):
        super(InitialAndIncrementalIndexTest, self).build_index()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.reporter.start()
        self.define_ddocs()
        from_ts, to_ts = self.build_init_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Initial index', time_elapsed)
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type='Initial')
        )

        self.access()
        self.wait_for_persistence()
        self.compact_bucket()

        from_ts, to_ts = self.build_incr_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Incremental index', time_elapsed)
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type='Incremental')
        )


class DevIndexTest(IndexTest):

    """
    Unlike base test this one introduces measurements per different index type.
    It only used as a base class for view query tests (in order to get separate
    measurements for different types of queries).
    """

    def __init__(self, *args):
        super(IndexTest, self).__init__(*args)

        index_type = self.test_config.index_settings.index_type
        if index_type is None:
            logger.interrupt('Missing index_type param')
        self.ddocs = ViewGenDev().generate_ddocs(index_type)

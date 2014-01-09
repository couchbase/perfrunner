from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.viewgen import ViewGen


class IndexTest(PerfTest):

    def __init__(self, *args):
        super(IndexTest, self).__init__(*args)

        views_settings = self.test_config.get_index_settings()
        if views_settings.disabled_updates:
            options = {'updateMinChanges': 0, 'replicaUpdateMinChanges': 0}
        else:
            options = None

        self.ddocs = ViewGen().generate_ddocs(views_settings.views, options)

    def define_ddocs(self):
        for target in self.target_iterator:
            for ddoc_name, ddoc in self.ddocs.iteritems():
                self.rest.create_ddoc(target.node, target.bucket,
                                      ddoc_name, ddoc)

    def build_index(self):
        for target in self.target_iterator:
            for ddoc_name, ddoc in self.ddocs.iteritems():
                for view_name in ddoc['views']:
                    self.rest.query_view(target.node, target.bucket,
                                         ddoc_name, view_name,
                                         params={'limit': 10})
            self.monitor.monitor_task(target, 'indexer')

    def compact_index(self):
        for target in self.target_iterator:
            for ddoc_name in self.ddocs:
                self.rest.trigger_index_compaction(target.node, ddoc_name,
                                                   target.bucket)
            self.monitor.monitor_task(target, 'view_compaction')


class InitialIndexTest(IndexTest):

    @with_stats()
    def build_index(self):
        super(InitialIndexTest, self).build_index()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        from_ts, to_ts = self.build_index()
        time_elapsed = (to_ts - from_ts) / 1000.0

        time_elapsed = self.reporter.finish('Initial index', time_elapsed)
        self.reporter.post_to_sf(time_elapsed)


class InitialAndIncrementalIndexTest(IndexTest):

    @with_stats()
    def build_init_index(self):
        return super(InitialAndIncrementalIndexTest, self).build_index()

    @with_stats()
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
        self.reporter.post_to_sf(
            *self.metric_helper.calc_cpu_utilization(from_ts, to_ts,
                                                     meta='Initial index')
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_views_disk_size(from_ts, to_ts,
                                                     meta='Initial index')
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
        self.reporter.post_to_sf(
            *self.metric_helper.calc_cpu_utilization(from_ts, to_ts,
                                                     meta='Incremental index')
        )
        self.reporter.post_to_sf(
            *self.metric_helper.calc_views_disk_size(from_ts, to_ts,
                                                     meta='Incremental index')
        )

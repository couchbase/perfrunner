import json
from time import sleep

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class ViewTest(PerfTest):

    """
    The test measures time it takes to build views. This is just a base
    class, actual measurements happen in initial and incremental
    indexing tests.
    """

    def __init__(self, *args):
        super(ViewTest, self).__init__(*args)

        if self.view_settings.disabled_updates:
            options = {'updateMinChanges': 0, 'replicaUpdateMinChanges': 0}
        else:
            options = None

        self.ddocs = self._parse_ddocs(self.view_settings, options)

    def define_ddocs(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.iteritems():
                    self.rest.create_ddoc(master, bucket, ddoc_name, ddoc)

    def build_index(self):
        """Query the views in order to build up the index"""
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for ddoc_name, ddoc in self.ddocs.iteritems():
                    for view_name in ddoc[self.view_key]:
                        params = {'limit': 10}
                        if self.view_key == 'views':
                            self.rest.query_view(master, bucket, ddoc_name,
                                                 view_name, params)
                        elif self.view_key == 'spatial':
                            self.rest.query_spatial(master, bucket, ddoc_name,
                                                    view_name, params)
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

    @property
    def view_settings(self):
        """The configuration of the views.

        The settings are in different sections depending on whether
        it's a mapreduce or spatial view
        """
        raise NotImplementedError(
            "Any subclass of `ViewTest` must have a view_settings property")

    @property
    def view_key(self):
        """The property defining the views.

        This is the property in the design document that contains the
        view definitions. For mapreduce views it's "views" for spatial
        views it's "spatial".
        """
        raise NotImplementedError(
            "Any subclass of `ViewTest` must have a view_key property")

    @staticmethod
    def _parse_ddocs(view_settings, options):
        ddocs = {}
        if view_settings.indexes is None:
            logger.interrupt('Missing indexes param')
        for index in view_settings.indexes:
            ddoc_name, ddoc = index.split('::', 1)
            ddocs[ddoc_name] = json.loads(ddoc)
            if options:
                ddocs[ddoc_name]['options'] = options
        return ddocs


class ViewIndexTest(ViewTest):

    """
    Initial indexing test with access phase for data/index mutation.
    It is critical to disable automatic index updates so that we can
    control index building.
    """

    @with_stats
    def build_init_index(self):
        return super(ViewIndexTest, self).build_index()

    @with_stats
    def build_incr_index(self):
        super(ViewIndexTest, self).build_index()

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


class ViewQueryTest(ViewTest):

    """
    The base test which defines workflow for different view query tests. Access
    phase represents mixed KV workload and queries on spatial views.
    """

    @with_stats
    def access(self):
        super(ViewTest, self).timer()

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

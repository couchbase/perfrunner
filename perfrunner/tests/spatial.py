from perfrunner.tests.view import ViewIndexTest, ViewQueryTest


class SpatialMixin(object):
    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        self._view_settings = test_config.spatial_settings
        super(SpatialMixin, self).__init__(
            cluster_spec, test_config, verbose, experiment)

    @property
    def view_settings(self):
        return self._view_settings

    @property
    def view_key(self):
        return "spatial"


class SpatialIndexTest(SpatialMixin, ViewIndexTest):
    pass


class SpatialQueryTest(SpatialMixin, ViewQueryTest):
    # NOTE vmx 2015-06-11: Getting the get/set latency currently doesn't work
    # COLLECTORS = {'latency': True, 'spatial_latency': True}
    COLLECTORS = {'spatial_latency': True}


class SpatialQueryThroughputTest(SpatialQueryTest):

    """
    The test adds a simple step to workflow: post-test calculation of average
    query throughput.
    """

    def run(self):
        super(SpatialQueryThroughputTest, self).run()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                self.metric_helper.calc_avg_couch_spatial_ops()
            )


class SpatialQueryLatencyTest(SpatialQueryTest):

    """The basic test for latency measurements.

    The class itself only adds calculation and posting of query latency.
    """

    def run(self):
        super(SpatialQueryLatencyTest, self).run()

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_query_latency(percentile=80)
            )
            if self.test_config.stats_settings.post_rss:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_max_beam_rss()
                )

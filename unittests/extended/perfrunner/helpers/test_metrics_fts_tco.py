"""cbft memory TCO KPIs in `perfrunner.helpers.metrics`, reported by the fast merge build test."""

from types import SimpleNamespace
from unittest import TestCase

from perfrunner.helpers.metrics import MetricHelper, integrate_timeseries

GIB = 1024 ** 3


class IntegrateTimeseriesTest(TestCase):
    def test_trapezoid_over_epoch_milliseconds(self):
        # 2 GiB for 10 s, then a ramp to 4 GiB over the next 10 s: 20 + 30 GiB-seconds.
        series = [[0, 2 * GIB], [10_000, 2 * GIB], [20_000, 4 * GIB]]
        integral, duration = integrate_timeseries(series)
        self.assertAlmostEqual(integral / GIB, 50)
        self.assertEqual(duration, 20)

    def test_sorts_out_of_order_samples(self):
        series = [[20_000, 4 * GIB], [0, 2 * GIB], [10_000, 2 * GIB]]
        self.assertAlmostEqual(integrate_timeseries(series)[0] / GIB, 50)

    def test_fewer_than_two_samples_yields_zero(self):
        self.assertEqual(integrate_timeseries([]), (0.0, 0.0))
        self.assertEqual(integrate_timeseries([[0, GIB]]), (0.0, 0.0))


class StubMetricHelper(MetricHelper):
    """Skips the store and serves fixed cbft RSS series."""

    _title = 'Index build time, initial (sec), OPEN_AI dataset, 3 node, FTS, TCO FastMerge'
    _snapshots = ['snapshot']

    def __init__(self, timeseries=None, store=None, servers=()):
        self.test_config = SimpleNamespace(
            name='fts_tco_test', cluster=SimpleNamespace(initial_nodes=[len(servers)])
        )
        self.cluster_spec = SimpleNamespace(clusters=[('couchbase1', list(servers))])
        self.test = SimpleNamespace(cbmonitor_clusters=['couchbase1_850-1257_build_index_abcd'])
        self.series_store = store or {}
        if timeseries is not None:
            self.fts_rss_timeseries = timeseries

    def _read_timeseries(self, metric, collector, *, cluster=None, server=None, **kwargs):
        return self.series_store.get((collector, metric, cluster, server), [])

    def _metric_info(self, metric_id=None, title=None, chirality=None, **kwargs):
        return {'id': metric_id, 'title': title, 'chirality': chirality}


class FTSRssSeriesTest(TestCase):
    CLUSTER = 'couchbase1_850-1257_build_index_abcd'

    def test_prefers_atop_and_falls_back_to_the_rest_series(self):
        atop = [[0, GIB], [1000, GIB]]
        rest = [[0, 2 * GIB], [1000, 2 * GIB]]
        metrics = StubMetricHelper(
            servers=['fts-1', 'fts-2', 'kv-1'],
            store={
                ('atop', 'cbft_rss', self.CLUSTER, 'fts-1'): atop,
                ('metrics_rest_api_processes', 'cbft_sysproc_mem_resident',
                 self.CLUSTER, 'fts-1'): [[0, 9 * GIB]],
                ('metrics_rest_api_processes', 'cbft_sysproc_mem_resident',
                 self.CLUSTER, 'fts-2'): rest,
            },
        )
        # fts-1 uses atop even though the REST series exists; kv-1 has no cbft and is skipped.
        self.assertEqual(metrics.fts_rss_timeseries, [atop, rest])

    def test_no_series_on_any_node_is_empty(self):
        self.assertEqual(StubMetricHelper(servers=['kv-1']).fts_rss_timeseries, [])


class FTSRssKpiTest(TestCase):
    # Node A sits at 2 GiB for an hour. Node B is sampled unevenly: 1 GiB for 50 min, then a
    # single sample at 7 GiB 10 min later. A plain sample mean would weight both samples equally.
    NODE_A = [[0, 2 * GIB], [3_600_000, 2 * GIB]]
    NODE_B = [[0, GIB], [3_000_000, GIB], [3_600_000, 7 * GIB]]

    def setUp(self):
        self.metrics = StubMetricHelper([self.NODE_A, self.NODE_B])

    def test_peak_is_the_highest_sample_on_any_node(self):
        value, snapshots, info = self.metrics.fts_peak_rss()
        self.assertEqual(value, 7)
        self.assertEqual(snapshots, ['snapshot'])
        self.assertEqual(info['id'], 'fts_tco_test_peak_cbft_rss')
        self.assertEqual(info['title'],
                         'Peak cbft RSS (GB), OPEN_AI dataset, 3 node, FTS, TCO FastMerge')
        self.assertEqual(info['chirality'], -1)

    def test_average_is_time_weighted_per_node_then_averaged(self):
        # Node B: (50 min x 1 GiB + 10 min x 4 GiB avg) / 60 min = 1.5 GiB. Mean of 2 and 1.5.
        value, _, info = self.metrics.fts_avg_rss()
        self.assertEqual(value, 1.75)
        self.assertEqual(info['id'], 'fts_tco_test_avg_cbft_rss')

    def test_gb_hours_sums_across_nodes(self):
        value, _, info = self.metrics.fts_rss_gb_hours()
        self.assertEqual(value, 3.5)
        self.assertEqual(info['id'], 'fts_tco_test_cbft_gb_hours')

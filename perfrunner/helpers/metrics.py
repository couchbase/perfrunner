import numpy as np
from seriesly import Seriesly

from perfrunner.settings import CBMONITOR_HOST


class MetricHelper(object):

    def __init__(self, test):
        self.seriesly = Seriesly(CBMONITOR_HOST)
        self.test_config = test.test_config
        self.test_descr = test.test_config.get_test_descr()
        self.cluster_spec = test.cluster_spec
        self.cluster_names = test.cbagent.clusters.keys()
        self.build = test.build
        self.master_node = test.master_node

    @staticmethod
    def _get_query_params(metric, from_ts=None, to_ts=None):
        """Convert metric definition to Seriesly query params. E.g.:

        'avg_xdc_ops' -> {'ptr': '/xdc_ops',
                          'group': 1000000000000, 'reducer': 'avg'}

        Where group is constant."""
        params = {'ptr': '/{}'.format(metric[4:]),
                  'reducer': metric[:3],
                  'group': 1000000000000}
        if from_ts and to_ts:
            params.update({'from': from_ts, 'to': to_ts})
        return params

    def _get_metric_info(self, descr, larger_is_better=False, level='Basic'):
        return {'title': descr,
                'cluster': self.cluster_spec.name,
                'larger_is_better': str(larger_is_better).lower(),
                'level': level}

    def calc_avg_xdcr_ops(self):
        metric = '{}_avg_xdcr_ops_{}'.format(self.test_config.name,
                                             self.cluster_spec.name)
        descr = 'Avg. XDCR ops/sec, {}'.format(self.test_descr)
        metric_info = self._get_metric_info(descr, larger_is_better=True)
        query_params = self._get_query_params('avg_xdc_ops')

        xdcr_ops = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{}{}'.format(self.cluster_names[1], bucket)
            data = self.seriesly[db].query(query_params)
            xdcr_ops += data.values()[0][0]
        xdcr_ops = round(xdcr_ops, 1)

        return xdcr_ops, metric, metric_info

    def calc_avg_set_meta_ops(self):
        metric = '{}_avg_set_meta_ops_{}'.format(self.test_config.name,
                                                 self.cluster_spec.name)
        descr = 'Avg. XDCR rate (items/sec), {}'.format(self.test_descr)
        metric_info = self._get_metric_info(descr, larger_is_better=True)
        query_params = self._get_query_params('avg_ep_num_ops_set_meta')

        set_meta_ops = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{}{}'.format(self.cluster_names[1], bucket)
            data = self.seriesly[db].query(query_params)
            set_meta_ops += data.values()[0][0]
        set_meta_ops = round(set_meta_ops, 1)

        return set_meta_ops, metric, metric_info

    def calc_xdcr_lag(self, percentile=90):
        metric = '{}_{}th_xdc_lag_{}'.format(self.test_config.name,
                                             percentile,
                                             self.cluster_spec.name)
        descr = '{}th percentile replication lag (ms), {}'.format(
            percentile, self.test_descr)
        metric_info = self._get_metric_info(descr)

        timings = []
        for bucket in self.test_config.get_buckets():
            db = 'xdcr_lag{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].get_all()
            timings += [v['xdcr_lag'] for v in data.values()]
        lag = round(np.percentile(timings, percentile))

        return lag, metric, metric_info

    def calc_replication_changes_left(self):
        metric = '{}_avg_replication_queue_{}'.format(self.test_config.name,
                                                      self.cluster_spec.name)
        descr = 'Avg. replication queue, {}'.format(self.test_descr)
        metric_info = self._get_metric_info(descr)
        query_params = self._get_query_params('avg_replication_changes_left')

        queues = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].get_all()
            data = self.seriesly[db].query(query_params)
            queues += data.values()[0][0]
        queue = round(queues)

        return queue, metric, metric_info

    def calc_avg_replication_rate(self, time_elapsed):
        initial_items = self.test_config.get_load_settings().items
        num_buckets = self.test_config.get_num_buckets()
        avg_replication_rate = num_buckets * initial_items / time_elapsed

        return round(avg_replication_rate)

    def calc_avg_drain_rate(self, time_elapsed):
        items_per_node = self.test_config.get_load_settings().items / \
            self.test_config.get_initial_nodes()[0]
        drain_rate = items_per_node / time_elapsed

        return round(drain_rate)

    def calc_avg_ep_bg_fetched(self):
        query_params = self._get_query_params('avg_ep_bg_fetched')

        ep_bg_fetched = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            ep_bg_fetched += data.values()[0][0]
        ep_bg_fetched /= self.test_config.get_initial_nodes()[0]

        return round(ep_bg_fetched)

    def calc_avg_couch_views_ops(self):
        query_params = self._get_query_params('avg_couch_views_ops')

        couch_views_ops = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            couch_views_ops += data.values()[0][0]

        if self.build < '2.5.0':
            couch_views_ops /= self.test_config.get_initial_nodes()[0]

        return round(couch_views_ops)

    def calc_query_latency(self, percentile):
        metric = '{}_{}'.format(self.test_config.name, self.cluster_spec.name)
        descr = '{}th percentile query latency (ms), {}'.format(percentile,
                                                                self.test_descr)
        metric_info = self._get_metric_info(descr)

        timings = []
        for bucket in self.test_config.get_buckets():
            db = 'spring_query_latency{}{}'.format(self.cluster_names[0],
                                                   bucket)
            data = self.seriesly[db].get_all()
            timings += [value['latency_query'] for value in data.values()]
        query_latency = np.percentile(timings, percentile)

        return round(query_latency), metric, metric_info

    def calc_kv_latency(self, operation, percentile):
        metric = '{}_{}_{}th_{}'.format(self.test_config.name,
                                        operation,
                                        percentile,
                                        self.cluster_spec.name)
        descr = '{}th percentile {} {}'.format(percentile,
                                               operation.upper(),
                                               self.test_descr)
        metric_info = self._get_metric_info(descr)

        timings = []
        for bucket in self.test_config.get_buckets():
            db = 'spring_latency{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].get_all()
            timings += [
                v['latency_{}'.format(operation)] for v in data.values()
            ]
        latency = round(np.percentile(timings, percentile), 1)

        return latency, metric, metric_info

    def calc_observe_latency(self, percentile):
        metric = '{}_{}th_{}'.format(self.test_config.name, percentile,
                                     self.cluster_spec.name)
        descr = '{}th percentile {}'.format(percentile, self.test_descr)
        metric_info = self._get_metric_info(descr)

        timings = []
        for bucket in self.test_config.get_buckets():
            db = 'observe{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].get_all()
            timings += [v['latency_observe'] for v in data.values()]
        latency = round(np.percentile(timings, percentile))

        return latency, metric, metric_info

    def calc_cpu_utilization(self, from_ts=None, to_ts=None, meta=None):
        metric = '{}_avg_cpu_{}'.format(self.test_config.name,
                                        self.cluster_spec.name)
        if meta:
            metric = '{}_{}'.format(metric, meta.split()[0].lower())
        descr = 'Avg. CPU utilization rate (%)'
        if meta:
            descr = '{}, {}'.format(descr, meta)
        descr = '{}, {}'.format(descr, self.test_descr)
        metric_info = self._get_metric_info(descr, level='Advanced')

        host = self.master_node.split(':')[0].replace('.', '')
        cluster = self.cluster_names[0]
        bucket = tuple(self.test_config.get_buckets())[0]

        query_params = self._get_query_params('avg_cpu_utilization_rate',
                                              from_ts, to_ts)
        db = 'ns_server{}{}{}'.format(cluster, bucket, host)
        data = self.seriesly[db].query(query_params)
        cpu_utilazion = round(data.values()[0][0], 1)

        return cpu_utilazion, metric, metric_info

    def calc_views_disk_size(self, from_ts=None, to_ts=None, meta=None):
        metric = '{}_max_views_disk_size_{}'.format(
            self.test_config.name, self.cluster_spec.name
        )
        if meta:
            metric = '{}_{}'.format(metric, meta.split()[0].lower())
        descr = 'Max. views disk size (GB)'
        if meta:
            descr = '{}, {}'.format(descr, meta)
        descr = '{}, {}'.format(descr, self.test_descr)
        descr = descr.replace(' (min)', '')  # rebalance tests
        metric_info = self._get_metric_info(descr, level='Advanced')

        query_params = self._get_query_params('max_couch_views_actual_disk_size',
                                              from_ts, to_ts)

        disk_size = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            disk_size += round(data.values()[0][0] / 1024 ** 3, 1)  # -> GB

        return disk_size, metric, metric_info

    def calc_max_beam_rss(self):
        metric = 'beam_rss_max_{}_{}'.format(self.test_config.name,
                                             self.cluster_spec.name)
        descr = 'Max. beam.smp RSS (MB), {}'.format(self.test_descr)
        metric_info = self._get_metric_info(descr)

        query_params = self._get_query_params('max_beam.smp_rss')

        max_rss = 0
        for cluster_name, servers in self.cluster_spec.yield_clusters():
            cluster = filter(lambda name: name.startswith(cluster_name),
                             self.cluster_names)[0]
            master = servers[0].split(':')[0].replace('.', '')
            db = 'atop{}{}'.format(cluster, master)  # Legacy
            data = self.seriesly[db].query(query_params)
            rss = round(data.values()[0][0] / 1024 ** 2)
            max_rss = max(max_rss, rss)

        return max_rss, metric, metric_info

    def get_indexing_meta(self, value, index_type):
        metric = '{}_{}_{}'.format(self.test_config.name,
                                   index_type.lower(),
                                   self.cluster_spec.name)
        descr = '{} index (min), {}'.format(index_type,
                                            self.test_descr)
        metric_info = self._get_metric_info(descr)

        return value, metric, metric_info

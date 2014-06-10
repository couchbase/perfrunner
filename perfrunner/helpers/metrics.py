import numpy as np
from collections import OrderedDict
from logger import logger
from seriesly import Seriesly

from perfrunner.settings import CBMONITOR, SGW_SERIESLY_HOST


class MetricHelper(object):

    def __init__(self, test):
        self.seriesly = Seriesly(CBMONITOR['host'])
        self.test_config = test.test_config
        self.metric_title = test.test_config.test_case.metric_title
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

    def _get_metric_info(self, title, larger_is_better=False, level='Basic'):
        return {'title': title,
                'cluster': self.cluster_spec.name,
                'larger_is_better': str(larger_is_better).lower(),
                'level': level}

    def calc_avg_xdcr_ops(self):
        metric = '{}_avg_xdcr_ops_{}'.format(self.test_config.name,
                                             self.cluster_spec.name)
        title = 'Avg. XDCR ops/sec, {}'.format(self.metric_title)
        metric_info = self._get_metric_info(title, larger_is_better=True)
        query_params = self._get_query_params('avg_xdc_ops')

        xdcr_ops = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[1], bucket)
            data = self.seriesly[db].query(query_params)
            xdcr_ops += data.values()[0][0]
        xdcr_ops = round(xdcr_ops, 1)

        return xdcr_ops, metric, metric_info

    def calc_avg_set_meta_ops(self):
        metric = '{}_avg_set_meta_ops_{}'.format(self.test_config.name,
                                                 self.cluster_spec.name)
        title = 'Avg. XDCR rate (items/sec), {}'.format(self.metric_title)
        metric_info = self._get_metric_info(title, larger_is_better=True)
        query_params = self._get_query_params('avg_ep_num_ops_set_meta')

        set_meta_ops = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[1], bucket)
            data = self.seriesly[db].query(query_params)
            set_meta_ops += data.values()[0][0]
        set_meta_ops = round(set_meta_ops, 1)

        return set_meta_ops, metric, metric_info

    def calc_xdcr_lag(self, percentile=90):
        metric = '{}_{}th_xdc_lag_{}'.format(self.test_config.name,
                                             percentile,
                                             self.cluster_spec.name)
        title = '{}th percentile replication lag (ms), {}'.format(
            percentile, self.metric_title)
        metric_info = self._get_metric_info(title)
        query_params = self._get_query_params('avg_xdcr_lag')
        query_params.update({'group': 1000})

        timings = []
        for bucket in self.test_config.buckets:
            db = 'xdcr_lag{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            timings = [v[0] for v in data.values()]
        lag = round(np.percentile(timings, percentile))

        return lag, metric, metric_info

    def calc_replication_changes_left(self):
        metric = '{}_avg_replication_queue_{}'.format(self.test_config.name,
                                                      self.cluster_spec.name)
        title = 'Avg. replication queue, {}'.format(self.metric_title)
        metric_info = self._get_metric_info(title)
        query_params = self._get_query_params('avg_replication_changes_left')

        queues = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            queues += data.values()[0][0]
        queue = round(queues)

        return queue, metric, metric_info

    def calc_avg_replication_rate(self, time_elapsed):
        initial_items = self.test_config.load_settings.ops or \
            self.test_config.load_settings.items
        num_buckets = self.test_config.cluster.num_buckets
        avg_replication_rate = num_buckets * initial_items / time_elapsed

        return round(avg_replication_rate)

    def calc_max_drain_rate(self, time_elapsed):
        items_per_node = self.test_config.load_settings.items / \
            self.test_config.cluster.initial_nodes[0]
        drain_rate = items_per_node / time_elapsed

        return round(drain_rate)

    def calc_avg_disk_write_queue(self):
        query_params = self._get_query_params('avg_disk_write_queue')

        disk_write_queue = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            disk_write_queue += data.values()[0][0]
        disk_write_queue /= self.test_config.cluster.initial_nodes[0]

        return round(disk_write_queue / 10 ** 3)

    def calc_avg_ep_bg_fetched(self):
        query_params = self._get_query_params('avg_ep_bg_fetched')

        ep_bg_fetched = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            ep_bg_fetched += data.values()[0][0]
        ep_bg_fetched /= self.test_config.cluster.initial_nodes[0]

        return round(ep_bg_fetched)

    def calc_avg_bg_wait_time(self):
        query_params = self._get_query_params('avg_avg_bg_wait_time')

        avg_bg_wait_time = []
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            avg_bg_wait_time.append(data.values()[0][0])
        avg_bg_wait_time = np.mean(avg_bg_wait_time) / 10 ** 3  # us -> ms

        return round(avg_bg_wait_time, 1)

    def calc_avg_couch_views_ops(self):
        query_params = self._get_query_params('avg_couch_views_ops')

        couch_views_ops = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            couch_views_ops += data.values()[0][0]

        if self.build < '2.5.0':
            couch_views_ops /= self.test_config.cluster.initial_nodes[0]

        return round(couch_views_ops)

    def calc_query_latency(self, percentile):
        metric = '{}_{}'.format(self.test_config.name, self.cluster_spec.name)
        title = '{}th percentile query latency (ms), {}'.format(percentile,
                                                                self.metric_title)
        metric_info = self._get_metric_info(title)

        timings = []
        for bucket in self.test_config.buckets:
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
        title = '{}th percentile {} {}'.format(percentile,
                                               operation.upper(),
                                               self.metric_title)
        metric_info = self._get_metric_info(title)

        timings = []
        for bucket in self.test_config.buckets:
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
        title = '{}th percentile {}'.format(percentile, self.metric_title)
        metric_info = self._get_metric_info(title)

        timings = []
        for bucket in self.test_config.buckets:
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
        title = 'Avg. CPU utilization rate (%)'
        if meta:
            title = '{}, {}'.format(title, meta)
        title = '{}, {}'.format(title, self.metric_title)
        metric_info = self._get_metric_info(title, level='Advanced')

        host = self.master_node.split(':')[0].replace('.', '')
        cluster = self.cluster_names[0]
        bucket = self.test_config.buckets[0]

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
        title = 'Max. views disk size (GB)'
        if meta:
            title = '{}, {}'.format(title, meta)
        title = '{}, {}'.format(title, self.metric_title)
        title = title.replace(' (min)', '')  # rebalance tests
        metric_info = self._get_metric_info(title, level='Advanced')

        query_params = self._get_query_params('max_couch_views_actual_disk_size',
                                              from_ts, to_ts)

        disk_size = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(query_params)
            disk_size += round(data.values()[0][0] / 1024 ** 3, 1)  # -> GB

        return disk_size, metric, metric_info

    def calc_max_beam_rss(self):
        metric = 'beam_rss_max_{}_{}'.format(self.test_config.name,
                                             self.cluster_spec.name)
        title = 'Max. beam.smp RSS (MB), {}'.format(self.metric_title)
        metric_info = self._get_metric_info(title)

        query_params = self._get_query_params('max_beam.smp_rss')

        max_rss = 0
        for cluster_name, servers in self.cluster_spec.yield_clusters():
            cluster = filter(lambda name: name.startswith(cluster_name),
                             self.cluster_names)[0]
            for server in servers:
                hostname = server.split(':')[0].replace('.', '')
                db = 'atop{}{}'.format(cluster, hostname)  # Legacy
                data = self.seriesly[db].query(query_params)
                rss = round(data.values()[0][0] / 1024 ** 2)
                max_rss = max(max_rss, rss)

        return max_rss, metric, metric_info

    def get_indexing_meta(self, value, index_type):
        metric = '{}_{}_{}'.format(self.test_config.name,
                                   index_type.lower(),
                                   self.cluster_spec.name)
        title = '{} index (min), {}'.format(index_type,
                                            self.metric_title)
        metric_info = self._get_metric_info(title)

        return value, metric, metric_info

    def calc_compaction_speed(self, time_elapsed, bucket=True):
        if bucket:
            max_query_params = \
                self._get_query_params('max_couch_docs_actual_disk_size')
            min_query_params = \
                self._get_query_params('min_couch_docs_actual_disk_size')
        else:
            max_query_params = \
                self._get_query_params('max_couch_views_actual_disk_size')
            min_query_params = \
                self._get_query_params('min_couch_views_actual_disk_size')

        max_diff = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(max_query_params)
            disk_size_before = data.values()[0][0]

            db = 'ns_server{}{}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(min_query_params)
            disk_size_after = data.values()[0][0]

            max_diff = max(max_diff, disk_size_before - disk_size_after)

        diff = max_diff / 1024 ** 2 / time_elapsed  # Mbytes/sec
        return round(diff, 1)

    def failover_time(self, reporter):
        metric = '{}_{}_failover'.format(self.test_config.name,
                                         self.cluster_spec.name)
        _split = self.metric_title.split(', ')

        title = 'Graceful failover (min), {}, {}'.format(
            _split[1][-1] + _split[1][1:-1] + _split[1][0],
            ', '.join(_split[2:]))
        metric_info = self._get_metric_info(title, larger_is_better=False)

        rebalance_time = reporter.finish('Failover')

        return rebalance_time, metric, metric_info

    @property
    def calc_network_throughput(self):
        for cluster_name, servers in self.cluster_spec.yield_clusters():
            cluster = filter(lambda name: name.startswith(cluster_name), self.cluster_names)[0]
            in_bytes_per_sec = []
            out_bytes_per_sec = []
            for server in servers:
                hostname = server.split(':')[0].replace('.', '')
                db = 'net{}{}'.format(cluster, hostname)
                data = self.seriesly[db].get_all()
                in_bytes_per_sec += [v['in_bytes_per_sec'] for v in data.values()]
                out_bytes_per_sec += [v['out_bytes_per_sec'] for v in data.values()]
        f = lambda v: format(int(v), ',d')
        network_matrix = OrderedDict((
            ('min in_bytes  per sec', f(min(in_bytes_per_sec))),
            ('max in_bytes  per sec', f(max(in_bytes_per_sec))),
            ('avg in_bytes  per sec', f(np.mean(in_bytes_per_sec))),
            ('p50 in_bytes  per sec', f(np.percentile(in_bytes_per_sec, 50))),
            ('p95 in_bytes  per sec', f(np.percentile(in_bytes_per_sec, 95))),
            ('p99 in_bytes  per sec', f(np.percentile(in_bytes_per_sec, 99))),
            ('min out_bytes per sec', f(min(out_bytes_per_sec))),
            ('max out_bytes per sec', f(max(out_bytes_per_sec))),
            ('avg out_bytes per sec', f(np.mean(out_bytes_per_sec))),
            ('p50 out_bytes per sec', f(np.percentile(out_bytes_per_sec, 50))),
            ('p95 out_bytes per sec', f(np.percentile(out_bytes_per_sec, 95))),
            ('p99 out_bytes per sec', f(np.percentile(out_bytes_per_sec, 99)))))
        return network_matrix


class SgwMetricHelper(MetricHelper):

    def __init__(self, *args, **kwargs):
        super(SgwMetricHelper, self).__init__(*args, **kwargs)
        self.seriesly = Seriesly(SGW_SERIESLY_HOST)

    def calc_push_latency(self, p=95, idx=1):
        query_params = self._get_query_params(
            'avg_gateload/ops/PushToSubscriberInteractive/p{}'.format(p)
        )
        db = 'gateload_{}'.format(idx)

        if not self.seriesly[db].get_all():
            logger.error('No data in {}'.format(db))

        data = self.seriesly[db].query(query_params)
        latency = float(data.values()[0][0])
        latency /= 10 ** 9  # ns -> s

        return round(latency, 2)

    def calc_sync_gateway_requests_per_sec(self, idx=1):
        query_params = self._get_query_params(
            'avg_syncGateway_rest/requests_total'
        )
        # Group by 1 second
        query_params.update({'group': 1000})
        db = 'gateway_{}'.format(idx)

        if not self.seriesly[db].get_all():
            logger.error('No data in {}'.format(db))

        data = self.seriesly[db].query(query_params)
        total_requests = sorted(v[0] for v in data.values())
        request_per_sec = [n - c for c, n in zip(total_requests, total_requests[1:])]
        return round(np.mean(request_per_sec))

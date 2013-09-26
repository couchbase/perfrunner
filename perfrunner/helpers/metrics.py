import math

from seriesly import Seriesly

from perfrunner.settings import CbAgentSettings


class MetricHelper(object):

    def __init__(self, test):
        self.seriesly = Seriesly(CbAgentSettings.seriesly_host)
        self.test_config = test.test_config
        self.cluster_spec = test.cluster_spec
        self.cluster_names = test.cbagent.clusters.keys()

    @staticmethod
    def _calc_percentile(data, percentile):
        data.sort()
        k = (len(data) - 1) * percentile
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return data[int(k)]
        else:
            return data[int(f)] * (c - k) + data[int(c)] * (k - f)

    def calc_avg_xdcr_ops(self):
        metric = '{0}_avg_xdcr_ops_{1}'.format(self.test_config.name,
                                               self.cluster_spec.name)
        descr = 'Avg. XDCR ops/sec, {0}'.format(
            self.test_config.get_test_descr())
        metric_info = {'title': descr, 'cluster': self.cluster_spec.name,
                       'larger_is_better': 'true'}
        params = {'group': 1000000000000, 'ptr': '/xdc_ops', 'reducer': 'avg'}

        xdcr_ops = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{0}{1}'.format(self.cluster_names[1], bucket)
            data = self.seriesly[db].query(params)
            xdcr_ops += data.values()[0][0]
        xdcr_ops = round(xdcr_ops, 1)

        return xdcr_ops, metric, metric_info

    def calc_avg_set_meta_ops(self):
        metric = '{0}_avg_set_meta_ops_{1}'.format(self.test_config.name,
                                                   self.cluster_spec.name)
        descr = 'Avg. setMeta ops/sec, {0}'.format(
            self.test_config.get_test_descr())
        metric_info = {'title': descr, 'cluster': self.cluster_spec.name,
                       'larger_is_better': 'true'}
        params = {'group': 1000000000000, 'ptr': '/ep_num_ops_set_meta',
                  'reducer': 'avg'}

        set_meta_ops = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{0}{1}'.format(self.cluster_names[1], bucket)
            data = self.seriesly[db].query(params)
            set_meta_ops += data.values()[0][0]
        set_meta_ops = round(set_meta_ops, 1)

        return set_meta_ops, metric, metric_info

    def calc_max_xdcr_lag(self):
        metric = '{0}_max_xdc_lag_{1}'.format(self.test_config.name,
                                              self.cluster_spec.name)
        descr = 'Max. replication lag (sec), {0}'.format(
            self.test_config.get_test_descr())
        metric_info = {'title': descr, 'cluster': self.cluster_spec.name,
                       'larger_is_better': 'false'}
        params = {'group': 1000000000000, 'ptr': '/xdcr_lag', 'reducer': 'max'}

        max_lag = 0
        for bucket in self.test_config.get_buckets():
            db = 'xdcr_lag{0}{1}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(params)
            max_lag = max(max_lag, data.values()[0][0])
        max_lag = round(max_lag / 1000, 1)

        return max_lag, metric, metric_info

    def calc_max_replication_changes_left(self):
        metric = '{0}_max_replication_queue_{1}'.format(self.test_config.name,
                                                        self.cluster_spec.name)
        descr = 'Max. replication queue, {0}'.format(
            self.test_config.get_test_descr())
        metric_info = {'title': descr, 'cluster': self.cluster_spec.name,
                       'larger_is_better': 'false'}
        params = {'group': 1000000000000,
                  'ptr': '/replication_changes_left', 'reducer': 'max'}

        max_queue = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{0}{1}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(params)
            max_queue += data.values()[0][0]
        max_queue = round(max_queue)

        return max_queue, metric, metric_info

    def calc_avg_replication_rate(self, time_elapsed):
        initial_items = self.test_config.get_load_settings().ops
        buckets = self.test_config.get_num_buckets()
        return round(buckets * initial_items / (time_elapsed * 60))

    def calc_avg_drain_rate(self):
        params = {'group': 1000000000000,
                  'ptr': '/ep_diskqueue_drain', 'reducer': 'avg'}
        drain_rate = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{0}{1}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(params)
            drain_rate += data.values()[0][0]
        drain_rate /= self.test_config.get_initial_nodes()
        return round(drain_rate)

    def calc_avg_ep_bg_fetched(self):
        params = {'group': 1000000000000,
                  'ptr': '/ep_bg_fetched', 'reducer': 'avg'}
        ep_bg_fetched = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{0}{1}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(params)
            ep_bg_fetched += data.values()[0][0]
        ep_bg_fetched /= self.test_config.get_initial_nodes()
        return round(ep_bg_fetched)

    def calc_avg_couch_views_ops(self):
        params = {'group': 1000000000000,
                  'ptr': '/couch_views_ops', 'reducer': 'avg'}
        couch_views_ops = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{0}{1}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].query(params)
            couch_views_ops += data.values()[0][0]
        couch_views_ops /= self.test_config.get_initial_nodes()
        return round(couch_views_ops)

    def calc_query_latency(self, percentile=0.9):
        timings = []
        for bucket in self.test_config.get_buckets():
            db = 'spring_query_latency{0}{1}'.format(self.cluster_names[0],
                                                     bucket)
            data = self.seriesly[db].get_all()
            timings += [value['latency_query'] for value in data.values()]
        return round(self._calc_percentile(timings, percentile))

    def calc_kv_latency(self, operation, percentile=0.9):
        percentile_int = int(percentile * 100)
        metric = '{0}_{1}_{2}th_{3}'.format(
            self.test_config.name,
            operation,
            percentile_int,
            self.cluster_spec.name)
        descr = '{0}th percentile {1} {2}'.format(
            percentile_int,
            operation.upper(),
            self.test_config.get_test_descr()
        )
        metric_info = {'title': descr, 'cluster': self.cluster_spec.name,
                       'larger_is_better': 'false'}

        timings = []
        for bucket in self.test_config.get_buckets():
            db = 'spring_latency{0}{1}'.format(self.cluster_names[0], bucket)
            data = self.seriesly[db].get_all()
            timings += [
                v['latency_{0}'.format(operation)] for v in data.values()
            ]
        latency = round(self._calc_percentile(timings, percentile), 1)

        return latency, metric, metric_info

    def calc_cpu_utilization(self):
        cpu_utilazion = dict()
        params = {'group': 1000000000000,
                  'ptr': '/cpu_utilization_rate', 'reducer': 'avg'}
        for cluster, master_host in self.cluster_spec.get_masters().items():
            cluster_name = filter(lambda name: name.startswith(cluster),
                                  self.cluster_names)[0]
            host = master_host.split(':')[0].replace('.', '')
            for bucket in self.test_config.get_buckets():
                db = 'ns_server{0}{1}{2}'.format(cluster_name, bucket, host)
                data = self.seriesly[db].query(params)
                cpu_utilazion[cluster] = round(data.values()[0][0], 2)
        return cpu_utilazion

    def get_indexing_meta(self, value, index_type='Initial'):
        metric = '{0}_{1}_{2}'.format(
            self.test_config.name, index_type.lower(), self.cluster_spec.name
        )
        descr = '{0} index (min), {1}'.format(
            index_type, self.test_config.get_test_descr()
        )
        metric_info = {'title': descr,
                       'cluster': self.cluster_spec.name,
                       'larger_is_better': 'false'}
        return value, metric, metric_info

from seriesly import Seriesly

from perfrunner.settings import CbAgentSettings


class MetricHelper(object):

    def __init__(self, test):
        self.seriesly = Seriesly(CbAgentSettings.seriesly_host)
        self.test_config = test.test_config
        self.cluster_spec_name = test.cluster_spec.name
        self.cluster_name = test.cbagent.clusters.keys()[0]

    def calc_max_xdcr_lag(self):
        metric = '{0}_max_xdc_lag_{1}'.format(self.test_config.name,
                                              self.cluster_spec_name)
        descr = 'Max. replication lag (sec), {0}'.format(
            self.test_config.get_test_descr())
        metric_info = {'title': descr, 'cluster': self.cluster_spec_name,
                       'larger_is_better': 'false'}
        params = {'group': 1000000000000, 'ptr': '/xdcr_lag', 'reducer': 'max'}

        max_lag = 0
        for bucket in self.test_config.get_buckets():
            db = 'xdcr_lag{0}{1}'.format(self.cluster_name, bucket)
            data = self.seriesly[db].query(params)
            max_lag = max(max_lag, data.values()[0][0])
        max_lag = round(max_lag / 1000, 1)

        return max_lag, metric, metric_info

    def calc_max_replication_changes_left(self):
        metric = '{0}_max_replication_queue_{1}'.format(self.test_config.name,
                                                        self.cluster_spec_name)
        descr = 'Max. replication queue, {0}'.format(
            self.test_config.get_test_descr())
        metric_info = {'title': descr, 'cluster': self.cluster_spec_name,
                       'larger_is_better': 'false'}
        params = {'group': 1000000000000,
                  'ptr': '/replication_changes_left', 'reducer': 'max'}

        max_queue = 0
        for bucket in self.test_config.get_buckets():
            db = 'ns_server{0}{1}'.format(self.cluster_name, bucket)
            data = self.seriesly[db].query(params)
            max_queue += data.values()[0][0]
        max_queue = round(max_queue)

        return max_queue, metric, metric_info

    def calc_avg_replication_rate(self, time_elapsed):
        initial_items = self.test_config.get_load_settings().ops
        buckets = self.test_config.get_num_buckets()
        return round(buckets * initial_items / (time_elapsed * 60))

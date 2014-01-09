import time

from btrc import CouchbaseClient, StatsReporter
from couchbase import Couchbase
from logger import logger

from perfrunner.helpers.misc import uhex, pretty_dict
from perfrunner.settings import SF_STORAGE


class BtrcReporter(object):

    def __init__(self, test):
        self.test = test

    def reset_utilzation_stats(self):
        for target in self.test.target_iterator:
            logger.info('Resetting utilization stats from {}/{}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket,
                                 target.username, target.password)
            cb.reset_utilization_stats()

    def save_utilzation_stats(self):
        for target in self.test.target_iterator:
            logger.info('Saving utilization stats from {}/{}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket,
                                 target.username, target.password)
            reporter = StatsReporter(cb)
            reporter.report_stats('util_stats')

    def save_btree_stats(self):
        for target in self.test.target_iterator:
            logger.info('Saving B-tree stats from {}/{}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket,
                                 target.username, target.password)
            reporter = StatsReporter(cb)
            reporter.report_stats('btree_stats')


class SFReporter(object):

    def __init__(self, test):
        self.test = test

    def _add_cluster(self):
        cluster = self.test.cluster_spec.name
        params = self.test.cluster_spec.get_parameters()
        try:
            cb = Couchbase.connect(bucket='clusters', **SF_STORAGE)
            cb.set(cluster, params)
        except Exception, e:
            logger.warn('Failed to add cluster, {}'.format(e))
        else:
            logger.info('Successfully posted: {}, {}'.format(
                cluster, pretty_dict(params)
            ))

    def _add_metric(self, metric, metric_info):
        if metric_info is None:
            metric_info = {
                'title': self.test.test_config.get_test_descr(),
                'cluster': self.test.cluster_spec.name,
                'larger_is_better': self.test.test_config.get_regression_criterion(),
                'level': self.test.test_config.get_level(),
            }
        try:
            cb = Couchbase.connect(bucket='metrics', **SF_STORAGE)
            cb.set(metric, metric_info)
        except Exception, e:
            logger.warn('Failed to add cluster, {}'.format(e))
        else:
            logger.info('Successfully posted: {}, {}'.format(
                metric, pretty_dict(metric_info)
            ))

    def _prepare_data(self, metric, value):
        key = uhex()
        master_node = self.test.cluster_spec.get_masters().values()[0]
        build = self.test.rest.get_version(master_node)
        data = {'build': build, 'metric': metric, 'value': value,
                'snapshots': self.test.snapshots}
        return key, data

    @staticmethod
    def _mark_previous_as_obsolete(cb, benckmark):
        for row in cb.query('benchmarks', 'values_by_build_and_metric',
                            key=[benckmark['metric'], benckmark['build']]):
            doc = cb.get(row.docid)
            doc.value.update({'obsolete': True})
            cb.set(row.docid, doc.value)

    def _log_benchmark(self, metric, value):
        _, benckmark = self._prepare_data(metric, value)
        logger.info('Dry run stats: {}'.format(
            pretty_dict(benckmark)
        ))

    def _post_benckmark(self, metric, value):
        key, benckmark = self._prepare_data(metric, value)
        try:
            cb = Couchbase.connect(bucket='benchmarks', **SF_STORAGE)
            self._mark_previous_as_obsolete(cb, benckmark)
            cb.set(key, benckmark)
        except Exception, e:
            logger.warn('Failed to post results, {}'.format(e))
        else:
            logger.info('Successfully posted: {}'.format(
                pretty_dict(benckmark)
            ))

    def post_to_sf(self, value, metric=None, metric_info=None):
        if metric is None:
            metric = '{}_{}'.format(self.test.test_config.name,
                                    self.test.cluster_spec.name)

        stats_settings = self.test.test_config.get_stats_settings()

        if stats_settings.post_to_sf:
            self._add_metric(metric, metric_info)
            self._add_cluster()
            self._post_benckmark(metric, value)
        else:
            self._log_benchmark(metric, value)


class LogReporter(object):

    def __init__(self, test):
        self.test = test

    def save_web_logs(self):
        for target in self.test.target_iterator:
            logs = self.test.rest.get_logs(target.node)
            fname = 'web_log_{}.json'.format(target.node.split(':')[0])
            with open(fname, 'w') as fh:
                fh.write(pretty_dict(logs))

    def save_master_events(self):
        for target in self.test.target_iterator:
            master_events = self.test.rest.get_master_events(target.node)
            fname = 'master_events_{}.log'.format(target.node.split(':')[0])
            with open(fname, 'w') as fh:
                fh.write(master_events)


class Reporter(BtrcReporter, SFReporter, LogReporter):

    def start(self):
        self.ts = time.time()

    def finish(self, action, time_elapsed=None):
        time_elapsed = time_elapsed or (time.time() - self.ts)
        time_elapsed = round(time_elapsed / 60, 1)
        logger.info(
            'Time taken to perform "{}": {} min'.format(action, time_elapsed)
        )
        return time_elapsed

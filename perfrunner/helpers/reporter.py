import time
from uuid import uuid4

from btrc import CouchbaseClient, StatsReporter
from couchbase import Couchbase
from logger import logger

from perfrunner.settings import SF_STORAGE


class BtrcReporter(object):

    def __init__(self, test):
        self.test = test

    def reset_utilzation_stats(self):
        for target in self.test.target_iterator:
            logger.info('Resetting utilization stats from {0}/{1}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket)
            cb.reset_utilization_stats()

    def save_util_stats(self):
        for target in self.test.target_iterator:
            logger.info('Saving utilization stats from {0}/{1}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket)
            reporter = StatsReporter(cb)
            reporter.report_stats('util_stats')

    def save_btree_stats(self):
        for target in self.test.target_iterator:
            logger.info('Saving B-tree stats from {0}/{1}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket)
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
            logger.warn('Failed to add cluster, {0}'.format(e))
        else:
            logger.info('Successfully posted: {0}, {1}'.format(
                cluster, params))

    def _add_metric(self, metric):
        metric_info = {
            'title': self.test.test_config.get_test_descr(),
            'cluster': self.test.cluster_spec.name
        }
        try:
            cb = Couchbase.connect(bucket='metrics', **SF_STORAGE)
            cb.set(metric, metric_info)
        except Exception, e:
            logger.warn('Failed to add cluster, {0}'.format(e))
        else:
            logger.info('Successfully posted: {0}, {1}'.format(metric,
                                                               metric_info))

    def _prepare_data(self, metric, value):
        key = uuid4().hex
        master_node = self.test.cluster_spec.get_clusters()[0][0]
        build = self.test.rest.get_version(master_node)
        data = {'build': build, 'metric': metric, 'value': value}
        return key, data

    def _mark_previous_as_obsolete(self, cb, benckmark):
        for row in cb.query('benchmarks', 'values_by_build_and_metric',
                            key=[benckmark['metric'], benckmark['build']]):
            doc = cb.get(row.docid)
            doc.value.update({'obsolete': True})
            cb.set(row.docid, doc.value)

    def _post_benckmark(self, metric, value):
        key, benckmark = self._prepare_data(metric, value)
        try:
            cb = Couchbase.connect(bucket='benchmarks', **SF_STORAGE)
            self._mark_previous_as_obsolete(cb, benckmark)
            cb.set(key, benckmark)
        except Exception, e:
            logger.warn('Failed to post results, {0}'.format(e))
        else:
            logger.info('Successfully posted: {0}'.format(benckmark))

    def post_to_sf(self, value, metric=None):
        if metric is None:
            metric = '{0}_{1}'.format(self.test.test_config.name,
                                      self.test.cluster_spec.name)
        self._add_metric(metric)
        self._add_cluster()
        self._post_benckmark(metric, value)


class MasterEventsReporter(object):

    def __init__(self, test):
        self.test = test

    def save_master_events(self):
        for target in self.test.target_iterator:
            master_events = self.test.rest.get_master_events(target.node)
            fname = '{0}_master_events.log'.format(target.node.split(':')[0])
            with open(fname, "w") as fh:
                fh.write(master_events)


class Reporter(BtrcReporter, SFReporter):

    def start(self):
        self.ts = time.time()

    def finish(self, action):
        elapsed = time.time() - self.ts
        logger.info(
            'Time taken to perform "{0}": {1:.1f} sec'.format(action, elapsed)
        )
        return elapsed

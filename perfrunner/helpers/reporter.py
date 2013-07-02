import time
from uuid import uuid4

from btrc import CouchbaseClient, StatsReporter
from couchbase import Couchbase
from logger import logger

from perfrunner.settings import SF_STORAGE


class Reporter(object):

    def start(self):
        self.ts = time.time()

    def finish(self, action):
        elapsed = time.time() - self.ts
        logger.info(
            'Time taken to perform "{0}": {1:.1f} sec'.format(action, elapsed)
        )
        return elapsed

    def post_to_sf(self, *args):
        SFReporter(*args).post()

    def btree_stats(self, host_port, bucket):
        logger.info('Getting B-tree stats from {0}/{1}'.format(
            host_port, bucket))
        cb = CouchbaseClient(host_port, bucket)
        reporter = StatsReporter(cb)
        reporter.report_stats('btree_stats')


class SFReporter(object):

    def __init__(self, test, value, metric=None):
        self.test = test
        self.value = value
        if metric is None:
            self.metric = '{0}_{1}'.format(self.test.test_config.name,
                                           self.test.cluster_spec.name)
        else:
            self.metric = metric

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

    def _add_metric(self):
        metric_info = {
            'title': self.test.test_config.get_test_descr(),
            'cluster': self.test.cluster_spec.name
        }
        try:
            cb = Couchbase.connect(bucket='metrics', **SF_STORAGE)
            cb.set(self.metric, metric_info)
        except Exception, e:
            logger.warn('Failed to add cluster, {0}'.format(e))
        else:
            logger.info('Successfully posted: {0}, {1}'.format(
                self.metric, metric_info))

    def _prepare_data(self):
        key = uuid4().hex
        master_node = self.test.cluster_spec.get_clusters()[0][0]
        build = self.test.rest.get_version(master_node)
        data = {'build': build, 'metric': self.metric, 'value': self.value}
        return key, data

    def _mark_previous_as_obsolete(self, cb):
        for row in cb.query('benchmarks', 'all_ids'):
            doc = cb.get(row.key)
            doc.update({'obsolete': True})
            cb.set(row.key, doc)

    def _post_benckmark(self):
        key, benckmark = self._prepare_data()
        try:
            cb = Couchbase.connect(bucket='benchmarks', **SF_STORAGE)
            self._mark_previous_as_obsolete(cb)
            cb.set(key, benckmark)
        except Exception, e:
            logger.warn('Failed to post results, {0}'.format(e))
        else:
            logger.info('Successfully posted: {0}'.format(benckmark))

    def post(self):
        self._add_metric()
        self._add_cluster()
        self._post_benckmark()

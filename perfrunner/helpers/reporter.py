import time
from uuid import uuid4

from btrc import CouchbaseClient, StatsReporter
from couchbase import Couchbase
from logger import logger

from perfrunner.settings import ShowFastSettings


class Reporter(object):

    def start(self):
        self.ts = time.time()

    def finish(self, action):
        elapsed = time.time() - self.ts
        logger.info(
            'Time taken to perform "{0}": {1:.1f} sec'.format(action, elapsed)
        )
        return elapsed

    def post_to_sf(self, test, value, metric=None):
        key = uuid4().hex
        master_node = test.cluster_spec.get_clusters()[0][0]
        build = test.rest.get_version(master_node)
        if metric is None:
            metric = '{0}_{1}'.format(test.test_config.name,
                                      test.cluster_spec.name)
        data = {'build': build, 'metric': metric, 'value': value}
        try:
            cb = Couchbase.connect(host=ShowFastSettings.HOST,
                                   port=ShowFastSettings.PORT,
                                   username=ShowFastSettings.USERNAME,
                                   password=ShowFastSettings.PASSWORD,
                                   bucket='benchmarks')
            cb.set(key, data)
        except Exception, e:
            logger.warn('Failed to post results, {0}'.format(e))
        else:
            logger.info('Successfully posted: {0}'.format(data))

    def btree_stats(self, host_port, bucket):
        cb = CouchbaseClient(host_port, bucket)
        reporter = StatsReporter(cb)
        reporter.report_stats('btree_stats')

from perfrunner.tests.kv import KVTest
from perfrunner.helpers.cbmonitor import with_stats


class NumaKVTest(KVTest):

    @with_stats(latency=True)
    def access(self):
        super(NumaKVTest, self).timer()

    def run(self):
        super(NumaKVTest, self).run()
        for operation in ('get', 'set'):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_kv_latency(operation=operation,
                    percentile=0.9)
            )


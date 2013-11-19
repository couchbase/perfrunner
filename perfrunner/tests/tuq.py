from logger import logger
from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.rest import TuqRestHelper

class TuqTest(PerfTest):
    BUCKET = 'bucket-1'

    def __init__(self,  cluster_spec, test_config):
        super(TuqTest, self).__init__(cluster_spec, test_config)
        self.tuq = self.test_config.get_tuq_settings()
        self.rest = TuqRestHelper(cluster_spec, self.tuq)

    @with_stats(tuq_latency=True)
    def access(self):
        super(TuqTest, self).timer()

    def create_tuq_index(self, tuq):
        for index in tuq.indexes:
            self.rest.create_index('%s_idx' % index, index, self.BUCKET)

    def load_neg_coins(self):
        logger.info('loading 100 negative coins items')
        load_settings = self.test_config.get_load_settings()
        load_settings.items = int(load_settings.ops)
        load_settings.ops = float(100)
        self.worker_manager.run_workload(load_settings, self.target_iterator, neg_coins=True)

    def run(self):
        self.load()
        self.wait_for_persistence()
        if 'where_lt_neg' in self.tuq.indexes['coins']:    # TODO: hard-coded 'coins'
            self.load_neg_coins()
            self.wait_for_persistence()
        self.compact_bucket()
        self.create_tuq_index(self.tuq)

        self.workload = self.test_config.get_access_settings()
        if self.tuq.kv_bg:
            self.hot_load()
            self.wait_for_persistence()
            self.compact_bucket()
            self.access_bg()
        self.access()
        self.shutdown_event.set()
        for operation in ('tuq', 'query'):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_tuq_latency(operation=operation,
                    percentile=0.9)
            )
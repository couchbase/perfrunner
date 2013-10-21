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

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.create_tuq_index(self.tuq)

        self.workload = self.test_config.get_access_settings()
        self.access()
        if self.tuq.kv_bg:
            self.access_bg()
        self.shutdown_event.set()
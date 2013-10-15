from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.rest import TuqRestHelper

class TuqTest(PerfTest):
    BUCKET = 'bucket-1'

    def __init__(self,  cluster_spec, test_config):
        super(TuqTest, self).__init__(cluster_spec, test_config)
        self.rest = TuqRestHelper(cluster_spec)

    @with_stats(query_latency=True)
    def access(self):
        super(TuqTest, self).timer()

    def create_tuq_index(self, tuq):
        for index in tuq.indexes:
            self.rest.create_index(tuq.server_addr, '%s_idx' % index,
                                   index, self.BUCKET)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.tuq = self.test_config.get_tuq_settings()
        self.create_tuq_index(self.tuq)

        self.workload = self.test_config.get_access_settings()
        self.access()
        self.shutdown_event.set()
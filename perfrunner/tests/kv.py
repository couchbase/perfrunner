from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats


class KVTest(PerfTest):

    @with_stats(latency=True)
    def run_access_phase(self):
        super(KVTest, self).run_access_phase()

    def run(self):
        self.run_load_phase()
        self.wait_for_persistence()
        self.compact_bucket()
        self.run_access_phase()

from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats


class KVTest(PerfTest):

    @with_stats(latency=True)
    def access(self):
        super(KVTest, self).access()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.access()

from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats


class KVTest(PerfTest):

    @with_stats(latency=True)
    def _run_access_phase(self):
        super(KVTest, self)._run_access_phase()

    def run(self):
        self._run_load_phase()
        self._compact_bucket()
        self._run_access_phase()

        self._debug()

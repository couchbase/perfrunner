from perfrunner.tests import PerfTest, with_stats


class KVTest(PerfTest):

    @with_stats
    def run(self):
        self._run_load_phase()
        self._compact_bucket()
        self._run_access_phase()

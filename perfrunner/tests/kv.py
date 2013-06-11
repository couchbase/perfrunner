from perfrunner.tests import PerfTest


class KVTest(PerfTest):

    def run(self):
        self._run_load_phase()

from spring.wgen import WorkloadGen

from perfrunner.tests import TargetIterator


class KVTest(TargetIterator):

    def _run_load_phase(self):
        workload_settings = self.test_config.get_load_settings()
        if workload_settings.ops:
            for target_settings in TargetIterator(self.cluster_spec,
                                                  self.test_config):
                wg = WorkloadGen(workload_settings, target_settings)
                wg.run()

    def run(self):
        self._run_load_phase()

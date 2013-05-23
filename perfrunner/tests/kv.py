from spring.wgen import WorkloadGen

from perfrunner.tests import PerfTest, TargetIterator


class KVTest(PerfTest):

    def _run_load_phase(self):
        load_settings = self.test_config.get_load_settings()
        if load_settings.ops:
            for target_settings in TargetIterator(self.cluster_spec,
                                                  self.test_config):
                wg = WorkloadGen(load_settings, target_settings)
                wg.run()
                self.monitor.monitor_disk_queue(target_settings)
                self.monitor.monitor_tap_replication(target_settings)

    def run(self):
        self._run_load_phase()

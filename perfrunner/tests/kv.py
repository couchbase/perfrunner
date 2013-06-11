from spring.wgen import WorkloadGen

from perfrunner.tests import PerfTest


class KVTest(PerfTest):

    def _run_load_phase(self):
        load_settings = self.test_config.get_load_settings()
        if load_settings.ops:
            for target in self.target_iterator:
                wg = WorkloadGen(load_settings, target)
                wg.run()
                self.monitor.monitor_disk_queue(target)
                self.monitor.monitor_tap_replication(target)

    def run(self):
        self._run_load_phase()

from spring.wgen import WorkloadGen

from perfrunner.helpers.monitor import Monitor
from perfrunner.tests import TargetIterator


class KVTest(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.monitor = Monitor(cluster_spec, test_config)

    def _run_load_phase(self):
        workload_settings = self.test_config.get_load_settings()
        if workload_settings.ops:
            for target_settings in TargetIterator(self.cluster_spec,
                                                  self.test_config):
                wg = WorkloadGen(workload_settings, target_settings)
                wg.run()
                self.monitor.monitor_disk_queue(target_settings)
                self.monitor.monitor_tap_replication(target_settings)

    def run(self):
        self._run_load_phase()

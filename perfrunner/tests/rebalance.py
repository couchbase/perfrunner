import time

from perfrunner.tests import PerfTest

from multiprocessing import Event


def with_delay(method):
    def wrapper(self, *args, **kwargs):
        time.sleep(self.rebalance_settings.start_after)
        method(self, *args, **kwargs)
        time.sleep(self.rebalance_settings.stop_after)
        self.shutdown_event.set()
    return wrapper


class RebalanceTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(RebalanceTest, self).__init__(*args, **kwargs)
        self.shutdown_event = Event()
        self.rebalance_settings = self.test_config.get_rebalance_settings()

    @with_delay
    def rebalance_in(self):
        for cluster in self.cluster_spec.get_clusters():
            master = cluster[0]
            known_nodes = cluster[:self.rebalance_settings.nodes_after]
            ejected_nodes = []
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)


class StaticRebalanceTest(RebalanceTest):

    def _run(self):
        self._run_load_phase()
        self._compact_bucket()

        self.reporter.start()
        self.rebalance_in()
        value = self.reporter.finish('Rebalance')
        self.reporter.post_to_sf(self, value)

        self._debug()

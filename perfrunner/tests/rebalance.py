import time
from multiprocessing import Event

from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


def with_delay(rebalance):
    def wrapper(self, *args, **kwargs):
        time.sleep(self.rebalance_settings.start_after)

        self.reporter.reset_utilzation_stats()

        rebalance(self, *args, **kwargs)

        self.reporter.save_utilzation_stats()
        self.reporter.save_master_events()

        time.sleep(self.rebalance_settings.stop_after)

        self.shutdown_event.set()
    return wrapper


class RebalanceTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(RebalanceTest, self).__init__(*args, **kwargs)
        self.shutdown_event = Event()
        self.rebalance_settings = self.test_config.get_rebalance_settings()

    @with_delay
    def rebalance(self):
        initial_nodes = self.test_config.get_initial_nodes()
        nodes_after = self.rebalance_settings.nodes_after
        for cluster in self.cluster_spec.get_clusters():
            master = cluster[0]
            if nodes_after > initial_nodes:
                for host_port in cluster[initial_nodes:nodes_after]:
                    host = host_port.split(':')[0]
                    self.rest.add_node(master, host)
                known_nodes = cluster[:nodes_after]
                ejected_nodes = []
            else:
                known_nodes = cluster[:initial_nodes]
                ejected_nodes = cluster[nodes_after:initial_nodes]
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)


class StaticRebalanceTest(RebalanceTest):

    def run(self):
        self._run_load_phase()
        self._compact_bucket()

        self.reporter.start()
        self.rebalance()
        value = self.reporter.finish('Rebalance')
        self.reporter.post_to_sf(self, value)

        self._debug()


class StaticRebalanceWithIndexTest(IndexTest, RebalanceTest):

    def run(self):
        self._run_load_phase()
        self._compact_bucket()

        self._define_ddocs()
        self._build_index()

        self.reporter.start()
        self.rebalance_in()
        value = self.reporter.finish('Rebalance')
        self.reporter.post_to_sf(self, value)

        self._debug()

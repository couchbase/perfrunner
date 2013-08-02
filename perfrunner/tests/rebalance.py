import time

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest


def with_delay(rebalance):
    def wrapper(self, *args, **kwargs):
        time.sleep(self.rebalance_settings.start_after)

        rebalance(self, *args, **kwargs)

        time.sleep(self.rebalance_settings.stop_after)
    return wrapper


def with_reporter(rebalance):
    def wrapper(self, *args, **kwargs):
        self.reporter.reset_utilzation_stats()

        self.reporter.start()

        rebalance(self, *args, **kwargs)

        value = self.reporter.finish('Rebalance')
        self.reporter.post_to_sf(value)

        self.reporter.save_utilzation_stats()
        self.reporter.save_master_events()
    return wrapper


class RebalanceTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(RebalanceTest, self).__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.get_rebalance_settings()

    @with_stats()
    @with_delay
    @with_reporter
    def rebalance(self):
        initial_nodes = self.test_config.get_initial_nodes()
        nodes_after = self.rebalance_settings.nodes_after
        for servers in self.cluster_spec.get_clusters().values():
            master = servers[0]
            if nodes_after > initial_nodes:
                for host_port in servers[initial_nodes:nodes_after]:
                    host = host_port.split(':')[0]
                    self.rest.add_node(master, host)
                known_nodes = servers[:nodes_after]
                ejected_nodes = []
            else:
                known_nodes = servers[:initial_nodes]
                ejected_nodes = servers[nodes_after:initial_nodes]
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.monitor.monitor_rebalance(master)
            if not self.rest.is_balanced(master):
                logger.interrupt('Rebalance failed')


class StaticRebalanceTest(RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.rebalance()


class StaticRebalanceWithIndexTest(IndexTest, RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        self.build_index()

        self.rebalance()


class DynamicRebalanceTest(RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.access_bg()
        self.rebalance()
        self.shutdown_event.set()

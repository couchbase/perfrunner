import time

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest
from perfrunner.tests.query import QueryTest
from perfrunner.tests.xdcr import XdcrTest, SymmetricXdcrTest


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
        self.servers = self.cluster_spec.get_masters().values()[-1]

    @with_stats()
    @with_delay
    @with_reporter
    def rebalance(self):
        initial_nodes = self.test_config.get_initial_nodes()
        nodes_after = self.rebalance_settings.nodes_after
        master = self.servers[0]

        if nodes_after > initial_nodes:
            for host_port in self.servers[initial_nodes:nodes_after]:
                host = host_port.split(':')[0]
                self.rest.add_node(master, host)
            known_nodes = self.servers[:nodes_after]
            ejected_nodes = []
        else:
            known_nodes = self.servers[:initial_nodes]
            ejected_nodes = self.servers[nodes_after:initial_nodes]
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


class RebalanceTest(RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.access_bg()
        self.rebalance()
        self.shutdown_event.set()


class RebalanceWithQueriesTest(QueryTest, RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        self.build_index()

        self.access_bg()
        self.rebalance()
        self.shutdown_event.set()


class RebalanceWithXdcrTest(XdcrTest, RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_xdcr()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()

        self.compact_bucket()

        bg_process = self.access_bg()
        self.rebalance()
        bg_process.terminate()


class RebalanceWithSymmetricXdcrTestTest(SymmetricXdcrTest, RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_xdcr()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()

        self.compact_bucket()

        bg_process = self.access_bg()
        self.rebalance()
        bg_process.terminate()

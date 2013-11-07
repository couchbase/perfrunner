import time
from decorator import decorator

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.index import IndexTest
from perfrunner.tests.query import QueryTest
from perfrunner.tests.xdcr import XdcrTest, SymmetricXdcrTest


@decorator
def with_delay(rebalance, *args, **kwargs):
    test = args[0]

    time.sleep(test.rebalance_settings.start_after)

    rebalance(*args, **kwargs)

    time.sleep(test.rebalance_settings.stop_after)


@decorator
def with_reporter(rebalance, *args, **kwargs):
    test = args[0]

    test.reporter.reset_utilzation_stats()

    test.reporter.start()

    rebalance(*args, **kwargs)

    test.rebalance_time = test.reporter.finish('Rebalance')

    test.reporter.save_utilzation_stats()
    test.reporter.save_master_events()


@decorator
def with_delayed_posting(rebalance, *args, **kwargs):
    test = args[0]

    rebalance(*args, **kwargs)

    test.reporter.post_to_sf(test.rebalance_time)


class RebalanceTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(RebalanceTest, self).__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.get_rebalance_settings()
        self.servers = self.cluster_spec.get_clusters().values()[-1]
        self.shutdown_event = None
        self.apply_rebalance_settings()

    def apply_rebalance_settings(self):
        if not self.rebalance_settings:
            return

        master = self.servers[0]
        if self.rebalance_settings.reb_moves_per_node:
            self.rest.change_rebalance_moves_per_node(master,
                self.rebalance_settings.reb_moves_per_node)
        if self.rebalance_settings.reb_moves_before_compaction:
            self.rest.change_reb_moves_before_compaction(master,
                self.rebalance_settings.reb_moves_before_compaction)

    @with_delayed_posting
    @with_stats(active_tasks=True)
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
        elif nodes_after == initial_nodes:
            swap_count = self.rebalance_settings.swap_count
            if swap_count > initial_nodes:
                logger.interrupt("invalid swap_count - %d" % swap_count)
            for host_port in self.servers[initial_nodes:(initial_nodes+swap_count)]:
                host = host_port.split(':')[0]
                self.rest.add_node(master, host)
            known_nodes = self.servers[:(initial_nodes+swap_count)]
            ejected_nodes = self.servers[(initial_nodes-swap_count):initial_nodes]
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


class RebalanceKVTest(RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.access_bg()
        self.rebalance()


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


class RebalanceWithSymmetricXdcrTest(SymmetricXdcrTest, RebalanceTest):

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

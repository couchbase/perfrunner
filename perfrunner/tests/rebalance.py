import time

from decorator import decorator
from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import server_group
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

    if test.rest.is_balanced(test.servers[0]):
        test.reporter.post_to_sf(test.rebalance_time)
    else:
        logger.error('Rebalance failed')


class RebalanceTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(RebalanceTest, self).__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.get_rebalance_settings()
        self.servers = self.cluster_spec.get_clusters().values()[-1]

    @with_delayed_posting
    @with_stats(latency=True)
    @with_delay
    @with_reporter
    def rebalance(self):
        initial_nodes = self.test_config.get_initial_nodes()
        nodes_after = self.rebalance_settings.nodes_after
        master = self.servers[0]

        group_number = self.test_config.get_group_number() or 1
        groups = group_number > 1 and self.rest.get_server_groups(master) or {}

        if nodes_after > initial_nodes:
            for i, host_port in enumerate(self.servers[initial_nodes:nodes_after],
                                          start=initial_nodes):
                host = host_port.split(':')[0]
                uri = groups.get(server_group(self.servers[:nodes_after],
                                              group_number, i))
                self.rest.add_node(master, host, uri)
            known_nodes = self.servers[:nodes_after]
            ejected_nodes = []
        else:
            known_nodes = self.servers[:initial_nodes]
            ejected_nodes = self.servers[nodes_after:initial_nodes]
        self.rest.rebalance(master, known_nodes, ejected_nodes)
        self.monitor.monitor_rebalance(master)


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

        self.workload = self.test_config.get_access_settings()
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

        self.workload = self.test_config.get_access_settings()
        self.access_bg()
        self.rebalance()
        self.shutdown_event.set()

        if self.rest.is_balanced(self.servers[0]):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_views_disk_size()
            )


class RebalanceWithXdcrTest(XdcrTest, RebalanceTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_xdcr()
        self.wait_for_persistence()

        self.hot_load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.workload = self.test_config.get_access_settings()
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

        self.workload = self.test_config.get_access_settings()
        bg_process = self.access_bg()
        self.rebalance()
        bg_process.terminate()

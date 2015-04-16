import time

from decorator import decorator


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
    test.worker_manager.terminate()


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

    if test.is_balanced():
        test.reporter.post_to_sf(test.rebalance_time)
        if hasattr(test, 'experiment'):
            test.experiment.post_results(test.rebalance_time)


class RebalanceTest(PerfTest):

    """
    This class implements methods required for rebalance management. See child
    classes for workflow details.

    Here is rebalance phase timeline:

        start stats collection ->
            sleep X minutes (observe pre-rebalance characteristics) ->
                trigger rebalance stopwatch ->
                    start rebalance -> wait for rebalance to finish ->
                trigger rebalance stopwatch ->
            sleep X minutes (observe post-rebalance characteristics) ->
        stop stats collection.

    The timeline is implement via a long chain of decorators.

    Actual rebalance step depends on test scenario (e.g., basic rebalance or
    rebalance after graceful failover, and etc.).
    """

    ALL_HOSTNAMES = True

    def __init__(self, *args, **kwargs):
        super(RebalanceTest, self).__init__(*args, **kwargs)
        self.rebalance_settings = self.test_config.rebalance_settings

    def is_balanced(self):
        for master in self.cluster_spec.yield_masters():
            if not self.rest.is_balanced(master):
                return False
        return True

    def change_watermarks(self, host_port):
        host = host_port.split(':')[0]
        memcached_port = self.rest.get_memcached_port(host_port)
        watermark_settings = self.test_config.watermark_settings
        mem_quota = self.test_config.cluster.mem_quota
        for bucket in self.test_config.buckets:
            for key, val in watermark_settings.items():
                val = self.memcached.calc_watermark(val, mem_quota)
                self.memcached.set_flusher_param(host, memcached_port,
                                                 bucket, key, val)

    @with_delayed_posting
    @with_stats
    @with_delay
    @with_reporter
    def rebalance(self):
        clusters = self.cluster_spec.yield_clusters()
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after = self.rebalance_settings.nodes_after
        swap = self.rebalance_settings.swap
        failover = self.rebalance_settings.failover
        graceful_failover = self.rebalance_settings.graceful_failover
        delta_recovery = self.rebalance_settings.delta_recovery
        sleep_after_failover = self.rebalance_settings.sleep_after_failover
        group_number = self.test_config.cluster.group_number or 1

        for (_, servers), initial_nodes, nodes_after in zip(clusters,
                                                            initial_nodes,
                                                            nodes_after):
            master = servers[0]
            groups = group_number > 1 and self.rest.get_server_groups(master) or {}

            new_nodes = []
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []
            failover_nodes = []
            graceful_failover_nodes = []
            if nodes_after > initial_nodes:  # rebalance-in
                new_nodes = enumerate(
                    servers[initial_nodes:nodes_after],
                    start=initial_nodes
                )
                known_nodes = servers[:nodes_after]
            elif nodes_after < initial_nodes:  # rebalance-out
                ejected_nodes = servers[nodes_after:initial_nodes]
            elif swap:
                new_nodes = enumerate(
                    servers[initial_nodes:initial_nodes + swap],
                    start=initial_nodes - swap
                )
                known_nodes = servers[:initial_nodes + swap]
                ejected_nodes = servers[initial_nodes - swap:initial_nodes]
            elif failover:
                failover_nodes = servers[initial_nodes - failover:initial_nodes]
            elif graceful_failover:
                graceful_failover_nodes = \
                    servers[initial_nodes - graceful_failover:initial_nodes]
            else:
                continue

            for i, host_port in new_nodes:
                group = server_group(servers[:nodes_after], group_number, i)
                uri = groups.get(group)
                self.rest.add_node(master, host_port, uri=uri)
            for host_port in failover_nodes:
                self.rest.fail_over(master, host_port)
                self.rest.add_back(master, host_port)
                if delta_recovery:
                    self.rest.set_delta_recovery_type(master, host_port)
            for host_port in graceful_failover_nodes:
                self.rest.graceful_fail_over(master, host_port)
                self.monitor.monitor_rebalance(master)
                self.rest.add_back(master, host_port)

            if graceful_failover:
                self.reporter.post_to_sf(
                    *self.metric_helper.failover_time(self.reporter)
                )
                self.reporter.start()

            if failover:
                time.sleep(sleep_after_failover)
                self.reporter.start()

            self.rest.rebalance(master, known_nodes, ejected_nodes)

            for _, host_port in new_nodes:
                self.change_watermarks(host_port)

            self.monitor.monitor_rebalance(master)


class StaticRebalanceTest(RebalanceTest):

    """
    KV rebalance test with no ongoing workload. Obsolete.
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.rebalance()


class StaticRebalanceWithIndexTest(IndexTest, RebalanceTest):

    """
    KV + Index rebalance test with no ongoing workload. Obsolete.
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        self.build_index()

        self.rebalance()


class RebalanceKVTest(RebalanceTest):

    """
    Workflow definition for KV rebalance tests.
    """

    COLLECTORS = {'latency': True}

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()


class RebalanceWithQueriesTest(QueryTest, RebalanceTest):

    """
    Workflow definition for KV + Index rebalance tests.
    """

    COLLECTORS = {'latency': True, 'query_latency': True}

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.define_ddocs()
        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()


class RebalanceWithXdcrTest(XdcrTest, RebalanceTest):

    """
    Workflow definition for KV + bidir XDCR rebalance tests.
    """

    COLLECTORS = {'latency': True, 'xdcr_lag': True}

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.enable_xdcr()
        self.monitor_replication()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()


class RebalanceWithSymmetricXdcrTest(SymmetricXdcrTest, RebalanceTest):

    """
    Workflow definition for KV + unidir XDCR rebalance tests.
    """

    COLLECTORS = {'latency': True, 'xdcr_lag': True}

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.enable_xdcr()
        self.monitor_replication()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()

import multiprocessing
import time

import dateutil.parser
from decorator import decorator
from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import log_phase, server_group
from perfrunner.tests import PerfTest
from perfrunner.tests.query import QueryTest
from perfrunner.tests.xdcr import (
    DestTargetIterator,
    UniDirXdcrTest,
    XdcrInitTest,
    XdcrTest,
)


@decorator
def with_delay(rebalance, *args, **kwargs):
    test = args[0]

    logger.info('Sleeping for {} seconds before taking actions'
                .format(test.rebalance_settings.start_after))
    time.sleep(test.rebalance_settings.start_after)

    rebalance(*args, **kwargs)

    logger.info('Sleeping for {} seconds before finishing'
                .format(test.rebalance_settings.stop_after))
    time.sleep(test.rebalance_settings.stop_after)
    test.worker_manager.terminate()


@decorator
def with_reporter(rebalance, *args, **kwargs):
    test = args[0]

    test.reporter.start()

    rebalance(*args, **kwargs)

    test.rebalance_time = test.reporter.finish('Rebalance')

    test.reporter.save_master_events()


@decorator
def with_delayed_posting(rebalance, *args, **kwargs):
    test = args[0]

    rebalance(*args, **kwargs)

    if test.is_balanced():
        test.reporter.post_to_sf(test.rebalance_time)


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

    The timeline is implemented via a long chain of decorators.

    Actual rebalance steps depend on test scenario (e.g., basic rebalance or
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

    @with_delayed_posting
    @with_stats
    @with_delay
    @with_reporter
    def rebalance(self):
        clusters = self.cluster_spec.yield_clusters()
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after = self.rebalance_settings.nodes_after
        swap = self.rebalance_settings.swap

        group_number = self.test_config.cluster.group_number or 1

        for (_, servers), initial_nodes, nodes_after in zip(clusters,
                                                            initial_nodes,
                                                            nodes_after):
            master = servers[0]
            groups = group_number > 1 and self.rest.get_server_groups(master) or {}

            new_nodes = []
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []

            if nodes_after > initial_nodes:  # rebalance-in
                new_nodes = enumerate(
                    servers[initial_nodes:nodes_after],
                    start=initial_nodes
                )
                known_nodes = servers[:nodes_after]
            elif nodes_after < initial_nodes:  # rebalance-out
                ejected_nodes = servers[nodes_after:initial_nodes]
            else:  # swap
                new_nodes = enumerate(
                    servers[initial_nodes:initial_nodes + swap],
                    start=initial_nodes - swap
                )
                known_nodes = servers[:initial_nodes + swap]
                ejected_nodes = servers[initial_nodes - swap:initial_nodes]

            for i, host_port in new_nodes:
                group = server_group(servers[:nodes_after], group_number, i)
                uri = groups.get(group)
                self.rest.add_node(master, host_port, uri=uri)

            self.rest.rebalance(master, known_nodes, ejected_nodes)

            self.monitor.monitor_rebalance(master)


class RebalanceKVTest(RebalanceTest):

    """
    Workflow definition for KV rebalance tests.
    """

    COLLECTORS = {'latency': True}

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()


class RecoveryTest(RebalanceKVTest):

    @with_delayed_posting
    @with_stats
    @with_delay
    @with_reporter
    def rebalance(self):
        clusters = self.cluster_spec.yield_clusters()
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            master = servers[0]

            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            for host_port in failed:
                if self.rebalance_settings.failover == 'hard':
                    self.rest.fail_over(master, host_port)
                else:
                    self.rest.graceful_fail_over(master, host_port)
                    self.monitor.monitor_rebalance(master)
                self.rest.add_back(master, host_port)

            if self.rebalance_settings.delta_recovery:
                for host_port in failed:
                    self.rest.set_delta_recovery_type(master, host_port)

            logger.info('Sleeping for {} seconds after failover'
                        .format(self.rebalance_settings.sleep_after_failover))
            time.sleep(self.rebalance_settings.sleep_after_failover)

            self.reporter.start()
            self.rest.rebalance(master, known_nodes=servers[:initial_nodes],
                                ejected_nodes=[])
            self.monitor.monitor_rebalance(master)


class FailoverTest(RebalanceKVTest):

    EXTRA_TIMEOUT = 30

    def check_auto_failover(self, master):
        timeout = self.test_config.cluster.auto_failover_timeout

        time.sleep(timeout + self.EXTRA_TIMEOUT)
        time_str = self.remote.detect_auto_failover(master.split(':')[0])
        if time_str is not None:
            t_failover = dateutil.parser.parse(time_str)
            t_failover = time.mktime(t_failover.timetuple())
            return t_failover

    def _report_kpi(self, delta):
        self.reporter.post_to_sf(delta)

    @with_delay
    def rebalance(self):
        clusters = self.cluster_spec.yield_clusters()
        initial_nodes = self.test_config.cluster.initial_nodes
        failed_nodes = self.rebalance_settings.failed_nodes

        for (_, servers), initial_nodes in zip(clusters,
                                               initial_nodes):
            master = servers[0].split(':')[0]
            failed = servers[initial_nodes - failed_nodes:initial_nodes]

            t_failure = time.time()
            for host_port in failed:
                self.remote.shutdown(host_port.split(':')[0])

            t_failover = self.check_auto_failover(master)
            if t_failover:
                delta = round(t_failover - t_failure, 1)
                self.report_kpi(delta)


class RebalanceWithQueriesTest(QueryTest, RebalanceTest):

    """
    Workflow definition for KV + Index rebalance tests.
    """

    COLLECTORS = {'latency': True, 'query_latency': True}

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.define_ddocs()
        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()


class RebalanceWithXDCRTest(XdcrTest, RebalanceTest):

    """
    Workflow definition for KV + bidir XDCR rebalance tests.
    """

    COLLECTORS = {'latency': True, 'xdcr_lag': True, 'xdcr_stats': True}

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.enable_xdcr()
        self.monitor_replication()
        self.wait_for_persistence()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()


class RebalanceWithUniDirXdcrTest(UniDirXdcrTest, RebalanceTest):

    """
    Workflow definition for KV + unidir XDCR rebalance tests.
    """

    COLLECTORS = {'latency': True, 'xdcr_lag': True, 'xdcr_stats': True}

    def run(self):
        if self.test_config.restore_settings.snapshot and self.build > '4':
            self.restore()
        else:
            self.load()
            self.wait_for_persistence()

        self.enable_xdcr()
        self.monitor_replication()
        self.wait_for_persistence()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.rebalance()


class RebalanceWithXdcrTest(XdcrInitTest, RebalanceTest):

    """
    Workflow definition unidir XDCR rebalance tests.
    """

    @with_stats
    def rebalance(self):
        clusters = self.cluster_spec.yield_clusters()
        initial_nodes = self.test_config.cluster.initial_nodes
        nodes_after = self.rebalance_settings.nodes_after
        group_number = self.test_config.cluster.group_number or 1
        self.master = None
        for (_, servers), initial_nodes, nodes_after in zip(clusters,
                                                            initial_nodes,
                                                            nodes_after):
            master = servers[0]
            groups = group_number > 1 and self.rest.get_server_groups(master) or {}

            new_nodes = []
            known_nodes = servers[:initial_nodes]
            ejected_nodes = []
            if nodes_after > initial_nodes:  # rebalance-in
                new_nodes = enumerate(
                    servers[initial_nodes:nodes_after],
                    start=initial_nodes
                )
                known_nodes = servers[:nodes_after]
            elif nodes_after < initial_nodes:  # rebalance-out
                ejected_nodes = servers[nodes_after:initial_nodes]
            else:
                continue

            for i, host_port in new_nodes:
                group = server_group(servers[:nodes_after], group_number, i)
                uri = groups.get(group)
                self.rest.add_node(master, host_port, uri=uri)
            self.rest.rebalance(master, known_nodes, ejected_nodes)
            self.master = master

        self.enable_xdcr()
        start = time.time()
        if self.master:
            p = multiprocessing.Process(target=self.monitor.monitor_rebalance, args=(self.master,))
            p.start()
        self.monitor_replication()
        self.time_elapsed = int(time.time() - start)

    def load_dest(self):
        load_settings = self.test_config.load_settings
        log_phase('load phase', load_settings)

        dest_target_iterator = DestTargetIterator(self.cluster_spec,
                                                  self.test_config)
        self.worker_manager.run_workload(load_settings, dest_target_iterator)
        self.worker_manager.wait_for_workers()

    def run(self):
        self.load()
        if self.test_config.cluster.initial_nodes[1] != self.rebalance_settings.nodes_after[1]:
            self.load_dest()

        self.wait_for_persistence()

        self.rebalance()

        if self.test_config.stats_settings.enabled:
            rate = self.metric_helper.calc_avg_replication_rate(self.time_elapsed)
            self.reporter.post_to_sf(value=rate)

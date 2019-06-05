import time
from collections import OrderedDict
from copy import copy
from multiprocessing import Process
from typing import Callable, Union

import requests
from decorator import decorator

from cbagent.collectors import (
    IO,
    PS,
    ActiveTasks,
    Disk,
    DurabilityLatency,
    ElasticStats,
    EventingStats,
    FTSCollector,
    FTSLatencyCollector,
    KVLatency,
    Memory,
    N1QLStats,
    Net,
    NSServer,
    NSServerOverview,
    NSServerSystem,
    ObserveIndexLatency,
    ObserveSecondaryIndexLatency,
    PageCache,
    QueryLatency,
    SecondaryDebugStats,
    SecondaryDebugStatsBucket,
    SecondaryDebugStatsIndex,
    SecondaryLatencyStats,
    SecondaryStats,
    SecondaryStorageStats,
    SecondaryStorageStatsMM,
    SGImportLatency,
    SyncGatewayStats,
    Sysdig,
    TypePerf,
    XdcrLag,
    XdcrStats,
)
from cbagent.metadata_client import MetadataClient
from cbagent.stores import PerfStore
from logger import logger
from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import StatsSettings
from perfrunner.tests import PerfTest


@decorator
def timeit(method: Callable, *args, **kwargs) -> float:
    t0 = time.time()
    method(*args, **kwargs)
    return time.time() - t0  # Elapsed time in seconds


@decorator
def with_stats(method: Callable, *args, **kwargs) -> Union[float, None]:
    with CbAgent(test=args[0], phase=method.__name__):
        return method(*args, **kwargs)


def new_cbagent_settings(test: PerfTest):
    if not test.test_config.stats_settings.enabled:
        return None

    if hasattr(test, 'ALL_BUCKETS'):
        buckets = None
    else:
        buckets = test.test_config.buckets[:1]
    if hasattr(test, 'ALL_HOSTNAMES'):
        hostnames = test.cluster_spec.servers
    else:
        hostnames = None

    settings = type('settings', (object,), {
        'cbmonitor_host': StatsSettings.CBMONITOR,
        'interval': test.test_config.stats_settings.interval,
        'lat_interval': test.test_config.stats_settings.lat_interval,
        'secondary_statsfile': test.test_config.stats_settings.secondary_statsfile,
        'buckets': buckets,
        'indexes': test.test_config.gsi_settings.indexes,
        'hostnames': hostnames,
        'client_processes': test.test_config.stats_settings.client_processes,
        'server_processes': test.test_config.stats_settings.server_processes,
        'traced_processes': test.test_config.stats_settings.traced_processes,
        'bucket_password': test.test_config.bucket.password,
        'workers': test.cluster_spec.workers,
    })()

    settings.ssh_username, settings.ssh_password = \
        test.cluster_spec.ssh_credentials
    settings.rest_username, settings.rest_password = \
        test.cluster_spec.rest_credentials

    if test.cluster_spec.servers_by_role('index'):
        settings.index_node = test.cluster_spec.servers_by_role('index')[0]

    return settings


class CbAgent:

    def __init__(self, test: PerfTest, phase: str):
        self.test = test
        self.settings = new_cbagent_settings(test=test)

        if self.test.test_config.stats_settings.enabled:
            self.init_clusters(phase=phase)
            self.add_collectors(**test.COLLECTORS)
            self.update_metadata()
            self.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test.test_config.stats_settings.enabled:
            self.stop()
            self.reconstruct()
            self.find_time_series()
            self.add_snapshots()

    def init_clusters(self, phase: str):
        self.cluster_map = OrderedDict()

        for cluster_name, servers in self.test.cluster_spec.clusters:
            cluster_id = '{}_{}_{}_{}'.format(cluster_name,
                                              self.test.build.replace('.', ''),
                                              phase,
                                              uhex()[:4])
            self.cluster_map[cluster_id] = servers[0]
        self.test.cbmonitor_clusters = list(self.cluster_map.keys())

    def add_collectors(self,
                       disk=False,
                       durability=False,
                       elastic_stats=False,
                       eventing_stats=False,
                       fts_latency=False,
                       fts_stats=False,
                       index_latency=False,
                       iostat=True,
                       latency=False,
                       memory=True,
                       n1ql_latency=False,
                       n1ql_stats=False,
                       net=True,
                       ns_server_system=False,
                       page_cache=False,
                       query_latency=False,
                       secondary_debugstats_bucket=False,
                       secondary_debugstats=False,
                       secondary_debugstats_index=False,
                       secondary_index_latency=False,
                       secondary_latency=False,
                       secondary_stats=False,
                       secondary_storage_stats=False,
                       secondary_storage_stats_mm=False,
                       xdcr_lag=False,
                       xdcr_stats=False,
                       ns_server=True,
                       ns_server_overview=True,
                       active_tasks=True,
                       syncgateway_stats=False,
                       sgimport_latency=False):

        self.collectors = []
        self.processes = []

        if ns_server:
            self.add_collector(NSServer)
        if ns_server_overview:
            self.add_collector(NSServerOverview)
        if active_tasks:
            self.add_collector(ActiveTasks)

        if self.test.remote.os != 'Cygwin':
            self.add_collector(PS)
            self.add_collector(Sysdig)
            if memory:
                self.add_collector(Memory)
            if net:
                self.add_collector(Net)
            if disk:
                self.add_io_collector(Disk)
            if iostat:
                self.add_io_collector(IO)
            if page_cache:
                self.add_io_collector(PageCache)
        else:
            self.add_collector(TypePerf)

        if latency:
            self.add_collector(KVLatency)
        if durability:
            self.add_durability_collector()

        if query_latency or n1ql_latency:
            self.add_collector(QueryLatency)
        if n1ql_stats:
            self.add_collector(N1QLStats)

        if index_latency:
            self.add_collector(ObserveIndexLatency)

        if eventing_stats:
            self.add_collector(EventingStats, self.test)

        if ns_server_system:
            self.add_collector(NSServerSystem)

        if elastic_stats:
            self.add_collector(ElasticStats, self.test)
        if fts_latency:
            self.add_collector(FTSLatencyCollector, self.test)
        if fts_stats:
            self.add_collector(FTSCollector, self.test)

        if syncgateway_stats:
            self.add_collector(SyncGatewayStats, self.test)

        if secondary_debugstats:
            self.add_collector(SecondaryDebugStats)
        if secondary_debugstats_bucket:
            self.add_collector(SecondaryDebugStatsBucket)
        if secondary_debugstats_index:
            self.add_collector(SecondaryDebugStatsIndex)
        if secondary_index_latency:
            self.add_collector(ObserveSecondaryIndexLatency)
        if secondary_latency:
            self.add_collector(SecondaryLatencyStats)
        if secondary_stats:
            self.add_collector(SecondaryStats)
        if secondary_storage_stats:
            self.add_collector(SecondaryStorageStats)
        if secondary_storage_stats_mm:
            self.add_collector(SecondaryStorageStatsMM)

        if xdcr_lag:
            self.add_xdcr_lag()
        if xdcr_stats:
            self.add_collector(XdcrStats)

        if sgimport_latency:
            self.add_sgimport_latency()

    def add_collector(self, cls, *args):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node

            collector = cls(settings, *args)
            self.collectors.append(collector)

    def add_io_collector(self, cls):
        data_path, index_path = self.test.cluster_spec.paths
        partitions = {
            'client': {},
            'server': {'data': data_path},
        }
        if self.test.test_config.test_case.component == 'views':
            partitions['server']['index'] = index_path
        elif self.test.test_config.test_case.component == 'tools':
            partitions['client']['tools'] = self.test.cluster_spec.backup

        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node
            settings.partitions = partitions

            collector = cls(settings)
            self.collectors.append(collector)

    def add_durability_collector(self):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node

            collector = DurabilityLatency(settings,
                                          self.test.test_config.access_settings)
            self.collectors.append(collector)

    def add_xdcr_lag(self):
        reversed_clusters = list(reversed(self.test.cbmonitor_clusters))

        for i, cluster_id in enumerate(self.cluster_map):
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = self.cluster_map[cluster_id]
            dest_cluster = reversed_clusters[i]
            settings.dest_master_node = self.cluster_map[dest_cluster]

            collector = XdcrLag(settings, self.test.test_config.access_settings)
            self.collectors.append(collector)

            if self.test.test_config.xdcr_settings.replication_type == 'unidir':
                break

    def add_sgimport_latency(self):
        # reversed_clusters = list(reversed(self.test.cbmonitor_clusters))

        for i, cluster_id in enumerate(self.cluster_map):
            settings = copy(self.settings)

            settings.cluster = cluster_id
            collector = SGImportLatency(settings, self.test.cluster_spec,
                                        self.test.test_config.syncgateway_settings)
            self.collectors.append(collector)

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        logger.info('Starting stats collectors')
        self.processes = [Process(target=c.collect) for c in self.collectors]
        for p in self.processes:
            p.start()

    def stop(self):
        logger.info('Terminating stats collectors')
        for p in self.processes:
            p.terminate()

    def reconstruct(self):
        logger.info('Reconstructing measurements')
        for collector in self.collectors:
            if hasattr(collector, 'reconstruct'):
                collector.reconstruct()

    def trigger_report(self, snapshot: str):
        url = 'http://{}/reports/html/?snapshot={}'.format(
            self.settings.cbmonitor_host, snapshot)
        logger.info('HTML report: {}'.format(url))
        requests.get(url=url)

    def add_snapshots(self):
        self.test.cbmonitor_snapshots = []
        for cluster_id in self.test.cbmonitor_clusters:
            self.settings.cluster = cluster_id
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(cluster_id)
            self.trigger_report(cluster_id)

            self.test.cbmonitor_snapshots.append(cluster_id)

    def find_time_series(self):
        store = PerfStore(host=StatsSettings.CBMONITOR)
        dbs = []
        for cluster_id in self.test.cbmonitor_clusters:
            dbs += store.find_dbs(cluster_id)
        logger.info('Time series: {}'.format(pretty_dict(dbs)))

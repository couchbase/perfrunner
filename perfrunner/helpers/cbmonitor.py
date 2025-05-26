import time
from collections import OrderedDict
from copy import copy
from multiprocessing import Process
from pathlib import Path
from typing import Callable, Union

import pkg_resources
import requests
from decorator import decorator

from cbagent.collectors import (
    IO,
    PS,
    VMSTAT,
    ActiveTasks,
    AnalyticsStats,
    CBStatsAll,
    CBStatsMemory,
    Disk,
    DurabilityLatency,
    ElasticStats,
    EventingConsumerStats,
    EventingPerHandlerStats,
    EventingPerNodeStats,
    EventingStats,
    FTSCollector,
    FTSUtilisationCollector,
    JTSCollector,
    KVLatency,
    KVStoreStats,
    Memory,
    MetricsRestApiDeduplication,
    MetricsRestApiMetering,
    MetricsRestApiProcesses,
    MetricsRestApiThroughputCollection,
    N1QLStats,
    Net,
    NSServer,
    NSServerSystem,
    ObserveIndexLatency,
    ObserveSecondaryIndexLatency,
    PageCache,
    QueryLatency,
    RegulatorStats,
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
from cbagent.collectors.ai_services import WorkflowMetadataStats
from cbagent.collectors.metrics_rest_api import MetricsRestApiAppTelemetry
from cbagent.metadata_client import MetadataClient
from cbagent.settings import CbAgentSettings
from cbagent.stores import PerfStore
from logger import logger
from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import CBMONITOR_HOST
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


@decorator
def with_cloudwatch(method, *args, **kwargs):
    sdk_major_version = int(pkg_resources.get_distribution("couchbase").version[0])

    if sdk_major_version >= 3:
        from perfrunner.helpers.cloudwatch import Cloudwatch
        t0 = time.time()
        method(*args, **kwargs)
        t1 = time.time()
        Cloudwatch(args[0].cluster_spec.servers, t0, t1, method.__name__)
    else:
        logger.info("Cloudwatch unavailable in Python SDK 2 Tests.")


class CbAgent:

    def __init__(self, test: PerfTest, phase: str):
        self.test = test
        if self.test.test_config.stats_settings.enabled:
            self.settings = CbAgentSettings(test)
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
            # self.find_time_series()
            self.add_snapshots()
            self.cleanup_spring_worker_files()

    def init_clusters(self, phase: str):
        self.cluster_map = OrderedDict()

        for cluster_name, servers in self.test.cluster_spec.clusters:
            cluster_id = '{}_{}_{}_{}'.format(cluster_name,
                                              self.test.build.replace('.', ''),
                                              phase,
                                              uhex()[:4])
            self.cluster_map[cluster_id] = servers[0]
        self.test.cbmonitor_clusters = list(self.cluster_map.keys())

    def add_collectors(
        self,
        active_tasks=True,
        analytics=False,
        cbstats_all=False,
        cbstats_memory=False,
        disk=False,
        durability=False,
        elastic_stats=False,
        eventing_stats=False,
        fts_stats=False,
        index_latency=False,
        iostat=True,
        jts_stats=False,
        kv_dedup=False,
        kvstore=False,
        latency=False,
        memory=True,
        n1ql_latency=False,
        n1ql_stats=False,
        net=True,
        ns_server=True,
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
        syncgateway_stats=False,
        sgimport_latency=False,
        vmstat=False,
        xdcr_lag=False,
        xdcr_stats=False,
        regulator_stats=False,
        utilisation_stats=False,
        app_telemetry=False,
        ai_workflow_stats=False,
    ):
        self.collectors = []
        self.processes = []

        if ns_server:
            self.add_collector(NSServer)

        if self.test.test_config.collection.collection_stat_groups:
            self.add_collector(MetricsRestApiThroughputCollection)

        if active_tasks:
            self.add_collector(ActiveTasks)

        if (
            self.test.test_config.cluster.fts_index_mem_quota != 0
            and self.test.test_config.cluster.serverless_mode == "enabled"
        ):
            self.add_collector(RegulatorStats, self.test)
            self.add_collector(FTSUtilisationCollector, self.test)

        if latency:
            self.add_collector(KVLatency)
        if query_latency or n1ql_latency:
            self.add_collector(QueryLatency)
        if n1ql_stats:
            self.add_collector(N1QLStats)
        if ns_server_system:
            self.add_collector(NSServerSystem)
        if xdcr_lag:
            self.add_xdcr_lag()
        if xdcr_stats:
            self.add_collector(XdcrStats)
        if analytics:
            self.add_collector(AnalyticsStats, self.test)
        if kv_dedup:
            self.add_collector(MetricsRestApiDeduplication)

        if self.test.test_config.cluster.serverless_mode == 'enabled':
            self.add_collector(MetricsRestApiMetering)

        # Always collect ns-server managed processes utilisation metrics
        self.add_collector(MetricsRestApiProcesses)
        if app_telemetry:
            self.add_collector(MetricsRestApiAppTelemetry)

        if self.test.dynamic_infra:
            return

        if not self.test.capella_infra:
            if self.test.remote.PLATFORM != "cygwin":
                self.add_collector(PS)
                self.add_collector(Sysdig)
                if memory:
                    self.add_collector(Memory)
                if net:
                    self.add_collector(Net)
                if page_cache:
                    self.add_io_collector(PageCache)
                if vmstat:
                    self.add_collector(VMSTAT)
                if kvstore:
                    self.add_collector(KVStoreStats, self.test)
                if cbstats_memory:
                    self.add_collector(CBStatsMemory, self.test)
                if cbstats_all:
                    self.add_collector(CBStatsAll, self.test)
                if not self.test.cloud_infra:
                    if disk:
                        self.add_io_collector(Disk)
                    if iostat:
                        self.add_io_collector(IO)
            else:
                self.add_collector(TypePerf)

        if durability:
            self.add_durability_collector()
        if index_latency:
            self.add_collector(ObserveIndexLatency)
        if eventing_stats:
            self.add_collector(EventingStats, self.test)
            self.add_collector(EventingPerNodeStats, self.test)
            if not self.test.capella_infra:
                self.add_collector(EventingPerHandlerStats, self.test)
                self.add_collector(EventingConsumerStats, self.test)
        if fts_stats:
            self.add_collector(FTSCollector, self.test)
        if elastic_stats:
            self.add_collector(ElasticStats, self.test)
        if jts_stats:
            self.add_collector(JTSCollector, self.test)
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
        if syncgateway_stats:
            self.add_collector(SyncGatewayStats, self.test)
        if sgimport_latency:
            self.add_sgimport_latency()

        if ai_workflow_stats:
            self.add_collector(WorkflowMetadataStats)

    def add_sgimport_latency(self):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node
            collector = SGImportLatency(settings, self.test.cluster_spec)
            self.collectors.append(collector)

    def add_collector(self, cls, *args):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node
            if settings.cloud.get('enabled') and not settings.cloud.get('dynamic', False):
                settings.hostnames = self.test.cluster_spec.cluster_servers(master_node)
            collector = cls(settings, *args)
            self.collectors.append(collector)

    def add_io_collector(self, cls):
        partitions = {
            'client': {},
            'server': {'data': self.test.cluster_spec.data_path},
        }
        if self.test.test_config.showfast.component == 'views':
            partitions['server']['index'] = self.test.cluster_spec.index_path
        elif self.test.test_config.showfast.component == 'tools':
            partitions['client']['tools'] = self.test.cluster_spec.backup
        elif self.test.test_config.showfast.component == 'analytics':
            for i, path in enumerate(self.test.cluster_spec.analytics_paths):
                partitions['server']['analytics{}'.format(i)] = path

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

            break

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
        store = PerfStore(host=CBMONITOR_HOST)
        dbs = []
        for cluster_id in self.test.cbmonitor_clusters:
            dbs += store.find_dbs(cluster_id)
        logger.info('Time series: {}'.format(pretty_dict(dbs)))

    def cleanup_spring_worker_files(self):
        for pattern in [KVLatency.PATTERN, QueryLatency.PATTERN]:
            for file in Path.cwd().glob(pattern):
                file.unlink()

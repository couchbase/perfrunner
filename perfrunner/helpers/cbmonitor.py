from calendar import timegm
from collections import OrderedDict
from copy import copy
from datetime import datetime
from multiprocessing import Process

import requests
from decorator import decorator
from logger import logger

from cbagent.collectors import (
    IO,
    PS,
    ActiveTasks,
    DurabilityLatency,
    ElasticStats,
    FTSCollector,
    FTSLatencyCollector,
    N1QLStats,
    Net,
    NSServer,
    NSServerOverview,
    ObserveIndexLatency,
    ObserveSecondaryIndexLatency,
    ReservoirQueryLatency,
    SecondaryDebugStats,
    SecondaryDebugStatsBucket,
    SecondaryDebugStatsIndex,
    SecondaryLatencyStats,
    SecondaryStats,
    SecondaryStorageStats,
    SecondaryStorageStatsMM,
    SpringLatency,
    SubdocLatency,
    TypePerf,
    XdcrLag,
    XdcrStats,
)
from cbagent.metadata_client import MetadataClient
from perfrunner.helpers.misc import target_hash, uhex
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import StatsSettings


@decorator
def with_stats(method, *args, **kwargs):
    test = args[0]

    stats_enabled = test.test_config.stats_settings.enabled

    if stats_enabled:
        test.cbagent.init_clusters(test=test, phase=method.__name__)
        test.cbagent.add_collectors(test, **test.COLLECTORS)
        test.cbagent.update_metadata()
        test.cbagent.start()

    from_ts = datetime.utcnow()
    method(*args, **kwargs)
    to_ts = datetime.utcnow()

    if stats_enabled:
        test.cbagent.stop()
        test.cbagent.reconstruct()
        test.snapshots = test.cbagent.add_snapshot(from_ts, to_ts)

    from_ts = timegm(from_ts.timetuple()) * 1000  # -> ms
    to_ts = timegm(to_ts.timetuple()) * 1000  # -> ms
    return from_ts, to_ts


class CbAgent:

    def __init__(self, test, verbose):
        self.remote = RemoteHelper(test.cluster_spec, test.test_config,
                                   verbose=verbose)

        if hasattr(test, 'ALL_BUCKETS'):
            buckets = None
        else:
            buckets = test.test_config.buckets[:1]
        if hasattr(test, 'ALL_HOSTNAMES'):
            hostnames = tuple(test.cluster_spec.yield_hostnames())
        else:
            hostnames = None

        self.settings = type('settings', (object,), {
            'seriesly_host': StatsSettings.SERIESLY,
            'cbmonitor_host_port': StatsSettings.CBMONITOR,
            'interval': test.test_config.stats_settings.interval,
            'lat_interval': test.test_config.stats_settings.lat_interval,
            'secondary_statsfile': test.test_config.stats_settings.secondary_statsfile,
            'buckets': buckets,
            'indexes': test.test_config.gsi_settings.indexes,
            'hostnames': hostnames,
            'monitored_processes': test.test_config.stats_settings.monitored_processes,
        })()
        if test.cluster_spec.ssh_credentials:
            self.settings.ssh_username, self.settings.ssh_password = \
                test.cluster_spec.ssh_credentials
        self.settings.rest_username, self.settings.rest_password = \
            test.cluster_spec.rest_credentials
        self.settings.bucket_password = test.test_config.bucket.password

        for _, servers in test.cluster_spec.yield_servers_by_role('index'):
            if servers:
                self.settings.index_node = servers[0].split(':')[0]

    def init_clusters(self, test, phase):
        self.cluster_map = OrderedDict()

        for cluster_name, servers in test.cluster_spec.yield_clusters():
            cluster_id = '{}_{}_{}_{}'.format(cluster_name,
                                              test.build.replace('.', ''),
                                              phase,
                                              uhex()[:4])
            master = servers[0].split(':')[0]
            self.cluster_map[cluster_id] = master
        self.cluster_ids = list(self.cluster_map.keys())

    def add_collectors(self, test,
                       elastic_stats=False,
                       durability=False,
                       fts_latency=False,
                       fts_stats=False,
                       iostat=True,
                       latency=False,
                       index_latency=False,
                       n1ql_latency=False,
                       n1ql_stats=False,
                       net=True,
                       query_latency=False,
                       secondary_debugstats=False,
                       secondary_debugstats_bucket=False,
                       secondary_debugstats_index=False,
                       secondary_index_latency=False,
                       secondary_latency=False,
                       secondary_stats=False,
                       secondary_storage_stats=False,
                       secondary_storage_stats_mm=False,
                       subdoc_latency=False,
                       xdcr_lag=False,
                       xdcr_stats=False):
        self.collectors = []
        self.processes = []

        self.add_collector(NSServer)
        self.add_collector(NSServerOverview)
        self.add_collector(ActiveTasks)

        if test.remote is None or test.remote.os != 'Cygwin':
            self.add_collector(PS)
            if net:
                self.add_collector(Net)
            if iostat:
                self.add_iostat(test)
        elif test.remote.os == 'Cygwin':
            self.add_collector(TypePerf)

        if durability:
            self.add_durability(test)
        if index_latency:
            self.add_collector(ObserveIndexLatency)
        if latency:
            self.add_kv_latency(test)
        if query_latency or n1ql_latency:
            self.add_collector(ReservoirQueryLatency)
        if subdoc_latency:
            self.add_subdoc_latency(test)

        if n1ql_stats:
            self.add_collector(N1QLStats)

        if elastic_stats:
            self.add_collector(ElasticStats, test)
        if fts_latency:
            self.add_collector(FTSLatencyCollector, test)
        if fts_stats:
            self.add_collector(FTSCollector, test)

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
            self.add_xdcr_lag(test)
        if xdcr_stats:
            self.add_collector(XdcrStats)

    def add_collector(self, cls, *args):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node

            collector = cls(settings, *args)
            self.collectors.append(collector)

    def add_iostat(self, test):
        data_path, index_path = test.cluster_spec.paths
        partitions = {'data': data_path}
        if hasattr(test, 'ddocs'):  # all instances of IndexTest have it
            partitions['index'] = index_path

        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node
            settings.partitions = partitions

            collector = IO(settings)
            self.collectors.append(collector)

    def add_kv_latency(self, test):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node
            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])

            collector = SpringLatency(settings, test.workload, prefix)
            self.collectors.append(collector)

    def add_durability(self, test):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node

            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])

            collector = DurabilityLatency(settings, test.workload, prefix)
            self.collectors.append(collector)

    def add_subdoc_latency(self, test):
        for cluster_id, master_node in self.cluster_map.items():
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = master_node

            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])

            collector = SubdocLatency(settings, test.workload, prefix)
            self.collectors.append(collector)

    def add_xdcr_lag(self, test):
        reversed_clusters = list(reversed(self.cluster_ids))

        for i, cluster_id in enumerate(self.cluster_map):
            settings = copy(self.settings)
            settings.cluster = cluster_id
            settings.master_node = self.cluster_map[cluster_id]
            dest_cluster = reversed_clusters[i]
            settings.dest_master_node = self.cluster_map[dest_cluster]

            collector = XdcrLag(settings)
            self.collectors.append(collector)

            if test.settings.replication_type == 'unidir':
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

    def trigger_reports(self, snapshot):
        for report_type in ('html', ):
            url = 'http://{}/reports/{}/?snapshot={}'.format(
                self.settings.cbmonitor_host_port, report_type, snapshot)
            logger.info(url)
            requests.get(url=url)

    def add_snapshot(self, ts_from, ts_to):
        snapshots = []
        for cluster_id in self.cluster_ids:
            self.settings.cluster = cluster_id
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(cluster_id, ts_from, ts_to)
            snapshots.append(cluster_id)
            self.trigger_reports(cluster_id)
        return snapshots

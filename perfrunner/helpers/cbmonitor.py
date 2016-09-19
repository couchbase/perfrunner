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
    FtsLatency,
    FtsQueryStats,
    FtsStats,
    N1QLStats,
    Net,
    NSServer,
    ObserveLatency,
    SecondaryDebugStats,
    SecondaryDebugStatsBucket,
    SecondaryLatencyStats,
    SecondaryStats,
    SpringLatency,
    SpringN1QLQueryLatency,
    SpringQueryLatency,
    SpringSubdocLatency,
    TypePerf,
    XdcrLag,
    XdcrStats,
)

from cbagent.collectors.secondary_debugstats import SecondaryDebugStatsIndex
from cbagent.metadata_client import MetadataClient
from perfrunner.helpers.misc import target_hash, uhex
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper


@decorator
def with_stats(method, *args, **kwargs):
    test = args[0]

    stats_enabled = test.test_config.stats_settings.enabled

    from_ts = datetime.utcnow()
    if stats_enabled:
        if not test.cbagent.collectors:
            test.cbagent.prepare_collectors(test, **test.COLLECTORS)
            test.cbagent.update_metadata()
        test.cbagent.start()

    method(*args, **kwargs)
    to_ts = datetime.utcnow()

    if stats_enabled:
        test.cbagent.stop()
        if test.test_config.stats_settings.add_snapshots:
            test.cbagent.add_snapshot(method.__name__, from_ts, to_ts)
            test.snapshots = test.cbagent.snapshots

    from_ts = timegm(from_ts.timetuple()) * 1000  # -> ms
    to_ts = timegm(to_ts.timetuple()) * 1000  # -> ms
    return from_ts, to_ts


class CbAgent(object):

    def __init__(self, test, verbose):
        self.clusters = OrderedDict()
        self.remote = RemoteHelper(test.cluster_spec, test.test_config,
                                   verbose=verbose)

        for cluster_name, servers in test.cluster_spec.yield_clusters():
            cluster = '{}_{}_{}'.format(cluster_name,
                                        test.build.replace('.', ''),
                                        uhex()[:3])
            master = servers[0].split(':')[0]
            self.clusters[cluster] = master

        self.index_node = ''
        for _, servers in test.cluster_spec.yield_servers_by_role('index'):
            if servers:
                self.index_node = servers[0].split(':')[0]

        if hasattr(test, 'ALL_BUCKETS'):
            buckets = None
        else:
            buckets = test.test_config.buckets[:1]
        if hasattr(test, 'ALL_HOSTNAMES'):
            hostnames = tuple(test.cluster_spec.yield_hostnames())
        else:
            hostnames = None

        self.settings = type('settings', (object,), {
            'seriesly_host': test.test_config.stats_settings.seriesly['host'],
            'cbmonitor_host_port': test.test_config.stats_settings.cbmonitor['host'],
            'interval': test.test_config.stats_settings.interval,
            'secondary_statsfile': test.test_config.stats_settings.secondary_statsfile,
            'buckets': buckets,
            'indexes': test.test_config.secondaryindex_settings.indexes,
            'hostnames': hostnames,
            'fts_server': test.test_config.test_case.fts_server
        })()
        self.lat_interval = test.test_config.stats_settings.lat_interval
        if test.cluster_spec.ssh_credentials:
            self.settings.ssh_username, self.settings.ssh_password = \
                test.cluster_spec.ssh_credentials
        self.settings.rest_username, self.settings.rest_password = \
            test.cluster_spec.rest_credentials
        self.settings.bucket_password = test.test_config.bucket.password

        self.settings.index_node = self.index_node

        self.collectors = []
        self.processes = []
        self.snapshots = []
        self.fts_stats = None

    def prepare_collectors(self, test,
                           bandwidth=False,
                           subdoc_latency=False,
                           latency=False,
                           secondary_stats=False,
                           query_latency=False,
                           n1ql_latency=False,
                           n1ql_stats=False,
                           index_latency=False,
                           persist_latency=False,
                           replicate_latency=False,
                           xdcr_lag=False,
                           secondary_latency=False,
                           secondary_debugstats=False,
                           secondary_debugstats_bucket=False,
                           secondary_debugstats_index=False,
                           fts_latency=False,
                           elastic_stats=False,
                           fts_stats=False,
                           fts_query_stats=False,
                           durability=False,
                           xdcr_stats=False):
        clusters = self.clusters.keys()
        self.bandwidth = bandwidth
        self.prepare_ns_server(clusters)
        self.prepare_active_tasks(clusters)
        if test.remote is None or test.remote.os != 'Cygwin':
            self.prepare_ps(clusters)
            self.prepare_net(clusters)
            self.prepare_iostat(clusters, test)
        elif test.remote.os == 'Cygwin':
            self.prepare_tp(clusters)
        if subdoc_latency:
            self.prepare_subdoc_latency(clusters, test)
        if latency:
            self.prepare_latency(clusters, test)
        if durability:
            self.prepare_durability(clusters, test)
        if query_latency:
            self.prepare_query_latency(clusters, test)
        if n1ql_latency:
            self.prepare_n1ql_latency(clusters, test)
        if secondary_stats:
            self.prepare_secondary_stats(clusters)
        if secondary_debugstats:
            self.prepare_secondary_debugstats(clusters)
        if secondary_debugstats_bucket:
            self.prepare_secondary_debugstats_bucket(clusters)
        if secondary_debugstats_index:
            self.prepare_secondary_debugstats_index(clusters)
        if secondary_latency:
            self.prepare_secondary_latency(clusters)
        if n1ql_stats:
            self.prepare_n1ql_stats(clusters)
        if index_latency:
            self.prepare_index_latency(clusters)
        if persist_latency:
            self.prepare_persist_latency(clusters)
        if replicate_latency:
            self.prepare_replicate_latency(clusters)
        if xdcr_lag:
            self.prepare_xdcr_lag(clusters)
        if fts_latency:
            self.prepare_fts_latency(clusters, test.test_config)
        if fts_stats:
            self.prepare_fts_stats(clusters, test.test_config)
        if fts_query_stats:
            self.prepare_fts_query_stats(clusters, test.test_config)
        if elastic_stats:
            self.prepare_elastic_stats(clusters, test.test_config)
        if xdcr_stats:
            self.prepare_xdcr_stats(clusters)

    def prepare_ns_server(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            collector = NSServer(settings)
            self.collectors.append(collector)

    def prepare_xdcr_stats(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            collector = XdcrStats(settings)
            self.collectors.append(collector)

    def prepare_secondary_stats(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SecondaryStats(settings))

    def prepare_secondary_debugstats(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SecondaryDebugStats(settings))

    def prepare_secondary_debugstats_bucket(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SecondaryDebugStatsBucket(settings))

    def prepare_secondary_debugstats_index(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SecondaryDebugStatsIndex(settings))

    def prepare_secondary_latency(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SecondaryLatencyStats(settings))

    def prepare_n1ql_stats(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(N1QLStats(settings))

    def prepare_ps(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            ps_collector = PS(settings)
            self.collectors.append(ps_collector)

    def prepare_tp(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            tp_collector = TypePerf(settings)
            self.collectors.append(tp_collector)

    def prepare_net(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            net_collector = Net(settings)
            self.collectors.append(net_collector)

    def prepare_iostat(self, clusters, test):
        # If tests are run locally, no paths are defined, hence
        # use the paths that are set by the server itself. Get
        # those paths via ther REST API
        rest = None

        if test.cluster_spec.paths:
            data_path, index_path = test.cluster_spec.paths
        else:
            rest = RestHelper(test.cluster_spec)

        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]

            if rest is not None:
                data_path, index_path = rest.get_data_path(settings.master_node)
                partitions = {'data': data_path}
                if hasattr(test, 'ddocs'):  # all instances of IndexTest have it
                    partitions['index'] = index_path

            settings.partitions = partitions
            io_collector = IO(settings)
            self.collectors.append(io_collector)

    def prepare_persist_latency(self, clusters):
        for i, cluster in enumerate(clusters):
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            settings.observe = 'persist'
            self.collectors.append(ObserveLatency(settings))

    def prepare_replicate_latency(self, clusters):
        for i, cluster in enumerate(clusters):
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            settings.observe = 'replicate'
            self.collectors.append(ObserveLatency(settings))

    def prepare_index_latency(self, clusters):
        for i, cluster in enumerate(clusters):
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            settings.observe = 'index'
            self.collectors.append(ObserveLatency(settings))

    def prepare_xdcr_lag(self, clusters):
        reversed_clusters = list(reversed(self.clusters.keys()))
        for i, cluster in enumerate(clusters):
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            dest_cluster = reversed_clusters[i]
            settings.dest_master_node = self.clusters[dest_cluster]
            self.collectors.append(XdcrLag(settings))

    def prepare_latency(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.interval = self.lat_interval
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])
            self.collectors.append(
                SpringLatency(settings, test.workload, prefix)
            )

    def prepare_durability(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.interval = self.lat_interval
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])
            self.collectors.append(
                DurabilityLatency(settings, test.workload, prefix)
            )

    def prepare_subdoc_latency(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.interval = self.lat_interval
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])
            self.collectors.append(
                SpringSubdocLatency(settings, test.workload, prefix)
            )

    def prepare_query_latency(self, clusters, test):
        params = test.test_config.index_settings.params
        index_type = test.test_config.index_settings.index_type
        for cluster in clusters:
            settings = copy(self.settings)
            settings.interval = self.lat_interval
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])
            self.collectors.append(
                SpringQueryLatency(settings, test.workload, prefix=prefix,
                                   ddocs=test.ddocs, params=params,
                                   index_type=index_type)
            )

    def prepare_n1ql_latency(self, clusters, test):
        default_queries = test.test_config.access_settings.n1ql_queries
        self.settings.new_n1ql_queries = getattr(test, 'n1ql_queries',
                                                 default_queries)
        for cluster in clusters:
            settings = copy(self.settings)
            settings.interval = self.lat_interval
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(
                SpringN1QLQueryLatency(settings, test.workload, prefix='n1ql')
            )

    def prepare_fts_latency(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(
                FtsLatency(settings, test)
            )

    def prepare_fts_stats(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(
                FtsStats(settings, test)
            )

    def prepare_fts_query_stats(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.fts_stats = FtsQueryStats(settings, test)
            self.collectors.append(
                FtsQueryStats(settings, test)
            )

    def prepare_elastic_stats(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.fts_stats = ElasticStats(settings, test)
            self.collectors.append(
                self.fts_stats
            )

    def prepare_active_tasks(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            collector = ActiveTasks(settings)
            self.collectors.append(collector)

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        logger.info('Starting stats collectors')
        self.processes = [Process(target=c.collect) for c in self.collectors]
        map(lambda p: p.start(), self.processes)

    def stop(self):
        logger.info('Terminating stats collectors')
        map(lambda p: p.terminate(), self.processes)
        if self.bandwidth:
            self.remote.kill_process('iptraf')

    def trigger_reports(self, snapshot):
        for report_type in ('html', ):
            url = 'http://{}/reports/{}/?snapshot={}'.format(
                self.settings.cbmonitor_host_port, report_type, snapshot)
            logger.info(url)
            requests.get(url=url)

    def add_snapshot(self, phase, ts_from, ts_to):
        for i, cluster in enumerate(self.clusters, start=1):
            snapshot = '{}_{}'.format(cluster, phase)
            self.settings.cluster = cluster
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(snapshot, ts_from, ts_to)
            self.snapshots.append(snapshot)
            self.trigger_reports(snapshot)

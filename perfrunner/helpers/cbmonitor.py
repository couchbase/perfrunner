from calendar import timegm
from collections import OrderedDict
from copy import copy
from datetime import datetime
from multiprocessing import Process

import requests
from cbagent.collectors import (NSServer, PS, TypePerf, IO, Net, ActiveTasks,
                                SpringLatency, SpringQueryLatency,
                                SpringSpatialQueryLatency,
                                SpringN1QLQueryLatency, SecondaryStats, SecondaryLatencyStats,
                                N1QLStats, SecondaryDebugStats, ObserveLatency, XdcrLag)
from cbagent.metadata_client import MetadataClient
from decorator import decorator
from logger import logger

from perfrunner.helpers.misc import target_hash, uhex
from perfrunner.helpers.rest import RestHelper


@decorator
def with_stats(method, *args, **kwargs):
    test = args[0]

    stats_enabled = test.test_config.stats_settings.enabled

    if stats_enabled:
        if not test.cbagent.collectors:
            test.cbagent.prepare_collectors(test, **test.COLLECTORS)
            test.cbagent.update_metadata()
        test.cbagent.start()

    from_ts = datetime.utcnow()
    method(*args, **kwargs)
    to_ts = datetime.utcnow()

    if stats_enabled:
        test.cbagent.stop()

        test.cbagent.add_snapshot(method.__name__, from_ts, to_ts)
        test.snapshots = test.cbagent.snapshots

    from_ts = timegm(from_ts.timetuple()) * 1000  # -> ms
    to_ts = timegm(to_ts.timetuple()) * 1000  # -> ms
    return from_ts, to_ts


class CbAgent(object):

    def __init__(self, test):
        self.clusters = OrderedDict()
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
            'hostnames': hostnames,
            'sync_gateway_nodes':
                test.remote.gateways if test.remote else None,
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

    def prepare_collectors(self, test,
                           latency=False, secondary_stats=False,
                           query_latency=False, spatial_latency=False,
                           n1ql_latency=False, n1ql_stats=False,
                           index_latency=False, persist_latency=False,
                           replicate_latency=False, xdcr_lag=False,
                           secondary_latency=False,
                           secondary_debugstats=False):
        clusters = self.clusters.keys()

        self.prepare_ns_server(clusters)
        self.prepare_active_tasks(clusters)
        if test.remote is None or test.remote.os != 'Cygwin':
            self.prepare_ps(clusters)
            self.prepare_net(clusters)
            self.prepare_iostat(clusters, test)
        elif test.remote.os == 'Cygwin':
            self.prepare_tp(clusters)
        if latency:
            self.prepare_latency(clusters, test)
        if query_latency:
            self.prepare_query_latency(clusters, test)
        if spatial_latency:
            self.prepare_spatial_latency(clusters, test)
        if n1ql_latency:
            self.prepare_n1ql_latency(clusters, test)
        if secondary_stats:
            self.prepare_secondary_stats(clusters)
        if secondary_debugstats:
            self.prepare_secondary_debugstats(clusters)
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

    def prepare_ns_server(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(NSServer(settings))

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
                data_path, index_path = rest.get_data_path(
                    settings.master_node)

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

    def prepare_spatial_latency(self, clusters, test):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.interval = self.lat_interval
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            prefix = test.target_iterator.prefix or \
                target_hash(settings.master_node.split(':')[0])
            self.collectors.append(
                SpringSpatialQueryLatency(
                    settings, test.workload, prefix=prefix,
                    spatial_settings=test.test_config.spatial_settings)
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

    def prepare_active_tasks(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(ActiveTasks(settings))

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        self.processes = [Process(target=c.collect) for c in self.collectors]
        map(lambda p: p.start(), self.processes)

    def stop(self):
        map(lambda p: p.terminate(), self.processes)
        return datetime.utcnow()

    def trigger_reports(self, snapshot):
        for report_type in ('html', 'get_corr_matrix'):
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

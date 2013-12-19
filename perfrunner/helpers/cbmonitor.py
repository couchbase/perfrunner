from calendar import timegm
from collections import OrderedDict
from copy import copy
from datetime import datetime
from multiprocessing import Process

from decorator import decorator
import requests
from cbagent.collectors import (NSServer, PS, IO, ActiveTasks,
                                SpringLatency, SpringQueryLatency, XdcrLag)
from cbagent.metadata_client import MetadataClient

from perfrunner.helpers.misc import target_hash, uhex
from perfrunner.settings import CBMONITOR_HOST


def with_stats(latency=False, query_latency=False, xdcr_lag=False):
    def with_stats(method, *args, **kwargs):
        test = args[0]

        if not test.cbagent.collectors:
            test.cbagent.prepare_collectors(test, latency, query_latency,
                                            xdcr_lag)
            test.cbagent.update_metadata()

        from_ts = test.cbagent.start()
        method(*args, **kwargs)
        to_ts = test.cbagent.stop()

        test.cbagent.add_snapshot(method.__name__, from_ts, to_ts)
        test.reports = test.cbagent.reports

        from_ts = timegm(from_ts.timetuple()) * 1000  # -> ms
        to_ts = timegm(to_ts.timetuple()) * 1000  # -> ms
        return from_ts, to_ts
    return decorator(with_stats)


class CbAgent(object):

    def __init__(self, cluster_spec, test_config, build):
        self.clusters = OrderedDict()
        for cluster, master in cluster_spec.get_masters().items():
            cluster = '{}_{}_{}'.format(cluster,
                                        build.replace('.', ''),
                                        uhex()[:3])
            master = master.split(':')[0]
            self.clusters[cluster] = master

        self.settings = type('settings', (object, ), {
            'seriesly_host': CBMONITOR_HOST,
            'cbmonitor_host_port': CBMONITOR_HOST,
            'interval': test_config.get_stats_settings().interval,
        })
        self.settings.ssh_username, self.settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        self.settings.rest_username, self.settings.rest_password = \
            cluster_spec.get_rest_credentials()

        self.collectors = []
        self.processes = []
        self.reports = {}

    def prepare_collectors(self, test, latency, query_latency, xdcr_lag):
        clusters = self.clusters.keys()

        self.prepare_ns_server(clusters)
        self.prepare_active_tasks(clusters)
        if test.remote.os != 'Cygwin':
            self.prepare_ps(clusters)
            self.prepare_iostat(clusters, test)
        if latency and hasattr(test, "workload"):
            self.prepare_latency(clusters, test.workload)
        if query_latency:
            self.prepare_query_latency(clusters, test.workload, test.ddocs)
        if xdcr_lag:
            self.prepare_xdcr_lag(clusters)

    def prepare_ns_server(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(NSServer(settings))

    def prepare_ps(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            ps_collector = PS(settings)
            self.collectors.append(ps_collector)

    def prepare_iostat(self, clusters, test):
        data_path, index_path = test.cluster_spec.get_paths()
        partitions = {'data': data_path}
        if hasattr(test, 'ddocs'):  # all instances of IndexTest have it
            partitions['index'] = index_path
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            settings.partitions = partitions
            io_collector = IO(settings)
            self.collectors.append(io_collector)

    def prepare_xdcr_lag(self, clusters):
        reversed_clusters = list(reversed(self.clusters.keys()))
        for i, cluster in enumerate(clusters):
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            dest_cluster = reversed_clusters[i]
            settings.dest_master_node = self.clusters[dest_cluster]
            self.collectors.append(XdcrLag(settings))

    def prepare_latency(self, clusters, workload):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            prefix = target_hash(settings.master_node.split(':')[0])
            self.collectors.append(SpringLatency(settings, workload, prefix))

    def prepare_query_latency(self, clusters, workload, ddocs):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SpringQueryLatency(settings, workload, ddocs))

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
        return datetime.utcnow()

    def stop(self):
        map(lambda p: p.terminate(), self.processes)
        return datetime.utcnow()

    def generate_report(self, snapshot):
        api = 'http://{}/reports/html/'.format(CBMONITOR_HOST)
        url = '{}?snapshot={}'.format(api, snapshot)
        requests.get(url=url)
        return url

    def add_snapshot(self, phase, ts_from, ts_to):
        for i, cluster in enumerate(self.clusters, start=1):
            snapshot = '{}_{}'.format(cluster, phase)
            self.settings.cluster = cluster
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(snapshot, ts_from, ts_to)
            self.reports['report{}'.format(i)] = self.generate_report(snapshot)

import pytz
from copy import copy
from datetime import datetime
from decorator import decorator
from multiprocessing import Process
from time import time

import requests
from cbagent.collectors import (NSServer, SpringLatency, SpringQueryLatency,
                                SpringTuqLatency, XdcrLag, ActiveTasks, Atop)
from cbagent.metadata_client import MetadataClient
from ordereddict import OrderedDict

from perfrunner.helpers.misc import uhex
from perfrunner.settings import CbAgentSettings


def with_stats(latency=False, query_latency=False, tuq_latency=False,
               xdcr_lag=False, active_tasks=False):
    def with_stats(method, *args, **kwargs):
        test = args[0]

        test.cbagent.prepare_collectors(test, latency, query_latency, tuq_latency,
                                        xdcr_lag, active_tasks)
        test.cbagent.update_metadata()

        from_ts = test.cbagent.start()
        method(*args, **kwargs)
        to_ts = test.cbagent.stop()

        report = test.test_config.get_stats_settings().report
        test.cbagent.add_snapshot(method.__name__, from_ts, to_ts, report)
        test.reports = test.cbagent.reports
    return decorator(with_stats)


class CbAgent(object):

    def __init__(self, cluster_spec, build):
        self.clusters = OrderedDict()
        for cluster, master in cluster_spec.get_masters().items():
            cluster = '{0}_{1}_{2}'.format(cluster,
                                           build.replace('.', ''),
                                           uhex()[:3])
            master = master.split(':')[0]
            self.clusters[cluster] = master

        self.settings = CbAgentSettings()
        self.settings.ssh_username, self.settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        self.settings.rest_username, self.settings.rest_password = \
            cluster_spec.get_rest_credentials()

    def prepare_collectors(self, test, latency, query_latency, tuq_latency,
                           xdcr_lag, active_tasks):
        clusters = self.clusters.keys()
        self.collectors = []

        self.prepare_ns_server(clusters)
        self.prepare_atop(clusters)
        if latency:
            self.prepare_latency(clusters, test.workload)
        if query_latency:
            self.prepare_query_latency(clusters, test.workload, test.ddocs)
        if tuq_latency:
            tuq = test.test_config.get_tuq_settings()
            self.prepare_tuq_latency(clusters, test.workload,
                                     tuq.indexes, tuq.server_addr)
        if xdcr_lag:
            self.prepare_xdcr_lag(clusters)
        if active_tasks:
            self.prepare_active_tasks(clusters)

    def prepare_ns_server(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(NSServer(settings))

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
            self.collectors.append(SpringLatency(settings, workload))

    def prepare_query_latency(self, clusters, workload, ddocs):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SpringQueryLatency(settings, workload, ddocs))

    def prepare_tuq_latency(self, clusters, workload, indexes, tuq_server):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(SpringTuqLatency(settings, workload,
                                                    indexes, tuq_server))

    def prepare_active_tasks(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            self.collectors.append(ActiveTasks(settings))

    def prepare_atop(self, clusters):
        for cluster in clusters:
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            atop = Atop(settings)
            atop.restart()
            atop.update_columns()
            self.collectors.append(atop)

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        self.processes = [Process(target=c.collect) for c in self.collectors]
        map(lambda p: p.start(), self.processes)
        return datetime.fromtimestamp(time(), tz=pytz.utc)

    def stop(self):
        map(lambda p: p.terminate(), self.processes)
        return datetime.fromtimestamp(time(), tz=pytz.utc)

    def generate_report(self, snapshot, report):
        api = 'http://{0}/reports/html/'.format(
            self.settings.cbmonitor_host_port)
        url = '{0}?snapshot={1}&report={2}'.format(api, snapshot, report)
        requests.get(url=url)
        return url

    def add_snapshot(self, phase, ts_from, ts_to, report):
        self.reports = {}
        for i, cluster in enumerate(self.clusters, start=1):
            snapshot = '{0}_{1}'.format(cluster, phase)
            self.settings.cluster = cluster
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(snapshot, ts_from, ts_to)
            self.reports['report{0}'.format(i)] = self.generate_report(snapshot,
                                                                       report)

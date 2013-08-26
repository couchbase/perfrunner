import pytz
from copy import copy
from datetime import datetime
from multiprocessing import Process
from time import time
from uuid import uuid4

import requests
from cbagent.collectors import (NSServer, SpringLatency, SpringQueryLatency,
                                XdcrLag)
from cbagent.metadata_client import MetadataClient
from logger import logger
from ordereddict import OrderedDict

from perfrunner.settings import CbAgentSettings


def with_stats(latency=False, query_latency=False, xdcr_lag=False):
    def outer_wrapper(method):
        def inner_wrapper(self, *args, **kwargs):
            self.cbagent.prepare_collectors(self, latency, query_latency,
                                            xdcr_lag)

            self.cbagent.update_metadata()

            from_ts = self.cbagent.start()
            method(self, *args, **kwargs)
            to_ts = self.cbagent.stop()

            report = self.test_config.get_stats_settings().report
            self.cbagent.add_snapshot(method.__name__, from_ts, to_ts, report)
        return inner_wrapper
    return outer_wrapper


class CbAgent(object):

    def __init__(self, cluster_spec, build):
        self.clusters = OrderedDict()
        for cluster, master in cluster_spec.get_masters().items():
            cluster = '{0}_{1}_{2}'.format(cluster,
                                           build.replace('.', ''),
                                           uuid4().hex[:3])
            master = master.split(':')[0]
            self.clusters[cluster] = master

        self.settings = CbAgentSettings()
        self.settings.ssh_username, self.settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        self.settings.rest_username, self.settings.rest_password = \
            cluster_spec.get_rest_credentials()

    def prepare_collectors(self, test, latency, query_latency, xdcr_lag):
        clusters = self.clusters.keys()
        self.collectors = []

        self.prepare_ns_server(clusters)
        if latency:
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
        API = 'http://{0}/reports/html/'.format(
            self.settings.cbmonitor_host_port)
        requests.head(API, data={'snapshot': snapshot, 'report': report})
        logger.info('Link to HTML report: {0}?snapshot={1}&report={2}'.format(
            API, snapshot, report))

    def add_snapshot(self, phase, ts_from, ts_to, report):
        for cluster in self.clusters:
            snapshot = '{0}_{1}'.format(cluster, phase)
            self.settings.cluster = cluster
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(snapshot, ts_from, ts_to)
            self.generate_report(snapshot, report)

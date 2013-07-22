import pytz
from copy import copy
from datetime import datetime
from multiprocessing import Process
from time import time

import requests
from cbagent.collectors import NSServer, Latency, XdcrLag
from cbagent.metadata_client import MetadataClient
from logger import logger

from perfrunner.settings import CbAgentSettings


def with_stats(latency=False, xdcr_lag=False):
    def outer_wrapper(method):
        def inner_wrapper(self, *args, **kwargs):
            self.cbagent.prepare_collectors(latency, xdcr_lag)

            self.cbagent.update_metadata()

            from_ts = self.cbagent.start()
            method(self, *args, **kwargs)
            to_ts = self.cbagent.stop()

            self.cbagent.add_snapshot(method.__name__, from_ts, to_ts)
        return inner_wrapper
    return outer_wrapper


class CbAgent(object):

    def __init__(self, cluster_spec):
        self.clusters = cluster_spec.get_masters()

        self.settings = CbAgentSettings()
        self.settings.ssh_username, self.settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        self.settings.rest_username, self.settings.rest_password = \
            cluster_spec.get_rest_credentials()

    def prepare_collectors(self, latency, xdcr_lag):
        collectors = [NSServer]
        if latency:
            collectors.append(Latency)
        if xdcr_lag:
            collectors.append(XdcrLag)

        self.collectors = []
        clusters = self.clusters.keys()
        reversed_clusters = list(reversed(self.clusters.keys()))
        for i, cluster in enumerate(clusters):
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = self.clusters[cluster]
            if xdcr_lag:
                dest_cluster = reversed_clusters[i]
                settings.dest_master_node = self.clusters[dest_cluster]
            for collector in collectors:
                self.collectors.append(collector(settings))

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

    def generate_report(self, snapshot, report='BaseReport'):
        API = 'http://{0}/reports/html/'.format(
            self.settings.cbmonitor_host_port)
        requests.head(API, data={'snapshot': snapshot, 'report': report})
        logger.info('Link to HTML report: {0}?snapshot={1}&report={2}'.format(
            API, snapshot, report))

    def add_snapshot(self, phase, ts_from, ts_to):
        for cluster in self.clusters:
            snapshot = '{0}_{1}'.format(cluster, phase)
            self.settings.cluster = cluster
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(snapshot, ts_from, ts_to)
            self.generate_report(snapshot)

import pytz
from copy import copy
from datetime import datetime
from multiprocessing import Process
from time import time

from cbagent.collectors import NSServer, ActiveTasks, Latency
from cbagent.metadata_client import MetadataClient

from perfrunner.settings import CbAgentSettings


def with_stats(latency=False):
    def outer_wrapper(method):
        def inner_wrapper(self, *args, **kwargs):
            cbagent = CbAgent(self.cluster_spec)
            cbagent.prepare_collectors(latency)

            cbagent.update_metadata()

            from_ts = cbagent.start()
            method(self, *args, **kwargs)
            to_ts = cbagent.stop()

            cbagent.add_snapshot(method.__name__, from_ts, to_ts)
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

    def prepare_collectors(self, latency):
        collectors = [NSServer, ActiveTasks]
        if latency:
            collectors.append(Latency)

        self.collectors = []
        for cluster, master_node in self.clusters.items():
            settings = copy(self.settings)
            settings.cluster = cluster
            settings.master_node = master_node
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

    def add_snapshot(self, phase, ts_from, ts_to):
        for cluster in self.clusters:
            snapshot = '{0}{1}'.format(cluster, phase)
            self.settings.cluster = cluster
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(snapshot, ts_from, ts_to)

from datetime import datetime
from multiprocessing import Process
from time import gmtime, mktime
from uuid import uuid4

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
        self.cluster_spec = cluster_spec

        self.settings = CbAgentSettings()
        self.settings.ssh_username, self.settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        self.settings.rest_username, self.settings.rest_password = \
            cluster_spec.get_rest_credentials()

    def gen_unique_cluster_name(self, cluster):
        return '{0}_{1}'.format(cluster, uuid4().hex[:6])

    def prepare_collectors(self, latency):
        collectors = [NSServer, ActiveTasks]
        if latency:
            collectors.append(Latency)

        self.collectors = []
        for cluster, master_node in self.cluster_spec.get_masters().items():
            self.settings.cluster = self.gen_unique_cluster_name(cluster)
            self.settings.master_node = master_node
            for collector in collectors:
                self.collectors.append(collector(self.settings))

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        self.processes = [Process(target=c.collect) for c in self.collectors]
        map(lambda p: p.start(), self.processes)
        return datetime.fromtimestamp(mktime(gmtime()))

    def stop(self):
        map(lambda p: p.terminate(), self.processes)
        return datetime.fromtimestamp(mktime(gmtime()))

    def add_snapshot(self, phase, ts_from, ts_to):
        for cluster in self.cluster_spec.get_masters():
            self.settings.cluster = self.gen_unique_cluster_name(cluster)
            snapshot = '{0}{1}'.format(self.settings.cluster, phase)
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(snapshot, ts_from, ts_to)

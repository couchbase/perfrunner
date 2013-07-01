from datetime import datetime
from multiprocessing import Process
from time import time
from uuid import uuid4

from cbagent.collectors import NSServer, ActiveTasks
from cbagent.metadata_client import MetadataClient

from perfrunner.settings import CbAgentSettings


def with_stats(method):
    def wrapper(self, *args, **kwargs):
        self.cbagent.update_metadata()

        from_ts = self.cbagent.start()
        method(self, *args, **kwargs)
        to_ts = self.cbagent.stop()

        self.cbagent.add_snapshot(method.__name__, from_ts, to_ts)
    return wrapper


class CbAgent(object):

    def __init__(self, cluster_spec, target_iterator):
        settings = CbAgentSettings()
        settings.cluster = '{0}_{1}'.format(cluster_spec.name, uuid4().hex[:6])
        settings.ssh_username, settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        settings.rest_username, settings.rest_password = \
            cluster_spec.get_rest_credentials()

        self.md_client = MetadataClient(settings)
        self.snapshot_prefix = settings.cluster

        self.collectors = []
        for target in target_iterator:
            settings.master_node = target.node.split(':')[0]
            for collector in (NSServer, ActiveTasks):
                self.collectors.append(collector(settings))

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        self.processes = [Process(target=c.collect) for c in self.collectors]
        map(lambda p: p.start(), self.processes)
        return datetime.fromtimestamp(time())

    def stop(self):
        map(lambda p: p.terminate(), self.processes)
        return datetime.fromtimestamp(time())

    def add_snapshot(self, phase, ts_from, ts_to):
        name = '{0}_{1}'.format(self.snapshot_prefix, phase)
        self.md_client.add_snapshot(name, ts_from, ts_to)

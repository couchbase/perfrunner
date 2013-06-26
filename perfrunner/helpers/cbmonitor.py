from multiprocessing import Process
from uuid import uuid4

from cbagent.collectors import NSServer

from perfrunner.settings import CbAgentSettings


class CbAgent(object):

    def __init__(self, cluster_spec, target_iterator):
        settings = CbAgentSettings()
        settings.cluster = cluster_spec.name + uuid4().hex[:6]
        settings.ssh_username, settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        settings.rest_username, settings.rest_password = \
            cluster_spec.get_rest_credentials()

        self.collectors = []
        for target in target_iterator:
            settings.master_node = target.node.split(':')[0]
            for collector in (NSServer, ):
                self.collectors.append(collector(settings))

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        self.processes = [Process(c.collect) for c in self.collectors]
        map(lambda p: p.start(), self.processes)

    def stop(self):
        map(lambda p: p.terminate(), self.processes)

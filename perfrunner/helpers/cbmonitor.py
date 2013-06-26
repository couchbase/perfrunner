import time
from multiprocessing import Process
from uuid import uuid4

from cbagent.collectors import NSServer
from logger import logger

from perfrunner.settings import CbAgentSettings


class CbAgent(object):

    def __init__(self, cluster_spec, target_iterator):
        settings = CbAgentSettings()
        settings.cluster = '{0}_{1}'.format(cluster_spec.name, uuid4().hex[:6])
        settings.ssh_username, settings.ssh_password = \
            cluster_spec.get_ssh_credentials()
        settings.rest_username, settings.rest_password = \
            cluster_spec.get_rest_credentials()

        self.collectors = []
        for target in target_iterator:
            settings.master_node = target.node.split(':')[0]
            for collector in (NSServer, ):
                self.collectors.append(collector(settings))
        self.interval = settings.interval

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def run(self, collector):
        while True:
            try:
                collector.collect()
                time.sleep(self.interval)
            except Exception as e:
                logger.warn(e)

    def start(self):
        self.processes = [
            Process(target=self.run, args=(c, )) for c in self.collectors]
        map(lambda p: p.start(), self.processes)

    def stop(self):
        map(lambda p: p.terminate(), self.processes)

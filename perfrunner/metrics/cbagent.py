from collections import OrderedDict
from multiprocessing import Process
from pathlib import Path

import requests

from cbagent.metadata_client import MetadataClient
from cbagent.registry import CollectorRegistry
from cbagent.settings import CbAgentSettings
from cbagent.stores import PerfStore
from logger import logger
from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import CBMONITOR_HOST
from perfrunner.tests import PerfTest


class CbAgent:
    def __init__(self, test: PerfTest, phase: str):
        self.test = test
        if self.test.test_config.stats_settings.enabled:
            self.settings = CbAgentSettings(test)
            self.init_clusters(phase=phase)
            self.add_collectors(**test.COLLECTORS)
            self.update_metadata()
            self.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test.test_config.stats_settings.enabled:
            self.stop()
            self.reconstruct()
            # self.find_time_series()
            self.add_snapshots()
            self.cleanup_spring_worker_files()

    def init_clusters(self, phase: str):
        self.cluster_map = OrderedDict()

        for cluster_name, servers in self.test.cluster_spec.clusters:
            cluster_id = f"{cluster_name}_{self.test.build.replace('.', '')}_{phase}_{uhex()[:4]}"
            self.cluster_map[cluster_id] = servers[0]
        self.test.cbmonitor_clusters = list(self.cluster_map.keys())

    def add_collectors(self, **collector_flags):
        self.collectors = CollectorRegistry().get_active_collectors(
            self.test, self.cluster_map, **collector_flags
        )
        self.processes = []

    def update_metadata(self):
        for collector in self.collectors:
            collector.update_metadata()

    def start(self):
        logger.info("Starting stats collectors")
        self.processes = [Process(target=c.collect) for c in self.collectors]
        for p in self.processes:
            p.start()

    def stop(self):
        logger.info("Terminating stats collectors")
        for p in self.processes:
            p.terminate()

    def reconstruct(self):
        logger.info("Reconstructing measurements")
        for collector in self.collectors:
            if hasattr(collector, "reconstruct"):
                collector.reconstruct()

    def trigger_report(self, snapshot: str):
        url = f"http://{self.settings.cbmonitor_host}/reports/html/?snapshot={snapshot}"
        logger.info(f"HTML report: {url}")
        requests.get(url=url)

    def add_snapshots(self):
        self.test.cbmonitor_snapshots = []
        for cluster_id in self.test.cbmonitor_clusters:
            self.settings.cluster = cluster_id
            md_client = MetadataClient(self.settings)
            md_client.add_snapshot(cluster_id)
            self.trigger_report(cluster_id)

            self.test.cbmonitor_snapshots.append(cluster_id)

    def find_time_series(self):
        store = PerfStore(host=CBMONITOR_HOST)
        dbs = []
        for cluster_id in self.test.cbmonitor_clusters:
            dbs += store.find_dbs(cluster_id)
        logger.info(f"Time series: {pretty_dict(dbs)}")

    def cleanup_spring_worker_files(self):
        from cbagent.collectors.latency import KVLatency, QueryLatency
        for pattern in [KVLatency.PATTERN, QueryLatency.PATTERN]:
            for file in Path.cwd().glob(pattern):
                file.unlink()

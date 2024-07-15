import json
from concurrent.futures import ThreadPoolExecutor
from time import time

import requests

from cbagent.collectors import Collector
from cbagent.collectors.libstats.pool import Pool
from cbagent.metadata_client import MetadataClient
from cbagent.settings import CbAgentSettings
from cbagent.stores import PerfStore
from logger import logger
from perfrunner.helpers.misc import uhex
from perfrunner.settings import ClusterSpec
from spring.docgen import SGImportLatencyDocument


class SGImportLatency(Collector):
    COLLECTOR = "sgimport_latency"

    METRICS = "sgimport_latency"

    INITIAL_POLLING_INTERVAL = 0.001  # 1 ms

    TIMEOUT = 600

    MAX_SAMPLING_INTERVAL = 10  # 250 ms

    def __init__(self, settings: CbAgentSettings, cluster_spec: ClusterSpec):
        super().__init__(settings)
        self.cluster_spec = cluster_spec
        self.mc = MetadataClient(settings)
        self.store = PerfStore(settings.cbmonitor_host)
        self.interval = self.MAX_SAMPLING_INTERVAL
        self.cluster = settings.cluster
        self.pools: list[Pool] = []
        self.sg_host = next(cluster_spec.sgw_masters)
        self.sg_db = "db-1"
        self.new_docs = SGImportLatencyDocument(1024)

    def check_longpoll_changefeed(self, host: str, key: str, last_sequence: str):
        if self.cluster_spec.capella_infrastructure:
            api = f"https://{host}:4985/{self.sg_db}/_changes"
        else:
            api = f"http://{host}:4985/{self.sg_db}/_changes"

        data = {"feed": "longpoll", "since": f"{last_sequence}", "heartbeat": 360}

        try:
            response = requests.post(url=api, data=json.dumps(data))
        except Exception as ex:
            logger.info(f"longpoll request failed: {ex}")
            raise ex
        t1 = time()
        record_found = 0
        if response.status_code == 200:
            for record in response.json()["results"]:
                if record["id"] == key:
                    record_found += 1

            if record_found == 0:
                self.check_longpoll_changefeed(host=host, key=key, last_sequence=last_sequence)

        return t1

    def insert_doc(self, cb_client, key: str, doc: dict):
        cb_client.upsert(key, doc)
        return time()

    def get_lastsequence(self, host: str):
        if self.cluster_spec.capella_infrastructure:
            api = f"https://{host}:4985/{self.sg_db}/_changes"
        else:
            api = f"http://{host}:4985/{self.sg_db}/_changes"
        data = {"feed": "normal", "since": "0"}
        response = requests.post(url=api, data=json.dumps(data))
        last_sequence = response.json()["last_seq"]
        return last_sequence

    def measure(self, cb_client):
        key = f"sgimport_{uhex()}"
        doc = self.new_docs.next(key)
        last_sequence = self.get_lastsequence(host=self.sg_host)
        executor = ThreadPoolExecutor(max_workers=2)
        future1 = executor.submit(
            self.check_longpoll_changefeed, host=self.sg_host, key=key, last_sequence=last_sequence
        )
        future2 = executor.submit(self.insert_doc, cb_client=cb_client, key=key, doc=doc)
        t0 = future2.result()
        t1 = future1.result()
        return {"sgimport_latency": (t1 - t0)}

    def sample(self):
        for pool in self.pools:
            cb_client = pool.get_client()
            lags = self.measure(cb_client)
            self.store.append(lags, cluster=self.cluster, collector=self.COLLECTOR)
            pool.release_client(cb_client)

    def update_metadata(self):
        self.mc.add_cluster()
        self.mc.add_metric(self.METRICS, collector=self.COLLECTOR)

    def _init_pool(self):
        for bucket in self.get_buckets():
            self.pools.append(
                Pool(
                    bucket=bucket,
                    host=self.master_node,
                    username=self.auth[0],
                    password=self.auth[1],
                    ssl_mode="n2n" if self.n2n_enabled else "none",
                    kv_timeout=self.TIMEOUT,
                )
            )

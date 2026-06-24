
from time import sleep, time
from typing import Optional

import numpy

from cbagent.collectors.latency import Latency
from cbagent.collectors.libstats.pool import Pool
from cbagent.settings import CbAgentSettings
from logger import logger
from perfrunner.tests import PerfTest
from spring.docgen import Document, Key


class XdcrLag(Latency):

    COLLECTOR = "xdcr_lag"
    COLLECTOR_FLAG = "xdcr_lag"

    METRICS = "xdcr_lag",

    INITIAL_POLLING_INTERVAL = 0.001  # 1 ms

    TIMEOUT = 10  # 10 seconds

    MAX_SAMPLING_INTERVAL = 0.25  # 250 ms

    @staticmethod
    def _dest_master_node(test) -> Optional[str]:
        """Master of the XDCR destination cluster."""
        clusters = list(test.cluster_spec.clusters)
        if len(clusters) < 2:
            return None
        _, dest_servers = clusters[-1]
        return dest_servers[0]

    @classmethod
    def create_instances(cls, test: PerfTest, cluster_map: dict):
        # Only the source (first) cluster gets an XdcrLag instance.
        if not cluster_map:
            return []
        first_cluster_id, first_master = next(iter(cluster_map.items()))
        settings = CbAgentSettings(test)
        settings.cluster = first_cluster_id
        settings.master_node = first_master
        return [cls(settings, test)]

    def __init__(self, settings: CbAgentSettings, test: PerfTest):
        super().__init__(settings, test)
        self.dest_master_node = self._dest_master_node(test)
        self.interval = self.MAX_SAMPLING_INTERVAL
        self.new_docs = Document(test.test_config.access_settings.size)
        self.pools = []

    @staticmethod
    def gen_key() -> Key:
        return Key(number=numpy.random.random_integers(0, 10 ** 9),
                   prefix='xdcr',
                   fmtr='hex')

    def measure(self, src_pool: Pool, dst_pool: Pool):
        key = self.gen_key()
        doc = self.new_docs.next(key)
        polling_interval = self.INITIAL_POLLING_INTERVAL

        src_client = src_pool.get_client()
        dst_client = dst_pool.get_client()
        src_client.upsert(key.string, doc)

        t0 = time()
        while time() - t0 < self.TIMEOUT:
            if dst_client.get(key.string):
                break
            sleep(polling_interval)
            polling_interval *= 1.05  # increase interval by 5%
        else:
            logger.warn(f"XDCR sampling timed out after {self.TIMEOUT} seconds")
        t1 = time()

        src_client.delete(key.string)
        dst_client.delete(key.string)
        src_pool.release_client(src_client)
        dst_pool.release_client(dst_client)

        return {'xdcr_lag': (t1 - t0) * 1000}  # s -> ms

    def sample(self):
        for bucket, src_pool, dst_pool in self.pools:
            lags = self.measure(src_pool, dst_pool)
            self.store.append(lags, cluster=self.cluster, bucket=bucket, collector=self.COLLECTOR)

    def _init_pool(self):
        params = {
            "username": self.auth[0],
            "password": self.auth[1],
            "ssl_mode": "n2n" if self.n2n_enabled else "none",
            "initial": 20,
            "max_clients": 40,
        }
        for bucket in self.get_buckets():
            target_collections = []
            if self.collections:
                for scope, collections in self.collections[bucket].items():
                    for collection, options in collections.items():
                        if options.get("access", 0):
                            target_collections.append((scope, collection))

            src_pool = Pool(
                bucket=bucket,
                host=self.master_node,
                target_collections=target_collections,
                **params,
            )
            dst_pool = Pool(
                bucket=bucket,
                host=self.dest_master_node,
                target_collections=target_collections,
                **params,
            )
            self.pools.append((bucket, src_pool, dst_pool))

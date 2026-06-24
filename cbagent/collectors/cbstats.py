import json
from typing import Optional

from cbagent.collectors.collector import CouchbaseCollector
from logger import logger
from perfrunner.helpers.local import extract_cb_any, run_cbstats
from perfrunner.tests import PerfTest


class CBStatsMemory(CouchbaseCollector):
    COLLECTOR = "cbstats_memory"
    COLLECTOR_FLAG = "cbstats_memory"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True
    CB_STATS_PORT = 11209
    METRICS = (
        "ep_mem_used_primary"
    )

    def __init__(self, settings, test: Optional[PerfTest] = None):
        super().__init__(settings)
        extract_cb_any(filename="couchbase")

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            uname, pwd = self.auth
            stdout, returncode = run_cbstats("memory", server, self.CB_STATS_PORT, uname, pwd,
                                             bucket)
            if returncode != 0:
                logger.warning("CBStatsMemory failed to get memory stats from server: {}"
                               .format(server))
                return stats

            data = json.loads(stdout)
            for metric, value in data.items():
                if metric in self.METRICS:
                    if metric in stats:
                        stats[metric] += value
                    else:
                        stats[metric] = value
        except Exception:
            pass

        return stats

    def _get_memory_stats(self, bucket: str, server: str):
        node_stats = self._get_stats_from_server(bucket, server=server)
        return node_stats

    def sample(self):
        for bucket in self.get_buckets():
            stats = {}
            for node in self.nodes:
                temp_stats = self._get_memory_stats(bucket, node)
                for st in temp_stats:
                    if st in stats:
                        stats[st] += temp_stats[st]
                    else:
                        stats[st] = temp_stats[st]

            if stats:
                self.update_metric_metadata(stats.keys(), bucket=bucket)
                self.store.append(
                    stats, cluster=self.cluster, bucket=bucket, collector=self.COLLECTOR
                )

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for node in self.nodes:
            self.mc.add_server(node)


class CBStatsAll(CouchbaseCollector):
    COLLECTOR = "cbstats_all"
    COLLECTOR_FLAG = "cbstats_all"
    SKIP_ON_DYNAMIC = True
    REQUIRES_ON_PREM = True
    REQUIRES_NON_CYGWIN = True
    CB_STATS_PORT = 11209
    METRICS = (
        "mem_used_secondary",
        "ep_magma_total_mem_used",
        "ep_magma_mem_used_diff",
        "ep_magma_data_blocks_uncompressed_size",
        "ep_magma_data_blocks_compressed_size",
        "ep_magma_data_blocks_compression_ratio",
        "ep_magma_data_blocks_space_reduction_estimate_pct"
    )
    METRICS_AVERAGE_PER_NODE = (
        "ep_magma_data_blocks_compression_ratio",
        "ep_magma_data_blocks_space_reduction_estimate_pct"
    )

    def __init__(self, settings, test: Optional[PerfTest] = None):
        super().__init__(settings)
        extract_cb_any(filename="couchbase")

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            uname, pwd = self.auth
            stdout, returncode = run_cbstats("all", server, self.CB_STATS_PORT, uname, pwd, bucket)
            if returncode != 0:
                logger.warning("CBStatsAll failed to get stats from server: {}".format(server))
                return stats

            data = json.loads(stdout)

            for metric, value in data.items():
                if metric in self.METRICS:
                    if metric in stats:
                        stats[metric] += value
                    else:
                        stats[metric] = value

            ep_magma_mem_used_diff = stats['mem_used_secondary'] - stats['ep_magma_total_mem_used']
            if 'ep_magma_mem_used_diff' in stats:
                stats['ep_magma_mem_used_diff'] += ep_magma_mem_used_diff
            else:
                stats['ep_magma_mem_used_diff'] = ep_magma_mem_used_diff
        except Exception:
            pass

        return stats

    def _get_cbstats_all_stats(self, bucket: str, server: str):
        node_stats = self._get_stats_from_server(bucket, server=server)
        return node_stats

    def sample(self):
        for bucket in self.get_buckets():
            stats = {}
            for node in self.nodes:
                temp_stats = self._get_cbstats_all_stats(bucket, node)
                for st in temp_stats:
                    if st in stats:
                        stats[st] += temp_stats[st]
                    else:
                        stats[st] = temp_stats[st]

            if stats:
                for metric in self.METRICS_AVERAGE_PER_NODE:
                    if metric in stats:
                        stats[metric] /= len(self.nodes)
                self.update_metric_metadata(stats.keys(), bucket=bucket)
                self.store.append(
                    stats, cluster=self.cluster, bucket=bucket, collector=self.COLLECTOR
                )

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for node in self.nodes:
            self.mc.add_server(node)

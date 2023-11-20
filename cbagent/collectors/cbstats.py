import json

from cbagent.collectors.collector import Collector
from logger import logger
from perfrunner.helpers.local import extract_cb_any, run_cbstats


class CBStatsMemory(Collector):
    COLLECTOR = "cbstats_memory"
    CB_STATS_PORT = 11209
    METRICS = (
        "ep_mem_used_primary"
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        extract_cb_any(filename='couchbase')
        self.cluster_spec = test.cluster_spec

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            uname, pwd = self.cluster_spec.rest_credentials
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
                self.append_to_store(stats, cluster=self.cluster,
                                     bucket=bucket,
                                     collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for node in self.nodes:
            self.mc.add_server(node)


class CBStatsAll(Collector):
    COLLECTOR = "cbstats_all"
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

    def __init__(self, settings, test):
        super().__init__(settings)
        extract_cb_any(filename='couchbase')
        self.cluster_spec = test.cluster_spec

    def _get_stats_from_server(self, bucket: str, server: str):
        stats = {}
        try:
            uname, pwd = self.cluster_spec.rest_credentials
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
                self.append_to_store(stats, cluster=self.cluster,
                                     bucket=bucket,
                                     collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()

        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for node in self.nodes:
            self.mc.add_server(node)

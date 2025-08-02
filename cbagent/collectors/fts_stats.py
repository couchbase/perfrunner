from cbagent.collectors.collector import Collector
from perfrunner.helpers.rest import RestHelper


class FTSCollector(Collector):

    COLLECTOR = "fts_stats"

    METRICS = (
        "doc_count",
        "num_bytes_used_disk",
        "num_bytes_used_ram",
        "num_mutations_to_index",
        "num_pindexes",
        "num_pindexes_actual",
        "num_pindexes_target",
        "num_recs_to_persist",
        "num_files_on_disk",
        "num_root_memorysegments",
        "num_root_filesegments",
        "pct_cpu_gc",
        "timer_batch_store_count",
        "timer_data_delete_count",
        "timer_data_update_count",
        "timer_opaque_get_count",
        "timer_opaque_set_count",
        "timer_rollback_count",
        "timer_snapshot_start_count",
        "total_bytes_indexed",
        "total_bytes_query_results",
        "total_gc",
        "total_queries",
        "total_queries_error",
        "total_queries_slow",
        "total_queries_timeout",
        "total_request_time",
        "total_term_searchers",
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        self.cbft_stats = dict()
        self.fts_index_name = "{}-0".format(test.jts_access.couchbase_index_name)
        self.fts_index_map = test.jts_access.fts_index_map
        self.allbuckets = [x for x in self.get_buckets()]
        self.fts_nodes = test.fts_nodes
        self.rest = RestHelper(test.cluster_spec, self.n2n_enabled)

    def collect_stats(self):
        for host in self.fts_nodes:
            self.cbft_stats[host] = self.rest.get_fts_stats(host)

    def get_fts_stats_by_name(self, host, bucket, index, name):
        if name in self.cbft_stats[host]:
            return self.cbft_stats[host][name]

        key = "{}:{}:{}".format(bucket, index, name)
        if key in self.cbft_stats[host]:
            return self.cbft_stats[host][key]
        return 0

    def measure(self):
        stats = dict()
        for metric in self.METRICS:
            for host in self.fts_nodes:
                if host not in stats:
                    stats[host] = dict()
                stats[host][metric] = 0
                for index in self.fts_index_map.keys():
                    stats[host][metric] += \
                        self.get_fts_stats_by_name(host,
                                                   self.fts_index_map[index]['bucket'],
                                                   self.fts_index_map[index]['full_index_name']
                                                   if 'full_index_name' in
                                                   self.fts_index_map[index].keys() else index,
                                                   metric)
        return stats

    def update_metadata(self):
        self.mc.add_cluster()
        for metric in self.METRICS:
            for host in self.fts_nodes:
                self.mc.add_metric(metric, server=host, collector=self.COLLECTOR)

    def sample(self):
        self.collect_stats()
        self.update_metric_metadata(self.METRICS)
        samples = self.measure()
        for host in self.fts_nodes:
            if host in samples:
                self.store.append(samples[host],
                                  cluster=self.cluster,
                                  server=host,
                                  collector=self.COLLECTOR)


class ElasticStats(FTSCollector):

    COLLECTOR = "fts_latency"

    METRICS = ("elastic_latency_get", "elastic_cache_size", "elastic_query_total",
               "elastic_cache_hit", "elastic_filter_cache_size",)

    def __init__(self, settings, test):
        super().__init__(settings, test)
        self.interval = settings.lat_interval
        self.host = settings.master_node

    def collect_stats(self):
        self.cbft_stats = self.rest.get_elastic_stats(self.host)

    def elastic_query_total(self):
        return self.cbft_stats["_all"]["total"]["search"]["query_total"]

    def elastic_cache_size(self):
        return self.cbft_stats["_all"]["total"]["query_cache"]["memory_size_in_bytes"]

    def elastic_cache_hit(self):
        return self.cbft_stats["_all"]["total"]["query_cache"]["hit_count"]

    def elastic_filter_cache_size(self):
        return self.cbft_stats["_all"]["total"]["filter_cache"]["memory_size_in_bytes"]

    def elastic_active_search(self):
        return self.cbft_stats["_all"]["total"]["search"]["open_contexts"]

    def measure(self):
        stats = {}
        for metric in self.METRICS:
            latency = getattr(self, metric)()
            if latency:
                stats[metric] = latency
        return stats

    def update_metadata(self):
        self.mc.add_cluster()
        for metric in self.METRICS:
            self.mc.add_metric(metric, collector=self.COLLECTOR)

    def sample(self):
        self.collect_stats()
        if self.cbft_stats:
            self.update_metric_metadata(self.METRICS)
            samples = self.measure()
            if samples:
                self.store.append(samples, cluster=self.cluster,
                                  collector=self.COLLECTOR)

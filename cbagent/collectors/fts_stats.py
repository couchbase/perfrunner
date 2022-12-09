from cbagent.collectors.collector import Collector
from perfrunner.helpers import rest


class FTSCollector(Collector):

    COLLECTOR = "fts_stats"

    METRICS = (
        "batch_merge_count",
        "doc_count",
        "iterator_next_count",
        "iterator_seek_count",
        "num_bytes_live_data",
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
        "reader_get_count",
        "reader_multi_get_count",
        "reader_prefix_iterator_count",
        "reader_range_iterator_count",
        "timer_batch_store_count",
        "timer_data_delete_count",
        "timer_data_update_count",
        "timer_opaque_get_count",
        "timer_opaque_set_count",
        "timer_rollback_count",
        "timer_snapshot_start_count",
        "total_bytes_indexed",
        "total_bytes_query_results",
        "total_compactions",
        "total_gc",
        "total_queries",
        "total_queries_error",
        "total_queries_slow",
        "total_queries_timeout",
        "total_request_time",
        "total_term_searchers",
        "writer_execute_batch_count",
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        self.cbft_stats = dict()
        self.fts_index_name = "{}-0".format(test.access.couchbase_index_name)
        self.fts_index_map = test.access.fts_index_map
        self.allbuckets = [x for x in self.get_buckets()]
        self.fts_nodes = test.fts_nodes
        self.rest = rest.RestHelper(test.cluster_spec, test.test_config)

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


class RegulatorStats(Collector):

    COLLECTOR = "regulator_stats"

    METRICS = (
        "total_RUs_metered",
        "total_WUs_metered",
        "total_read_ops_capped",
        "total_read_ops_rejected",
        "total_write_ops_rejected",
        "total_read_throttle_seconds",
        "total_write_throttle_seconds",
        "total_read_ops_metering_errs",
        "total_write_ops_metering_errs",
        "total_ops_timed_out_while_metering",
        "total_batch_limting_timeouts",
        "total_batch_rejection_backoff_time_ms"
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        self.cbft_stats = dict()
        self.fts_index_name = "{}-0".format(test.access.couchbase_index_name)
        self.fts_index_map = test.access.fts_index_map
        self.allbuckets = [x for x in self.get_buckets()]
        self.fts_nodes = test.fts_nodes
        self.rest = rest.RestHelper(test.cluster_spec, test.test_config)

    def collect_stats(self):
        for host in self.fts_nodes:
            self.cbft_stats[host] = self.rest.get_fts_stats(host)

    def get_regulator_stats_by_name(self, host, bucket, name):
        if name in self.cbft_stats[host]:
            return self.cbft_stats[host][name]

        key = "regulatorStats:{}".format(bucket)
        if key in self.cbft_stats[host]:
            if name in self.cbft_stats[host][key]:
                return self.cbft_stats[host][key][name]
        return 0

    def measure(self):
        stats = dict()
        for metric in self.METRICS:
            for host in self.fts_nodes:
                if host not in stats:
                    stats[host] = dict()
                for bucket in self.allbuckets:
                    if bucket not in stats[host]:
                        stats[host][bucket] = dict()
                    stats[host][bucket][metric] = \
                        self.get_regulator_stats_by_name(host,
                                                         bucket,
                                                         metric)
        return stats

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
        for host in self.fts_nodes:
            self.mc.add_server(host)

    def sample(self):
        self.collect_stats()

        samples = self.measure()
        for host in self.fts_nodes:
            if host in samples:
                for bucket in self.allbuckets:
                    if bucket in samples[host]:
                        self.update_metric_metadata(self.METRICS, bucket=bucket, server=host)
                        self.store.append(samples[host][bucket],
                                          cluster=self.cluster,
                                          bucket=bucket,
                                          server=host,
                                          collector=self.COLLECTOR)

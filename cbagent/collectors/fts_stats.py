import time

from cbagent.collectors import Collector
from perfrunner.helpers import rest
from spring.cbgen import ElasticGen, FtsGen


class FTSCollector(Collector):

    COLLECTOR = "fts_stats"

    METRICS = ("batch_merge_count",
               "doc_count",
               "iterator_next_count",
               "iterator_seek_count",
               "num_bytes_live_data",
               "num_bytes_used_disk",
               "num_mutations_to_index",
               "num_pindexes",
               "num_pindexes_actual",
               "num_pindexes_target",
               "num_recs_to_persist",
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
               "total_queries",
               "total_queries_error",
               "total_queries_slow",
               "total_queries_timeout",
               "total_request_time",
               "total_term_searchers",
               "writer_execute_batch_count",
               "num_bytes_used_ram",
               "pct_cpu_gc",
               "total_gc",
               )

    def __init__(self, settings, test):
        super(FTSCollector, self).__init__(settings)
        self.cbft_stats = dict()
        self.fts_settings = test.test_config.fts_settings
        self.fts_index_name = test.fts_index
        self.allbuckets = [x for x in self.get_buckets()]
        self.active_fts_hosts = test.active_fts_hosts
        self.master_node = settings.master_node
        self.rest = rest.RestHelper(test.cluster_spec)
        self.test = test
        self.fts_client = self.init_client(test.test_config)

    def init_client(self, test_config):
        return FtsGen(self.master_node, test_config.fts_settings, self.test.auth)

    def collect_stats(self):
        for host in self.active_fts_hosts:
            self.cbft_stats[host] = self.rest.get_fts_stats(host)

    def get_fts_stats_by_name(self, host, bucket, index, name):
        key = "{}:{}:{}".format(bucket, index, name)
        if key not in self.cbft_stats[host]:
            return 0
        return self.cbft_stats[host][key]

    def measure(self):
        stats = dict()
        for metric in self.METRICS:
            for host in self.active_fts_hosts:
                stats[host] = dict()
                stats[host][metric] = self.get_fts_stats_by_name(host,
                                                                 self.allbuckets[0],
                                                                 self.fts_index_name,
                                                                 metric)
        return stats

    def update_metadata(self):
        self.mc.add_cluster()
        for metric in self.METRICS:
            for host in self.active_fts_hosts:
                self.mc.add_metric(metric, server=host, collector=self.COLLECTOR)

    def sample(self):
            self.collect_stats()
            self.update_metric_metadata(self.METRICS)
            samples = self.measure()
            for host in self.active_fts_hosts:
                if host in samples:
                    self.store.append(samples[host], cluster=self.cluster, server=host, collector=self.COLLECTOR)

    def check_total_hits(self, r):
        if self.fts_client.settings.elastic:
            return r.json()["hits"]["total"] != 0
        return r.json()['total_hits'] != 0

    def measure_latency(self, client):
        cmd, query = client.next()
        t0 = time.time()
        r = cmd(**query)
        t1 = time.time()
        if r.status_code in range(200, 203) and self.check_total_hits(r):
            return 1000 * (t1 - t0)
        return 0


class FTSTotalsCollector (FTSCollector):
    COLLECTOR = "fts_totals"

    def update_metadata(self):
        self.mc.add_cluster()
        for metric in self.METRICS:
            self.mc.add_metric(metric, collector=self.COLLECTOR)

    def sample(self):
            self.collect_stats()
            self.update_metric_metadata(self.METRICS)
            samples = dict()
            samples["totals"] = dict()
            hosts_samples = self.measure()
            for host in self.active_fts_hosts:
                if host in hosts_samples:
                    for metric in self.METRICS:
                        if metric not in samples["totals"]:
                            samples["totals"][metric] = 0
                        if metric in hosts_samples[host]:
                            samples["totals"][metric] += hosts_samples[host][metric]
            self.store.append(samples["totals"], cluster=self.cluster, collector=self.COLLECTOR)


class FTSLatencyCollector (FTSTotalsCollector):
    COLLECTOR = "fts_latency"

    METRICS = ("cbft_latency_get")

    def __init__(self, settings, test):
        super(FTSLatencyCollector, self).__init__(settings, test)
        self.interval = settings.lat_interval

    def sample(self):
            self.collect_stats()
            self.update_metric_metadata(self.METRICS)
            samples = self.measure()
            if samples:
                self.store.append(samples, cluster=self.cluster, collector=self.COLLECTOR)

    def measure(self):
        stats = dict()
        stats["cbft_latency_get"] = self.measure_latency(self.fts_client)
        return stats


class ElasticStats(FTSCollector):

    COLLECTOR = "fts_latency"

    METRICS = ("elastic_latency_get", "elastic_cache_size", "elastic_query_total",
               "elastic_cache_hit", "elastic_filter_cache_size")

    def __init__(self, settings, test):
        super(ElasticStats, self).__init__(settings, test)
        self.interval = settings.lat_interval
        self.host = settings.master_node

    def init_client(self, test_config):
        return ElasticGen(self.host, test_config.fts_settings)

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

    def elastic_latency_get(self):
        return self.measure_latency(self.fts_client)

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
        if (self.cbft_stats):
            self.update_metric_metadata(self.METRICS)
            samples = self.measure()
            if samples:
                self.store.append(samples, cluster=self.cluster,
                                  collector=self.COLLECTOR)

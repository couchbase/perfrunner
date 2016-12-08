import time
from math import pow

from cbagent.collectors import Collector
from spring.cbgen import ElasticGen, FtsGen


class FtsCollector(Collector):

    METRICS = ()

    def __init__(self, settings, test_config):
        super(FtsCollector, self).__init__(settings)
        self.cbft_stats = None
        self.fts_settings = test_config.fts_settings
        self.host = settings.master_node
        self.fts_client = self.init_client(test_config)

    def init_client(self, test_config):
        return FtsGen(self.host, test_config.fts_settings, self.auth)

    def collect_stats(self):
        r = self.session.get("http://{}:{}/api/nsstats".format(self.host, self.fts_settings.port))
        if r.status_code == 200:
            self.cbft_stats = r.json()
        else:
            self.cbft_stats = None

    def cbft_stats_get(self, key):
        if key not in self.cbft_stats:
            return 0
        return self.cbft_stats[key]

    def cbft_pct_cpu_gc(self):
        return self.cbft_stats_get("pct_cpu_gc")

    def cbft_num_bytes_used_ram(self):
        return self.cbft_stats_get("num_bytes_used_ram")

    def cbft_total_gc(self):
        return self.cbft_stats_get("total_gc")

    def measure(self):
        stats = {}
        for metric in self.METRICS:
            # the getattr is used to make code simple
            # the metric name should be same as the method name
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
            self.update_metric_metadata(self.METRICS)
            samples = self.measure()
            if samples:
                self.store.append(samples, cluster=self.cluster,
                                  collector=self.COLLECTOR)

    def check_total_hits(self, r):
        if self.fts_client.settings.elastic:
            return r.json()["hits"]["total"] != 0
        return r.json()['total_hits'] != 0


class FtsLatency(FtsCollector):

    COLLECTOR = "fts_latency"

    METRICS = ("cbft_latency_get", )

    def __init__(self, settings, test_config):
        super(FtsLatency, self).__init__(settings, test_config)

    def cbft_latency_get(self):
        cmd, query = self.fts_client.next()
        t0 = time.time()
        r = cmd(**query)
        t1 = time.time()
        if r.status_code in range(200, 203) and self.check_total_hits(r):
            return 1000 * (t1 - t0)
        return 0


class FtsStats(FtsCollector):

    COLLECTOR = "fts_stats"

    METRICS = ("cbft_doc_count", "cbft_num_bytes_used_disk",
               "cbft_num_bytes_used_ram", "cbft_pct_cpu_gc",
               "cbft_batch_merge_count", "cbft_total_gc",
               "cbft_num_bytes_live_data", "cbft_total_bytes_indexed",
               "cbft_num_recs_to_persist", )

    def __init__(self, settings, test_config):
        super(FtsStats, self).__init__(settings, test_config)
        for bucket in self.get_buckets():
            self.disk_key = bucket + ':' + test_config.fts_settings.name + \
                ":num_bytes_used_disk"
            self.count_key = bucket + ':' + test_config.fts_settings.name + \
                ":doc_count"
            self.batch_count = bucket + ':' + test_config.fts_settings.name + \
                ":batch_merge_count"
            self.live_data = bucket + ':' + test_config.fts_settings.name + \
                ":num_bytes_live_data"
            self.power = pow(10, 9)
            self.bytes_index = bucket + ':' + test_config.fts_settings.name + \
                ":cbft_total_bytes_indexed"
            self.recs_presist = bucket + ':' + test_config.fts_settings.name + \
                ":num_recs_to_persist"

    def cbft_batch_merge_count(self):
        return self.cbft_stats_get(self.live_data)

    def cbft_num_bytes_live_data(self):
        return self.cbft_stats_get(self.batch_count)

    def cbft_num_bytes_used_disk(self):
        return float(self.cbft_stats_get(self.disk_key) / self.power)

    def cbft_doc_count(self):
        return self.cbft_stats_get(self.count_key)

    def cbft_total_bytes_indexed(self):
        return self.cbft_stats_get(self.bytes_index)

    def cbft_num_recs_to_persist(self):
        return self.cbft_stats_get(self.recs_presist)


class FtsQueryStats(FtsLatency):

    COLLECTOR = "fts_query_stats"

    METRICS = ("cbft_query_slow", "cbft_query_timeout",
               'cbft_query_error', "cbft_total_term_searchers",
               "cbft_query_total", "cbft_total_bytes_query_results",
               "cbft_writer_execute_batch_count")

    def __init__(self, settings, test_config):
        super(FtsQueryStats, self).__init__(settings, test_config)

        # following is to add different query stats'
        for bucket in self.get_buckets():
            self.total_queries = bucket + ':' + test_config.fts_settings.name + \
                ":total_queries"
            self.total_queries_slow = bucket + ':' + test_config.fts_settings.name + \
                ":total_queries_slow"
            self.total_queries_timeout = bucket + ':' + test_config.fts_settings.name + \
                ":total_queries_timeout"
            self.total_queries_error = bucket + ':' + test_config.fts_settings.name + \
                ":total_queries_error"
            self.total_term_searchers = bucket + ':' + test_config.fts_settings.name + \
                ":total_term_searchers"
            self.total_query_bytes = bucket + ':' + test_config.fts_settings.name + \
                ":total_bytes_query_results"
            self.total_write_batch = bucket + ':' + test_config.fts_settings.name + \
                ":writer_execute_batch_count"

    def cbft_writer_execute_batch_count(self):
        return self.cbft_stats_get(self.total_write_batch)

    def cbft_total_term_searchers(self):
        return self.cbft_stats_get(self.total_term_searchers)

    def cbft_total_bytes_query_results(self):
        return self.cbft_stats_get(self.total_query_bytes)

    def cbft_query_slow(self):
        return self.cbft_stats_get(self.total_queries_slow)

    def cbft_query_total(self):
        return self.cbft_stats_get(self.total_queries)

    def cbft_query_timeout(self):
        return self.cbft_stats_get(self.total_queries_timeout)

    def cbft_query_error(self):
        return self.cbft_stats_get(self.total_queries_error)


class ElasticStats(FtsCollector):

    COLLECTOR = "fts_latency"

    METRICS = ("elastic_latency_get", "elastic_cache_size", "elastic_query_total",
               "elastic_cache_hit", "elastic_filter_cache_size")

    def __init__(self, settings, test_config):
        super(ElasticStats, self).__init__(settings, test_config)
        self.host = settings.master_node

    def init_client(self, test_config):
        return ElasticGen(self.host, test_config.fts_settings)

    def collect_stats(self):
        r = self.session.get("http://{}:9200/_stats".format(self.host, self.fts_settings.port))
        if r.status_code == 200:
            self.cbft_stats = r.json()
        else:
            self.cbft_stats = None

    def cbft_query_total(self):
        if self.cbft_stats:
            return self.cbft_stats["_all"]["total"]["search"]["query_total"]

    def elastic_query_total(self):
        return self.cbft_query_total()

    def elastic_cache_size(self):
        if self.cbft_stats:
            return self.cbft_stats["_all"]["total"]["query_cache"]["memory_size_in_bytes"]

    def elastic_cache_hit(self):
        return self.cbft_stats["_all"]["total"]["query_cache"]["hit_count"]

    def elastic_filter_cache_size(self):
        return self.cbft_stats["_all"]["total"]["filter_cache"]["memory_size_in_bytes"]

    def elastic_active_search(self):
        return self.cbft_stats["_all"]["total"]["search"]["open_contexts"]

    def elastic_latency_get(self):
        cmd, query = self.fts_client.next()
        t0 = time.time()
        r = cmd(**query)
        t1 = time.time()
        if r.status_code in range(200, 203) and self.check_total_hits(r):
            return 1000 * (t1 - t0)
        return 0

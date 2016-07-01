import time
from logger import logger

import json
from cbagent.collectors import Collector
from cbagent.collectors import Latency
from spring.cbgen import FtsGen, ElasticGen
from math import pow


class FtsCollector(Collector):
    def __init__(self, settings, test_config, prefix=None):
            super(FtsCollector, self).__init__(settings)
            self.host = settings.master_node
            self.fts_client = FtsGen(self.host, test_config.fts_settings)
            self.cbft_stats = None
            self.NO_RESULT = 0
            self.scmd, self.squery = self.fts_client.prepare_query(type="nsstats")

    def collect_stats(self):
        r = self.scmd(**self.squery)
        '''
         The nsserver stats not exposed
         following fundtion is added to collect nsserver stats per METRICS,
         no need to call same stats for different metrics. Optimized and reliable code.
         collect_stats should be called once.
        '''
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
            stats={}
            for metric in self.METRICS:
                '''
                the getattr is used to make code simple , avoid using many function
                the metric name should be same as the method name
                '''
                stats[metric] = getattr(self, metric)()
            return stats

    def update_metadata(self):
        self.mc.add_cluster()
        for metric in self.METRICS:
                self.mc.add_metric(metric, collector=self.COLLECTOR)

    def sample(self):
            self.collect_stats()
            self.update_metric_metadata(self.METRICS)
            samples = self.measure()
            self.store.append(samples, cluster=self.cluster,
                              collector=self.COLLECTOR)


class FtsLatency(FtsCollector):

    COLLECTOR = "fts_latency"
    METRICS = ("cbft_latency_get", )

    def __init__(self, settings, test_config, prefix=None):
        super(FtsLatency, self).__init__(settings, test_config)
        self.fts_client.prepare_query()

    def cbft_latency_get(self):
        cmd, query = self.fts_client.next()
        t0 = time.time()
        cmd(**query)
        return 1000 * (time.time() - t0)


class FtsStats(FtsCollector):

    COLLECTOR = "fts_stats"
    METRICS = ("cbft_doc_count", "cbft_num_bytes_used_disk",
               "cbft_num_bytes_used_ram", "cbft_pct_cpu_gc", "cbft_batch_merge_count",
                    "cbft_total_gc", "cbft_num_bytes_live_data")

    def __init__(self, settings, test_config, prefix=None):
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

    def cbft_batch_merge_count(self):
        return self.cbft_stats_get(self.live_data)

    def cbft_num_bytes_live_data(self):
        return self.cbft_stats_get(self.batch_count)

    def cbft_num_bytes_used_disk(self):
        return float(self.cbft_stats_get(self.disk_key) / self.power)

    def cbft_doc_count(self):
        return self.cbft_stats_get(self.count_key)


class FtsQueryStats(FtsLatency):

    COLLECTOR = "fts_query_stats"
    METRICS = ("cbft_query_slow", "cbft_query_timeout",
               'cbft_query_error', "cbft_total_term_searchers",
               "cbft_query_total", "cbft_total_bytes_query_results",
                "cbft_writer_execute_batch_count")

    def __init__(self, settings, test_config, prefix=None):
        super(FtsQueryStats, self).__init__(settings, test_config)
        '''
         following is to add different query stats'
        '''
        for bucket in self.get_buckets():
            self.total_queries = bucket + ':' + test_config.fts_settings.name \
                            + ":total_queries"
            self.total_queries_slow = bucket + ':' + test_config.fts_settings.name \
                                      + ":total_queries_slow"
            self.total_queries_timeout = bucket +':' + test_config.fts_settings.name \
                                         + ":total_queries_timeout"
            self.total_queries_error = bucket + ':' + test_config.fts_settings.name \
                                       + ":total_queries_error"
            self.total_term_searchers = bucket + ':' + test_config.fts_settings.name \
                                       + ":total_term_searchers"
            self.total_query_bytes = bucket + ':' + test_config.fts_settings.name \
                                       + ":total_bytes_query_results"
            self.total_write_batch = bucket + ':' + test_config.fts_settings.name \
                                       + ":writer_execute_batch_count"

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

    COLLECTOR = "elastic_latency"

    METRICS = ("elastic_latency_get",)

    def __init__(self, settings, test_config, prefix=None):
        super(ElasticStats, self).__init__(settings, test_config)
        self.host = settings.master_node
        self.elastic_client = ElasticGen(self.host, test_config.fts_settings)
        self.elastic_client.prepare_query()

    def elastic_latency_get(self):
        cmd, query = self.elastic_client.next()
        t0 = time.time()
        cmd(**query)
        return 1000 * (time.time() - t0)

import time

from cbagent.collectors.collector import CouchbaseCollector
from perfrunner.helpers.local_stats import consolidate_jts_log
from perfrunner.tests import PerfTest


class JTSCollector(CouchbaseCollector):
    results = dict()

    COLLECTOR = "jts_stats"
    COLLECTOR_FLAG = "jts_stats"
    SKIP_ON_DYNAMIC = True

    # jts_throughput/jts_latency are computed from local JTS log files and pushed
    # to Prometheus in reconstruct(), so opt in to the custom Prometheus path
    PROMETHEUS_CUSTOM = True

    METRICS = ("jts_throughput", "jts_latency",)

    def __init__(self, settings, test: PerfTest):
        super().__init__(settings)
        self.settings = test.jts_access

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.buckets:
            self.mc.add_bucket(bucket)
            for metric in self.METRICS:
                self.mc.add_metric(metric, bucket=bucket,
                                   collector=self.COLLECTOR)

    def _consolidate_results(self, filename_pattern: str, storage_name: str):
        self.results[storage_name] = dict()
        for bucket in self.buckets:
            new_filename = filename_pattern
            if self.settings.logging_method == "bucket_wise":
                new_filename = bucket + "_" + filename_pattern
            # Shared with MetricHelper's local JTS compute so the pushed values and
            # the locally-computed KPI can't drift.
            self.results[storage_name][bucket] = consolidate_jts_log(
                self.settings.jts_logs_dir, new_filename, storage_name == "latency"
            )

    def sample(self):
        pass

    def custom_bucket_list(self):
        return [
                'bucket-{}'.format(i + 1) for i in range(int(self.settings.custom_num_buckets))
            ]

    def read_stats(self):
        self._consolidate_results("aggregated_throughput.log", "throughput")
        self._consolidate_results("aggregated_latency.log", "latency")

    def reconstruct(self):
        if int(self.settings.custom_num_buckets) > 0:
            self.buckets = self.custom_bucket_list()

        timestamp_offset = round(time.time() * 1000)
        self.read_stats()

        for bucket in self.buckets:
            if "throughput" in self.results:
                for k in self.results["throughput"][bucket].keys():
                    data = {
                        'jts_throughput': float(self.results["throughput"][bucket][k])
                    }
                    self.store.append(
                        data=data,
                        timestamp=timestamp_offset
                        + int(k) * int(self.settings.aggregation_buffer_ms),
                        cluster=self.cluster,
                        bucket=bucket,
                        collector=self.COLLECTOR,
                    )

            if "latency" in self.results:
                for k in self.results["latency"][bucket].keys():
                    data = {
                        'jts_latency': float(self.results["latency"][bucket][k])
                    }
                    self.store.append(
                        data=data,
                        timestamp=timestamp_offset + int(k) * 1000,
                        cluster=self.cluster,
                        bucket=bucket,
                        collector=self.COLLECTOR,
                    )

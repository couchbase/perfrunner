import time
from glob import glob

from cbagent.collectors.collector import Collector


class JTSCollector(Collector):
    results = dict()

    COLLECTOR = "jts_stats"
    METRICS = ("jts_throughput", "jts_latency",)

    def __init__(self, settings, test):
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
            all_results = dict()
            self.results[storage_name][bucket] = dict()
            new_filename = filename_pattern
            if self.settings.logging_method == "bucket_wise":
                new_filename = bucket + "_" + filename_pattern
            for file in glob("{}/*/{}".format(self.settings.jts_logs_dir, new_filename)):
                f = open(file)
                lines = f.readlines()
                for line in lines:
                    kv = line.split(":")
                    k = 0
                    v = 0
                    if len(kv) > 0:
                        k = int(kv[0])
                        if len(kv) > 1:
                            v = float(kv[1].rstrip('\n'))
                        else:
                            v = 0
                    if k not in all_results:
                        all_results[k] = list()
                    all_results[k].append(v)

            for k in all_results.keys():
                self.results[storage_name][bucket][k] = 0
                for v in all_results[k]:
                    self.results[storage_name][bucket][k] += float(v)
                if storage_name == "latency":
                    self.results[storage_name][bucket][k] /= len(all_results[k])

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
                    self.append_to_store(data=data,
                                         timestamp=timestamp_offset +
                                         int(k) * int(self.settings.aggregation_buffer_ms),
                                         cluster=self.cluster, bucket=bucket,
                                         collector=self.COLLECTOR)

            if "latency" in self.results:
                for k in self.results["latency"][bucket].keys():
                    data = {
                        'jts_latency': float(self.results["latency"][bucket][k])
                    }
                    self.append_to_store(data=data, timestamp=timestamp_offset + int(k) * 1000,
                                         cluster=self.cluster, bucket=bucket,
                                         collector=self.COLLECTOR)


class JTSThroughputCollector(JTSCollector):
    METRICS = "jts_throughput"

    def read_stats(self):
        self._consolidate_results("aggregated_throughput.log", "throughput")

    def reconstruct(self):
        if int(self.settings.custom_num_buckets) > 0:
            self.buckets = self.custom_bucket_list()
        timestamp_offset = round(time.time() * 1000)
        self.read_stats()

        if "throughput" in self.results:
            for bucket in self.buckets:
                for k in self.results["throughput"][bucket].keys():
                    data = {
                        'jts_throughput': float(self.results["throughput"][bucket][k])
                    }
                    self.append_to_store(data=data,
                                         timestamp=timestamp_offset +
                                         int(k) * int(self.settings.aggregation_buffer_ms),
                                         cluster=self.cluster, bucket=bucket,
                                         collector=self.COLLECTOR)


class JTSLatencyCollector(JTSCollector):
    METRICS = "jts_latency"
    results = dict()

    def read_stats(self):
        self._consolidate_results("aggregated_latency.log", "latency")

    def reconstruct(self):
        if int(self.settings.custom_num_buckets) > 0:
            self.buckets = self.custom_bucket_list()
        timestamp_offset = round(time.time() * 1000)
        self.read_stats()
        for bucket in self.buckets:
            for k in self.results["latency"][bucket].keys():
                data = {
                    'jts_latency': float(self.results["latency"][bucket][k])
                }
                self.append_to_store(data=data, timestamp=timestamp_offset + int(k) * 1000,
                                     cluster=self.cluster, bucket=bucket,
                                     collector=self.COLLECTOR)

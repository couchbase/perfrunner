import csv
import glob
from typing import Iterator

from cbagent.collectors import Collector


class Latency(Collector):

    COLLECTOR = "latency"

    METRICS = ()

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
            for metric in self.METRICS:
                self.mc.add_metric(metric, bucket=bucket,
                                   collector=self.COLLECTOR)

    def sample(self):
        pass


class KVLatency(Latency):

    COLLECTOR = "spring_latency"

    METRICS = "latency_get", "latency_set"

    PATTERN = '*-worker-*'

    def collect(self):
        pass

    def read_stats(self) -> Iterator:
        for filename in glob.glob(self.PATTERN):
            with open(filename) as fh:
                reader = csv.reader(fh)
                for operation, timestamp, latency in reader:
                    yield operation, timestamp, latency

    def reconstruct(self):
        for bucket in self.get_buckets():
            for operation, timestamp, latency in self.read_stats():
                data = {
                    'latency_' + operation: float(latency) * 1000,  # Latency in ms
                }
                self.store.append(data=data, timestamp=int(timestamp),
                                  cluster=self.cluster, bucket=bucket,
                                  collector=self.COLLECTOR)


class QueryLatency(KVLatency):

    COLLECTOR = "spring_query_latency"

    METRICS = "latency_query",

    PATTERN = 'query-worker-*'

import csv
import glob

from cbagent.collectors.latency import Latency


class ReservoirQueryLatency(Latency):

    COLLECTOR = "spring_query_latency"

    METRICS = "latency_query",

    def __init__(self, settings):
        super(Latency, self).__init__(settings)

    def collect(self):
        pass

    @staticmethod
    def get_stats():
        for filename in glob.glob('*-worker-*'):
            with open(filename) as fh:
                reader = csv.reader(fh)
                for timestamp, latency in reader:
                    yield timestamp, latency

    def reconstruct(self):
        for bucket in self.get_buckets():
            for timestamp, latency in self.get_stats():
                data = {self.METRICS[0]: float(latency) * 1000}  # Latency in ms
                self.store.append(data=data, timestamp=int(timestamp),
                                  cluster=self.cluster, bucket=bucket,
                                  collector=self.COLLECTOR)

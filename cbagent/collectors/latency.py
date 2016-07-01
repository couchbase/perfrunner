from time import time
from uuid import uuid4

from couchbase import Couchbase

from cbagent.collectors import Collector

uhex = lambda: uuid4().hex


class Latency(Collector):

    COLLECTOR = "latency"

    METRICS = ("latency_set", "latency_get", "latency_delete")

    def __init__(self, settings):
        super(Latency, self).__init__(settings)
        self.clients = []
        for bucket in self.get_buckets():
            self.clients.append(Couchbase.connect(
                bucket=bucket, host=settings.master_node,
                username=bucket, password=settings.bucket_password
            ))

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.get_buckets():
            self.mc.add_bucket(bucket)
            for metric in self.METRICS:
                self.mc.add_metric(metric, bucket=bucket,
                                   collector=self.COLLECTOR)

    @staticmethod
    def _measure_latency(client, metric, key):
        t0 = time()
        if metric == "latency_set":
            client.set(key, key)
        elif metric == "latency_get":
            client.get(key)
        elif metric == "latency_delete":
            client.delete(key)
        return 1000 * (time() - t0)  # Latency in ms

    def sample(self):
        for client in self.clients:
            key = uhex()
            samples = {}
            for metric in self.METRICS:
                latency = self._measure_latency(client, metric, key)
                samples[metric] = latency
            self.store.append(samples, cluster=self.cluster,
                              bucket=client.bucket, collector=self.COLLECTOR)

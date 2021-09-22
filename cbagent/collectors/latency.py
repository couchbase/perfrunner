import asyncio
import csv
import glob
from typing import Iterator

from aiohttp import ClientSession

from cbagent.collectors.collector import Collector


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

    METRICS = "latency_get", "latency_set", "latency_total_get", "latency_total_set"

    PATTERN = '*-worker-*'

    def collect(self):
        pass

    def read_stats(self) -> Iterator:
        for filename in glob.glob(self.PATTERN):
            with open(filename) as fh:
                reader = csv.reader(fh)
                for line in reader:
                    yield line

    async def post_results(self, bucket: str):
        async with ClientSession() as self.store.async_session:
            for line in self.read_stats():
                operation = line[0]
                timestamp = line[1]
                latency = line[2]
                data = {
                    'latency_' + operation: float(latency) * 1000,  # Latency in ms
                }
                await self.store.append_async(data=data,
                                              timestamp=int(timestamp),
                                              cluster=self.cluster,
                                              bucket=bucket,
                                              collector=self.COLLECTOR)
                if len(line) == 4:
                    total_latency = line[3]
                    data = {
                        'latency_total_' + operation: float(total_latency) * 1000,  # Latency in ms
                    }
                    await self.store.append_async(data=data,
                                                  timestamp=int(timestamp),
                                                  cluster=self.cluster,
                                                  bucket=bucket,
                                                  collector=self.COLLECTOR)

    def reconstruct(self):
        loop = asyncio.get_event_loop()
        for bucket in self.get_buckets():
            loop.run_until_complete(self.post_results(bucket))
        loop.close()


class QueryLatency(KVLatency):

    COLLECTOR = "spring_query_latency"

    METRICS = "latency_query",

    PATTERN = 'query-worker-*'

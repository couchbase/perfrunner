import asyncio
import csv
import glob
from typing import Iterator

from aiohttp import ClientSession, TCPConnector
from fabric.api import cd, execute, get, parallel, run

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

    PATTERN = '*kv-worker-*'

    def collect(self):
        pass

    def get_remote_stat_files(self):
        def task():
            with cd(self.remote_worker_home), cd('perfrunner'):
                r = run('stat {}'.format(self.PATTERN), quiet=True)
                if not r.return_code:
                    get(self.PATTERN, local_path='./')

        execute(parallel(task), hosts=self.workers)

    def read_stats(self, filename: str) -> Iterator:
        with open(filename) as fh:
            reader = csv.reader(fh)
            for line in reader:
                yield line

    async def post_results(self, filename: str, bucket: str):
        for line in self.read_stats(filename):
            operation, timestamp, latency = line[:3]
            data = {
                'latency_' + operation: float(latency) * 1000,  # Latency in ms
            }
            await self.append_to_store_async(data=data,
                                             timestamp=int(timestamp),
                                             cluster=self.cluster,
                                             bucket=bucket,
                                             collector=self.COLLECTOR)
            if len(line) == 4:
                total_latency = line[3]
                data = {
                    'latency_total_' + operation: float(total_latency) * 1000,  # Latency in ms
                }
                await self.append_to_store_async(data=data,
                                                 timestamp=int(timestamp),
                                                 cluster=self.cluster,
                                                 bucket=bucket,
                                                 collector=self.COLLECTOR)

    async def post_all_results(self):
        async with ClientSession(connector=TCPConnector()) as self.store.async_session:
            await asyncio.gather(*[
                self.post_results(fn, bucket)
                for bucket in self.get_buckets()
                for fn in glob.glob(self.PATTERN + bucket)
            ])

    def reconstruct(self):
        if self.remote_workers:
            self.get_remote_stat_files()

        # Create a new event loop if the current one is closed
        if (loop := asyncio.get_event_loop()).is_closed():
            loop = asyncio.new_event_loop()

        loop.run_until_complete(self.post_all_results())

        loop.close()


class QueryLatency(KVLatency):

    COLLECTOR = "spring_query_latency"

    METRICS = "latency_query",

    PATTERN = 'query-worker-*'

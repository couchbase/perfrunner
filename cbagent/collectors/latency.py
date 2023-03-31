import asyncio
import csv
from pathlib import Path
from typing import Iterator
from uuid import uuid4

from aiohttp import ClientSession, TCPConnector
from fabric.api import cd, execute, get, parallel, run

from cbagent.collectors.collector import Collector
from cbagent.settings import CbAgentSettings


class Latency(Collector):

    COLLECTOR = "latency"

    METRICS = ()

    def __init__(self, settings):
        super().__init__(settings)
        self.stat_dir = 'spring_latency/master_{}'.format(self.master_node)

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

    METRICS = ["latency_get", "latency_set", "latency_durable_set",
               "latency_total_get", "latency_total_set", "latency_total_durable_set"]

    PATTERN = '*kv-worker-*'

    def __init__(self, settings: CbAgentSettings):
        super().__init__(settings)
        if self.collections is not None:
            self.target_groups = {
                bucket: {
                    '{}:{}'.format(scope, collection): options.get('stat_group', '')
                    for scope, collections in scopes.items()
                    for collection, options in collections.items()
                }
                for bucket, scopes in self.collections.items()
            }
        else:
            self.target_groups = {
                bucket: {'_default:_default': ''}
                for bucket in self.get_buckets()
            }

    @staticmethod
    def bucket_stat_group(bucket: str, group: str) -> str:
        if group != '':
            return '{}_{}'.format(bucket, group)
        return bucket

    def update_metadata(self):
        self.mc.add_cluster()
        for bucket in self.get_buckets():
            for group in set(self.target_groups[bucket].values()):
                bucket_group = self.bucket_stat_group(bucket, group)
                self.mc.add_bucket(bucket_group)
                for metric in self.METRICS:
                    self.mc.add_metric(metric, bucket=bucket_group,
                                       collector=self.COLLECTOR)

    def collect(self):
        pass

    def get_remote_stat_files(self):
        Path(self.stat_dir).mkdir(parents=True, exist_ok=True)

        def task():
            with cd(self.remote_worker_home), cd('perfrunner'):
                pattern = '{}/{}'.format(self.stat_dir, self.PATTERN)
                r = run('stat {}'.format(pattern), quiet=True)
                if not r.return_code:
                    run('for f in {}; do mv $f $f-{}; done'.format(pattern, uuid4().hex[:6]))
                    get(pattern, local_path='./{}'.format(self.stat_dir))

        execute(parallel(task), hosts=self.workers)

    def read_stats(self, filename: str) -> Iterator:
        with open(filename) as fh:
            reader = csv.reader(fh)
            for line in reader:
                yield line

    async def post_results(self, filename: str, bucket: str):
        for line in self.read_stats(filename):
            operation, timestamp, latency_single, latency_total, target = line

            target_group = self.target_groups[bucket].get(target, '')
            bucket_group = self.bucket_stat_group(bucket, target_group)

            # Latency in ms
            data = {'latency_' + operation: float(latency_single) * 1000}

            await self.append_to_store_async(data=data,
                                             timestamp=int(timestamp),
                                             cluster=self.cluster,
                                             bucket=bucket_group,
                                             collector=self.COLLECTOR)

            if latency_total:
                # Latency in ms
                data = {'latency_total_' + operation: float(latency_total) * 1000}

                await self.append_to_store_async(data=data,
                                                 timestamp=int(timestamp),
                                                 cluster=self.cluster,
                                                 bucket=bucket_group,
                                                 collector=self.COLLECTOR)

    async def post_all_results(self):
        async with ClientSession(connector=TCPConnector()) as self.store.async_session:
            await asyncio.gather(*[
                self.post_results(fn, bucket)
                for bucket in self.get_buckets()
                for fn in Path(self.stat_dir).glob(self.PATTERN + bucket + "*")
            ])

    def move_remote_stat_files(self):
        def task():
            with cd(self.remote_worker_home), cd('perfrunner'):
                r = run('stat {}'.format(self.PATTERN), quiet=True)
                if not r.return_code:
                    run('mkdir {0} && mv {1} {0}/'.format(self.cluster, self.PATTERN))

        execute(parallel(task), hosts=self.workers)

    def move_local_stat_files(self):
        dest = Path(self.cluster)
        dest.mkdir()
        for file in Path.cwd().glob(self.PATTERN):
            file.rename(dest / file.name)

    def reconstruct(self):
        if self.remote_workers:
            self.get_remote_stat_files()
            self.move_remote_stat_files()

        # Create a new event loop if the current one is closed
        if (loop := asyncio.get_event_loop()).is_closed():
            loop = asyncio.new_event_loop()

        loop.run_until_complete(self.post_all_results())
        loop.close()

        self.move_local_stat_files()


class QueryLatency(KVLatency):

    COLLECTOR = "spring_query_latency"

    METRICS = "latency_query",

    PATTERN = 'query-worker-*'

import asyncio
from pathlib import Path

from aiohttp import ClientSession, TCPConnector

from cbagent.collectors.collector import CouchbaseCollector
from cbagent.settings import CbAgentSettings
from logger import logger
from perfrunner.helpers.local_stats import (
    KV_WORKER_PATTERN,
    QUERY_WORKER_PATTERN,
    parse_spring_latency_file,
    spring_latency_key,
    spring_latency_live_dir,
    spring_latency_snapshot_dir,
    spring_latency_target_groups,
)
from perfrunner.tests import PerfTest


class Latency(CouchbaseCollector):

    ABSTRACT = True
    COLLECTOR = "latency"

    METRICS = ()

    def __init__(self, settings, test: PerfTest):
        super().__init__(settings, test)
        self.test = test
        self.live_stat_dir = spring_latency_live_dir(self.master_node)

    @property
    def snapshot_stat_dir(self) -> str:
        """Dir where this phase's data files are archived once they have been processed.

        The cbmonitor cluster id is minted per phase in the CbAgent path; the Prometheus path
        reuses one cluster id for the whole test, so its per-phase label is appended when
        present, otherwise two phases would archive into the same dir. Once CbAgent is gone
        this collapses to always using the phase label.
        """
        label = self.cluster
        if self.test.use_prometheus and (agent := self.test.collector_agent):
            label = f"{label}_{agent.phase_label}"
        return spring_latency_snapshot_dir(label, self.master_node)

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
    COLLECTOR_FLAG = "latency"

    PROMETHEUS_CUSTOM = True

    METRICS = ["latency_get", "latency_set", "latency_durable_set",
               "latency_total_get", "latency_total_set", "latency_total_durable_set"]

    PATTERN = KV_WORKER_PATTERN

    def __init__(self, settings: CbAgentSettings, test: PerfTest):
        super().__init__(settings, test)
        if self.collections is not None:
            self.target_groups = spring_latency_target_groups(self.collections)
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

    async def post_results(self, filename: str, bucket: str):
        for sample in parse_spring_latency_file(filename):
            target_group = self.target_groups[bucket].get(sample.target, "")
            bucket_group = self.bucket_stat_group(bucket, target_group)

            await self.store.append_async(
                data={f"latency_{sample.operation}": sample.latency_ms},
                timestamp=sample.timestamp_ms,
                cluster=self.cluster,
                bucket=bucket_group,
                collector=self.COLLECTOR,
            )

            if sample.latency_total_ms is not None:
                await self.store.append_async(
                    data={f"latency_total_{sample.operation}": sample.latency_total_ms},
                    timestamp=sample.timestamp_ms,
                    cluster=self.cluster,
                    bucket=bucket_group,
                    collector=self.COLLECTOR,
                )

    async def post_all_results(self):
        async with ClientSession(connector=TCPConnector()) as session:
            self.store.async_session = session
            results = await asyncio.gather(
                *[
                    self.post_results(fn, bucket)
                    for bucket in self.get_buckets()
                    for fn in Path(self.live_stat_dir).glob(self.PATTERN + bucket + "*")
                ],
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.warning(f"Failed to push latency stats to perfstore: {result}")

    def move_local_stat_files(self):
        """Move local latency data files to a cbmonitor snapshot-specific dir."""
        files = list(Path(self.live_stat_dir).glob(self.PATTERN))
        if not files:
            logger.warning(
                "Could not archive spring latency files. Did not find any "
                f"files matching {self.live_stat_dir}/{self.PATTERN}"
            )
            # Deliberately leave any existing `spring_latency_snapshot_dirs` entry alone.
            # A phase that collected nothing must not retract the last phase that did:
            # `report_kpi` runs after the final stats phase, which is often not the phase
            # that produced the workload (e.g. `N1QLTest.run` ends with the `@with_stats`
            # `generate_query_awr_report`), and nothing tells us which phase a KPI is for.
            return

        dest = Path(self.snapshot_stat_dir)
        dest.mkdir(parents=True, exist_ok=True)

        for file in files:
            file.rename(dest / file.name)

        key = spring_latency_key(self.master_node, self.PATTERN)
        self.test.spring_latency_snapshot_dirs[key] = dest

        logger.info(
            f"Archived {len(files)} latency data files for snapshot {self.cluster} in {dest}"
        )

    def reconstruct(self):
        if self.test.has_remote_workers:
            Path(self.live_stat_dir).mkdir(parents=True, exist_ok=True)
            self.test.remote.get_spring_data_files(
                self.test.worker_manager.WORKER_HOME,
                self.PATTERN,
                self.live_stat_dir,
                self.snapshot_stat_dir,
            )

        asyncio.run(self.post_all_results())

        self.move_local_stat_files()


class QueryLatency(KVLatency):

    COLLECTOR = "spring_query_latency"
    COLLECTOR_FLAG = "n1ql_latency"

    PROMETHEUS_CUSTOM = True

    METRICS = "latency_query",

    PATTERN = QUERY_WORKER_PATTERN

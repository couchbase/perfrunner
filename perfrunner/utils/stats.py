from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterator, Optional

from couchbase.options import QueryOptions

from cbagent.metadata_client import MetadataClient
from cbagent.stores import PerfStore
from logger import logger
from perfrunner.settings import CBMONITOR_HOST
from perfrunner.utils.jenkins import BaseScanner, JenkinsScanner

StatsSettings = namedtuple("StatsSettings", ("cluster", "cbmonitor_host"))


class StatsScanner(BaseScanner):
    MAX_WORKERS = 8

    STATUS_QUERY = """
        SELECT component, COUNT(1) AS total
        FROM stats
        WHERE version = $1
        GROUP BY component
        ORDER BY component;
    """

    SNAPSHOT_QUERY = """
        SELECT RAW snapshots
        FROM benchmarks
        WHERE buildURL = $1;
    """

    def __init__(self):
        super().__init__("stats")
        self.jenkins = JenkinsScanner()
        self.ps = PerfStore(host=CBMONITOR_HOST)

    @staticmethod
    def generate_key(attributes: dict) -> str:
        return "".join((
            attributes["cluster"],
            attributes["test_config"],
            attributes["version"],
            attributes["metric"],
            attributes.get("bucket", ""),
            attributes.get("server", ""),
            attributes.get("index", ""),
        ))

    @staticmethod
    def get_metadata_client(cluster: str) -> MetadataClient:
        return MetadataClient(settings=StatsSettings(cluster, CBMONITOR_HOST))

    def store_metric_info(self, attributes: dict):
        key = self.generate_key(attributes)
        self.upsert_to_bucket(key, attributes)

    def get_summary(self, db: str, metric: str) -> Optional[Dict[str, float]]:
        if self.ps.exists(db=db, metric=metric):
            return self.ps.get_summary(db=db, metric=metric)
        return {}

    def cluster_stats(self, m: MetadataClient, cluster: str) -> Iterator[dict]:
        for metric in m.get_metrics():
            db = self.ps.build_dbname(cluster=cluster, collector=metric["collector"])
            summary = self.get_summary(db=db, metric=metric["name"])
            if summary:
                yield {
                    "metric": metric["name"],
                    "summary": summary,
                }

    def bucket_stats(self, m: MetadataClient, cluster: str) -> Iterator[dict]:
        for bucket in m.get_buckets():
            for metric in m.get_metrics(bucket=bucket):
                db = self.ps.build_dbname(
                    cluster=cluster, collector=metric["collector"], bucket=bucket
                )
                summary = self.get_summary(db=db, metric=metric["name"])
                if summary:
                    yield {
                        "bucket": bucket,
                        "metric": metric["name"],
                        "summary": summary,
                    }

    def server_stats(self, m: MetadataClient, cluster: str) -> Iterator[dict]:
        for server in m.get_servers():
            for metric in m.get_metrics(server=server):
                db = self.ps.build_dbname(
                    cluster=cluster, collector=metric["collector"], server=server
                )
                summary = self.get_summary(db=db, metric=metric["name"])
                if summary:
                    yield {
                        "metric": metric["name"],
                        "server": server,
                        "summary": summary,
                    }

    def index_stats(self, m: MetadataClient, cluster: str) -> Iterator[dict]:
        for index in m.get_indexes():
            for metric in m.get_metrics(index=index):
                db = self.ps.build_dbname(
                    cluster=cluster, collector=metric["collector"], index=index
                )
                summary = self.get_summary(db=db, metric=metric["name"])
                if summary:
                    yield {
                        "index": index,
                        "metric": metric["name"],
                        "summary": summary,
                    }

    def find_snapshots(self, url: str):
        for snapshots in self.cluster.query(
            self.SNAPSHOT_QUERY, QueryOptions(positional_parameters=[url])
        ):
            for snapshot in snapshots:
                yield snapshot

    def all_stats(self, url: str):
        for snapshot in self.find_snapshots(url=url):
            m = self.get_metadata_client(snapshot)
            for stats in self.cluster_stats(m, cluster=snapshot):
                yield stats
            for stats in self.bucket_stats(m, cluster=snapshot):
                yield stats
            for stats in self.server_stats(m, cluster=snapshot):
                yield stats
            for stats in self.index_stats(m, cluster=snapshot):
                yield stats

    def _process_build(self, build: dict, version: str):
        if self.get_checkpoint(build["url"]) is not None:
            return

        meta = {
            'version': version,
            'cluster': build['cluster'],
            'component': build['component'],
            'test_config': build['test_config'],
        }
        for stats in self.all_stats(url=build['url']):
            attributes = {**stats, **meta}
            if attributes is not None:
                self.store_metric_info(attributes)
        self.upsert_to_bucket(key=build["url"])
        logger.info(f"Added checkpoint for {build['url']}")

    def run(self):
        for version in self.weekly.builds:
            logger.info(f"Scanning stats from build {version}")
            builds = list(self.jenkins.find_builds(version=version))
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self._process_build, build, version): build
                    for build in builds
                }
                for future in as_completed(futures):
                    future.result()

    def update_status(self):
        for build in self.weekly.builds:
            logger.info(f"Updating status of build {build}")

            for status in self.cluster.query(
                self.STATUS_QUERY, QueryOptions(positional_parameters=[build])
            ):
                status = {
                    'build': build,
                    'component': status['component'],
                    'metric_status': {
                        'collected': status['total'],
                    },
                }
                self.weekly.update_status(status)


def main():
    scanner = StatsScanner()
    scanner.run()
    scanner.update_status()


if __name__ == '__main__':
    main()

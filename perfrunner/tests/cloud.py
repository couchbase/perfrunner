import os
import time

from capella.dedicated.CapellaAPI import CapellaAPI as CapellaAPIDedicated

from logger import logger
from perfrunner.helpers.cbmonitor import with_cloudwatch, with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.rebalance import RebalanceKVTest
from perfrunner.tests.ycsb import YCSBN1QLTest


class CloudTest(PerfTest):
    def __init__(self, *args):
        super().__init__(*args)
        self.server_processes = ["beam.smp", "memcached", "projector", "prometheus"]

    @with_stats
    def access(self, *args):
        pass

    def run(self):
        pass


class CloudIdleTest(CloudTest):
    def _report_kpi(self):
        self.reporter.post(*self.metrics.cpu_utilization())

        for process in self.server_processes:
            self.reporter.post(*self.metrics.avg_server_process_cpu(process))

    @with_stats
    def access(self, *args):
        time.sleep(self.test_config.access_settings.time)

    def run(self):
        self.access()
        self.report_kpi()


class CloudIdleDocsTest(CloudIdleTest):
    def _report_kpi(self):
        self.reporter.post(*self.metrics.cpu_utilization())

        for process in self.server_processes:
            self.reporter.post(*self.metrics.avg_server_process_cpu(process))

    @with_cloudwatch
    @with_stats
    def access(self, *args):
        time.sleep(self.test_config.access_settings.time)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.remote.restart()
        time.sleep(200)
        self.access()
        self.report_kpi()


class CloudIdleKVN1QLTest(YCSBN1QLTest, CloudIdleTest):
    def __init__(self, *args):
        CloudIdleTest.__init__(self, *args)

    def _report_kpi(self):
        self.reporter.post(*self.metrics.cpu_utilization())

        for process in self.server_processes:
            self.reporter.post(*self.metrics.avg_server_process_cpu(process))

    @with_stats
    def access(self, *args):
        time.sleep(self.test_config.access_settings.time)

    def run(self):
        if self.test_config.access_settings.ssl_mode == "data":
            self.download_certificate()
            self.generate_keystore()
        self.download_ycsb()

        self.create_indexes()
        self.wait_for_indexing()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.wait_for_indexing()

        self.access()

        self.report_kpi()


class CloudRebalanceKVTest(RebalanceKVTest, CloudTest):
    def __init__(self, *args):
        CloudTest.__init__(self, *args)
        self.rebalance_settings = self.test_config.rebalance_settings
        self.rebalance_time = 0

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.hot_load()

        self.reset_kv_stats()

        self.access_bg()
        self.rebalance(services="kv,index,n1ql,fts")

        if self.is_balanced():
            self.report_kpi()


class ControlPlaneAPITest(PerfTest):
    def query(self, endpoint, capella_client, url):
        resp = capella_client.do_internal_request(url=url, method="GET")
        if resp.status_code not in (200, 204):
            logger.error(f"Failed to query endpoint {endpoint}: {resp.status_code}")

    @with_stats
    def run_queries(self, endpoint):
        capella_client = CapellaAPIDedicated(
            self.cluster_spec.controlplane_settings["public_api_url"],
            None,
            None,
            "team-performance@couchbase.com",
            os.getenv("CBC_PWD"),
        )

        url = (
            endpoint.replace("{}", capella_client.internal_url)
            .replace("{org_id}", self.cluster_spec.controlplane_settings["org"])
            .replace("{project_id}", self.cluster_spec.controlplane_settings["project"])
            .replace("{cluster_id}", self.cluster_spec.capella_cluster_ids[0])
        )

        latencies = []

        logger.info(f"Querying endpoint {endpoint} for 2 requests/min for 10 minutes...")

        for i in range(20):
            t0 = time.time()
            self.query(endpoint, capella_client, url)
            latency = time.time() - t0
            latencies.append(latency)
            time.sleep(30)
        return latencies

    def _report_kpi(self, latencies, endpoint):
        for percentile in [50, 95]:
            self.reporter.post(*self.metrics.api_latency(endpoint, latencies, percentile))

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        for endpoint_spec in self.test_config.access_settings.capella_endpoints:
            endpoint_name, endpoint = endpoint_spec.split(":", 1)
            latencies = self.run_queries(endpoint)
            self.report_kpi(latencies, endpoint_name)

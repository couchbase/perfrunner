from cbagent.collectors.collector import Collector
from perfrunner.helpers.misc import create_build_tuple
from perfrunner.helpers.rest import RestHelper


class AnalyticsStats(Collector):

    COLLECTOR = "analytics"

    PORT = 9110

    METRICS_MAPPING = {
        "cbas_system_load_average": "system_load_average",
        "cbas_io_reads_total": "io_reads",
        "cbas_gc_count_total": "gc_count",
        "cbas_disk_used_bytes_total": "disk_used",
        "cbas_thread_count": "thread_count",
        "cbas_heap_memory_used_bytes": "heap_used",
        "cbas_io_writes_total": "io_writes",
        "cbas_gc_time_milliseconds_total": "gc_time"
    }

    def __init__(self, settings, test):
        super().__init__(settings)

        self.rest = RestHelper(test.cluster_spec, self.n2n_enabled)
        self.build = self.rest.get_version(host=self.master_node)
        self.servers = self.rest.get_active_nodes_by_role(self.master_node, 'cbas')
        self.build_version_number = create_build_tuple(self.build)
        self.is_columnar = self.rest.is_columnar(self.master_node)

    def update_metadata(self):
        self.mc.add_cluster()

        for server in self.servers:
            self.mc.add_server(server)

    def get_stats(self, server: str, build) -> dict:
        if build < (7, 0, 0, 0) and not self.is_columnar:
            return self.get_http(path="/analytics/node/stats", server=server, port=self.PORT)

        return {
            self.METRICS_MAPPING[metric]: value
            for metric, value in self.rest.get_analytics_prometheus_stats(server).items()
            if metric in self.METRICS_MAPPING
        }

    def sample(self):
        for server in self.servers:
            stats = self.get_stats(server, self.build_version_number)
            self.update_metric_metadata(stats.keys(), server=server)
            self.store.append(stats,
                              cluster=self.cluster,
                              server=server,
                              collector=self.COLLECTOR)

from cbagent.collectors.collector import Collector
from perfrunner.helpers import rest


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

        self.rest = rest.RestHelper(test.cluster_spec)
        self.build = self.rest.get_version(host=self.master_node)
        self.servers = self.rest.get_active_nodes_by_role(self.master_node, 'cbas')
        version, build_number = self.build.split('-')
        self.build_version_number = tuple(map(int, version.split('.'))) + (int(build_number),)

    def update_metadata(self):
        self.mc.add_cluster()

        for server in self.servers:
            self.mc.add_server(server)

    def get_stats(self, server: str, build) -> dict:
        if build < (7, 0, 0, 0):
            return self.get_http(path='/analytics/node/stats',
                                 server=server,
                                 port=self.PORT)
        else:
            stats = {}
            api = 'http://{}:8095/_prometheusMetrics'.format(server)
            api_return = self.rest.get(url=api)
            for line in api_return.text.splitlines():
                if "#" not in line:
                    metric_line = line.split()
                    metric = metric_line[0]
                    value = metric_line[1]
                    if metric in self.METRICS_MAPPING:
                        stats[self.METRICS_MAPPING[metric]] = float(value)
            return stats

    def sample(self):
        for server in self.servers:
            stats = self.get_stats(server, self.build_version_number)
            self.update_metric_metadata(stats.keys(), server=server)
            self.store.append(stats,
                              cluster=self.cluster,
                              server=server,
                              collector=self.COLLECTOR)

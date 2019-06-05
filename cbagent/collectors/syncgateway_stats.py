from cbagent.collectors import Collector
from cbagent.metadata_client import MetadataClient
from cbagent.stores import PerfStore
from perfrunner.helpers import rest


class SyncGatewayStats(Collector):
    COLLECTOR_NODE = "syncgateway_node_stats"
    COLLECTOR_CLUSTER = "syncgateway_cluster_stats"
    REPORT_STATS_PER_NODE = False

    METRICS = (
        "syncGateway_changeCache__lag-queue-0000ms",
        "syncGateway_changeCache__lag-tap-0000ms",
        "syncGateway_changeCache__lag-total-0000ms",
        "syncGateway_changeCache__lag-total-9223372036800ms",
        "syncGateway_changeCache__maxPending",
        "syncGateway_db__document_gets",
        "syncGateway_db__revisionCache_adds",
        "syncGateway_db__revs_added",
        "syncGateway_db__sequence_gets:",
        "syncGateway_db__sequence_reserves",
        "syncGateway_dcp__dataUpdate_count",
        "syncGateway_dcp__setMetadata_count",
        "syncGateway_gocb__Get",
        "syncGateway_gocb__Set",
        "syncGateway_gocb__SingleOps",
        "syncGateway_gocb__Update_Get",
        "syncGateway_gocb__Update_GetWithXattr",
        "syncGateway_gocb__Update_Insert",
        "syncGateway_gocb__Update_Replace",
        "syncGateway_gocb__ViewOps",
        "syncGateway_gocb__WriteCasWithXattr_Insert",
        "syncGateway_rest__requests_0000ms",
        "syncGateway_rest__requests_0100ms",
        "syncGateway_rest__requests_0200ms",
        "syncGateway_rest__requests_0300ms",
        "syncGateway_rest__requests_0700ms",
        "syncGateway_rest__requests_0800ms",
        "syncGateway_rest__requests_0900ms",
        "syncGateway_rest__requests_1000ms",
        "syncgateway__global__resource_utilization__process_cpu_percent_utilization",
        "syncgateway__global__resource_utilization__go_memstats_heapinuse",
        "syncgateway__per_db__db__database__doc_writes_bytes_blip",
        "syncgateway__per_db__db__database__doc_reads_bytes_blip",
        "syncgateway__per_db__db__cache__rev_cache_hits",
        "syncgateway__per_db__db__rev_cache_misses",
        "syncgateway__per_db__db__chan_cache_misses",
        "syncGateway_import__import_count",
    )

    def __init__(self, settings, test):
        self.interval = settings.interval
        self.cluster = settings.cluster
        self.master_node = settings.master_node
        self.auth = (settings.rest_username, settings.rest_password)

        self.buckets = settings.buckets
        self.indexes = settings.indexes
        self.hostnames = settings.hostnames
        self.workers = settings.workers
        self.ssh_username = getattr(settings, 'ssh_username', None)
        self.ssh_password = getattr(settings, 'ssh_password', None)
        self.secondary_statsfile = settings.secondary_statsfile

        self.store = PerfStore(settings.cbmonitor_host)
        self.mc = MetadataClient(settings)

        self.metrics = set()
        self.updater = None

        self.cg_settings = test.settings.syncgateway_settings
        self.hosts = test.cluster_spec.servers[:int(self.cg_settings.nodes)]
        self.rest = rest.RestHelper(test.cluster_spec)
        self.sg_stats = dict()

    def get_metric_value_by_name(self, host,  metic_name):
        metric_array = metic_name.split("__")
        if len(metric_array) == 2:
            g, m = metic_name.split("__")
            if g in self.sg_stats[host]:
                if m in self.sg_stats[host][g]:
                    return self.sg_stats[host][g][m]
        if len(metric_array) == 4:
            i, j, k, l = metic_name.split("__")
            if i in self.sg_stats[host]:
                if j in self.sg_stats[host][i]:
                    if k in self.sg_stats[host][i][j]:
                        if l in self.sg_stats[host][i][j][k]:
                            return self.sg_stats[host][i][j][k][l]
        if len(metric_array) == 5:
            i, j, k, l, m = metic_name.split("__")
            if i in self.sg_stats[host]:
                if j in self.sg_stats[host][i]:
                    if k in self.sg_stats[host][i][j]:
                        if l in self.sg_stats[host][i][j][k]:
                            if m in self.sg_stats[host][i][j][k][l]:
                                return self.sg_stats[host][i][j][k][l][m]
        return 0

    def update_metadata(self):
        self.mc.add_cluster()
        for metric in self.METRICS:
            for host in self.hosts:
                self.mc.add_metric(metric, server=host, collector=self.COLLECTOR_NODE)
            self.mc.add_metric(metric, collector=self.COLLECTOR_CLUSTER)

    def collect_stats(self):
        for host in self.hosts:
            self.sg_stats[host] = self.rest.get_sg_stats(host)

    def measure(self):
        stats = dict()
        stats["_totals"] = dict()
        for metric in self.METRICS:
            for host in self.hosts:
                if host not in stats:
                    stats[host] = dict()
                stats[host][metric] = float(self.get_metric_value_by_name(host, metric))
                if metric not in stats["_totals"]:
                    stats["_totals"][metric] = 0
                stats["_totals"][metric] += stats[host][metric]
        return stats

    def sample(self):
        self.collect_stats()
        self.update_metric_metadata(self.METRICS)
        samples = self.measure()
        if self.REPORT_STATS_PER_NODE:
            for host in self.hosts:
                self.store.append(samples[host],
                                  cluster=self.cluster,
                                  server=host,
                                  collector=self.COLLECTOR_NODE)
        self.store.append(samples["_totals"],
                          cluster=self.cluster,
                          collector=self.COLLECTOR_CLUSTER)

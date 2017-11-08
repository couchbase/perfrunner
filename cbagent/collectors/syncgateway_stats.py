from cbagent.collectors import Collector
from perfrunner.helpers import rest


class SyncGatewayStats(Collector):
    COLLECTOR_NODE = "syncgateway_node_stats"
    COLLECTOR_CLUSTER = "syncgateway_cluster_stats"

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
        "syncGateway_stats__bulkApi_BulkDocsPerDocRollingMean",
        "syncGateway_stats__bulkApi.BulkDocsRollingMean",
        "syncGateway_stats__bulkApi.BulkGetPerDocRollingMean",
        "syncGateway_stats__bulkApi.BulkGetRollingMean",
        "syncGateway_stats__changesFeeds_active",
        "syncGateway_stats__changesFeeds_total",
        "syncGateway_stats__goroutines_highWaterMark",
        "syncGateway_stats__handler.CheckAuthRollingMean",
        "syncGateway_stats__indexReader.getChanges.Count",
        "syncGateway_stats__indexReader.getChanges.Time",
        "syncGateway_stats__indexReader.getChanges.UseCached",
        "syncGateway_stats__indexReader.getChanges.UseIndexed",
        "syncGateway_stats__indexReader.numReaders.OneShot",
        "syncGateway_stats__indexReader.numReaders.Persistent",
        "syncGateway_stats__indexReader.pollPrincipals.Count",
        "syncGateway_stats__indexReader.pollPrincipals.Time:",
        "syncGateway_stats__indexReader.pollReaders.Count",
        "syncGateway_stats__indexReader.pollReaders.Time",
        "syncGateway_stats__indexReader.seqHasher.GetClockTime",
        "syncGateway_stats__indexReader.seqHasher.GetHash",
        "syncGateway_stats__requests_active",
        "syncGateway_stats__requests_total",
        "syncGateway_stats__revisionCache_hits",
        "syncGateway_stats__revisionCache_misses",
    )

    def __init__(self, settings, test):
        super().__init__(settings)
        self.cg_settings = settings.syncgateway_settings
        self.hosts = test.cluster_spec.servers[:int(self.cg_settings.nodes)]
        self.rest = rest.RestHelper(test.cluster_spec)
        self.sg_stats = dict()

    def get_metric_value_by_name(self, host, metic_name):
        g, m = metic_name.split("__")
        if g in self.sg_stats:
            if m in self.sg_stats[m]:
                return self.sg_stats[g][m]
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
        stats = dict
        stats["_totals"] = dict()
        for metric in self.METRICS:
            for host in self.hosts:
                if host not in stats:
                    stats[host] = dict()
                stats[host][metric] = float(self.get_metric_value_by_name(metric))
                if metric not in stats["_totals"]:
                    stats["_totals"][metric] = 0
                stats["_totals"][metric] += stats[host][metric]
        return stats

    def sample(self):
        self.collect_stats()
        self.update_metric_metadata(self.METRICS)
        samples = self.measure()
        for host in self.hosts:
            self.store.append(samples[host],
                              cluster=self.cluster,
                              server=host,
                              collector=self.COLLECTOR_NODE)
        self.store.append(samples["_totals"],
                          cluster=self.cluster,
                          collector=self.COLLECTOR_SERVER)

from cbagent.collectors import Collector
from cbagent.metadata_client import MetadataClient
from cbagent.stores import PerfStore
from perfrunner.helpers import rest


class SyncGatewayStats(Collector):
    COLLECTOR_NODE = "syncgateway_node_stats"
    COLLECTOR_CLUSTER = "syncgateway_cluster_stats"
    REPORT_STATS_PER_NODE = False

    METRICS = (
        # Iridium and Cobalt stats
        "syncgateway__global__resource_utilization__process_cpu_percent_utilization",
        "syncgateway__global__resource_utilization__process_memory_resident",
        "syncgateway__global__resource_utilization__system_memory_total",
        "syncgateway__global__resource_utilization__pub_net_bytes_sent",
        "syncgateway__global__resource_utilization__pub_net_bytes_recv",
        "syncgateway__global__resource_utilization__admin_net_bytes_sent",
        "syncgateway__global__resource_utilization__admin_net_bytes_recv",
        "syncgateway__global__resource_utilization__num_goroutines",
        "syncgateway__global__resource_utilization__goroutines_high_watermark",
        "syncgateway__global__resource_utilization__go_memstats_sys",
        "syncgateway__global__resource_utilization__go_memstats_heapalloc",
        "syncgateway__global__resource_utilization__go_memstats_heapidle",
        "syncgateway__global__resource_utilization__go_memstats_heapinuse",
        "syncgateway__global__resource_utilization__go_memstats_heapreleased",
        "syncgateway__global__resource_utilization__go_memstats_stackinuse",
        "syncgateway__global__resource_utilization__go_memstats_stacksys",
        "syncgateway__global__resource_utilization__go_memstats_pausetotalns",
        "syncgateway__global__resource_utilization__error_count",
        "syncgateway__global__resource_utilization__warn_count",

        "syncgateway__per_db__db__cache__rev_cache_hits",
        "syncgateway__per_db__db__cache__rev_cache_misses",
        "syncgateway__per_db__db__cache__rev_cache_bypass",
        "syncgateway__per_db__db__cache__chan_cache_hits",
        "syncgateway__per_db__db__cache__chan_cache_misses",
        "syncgateway__per_db__db__cache__chan_cache_active_revs",
        "syncgateway__per_db__db__cache__chan_cache_tombstone_revs",
        "syncgateway__per_db__db__cache__chan_cache_removal_revs",
        "syncgateway__per_db__db__cache__chan_cache_num_channels",
        "syncgateway__per_db__db__cache__chan_cache_max_entries",
        "syncgateway__per_db__db__cache__chan_cache_pending_queries",
        "syncgateway__per_db__db__cache__chan_cache_channels_added",
        "syncgateway__per_db__db__cache__chan_cache_channels_evicted_inactive",
        "syncgateway__per_db__db__cache__chan_cache_channels_evicted_nru",
        "syncgateway__per_db__db__cache__chan_cache_compact_count",
        "syncgateway__per_db__db__cache__chan_cache_compact_time",
        "syncgateway__per_db__db__cache__num_active_channels",
        "syncgateway__per_db__db__cache__num_skipped_seqs",
        "syncgateway__per_db__db__cache__abandoned_seqs",
        "syncgateway__per_db__db__cache__high_seq_cached",
        "syncgateway__per_db__db__cache__high_seq_stable",
        "syncgateway__per_db__db__cache__skipped_seq_len",
        "syncgateway__per_db__db__cache__pending_seq_len",

        "syncgateway__per_db__db__database__sequence_get_count",
        "syncgateway__per_db__db__database__sequence_incr_count",
        "syncgateway__per_db__db__database__sequence_reserved_count",
        "syncgateway__per_db__db__database__sequence_assigned_count",
        "syncgateway__per_db__db__database__sequence_released_count",
        "syncgateway__per_db__db__database__crc32c_match_count",
        "syncgateway__per_db__db__database__num_replications_active",
        "syncgateway__per_db__db__database__num_replications_total",
        "syncgateway__per_db__db__database__num_doc_writes",
        "syncgateway__per_db__db__database__num_tombstones_compacted",
        "syncgateway__per_db__db__database__doc_writes_bytes",
        "syncgateway__per_db__db__database__doc_writes_xattr_bytes",
        "syncgateway__per_db__db__database__num_doc_reads_rest",
        "syncgateway__per_db__db__database__num_doc_reads_blip",
        "syncgateway__per_db__db__database__doc_writes_bytes_blip",
        "syncgateway__per_db__db__database__doc_reads_bytes_blip",
        "syncgateway__per_db__db__database__warn_xattr_size_count",
        "syncgateway__per_db__db__database__warn_channels_per_doc_count",
        "syncgateway__per_db__db__database__warn_grants_per_doc_count",
        "syncgateway__per_db__db__database__dcp_received_count",
        "syncgateway__per_db__db__database__high_seq_feed",
        "syncgateway__per_db__db__database__dcp_received_time",
        "syncgateway__per_db__db__database__dcp_caching_count",
        "syncgateway__per_db__db__database__dcp_caching_time",

        "syncgateway__per_db__db__delta_sync__deltas_requested",
        "syncgateway__per_db__db__delta_sync__deltas_sent",
        "syncgateway__per_db__db__delta_sync__delta_pull_replication_count",
        "syncgateway__per_db__db__delta_sync__delta_cache_hit",
        "syncgateway__per_db__db__delta_sync__delta_cache_miss",
        "syncgateway__per_db__db__delta_sync__delta_push_doc_count",

        "syncgateway__per_db__db__shared_bucket_import__import_count",
        "syncgateway__per_db__db__shared_bucket_import__import_cancel_cas",
        "syncgateway__per_db__db__shared_bucket_import__import_error_count",
        "syncgateway__per_db__db__shared_bucket_import__import_processing_time",

        "syncgateway__per_db__db__cbl_replication_push__doc_push_count",
        "syncgateway__per_db__db__cbl_replication_push__write_processing_time",
        "syncgateway__per_db__db__cbl_replication_push__sync_function_time",
        "syncgateway__per_db__db__cbl_replication_push__sync_function_count",
        "syncgateway__per_db__db__cbl_replication_push__propose_change_time",
        "syncgateway__per_db__db__cbl_replication_push__propose_change_count",
        "syncgateway__per_db__db__cbl_replication_push__attachment_push_count",
        "syncgateway__per_db__db__cbl_replication_push__attachment_push_bytes",
        "syncgateway__per_db__db__cbl_replication_push__conflict_write_count",

        "syncgateway__per_db__db__cbl_replication_pull__num_pull_repl_active_one_shot",
        "syncgateway__per_db__db__cbl_replication_pull__num_pull_repl_active_continuous",
        "syncgateway__per_db__db__cbl_replication_pull__num_pull_repl_total_one_shot",
        "syncgateway__per_db__db__cbl_replication_pull__num_pull_repl_total_continuous",
        "syncgateway__per_db__db__cbl_replication_pull__num_pull_repl_since_zero",
        "syncgateway__per_db__db__cbl_replication_pull__num_pull_repl_caught_up",
        "syncgateway__per_db__db__cbl_replication_pull__request_changes_count",
        "syncgateway__per_db__db__cbl_replication_pull__request_changes_time",
        "syncgateway__per_db__db__cbl_replication_pull__rev_send_count",
        "syncgateway__per_db__db__cbl_replication_pull__rev_send_latency",
        "syncgateway__per_db__db__cbl_replication_pull__rev_processing_time",
        "syncgateway__per_db__db__cbl_replication_pull__max_pending",
        "syncgateway__per_db__db__cbl_replication_pull__attachment_pull_count",
        "syncgateway__per_db__db__cbl_replication_pull__attachment_pull_bytes",

        "syncgateway__per_db__db__security__num_docs_rejected",
        "syncgateway__per_db__db__security__num_access_errors",
        "syncgateway__per_db__db__security__auth_success_count",
        "syncgateway__per_db__db__security__auth_failed_count",
        "syncgateway__per_db__db__security__total_auth_time",

        "syncgateway__per_db__db__gsi_views__access_count",
        "syncgateway__per_db__db__gsi_views__roleAccess_count",
        "syncgateway__per_db__db__gsi_views__channels_count",

        # 2.1 stats
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
        if test.cluster_spec.infrastructure_sync_gateways:
            self.hosts = test.cluster_spec.sgw_servers
        else:
            self.hosts = test.cluster_spec.servers[:int(self.cg_settings.nodes)]
        self.rest = rest.RestHelper(test.cluster_spec, test.test_config)
        self.sg_stats = dict()

    def get_metric_value_by_name(self, host,  metric_name):
        metric_array = metric_name.split("__")
        if len(metric_array) == 2:
            g, m = metric_name.split("__")
            if g in self.sg_stats[host]:
                if m in self.sg_stats[host][g]:
                    return self.sg_stats[host][g][m]
        if len(metric_array) == 4:
            i, j, k, m = metric_name.split("__")
            if i in self.sg_stats[host]:
                if j in self.sg_stats[host][i]:
                    if k in self.sg_stats[host][i][j]:
                        if m in self.sg_stats[host][i][j][k]:
                            return self.sg_stats[host][i][j][k][m]
        if len(metric_array) == 5:
            i, j, k, l, m = metric_name.split("__")
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

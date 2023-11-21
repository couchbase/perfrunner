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

    METRICS_CAPELLA = (
        "sgw_resource_utilization_process_cpu_percent_utilization",
        "sgw_resource_utilization_process_memory_resident",
        "sgw_resource_utilization_system_memory_total",
        "sgw_resource_utilization_pub_net_bytes_sent",
        "sgw_resource_utilization_pub_net_bytes_recv",
        "sgw_resource_utilization_admin_net_bytes_sent",
        "sgw_resource_utilization_admin_net_bytes_recv",
        "sgw_resource_utilization_num_goroutines",
        "sgw_resource_utilization_goroutines_high_watermark",
        "sgw_resource_utilization_go_memstats_sys",
        "sgw_resource_utilization_go_memstats_heapalloc",
        "sgw_resource_utilization_go_memstats_heapidle",
        "sgw_resource_utilization_go_memstats_heapinuse",
        "sgw_resource_utilization_go_memstats_heapreleased",
        "sgw_resource_utilization_go_memstats_stackinuse",
        "sgw_resource_utilization_go_memstats_stacksys",
        "sgw_resource_utilization_go_memstats_pausetotalns",
        "sgw_resource_utilization_error_count",
        "sgw_resource_utilization_warn_count",

        "sgw_cache_rev_cache_hits",
        "sgw_cache_rev_cache_misses",
        "sgw_cache_rev_cache_bypass",
        "sgw_cache_chan_cache_hits",
        "sgw_cache_chan_cache_misses",
        "sgw_cache_chan_cache_active_revs",
        "sgw_cache_chan_cache_tombstone_revs",
        "sgw_cache_chan_cache_removal_revs",
        "sgw_cache_chan_cache_num_channels",
        "sgw_cache_chan_cache_max_entries",
        "sgw_cache_chan_cache_pending_queries",
        "sgw_cache_chan_cache_channels_added",
        "sgw_cache_chan_cache_channels_evicted_inactive",
        "sgw_cache_chan_cache_channels_evicted_nru",
        "sgw_cache_chan_cache_compact_count",
        "sgw_cache_chan_cache_compact_time",
        "sgw_cache_num_active_channels",
        "sgw_cache_num_skipped_seqs",
        "sgw_cache_abandoned_seqs",
        "sgw_cache_high_seq_cached",
        "sgw_cache_high_seq_stable",
        "sgw_cache_skipped_seq_len",
        "sgw_cache_pending_seq_len",

        "sgw_database_sequence_get_count",
        "sgw_database_sequence_incr_count",
        "sgw_database_sequence_reserved_count",
        "sgw_database_sequence_assigned_count",
        "sgw_database_sequence_released_count",
        "sgw_database_crc32c_match_count",
        "sgw_database_num_replications_active",
        "sgw_database_num_replications_total",
        "sgw_database_num_doc_writes",
        "sgw_database_num_tombstones_compacted",
        "sgw_database_doc_writes_bytes",
        "sgw_database_doc_writes_xattr_bytes",
        "sgw_database_num_doc_reads_rest",
        "sgw_database_num_doc_reads_blip",
        "sgw_database_doc_writes_bytes_blip",
        "sgw_database_doc_reads_bytes_blip",
        "sgw_database_warn_xattr_size_count",
        "sgw_database_warn_channels_per_doc_count",
        "sgw_database_dcp_received_count",
        "sgw_database_high_seq_feed",
        "sgw_database_dcp_received_time",
        "sgw_database_dcp_caching_count",
        "sgw_database_dcp_caching_time",
        "sgw_database_conflict_write_count",

        "sgw_shared_bucket_import_import_count",
        "sgw_shared_bucket_import_import_cancel_cas",
        "sgw_shared_bucket_import_import_error_count",
        "sgw_shared_bucket_import_import_processing_time",

        "sgw_replication_push_doc_push_count",
        "sgw_replication_push_write_processing_time",
        "sgw_database_sync_function_time",
        "sgw_database_sync_function_count",
        "sgw_replication_push_propose_change_time",
        "sgw_replication_push_propose_change_count",
        "sgw_replication_push_attachment_push_count",
        "sgw_replication_push_attachment_push_bytes",

        "sgw_replication_pull_num_pull_repl_active_one_shot",
        "sgw_replication_pull_num_pull_repl_active_continuous",
        "sgw_replication_pull_num_pull_repl_total_one_shot",
        "sgw_replication_pull_num_pull_repl_total_continuous",
        "sgw_replication_pull_num_pull_repl_since_zero",
        "sgw_replication_pull_num_pull_repl_caught_up",
        "sgw_replication_pull_request_changes_count",
        "sgw_replication_pull_request_changes_time",
        "sgw_replication_pull_rev_send_count",
        "sgw_replication_pull_rev_send_latency",
        "sgw_replication_pull_rev_processing_time",
        "sgw_replication_pull_max_pending",
        "sgw_replication_pull_attachment_pull_count",
        "sgw_replication_pull_attachment_pull_bytes",

        "sgw_security_num_docs_rejected",
        "sgw_security_num_access_errors",
        "sgw_security_auth_success_count",
        "sgw_security_auth_failed_count",
        "sgw_security_total_auth_time",

        "sgw_gsi_views_access_count",
        "sgw_gsi_views_roleAccess_count",
        "sgw_gsi_views_channels_count"
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
        self.use_capella = test.cluster_spec.capella_infrastructure
        self.num_buckets = test.test_config.cluster.num_buckets

        self.store = PerfStore(settings.cbmonitor_host)
        self.mc = MetadataClient(settings)

        self.metrics = set()
        self.updater = None

        self.cg_settings = test.settings.syncgateway_settings
        if test.cluster_spec.infrastructure_syncgateways:
            self.hosts = test.cluster_spec.sgw_servers[:int(self.cg_settings.nodes)]
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
            i, j, k, l, m = metric_name.split("__")  # noqa: E741
            if i in self.sg_stats[host]:
                if j in self.sg_stats[host][i]:
                    for db in range(1, self.num_buckets + 1):
                        k = "db-{}".format(db)
                        if k in self.sg_stats[host][i][j]:
                            if l in self.sg_stats[host][i][j][k]:
                                if m in self.sg_stats[host][i][j][k][l]:
                                    return self.sg_stats[host][i][j][k][l][m]
        return 0

    def update_metadata(self):
        self.mc.add_cluster()
        if self.use_capella:
            for metric in self.METRICS_CAPELLA:
                self.mc.add_metric(metric, collector=self.COLLECTOR_CLUSTER)
        else:
            for metric in self.METRICS:
                for host in self.hosts:
                    self.mc.add_metric(metric, server=host, collector=self.COLLECTOR_NODE)
                self.mc.add_metric(metric, collector=self.COLLECTOR_CLUSTER)

    def collect_stats(self):
        for host in self.hosts:
            self.sg_stats[host] = self.rest.get_sg_stats(host)
            if self.use_capella:
                break

    def measure(self):
        stats = dict()
        stats["_totals"] = dict()
        if self.use_capella:
            for host in self.hosts:
                for line in self.sg_stats[host].splitlines():
                    if "#" not in line:
                        metric_line = line.split()
                        metric = metric_line[0].split('{')[0]
                        value = metric_line[1]
                        if metric in self.METRICS_CAPELLA:
                            if metric in stats["_totals"]:
                                stats["_totals"][metric] += float(value)
                            else:
                                stats["_totals"][metric] = float(value)
                for metric in self.METRICS_CAPELLA:
                    stats["_totals"][metric] /= 2
                break
        else:
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

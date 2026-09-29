from cbagent.collectors import CouchbaseCollector
from perfrunner.helpers.misc import iter_prometheus_samples
from perfrunner.helpers.rest import RestHelper
from perfrunner.tests import PerfTest


class SyncGatewayStats(CouchbaseCollector):
    COLLECTOR = "syncgateway_cluster_stats"
    COLLECTOR_FLAG = "syncgateway_stats"
    SKIP_ON_DYNAMIC = True
    PROMETHEUS_CUSTOM = True

    # Derived here rather than scraped: the vCPUs the SGW processes use, summed over nodes.
    # process_cpu_percent_utilization is 10x the share of the whole machine SGW uses (SGW's own
    # description calls the 10x a mistake kept for backwards compatibility), so it takes the
    # node's cores, which GOMAXPROCS defaults to, to turn it into vCPUs.
    VCPUS_METRIC = "sgw_process_vcpus"
    PROCESS_CPU = "sgw_resource_utilization_process_cpu_percent_utilization"
    GOMAXPROCS = "go_sched_gomaxprocs_threads"  # SGW 3.3+

    METRICS = (
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
        "sgw_resource_utilization_node_cpu_percent_utilization",

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
        "sgw_cache_skipped_sequence_skip_list_nodes",
        "sgw_cache_current_skipped_seq_count",

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
        "sgw_database_warn_grants_per_doc_count",
        "sgw_database_dcp_received_count",
        "sgw_database_high_seq_feed",
        "sgw_database_dcp_received_time",
        "sgw_database_dcp_caching_count",
        "sgw_database_dcp_caching_time",
        "sgw_database_conflict_write_count",
        "sgw_database_num_doc_writes_rejected",

        "sgw_delta_sync_deltas_requested",
        "sgw_delta_sync_deltas_sent",
        "sgw_delta_sync_delta_pull_replication_count",
        "sgw_delta_sync_delta_cache_hit",
        "sgw_delta_sync_delta_sync_miss",  # (sic) expvar calls it delta_cache_miss
        "sgw_delta_sync_delta_cache_num_items",
        "sgw_delta_sync_delta_push_doc_count",

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
        "sgw_gsi_views_channels_count",
    )

    def __init__(self, settings, test: PerfTest):
        super().__init__(settings)

        if test.settings.syncgateway_settings.log_streaming:
            self.METRICS += (
                "fluentbit_output_proc_records_total",
                "fluentbit_output_proc_bytes_total",
                "fluentbit_output_dropped_records_total",
                "fluentbit_output_retried_records_total",
                "fluentbit_output_retries_failed_total",
                "fluentbit_output_errors_total",
                "fluentbit_input_records_total",
                "fluentbit_input_bytes_total"
            )

        sg_settings = test.settings.syncgateway_settings
        if test.cluster_spec.infrastructure_syncgateways:
            self.hosts = test.cluster_spec.sgw_servers[:int(sg_settings.nodes)]
        else:
            self.hosts = test.cluster_spec.servers[:int(sg_settings.nodes)]
        if test.cluster_spec.capella_infrastructure:
            # App Services serves one cluster-wide endpoint, which the spec repeats per node.
            self.hosts = self.hosts[:1]
        self.rest = RestHelper(
            test.cluster_spec, bool(test.test_config.cluster.enable_n2n_encryption)
        )

    def update_metadata(self):
        # Metrics are registered in sample(), as they are first seen: SGW only exposes some of
        # them on newer builds or with a feature enabled (e.g. delta sync), so registering the
        # whole list up front would list metrics that have no data.
        self.mc.add_cluster()

    def measure(self) -> dict[str, float]:
        """Sum each metric over every Sync Gateway node and every label set (e.g. database)."""
        metrics = set(self.METRICS)
        stats = {}
        for host in self.hosts:
            # Keyed by label set, to pair up each node's series should an endpoint serve several.
            process_cpu, cores = {}, {}
            for name, labels, value in iter_prometheus_samples(self.rest.get_sg_stats(host)):
                if name in metrics:
                    stats[name] = stats.get(name, 0) + value
                if name == self.PROCESS_CPU:
                    process_cpu[frozenset(labels.items())] = value
                elif name == self.GOMAXPROCS:
                    cores[frozenset(labels.items())] = value
            for node in process_cpu.keys() & cores.keys():
                vcpus = process_cpu[node] * cores[node] / 1000
                stats[self.VCPUS_METRIC] = stats.get(self.VCPUS_METRIC, 0) + vcpus
        return stats

    def sample(self):
        samples = self.measure()
        self.update_metric_metadata(samples)
        self.store.append(samples, cluster=self.cluster, collector=self.COLLECTOR)

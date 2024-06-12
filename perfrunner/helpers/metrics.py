from __future__ import annotations

import glob
import os
import re
import statistics
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np

from cbagent.stores import PerfStore
from logger import logger
from perfrunner.settings import CBMONITOR_HOST, ClusterSpec, TestConfig
from perfrunner.workloads.bigfun.query_gen import Query

if TYPE_CHECKING:
    from perfrunner.tests import PerfTest

Number = Union[float, int]

Metric = Tuple[
    Number,          # Value
    List[str],       # Snapshots
    Dict[str, str],  # Metric info
]

DailyMetric = Tuple[
    str,        # Metric
    Number,     # Value
    List[str],  # Snapshots
]


def s2m(seconds: float, precision:int = 1) -> float:
    """Convert seconds to minutes."""
    return round(seconds / 60, precision)


def strip(s: str) -> str:
    for c in ' &()':
        s = s.replace(c, '')
    return s.lower()


@dataclass
class CH2Metrics:
    # transactions
    total_txn_time_us: float = 0
    txn_success_count: int = 0

    # analytical queries
    geo_mean_cbas_query_time: float = 0
    average_cbas_query_set_time: float = 0
    cbas_qph: float = 0

    @property
    def tpm(self) -> float:
        """Return transactions per minute."""
        return (
            self.txn_success_count * 60 / self.average_cbas_query_set_time
            if self.average_cbas_query_set_time > 0
            else 0
        )

    @property
    def txn_response_time(self) -> float:
        """Return average transaction response time in seconds."""
        return (
            self.total_txn_time_us / 1e6 / self.txn_success_count
            if self.txn_success_count > 0
            else float("inf")
        )


@dataclass
class CH3Metrics(CH2Metrics):
    # fts queries
    average_fts_query_set_time_ms: float = 0
    average_fts_client_time_ms: float = 0
    fts_qph: float = 0


class MetricHelper:

    def __init__(self, test: PerfTest):
        self.test = test
        self.test_config: TestConfig = test.test_config
        self.cluster_spec: ClusterSpec = test.cluster_spec
        if self.test.dynamic_infra:
            self.store = None
        else:
            self.store = PerfStore(CBMONITOR_HOST)

    @property
    def _title(self) -> str:
        title = self.test_config.showfast.title
        if self.cluster_spec.capella_infrastructure:
            return title.format(provider=self.cluster_spec.capella_backend.upper())
        return title

    @property
    def _order_by(self) -> str:
        return self.test_config.showfast.order_by

    @property
    def _chirality(self) -> str:
        return 0

    @property
    def _snapshots(self) -> List[str]:
        return self.test.cbmonitor_snapshots

    @property
    def _num_nodes(self):
        return self.test_config.cluster.initial_nodes[0]

    @property
    def _mem_quota(self):
        return self.test_config.cluster.mem_quota

    def _metric_info(self,
                     metric_id: Optional[str] = None,
                     title: Optional[str] = None,
                     order_by: Optional[str] = None,
                     chirality: Optional[int] = None,
                     mem_quota: Optional[int] = None,
                     stat_group: str = '') -> Dict[str, str]:
        return {
            'id': metric_id or self.test_config.name,
            'title': title or self._title,
            'orderBy': order_by or self._order_by,
            'chirality': chirality or self._chirality,
            'memquota': mem_quota or self._mem_quota,
            'statGroup': stat_group or ''
        }

    @property
    def _bucket_names(self):
        bucket_names = self.test_config.buckets
        if self.cluster_spec.serverless_infrastructure:
            bucket_names = [
                self.test_config.serverless_db.db_map[bucket]['name'] for bucket in bucket_names
            ]
        return bucket_names

    @property
    def _custom_bucket_names(self):
        return [
            'bucket-{}'.format(i + 1)
            for i in range(int(self.test_config.jts_access_settings.custom_num_buckets))
        ]

    @property
    def query_id(self) -> str:
        if 'views' in self._title:
            return ''

        query_id = self._title.split(',')[0]
        query_id = query_id.split()[0]
        for prefix in 'CU', 'QU', 'QF', 'CF', 'CI', 'Q', 'UP', 'DL', 'AG', 'PI', 'BF', 'WF':
            query_id = query_id.replace(prefix, '')
        if query_id.isnumeric():
            return '{:05d}'.format(int(query_id))
        else:
            return query_id

    def avg_n1ql_throughput(self, master_node: str) -> Metric:
        """Generate cluster total query throughput metric."""
        metric_id = '{}_avg_query_requests'.format(self.test_config.name)
        title = 'Avg. Query Throughput (queries/sec), {}'.format(self._title)

        metric_info = self._metric_info(metric_id, title,
                                        order_by=self.query_id, chirality=1)
        throughput = self._avg_n1ql_throughput(master_node)
        return throughput, self._snapshots, metric_info

    def _avg_n1ql_throughput(self, master_node: str) -> int:
        """Calculate cluster total queries/sec.

        Calculation:
         1. Sum up total query requests over all query nodes.
         2. Divide by access phase time.
        """
        test_time = self.test_config.access_settings.time
        total_requests = 0
        for query_node in self.test.rest.get_active_nodes_by_role(master_node, 'n1ql'):
            vitals = self.test.rest.get_query_stats(query_node)
            total_requests += vitals['requests.count']

        throughput = total_requests / test_time
        return round(throughput, throughput < 1 and 1 or 0)

    def avg_n1ql_rebalance_throughput(self, rebalance_time, total_requests) -> Metric:
        metric_id = '{}_avg_query_requests'.format(self.test_config.name)
        title = 'Avg. Query Throughput (queries/sec), {}'.format(self._title)

        metric_info = self._metric_info(metric_id, title,
                                        order_by=self.query_id, chirality=1)
        throughput = int(total_requests / rebalance_time)
        return throughput, self._snapshots, metric_info

    def bulk_n1ql_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        items = self.test_config.load_settings.items / 4
        throughput = round(items / time_elapsed)

        return throughput, self._snapshots, metric_info

    def n1ql_vector_recall_and_accuracy(self, k: int, probe: int,  value: float,
                                        metric: str) -> Metric:
        metric_id = f'{metric}_{k}_{probe}_{self.test_config.name}'
        title_prefix = f"{metric}@{k}, probes-{probe} across 1000 queries"
        metric_id = metric_id.replace('.', '')
        title = f'{title_prefix}, {self._title}'
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        return round(value, 3), self._snapshots, metric_info

    def fts_index(self, elapsed_time: float) -> Metric:
        metric_id = self.test_config.name
        title_temp = self._title
        title_test = title_temp.split("(sec), ")[1]
        title = 'Total Index build time(sec), {}'.format(title_test)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        index_time = round(elapsed_time, 1)

        return index_time, self._snapshots, metric_info

    def fts_index_size(self, index_size_raw: int) -> Metric:
        metric_id = "{}_indexsize".format(self.test_config.name)
        title_temp = self._title
        title_test = title_temp.split("(sec), ")[1]
        title = 'Index size (MB), {}'.format(title_test)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        index_size_mb = int(index_size_raw / (1024 ** 2))

        return index_size_mb, self._snapshots, metric_info

    def fts_index_with_latency(self, elapsed_time: float) -> Metric:
        metric_id = self.test_config.name.replace("latency", "index_time")
        title = 'Total Index build time(sec), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        metric_info['subCategory'] = "Index"
        index_time = round(elapsed_time, 1)
        return index_time, self._snapshots, metric_info

    def fts_size_with_latency(self, index_size_raw: int) -> Metric:
        metric_id = "{}_indexsize".format(self.test_config.name).replace("latency", "")
        title = 'Index size (MB), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        index_size_mb = int(index_size_raw / (1024 ** 2))
        metric_info['subCategory'] = "Index"
        return index_size_mb, self._snapshots, metric_info

    def jts_throughput(self) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, "jts_throughput")
        metric_id = metric_id.replace('.', '')
        title = "Average Throughput (q/sec), {}".format(self._title)
        metric_info = self._metric_info(metric_id, title, chirality=1)
        timings = self._jts_metric(collector="jts_stats", metric="jts_throughput")
        thr = round(np.average(timings), 2)
        thr = round(thr/(int(self.test_config.jts_access_settings.aggregation_buffer_ms)/1000))
        if thr > 100:
            thr = round(thr)
        return thr, self._snapshots, metric_info

    def jts_latency(self, percentile=50) -> Metric:
        prefix = "Average latency (ms)"
        if percentile != 50:
            prefix = "{}th percentile latency (ms)".format(percentile)
        metric_id = '{}_{}_{}th'.format(self.test_config.name, "jts_latency", percentile)
        metric_id = metric_id.replace('.', '')
        title = "{}, {}".format(prefix, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        timings = self._jts_metric(collector="jts_stats", metric="jts_latency",
                                   percentile=percentile)
        lat = round(np.percentile(timings, percentile), 2)
        if lat > 100:
            lat = round(lat)
        return lat, self._snapshots, metric_info

    def jts_recall_and_accuracy(self, value, metric, k_nearest_neighbour):
        metric_id = '{}_{}at{}'.format(self.test_config.name, metric, k_nearest_neighbour)
        title_prefix = "Average {}@{} across 1000 queries".format(metric, k_nearest_neighbour)
        metric_id = metric_id.replace('.', '')
        title = "{}, {}".format(title_prefix, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        metric_info['subCategory'] = metric
        return round(value, 3), self._snapshots, metric_info

    def _jts_metric(self, collector, metric, percentile=None):
        timings = []
        bucket_names = self._bucket_names
        if int(self.test_config.jts_access_settings.custom_num_buckets) > 0:
            bucket_names = self._custom_bucket_names
        bucket_metric_list = []
        for bucket in bucket_names:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector=collector,
                                         bucket=bucket)
            bucket_timings = self.store.get_values(db, metric=metric)
            bucket_metric = 0
            if metric == "jts_latency":
                bucket_metric = round(np.percentile(bucket_timings, percentile), 2)
            elif metric == "jts_throughput":
                bucket_metric = round(np.average(bucket_timings), 2)

            if bucket_metric > 100:
                bucket_metric = round(bucket_metric)
            logger.info("The {}{} value for {} is {}".format(f"{percentile}th "
                        if percentile is not None else "", metric, bucket, bucket_metric))
            bucket_metric_list.append(bucket_metric)
            timings += bucket_timings
        if len(bucket_metric_list) > 1:
            logger.info("The standard deviation across all buckets is: {}".format(
                statistics.stdev(bucket_metric_list)))
        return timings

    def _ops_data(self,
                  buckets: List[str] = [],
                  cluster_idx: int = 0,
                  collector: str = 'ns_server',
                  stat_group: str = '',
                  metric: str = 'ops') -> List[int]:
        """Calculate total ops/sec over a given set of buckets on a given cluster.

         At each time point, sum ops/sec for buckets (to get time series of total ops/sec):
            [
                bucket-1 ops/sec at t0 + bucket-2 ops/sec at t0 + ... + bucket-N ops/sec at t0,
                bucket-1 ops/sec at t1 + bucket-2 ops/sec at t1 + ... + bucket-N ops/sec at t1,
                ...,
                bucket-1 ops/sec at tN + bucket-2 ops/sec at tN + ... + bucket-N ops/sec at tN
            ]

        If no buckets are specified, use all buckets (the default).

        If no cluster_idx is specified, use the first cluster (the default).
        """
        buckets = buckets or self._bucket_names
        values = []
        for bucket in buckets:
            bucket_group = '{}{}'.format(bucket, '_' + stat_group if stat_group != '' else '')
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[cluster_idx],
                                         collector=collector,
                                         bucket=bucket_group)
            if self.store.exists(db, metric=metric):
                return_ops = self.store.get_values(db, metric=metric)
                if len(values) != 0:
                    sum_ops = [ops1 + ops2 for ops1, ops2 in zip(values, return_ops)]
                    values = sum_ops
                else:
                    values = return_ops
        return values

    def _avg_ops(self,
                 buckets: List[str] = [],
                 cluster_idx: int = 0,
                 collector: str = 'ns_server',
                 stat_group: str = '',
                 metric: str = 'ops') -> int:
        """Calculate average total ops/sec for a given set of buckets on a given cluster.

        Calculation:
         1. At each time point, sum ops/sec for buckets (to get time series of total ops/sec):
            [
                bucket-1 ops/sec at t0 + bucket-2 ops/sec at t0 + ... + bucket-N ops/sec at t0,
                bucket-1 ops/sec at t1 + bucket-2 ops/sec at t1 + ... + bucket-N ops/sec at t1,
                ...,
                bucket-1 ops/sec at tN + bucket-2 ops/sec at tN + ... + bucket-N ops/sec at tN
            ]
         2. Take average ops/sec of this new time series.

        If no buckets are specified, use all buckets (the default).

        If no cluster_idx is specified, use the first cluster (the default).
        """
        if values := self._ops_data(buckets, cluster_idx, collector, stat_group, metric):
            return int(np.average(values))
        return -1

    def _max_ops(self,
                 buckets: List[str] = [],
                 cluster_idx: int = 0,
                 collector: str = 'ns_server',
                 stat_group: str = '',
                 metric: str = 'ops',
                 percentile: Number = 90) -> int:
        """Calculate P90 total ops/sec over a given set of buckets on a given cluster.

        Calculation:
         1. At each time point, sum ops/sec for buckets (to get time series of total ops/sec):
            [
                bucket-1 ops/sec at t0 + bucket-2 ops/sec at t0 + ... + bucket-N ops/sec at t0,
                bucket-1 ops/sec at t1 + bucket-2 ops/sec at t1 + ... + bucket-N ops/sec at t1,
                ...,
                bucket-1 ops/sec at tN + bucket-2 ops/sec at tN + ... + bucket-N ops/sec at tN
            ]
         2. Take P90 ops/sec of this new time series.

        If no buckets are specified, use all buckets (the default).

        If no cluster_idx is specified, use the first cluster (the default).
        """
        if values := self._ops_data(buckets, cluster_idx, collector, stat_group, metric):
            return int(np.percentile(values, percentile))
        return -1

    def _construct_ops_metrics(self,
                               metric_name: str,
                               overall_throughput: Number,
                               stat_group_throughputs: dict[str, Number] = {},
                               cluster_idx: int = 0) -> List[Metric]:
        if stat_group_throughputs:
            # Overall throughput first
            # We do this here to ensure the title is correct when we are using stat groups
            metric_id = None
            title = '{}, {}'.format(metric_name, self._title)
            if len(self.test.cbmonitor_clusters) > 1:
                metric_id = '{}_cluster{}'.format(self.test_config.name, cluster_idx + 1)
                title = '{} (cluster {})'.format(title, cluster_idx + 1)

            metric_info = self._metric_info(metric_id, title, chirality=1)
            metrics = [(overall_throughput, self._snapshots, metric_info)]

            # Per-collection throughputs
            for stat_group, throughput in stat_group_throughputs.items():
                metric_id = '{}_{}'.format(self.test_config.name, stat_group)
                title = '{} per collection ({}), {}'.format(metric_name, stat_group, self._title)
                if len(self.test.cbmonitor_clusters) > 1:
                    metric_id = '{}_cluster{}'.format(metric_id, cluster_idx + 1)
                    title = '{} (cluster {})'.format(title, cluster_idx + 1)

                metric_info = self._metric_info(metric_id, title, chirality=1)
                metrics.append((throughput, self._snapshots, metric_info))

            return metrics

        metric_info = self._metric_info(chirality=0)
        return [(overall_throughput, self._snapshots, metric_info)]

    def avg_ops(self, buckets: List[str] = [], cluster_idx: int = 0) -> List[Metric]:
        """Generate average total ops/sec metrics for a given set of buckets on a given cluster.

        Generates overall average ops/sec and per-stat-group average ops/sec metrics (if stat
        groups are being used).

        Example: with 2 buckets doing steady 1000 ops/sec and steady 2000 ops/sec respectively, the
        overall average total ops/sec is 3000.

        If no buckets are specified, use all buckets (the default).

        If no cluster_idx is specified, use the first cluster (the default).
        """
        overall_throughput = self._avg_ops(buckets=buckets, cluster_idx=cluster_idx)
        stat_group_throughputs = {
            stat_group: self._avg_ops(buckets=buckets,
                                      cluster_idx=cluster_idx,
                                      collector='metrics_rest_api_collection_throughput',
                                      stat_group=stat_group,
                                      metric='kv_collection_ops')
            for stat_group in self.test_config.collection.collection_stat_groups
        }
        return self._construct_ops_metrics('Average Throughput (ops/sec)',
                                           overall_throughput,
                                           stat_group_throughputs,
                                           cluster_idx)

    def max_ops(self, buckets: List[str] = [],
                cluster_idx: int = 0,
                percentiles: Iterable[Number] = [90]) -> List[Metric]:
        """Generate P90 total ops/sec metrics over a given set of buckets on a given cluster.

        Generates overall P90 ops/sec and per-stat-group P90 ops/sec metrics (if stat groups are
        being used).

        Example: with 5 buckets each doing constant 1000 ops/sec, the overall P90 total ops/sec
        according to this function will be ~5000 (subject to how steady the ops/sec are).

        If no buckets are specified, use all buckets (the default).

        If no cluster_idx is specified, use the first cluster (the default).
        """
        metrics = []
        stat_groups = self.test_config.collection.collection_stat_groups or ['']
        for stat_group in stat_groups:
            for percentile in percentiles:
                logger.info(f'percentile is: {percentile}')
                overall_throughput = self._max_ops(buckets=buckets,
                                                   cluster_idx=cluster_idx,
                                                   percentile=percentile)
                logger.info(f'overall throughput is: {overall_throughput}')
                stat_group_throughput = self._max_ops(buckets=buckets,
                                                      cluster_idx=cluster_idx,
                                                      collector='metrics_rest_api_collection_throughput',
                                                      stat_group=stat_group,
                                                      metric='kv_collection_ops',
                                                      percentile=percentile)
                logger.info(f'stat group throughput is: {stat_group_throughput}')
                extra_ = '_' + stat_group if stat_group != '' else ''
                metric_id = f"{self.test_config.name}_{extra_}_{percentile:g}th"
                metric_id = metric_id.replace('.', '')

                title_prefix = f'{percentile:g}th percentile {extra_}'

                if len(self.test.cbmonitor_clusters) > 1:
                    metric_id = f'{metric_id}_cluster{cluster_idx + 1}'
                    title_prefix = f'{title_prefix} (cluster {cluster_idx + 1})'

                title = f'{title_prefix} {self._title}'

                metric_info = self._metric_info(metric_id, title, chirality=-1,
                                                stat_group=stat_group)
                metric_info.update({'percentile': percentile})

                metrics.append((overall_throughput if stat_group == '' else stat_group_throughput,
                                self._snapshots, metric_info))

        return metrics

    def get_percentile_value_of_node_metric(self, collector, metric, server, percentile):
        values = []
        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector=collector,
                                     server=server)
        values += self.store.get_values(db, metric=metric)
        return int(np.percentile(values, percentile))

    def get_collector_values(self, collector):
        values = []
        for bucket in self._bucket_names:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector=collector,
                                         bucket=bucket)
            values += self.store.get_values(db, metric=collector)
        return values

    def count_overthreshold_value_of_collector(self, collector, threshold):
        values = self.get_collector_values(collector)
        return sum(v >= threshold for v in values)

    def get_percentile_value_of_collector(self, collector, percentile):
        values = self.get_collector_values(collector)
        return np.percentile(values, percentile)

    def xdcr_lag(self, percentile: Number = 95) -> Metric:
        metric_id = '{}_{}th_xdcr_lag'.format(self.test_config.name, percentile)
        title = '{}th percentile replication lag (ms), {}'.format(percentile,
                                                                  self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        xdcr_lag = self.get_percentile_value_of_collector('xdcr_lag', percentile)

        return round(xdcr_lag, 1), self._snapshots, metric_info

    def avg_replication_rate(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        rate = self._avg_replication_rate(time_elapsed)

        return rate, self._snapshots, metric_info

    def avg_replication_throughput(self, throughput: float, xdcr_link: str) -> Metric:
        metric_id = self.test_config.name + '_' + xdcr_link
        title = self.test_config.showfast.title + ', ' + xdcr_link

        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=1)

        return round(throughput), self._snapshots, metric_info

    def replication_throughput(self, throughput: float) -> Metric:
        metric_id = self.test_config.name
        title = self.test_config.showfast.title
        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=1)

        return round(throughput), self._snapshots, metric_info

    def avg_replication_multilink(self, time_elapsed: float, xdcr_link: str) -> Metric:

        metric_id = self.test_config.name + '_' + xdcr_link
        title = self.test_config.showfast.title + ', ' + xdcr_link

        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=1)

        rate = self._avg_replication_rate(time_elapsed)

        return rate, self._snapshots, metric_info

    def _avg_replication_rate(self, time_elapsed: float) -> float:
        initial_items = self.test_config.load_settings.ops or \
            self.test_config.load_settings.items
        num_buckets = self.test_config.cluster.num_buckets
        avg_replication_rate = num_buckets * initial_items / time_elapsed

        return round(avg_replication_rate)

    def max_drain_rate(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        items_per_node = self.test_config.load_settings.items / self._num_nodes
        drain_rate = round(items_per_node / time_elapsed)

        return drain_rate, self._snapshots, metric_info

    def avg_disk_write_queue(self) -> Metric:
        metric_info = self._metric_info(chirality=-1)

        values = []
        for bucket in self._bucket_names:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='disk_write_queue')

        disk_write_queue = int(np.average(values))

        return disk_write_queue, self._snapshots, metric_info

    def avg_total_queue_age(self) -> Metric:
        metric_info = self._metric_info(chirality=-1)

        values = []
        for bucket in self._bucket_names:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='vb_avg_total_queue_age')

        avg_total_queue_age = int(np.average(values))

        return avg_total_queue_age, self._snapshots, metric_info

    def avg_couch_views_ops(self) -> Metric:
        metric_info = self._metric_info(chirality=1)

        values = []
        for bucket in self._bucket_names:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='couch_views_ops')

        couch_views_ops = int(np.average(values))

        return couch_views_ops, self._snapshots, metric_info

    def query_latency(self, percentile: Number, cluster_idx: int = 0) -> Metric:
        metric_id = '{}_query_{:g}th'.format(self.test_config.name, percentile)
        metric_id = metric_id.replace('.', '')

        title_prefix = '{:g}th percentile query latency (ms)'.format(percentile)

        if len(self.test.cbmonitor_clusters) > 1:
            metric_id = '{}_cluster{}'.format(metric_id, cluster_idx + 1)
            title_prefix = '{} (cluster {})'.format(title_prefix, cluster_idx + 1)

        title = '{}, {}'.format(title_prefix, self._title)

        metric_info = self._metric_info(metric_id, title,
                                        order_by=self.query_id, chirality=-1)

        latency = self._query_latency(percentile, cluster_idx)

        return latency, self._snapshots, metric_info

    def _query_latency(self, percentile: Number, cluster_idx: int = 0) -> float:
        values = []
        for bucket in self._bucket_names:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[cluster_idx],
                                         collector='spring_query_latency',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='latency_query')

        query_latency = np.percentile(values, percentile)
        if query_latency < 100:
            return round(query_latency, 1)
        return int(query_latency)

    def secondary_scan_latency(self, percentile: Number, title: str = None) -> Metric:
        metric_id = "{}_{:g}th".format(self.test_config.name, percentile)
        if title is None:
            title = '{:g}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                               self._title)
        else:
            title = '{:g}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                               title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        metric_info['category'] = "lat"

        cluster = ""
        for cid in self.test.cbmonitor_clusters:
            if "apply_scanworkload" in cid:
                cluster = cid
                break
        db = self.store.build_dbname(cluster=cluster,
                                     collector='secondaryscan_latency')
        timings = self.store.get_values(db, metric='Nth-latency')
        timings = list(map(int, timings))
        logger.info("Number of samples are {}".format(len(timings)))
        scan_latency = np.percentile(timings, percentile) / 1e6
        scan_latency = round(scan_latency, 2)

        return scan_latency, self._snapshots, metric_info

    def secondary_scan_latency_value(self, scan_latency,
                                     percentile: Number, title: str = None,
                                     update_category: bool = True) -> Metric:
        metric_id = "{}_{:g}th".format(self.test_config.name, percentile)
        title = '{:g}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                           title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        if update_category:
            metric_info['category'] = "lat"

        scan_latency = scan_latency / 1e6
        scan_latency = round(scan_latency, 2)

        return scan_latency, self._snapshots, metric_info

    def analytics_time_taken(self,
                             time_taken: float,
                             sql_suite: str) -> Metric:

        metric_id = f'{self.test_config.name}_time_taken_{sql_suite}'
        metric_id = metric_id.replace('.', '')

        title = f'{sql_suite} Time Elapsed (sec), {self._title}'
        metric_info = self._metric_info(metric_id, title, chirality=1)
        time_taken = round(time_taken, 2)

        return time_taken, self._snapshots, metric_info

    def query_suite_runtime(self,
                            time_taken: float,
                            suite: str) -> Metric:

        metric_id = f'{self.test_config.name}_runtime_{suite}'
        metric_id = metric_id.replace('.', '')

        title = f'{suite} Runtime (sec), {self._title}'
        metric_info = self._metric_info(metric_id, title, chirality=1)
        time_taken = round(time_taken, 2)

        return time_taken, self._snapshots, metric_info

    def kv_latency(self,
                   operation: str,
                   percentiles: Iterable[Number] = [99.9],
                   collector: str = 'spring_latency',
                   cluster_idx: int = 0) -> List[Metric]:
        metrics = []

        # The [''] represents the default case of not having any stat groups
        stat_groups = self.test_config.collection.collection_stat_groups or ['']

        for stat_group in stat_groups:
            latencies = self._kv_latency(operation, percentiles, collector, stat_group, cluster_idx)
            for percentile, latency in zip(percentiles, latencies):
                metric_id = '{}_{}{}_{:g}th'.format(
                    self.test_config.name,
                    operation,
                    '_' + stat_group if stat_group != '' else '',
                    percentile
                )
                metric_id = metric_id.replace('.', '')

                title_prefix = '{:g}th percentile {}{}'.format(
                    percentile,
                    operation.upper(),
                    ' (' + stat_group + ')' if stat_group != '' else ''
                )

                if len(self.test.cbmonitor_clusters) > 1:
                    metric_id = '{}_cluster{}'.format(metric_id, cluster_idx + 1)
                    title_prefix = '{} (cluster {})'.format(title_prefix, cluster_idx + 1)

                title = '{} {}'.format(title_prefix, self._title)

                metric_info = self._metric_info(metric_id, title, chirality=-1,
                                                stat_group=stat_group)
                metric_info.update({'percentile': percentile, 'operation': operation})

                metrics.append((latency, self._snapshots, metric_info))

        return metrics

    def _kv_latency(self,
                    operation: str,
                    percentiles: Iterable[Number],
                    collector: str,
                    stat_group: str = '',
                    cluster_idx: int = 0) -> list[float]:
        timings = []
        metric = 'latency_{}'.format(operation)
        for bucket in self._bucket_names:
            bucket_group = '{}{}'.format(bucket, '_' + stat_group if stat_group != '' else '')

            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[cluster_idx],
                                         collector=collector,
                                         bucket=bucket_group)

            if self.store.exists(db, metric):
                timings += self.store.get_values(db, metric=metric)

        if not timings:
            logger.warn('No latency data found for operation = {}, collector = {}, stat_group = {}'
                        .format(operation, collector, stat_group))
            return []

        latencies = [
            round(latency) if (latency := np.percentile(timings, p)) > 100 else round(latency, 2)
            for p in percentiles
        ]

        return latencies

    def observe_latency(self, percentile: Number) -> Metric:
        metric_id = '{}_{:g}th'.format(self.test_config.name, percentile)
        title = '{:g}th percentile {}'.format(percentile, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        timings = []
        for bucket in self._bucket_names:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='observe',
                                         bucket=bucket)
            timings += self.store.get_values(db, metric='latency_observe')

        latency = round(np.percentile(timings, percentile), 2)

        return latency, self._snapshots, metric_info

    def cpu_utilization(self) -> Metric:
        metric_id = '{}_avg_cpu'.format(self.test_config.name)
        title = 'Avg. CPU utilization (%)'
        title = '{}, {}'.format(title, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        cluster = self.test.cbmonitor_clusters[0]
        bucket = self._bucket_names[0]

        db = self.store.build_dbname(cluster=cluster,
                                     collector='ns_server',
                                     bucket=bucket)
        values = self.store.get_values(db, metric='cpu_utilization_rate')

        cpu_utilization = int(np.average(values))

        return cpu_utilization, self._snapshots, metric_info

    def avg_server_process_cpu(self, server_process: str) -> Metric:
        metric_id = '{}_avg_{}_cpu'.format(self.test_config.name, server_process)
        metric_id = metric_id.replace('.', '_')
        title = 'Avg. {} CPU utilization (%)'.format(server_process)
        title = '{}, {}'.format(title, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        values = []
        for (cluster_name, servers), initial_nodes in zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        ):
            cluster = list(filter(lambda name: name.startswith(cluster_name),
                                  self.test.cbmonitor_clusters))[0]
            for server in servers[:initial_nodes]:
                hostname = server.replace('.', '')

                db = self.store.build_dbname(cluster=cluster,
                                             collector='atop',
                                             server=hostname)
                metric = server_process + '_cpu'
                values += self.store.get_values(db, metric=metric)

        server_process_cpu = round(np.average(values), 1)

        return server_process_cpu, self._snapshots, metric_info

    def max_memcached_rss(self) -> Metric:
        metric_id = '{}_memcached_rss'.format(self.test_config.name)
        title = 'Max. memcached RSS (MB),{}'.format(
            self._title.split(',')[-1]
        )
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        max_rss = 0
        for (cluster_name, servers), initial_nodes in zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        ):
            cluster = list(filter(lambda name: name.startswith(cluster_name),
                                  self.test.cbmonitor_clusters))[0]
            for server in servers[:initial_nodes]:
                hostname = server.replace('.', '')
                db = self.store.build_dbname(cluster=cluster,
                                             collector='atop',
                                             server=hostname)
                values = self.store.get_values(db, metric='memcached_rss')
                rss = round(max(values) / 1024 ** 2)
                max_rss = max(max_rss, rss)

        return max_rss, self._snapshots, metric_info

    def avg_memcached_rss(self) -> Metric:
        metric_id = '{}_avg_memcached_rss'.format(self.test_config.name)
        title = 'Avg. memcached RSS (MB),{}'.format(
            self._title.split(',')[-1]
        )
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        rss = []
        for (cluster_name, servers), initial_nodes in zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        ):
            cluster = list(filter(lambda name: name.startswith(cluster_name),
                                  self.test.cbmonitor_clusters))[0]
            for server in servers[:initial_nodes]:
                hostname = server.replace('.', '')

                db = self.store.build_dbname(cluster=cluster,
                                             collector='atop',
                                             server=hostname)
                rss += self.store.get_values(db, metric='memcached_rss')

        avg_rss = int(np.average(rss) / 1024 ** 2)

        return avg_rss, self._snapshots, metric_info

    def memory_overhead(self, key_size: int = 20) -> Metric:
        metric_info = self._metric_info(chirality=-1)

        item_size = key_size + self.test_config.load_settings.size
        user_data = self.test_config.load_settings.items * item_size
        user_data *= self.test_config.bucket.replica_number + 1
        user_data /= 2 ** 20

        mem_used, _, _ = self.avg_memcached_rss()
        mem_used *= self._num_nodes

        overhead = int(100 * (mem_used / user_data - 1))

        return overhead, self._snapshots, metric_info

    def get_indexing_meta(self,
                          value: float,
                          index_type: str,
                          unit: str = "min",
                          name: str = "",
                          update_category: bool = True) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, index_type.lower())
        test_name = self._title
        if name:
            test_name = name
        title = '{} index ({}), {}'.format(index_type, unit, test_name)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        if update_category:
            metric_info['category'] = index_type.lower()

        value = s2m(value)

        return value, self._snapshots, metric_info

    def get_ddl_time(self,
                     value: float,
                     index_type: str,
                     unit: str = "min",
                     name: str = "") -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, index_type.lower())
        test_name = self._title
        if name:
            test_name = name
        title = '{} index ({}), {}'.format(index_type, unit, test_name)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        metric_info['category'] = "ddl"

        if index_type == "Backup":
            metric_info['orderBy'] = 'B' + str(metric_info['orderBy'][1:])
        if index_type == "Restore":
            metric_info['orderBy'] = 'C' + str(metric_info['orderBy'][1:])

        if unit == "min":
            value = s2m(value)
        else:
            value = round(value, 2)

        return value, self._snapshots, metric_info

    def get_memory_meta(self,
                        value: float,
                        memory_type: str) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name,
                                   memory_type.replace(" ", "").lower())
        title = '{} (GB), {}'.format(memory_type, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        return value, self._snapshots, metric_info

    def bnr_throughput(self,
                       time_elapsed: float,
                       edition: str,
                       tool: str,
                       storage: str = None) -> Metric:

        tool_and_storage = tool + '-' + storage if storage else tool
        metric_id = '{}_{}_thr_{}'.format(self.test_config.name,
                                          tool_and_storage,
                                          edition)
        title = '{} {} throughput (Avg. MB/sec), {}'.format(
            edition, tool, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=1)

        data_size = self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2 ** 20  # MB

        avg_throughput = round(data_size / time_elapsed)

        return avg_throughput, self._snapshots, metric_info

    def tool_time(self,
                  time_elapsed: float,
                  edition: str,
                  tool: str,
                  storage: str = None) -> Metric:

        tool_and_storage = tool + '-' + storage if storage else tool
        metric_id = '{}_{}_time_{}'.format(
            self.test_config.name, tool_and_storage, edition)
        title = '{} {} time elapsed (seconds), {}'.format(
            edition, tool, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        return round(time_elapsed), self._snapshots, metric_info

    def backup_size(self, size: float,
                    edition: str,
                    tool: str,
                    storage: str = None) -> Metric:

        tool_and_storage = tool + '-' + storage if storage else tool
        metric_id = '{}_{}_size_{}'.format(
            self.test_config.name, tool_and_storage, edition)
        title = '{} {} size (GB), {}'.format(edition,
                                             tool,
                                             self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)

        return size, self._snapshots, metric_info

    def disk_size(self, size: float) -> Metric:

        metric_id = '{}_size'.format(self.test_config.name)
        title = 'Disk Size (GB), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title, chirality=-1)
        size = round(float(size) / 2 ** 30)  # B -> GB

        return size, self._snapshots, metric_info

    def disk_size_reduction(self, disk_size: float, raw_data_size: float) -> Metric:

        metric_id = '{}_disk_size_reduction'.format(self.test_config.name)
        title = 'Disk Size Reduction (%), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title, chirality=1)
        reduction = round((1.0 - disk_size / raw_data_size) * 100)

        return reduction, self._snapshots, metric_info

    def merge_throughput(self,
                         time_elapsed: float,
                         edition: str,
                         tool: str = None,
                         storage: str = None) -> Metric:

        tool_and_storage = tool + '-' + storage if storage else tool
        metric_id = '{}_{}_thr_{}'.format(
            self.test_config.name, tool_and_storage, edition)
        title = '{} {} throughput (Avg. MB/sec), {}'.format(
            edition, tool, self._title)
        metric_info = self._metric_info(metric_id, title, chirality=1)

        data_size = 2 * self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2 ** 20  # MB

        avg_throughput = round(data_size / time_elapsed)

        return avg_throughput, self._snapshots, metric_info

    def tool_size_diff(self, size_diff: float,
                       edition: str,
                       tool: str,
                       storage: str = None) -> Metric:

        tool_and_storage = tool + '-' + storage if storage else tool
        metric_id = '{}_{}_size_diff_{}'.format(
            self.test_config.name, tool_and_storage, edition)
        title = '{} {} size difference (GB), {}'.format(edition,
                                                        tool,
                                                        self._title)
        return size_diff, self._snapshots, self._metric_info(metric_id, title, chirality=-1)

    def import_and_export_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        data_size = self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2 ** 20  # MB

        avg_throughput = round(data_size / time_elapsed)

        return avg_throughput, self._snapshots, metric_info

    def import_file_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        import_file = self.test_config.export_settings.import_file
        data_size = os.path.getsize(import_file) / 2.0 ** 20
        avg_throughput = round(data_size / time_elapsed)

        return avg_throughput, self._snapshots, metric_info

    def verify_series_in_limits(self, expected_number: int) -> bool:
        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector='secondary_debugstats')
        values = self.store.get_values(db, metric='num_connections')
        values = list(map(float, values))
        logger.info("Number of samples: {}".format(len(values)))
        logger.info("Sample values: {}".format(values))

        if any(value > expected_number for value in values):
            return False
        return True

    def _parse_ycsb_throughput(self, operation: str = "access") -> int:
        throughput = 0
        if operation == "load":
            ycsb_log_files = [filename
                              for filename in glob.glob("YCSB/ycsb_load_*.log")
                              if "stderr" not in filename]
        else:
            ycsb_log_files = [filename
                              for filename in glob.glob("YCSB/ycsb_run_*.log")
                              if "stderr" not in filename]

        for filename in ycsb_log_files:
            with open(filename) as fh:
                for line in fh.readlines():
                    if line.startswith('[OVERALL], Throughput(ops/sec)'):
                        throughput += int(float(line.split()[-1]))
                        break
        return throughput

    def _parse_pytpcc_throughput(self) -> int:
        executed = 0

        pytpcc_log_file = [filename for filename
                           in glob.glob("py-tpcc/pytpcc/pytpcc_run_result.log")]

        for filename in pytpcc_log_file:
            with open(filename) as fh:
                for line in fh.readlines():
                    if 'NEW_ORDER' in line:
                        if line.split()[4] == 'txn/s':
                            executed = line.split()[1]
        return int(executed)

    def _ycsb_perc_calc(self, _temp: List[Number], io_type: str, percentile: Number,
                        lat_dic: Dict[str, Number], _fc: int) -> Dict[str, Number]:
        pio_type = '{}th Percentile {}'.format(percentile, io_type)
        p_lat = round(np.percentile(_temp, percentile) / 1000, 3)
        if _fc > 1:
            p_lat = round((((lat_dic[pio_type] * (_fc - 1)) + p_lat) / _fc), 3)
        lat_dic.update({pio_type: p_lat})
        return lat_dic

    def _ycsb_avg_calc(self, _temp: List[Number], io_type: str, lat_dic: Dict[str, Number],
                       _fc: int) -> Dict[str, Number]:
        aio_type = 'Average {}'.format(io_type)
        a_lat = round((sum(_temp) / len(_temp)) / 1000, 3)
        if _fc > 1:
            a_lat = round((((lat_dic[aio_type] * (_fc - 1)) + a_lat) / _fc), 3)
        lat_dic.update({aio_type: a_lat})
        return lat_dic

    def _parse_ycsb_latency(self, percentile: str, operation: str = "access") -> int:
        lat_dic = {}
        _temp = []
        _fc = 1
        if operation == "load":
            ycsb_log_files = [filename
                              for filename in glob.glob("YCSB/ycsb_load_*.log")
                              if "stderr" not in filename]
        else:
            ycsb_log_files = [filename
                              for filename in glob.glob("YCSB/ycsb_run_*.log")
                              if "stderr" not in filename]
        for filename in ycsb_log_files:
            fh2 = open(filename)
            _l1 = fh2.readlines()
            _l1_len = len(_l1)
            fh = open(filename)
            _c = 0
            for x in range(_l1_len - 1):
                line = fh.readline()
                if re.search('], (.*?)000,', line):
                    io_type = line.split('[')[1].split(']')[0]
                    _n = 0
                    while (line.startswith('[{}]'.format(io_type))):
                        lat = float(line.split()[-1])
                        _temp.append(lat)
                        line = fh.readline()
                        _n += 1
                    _temp.sort()
                    if "FAILED" not in io_type and "CLEANUP" not in io_type:
                        lat_dic = self._ycsb_perc_calc(_temp=_temp,
                                                       io_type=io_type,
                                                       lat_dic=lat_dic,
                                                       _fc=_fc,
                                                       percentile=percentile)
                        lat_dic = self._ycsb_avg_calc(_temp=_temp,
                                                      io_type=io_type,
                                                      lat_dic=lat_dic,
                                                      _fc=_fc)
                    _temp.clear()
                    _c += _n
                _c += 1
                x += _c
            _fc += 1
        return lat_dic

    def ycsb_get_max_latency(self):
        max_lats = {}
        ycsb_log_files = [filename
                          for filename in glob.glob("YCSB/ycsb_run_*.log")
                          if "stderr" not in filename]
        for filename in ycsb_log_files:
            fh = open(filename)
            lines = fh.readlines()
            num_lines = len(lines)

            fh2 = open(filename)
            for x in range(0, num_lines):
                line = fh2.readline()
                if line.find("], MaxLatency(us),") >= 1:
                    parts = line.split(",")
                    type = parts[0].replace("[", "").replace("]", "")
                    if type == "CLEANUP"\
                            or "FAILED" in type:
                        continue
                    value = parts[2].strip()
                    max_lats[type] = max(float(value)/1000.0, max_lats.get(type, 0))

        return max_lats

    def ycsb_get_failed_ops(self):
        failures = {"READ": 0, "UPDATE": 0}
        ycsb_log_files = [filename
                          for filename in glob.glob("YCSB/ycsb_run_*.log")
                          if "stderr" not in filename]
        for filename in ycsb_log_files:
            fh = open(filename)
            lines = fh.readlines()
            num_lines = len(lines)

            fh2 = open(filename)
            for x in range(0, num_lines):
                line = fh2.readline()
                if line.find("-FAILED], Operations,") >= 1:
                    parts = line.split(",")
                    type = parts[0].replace("[", "").replace("]", "").split("-")[0]
                    if type in ["READ", "UPDATE"]:
                        value = parts[2].strip()
                        failures[type] = int(value) + failures.get(type, 0)
        return failures

    def ycsb_get_gcs(self):
        ycsb_log_files = [filename
                          for filename in glob.glob("YCSB/ycsb_run_*.log")
                          if "stderr" not in filename]
        gcs = 0
        for filename in ycsb_log_files:
            fh = open(filename)
            lines = fh.readlines()
            num_lines = len(lines)
            fh2 = open(filename)
            for x in range(0, num_lines):
                line = fh2.readline()
                if line.find("[TOTAL_GCs], Count,") >= 0:
                    gcs += int(line.split(",")[2].strip())

        return gcs

    def ycsb_gcs(self) -> Metric:
        title = '{}, {}'.format("Garbage Collections", self._title)
        metric_id = '{}_{}'\
            .format(self.test_config.name, "Garbage Collections".replace(' ', '_').casefold())
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=-1)
        gcs = self.ycsb_get_gcs()
        return gcs, self._snapshots, metric_info

    def ycsb_failed_ops(self,
                        io_type: str,
                        failures: int,) -> Metric:
        type = io_type + " Failures"
        title = '{} {}'.format(type, self._title)
        metric_id = '{}_{}'.format(self.test_config.name, type.replace(' ', '_').casefold())
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=-1)
        return failures, self._snapshots, metric_info

    def ycsb_slo_max_latency(self,
                             io_type: str,
                             max_latency: int,) -> Metric:

        max_type = "Max " + io_type + " Latency (ms)"
        title = '{}, {}'.format(max_type, self._title)
        metric_id = '{}_{}'.format(self.test_config.name, max_type.replace(' ', '_')
                                   .replace('(', '').replace(')', '').casefold())
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=-1)

        return max_latency, self._snapshots, metric_info

    def dcp_throughput(self,
                       time_elapsed: float,
                       clients: int,
                       stream: str) -> Metric:
        metric_info = self._metric_info(chirality=1)
        if stream == 'all':
            throughput = round(
                (self.test_config.load_settings.items * clients) / time_elapsed)
        else:
            throughput = round(
                self.test_config.load_settings.items / time_elapsed)

        return throughput, self._snapshots, metric_info

    def fragmentation_ratio(self, ratio: float) -> Metric:
        metric_info = self._metric_info()

        return ratio, self._snapshots, metric_info

    def elapsed_time(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=-1)

        time_elapsed = s2m(time_elapsed)

        return time_elapsed, self._snapshots, metric_info

    def cluster_deployment_time(self, deployment_time, prefix, title) -> Metric:
        metric_id = f'{self.test_config.name}_{prefix}'
        metric_title = f'{title},{",".join(self._title.split(",")[2:])}'
        metric_info = self._metric_info(metric_id, metric_title)
        time = round(float(deployment_time), 2)

        return time, self._snapshots, metric_info

    def kv_throughput(self, total_ops: int) -> Metric:
        metric_info = self._metric_info(chirality=1)

        throughput = total_ops // self.test_config.access_settings.time

        return throughput, self._snapshots, metric_info

    def ycsb_throughput(self, operation: str = "access") -> Metric:
        metric_info = self._metric_info(chirality=1)

        throughput = self._parse_ycsb_throughput(operation)

        return throughput, self._snapshots, metric_info

    def pytpcc_tpmc_throughput(self, duration: int) -> Metric:

        metric_info = self._metric_info(chirality=1)

        executed = self._parse_pytpcc_throughput()

        tpmc = round(executed / duration * 60)

        return tpmc, self._snapshots, metric_info

    def ycsb_throughput_phase(self,
                              phase: int,
                              workload: str,
                              operation: str = "access"
                              ) -> Metric:

        title = '{}, {}, Phase {}, {}'.format(
            "Avg Throughput (ops/sec)", self._title, phase, workload)
        metric_id = '{}_{}_{}'.format(self.test_config.name, workload.replace(' ', '_').casefold(),
                                      phase)
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=1)

        throughput = self._parse_ycsb_throughput(operation)

        return throughput, self._snapshots, metric_info

    def ycsb_durability_throughput(self) -> Metric:
        title = '{}, {}'.format("Avg Throughput (ops/sec)", self._title)
        metric_id = '{}_{}'\
            .format(self.test_config.name, "Avg Throughput".replace(' ', '_').casefold())
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=1)

        throughput = self._parse_ycsb_throughput()

        return throughput, self._snapshots, metric_info

    def ycsb_latency(self,
                     io_type: str,
                     latency: int,
                     ) -> Metric:
        title = '{} {}'.format(io_type, self._title)
        metric_id = '{}_{}'.format(self.test_config.name, io_type.replace(' ', '_').casefold())
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=-1)
        return latency, self._snapshots, metric_info

    def ycsb_latency_phase(self,
                           io_type: str,
                           latency: int,
                           phase: int,
                           workload: str
                           ) -> Metric:
        title = '{} Latency(ms), {}, Phase {}, {}'.format(io_type, self._title, phase, workload)
        metric_id = '{}_{}_{}_{}'.\
            format(self.test_config.name, workload.replace(' ', '_').casefold(),
                   phase, io_type.replace(' ', '_').casefold())
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=-1)
        return latency, self._snapshots, metric_info

    def ycsb_slo_latency(self,
                         io_type: str,
                         latency: int,
                         ) -> Metric:
        title = '{} Latency (ms), {}'.format(io_type, self._title)
        metric_id = '{}_{}'.format(self.test_config.name, io_type.replace(' ', '_')
                                   .replace('(', '').replace(')', '').casefold())
        metric_info = self._metric_info(title=title, metric_id=metric_id, chirality=-1)
        return latency, self._snapshots, metric_info

    def ycsb_get_latency(self,
                         percentile: str,
                         operation: str = "access"
                         ) -> Metric:
        latency_dic = self._parse_ycsb_latency(percentile, operation)
        return latency_dic

    def indexing_time(self, indexing_time: float) -> Metric:
        return self.elapsed_time(indexing_time)

    @property
    def rebalance_order_by(self) -> str:
        order_by = ''
        for num_nodes in self.test_config.cluster.initial_nodes:
            order_by += '{:03d}'.format(num_nodes)

        order_by += '{:018d}'.format(self.test_config.load_settings.items)

        for num_nodes in self.test_config.rebalance_settings.nodes_after:
            order_by += '{:03d}'.format(num_nodes)

        return order_by

    def rebalance_time(self, rebalance_time: float) -> Metric:
        metric = self.elapsed_time(rebalance_time)
        metric[-1]["orderBy"] = self.rebalance_order_by + self._order_by
        return metric

    def failover_time(self, delta: float) -> Metric:
        metric_info = self._metric_info(chirality=-1)

        return delta, self._snapshots, metric_info

    def failure_detection_time(self, delta: float) -> Metric:
        title_split = self._title.split(sep=",", maxsplit=1)
        title = "[{}] Failure detection time (s),{}".format(title_split[0], title_split[1])
        metric_id = '{}_detection_time'.format(self.test_config.name)
        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=-1)

        return delta, self._snapshots, metric_info

    def autofailover_time(self, delta: float) -> Metric:
        title_split = self._title.split(sep=",", maxsplit=1)
        title = "[{}] Auto failover time (ms),{}".format(title_split[0], title_split[1])
        metric_id = '{}_failover_time'.format(self.test_config.name)
        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=-1)

        return delta, self._snapshots, metric_info

    def scan_throughput(self, throughput: float, metric_id_append_str: str = None,
                        title: str = None, update_category: bool = True) -> Metric:
        metric_info = self._metric_info()
        if metric_id_append_str is not None:
            metric_id = '{}_{}'.format(self.test_config.name, metric_id_append_str)
            metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=1)
        if update_category:
            metric_info['category'] = "thr"

        throughput = round(throughput, 1)

        return throughput, self._snapshots, metric_info

    def multi_scan_diff(self, time_diff: float):
        metric_info = self._metric_info(chirality=-1)

        time_diff = round(time_diff, 2)

        return time_diff, self._snapshots, metric_info

    def get_functions_throughput(self, time: int, event_name: str, events_processed: int) -> float:
        throughput = 0
        if event_name:
            for name, file in self.test.functions.items():
                for node in self.test.eventing_nodes:
                    throughput += self.test.rest.get_num_events_processed(
                        event=event_name, node=node, name=name)
        else:
            throughput = events_processed
        throughput /= len(self.test.functions)
        throughput /= time
        return round(throughput, 0)

    def function_throughput(self, time: int, event_name: str, events_processed: int) -> Metric:
        metric_info = self._metric_info(chirality=1)

        throughput = self.get_functions_throughput(time, event_name, events_processed)

        return throughput, self._snapshots, metric_info

    def function_throughput_sg(self, time: int, event_name: str, events_processed: int) -> Metric:
        metric_id = f'{self.test_config.name}_eventing_throughput'
        metric_title = f'Eventing Throughput{self._title}'
        metric_info = self._metric_info(metric_id, metric_title, chirality=1)

        throughput = self.get_functions_throughput(time, event_name, events_processed)

        return throughput, self._snapshots, metric_info

    def eventing_rebalance_time(self, time: int) -> Metric:
        title_split = self._title.split(sep=",", maxsplit=1)
        title = "Rebalance Time(sec)," + title_split[1]
        metric_id = f'{self.test_config.name}_rebalance_time'
        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=-1)
        return time, self._snapshots, metric_info

    def magma_benchmark_metrics(self, throughput: float, precision: int, benchmark: str) -> Metric:
        title = "{}, {}".format(benchmark, self._title)
        metric_id = '{}_{}'.format(self.test_config.name,
                                   benchmark.replace(" ", "_").replace(",", "").replace("%", ""))
        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=1)
        return round(throughput, precision), self._snapshots, metric_info

    @staticmethod
    def eventing_get_percentile_latency(percentile: float, stats: dict) -> float:
        """Calculate percentile latency.

        We get latency stats in format of- time:number of events processed in that time(samples)
        In this method we get latency stats then calculate percentile latency using-
        Calculate total number of samples.
        Keep adding sample until we get required percentile number.
        For now it calculates for one function only
        """
        latency = 0

        total_samples = sum([sample for time, sample in stats])
        latency_samples = 0
        for time, samples in stats:
            latency_samples += samples
            if latency_samples >= (total_samples * percentile / 100):
                latency = float(time) / 1000
                break

        latency = round(latency, 1)
        return latency

    def function_latency(self, percentile: float, latency_stats: dict) -> Metric:
        """Calculate eventing function latency from stats."""
        metric_info = self._metric_info(chirality=-1)
        latency = 0
        curl_latency = 0
        for name, stats in latency_stats.items():
            if name.startswith("curl_latency_"):
                curl_latency = self.eventing_get_percentile_latency(percentile, stats)
                logger.info(f'Curl percentile latency is {curl_latency}ms')
            else:
                latency = self.eventing_get_percentile_latency(percentile, stats)
                logger.info(f'On update percentile latency is {latency}ms')

        latency -= curl_latency
        latency = round(latency, 1)
        return latency, self._snapshots, metric_info

    def function_time(self, time: int, time_type: str, initials: str, unit: str = "min") -> Metric:
        title = initials + ", " + self._title
        metric_id = f'{self.test_config.name}_{time_type.lower()}'
        metric_info = self._metric_info(metric_id=metric_id, title=title, chirality=-1)
        if unit == "min":
            time = s2m(seconds=time)

        return time, self._snapshots, metric_info

    def analytics_latency(self, query: Query, latency: int) -> Metric:
        metric_id = self.test_config.name + strip(query.description)

        title = 'Avg. query latency (ms), {} {}, {}'.format(query.id,
                                                            query.description,
                                                            self._title)

        order_by = '{}_{:05d}_{}'.format(query.id[:2], int(query.id[2:]), self._order_by)

        metric_info = self._metric_info(metric_id,
                                        title,
                                        order_by,
                                        chirality=-1)

        return latency, self._snapshots, metric_info

    def analytics_avg_connect_time(self, avg_connect_time: int) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, "connect")

        title = 'Avg. connect time (sec), {}'.format(self._title)

        metric_info = self._metric_info(metric_id,
                                        title,
                                        chirality=-1)

        return round(avg_connect_time, 1), self._snapshots, metric_info

    def analytics_avg_disconnect_time(self, avg_disconnect_time: int) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, "disconnect")

        title = 'Avg. disconnect time (sec), {}'.format(self._title)

        metric_info = self._metric_info(metric_id,
                                        title,
                                        chirality=-1)

        return round(avg_disconnect_time, 1), self._snapshots, metric_info

    def analytics_volume_latency(self,
                                 query: Query,
                                 latency: int,
                                 with_index: bool = False) -> Metric:
        metric_id = self.test_config.name + strip(query.description)

        title = 'Avg. query latency (ms), {} {}, {}'
        if with_index:
            title = 'Avg. query latency (ms), {} {} with index, {}'

        title = title.format(query.id, query.description, self._title)

        order_by = '{}_{:05d}_{}'.format(query.id[:2], int(query.id[2:]), self._order_by)

        metric_info = self._metric_info(metric_id,
                                        title,
                                        order_by,
                                        chirality=-1)

        return latency, self._snapshots, metric_info

    def get_max_rss_values(self, function_name: str, server: str):
        ratio = 1024 * 1024
        if self.cluster_spec.serverless_infrastructure:
            function_name = self.test_config.serverless_db.db_map[function_name]['name']
        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector='eventing_consumer_stats',
                                     bucket=function_name, server=server)
        rss_list = self.store.get_values(db, metric='eventing_consumer_rss')
        max_consumer_rss = round(max(rss_list) / ratio, 2)

        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector='atop', server=server)
        rss_list = self.store.get_values(db, metric='eventing-produc_rss')
        max_producer_rss = round(max(rss_list) / ratio, 2)

        return max_consumer_rss, max_producer_rss

    def avg_ingestion_rate(self, num_items: int, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        rate = round(num_items / time_elapsed)

        return rate, self._snapshots, metric_info

    def ingestion_time(self, time_elapsed: float, ingest_method: str) -> Metric:
        return self.custom_metric(
            round(time_elapsed, 2),
            f"Data ingestion time (sec), {ingest_method.replace('_', ' ').title()}, {{}}",
            f"ingest_time_{ingest_method.lower().replace(' ', '_')}",
            chirality=-1,
        )

    def avg_drop_rate(self, num_items: int, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        rate = round(num_items / time_elapsed)

        return rate, self._snapshots, metric_info

    def compression_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(chirality=1)

        throughput = round(self.test_config.load_settings.items / time_elapsed)

        return throughput, self._snapshots, metric_info

    def ch2_metrics(self, duration: float, logfile: str) -> CH2Metrics:
        filename = logfile + '.log'
        metrics = CH2Metrics(average_cbas_query_set_time=duration)
        with open(filename) as fh:
            for line in fh.readlines():
                if 'NEW_ORDER' in line and 'success' in line:
                    elements = line.split()
                    # "total_txn_time" is the number of microseconds spent executing NEW_ORDER txns
                    # by all tclients (so wall-clock time spent is total_txn_time / tclients)
                    metrics.total_txn_time_us = float(elements[2].split(".")[0])
                    metrics.txn_success_count = int("".join(filter(str.isdigit, elements[-1])))
                if "OVERALL GEOMETRIC MEAN" in line:
                    metrics.geo_mean_cbas_query_time = float(line.split()[-1])
                if 'AVERAGE TIME PER QUERY SET' in line:
                    metrics.average_cbas_query_set_time = float(line.split()[-1])
                if "QUERIES PER HOUR" in line:
                    metrics.cbas_qph = float(line.split()[-1])
        return metrics

    def custom_metric(
        self, value: float, title_template: str, metric_id_suffix: str, chirality: int = 1
    ) -> Metric:
        """Return a simple metric with custom title and metric ID.

        Args:
            value (float): Metric value that will be used without any modification.
            title_template (str): Template for metric title, with a single placeholder for the
            MetricHelper._title value.
            metric_id_suffix (str): Suffix to append to the test config name to form the metric ID.
            chirality (int): Metric chirality (-1 if smaller values are better, 1 otherwise).
        """
        metric_id = f"{self.test_config.name}_{metric_id_suffix}"
        title = title_template.format(self._title)

        metric_info = self._metric_info(metric_id, title, chirality=chirality)

        return value, self._snapshots, metric_info

    def ch2_tpm(self, tpm: float, tclients: int, extra_metric_id_suffix: str = "") -> Metric:
        return self.custom_metric(
            tpm,
            f"Transactions per minute (tpm), {{}}, {tclients} tclients",
            f"tmp{'_' + extra_metric_id_suffix if extra_metric_id_suffix else ''}",
            chirality=1,
        )

    def ch2_response_time(
        self, response_time: float, tclients: int, extra_metric_id_suffix: str = ""
    ) -> Metric:
        return self.custom_metric(
            response_time,
            f"Average response time (sec), {{}}, {tclients} tclients",
            f"response_time{'_' + extra_metric_id_suffix if extra_metric_id_suffix else ''}",
            chirality=-1,
        )

    def ch2_geo_mean_query_time(
        self, query_time: float, tclients: int, extra_metric_id_suffix: str = ""
    ) -> Metric:
        return self.custom_metric(
            query_time,
            f"Geo-mean analytics query time (sec), {{}}, {tclients} tclients",
            f"geo_mean_query_time{'_' + extra_metric_id_suffix if extra_metric_id_suffix else ''}",
            chirality=-1,
        )

    def ch2_analytics_query_set_time(
        self, query_set_time: float, tclients: int, extra_metric_id_suffix: str = ""
    ) -> Metric:
        return self.custom_metric(
            query_set_time,
            f"Average time per analytics query set (sec), {{}}, {tclients} tclients",
            f"analytics_query_time{'_' + extra_metric_id_suffix if extra_metric_id_suffix else ''}",
            chirality=-1,
        )

    def ch2_analytics_qph(
        self, qph: float, tclients: int, extra_metric_id_suffix: str = ""
    ) -> Metric:
        return self.custom_metric(
            qph,
            f"Analytics queries per hour, {{}}, {tclients} tclients",
            f"analytics_qph{'_' + extra_metric_id_suffix if extra_metric_id_suffix else ''}",
        )

    def ch3_metrics(self, duration: float, logfile: str) -> CH3Metrics:
        filename = logfile + '.log'
        metrics = CH3Metrics(average_cbas_query_set_time=duration)
        with open(filename) as fh:
            for line in fh.readlines():
                if 'NEW_ORDER' in line and 'success' in line:
                    elements = line.split()
                    # For CH3 the txn times are in ms not us so we have to convert
                    metrics.total_txn_time_us = float(elements[2].split(".")[0]) * 1000
                    metrics.txn_success_count = int("".join(filter(str.isdigit, elements[-1])))
                if 'AVERAGE TIME PER ANALYTICS QUERY SET' in line:
                    metrics.average_cbas_query_set_time = float(line.split()[-1])
                if 'AVERAGE TIME PER FTS QUERY SET' in line:
                    metrics.average_fts_query_set_time_ms = float(line.split()[-1])
                if 'AVERAGE TIME PER FTS CLIENT' in line:
                    metrics.average_fts_client_time_ms = float(line.split()[-1])
                if 'FTS QUERIES PER HOUR' in line:
                    metrics.fts_qph = float(line.split()[-1])
        return metrics

    def ch3_fts_query_time(self, query_time: float, tclients: int) -> Metric:
        return self.custom_metric(
            query_time,
            f"Average time per fts query set (sec), {{}}, {tclients} tclients",
            "fts_query_time",
            chirality=-1,
        )

    def ch3_fts_client_time(self, client_time: float, tclients: int) -> Metric:
        return self.custom_metric(
            client_time,
            f"Average time per fts client (sec), {{}}, {tclients} tclients",
            "fts_client_time",
            chirality=-1,
        )

    def ch3_fts_qph(self, qph: float, tclients: int) -> Metric:
        return self.custom_metric(
            qph, f"FTS queries per hour (Qph), {{}}, {tclients} tclients", "fts_qph"
        )

    def custom_elapsed_time(self, time_elapsed: float, op: str) -> Metric:
        return self.custom_metric(
            round(time_elapsed, 1),
            f"Time elapsed (sec), {op.replace('_', ' ').title()}, {{}}",
            f"{op}_time",
            chirality=-1,
        )

    def sgimport_latency(self, percentile: Number = 95) -> Metric:
        metric_id = '{}_{}th_sgimport_latency'.format(self.test_config.name, percentile)
        title = '{}th percentile sgimport latency (ms), {}'.format(
            percentile, self._title)
        metric_info = self._metric_info(metric_id, title)
        values = []

        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector='sgimport_latency')
        logger.info("db: {}, cluster: {}".format(db, self.test.cbmonitor_clusters[0]))
        values += self.store.get_values(db, metric='sgimport_latency')
        lag = round(np.percentile(values, percentile), 2)
        return lag, self._snapshots, metric_info

    def sgimport_items_per_sec(self, time_elapsed: float, items_in_range: int,
                               operation: str) -> Metric:
        title = 'Average throughput (docs/sec) {}, {}'.format(operation, self._title)
        metric_id = '{}_{}_{}'.format(
            self.test_config.name, "throughput", operation)
        metric_info = self._metric_info(title=title, metric_id=metric_id)
        items_in_range = items_in_range
        rate = round(items_in_range / time_elapsed)
        return rate, self._snapshots, metric_info

    def sgreplicate_items_per_sec(self, time_elapsed: float, items_in_range: int) -> Metric:
        items_in_range = items_in_range
        metric_info = self._metric_info()
        logger.info("*** {} {} ***".format(items_in_range, time_elapsed))
        rate = round(items_in_range / time_elapsed)
        return rate, self._snapshots, metric_info

    def _parse_sg_throughput(self, operation: str = "access") -> int:
        throughput = 0
        if operation == "load_":
            pattern = "YCSB/*loaddocs*.result"
        elif operation == "load_users_":
            pattern = "YCSB/*loadusers*.result"
        else:
            pattern = "YCSB/*_runtest_*.result"
        for filename in glob.glob(pattern):
            with open(filename) as fh:
                for line in fh.readlines():
                    if line.startswith('[OVERALL], Throughput(ops/sec)'):
                        throughput += float(line.split()[-1])
        if throughput < 1:
            throughput = round(throughput, 2)
        else:
            throughput = round(throughput, 0)
        return throughput

    def _sg_bp_total_docs_pulled(self) -> int:
        total_docs = 0
        for filename in glob.glob("sg_stats_blackholepuller_*.json"):
            total_docs_pulled_per_file = 0
            with open(filename) as fh:
                content_lines = fh.readlines()
                for i in content_lines:
                    if "docs_pulled" in i:
                        total_docs_pulled_per_file += int(i.split(':')[1].split(',')[0])
            total_docs += total_docs_pulled_per_file
        return total_docs

    def _sg_bp_total_docs_pushed(self) -> int:
        total_docs = 0
        for filename in glob.glob("sg_stats_newdocpusher_*.json"):
            total_docs_pulled_per_file = 0
            with open(filename) as fh:
                content_lines = fh.readlines()
                for i in content_lines:
                    if "docs_pushed" in i:
                        total_docs_pulled_per_file += int(i.split(':')[1].split(',')[0])
            total_docs += total_docs_pulled_per_file
        return total_docs

    def _parse_sg_bp_throughput(self) -> int:
        throughput = 0
        fc = 0
        sum_doc_per_sec = 0
        for filename in glob.glob("sg_stats_blackholepuller_*.json"):
            total_docs_per_sec = 0
            with open(filename) as fh:
                content_lines = fh.readlines()
                c = 0
                for i in content_lines:
                    if "docs_per_sec" in i:
                        c += 1
                        total_docs_per_sec += float(i.split(':')[1])
                average_doc_per_sec = total_docs_per_sec / c
            fc += 1
            sum_doc_per_sec += average_doc_per_sec
        throughput = sum_doc_per_sec / fc
        return throughput

    def _get_num_replications(self, documents: int) -> int:
        num_replications = 0
        total_docs = 0
        for filename in glob.glob("sg_stats_blackholepuller_*.json"):
            total_docs_pulled_per_file = 0
            with open(filename) as fh:
                content_lines = fh.readlines()
                for i in content_lines:
                    if "docs_pulled" in i:
                        total_docs_pulled_per_file += int(i.split(':')[1].split(',')[0])
            total_docs += total_docs_pulled_per_file
        num_replications = round(total_docs / documents)
        return num_replications

    def _parse_newdocpush_throughput(self) -> int:
        throughput = 0
        fc = 0
        sum_doc_per_sec = 0
        for filename in glob.glob("sg_stats_newdocpusher_*.json"):
            total_docs_per_sec = 0
            with open(filename) as fh:
                content_lines = fh.readlines()
                c = 0
                for i in content_lines:
                    if "docs_per_sec" in i:
                        c += 1
                        total_docs_per_sec += float(i.split(':')[1].split(',')[0])
                average_doc_per_sec = total_docs_per_sec / c
            fc += 1
            sum_doc_per_sec += average_doc_per_sec
        throughput = sum_doc_per_sec / fc
        return throughput

    def _parse_sg_latency(self, metric_name) -> float:
        lat = 0
        count = 0
        for filename in glob.glob("YCSB/*_runtest_*.result"):
            with open(filename) as fh:
                for line in fh.readlines():
                    if line.startswith(metric_name):
                        lat += float(line.split()[-1])
                        count += 1
        if count > 0:
            return lat / count
        return 0

    def parses_sg_failures(self):
        failed_ops = 0
        for filename in glob.glob("YCSB/*_runtest_*.result"):
            with open(filename) as fh:
                for line in fh.readlines():
                    if 'FAILED' in line:
                        failed_ops = 1
                        break
        if failed_ops == 1:
            return 1
        else:
            return 0

    def sg_throughput(self, title, operation: str = "") -> Metric:
        metric_id = f'{self.test_config.name}_{operation}throughput'
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        throughput = self._parse_sg_throughput(operation)
        return throughput, self._snapshots, metric_info

    def avg_sg_cpu_usage(self, title) -> Metric:
        metric_id = '{}_Average_sg_cpu_usage'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        values = []

        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector='syncgateway_cluster_stats')

        if self.cluster_spec.capella_infrastructure:
            values += self.store.get_values(
                db,
                metric='sgw_resource_utilization_process_cpu_percent_utilization')
        else:
            values += self.store.get_values(
                db,
                metric='syncgateway__global__resource_utilization__process_cpu_percent_utilization')
        avg_cpu = round(np.average(values) / 100, 2)
        return avg_cpu, self._snapshots, metric_info

    def avg_sg_mem_usage(self, title) -> Metric:
        metric_id = '{}_Average_sg_memory_usage'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        values = []

        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector=' syncgateway_cluster_stats')
        if self.cluster_spec.capella_infrastructure:
            values += self.store.get_values(
                db,
                metric='sgw_resource_utilization_process_memory_resident')
        else:
            values += self.store.get_values(
                db,
                metric='syncgateway__global__resource_utilization__process_memory_resident')
        avg_mem = round(np.average(values), 2)
        avg_mem = int(avg_mem / (1024 ** 2))
        return avg_mem, self._snapshots, metric_info

    def sg_bp_throughput(self, title) -> Metric:
        metric_id = '{}_throughput'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        throughput = round(self._parse_sg_bp_throughput())
        return throughput, self._snapshots, metric_info

    def sg_newdocpush_throughput(self, title) -> Metric:
        metric_id = '{}_throughput'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        throughput = round(self._parse_newdocpush_throughput())
        return throughput, self._snapshots, metric_info

    def sg_bp_total_docs_pulled(self, title, duration) -> Metric:
        metric_id = '{}_docs_pulled'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        docs_pulled_per_sec = round(self._sg_bp_total_docs_pulled() / duration)
        return docs_pulled_per_sec, self._snapshots, metric_info

    def sg_bp_total_docs_pushed(self, title, duration) -> Metric:
        metric_id = '{}_docs_pushed'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        docs_pulled_per_sec = round(self._sg_bp_total_docs_pushed() / duration)
        return docs_pulled_per_sec, self._snapshots, metric_info

    def sg_bp_num_replications(self, title, documents: int) -> Metric:
        metric_id = '{}_num_replications'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        num_replications = self._get_num_replications(documents)
        return num_replications, self._snapshots, metric_info

    def sg_latency(self, metric_name, title) -> Metric:
        metric_id = '{}_latency'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)

        lat = float(self._parse_sg_latency(metric_name) / 1000)
        if lat < 10:
            lat = round(lat, 2)
        else:
            lat = round(lat)

        return lat, self._snapshots, metric_info

    def deltasync_time(self, replication_time: float) -> Metric:
        title = 'Replication time (sec) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "time")
        metric_info = self._metric_info(title=title, metric_id=metric_id)
        replication_time = round(replication_time, 3)
        return replication_time, self._snapshots, metric_info

    def deltasync_throughput(self, throughput: int) -> Metric:
        title = 'Throughput (docs/sec) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "throughput")
        metric_info = self._metric_info(title=title, metric_id=metric_id)

        return throughput, self._snapshots, metric_info

    def deltasync_bandwidth(self, bandwidth: float) -> Metric:
        title = 'Bandwidth Usage (MB/sec) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "bandwidth")
        metric_info = self._metric_info(title=title, metric_id=metric_id)
        return bandwidth, self._snapshots, metric_info

    def deltasync_bytes(self, bytes: float) -> Metric:
        title = 'Bytes Transfer (MB) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "Mbytes")
        metric_info = self._metric_info(title=title, metric_id=metric_id)
        # in MB
        bytes = round(((bytes/1024)/1024), 2)
        return bytes, self._snapshots, metric_info

    def sgw_e2e_throughput(self, throughput: int,
                           operation: str, replication: str) -> Metric:
        title = 'SGW {} {} Throughput (docs/sec) {}'.format(
            replication.lower(), operation, self._title)
        metric_id = '{}_{}_{}_{}'.format(
            self.test_config.name, "throughput", operation, replication.lower())
        metric_info = self._metric_info(title=title, metric_id=metric_id)
        return round(throughput), self._snapshots, metric_info

    def sgw_e2e_throughput_per_cblite(self, throughput: int,
                                      operation: str, replication: str) -> Metric:
        title = 'SGW {} {} Throughput (docs/sec) per cblite {}'.format(
            replication.lower(), operation, self._title)
        metric_id = '{}_{}_{}_{}'.format(
            self.test_config.name, "throughput_per_cblite", operation, replication.lower())
        metric_info = self._metric_info(title=title, metric_id=metric_id)
        return round(throughput), self._snapshots, metric_info

    def sdk_bench_config_push_time(self, failure_time: tuple, benchmark_name: str,
                                   time_func) -> Metric:
        sdk_type = self.test_config.sdktesting_settings.sdk_type[-1]
        filename = 'sdks/{}/{}_stdout.log'.format(sdk_type, benchmark_name)
        write_unavailable_time = self._parse_sdk_benchmark_logs(filename, failure_time, time_func)
        return write_unavailable_time, self._snapshots, self._metric_info()

    def _parse_sdk_benchmark_logs(self, filename: str, failure_time: tuple, time_func) -> float:
        # Process:
        # 1. Find the first FAILURE log line after failure initiation. This is the start of the
        # logs were interested in.
        # 2. From there find the first SUCCESS
        first_failure_time = 0
        failure_start_found = False
        lines = []
        with open(filename) as file:
            lines = file.readlines()

        # Ignore any temporary errors that occurred before the actual failure and then
        # followed by a success. Here we have no way of knowing which timeout was actually applied,
        # but assume the first one is the minimum possible padding we can add.
        if len(lines) <= 5:
            # In some testing we avoided using logging for better performance, and only recorded
            # the processed times
            logger.info(lines)
            first_failure_time = int(lines[0].strip().split()[-1])
            if first_failure_time < failure_time[1]:
                logger.warn('Failure happened before failover time')
                return 0
            return float(lines[2].strip().split()[-1])
        else:
            # Here we are dealing with raw logs, which can be huge, avoid printing and
            # only process the necessary part
            for line in lines:
                try:
                    log_time = time_func(line.split(',')[0])
                except Exception:
                    continue
                log_msg = line.split(',')[1].strip()
                if not failure_start_found and log_time > failure_time[0] \
                        and log_msg.startswith('[FAILURE]'):
                    failure_start_found = True
                    first_failure_time = log_time
                    logger.info('Failure start: "{}"'.format(line.rstrip()))
                elif failure_start_found and log_msg.startswith('[SUCCESS] Store') \
                    and log_time > failure_time[0]:
                    logger.info('Write success start: "{}"'.format(line.rstrip()))
                    return round(log_time - failure_time[0], 2)

        raise Exception('Benchmark didnot recover from failure' if failure_start_found else
                    'No failures found, either no failover happened or timout is too large')

    def vectordb_bench_metrics(self, metrics: dict, base_title: str, case_id: str) -> list[Metric]:
        """Take vectorDBBench json metrics results and returns a list of showfast metrics."""
        reported_metrics = []
        proper_names = {
            "qps": "Throughput (queries/sec)",
            "serial_latency_p99": "99th percentile latency (ms)",
            "load_duration": "Load duration (s)",
        }
        for name, result in metrics.items():
            value = float(result)
            if value == 0:
                continue
            if name == "serial_latency_p99":  # to ms
                value = round(value * 1000)
            else:
                value = round(value, 2)
            metric_id = f"{case_id}_{name}"
            p_name = proper_names.get(name, name).replace("_", " ").capitalize()
            title = f"{p_name}, {base_title}, {self._title}"
            metric_info = self._metric_info(metric_id, title, order_by=name)

            reported_metrics.append((value, self._snapshots, metric_info))

        return reported_metrics


class DailyMetricHelper(MetricHelper):

    def indexing_time(self, time_elapsed: float) -> DailyMetric:
        return 'Initial Indexing Time (min)', \
            s2m(time_elapsed), \
            self._snapshots

    def dcp_throughput(self,
                       time_elapsed: float,
                       clients: int,
                       stream: str) -> DailyMetric:
        if stream == 'all':
            throughput = round(
                (self.test_config.load_settings.items * clients) / time_elapsed)
        else:
            throughput = round(
                self.test_config.load_settings.items / time_elapsed)

        return 'Avg Throughput (items/sec)', \
               throughput, \
               self._snapshots

    def rebalance_time(self, rebalance_time: float) -> DailyMetric:
        return 'Rebalance Time (min)', \
            s2m(rebalance_time), \
            self._snapshots

    def avg_n1ql_throughput(self, master_node: str) -> DailyMetric:
        return 'Avg Query Throughput (queries/sec)', \
            self._avg_n1ql_throughput(master_node), \
            self._snapshots

    def max_ops(self) -> list[DailyMetric]:
        metrics = [('Max Throughput (ops/sec)', self._max_ops(), self._snapshots)]
        for stat_group in self.test_config.collection.collection_stat_groups:
            throughput = self._max_ops(collector='metrics_rest_api_collection_throughput',
                                       stat_group=stat_group,
                                       metric='kv_collection_ops')
            metric_title = 'Max throughput per collection (ops/sec) ({})'.format(stat_group)
            metrics.append((metric_title, throughput, self._snapshots))
        return metrics

    def avg_replication_rate(self, time_elapsed: float) -> DailyMetric:
        return 'Avg XDCR Rate (items/sec)', \
            super()._avg_replication_rate(time_elapsed), \
            self._snapshots

    def ycsb_throughput(self) -> DailyMetric:
        return 'Avg Throughput (ops/sec)', \
            self._parse_ycsb_throughput(), \
            self._snapshots

    def backup_throughput(self, time_elapsed: float) -> DailyMetric:
        data_size = self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2 ** 20  # MB
        throughput = round(data_size / time_elapsed)

        return 'Avg Throughput (MB/sec)', \
            throughput, \
            self._snapshots

    def jts_throughput(self) -> DailyMetric:
        timings = self._jts_metric(collector="jts_stats", metric="jts_throughput")
        throughput = round(np.average(timings), 2)
        if throughput > 100:
            throughput = round(throughput)

        return 'Avg Query Throughput (queries/sec)', \
            throughput, \
            self._snapshots

    def function_throughput(self, time: int, event_name: str,
                            events_processed: int) -> DailyMetric:
        throughput = self.get_functions_throughput(time, event_name, events_processed)

        metric = "Avg Throughput (functions executed/sec)"
        if "timer" in self._title:
            metric = "Avg Throughput (timers executed/sec)"

        return metric, \
            throughput, \
            self._snapshots

    def avg_ingestion_rate(self, num_items: int, time_elapsed: float) -> DailyMetric:
        rate = round(num_items / time_elapsed)
        return "Avg Ingestion Rate (items/sec)", rate, self._snapshots

    def analytics_latency(self, query: Query, latency: int) -> DailyMetric:
        matches = query.description.split("(")[1].split(")")[0]
        metric = 'Avg Latency {} {}'.format(query.id, matches)
        return metric, latency,  self._snapshots

    def magma_benchmark_metrics(self, throughput: float, precision: int, benchmark: str) -> Metric:
        return benchmark, round(throughput, precision), self._snapshots

    def kv_latency(self,
                   operation: str,
                   percentiles: Iterable[Number] = [99.9],
                   collector: str = 'spring_latency') -> list[DailyMetric]:
        metrics = []
        stat_groups = self.test_config.collection.collection_stat_groups or ['']
        for stat_group in stat_groups:
            latencies = self._kv_latency(operation, percentiles, collector, stat_group)
            for percentile, latency in zip(percentiles, latencies):
                metric_title = '{}th percentile {}{}'.format(
                    percentile,
                    operation.upper(),
                    ' (' + stat_group + ')' if stat_group != '' else ''
                )

                metrics.append((metric_title, latency, self._snapshots))

        return metrics

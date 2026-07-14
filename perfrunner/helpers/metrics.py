from __future__ import annotations

import glob
import os
import re
import statistics
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from cbagent.stores import PerfStore
from logger import logger
from perfrunner.helpers.local_stats import consolidate_jts_log, parse_spring_latency_file
from perfrunner.helpers.misc import sort_bucket_key
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


def bytes_to_mib(num_bytes: float) -> float:
    """Convert bytes to mebibytes (MiB)."""
    return num_bytes / 1024 ** 2


def bytes_to_gib(num_bytes: float) -> float:
    """Convert bytes to gibibytes (GiB)."""
    return num_bytes / 1024 ** 3


def ns_to_ms(nanoseconds: float) -> float:
    """Convert nanoseconds to milliseconds."""
    return nanoseconds / 1e6


def s_to_ms(seconds: float) -> float:
    """Convert seconds to milliseconds."""
    return seconds * 1000


def adaptive_round(value: float, *, threshold: float = 100, small_digits: int = 1) -> float:
    """Round to ``small_digits`` decimals below ``threshold``, else truncate to int.

    Folds the recurring ``round(x, 1) if x < 100 else int(x)`` latency idiom.
    """
    return round(value, small_digits) if value < threshold else int(value)


def strip(s: str) -> str:
    for c in ' &()':
        s = s.replace(c, '')
    return s.lower()


def calc_percentiles_fn(percentiles: list[float]) -> Callable[[np.ndarray[float]], list[float]]:
    def _calc_percentiles(w_timings: np.ndarray[float]) -> list[float]:
        return [
            round(latency)
            if (latency := np.percentile(w_timings[:, 1], p)) > 100
            else round(latency, 2)
            for p in percentiles
        ]

    return _calc_percentiles


@dataclass
class CH2Metrics:
    # transactions
    total_no_txn_time_us: float = 0
    no_txn_success_count: int = 0
    txn_workload_duration_secs: float = 0

    # analytical queries
    geo_mean_cbas_query_time_secs: float = 0
    average_cbas_query_set_time_secs: float = 0
    cbas_qph: float = 0

    @property
    def tpm(self) -> float:
        """Return NewOrder txns successfully executed per minute while other txns were running."""
        return (
            self.no_txn_success_count * 60 / self.txn_workload_duration_secs
            if self.txn_workload_duration_secs > 0
            else 0
        )

    @property
    def txn_response_time(self) -> float:
        """Return average NewOrder txn response time in seconds."""
        return (
            self.total_no_txn_time_us / 1e6 / self.no_txn_success_count
            if self.no_txn_success_count > 0
            else float("inf")
        )


@dataclass
class CH3Metrics(CH2Metrics):
    # fts queries
    average_fts_query_set_time_ms: float = 0
    average_fts_client_time_ms: float = 0
    fts_qph: float = 0


CHXMetrics = TypeVar("CHXMetrics", CH2Metrics, CH3Metrics)


@dataclass(frozen=True)
class TimeseriesWindow:
    start_ts: float = -1.0
    end_ts: float = float("inf")
    label: str = ""

    def apply(self, timeseries: np.ndarray) -> np.ndarray:
        timestamps = timeseries[:, 0]
        return timeseries[(timestamps >= self.start_ts) & (timestamps < self.end_ts)]


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

    def _metric(
        self,
        value,
        *,
        metric_id: Optional[str] = None,
        title: Optional[str] = None,
        order_by: Optional[str] = None,
        chirality: Optional[int] = None,
        mem_quota: Optional[int] = None,
        stat_group: str = "",
        extra: Optional[Dict[str, str]] = None,
    ) -> Metric:
        """Assemble a ``Metric`` triple (value, snapshots, metric_info) in one place.

        Folds the ``metric_info = self._metric_info(...); return value,
        self._snapshots, metric_info`` tail that every KPI repeats, plus the
        ``metric_info[...] = ...`` extras (subCategory/category/...).
        """
        metric_info = self._metric_info(
            metric_id,
            title,
            order_by=order_by,
            chirality=chirality,
            mem_quota=mem_quota,
            stat_group=stat_group,
        )
        if extra:
            metric_info.update(extra)
        return value, self._snapshots, metric_info

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

    def avg_n1ql_throughput(self, master_node: str, initial_throughput: str = None,
                            custom_title_postfix: str = None,
                            update_subcategory: bool = False) -> Metric:
        """Generate cluster total query throughput metric."""
        metric_id = '{}_avg_query_requests'.format(self.test_config.name)
        title = 'Avg. Query Throughput (queries/sec), {}'.format(self._title)
        if custom_title_postfix:
            title = f"{title} {custom_title_postfix}"

        if initial_throughput is not None:
            throughput = self._avg_n1ql_throughput(master_node) - initial_throughput
        else:
            throughput = self._avg_n1ql_throughput(master_node)
        extra = {"subCategory": "Throughput"} if update_subcategory else None
        return self._metric(
            throughput,
            metric_id=metric_id,
            title=title,
            order_by=self.query_id,
            chirality=1,
            extra=extra,
        )

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
        title = 'Avg. Query Throughput (queries/sec), during rebalance, {}'.format(self._title)

        throughput = int(total_requests / rebalance_time)
        return self._metric(
            throughput, metric_id=metric_id, title=title, order_by=self.query_id, chirality=1
        )

    def bulk_n1ql_throughput(self, time_elapsed: float) -> Metric:
        items = self.test_config.load_settings.items / 4
        throughput = round(items / time_elapsed)
        return self._metric(throughput, chirality=1)

    def n1ql_vector_recall_and_accuracy(self, k: int, probe: int,  value: float,
                                        metric: str, custom_title_postfix: str = None) -> Metric:
        metric_id = f'{metric}_{k}_{probe}_{self.test_config.name}'
        title_prefix = f"{metric}@{k}, probes-{probe} across 1000 queries"
        if custom_title_postfix:
            title_prefix = title_prefix + f" {custom_title_postfix}"
        metric_id = metric_id.replace('.', '')
        title = f'{title_prefix}, {self._title}'
        return self._metric(
            round(value, 3),
            metric_id=metric_id,
            title=title,
            chirality=-1,
            extra={"subCategory": "Recall"},
        )

    def fts_index(self, elapsed_time: float) -> Metric:
        metric_id = self.test_config.name
        title_temp = self._title
        title_test = title_temp.split("(sec), ")[1]
        title = 'Total Index build time(sec), {}'.format(title_test)

        index_time = round(elapsed_time, 1)

        return self._metric(index_time, metric_id=metric_id, title=title, chirality=-1)

    def fts_index_size(self, index_size_raw: int) -> Metric:
        metric_id = "{}_indexsize".format(self.test_config.name)
        title_temp = self._title
        title_test = title_temp.split("(sec), ")[1]
        title = 'Index size (MB), {}'.format(title_test)

        index_size_mb = int(bytes_to_mib(index_size_raw))

        return self._metric(index_size_mb, metric_id=metric_id, title=title, chirality=-1)

    def fts_index_with_latency(self, elapsed_time: float) -> Metric:
        metric_id = self.test_config.name.replace("latency", "index_time")
        title = 'Total Index build time(sec), {}'.format(self._title)
        index_time = round(elapsed_time, 1)
        return self._metric(
            index_time,
            metric_id=metric_id,
            title=title,
            chirality=-1,
            extra={"subCategory": "Index"},
        )

    def fts_size_with_latency(self, index_size_raw: int) -> Metric:
        metric_id = "{}_indexsize".format(self.test_config.name).replace("latency", "")
        title = 'Index size (MB), {}'.format(self._title)
        index_size_mb = int(bytes_to_mib(index_size_raw))
        return self._metric(
            index_size_mb,
            metric_id=metric_id,
            title=title,
            chirality=-1,
            extra={"subCategory": "Index"},
        )

    def jts_throughput(
        self,
        name_suffix: str = "",
        title_override: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Metric:
        metric_id = f'{self.test_config.name}_jts_throughput'
        if name_suffix:
            metric_id = f'{self.test_config.name}_{name_suffix}_jts_throughput'
        metric_id = metric_id.replace('.', '')
        title = f"Average Throughput (q/sec), {title_override or self._title}"
        timings = self._jts_metric(collector="jts_stats", metric="jts_throughput")
        thr = round(self._mean(timings), 2)
        thr = round(thr/(int(self.test_config.jts_access_settings.aggregation_buffer_ms)/1000))
        if thr > 100:
            thr = round(thr)
        return self._metric(thr, metric_id=metric_id, title=title, order_by=order_by, chirality=1)

    def jts_latency(
        self,
        percentile: int = 50,
        name_suffix: str = "",
        title_override: Optional[str] = None,
        order_by: Optional[str] = None,
    ) -> Metric:
        prefix = "Average latency (ms)"
        if percentile != 50:
            prefix = f"{percentile}th percentile latency (ms)"
        metric_id = f'{self.test_config.name}_jts_latency_{percentile}th'
        if name_suffix:
            metric_id = f'{self.test_config.name}_{name_suffix}_jts_latency_{percentile}th'
        metric_id = metric_id.replace('.', '')
        title = f"{prefix}, {title_override or self._title}"
        timings = self._jts_metric(
            collector="jts_stats", metric="jts_latency", percentile=percentile
        )
        lat = round(self._percentile(timings, percentile), 2)
        if lat > 100:
            lat = round(lat)
        return self._metric(lat, metric_id=metric_id, title=title, order_by=order_by, chirality=-1)

    def jts_recall_and_accuracy(self, value, metric, k_nearest_neighbour):
        metric_id = '{}_{}at{}'.format(self.test_config.name, metric, k_nearest_neighbour)
        title_prefix = "Average {}@{} across 1000 queries".format(metric, k_nearest_neighbour)
        metric_id = metric_id.replace('.', '')
        title = "{}, {}".format(title_prefix, self._title)
        return self._metric(
            round(value, 3),
            metric_id=metric_id,
            title=title,
            chirality=-1,
            extra={"subCategory": metric},
        )

    def _jts_metric(self, collector, metric, percentile=None):
        timings = []
        bucket_names = self.test_config.buckets
        if int(self.test_config.jts_access_settings.custom_num_buckets) > 0:
            bucket_names = self._custom_bucket_names
        bucket_metric_list = []
        for bucket in bucket_names:
            bucket_timings = self._local_jts_values(metric, bucket)
            bucket_metric = 0
            if not bucket_timings:
                logger.warning(f"No {metric} data found for bucket {bucket}")
            elif metric == "jts_latency":
                bucket_metric = round(self._percentile(bucket_timings, percentile), 2)
            elif metric == "jts_throughput":
                bucket_metric = round(self._mean(bucket_timings), 2)

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
        buckets = buckets or self.test_config.buckets
        values = []
        for bucket in buckets:
            bucket_group = '{}{}'.format(bucket, '_' + stat_group if stat_group != '' else '')
            return_ops = self._read_values(
                metric, collector, cluster_idx=cluster_idx, bucket=bucket_group
            )
            if not return_ops:  # bucket/stat-group absent for this cluster
                continue
            if values:
                values = [ops1 + ops2 for ops1, ops2 in zip(values, return_ops)]
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
            return int(self._mean(values))
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
            return int(self._percentile(values, percentile))
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

    def _read_values(
        self,
        metric: str,
        collector: str,
        *,
        cluster: Optional[str] = None,
        cluster_idx: int = 0,
        bucket: Optional[str] = None,
        server: Optional[str] = None,
    ) -> list[float]:
        """Read a metric's values for a single (cluster, bucket, server) scope."""
        if cluster is None:
            cluster = self.test.cbmonitor_clusters[cluster_idx]
        db = self.store.build_dbname(
            cluster=cluster, collector=collector, bucket=bucket, server=server
        )
        return self.store.get_values(db, metric=metric)

    def _percentile(self, values, percentile: Number) -> float:
        """Compute the Nth percentile of ``values`` in-process."""
        return np.percentile(values, percentile)

    def _mean(self, values) -> float:
        """Compute the arithmetic mean of ``values`` in-process."""
        return np.mean(values)

    def _count_ge(self, values, threshold: Number) -> int:
        """Count how many of ``values`` are greater than or equal to ``threshold``."""
        return sum(v >= threshold for v in values)

    def _local_spring_latency_values(
        self, pattern: str, operation: str, cluster_idx: int = 0
    ) -> list[float]:
        """Read spring latency samples straight from the local worker dump files.

        Spring workers dump their latency reservoir to CSV rows
        ``(operation, timestamp_ns, latency_single_s, latency_total_s, target)``
        under ``spring_latency/master_<node>/<pattern>``. The collector parses
        these to push to the store; reading them directly here computes the KPI
        without the push-then-pull-back round trip.
        """
        master = list(self.cluster_spec.masters)[cluster_idx]
        values = []
        for path in glob.glob(f"spring_latency/master_{master}/{pattern}"):
            for sample in parse_spring_latency_file(path):
                if sample.operation == operation:
                    values.append(sample.latency_ms)
        return values

    def _local_jts_values(self, metric: str, bucket: str) -> List[float]:
        """Consolidate JTS metric values for ``bucket`` from the local JTS logs."""
        settings = self.test_config.jts_access_settings
        filename = (
            "aggregated_latency.log" if metric == "jts_latency" else "aggregated_throughput.log"
        )
        if settings.logging_method == "bucket_wise":
            filename = f"{bucket}_{filename}"

        return list(
            consolidate_jts_log(settings.jts_logs_dir, filename, metric == "jts_latency").values()
        )

    def _local_kv_latency_timings(self, operation: str, cluster_idx: int = 0) -> list[list[float]]:
        """Read KV latency samples as ``[timestamp_ms, latency_ms]`` from local dumps."""
        if operation.startswith("total_"):
            csv_op, want_total = operation[len("total_"):], True
        else:
            csv_op, want_total = operation, False
        master = list(self.cluster_spec.masters)[cluster_idx]
        timings = []
        for path in glob.glob(f"spring_latency/master_{master}/*kv-worker-*"):
            for sample in parse_spring_latency_file(path):
                if sample.operation != csv_op:
                    continue
                value = sample.latency_total_ms if want_total else sample.latency_ms
                if value is None:
                    continue
                timings.append([sample.timestamp_ms, value])
        return sorted(timings, key=lambda pair: pair[0])

    def get_percentile_value_of_node_metric(self, collector, metric, server, percentile):
        values = self._read_values(metric, collector, server=server)
        return int(self._percentile(values, percentile))

    def get_collector_values(self, collector):
        values = []
        for bucket in self.test_config.buckets:
            values += self._read_values(collector, collector, bucket=bucket)
        return values

    def count_overthreshold_value_of_collector(self, collector, threshold):
        values = self.get_collector_values(collector)
        return self._count_ge(values, threshold)

    def get_percentile_value_of_collector(self, collector, percentile):
        values = self.get_collector_values(collector)
        return self._percentile(values, percentile)

    def xdcr_lag(self, percentile: Number = 95) -> Metric:
        metric_id = f'{self.test_config.name}_{percentile}th_xdcr_lag'
        title = f'{percentile}th percentile replication lag (ms), {self._title}'

        xdcr_lag = self.get_percentile_value_of_collector('xdcr_lag', percentile)

        return self._metric(round(xdcr_lag, 1), metric_id=metric_id, title=title, chirality=-1)

    def bidir_replication_rate_total_docs(self, time_elapsed: float) -> Metric:
        metric_id = f'{self.test_config.name}_total_docs'
        title = f'{self._title} Total Docs'

        initial_items = self.test_config.load_settings.items + \
                        self.test_config.load_settings.items * \
                        (1 - self.test_config.load_settings.conflict_ratio)
        rate = self._bidir_replication_rate(time_elapsed, initial_items)

        return self._metric(rate, metric_id=metric_id, title=title, chirality=1)

    def bidir_replication_rate_written_docs(self,
                                            time_elapsed: float,
                                            total_docs_written: int) -> Metric:
        metric_id = f'{self.test_config.name}_written_docs'
        title = f'{self._title} Written Docs'

        rate = self._bidir_replication_rate(time_elapsed, total_docs_written)

        return self._metric(rate, metric_id=metric_id, title=title, chirality=1)

    def bidir_replication_rate_dcp_docs(self, time_elapsed: float, total_dcp_docs: int) -> Metric:
        metric_id = f'{self.test_config.name}_dcp_docs'
        title = f'{self._title} DCP Docs'

        rate = self._bidir_replication_rate(time_elapsed, total_dcp_docs)

        return self._metric(rate, metric_id=metric_id, title=title, chirality=1)

    def _bidir_replication_rate(self, time_elapsed: float, initial_items: int) -> float:

        num_buckets = self.test_config.cluster.num_buckets
        bidir_replication_rate = num_buckets * initial_items / time_elapsed

        return round(bidir_replication_rate)

    def avg_replication_rate(self, time_elapsed: float) -> Metric:
        rate = self._avg_replication_rate(time_elapsed)

        return self._metric(rate, chirality=1)

    def avg_replication_throughput(self, throughput: float, xdcr_link: str) -> Metric:
        metric_id = self.test_config.name + '_' + xdcr_link
        title = self.test_config.showfast.title + ', ' + xdcr_link

        return self._metric(round(throughput), metric_id=metric_id, title=title, chirality=1)

    def replication_throughput(self, throughput: float) -> Metric:
        metric_id = self.test_config.name
        title = self.test_config.showfast.title

        return self._metric(round(throughput), metric_id=metric_id, title=title, chirality=1)

    def avg_replication_multilink(self, time_elapsed: float, xdcr_link: str) -> Metric:

        metric_id = self.test_config.name + '_' + xdcr_link
        title = self.test_config.showfast.title + ', ' + xdcr_link

        rate = self._avg_replication_rate(time_elapsed)

        return self._metric(rate, metric_id=metric_id, title=title, chirality=1)

    def _avg_replication_rate(self, time_elapsed: float) -> float:
        initial_items = self.test_config.load_settings.ops or \
            self.test_config.load_settings.items
        num_buckets = self.test_config.cluster.num_buckets
        avg_replication_rate = num_buckets * initial_items / time_elapsed

        return round(avg_replication_rate)

    def max_drain_rate(self, time_elapsed: float) -> Metric:
        items_per_node = self.test_config.load_settings.items / self._num_nodes
        drain_rate = round(items_per_node / time_elapsed)

        return self._metric(drain_rate, chirality=1)

    def avg_disk_write_queue(self) -> Metric:
        values = []
        for bucket in self.test_config.buckets:
            values += self._read_values("disk_write_queue", "ns_server", bucket=bucket)

        return self._metric(int(self._mean(values)), chirality=-1)

    def avg_total_queue_age(self) -> Metric:
        values = []
        for bucket in self.test_config.buckets:
            values += self._read_values("vb_avg_total_queue_age", "ns_server", bucket=bucket)

        return self._metric(int(self._mean(values)), chirality=-1)

    def avg_couch_views_ops(self) -> Metric:
        values = []
        for bucket in self.test_config.buckets:
            values += self._read_values('couch_views_ops', 'ns_server', bucket=bucket)

        return self._metric(int(self._mean(values)), chirality=1)

    def query_latency(self, percentile: Number, cluster_idx: int = 0,
                      custom_title_postfix: str = None,
                      update_subcategory: bool = False) -> Metric:
        metric_id = '{}_query_{:g}th'.format(self.test_config.name, percentile)
        metric_id = metric_id.replace('.', '')

        title_prefix = '{:g}th percentile query latency (ms)'.format(percentile)
        if custom_title_postfix:
            title_prefix = title_prefix + f" {custom_title_postfix}"

        if len(self.test.cbmonitor_clusters) > 1:
            metric_id = '{}_cluster{}'.format(metric_id, cluster_idx + 1)
            title_prefix = '{} (cluster {})'.format(title_prefix, cluster_idx + 1)

        title = '{}, {}'.format(title_prefix, self._title)

        latency = self._query_latency(percentile, cluster_idx)
        extra = {"subCategory": "Latency"} if update_subcategory else None

        return self._metric(
            latency,
            metric_id=metric_id,
            title=title,
            order_by=self.query_id,
            chirality=-1,
            extra=extra,
        )

    def _query_latency(self, percentile: Number, cluster_idx: int = 0) -> float:
        values = self._local_spring_latency_values("query-worker-*", "query", cluster_idx)
        query_latency = self._percentile(values, percentile)
        return adaptive_round(query_latency)

    def avg_query_latency(self) -> Metric:
        metric_id = f'{self.test_config.name}_query_avg'.replace('.', '')
        title = f'Average query latency (ms), {self._title}'

        values = self._local_spring_latency_values("query-worker-*", "query")
        avg_latency = float(self._mean(values))
        latency = adaptive_round(avg_latency)
        return self._metric(
            latency, metric_id=metric_id, title=title, order_by=self.query_id, chirality=-1
        )

    def api_latency(self, endpoint, values, percentile: Number, cluster_idx: int = 0) -> Metric:
        metric_id = f"{endpoint}_api_{percentile:g}th"
        metric_id = metric_id.replace(".", "")

        title_prefix = f"{percentile:g}th percentile api latency (ms)"

        if len(self.test.cbmonitor_clusters) > 1:
            metric_id = f"{metric_id}_cluster{cluster_idx + 1}"
            title_prefix = f"{title_prefix} (cluster {cluster_idx + 1})"

        title = f"{title_prefix}, {endpoint} Endpoint"
        latencies = round(s_to_ms(self._percentile(values, percentile)), 1)

        return self._metric(
            latencies, metric_id=metric_id, title=title, order_by=self.query_id, chirality=-1
        )

    def webhook_success_rate(self, total: int, successful: int, label: str = "") -> Metric:
        metric_id = f"webhook_success_rate_{label}" if label else "webhook_success_rate"
        metric_id = metric_id.replace(".", "")

        rate = round(successful / total * 100, 2) if total > 0 else 0

        title = f"Webhook Success Rate (%), {self._title}"
        if label:
            title = f"{title} - {label}"

        return self._metric(rate, metric_id=metric_id, title=title, chirality=1)

    def secondary_scan_latency(self, percentile: Number, title: str = None) -> Metric:
        metric_id = "{}_{:g}th".format(self.test_config.name, percentile)
        if title is None:
            title = '{:g}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                               self._title)
        else:
            title = '{:g}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                               title)
        cluster = ""
        for cid in self.test.cbmonitor_clusters:
            if "apply_scanworkload" in cid:
                cluster = cid
                break
        timings = self._read_values("Nth-latency", "secondaryscan_latency", cluster=cluster)
        timings = list(map(int, timings))
        logger.info("Number of samples are {}".format(len(timings)))
        scan_latency = ns_to_ms(self._percentile(timings, percentile))
        scan_latency = round(scan_latency, 2)

        return self._metric(
            scan_latency, metric_id=metric_id, title=title, chirality=-1, extra={"category": "lat"}
        )

    def secondary_scan_latency_value(self, scan_latency,
                                     percentile: Number, title: str = None,
                                     update_category: bool = True) -> Metric:
        metric_id = "{}_{:g}th".format(self.test_config.name, percentile)
        title = '{:g}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                           title)
        scan_latency = ns_to_ms(scan_latency)
        scan_latency = round(scan_latency, 2)

        extra = {'category': "lat"} if update_category else None
        return self._metric(
            scan_latency, metric_id=metric_id, title=title, chirality=-1, extra=extra
        )

    def analytics_time_taken(self,
                             time_taken: float,
                             sql_suite: str) -> Metric:

        metric_id = f'{self.test_config.name}_time_taken_{sql_suite}'
        metric_id = metric_id.replace('.', '')

        title = f'{sql_suite} Time Elapsed (sec), {self._title}'
        time_taken = round(time_taken, 2)

        return self._metric(time_taken, metric_id=metric_id, title=title, chirality=1)

    def query_suite_runtime(self,
                            time_taken: float,
                            suite: str) -> Metric:

        metric_id = f'{self.test_config.name}_runtime_{suite}'
        metric_id = metric_id.replace('.', '')

        title = f'{suite} Runtime (sec), {self._title}'
        time_taken = round(time_taken, 2)

        return self._metric(time_taken, metric_id=metric_id, title=title, chirality=1)

    def _build_kv_latency_metric(
        self,
        latency: float,
        operation: str,
        stat_group: str,
        window: TimeseriesWindow,
        cluster_idx: int,
        id_label: str,
        title_label: str,
        extra_info: Optional[dict] = None,
    ) -> Metric:
        metric_id_parts = [
            self.test_config.name,
            operation,
            stat_group if stat_group != "" else None,
            id_label,
            window.label if window.label != "" else None,
        ]
        title_prefix_parts = [
            title_label,
            operation.upper(),
            f"({stat_group})" if stat_group != "" else None,
            f"({window.label})" if window.label != "" else None,
        ]

        if len(self.test.cbmonitor_clusters) > 1:
            metric_id_parts.append(f"cluster{cluster_idx + 1}")
            title_prefix_parts.append(f"(cluster {cluster_idx + 1})")

        metric_id = "_".join(filter(None, metric_id_parts)).replace(".", "")
        title_prefix = " ".join(filter(None, title_prefix_parts))

        metric_info = self._metric_info(
            metric_id,
            title=f"{title_prefix} {self._title}",
            chirality=-1,
            stat_group=stat_group,
        )
        metric_info.update({"operation": operation})
        if extra_info:
            metric_info.update(extra_info)
        if window.label:
            metric_info["window"] = window.label
        return latency, self._snapshots, metric_info

    def _get_kv_latency_timings(
        self,
        operation: str,
        collector: str,
        stat_group: str = "",
        cluster_idx: int = 0,
    ) -> list[list[float]]:
        """Return sorted ``[[timestamp_ms, latency_ms]]`` of KV op latencies."""
        timings = self._local_kv_latency_timings(operation, cluster_idx)
        if not timings:
            logger.warning(f"No latency data found for {operation=}, {collector=}, {stat_group=}")
            return []

        return timings

    def _calculate_timeseries_stats(
        self,
        stat_fn: Callable[[list[list[float]]], Any],
        operation: str,
        collector: str,
        windows: list[TimeseriesWindow],
        stat_group: str = "",
        cluster_idx: int = 0,
    ) -> list[tuple[TimeseriesWindow, Any]]:
        if not (
            timings := self._get_kv_latency_timings(operation, collector, stat_group, cluster_idx)
        ):
            return []

        arr_timings = np.array(timings)
        window_stats = []
        for w in windows:
            w_timings = w.apply(arr_timings)
            if len(w_timings) == 0:
                logger.error(f"No data found for timeseries window: {w}")
                continue

            if w.label:
                min_ts, max_ts = np.min(w_timings[:, 0]), np.max(w_timings[:, 0])
                logger.info(
                    f"Fetched data for timeseries window: label={w.label}, samples={len(w_timings)}"
                    f", {min_ts=}, {max_ts=}, window_duration_secs={(max_ts - min_ts) / 1000:.2f}"
                )

            window_stats.append((w, stat_fn(w_timings)))

        return window_stats

    def avg_kv_latency(
        self,
        operation: str,
        collector: str = "spring_latency",
        cluster_idx: int = 0,
        windows: Optional[list[TimeseriesWindow]] = None,
    ) -> list[Metric]:
        windows = windows or [TimeseriesWindow()]
        metrics = []
        stat_groups = self.test_config.collection.collection_stat_groups or [""]

        def calc_avg(w_timings: np.ndarray[float]) -> float:
            avg = self._mean(w_timings[:, 1])
            return round(avg) if avg > 100 else round(avg, 2)

        for stat_group in stat_groups:
            window_latencies = self._calculate_timeseries_stats(
                calc_avg, operation, collector, windows, stat_group, cluster_idx
            )
            for window, latency in window_latencies:
                metrics.append(
                    self._build_kv_latency_metric(
                        latency,
                        operation,
                        stat_group,
                        window,
                        cluster_idx,
                        id_label="avg",
                        title_label="Average",
                    )
                )

        return metrics

    def percentile_kv_latency(
        self,
        operation: str,
        percentiles: Optional[Iterable[Number]] = None,
        collector: str = "spring_latency",
        cluster_idx: int = 0,
        windows: Optional[list[TimeseriesWindow]] = None,
    ) -> list[Metric]:
        percentiles = list(percentiles or [99.9])
        windows = windows or [TimeseriesWindow()]
        metrics = []
        stat_groups = self.test_config.collection.collection_stat_groups or [""]

        for stat_group in stat_groups:
            window_latencies = self._calculate_timeseries_stats(
                calc_percentiles_fn(percentiles),
                operation,
                collector,
                windows,
                stat_group,
                cluster_idx,
            )
            for window, latencies in window_latencies:
                for percentile, latency in zip(percentiles, latencies):
                    metrics.append(
                        self._build_kv_latency_metric(
                            latency,
                            operation,
                            stat_group,
                            window,
                            cluster_idx,
                            id_label=f"{percentile:g}th",
                            title_label=f"{percentile:g}th percentile",
                            extra_info={"percentile": percentile},
                        )
                    )

        return metrics

    def observe_latency(self, percentile: Number) -> Metric:
        metric_id = '{}_{:g}th'.format(self.test_config.name, percentile)
        title = '{:g}th percentile {}'.format(percentile, self._title)

        timings = []
        for bucket in self.test_config.buckets:
            timings += self._read_values("latency_observe", "observe", bucket=bucket)

        latency = round(self._percentile(timings, percentile), 2)

        return self._metric(latency, metric_id=metric_id, title=title, chirality=-1)

    def cpu_utilization(self) -> Metric:
        metric_id = f"{self.test_config.name}_avg_cpu"
        title = f"Avg. CPU utilization (%), {self._title}"

        bucket = self.test_config.buckets[0]
        values = self._read_values("cpu_utilization_rate", "ns_server", bucket=bucket)

        return self._metric(int(self._mean(values)), metric_id=metric_id, title=title, chirality=-1)

    def avg_server_process_cpu(self, server_process: str) -> Metric:
        metric_id = f"{self.test_config.name}_avg_{server_process}_cpu".replace(".", "_")
        title = f"Avg. {server_process} CPU utilization (%), {self._title}"

        values = []
        for cluster_idx, ((_, servers), initial_nodes) in enumerate(zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        )):
            cluster = self.test.cbmonitor_clusters[cluster_idx]
            for server in servers[:initial_nodes]:
                values += self._read_values(server_process + '_cpu', 'atop',
                                            cluster=cluster, server=server)

        return self._metric(
            round(self._mean(values), 1), metric_id=metric_id, title=title, chirality=-1
        )

    def max_memcached_rss(self) -> Metric:
        metric_id = f"{self.test_config.name}_memcached_rss"
        title = f"Max. memcached RSS (MB),{self._title.split(',')[-1]}"

        max_rss = 0
        for cluster_idx, ((_, servers), initial_nodes) in enumerate(zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        )):
            cluster = self.test.cbmonitor_clusters[cluster_idx]
            for server in servers[:initial_nodes]:
                values = self._read_values("memcached_rss", "atop", cluster=cluster, server=server)
                rss = round(bytes_to_mib(max(values)))
                max_rss = max(max_rss, rss)

        return self._metric(max_rss, metric_id=metric_id, title=title, chirality=-1)

    def avg_memcached_rss(self) -> Metric:
        metric_id = f"{self.test_config.name}_avg_memcached_rss"
        title = f"Avg. memcached RSS (MB),{self._title.split(',')[-1]}"

        rss = []
        for cluster_idx, ((_, servers), initial_nodes) in enumerate(zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        )):
            cluster = self.test.cbmonitor_clusters[cluster_idx]
            for server in servers[:initial_nodes]:
                rss += self._read_values("memcached_rss", "atop", cluster=cluster, server=server)

        return self._metric(
            int(bytes_to_mib(self._mean(rss))), metric_id=metric_id, title=title, chirality=-1
        )

    def memory_overhead(self, key_size: int = 20) -> Metric:
        item_size = key_size + self.test_config.load_settings.size
        user_data = self.test_config.load_settings.items * item_size
        user_data *= self.test_config.bucket.replica_number + 1
        user_data = bytes_to_mib(user_data)

        mem_used, _, _ = self.avg_memcached_rss()
        mem_used *= self._num_nodes

        overhead = int(100 * (mem_used / user_data - 1))

        return self._metric(overhead, chirality=-1)

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

        value = s2m(value)

        extra = {"category": index_type.lower()} if update_category else None
        return self._metric(value, metric_id=metric_id, title=title, chirality=-1, extra=extra)

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

        return self._metric(value, metric_id=metric_id, title=title, chirality=-1)

    def bnr_throughput(self,
                       time_elapsed: float,
                       edition: str,
                       tool: str,
                       storage: str = None) -> Metric:

        tool_and_storage = tool + '-' + storage if storage else tool
        metric_id = f'{self.test_config.name}_{tool_and_storage}_thr_{edition}'
        title = f'{edition} {tool} throughput (Avg. MB/sec), {self._title}'

        if self.test_config.access_settings.ops != float('inf'):
            access_items = self.test_config.access_settings.ops * (
                self.test_config.access_settings.creates / 100
            )
        else:
            access_items = 0

        data_size = (self.test_config.load_settings.items + access_items) * bytes_to_mib(
            self.test_config.load_settings.size
        )  # MB

        avg_throughput = round(data_size / time_elapsed)

        return self._metric(avg_throughput, metric_id=metric_id, title=title, chirality=1)

    def contbk_restore_throughput(
        self, time_elapsed: float, edition: str, tool: str, storage: str = None
    ) -> Metric:
        tool_and_storage = tool + "-" + storage if storage else tool
        metric_id = f"{self.test_config.name}_{tool_and_storage}_thr_{edition}"
        title = f"{edition} {tool} restore throughput (Avg. MB/sec), {self._title}"

        data_size = bytes_to_mib(
            self.test_config.access_settings.ops * self.test_config.load_settings.size
        )
        avg_throughput = round(data_size / time_elapsed)

        return self._metric(avg_throughput, metric_id=metric_id, title=title, chirality=1)

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

        return self._metric(round(time_elapsed), metric_id=metric_id, title=title, chirality=-1)

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

        return self._metric(size, metric_id=metric_id, title=title, chirality=-1)

    def disk_size(self, size: float) -> Metric:

        metric_id = '{}_size'.format(self.test_config.name)
        title = 'Disk Size (GB), {}'.format(self._title)
        size = round(bytes_to_gib(float(size)))

        return self._metric(size, metric_id=metric_id, title=title, chirality=-1)

    def disk_size_reduction(self, disk_size: float, raw_data_size: float) -> Metric:

        metric_id = '{}_disk_size_reduction'.format(self.test_config.name)
        title = 'Disk Size Reduction (%), {}'.format(self._title)
        reduction = round((1.0 - disk_size / raw_data_size) * 100)

        return self._metric(reduction, metric_id=metric_id, title=title, chirality=1)

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

        data_size = bytes_to_mib(
            2 * self.test_config.load_settings.items * self.test_config.load_settings.size
        )  # MB

        avg_throughput = round(data_size / time_elapsed)

        return self._metric(avg_throughput, metric_id=metric_id, title=title, chirality=1)

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
        data_size = bytes_to_mib(
            self.test_config.load_settings.items * self.test_config.load_settings.size
        )  # MB

        avg_throughput = round(data_size / time_elapsed)

        return self._metric(avg_throughput, chirality=1)

    def import_file_throughput(self, time_elapsed: float) -> Metric:
        import_file = self.test_config.export_settings.import_file
        data_size = bytes_to_mib(os.path.getsize(import_file))
        avg_throughput = round(data_size / time_elapsed)

        return self._metric(avg_throughput, chirality=1)

    def verify_series_in_limits(self, expected_number: int) -> bool:
        values = self._read_values("num_connections", "secondary_debugstats")
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
        p_lat = round(self._percentile(_temp, percentile) / 1000, 3)
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
        gcs = self.ycsb_get_gcs()
        return self._metric(gcs, title=title, metric_id=metric_id, chirality=-1)

    def ycsb_failed_ops(self,
                        io_type: str,
                        failures: int,) -> Metric:
        type = io_type + " Failures"
        title = '{} {}'.format(type, self._title)
        metric_id = '{}_{}'.format(self.test_config.name, type.replace(' ', '_').casefold())
        return self._metric(failures, title=title, metric_id=metric_id, chirality=-1)

    def ycsb_slo_max_latency(self,
                             io_type: str,
                             max_latency: int,) -> Metric:

        max_type = "Max " + io_type + " Latency (ms)"
        title = '{}, {}'.format(max_type, self._title)
        metric_id = '{}_{}'.format(self.test_config.name, max_type.replace(' ', '_')
                                   .replace('(', '').replace(')', '').casefold())

        return self._metric(max_latency, title=title, metric_id=metric_id, chirality=-1)

    def dcp_throughput(self,
                       time_elapsed: float,
                       clients: int,
                       stream: str) -> Metric:
        if stream == 'all':
            throughput = round(
                (self.test_config.load_settings.items * clients) / time_elapsed)
        else:
            throughput = round(
                self.test_config.load_settings.items / time_elapsed)

        return self._metric(throughput, chirality=1)

    def fragmentation_ratio(self, ratio: float) -> Metric:
        return self._metric(ratio)

    def elapsed_time(self, time_elapsed: float) -> Metric:
        time_elapsed = s2m(time_elapsed)

        return self._metric(time_elapsed, chirality=-1)

    def cluster_deployment_time(self, deployment_time, prefix, title) -> Metric:
        metric_id = f'{self.test_config.name}_{prefix}'
        metric_title = f'{title},{",".join(self._title.split(",")[2:])}'
        time = round(float(deployment_time), 2)

        return self._metric(time, metric_id=metric_id, title=metric_title)

    def kv_throughput(self, total_ops: int) -> Metric:
        throughput = total_ops // self.test_config.access_settings.time

        return self._metric(throughput, chirality=1)

    def mctimings_latency(self, operation: str, latency: float) -> Metric:
        title = f'{operation} Latency (ms), {self._title}'
        metric_id = f'{self.test_config.name}_{operation.replace(" ", "_").casefold()}'

        latency = round(latency, 2)

        return self._metric(latency, title=title, metric_id=metric_id, chirality=-1)

    def ycsb_throughput(self, operation: str = "access") -> Metric:
        throughput = self._parse_ycsb_throughput(operation)

        return self._metric(throughput, chirality=1)

    def pytpcc_tpmc_throughput(self, duration: int) -> Metric:

        executed = self._parse_pytpcc_throughput()

        tpmc = round(executed / duration * 60)

        return self._metric(tpmc, chirality=1)

    def ycsb_throughput_phase(self,
                              phase: int,
                              workload: str,
                              operation: str = "access"
                              ) -> Metric:

        title = '{}, {}, Phase {}, {}'.format(
            "Avg Throughput (ops/sec)", self._title, phase, workload)
        metric_id = '{}_{}_{}'.format(self.test_config.name, workload.replace(' ', '_').casefold(),
                                      phase)

        throughput = self._parse_ycsb_throughput(operation)

        return self._metric(throughput, title=title, metric_id=metric_id, chirality=1)

    def ycsb_durability_throughput(self) -> Metric:
        title = '{}, {}'.format("Avg Throughput (ops/sec)", self._title)
        metric_id = '{}_{}'\
            .format(self.test_config.name, "Avg Throughput".replace(' ', '_').casefold())

        throughput = self._parse_ycsb_throughput()

        return self._metric(throughput, title=title, metric_id=metric_id, chirality=1)

    def ycsb_latency(self,
                     io_type: str,
                     latency: int,
                     ) -> Metric:
        title = '{} {}'.format(io_type, self._title)
        title = title.replace('.0', '')
        metric_id = '{}_{}'.format(self.test_config.name, io_type.replace(' ', '_').casefold())
        # 50.0 -> 50
        metric_id = metric_id.replace('.0', '')
        # 99.9 -> 999
        metric_id = metric_id.replace('.', '')
        return self._metric(latency, title=title, metric_id=metric_id, chirality=-1)

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
        return self._metric(latency, title=title, metric_id=metric_id, chirality=-1)

    def ycsb_slo_latency(self,
                         io_type: str,
                         latency: int,
                         ) -> Metric:
        title = '{} Latency (ms), {}'.format(io_type, self._title)
        metric_id = '{}_{}'.format(self.test_config.name, io_type.replace(' ', '_')
                                   .replace('(', '').replace(')', '').casefold())
        return self._metric(latency, title=title, metric_id=metric_id, chirality=-1)

    def ycsb_get_latency(self,
                         percentile: str,
                         operation: str = "access"
                         ) -> Metric:
        latency_dic = self._parse_ycsb_latency(percentile, operation)
        return latency_dic

    def get_latency_histogram(self, aggregated_histogram_file: str,
                              operation: str = None,
                              latency_percentiles: list[float] = [25, 50, 75, 90, 95, 99]
                              ) -> list[tuple[str, str, float]]:
        # Parse HDR histogram lines: [OPERATION-HISTOGRAM], latency_us, count
        # File is pre-sorted by latency_us ascending.
        per_op: dict[str, dict[int, int]] = {}
        with open(aggregated_histogram_file) as f:
            for line in f:
                parts = line.strip().split(', ')
                if operation is not None:
                    op_name = operation.strip('[]')
                else:
                    op_key = parts[0].strip('[]')
                    if (
                        "CLEANUP" in op_key
                        or not op_key.endswith("-HISTOGRAM")
                        or "FAILED" in op_key
                    ):
                        continue
                    op_name = op_key.removesuffix('-HISTOGRAM')
                latency_us, count = int(parts[1]), int(float(parts[2]))
                if op_name not in per_op:
                    per_op[op_name] = {}
                per_op[op_name][latency_us] = per_op[op_name].get(latency_us, 0) + count

        result = []
        for op, counts in per_op.items():
            if not counts:
                logger.warning(f"No histogram data found for operation {op}")
                continue

            latencies: list[tuple[int, int]] = sorted(counts.items())
            total_count = sum(count for _, count in latencies)

            sorted_percentiles = sorted(latency_percentiles)
            thresholds = [total_count * p / 100 for p in sorted_percentiles]
            percentile_latencies = {p: latencies[-1][0] for p in sorted_percentiles}
            cumulative = 0
            target_idx = 0
            for lat_us, count in latencies:
                cumulative += count
                while target_idx < len(thresholds) and cumulative >= thresholds[target_idx]:
                    percentile_latencies[sorted_percentiles[target_idx]] = lat_us
                    target_idx += 1
                if target_idx == len(thresholds):
                    break

            for percentile in latency_percentiles:
                latency_ms = round(percentile_latencies[percentile] / 1000, 3)
                result.append((op, f"{percentile:g}th Percentile", latency_ms))

            avg_latency_us = round(sum(lat * cnt for lat, cnt in latencies) / total_count)
            avg_latency_ms = round(avg_latency_us / 1000, 3)
            result.append((op, "avg", avg_latency_ms))

        return result

    @staticmethod
    def parse_mctimings_histogram(raw_output: str) -> dict[str, list[tuple[float, int]]]:
        # Returns a dict mapping operation name to a list of (upper_bound_us, count) tuples.
        # Example input line:
        #    [ 10.00 -  11.00]us (30.0000%)	 303| ########################
        per_op: dict[str, list[tuple[float, int]]] = {}
        current_op = None

        for line in raw_output.splitlines():
            line = line.strip()

            # Detect operation header: 'The following data is collected for "GET"'
            if line.startswith('The following data is collected for'):
                match = re.search(r'"([^"]+)"', line)
                if match:
                    current_op = match.group(1)
                    per_op[current_op] = []
                continue

            # Skip Total lines, legend, blank lines, etc.
            if not line.startswith('[') or current_op is None:
                continue

            # Parse histogram row:
            # [ 13.00 -  14.00]us (65.0000%)	  4592| ###################
            # [  0.89 -   1.34]ms (99.3750%)	    51|
            match = re.match(
                r'\[\s*([\d.]+)\s*-\s*([\d.]+)\](us|ms|s)\s+'
                r'\(([\d.]+)%\)\s+'
                r'(\d+)\|',
                line,
            )
            if not match:
                continue

            upper_bound = float(match.group(2))
            unit = match.group(3)
            count = int(match.group(5))

            # Normalize upper_bound to microseconds
            if unit == 'ms':
                upper_bound_us = upper_bound * 1000
            elif unit == 's':
                upper_bound_us = upper_bound * 1_000_000
            else:
                upper_bound_us = upper_bound

            per_op[current_op].append((upper_bound_us, count))

        return per_op

    def get_mctimings_latency_histogram(
        self,
        mctimings_data: dict[str, list[tuple[float, int]]],
        latency_percentiles: list[float] = [25, 50, 75, 90, 95, 99],
    ) -> list[tuple[str, str, float]]:
        """Compute percentile latencies from parsed mctimings histogram data.

        Similar to get_latency_histogram but works with mctimings format.
        Returns list of (operation, label, latency_ms) tuples.
        """
        result = []
        for op, rows in mctimings_data.items():
            if not rows:
                logger.warning(f"No mctimings histogram data for operation {op}")
                continue

            total_count = sum(count for _, count in rows)
            if total_count == 0:
                continue

            sorted_percentiles = sorted(latency_percentiles)
            thresholds = [total_count * p / 100 for p in sorted_percentiles]

            percentile_latencies = {p: rows[-1][0] for p in sorted_percentiles}
            cumulative = 0
            target_idx = 0
            for upper_bound_us, count in rows:
                cumulative += count
                while target_idx < len(thresholds) and cumulative >= thresholds[target_idx]:
                    percentile_latencies[sorted_percentiles[target_idx]] = upper_bound_us
                    target_idx += 1
                if target_idx == len(thresholds):
                    break

            for percentile in latency_percentiles:
                latency_ms = round(percentile_latencies[percentile] / 1000, 3)
                result.append((op, f"{percentile:g}th Percentile", latency_ms))

            avg_latency_us = sum(ub * cnt for ub, cnt in rows) / total_count
            avg_latency_ms = round(avg_latency_us / 1000, 3)
            result.append((op, "avg", avg_latency_ms))

        return result

    def log_mctimings_histogram(
        self,
        mctimings_data: dict[str, list[tuple[float, int]]],
        bucket_size_us: int = 1000,
        output_file: str = "YCSB/mctimings_histogram.log",
    ):
        """Bucket and log mctimings histogram data, similar to _log_histogram.

        Groups raw mctimings rows into fixed-width buckets based on bucket_size_us
        (from histogram_bucket_size setting) and prints a summary.
        """
        all_lines = []

        for op, rows in mctimings_data.items():
            if not rows:
                continue

            total_count = sum(count for _, count in rows)
            if total_count == 0:
                continue

            # Bucket the data into fixed-width ranges
            # Pessimistically assume all observations happened at the high end of this bucket range,
            # as we'd rather overestimate than underestimate latencies.
            # Example, with bucket_size_us = 10:

            # [  3.00 - 831.00]us 30640|
            # becomes
            # [830.00 - 840.00]us 30640|

            bucketed: dict[str, int] = {}
            for upper_bound_us, count in rows:
                if count == 0:
                    continue
                bucket_start = int(upper_bound_us / bucket_size_us) * bucket_size_us
                bucket_end = bucket_start + bucket_size_us
                if bucket_size_us >= 1000:
                    label = f"{bucket_start // 1000}-{bucket_end // 1000}ms"
                else:
                    label = f"{bucket_start}-{bucket_end}us"
                bucketed[label] = bucketed.get(label, 0) + count

            lines = [
                f"mctimings {op} LATENCY HISTOGRAM:",
                "-" * 40,
                f"Total operations: {total_count}",
            ]

            cumulative = 0
            for bucket_label in sorted(
                bucketed,
                key=lambda b: sort_bucket_key(b, bucket_size_us,
                                              bucket_count=999999),
            ):
                count = bucketed[bucket_label]
                if count > 0:
                    cumulative += count
                    pct = (count / total_count) * 100
                    cum_pct = (cumulative / total_count) * 100
                    lines.append(
                        f"{bucket_label:>15}: {count:>8} ({pct:>5.1f}%) "
                        f"- Cumulative: {cum_pct:>5.1f}%"
                    )

            for line in lines:
                logger.info(line)
            all_lines.extend(lines)
            all_lines.append("")

        if all_lines:
            with open(output_file, 'w') as f:
                f.write("\n".join(all_lines) + "\n")
            logger.info(f"mctimings histogram written to {output_file}")

    def aggregate_and_print_histogram(self, *YCSB_files: str,
                                      measurement_type: str = "histogram",
                                      output_file: str = "YCSB/aggregated_histogram.log",
                                      verbose_file: str = "YCSB/verbose_histogram.log"):
        data = {}
        all_files = []
        for pattern in YCSB_files:
            all_files.extend(glob.glob(pattern))

        label = "HDR histogram" if measurement_type == "hdrhistogram" else "histogram"
        logger.info(f"Aggregating {label} data from {len(all_files)} files")

        for filename in all_files:
            with open(filename) as f:
                for line in f:
                    parts = line.strip().split(', ')
                    if len(parts) != 3:
                        continue
                    op = parts[0].strip('[]')

                    if measurement_type == "hdrhistogram" and '-HISTOGRAM' in op:
                        while op.endswith('-HISTOGRAM'):
                            op = op[:-len('-HISTOGRAM')]
                            key, count = int(parts[1]), int(float(parts[2]))
                    else:
                        bucket, count_str = parts[1], parts[2]
                        if not (
                            (bucket.endswith("ms") or bucket.endswith("us")
                             or bucket == ">10000ms")
                            and "GC" not in op and op != "OVERALL"
                        ):
                            continue
                        key = bucket
                        count = int(float(count_str))

                    if op not in data:
                        data[op] = {}
                    data[op][key] = data[op].get(key, 0) + count

        # Write aggregated data to file
        with open(output_file, 'w') as f:
            for op in data:
                if measurement_type == "hdrhistogram":
                    for key in sorted(data[op]):
                        f.write(f"[{op}-HISTOGRAM], {key}, {data[op][key]}\n")
                else:
                    collapsed = self._collapse_overflow_buckets(data[op])
                    for key in sorted(
                        collapsed,
                        key=lambda b: sort_bucket_key(
                            b, bucket_size=self.test_config.access_settings.histogram_bucket_size,
                            bucket_count=self.test_config.access_settings.histogram_buckets
                        ),
                    ):
                        f.write(f"[{op}], {key}, {collapsed[key]}\n")
        logger.info(f"Aggregated {label} written to {output_file}")

        # Print verbose histogram to console and write to verbose_file
        logger.info("=" * 80)
        logger.info(f"YCSB VERBOSE {label.upper()} DATA")
        logger.info("=" * 80)

        for op, buckets in data.items():
            self._log_histogram(
                op, buckets, verbose_file, is_hdr=measurement_type == "hdrhistogram"
            )

        logger.info("=" * 80)
        logger.info(f"Verbose {label} written to {verbose_file}")

    @staticmethod
    def _bucket_hdr_data(buckets: dict[int, int], bucket_size: int = 1000, bucket_count: int = 100):
        bucketed = {}
        threshold_us = bucket_size * bucket_count
        for latency_us, count in buckets.items():
            if latency_us >= threshold_us:
                if bucket_size >= 1000:
                    label = f">{bucket_size * bucket_count // 1000}ms"
                else:
                    label = f">{bucket_size * bucket_count}us"
            else:
                bucket_start = (latency_us // bucket_size) * bucket_size
                bucket_end = bucket_start + bucket_size
                if bucket_size >= 1000:
                    label = f"{bucket_start // 1000}-{bucket_end // 1000}ms"
                else:
                    label = f"{bucket_start}-{bucket_end}us"

            bucketed[label] = bucketed.get(label, 0) + count
        return bucketed

    def _collapse_overflow_buckets(self, buckets: dict,) -> dict:
        bucket_size = self.test_config.access_settings.histogram_bucket_size
        bucket_count = self.test_config.access_settings.histogram_buckets
        if bucket_size >= 1000:
            threshold = f">{bucket_size * bucket_count // 1000}ms"
        else:
            threshold = f">{bucket_size * bucket_count}us"
        result = {}
        overflow_count = 0
        for bucket, count in buckets.items():
            if (sort_bucket_key(bucket, bucket_size, bucket_count) == float('inf')):
                overflow_count += count
            else:
                result[bucket] = count
        if overflow_count:
            result[threshold] = overflow_count
        return result

    def _log_histogram(
        self,
        operation: str,
        buckets: dict,
        verbose_file: str = "YCSB/verbose_histogram.log",
        is_hdr: bool = False,
    ):
        if is_hdr:
            buckets = self._bucket_hdr_data(
                buckets, bucket_size=self.test_config.access_settings.histogram_bucket_size,
                bucket_count=self.test_config.access_settings.histogram_buckets
            )

        total = sum(buckets.values())
        if total == 0:
            return

        lines = [
            f"{operation} LATENCY HISTOGRAM:",
            "-" * 40,
            f"Total operations: {total}",
        ]
        cumulative = 0
        collapsed_buckets = self._collapse_overflow_buckets(buckets)
        for bucket in sorted(
            collapsed_buckets,
            key=lambda b: sort_bucket_key(
                b, bucket_size=self.test_config.access_settings.histogram_bucket_size,
                bucket_count=self.test_config.access_settings.histogram_buckets
            ),
        ):
            count = collapsed_buckets[bucket]
            if count > 0:
                cumulative += count
                pct = (count / total) * 100
                cum_pct = (cumulative / total) * 100
                lines.append(
                    f"{bucket:>15}: {count:>8} ({pct:>5.1f}%) - Cumulative: {cum_pct:>5.1f}%"
                )

        for line in lines:
            logger.info(line)
        with open(verbose_file, 'w') as vf:
            vf.write("\n".join(lines) + "\n")

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
        return self._metric(delta, chirality=-1)

    def failure_detection_time(self, delta: float) -> Metric:
        title_split = self._title.split(sep=",", maxsplit=1)
        title = "[{}] Failure detection time (s),{}".format(title_split[0], title_split[1])
        metric_id = '{}_detection_time'.format(self.test_config.name)

        return self._metric(delta, metric_id=metric_id, title=title, chirality=-1)

    def autofailover_time(self, delta: float) -> Metric:
        title_split = self._title.split(sep=",", maxsplit=1)
        title = "[{}] Auto failover time (ms),{}".format(title_split[0], title_split[1])
        metric_id = '{}_failover_time'.format(self.test_config.name)

        return self._metric(delta, metric_id=metric_id, title=title, chirality=-1)

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
        time_diff = round(time_diff, 2)

        return self._metric(time_diff, chirality=-1)

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
        throughput = self.get_functions_throughput(time, event_name, events_processed)

        return self._metric(throughput, chirality=1)

    def function_throughput_sg(self, time: int, event_name: str, events_processed: int) -> Metric:
        metric_id = f'{self.test_config.name}_eventing_throughput'
        metric_title = f'Eventing Throughput{self._title}'

        throughput = self.get_functions_throughput(time, event_name, events_processed)

        return self._metric(throughput, metric_id=metric_id, title=metric_title, chirality=1)

    def eventing_rebalance_time(self, time: int) -> Metric:
        title_split = self._title.split(sep=",", maxsplit=1)
        title = "Rebalance Time(sec)," + title_split[1]
        metric_id = f'{self.test_config.name}_rebalance_time'
        return self._metric(time, metric_id=metric_id, title=title, chirality=-1)

    def magma_benchmark_metrics(self, throughput: float, precision: int, benchmark: str) -> Metric:
        title = "{}, {}".format(benchmark, self._title)
        metric_id = '{}_{}'.format(self.test_config.name,
                                   benchmark.replace(" ", "_").replace(",", "").replace("%", ""))
        return self._metric(
            round(throughput, precision), metric_id=metric_id, title=title, chirality=1
        )

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
        return self._metric(latency, chirality=-1)

    def function_time(self, time: int, time_type: str, initials: str, unit: str = "min") -> Metric:
        title = initials + ", " + self._title
        metric_id = f'{self.test_config.name}_{time_type.lower()}'
        if unit == "min":
            time = s2m(seconds=time)

        return self._metric(time, metric_id=metric_id, title=title, chirality=-1)

    def analytics_latency(self, query: Query, latency: int) -> Metric:
        metric_id = self.test_config.name + strip(query.description)

        title = 'Avg. query latency (ms), {} {}, {}'.format(query.id,
                                                            query.description,
                                                            self._title)

        order_by = '{}_{:05d}_{}'.format(query.id[:2], int(query.id[2:]), self._order_by)

        return self._metric(
            latency, metric_id=metric_id, title=title, order_by=order_by, chirality=-1
        )

    def analytics_avg_connect_time(self, avg_connect_time: int) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, "connect")

        title = 'Avg. connect time (sec), {}'.format(self._title)

        return self._metric(
            round(avg_connect_time, 1), metric_id=metric_id, title=title, chirality=-1
        )

    def analytics_avg_disconnect_time(self, avg_disconnect_time: int) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, "disconnect")

        title = 'Avg. disconnect time (sec), {}'.format(self._title)

        return self._metric(
            round(avg_disconnect_time, 1), metric_id=metric_id, title=title, chirality=-1
        )

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

        return self._metric(
            latency, metric_id=metric_id, title=title, order_by=order_by, chirality=-1
        )

    def get_max_rss_values(self, function_name: str, server: str):
        rss_list = self._read_values(
            "eventing_consumer_rss", "eventing_consumer_stats", bucket=function_name, server=server
        )
        max_consumer_rss = round(bytes_to_mib(max(rss_list)), 2)

        rss_list = self._read_values("eventing-produc_rss", "atop", server=server)
        max_producer_rss = round(bytes_to_mib(max(rss_list)), 2)

        return max_consumer_rss, max_producer_rss

    def avg_ingestion_rate(
        self, num_items: int, time_elapsed: float, sync_type: str = "initial"
    ) -> Metric:
        return self.custom_metric(
            round(num_items / time_elapsed),
            f"Avg. {sync_type.replace('_', ' ')} ingestion rate (items/sec), {{}}",
            f"{sync_type.lower().replace(' ', '_')}_ingest_rate",
            chirality=1,
        )

    def avg_drop_rate(self, num_items: int, time_elapsed: float) -> Metric:
        rate = round(num_items / time_elapsed)

        return self._metric(rate, chirality=1)

    def compression_throughput(self, time_elapsed: float) -> Metric:
        throughput = round(self.test_config.load_settings.items / time_elapsed)

        return self._metric(throughput, chirality=1)

    def _chX_process_line(self, line: str, metrics: CHXMetrics, tclients: int):
        """Process a line from a CH2/CH3 log file and update the given metrics object if needed."""
        if "NEW_ORDER" in line and "success" in line:
            elements = line.split()
            # "total_no_txn_time_us" is number of microsecs spent executing NEW_ORDER txns
            # by all tclients (so wall-clock time spent is total_no_txn_time_us / tclients)
            metrics.total_no_txn_time_us = float(elements[2])
            metrics.no_txn_success_count = int("".join(filter(str.isdigit, elements[-1])))
            if isinstance(metrics, CH3Metrics):
                # CH3 reports txn timings in ms not us so we need to correct here
                metrics.total_no_txn_time_us *= 1000
        elif line.strip().startswith("TOTAL") and tclients > 0:
            # TOTAL time is the sum of microsecs spent executing txns by all tclients,
            # hence we divide by tclients to get average wall-clock time spent per client
            metrics.txn_workload_duration_secs = float(line.split()[2]) / 1e6 / tclients
            if isinstance(metrics, CH3Metrics):
                # CH3 reports txn timings in ms not us so we need to correct here
                metrics.txn_workload_duration_secs *= 1000
        elif "OVERALL GEOMETRIC MEAN" in line or "OVERALL ANALYTICS GEOMETRIC MEAN" in line:
            metrics.geo_mean_cbas_query_time_secs = float(line.split()[-1])
        elif "AVERAGE TIME PER QUERY SET" in line or "AVERAGE TIME PER ANALYTICS QUERY SET" in line:
            metrics.average_cbas_query_set_time_secs = float(line.split()[-1])
        elif "QUERIES PER HOUR" in line or "ANAYTICS QUERIES PER HOUR" in line:
            # typo 'ANAYTICS' is deliberate, inherited from CH3 repo...
            metrics.cbas_qph = float(line.split()[-1])

        if isinstance(metrics, CH3Metrics):
            if "AVERAGE TIME PER FTS QUERY SET" in line:
                metrics.average_fts_query_set_time_ms = float(line.split()[-1])
            elif "AVERAGE TIME PER FTS CLIENT" in line:
                metrics.average_fts_client_time_ms = float(line.split()[-1])
            elif "FTS QUERIES PER HOUR" in line:
                metrics.fts_qph = float(line.split()[-1])

    def _chX_metrics(self, metrics: CHXMetrics, logfile: str, tclients: int) -> CHXMetrics:
        filename = logfile + ".log"
        with open(filename) as fh:
            for line in fh.readlines():
                self._chX_process_line(line, metrics, tclients)
        return metrics

    def ch2_metrics(self, logfile: str, tclients: int) -> CH2Metrics:
        return self._chX_metrics(CH2Metrics(), logfile, tclients)

    def ch3_metrics(self, logfile: str, tclients: int) -> CH3Metrics:
        return self._chX_metrics(CH3Metrics(), logfile, tclients)

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

        return self._metric(value, metric_id=metric_id, title=title, chirality=chirality)

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

        values = self._read_values("sgimport_latency", "sgimport_latency")
        lag = round(self._percentile(values, percentile), 2)
        return self._metric(lag, metric_id=metric_id, title=title)

    def sgimport_items_per_sec(self, time_elapsed: float, items_in_range: int,
                               operation: str) -> Metric:
        title = 'Average throughput (docs/sec) {}, {}'.format(operation, self._title)
        metric_id = '{}_{}_{}'.format(
            self.test_config.name, "throughput", operation)
        items_in_range = items_in_range
        rate = round(items_in_range / time_elapsed)
        return self._metric(rate, title=title, metric_id=metric_id)

    def sgreplicate_items_per_sec(self, time_elapsed: float, items_in_range: int) -> Metric:
        items_in_range = items_in_range
        logger.info("*** {} {} ***".format(items_in_range, time_elapsed))
        rate = round(items_in_range / time_elapsed)
        return self._metric(rate)

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
        if throughput < 100:
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
        throughput = self._parse_sg_throughput(operation)
        return self._metric(throughput, metric_id=metric_id, title=metric_title)

    def avg_sg_cpu_usage(self, title) -> Metric:
        metric_id = '{}_Average_sg_cpu_usage'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        if self.cluster_spec.capella_infrastructure:
            metric = "sgw_resource_utilization_process_cpu_percent_utilization"
        else:
            metric = "syncgateway__global__resource_utilization__process_cpu_percent_utilization"
        values = self._read_values(metric, "syncgateway_cluster_stats")
        avg_cpu = round(self._mean(values) / 100, 2)
        return self._metric(avg_cpu, metric_id=metric_id, title=metric_title)

    def avg_sg_mem_usage(self, title) -> Metric:
        metric_id = '{}_Average_sg_memory_usage'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)

        if self.cluster_spec.capella_infrastructure:
            metric = "sgw_resource_utilization_process_memory_resident"
        else:
            metric = "syncgateway__global__resource_utilization__process_memory_resident"
        values = self._read_values(metric, "syncgateway_cluster_stats")
        avg_mem = round(self._mean(values), 2)
        avg_mem = int(bytes_to_mib(avg_mem))
        return self._metric(avg_mem, metric_id=metric_id, title=metric_title)

    def sg_resync_throughput(self, resync_throughput, title) -> Metric:
        metric_id = f'{self.test_config.name}_resync_throughput'
        metric_title = f"{title}{self._title}"
        return self._metric(round(resync_throughput), metric_id=metric_id, title=metric_title)

    def sg_bp_throughput(self, title) -> Metric:
        metric_id = '{}_throughput'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        throughput = round(self._parse_sg_bp_throughput())
        return self._metric(throughput, metric_id=metric_id, title=metric_title)

    def sg_newdocpush_throughput(self, title) -> Metric:
        metric_id = '{}_throughput'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        throughput = round(self._parse_newdocpush_throughput())
        return self._metric(throughput, metric_id=metric_id, title=metric_title)

    def sg_bp_total_docs_pulled(self, title, duration) -> Metric:
        metric_id = '{}_docs_pulled'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        docs_pulled_per_sec = round(self._sg_bp_total_docs_pulled() / duration)
        return self._metric(docs_pulled_per_sec, metric_id=metric_id, title=metric_title)

    def sg_bp_total_docs_pushed(self, title, duration) -> Metric:
        metric_id = '{}_docs_pushed'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        docs_pulled_per_sec = round(self._sg_bp_total_docs_pushed() / duration)
        return self._metric(docs_pulled_per_sec, metric_id=metric_id, title=metric_title)

    def sg_bp_num_replications(self, title, documents: int) -> Metric:
        metric_id = '{}_num_replications'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        num_replications = self._get_num_replications(documents)
        return self._metric(num_replications, metric_id=metric_id, title=metric_title)

    def sg_latency(self, metric_name, title) -> Metric:
        metric_id = '{}_latency'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)

        lat = float(self._parse_sg_latency(metric_name) / 1000)
        if lat < 10:
            lat = round(lat, 2)
        else:
            lat = round(lat)

        return self._metric(lat, metric_id=metric_id, title=metric_title)

    def deltasync_time(self, replication_time: float) -> Metric:
        title = 'Replication time (sec) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "time")
        replication_time = round(replication_time, 3)
        return self._metric(replication_time, title=title, metric_id=metric_id)

    def deltasync_throughput(self, throughput: int) -> Metric:
        title = 'Throughput (docs/sec) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "throughput")

        return self._metric(throughput, title=title, metric_id=metric_id)

    def deltasync_bandwidth(self, bandwidth: float) -> Metric:
        title = 'Bandwidth Usage (MB/sec) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "bandwidth")
        return self._metric(bandwidth, title=title, metric_id=metric_id)

    def deltasync_bytes(self, bytes: float) -> Metric:
        title = 'Bytes Transfer (MB) {}'.format(self._title)
        metric_id = '{}_{}'.format(self.test_config.name, "Mbytes")
        # in MB
        bytes = round(((bytes/1024)/1024), 2)
        return self._metric(bytes, title=title, metric_id=metric_id)

    def sgw_e2e_throughput(self, throughput: int,
                           operation: str, replication: str) -> Metric:
        title = 'SGW {} {} Throughput (docs/sec) {}'.format(
            replication.lower(), operation, self._title)
        metric_id = '{}_{}_{}_{}'.format(
            self.test_config.name, "throughput", operation, replication.lower())
        return self._metric(round(throughput), title=title, metric_id=metric_id)

    def sgw_e2e_throughput_per_cblite(self, throughput: int,
                                      operation: str, replication: str) -> Metric:
        title = 'SGW {} {} Throughput (docs/sec) per cblite {}'.format(
            replication.lower(), operation, self._title)
        metric_id = '{}_{}_{}_{}'.format(
            self.test_config.name, "throughput_per_cblite", operation, replication.lower())
        return self._metric(round(throughput), title=title, metric_id=metric_id)

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

    def aibench_metrics(self, metrics: dict) -> list[Metric]:
        reported_metrics = []
        tracked_metrics = {
            "requests_per_second": "Throughput (requests/sec)",
            "p50_latency": "50th percentile latency (sec)",
            "p99_latency": "99th percentile latency (sec)",
            "ttft_avg": "Time to first token (sec)",
            "tokens_per_second": "Tokens per second (tokens/sec)",
        }
        metadata = metrics.pop("metadata", {})
        endpoint = metadata.get("endpoint", "")
        model_name = metadata.get("model_name", "")
        concurrency = int(metadata.get("concurrency", ""))
        sub_title = f"({model_name} - {endpoint}), {concurrency} concurrent requests"

        for name, proper_name in tracked_metrics.items():
            value = float(metrics.get(name, 0))
            if value == 0:
                continue
            metric_id = f"{self.test_config.name}_{name}_{endpoint}_{concurrency}"
            title = f"{proper_name}, {self._title} {sub_title}"
            metric_info = self._metric_info(metric_id, title, order_by=name)
            reported_metrics.append((round(value, 2), self._snapshots, metric_info))
        return reported_metrics

    def _workflow_throughput(
        self,
        workflow_time: float,
        successful_items: int,
        workflow_type: str,
        unit: str,
        model_name: str,
    ) -> Metric:
        title = f"{workflow_type} Throughput ({unit}/sec), {self._title} ({model_name})"
        metric_id = f"{self.test_config.name}_{workflow_type}_throughput"
        throughput = successful_items / workflow_time
        return self._metric(round(throughput, 2), metric_id=metric_id, title=title)

    def uds_throughput(
        self, ingestion_time: float, num_successful_files: int, model_name: str
    ) -> Metric:
        return self._workflow_throughput(
            ingestion_time, num_successful_files, "UDS", "docs", model_name
        )

    def workflow_execution_time(self, workflow_time: float, model_name: str) -> Metric:
        title = f"Workflow Processing Time (min), {self._title} ({model_name})"
        metric_id = f"{self.test_config.name}_processing_time"
        return self._metric(s2m(workflow_time), metric_id=metric_id, title=title)

    def vectorization_throughput(
        self, autovec_time: float, num_successful_embeddings: int, model_name: str
    ) -> Metric:
        return self._workflow_throughput(
            autovec_time, num_successful_embeddings, "Autovec", "embeddings", model_name
        )

    def model_deployment_time(
        self, deployment_time: float, model_name: str, model_kind: str
    ) -> Metric:
        title = f"Model Deployment Time, {model_name}, {model_kind} (min)"
        model_name = (
            model_name.replace(" ", "_").replace("-", "_").replace(".", "_").replace("/", "_")
        )
        metric_id = f"model_deployment_time_{model_name}"
        return self._metric(s2m(deployment_time), metric_id=metric_id, title=title)

    def fio_iops(self, iops: float, cluster: str, node: str, job_name: str) -> Metric:
        title = f"{node}, {job_name} (iops), {self._title}"
        metric_id = f"{self.test_config.name}_{node.replace('.', '')}_{job_name}"
        metric_info = self._metric_info(metric_id, title)
        metric_info["category"] = cluster
        return round(iops), None, metric_info


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
        data_size = bytes_to_mib(
            self.test_config.load_settings.items * self.test_config.load_settings.size
        )  # MB
        throughput = round(data_size / time_elapsed)

        return 'Avg Throughput (MB/sec)', \
            throughput, \
            self._snapshots

    def jts_throughput(self) -> DailyMetric:
        timings = self._jts_metric(collector="jts_stats", metric="jts_throughput")
        throughput = round(self._mean(timings), 2)
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

    def percentile_kv_latency(
        self,
        operation: str,
        percentiles: Optional[Iterable[Number]] = None,
        collector: str = "spring_latency",
    ) -> list[DailyMetric]:
        percentiles = list(percentiles or [99.9])
        metrics = []
        stat_groups = self.test_config.collection.collection_stat_groups or ['']

        for stat_group in stat_groups:
            w_latencies = self._calculate_timeseries_stats(
                calc_percentiles_fn(percentiles),
                operation,
                collector,
                [TimeseriesWindow()],
                stat_group,
            )
            for _, latencies in w_latencies:
                for percentile, latency in zip(percentiles, latencies):
                    metric_title = "{}th percentile {}{}".format(
                        percentile,
                        operation.upper(),
                        " (" + stat_group + ")" if stat_group != "" else "",
                    )

                    metrics.append((metric_title, latency, self._snapshots))

        return metrics

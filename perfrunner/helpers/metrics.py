import glob
import os
import re
from typing import Dict, List, Tuple, Union

import numpy as np

from cbagent.stores import PerfStore
from logger import logger
from perfrunner.settings import CBMONITOR_HOST

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


def s2m(seconds: float) -> float:
    """Convert seconds to minutes."""
    return round(seconds / 60, 1)


def strip(s: str) -> str:
    for c in ' &()':
        s = s.replace(c, '')
    return s.lower()


class MetricHelper:

    def __init__(self, test):
        self.test = test
        self.test_config = test.test_config
        self.cluster_spec = test.cluster_spec

        self.store = PerfStore(CBMONITOR_HOST)

    @property
    def _title(self) -> str:
        return self.test_config.showfast.title

    @property
    def _snapshots(self) -> List[str]:
        return self.test.cbmonitor_snapshots

    @property
    def _num_nodes(self):
        return self.test_config.cluster.initial_nodes[0]

    def _metric_info(self,
                     metric_id: str = None,
                     title: str = None,
                     order_by: str = '') -> Dict[str, str]:
        return {
            'id': metric_id or self.test_config.name,
            'title': title or self._title,
            'orderBy': order_by,
        }

    def ycsb_queries(self, value: float, name: str, title: str) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, name)
        title = '{}, {}'.format(title, self.test_config.showfast.title)
        metric_info = self._metric_info(metric_id, title)
        return value, self._snapshots, metric_info

    @property
    def query_id(self) -> str:
        if 'views' in self._title:
            return ''

        query_id = self._title.split(',')[0]
        query_id = query_id.split()[0]
        for prefix in 'CI', 'Q', 'UP', 'DL', 'AG':
            query_id = query_id.replace(prefix, '')
        return '{:05d}'.format(int(query_id))

    def avg_n1ql_throughput(self) -> Metric:
        metric_id = '{}_avg_query_requests'.format(self.test_config.name)
        title = 'Avg. Query Throughput (queries/sec), {}'.format(self._title)

        metric_info = self._metric_info(metric_id, title,
                                        order_by=self.query_id)
        throughput = self._avg_n1ql_throughput()
        return throughput, self._snapshots, metric_info

    def _avg_n1ql_throughput(self) -> int:
        test_time = self.test_config.access_settings.time

        query_node = self.cluster_spec.servers_by_role('n1ql')[0]
        vitals = self.test.rest.get_query_stats(query_node)
        total_requests = vitals['requests.count']

        throughput = total_requests / test_time
        return round(throughput, throughput < 1 and 1 or 0)

    def bulk_n1ql_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

        items = self.test_config.load_settings.items / 4
        throughput = round(items / time_elapsed)

        return throughput, self._snapshots, metric_info

    def fts_index(self, elapsed_time: float, order_by: str) -> Metric:
        metric_id = self.test_config.name
        title = 'Index build time(sec), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title, order_by)

        index_time = round(elapsed_time, 1)

        return index_time, self._snapshots, metric_info

    def fts_index_size(self, index_size_raw: int, order_by: str) -> Metric:
        metric_id = "{}_indexsize".format(self.test_config.name)
        title = 'Index size (MB), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title, order_by)

        index_size_mb = int(index_size_raw / (1024 ** 2))

        return index_size_mb, self._snapshots, metric_info

    def fts_rebalance_time(self,
                           reb_time: float,
                           order_by: str,
                           name: str = 'FTS') -> Metric:
        metric_id = "{}_reb".format(self.test_config.name)
        title = 'Rebalance time (min), {}, {}'.format(self._title, name)
        metric_info = self._metric_info(metric_id, title, order_by)

        reb_time = s2m(reb_time)

        return reb_time, self._snapshots, metric_info

    def jts_throughput(self, order_by) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, "jts_throughput")
        metric_id = metric_id.replace('.', '')
        title = "Average Throughput (q/sec), {}".format(self._title)
        metric_info = self._metric_info(metric_id, title, order_by)
        timings = self._jts_metric(collector="jts_stats", metric="jts_throughput")
        thr = round(np.average(timings), 2)
        if thr > 100:
            thr = round(thr)
        return thr, self._snapshots, metric_info

    def jts_latency(self, order_by, percentile=50) -> Metric:
        prefix = "Average latency (ms)"
        if percentile != 50:
            prefix = "{}th percentile latency (ms)".format(percentile)
        metric_id = '{}_{}'.format(self.test_config.name, "jts_latency")
        metric_id = metric_id.replace('.', '')
        title = "{}, {}".format(prefix, self._title)
        metric_info = self._metric_info(metric_id, title, order_by)
        timings = self._jts_metric(collector="jts_stats", metric="jts_latency")
        lat = round(np.percentile(timings, percentile), 2)
        if lat > 100:
            lat = round(lat)
        return lat, self._snapshots, metric_info

    def _jts_metric(self, collector, metric):
        timings = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector=collector,
                                         bucket=bucket)
            timings += self.store.get_values(db, metric=metric)
        return timings

    def max_ops(self) -> Metric:
        metric_info = self._metric_info(
            order_by=self.test_config.showfast.order_by,
        )
        throughput = self._max_ops()

        return throughput, self._snapshots, metric_info

    def _max_ops(self) -> int:
        values = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='ops')

        return int(np.percentile(values, 90))

    def get_collector_values(self, collector):
        values = []
        for bucket in self.test_config.buckets:
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
        metric_info = self._metric_info(metric_id, title)

        xdcr_lag = self.get_percentile_value_of_collector('xdcr_lag', percentile)

        return round(xdcr_lag, 1), self._snapshots, metric_info

    def avg_replication_rate(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info(
            order_by=self.test_config.showfast.order_by,
        )

        rate = self._avg_replication_rate(time_elapsed)

        return rate, self._snapshots, metric_info

    def _avg_replication_rate(self, time_elapsed: float) -> float:
        initial_items = self.test_config.load_settings.ops or \
            self.test_config.load_settings.items
        num_buckets = self.test_config.cluster.num_buckets
        avg_replication_rate = num_buckets * initial_items / time_elapsed

        return round(avg_replication_rate)

    def max_drain_rate(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

        items_per_node = self.test_config.load_settings.items / self._num_nodes
        drain_rate = round(items_per_node / time_elapsed)

        return drain_rate, self._snapshots, metric_info

    def avg_disk_write_queue(self) -> Metric:
        metric_info = self._metric_info()

        values = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='disk_write_queue')

        disk_write_queue = int(np.average(values))

        return disk_write_queue, self._snapshots, metric_info

    def avg_total_queue_age(self) -> Metric:
        metric_info = self._metric_info()

        values = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='vb_avg_total_queue_age')

        avg_total_queue_age = int(np.average(values))

        return avg_total_queue_age, self._snapshots, metric_info

    def avg_couch_views_ops(self) -> Metric:
        metric_info = self._metric_info()

        values = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='couch_views_ops')

        couch_views_ops = int(np.average(values))

        return couch_views_ops, self._snapshots, metric_info

    def query_latency(self, percentile: Number) -> Metric:
        metric_id = self.test_config.name
        title = '{}th percentile query latency (ms), {}'.format(percentile,
                                                                self._title)
        metric_info = self._metric_info(metric_id, title,
                                        order_by=self.query_id)

        latency = self._query_latency(percentile)

        return latency, self._snapshots, metric_info

    def _query_latency(self, percentile: Number) -> float:
        values = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='spring_query_latency',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='latency_query')

        query_latency = np.percentile(values, percentile)
        if query_latency < 100:
            return round(query_latency, 1)
        return int(query_latency)

    def secondary_scan_latency(self, percentile: Number) -> Metric:
        metric_id = self.test_config.name
        title = '{}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                         self._title)
        metric_info = self._metric_info(metric_id, title)

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

    def kv_latency(self,
                   operation: str,
                   percentile: Number = 99.9,
                   collector: str = 'spring_latency') -> Metric:
        metric_id = '{}_{}_{}th'.format(self.test_config.name,
                                        operation,
                                        percentile)
        metric_id = metric_id.replace('.', '')
        title = '{}th percentile {} {}'.format(percentile,
                                               operation.upper(),
                                               self._title)
        metric_info = self._metric_info(metric_id, title)

        latency = self._kv_latency(operation, percentile, collector)

        return latency, self._snapshots, metric_info

    def _kv_latency(self,
                    operation: str,
                    percentile: Number,
                    collector: str) -> float:
        timings = []
        metric = 'latency_{}'.format(operation)
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector=collector,
                                         bucket=bucket)
            timings += self.store.get_values(db, metric=metric)

        latency = np.percentile(timings, percentile)
        if latency > 100:
            return round(latency)
        return round(latency, 2)

    def observe_latency(self, percentile: Number) -> Metric:
        metric_id = '{}_{}th'.format(self.test_config.name, percentile)
        title = '{}th percentile {}'.format(percentile, self._title)
        metric_info = self._metric_info(metric_id, title)

        timings = []
        for bucket in self.test_config.buckets:
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
        metric_info = self._metric_info(metric_id, title)

        cluster = self.test.cbmonitor_clusters[0]
        bucket = self.test_config.buckets[0]

        db = self.store.build_dbname(cluster=cluster,
                                     collector='ns_server',
                                     bucket=bucket)
        values = self.store.get_values(db, metric='cpu_utilization_rate')

        cpu_utilization = int(np.average(values))

        return cpu_utilization, self._snapshots, metric_info

    def max_memcached_rss(self) -> Metric:
        metric_id = '{}_memcached_rss'.format(self.test_config.name)
        title = 'Max. memcached RSS (MB),{}'.format(
            self._title.split(',')[-1]
        )
        metric_info = self._metric_info(metric_id, title)

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
        metric_info = self._metric_info(metric_id, title)

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
        metric_info = self._metric_info()

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
                          unit: str = "min") -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, index_type.lower())
        title = '{} index ({}), {}'.format(index_type, unit, self._title)
        metric_info = self._metric_info(metric_id, title)
        metric_info['category'] = index_type.lower()

        value = s2m(value)

        return value, self._snapshots, metric_info

    def get_memory_meta(self,
                        value: float,
                        memory_type: str) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name,
                                   memory_type.replace(" ", "").lower())
        title = '{} (GB), {}'.format(memory_type, self._title)
        metric_info = self._metric_info(metric_id, title)

        return value, self._snapshots, metric_info

    def bnr_throughput(self,
                       time_elapsed: float,
                       edition: str,
                       tool: str) -> Metric:
        metric_id = '{}_{}_thr_{}'.format(self.test_config.name, tool, edition)
        title = '{} {} throughput (Avg. MB/sec), {}'.format(
            edition, tool, self._title)
        metric_info = self._metric_info(metric_id, title)

        data_size = self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2 ** 20  # MB

        avg_throughput = round(data_size / time_elapsed)

        return avg_throughput, self._snapshots, metric_info

    def backup_size(self, size: float, edition: str) -> Metric:
        metric_id = '{}_size_{}'.format(self.test_config.name, edition)
        title = '{} backup size (GB), {}'.format(edition, self._title)
        metric_info = self._metric_info(metric_id, title)

        return size, self._snapshots, metric_info

    def merge_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

        data_size = 2 * self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2 ** 20  # MB

        avg_throughput = round(data_size / time_elapsed)

        return avg_throughput, self._snapshots, metric_info

    def import_and_export_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

        data_size = self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2 ** 20  # MB

        avg_throughput = round(data_size / time_elapsed)

        return avg_throughput, self._snapshots, metric_info

    def import_file_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

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

    def _parse_ycsb_throughput(self) -> int:
        throughput = 0
        for filename in glob.glob("YCSB/ycsb_run_*.log"):
            with open(filename) as fh:
                for line in fh.readlines():
                    if line.startswith('[OVERALL], Throughput(ops/sec)'):
                        throughput += int(float(line.split()[-1]))
        return throughput

    def _parse_dcp_throughput(self, output_file: str = 'dcpstatsfile') -> int:
        # Get throughput from OUTPUT_FILE for posting to showfast
        with open(output_file) as fh:
            output_text = fh.read()
            groups = re.search(
                r"Throughput = [^\d]*(\d*).*?",
                output_text)
            return int(groups.group(1))

    def dcp_throughput(self) -> Metric:
        metric_info = self._metric_info()

        throughput = self._parse_dcp_throughput()

        return throughput, self._snapshots, metric_info

    def fragmentation_ratio(self, ratio: float) -> Metric:
        metric_info = self._metric_info()

        return ratio, self._snapshots, metric_info

    def elapsed_time(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

        time_elapsed = s2m(time_elapsed)

        return time_elapsed, self._snapshots, metric_info

    def kv_throughput(self, total_ops: int) -> Metric:
        metric_info = self._metric_info()

        throughput = total_ops // self.test_config.access_settings.time

        return throughput, self._snapshots, metric_info

    def ycsb_throughput(self) -> Metric:
        metric_info = self._metric_info()

        throughput = self._parse_ycsb_throughput()

        return throughput, self._snapshots, metric_info

    def indexing_time(self, indexing_time: float) -> Metric:
        return self.elapsed_time(indexing_time)

    @property
    def rebalance_order_by(self) -> str:
        order_by = ''
        for num_nodes in self.test_config.cluster.initial_nodes:
            order_by += '{:03d}'.format(num_nodes)
        order_by += '{:018d}'.format(self.test_config.load_settings.items)
        return order_by

    def rebalance_time(self, rebalance_time: float) -> Metric:
        metric = self.elapsed_time(rebalance_time)
        metric[-1]['orderBy'] = self.rebalance_order_by
        return metric

    def failover_time(self, delta: float) -> Metric:
        metric_info = self._metric_info()

        return delta, self._snapshots, metric_info

    def gsi_connections(self, num_connections: int) -> Metric:
        metric_info = self._metric_info()

        return num_connections, self._snapshots, metric_info

    def scan_throughput(self, throughput: float) -> Metric:
        metric_info = self._metric_info()

        throughput = round(throughput, 1)

        return throughput, self._snapshots, metric_info

    def multi_scan_diff(self, time_diff: float):
        metric_info = self._metric_info()

        time_diff = round(time_diff, 2)

        return time_diff, self._snapshots, metric_info

    def get_functions_throughput(self, time: int, event_name: str, events_processed: int) -> float:
        throughput = 0
        if event_name:
            for name, file in self.test.functions.items():
                throughput += self.test.rest.get_num_events_processed(
                    event=event_name, node=self.test.eventing_nodes[0], name=name)
        else:
            throughput = events_processed
        throughput /= len(self.test.functions)
        throughput /= time
        return round(throughput, 0)

    def function_throughput(self, time: int, event_name: str, events_processed: int) -> Metric:
        metric_info = self._metric_info()

        throughput = self.get_functions_throughput(time, event_name, events_processed)

        return throughput, self._snapshots, metric_info

    def eventing_rebalance_time(self, time: int) -> Metric:
        title_split = self._title.split(sep=",", maxsplit=2)
        title = "Rebalance Time(sec)," + title_split[1]
        metric_id = '{}_rebalance_time'.format(self.test_config.name)
        metric_info = self._metric_info(metric_id=metric_id, title=title)
        return time, self._snapshots, metric_info

    def function_latency(self, percentile: float, latency_stats: dict) -> Metric:
        """Calculate eventing function latency stats.

        We get latency stats in format of- time:number of events processed in that time(samples)
        In this method we get latency stats then calculate percentile latency using-
        Calculate total number of samples.
        Keep adding sample until we get required percentile number.
        For now it calculates for one function only
        """
        metric_info = self._metric_info()
        latency = 0
        for name, stats in latency_stats.items():
            total_samples = sum([sample for time, sample in stats])
            latency_samples = 0
            for time, samples in stats:
                latency_samples += samples
                if latency_samples >= (total_samples * percentile / 100):
                    latency = float(time) / 1000
                    break

        latency = round(latency, 1)
        return latency, self._snapshots, metric_info

    def function_time(self, time: int, time_type: str, initials: str) -> Metric:
        title = initials + ", " + self._title
        metric_id = '{}_{}'.format(self.test_config.name, time_type.lower())
        metric_info = self._metric_info(metric_id=metric_id, title=title)
        time = s2m(seconds=time)

        return time, self._snapshots, metric_info

    def analytics_latency(self, query: dict, latency: int) -> Metric:
        metric_id = self.test_config.name + strip(query['description'])

        title = 'Avg. query latency (ms), {} {}, {}'.format(query['name'],
                                                            query['description'],
                                                            self._title)

        order_by = '{:05d}'.format(int(query['name'][1:]))

        metric_info = self._metric_info(metric_id,
                                        title,
                                        order_by)

        return latency, self._snapshots, metric_info

    def get_max_rss_values(self, function_name: str, server: str):
        ratio = 1024 * 1024
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
        metric_info = self._metric_info()

        rate = round(num_items / time_elapsed)

        return rate, self._snapshots, metric_info


class DailyMetricHelper(MetricHelper):

    def indexing_time(self, time_elapsed: float) -> DailyMetric:
        return 'Initial Indexing Time (min)', \
            s2m(time_elapsed), \
            self._snapshots

    def dcp_throughput(self) -> DailyMetric:
        return 'Avg Throughput (items/sec)', \
            self._parse_dcp_throughput(), \
            self._snapshots

    def rebalance_time(self, rebalance_time: float) -> DailyMetric:
        return 'Rebalance Time (min)', \
            s2m(rebalance_time), \
            self._snapshots

    def avg_n1ql_throughput(self) -> DailyMetric:
        return 'Avg Query Throughput (queries/sec)', \
            self._avg_n1ql_throughput(), \
            self._snapshots

    def max_ops(self) -> DailyMetric:
        return 'Max Throughput (ops/sec)', \
            super()._max_ops(), \
            self._snapshots

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

    def jts_throughput(self, order_by) -> DailyMetric:
        timings = self._jts_metric(collector="jts_stats", metric="jts_throughput")
        throughput = round(np.average(timings), 2)
        if throughput > 100:
            throughput = round(throughput)

        return 'Avg Query Throughput (queries/sec)', \
            throughput, \
            self._snapshots

    def function_throughput(self, time: int, event_name: str, events_processed: int) -> DailyMetric:
        throughput = self.get_functions_throughput(time, event_name, events_processed)

        metric = "Avg Throughput (functions executed/sec)"
        if "timer" in self._title:
            metric = "Avg Throughput (timers executed/sec)"

        return metric, \
            throughput, \
            self._snapshots

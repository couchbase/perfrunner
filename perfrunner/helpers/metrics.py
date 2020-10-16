import glob
import os
import re
from typing import Dict, List, Tuple, Union

import numpy as np

from cbagent.stores import PerfStore
from logger import logger
from perfrunner.settings import StatsSettings

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
    return round(seconds / 60, 2)


class MetricHelper:

    def __init__(self, test):
        self.test = test
        self.test_config = test.test_config
        self.cluster_spec = test.cluster_spec

        self.store = PerfStore(StatsSettings.CBMONITOR)

    @property
    def _title(self) -> str:
        return self.test_config.test_case.title

    @property
    def _snapshots(self) -> List[str]:
        return self.test.cbmonitor_snapshots

    @property
    def _num_nodes(self):
        return self.test_config.cluster.initial_nodes[0]

    @property
    def _order_by(self) -> str:
        return self.test_config.test_case.order_by

    def _metric_info(self,
                     metric_id: str = None,
                     title: str = None,
                     order_by: str = '') -> Dict[str, str]:
        return {
            'id': metric_id or self.test_config.name,
            'title': title or self._title,
            'orderBy': order_by or self._order_by,
        }

    def ycsb_queries(self, value: float, name: str, title: str) -> Metric:
        metric_id = '{}_{}'.format(self.test_config.name, name)
        title = '{}, {}'.format(title, self.test_config.test_case.title)
        metric_info = self._metric_info(metric_id, title)

        return value, self._snapshots, metric_info

    def avg_n1ql_throughput(self) -> Metric:
        metric_id = '{}_avg_query_requests'.format(self.test_config.name)
        title = 'Avg. Query Throughput (queries/sec), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title)

        throughput = self._avg_n1ql_throughput()

        return throughput, self._snapshots, metric_info

    def _avg_n1ql_throughput(self) -> int:
        test_time = self.test_config.access_settings.time

        query_node = self.cluster_spec.servers_by_role('n1ql')[0]
        vitals = self.test.rest.get_query_stats(query_node)
        total_requests = vitals['requests.count']

        return int(total_requests / test_time)

    def bulk_n1ql_throughput(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

        items = self.test_config.load_settings.items / 4
        throughput = round(items / time_elapsed)

        return throughput, self._snapshots, metric_info

    def avg_fts_throughput(self, order_by: str, name: str = 'FTS') -> Metric:
        metric_id = '{}_avg_query_requests'.format(self.test_config.name)
        title = 'Query Throughput (queries/sec), {}, {} node, {}'.\
            format(self._title, self._num_nodes, name)
        metric_info = self._metric_info(metric_id, title, order_by)

        if name == 'FTS':
            total_queries = 0
            for host in self.test.active_fts_hosts:
                all_stats = self.test.rest.get_fts_stats(host)
                key = "{}:{}:{}".format(self.test_config.buckets[0],
                                        self.test.index_name,
                                        "total_queries")
                if key in all_stats:
                    total_queries += all_stats[key]

        else:
            all_stats = self.test.rest.get_elastic_stats(self.test.master_host)
            total_queries = all_stats["_all"]["total"]["search"]["query_total"]

        time_taken = self.test_config.access_settings.time
        throughput = total_queries / float(time_taken)
        if throughput < 100:
            throughput = round(throughput, 2)
        else:
            throughput = round(throughput)

        return throughput, self._snapshots, metric_info

    def latency_fts_queries(self,
                            percentile: Number,
                            dbname: str,
                            metric: str,
                            order_by: str,
                            name='FTS') -> Metric:
        if percentile == 0:
            metric_id = '{}_average'.format(self.test_config.name)
            title = 'Average query latency (ms), {}, {} node, {}'.\
                format(self._title, self._num_nodes, name)
        else:
            metric_id = self.test_config.name
            title = '{}th percentile query latency (ms), {}, {} node, {}'. \
                format(percentile, self._title, self._num_nodes, name)

        metric_info = self._metric_info(metric_id, title, order_by)

        db = '{}{}'.format(dbname, self.test.cbmonitor_clusters[0])

        timings = self.store.get_values(db, metric)

        if percentile == 0:
            latency = np.average(timings)
        else:
            latency = np.percentile(timings, percentile)

        latency = round(latency)

        return latency, self._snapshots, metric_info

    def fts_index(self,
                  elapsed_time: float,
                  order_by: str,
                  name: str ='FTS') -> Metric:
        metric_id = self.test_config.name
        title = 'Index build time(sec), {}, {} node, {}'.\
            format(self._title, self._num_nodes, name)
        metric_info = self._metric_info(metric_id, title, order_by)

        index_time = round(elapsed_time, 1)

        return index_time, self._snapshots, metric_info

    def fts_index_size(self,
                       index_size_raw: int,
                       order_by: str,
                       name: str = 'FTS') -> Metric:
        metric_id = "{}_indexsize".format(self.test_config.name)
        title = 'Index size (MB), {}, {} node, {}'.\
                format(self._title, self._num_nodes, name)
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

    def max_ops(self) -> Metric:
        metric_info = self._metric_info()
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

    def xdcr_lag(self, percentile: Number = 95) -> Metric:
        metric_id = '{}_{}th_xdc_lag'.format(self.test_config.name, percentile)
        title = '{}th percentile replication lag (ms), {}'.format(
            percentile, self._title)
        metric_info = self._metric_info(metric_id, title)

        values = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='xdcr_lag',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='xdcr_lag')

        lag = round(np.percentile(values, percentile), 1)

        return lag, self._snapshots, metric_info

    def sgimport_latency(self, percentile: Number = 95) -> Metric:
        metric_id = '{}_{}th_sgimport_latency'.format(self.test_config.name, percentile)
        title = '{}th percentile sgimport latency (ms), {}'.format(
            percentile, self._title)
        metric_info = self._metric_info(metric_id, title)

        values = []

        db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                     collector='sgimport_latency')
        values += self.store.get_values(db, metric='sgimport_latency')

        lag = round(np.percentile(values, percentile), 1)

        return lag, self._snapshots, metric_info

    def avg_replication_rate(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

        rate = self._avg_replication_rate(time_elapsed)

        return rate, self._snapshots, metric_info

    def sgimport_items_per_sec(self, time_elapsed: float, items_in_range: int) -> Metric:
        items_in_range = items_in_range

        metric_info = self._metric_info()

        rate = round(items_in_range / time_elapsed)

        return rate, self._snapshots, metric_info

    def sgreplicate_items_per_sec(self, time_elapsed: float, items_in_range: int) -> Metric:
        items_in_range = items_in_range

        metric_info = self._metric_info()

        rate = round(items_in_range / time_elapsed)

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

    def avg_bg_wait_time(self) -> Metric:
        metric_info = self._metric_info()

        values = []
        for bucket in self.test_config.buckets:
            db = self.store.build_dbname(cluster=self.test.cbmonitor_clusters[0],
                                         collector='ns_server',
                                         bucket=bucket)
            values += self.store.get_values(db, metric='avg_bg_wait_time')

        avg_bg_wait_time = np.mean(values) / 10 ** 3  # us -> ms
        avg_bg_wait_time = round(avg_bg_wait_time, 2)

        return avg_bg_wait_time, self._snapshots, metric_info

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

        metric_info = self._metric_info(metric_id, title)

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

    def max_beam_rss(self) -> Metric:
        metric_id = 'beam_rss_max_{}'.format(self.test_config.name)
        title = 'Max. beam.smp RSS (MB), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title)

        max_rss = 0
        for cluster_name, servers in self.cluster_spec.clusters:
            cluster = list(filter(lambda name: name.startswith(cluster_name),
                                  self.test.cbmonitor_clusters))[0]
            for server in servers:
                hostname = server.replace('.', '')
                db = self.store.build_dbname(cluster=cluster,
                                             collector='atop',
                                             server=hostname)
                values = self.store.get_values(db, metric='beam.smp_rss')
                rss = round(max(values) / 1024 ** 2)
                max_rss = max(max_rss, rss)

        return max_rss, self._snapshots, metric_info

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

    def _parse_sg_throughput(self) -> int:
        throughput = 0
        for filename in glob.glob("YCSB/*_runtest_*.result"):
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
                    # print('printing each line :', i)
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
                    # print('printing each line :', i)
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
                    # print('printing each line :', i)
                    if "docs_per_sec" in i:
                        c += 1
                        total_docs_per_sec += float(i.split(':')[1])
                average_doc_per_sec = total_docs_per_sec / c
            fc += 1
            sum_doc_per_sec += average_doc_per_sec

        throughput = sum_doc_per_sec / fc

        return throughput

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
                    # print('printing each line :', i)
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

    def sg_throughput(self, title) -> Metric:
        metric_id = '{}_throughput'.format(self.test_config.name)
        metric_title = "{}{}".format(title, self._title)
        metric_info = self._metric_info(metric_id, metric_title)
        throughput = self._parse_sg_throughput()
        return throughput, self._snapshots, metric_info

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

    def indexing_time(self, indexing_time: float) -> Metric:
        return self.elapsed_time(indexing_time)

    def rebalance_time(self, rebalance_time: float) -> Metric:
        return self.elapsed_time(rebalance_time)

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

    def function_throughput(self, time) -> Metric:
        metric_info = self._metric_info()

        throughput = 0
        for name, file in self.test.functions.items():
            throughput += self.test.rest.get_num_events_processed(
                node=self.test.function_nodes[0], name=name)
        throughput /= len(self.test.functions)
        throughput /= time

        return throughput, self._snapshots, metric_info


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

    def avg_fts_throughput(self, *args, **kwargs) -> DailyMetric:
        total_queries = 0
        for host in self.test.active_fts_hosts:
            all_stats = self.test.rest.get_fts_stats(host)
            key = "{}:{}:{}".format(self.test_config.buckets[0],
                                    self.test.index_name,
                                    "total_queries")
            if key in all_stats:
                total_queries += all_stats[key]

        throughput = total_queries // self.test_config.access_settings.time

        return 'Avg Query Throughput (queries/sec)', \
            throughput, \
            self._snapshots

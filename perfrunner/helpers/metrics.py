import glob
import os
import re
from typing import Any, Dict, List, Tuple, Union

import numpy as np
from logger import logger
from seriesly import Seriesly

from perfrunner.settings import StatsSettings

Metric = Tuple[
    Union[float, int],  # Value
    List[str],          # Snapshots
    Dict[str, str],     # Metric info
]

DailyMetric = Tuple[
    str,                # Metric
    Union[float, int],  # Value
    List[str],          # Snapshots
]


def s2m(seconds: float) -> float:
    """Converts seconds to minutes."""
    return round(seconds / 60, 2)


def get_query_params(metric: str) -> Dict[str, Any]:
    """Convert metric definition to Seriesly query params. E.g.:

        'avg_xdc_ops' -> {'ptr': '/xdc_ops',
                          'group': 1000000000000, 'reducer': 'avg'}

    Where group is constant."""
    return {
        'ptr': '/{}'.format(metric[4:]),
        'reducer': metric[:3],
        'group': 1000000000000,
    }


class MetricHelper:

    def __init__(self, test):
        self.test = test
        self.test_config = test.test_config
        self.cluster_spec = test.cluster_spec

        self.seriesly = Seriesly(StatsSettings.SERIESLY)

    @property
    def _title(self) -> str:
        return self.test_config.test_case.title

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

        for servers in self.cluster_spec.servers_by_role('n1ql'):
            query_node = servers[0]

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
                                        self.test.fts_index,
                                        "total_queries")
                if key in all_stats:
                    total_queries += all_stats[key]

        else:
            all_stats = self.test.rest.get_elastic_stats(self.test.fts_master_host)
            total_queries = all_stats["_all"]["total"]["search"]["query_total"]

        time_taken = self.test_config.access_settings.time
        throughput = total_queries / float(time_taken)
        if throughput < 100:
            throughput = round(throughput, 2)
        else:
            throughput = round(throughput)

        return throughput, self._snapshots, metric_info

    def latency_fts_queries(self,
                            percentile: int,
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

        timings = []
        db = '{}{}'.format(dbname, self.test.cbmonitor_clusters[0])
        data = self.seriesly[db].get_all()
        timings += [v[metric] for v in data.values()]

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
            db = 'ns_server{}{}'.format(self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].get_all()
            values += [v['ops'] for v in data.values()]
        return int(np.percentile(values, 90))

    def xdcr_lag(self, percentile: int = 95) -> Metric:
        metric_id = '{}_{}th_xdc_lag'.format(self.test_config.name, percentile)
        title = '{}th percentile replication lag (ms), {}'.format(
            percentile, self._title)
        metric_info = self._metric_info(metric_id, title)

        timings = []
        for bucket in self.test_config.buckets:
            db = 'xdcr_lag{}{}'.format(self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].get_all()
            timings += [v['xdcr_lag'] for v in data.values()]
        lag = round(np.percentile(timings, percentile), 1)

        return lag, self._snapshots, metric_info

    def avg_replication_rate(self, time_elapsed: float) -> Metric:
        metric_info = self._metric_info()

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

        query_params = get_query_params('avg_disk_write_queue')
        disk_write_queue = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].query(query_params)
            disk_write_queue += list(data.values())[0][0]
        disk_write_queue = round(disk_write_queue / 10 ** 6, 2)

        return disk_write_queue, self._snapshots, metric_info

    def avg_bg_wait_time(self) -> Metric:
        metric_info = self._metric_info()

        query_params = get_query_params('avg_avg_bg_wait_time')

        avg_bg_wait_time = []
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].query(query_params)
            avg_bg_wait_time.append(list(data.values())[0][0])
        avg_bg_wait_time = np.mean(avg_bg_wait_time) / 10 ** 3  # us -> ms
        avg_bg_wait_time = round(avg_bg_wait_time, 2)

        return avg_bg_wait_time, self._snapshots, metric_info

    def avg_couch_views_ops(self) -> Metric:
        metric_info = self._metric_info()

        query_params = get_query_params('avg_couch_views_ops')

        couch_views_ops = 0
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].query(query_params)
            couch_views_ops += list(data.values())[0][0]
        couch_views_ops = round(couch_views_ops)

        return couch_views_ops, self._snapshots, metric_info

    def query_latency(self, percentile: int) -> Metric:
        metric_id = self.test_config.name
        title = '{}th percentile query latency (ms), {}'.format(percentile,
                                                                self._title)

        metric_info = self._metric_info(metric_id, title)

        latency = self._query_latency(percentile)

        return latency, self._snapshots, metric_info

    def _query_latency(self, percentile: int) -> float:
        timings = []
        for bucket in self.test_config.buckets:
            db = 'spring_query_latency{}{}'.format(self.test.cbmonitor_clusters[0],
                                                   bucket)
            data = self.seriesly[db].get_all()
            timings += [value['latency_query'] for value in data.values()]
        query_latency = np.percentile(timings, percentile)
        if query_latency < 100:
            return round(query_latency, 1)
        return int(query_latency)

    def secondary_scan_latency(self, percentile: int) -> Metric:
        metric_id = self.test_config.name
        title = '{}th percentile secondary scan latency (ms), {}'.format(percentile,
                                                                         self._title)
        metric_info = self._metric_info(metric_id, title)

        timings = []
        cluster = ""
        for cid in self.test.cbmonitor_clusters:
            if "apply_scanworkload" in cid:
                cluster = cid
                break
        db = 'secondaryscan_latency{}'.format(cluster)
        data = self.seriesly[db].get_all()
        timings += [value['Nth-latency'] for value in data.values()]
        timings = list(map(int, timings))
        logger.info("Number of samples are {}".format(len(timings)))
        scan_latency = np.percentile(timings, percentile) / 1e6
        scan_latency = round(scan_latency, 2)

        return scan_latency, self._snapshots, metric_info

    def kv_latency(self,
                   operation: str,
                   percentile: int = 99,
                   dbname: str = 'spring_latency') -> Metric:
        metric_id = '{}_{}_{}th'.format(self.test_config.name,
                                        operation,
                                        percentile)
        title = '{}th percentile {} {}'.format(percentile,
                                               operation.upper(),
                                               self._title)
        metric_info = self._metric_info(metric_id, title)

        latency = self._kv_latency(operation, percentile, dbname)

        return latency, self._snapshots, metric_info

    def _kv_latency(self,
                    operation: str,
                    percentile: int,
                    dbname: str) -> float:
        timings = []
        op_key = 'latency_{}'.format(operation)
        for bucket in self.test_config.buckets:
            db = '{}{}{}'.format(dbname, self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].get_all()
            timings += [
                v[op_key] for v in data.values() if op_key in v
            ]
        return round(np.percentile(timings, percentile), 2)

    def observe_latency(self, percentile: int) -> Metric:
        metric_id = '{}_{}th'.format(self.test_config.name, percentile)
        title = '{}th percentile {}'.format(percentile, self._title)
        metric_info = self._metric_info(metric_id, title)

        timings = []
        for bucket in self.test_config.buckets:
            db = 'observe{}{}'.format(self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].get_all()
            timings += [v['latency_observe'] for v in data.values()]
        latency = round(np.percentile(timings, percentile), 2)

        return latency, self._snapshots, metric_info

    def cpu_utilization(self) -> Metric:
        metric_id = '{}_avg_cpu'.format(self.test_config.name)
        title = 'Avg. CPU utilization (%)'
        title = '{}, {}'.format(title, self._title)
        metric_info = self._metric_info(metric_id, title)

        cluster = self.test.cbmonitor_clusters[0]
        bucket = self.test_config.buckets[0]

        query_params = get_query_params('avg_cpu_utilization_rate')
        db = 'ns_server{}{}'.format(cluster, bucket)
        data = self.seriesly[db].query(query_params)
        cpu_utilization = round(list(data.values())[0][0])

        return cpu_utilization, self._snapshots, metric_info

    def mem_used(self, max_min: str = 'max') -> Metric:
        metric_id = '{}_{}_mem_used'.format(self.test_config.name, max_min)
        title = '{}. mem_used (MB), {}'.format(max_min.title(),
                                               self._title)
        metric_info = self._metric_info(metric_id, title)

        query_params = get_query_params('max_mem_used')

        mem_used = []
        for bucket in self.test_config.buckets:
            db = 'ns_server{}{}'.format(self.test.cbmonitor_clusters[0], bucket)
            data = self.seriesly[db].query(query_params)

            mem_used.append(
                round(list(data.values())[0][0] / 1024 ** 2)  # -> MB
            )
        mem_used = eval(max_min)(mem_used)

        return mem_used, self._snapshots, metric_info

    def max_beam_rss(self) -> Metric:
        metric_id = 'beam_rss_max_{}'.format(self.test_config.name)
        title = 'Max. beam.smp RSS (MB), {}'.format(self._title)
        metric_info = self._metric_info(metric_id, title)

        query_params = get_query_params('max_beam.smp_rss')

        max_rss = 0
        for cluster_name, servers in self.cluster_spec.clusters:
            cluster = list(filter(lambda name: name.startswith(cluster_name),
                                  self.test.cbmonitor_clusters))[0]
            for server in servers:
                hostname = server.replace('.', '')
                db = 'atop{}{}'.format(cluster, hostname)  # Legacy
                data = self.seriesly[db].query(query_params)
                rss = round(list(data.values())[0][0] / 1024 ** 2)
                max_rss = max(max_rss, rss)

        return max_rss, self._snapshots, metric_info

    def max_memcached_rss(self) -> Metric:
        metric_id = '{}_memcached_rss'.format(self.test_config.name)
        title = 'Max. memcached RSS (MB),{}'.format(
            self._title.split(',')[-1]
        )
        metric_info = self._metric_info(metric_id, title)

        query_params = get_query_params('max_memcached_rss')

        max_rss = 0
        for (cluster_name, servers), initial_nodes in zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        ):
            cluster = list(filter(lambda name: name.startswith(cluster_name),
                                  self.test.cbmonitor_clusters))[0]
            for server in servers[:initial_nodes]:
                hostname = server.replace('.', '')
                db = 'atop{}{}'.format(cluster, hostname)
                data = self.seriesly[db].query(query_params)
                rss = round(list(data.values())[0][0] / 1024 ** 2)
                max_rss = max(max_rss, rss)

        return max_rss, self._snapshots, metric_info

    def avg_memcached_rss(self) -> Metric:
        metric_id = '{}_avg_memcached_rss'.format(self.test_config.name)
        title = 'Avg. memcached RSS (MB),{}'.format(
            self._title.split(',')[-1]
        )
        metric_info = self._metric_info(metric_id, title)

        query_params = get_query_params('avg_memcached_rss')

        rss = list()
        for (cluster_name, servers), initial_nodes in zip(
                self.cluster_spec.clusters,
                self.test_config.cluster.initial_nodes,
        ):
            cluster = list(filter(lambda name: name.startswith(cluster_name),
                                  self.test.cbmonitor_clusters))[0]
            for server in servers[:initial_nodes]:
                hostname = server.replace('.', '')
                db = 'atop{}{}'.format(cluster, hostname)
                data = self.seriesly[db].query(query_params)
                rss.append(round(list(data.values())[0][0] / 1024 ** 2))

        avg_rss = sum(rss) // len(rss)

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

    def verify_series_in_limits(self,
                                db: str,
                                expected_number: int,
                                metric: str,
                                larger_is_better: bool = False) -> bool:
        values = []
        data = self.seriesly[db].get_all()
        values += [value[metric] for value in data.values()]
        values = list(map(float, values))
        logger.info("Number of samples for {} are {}".format(metric, len(values)))
        logger.info("Sample values: {}".format(values))

        if larger_is_better and any(value < expected_number for value in values):
                return False
        else:
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

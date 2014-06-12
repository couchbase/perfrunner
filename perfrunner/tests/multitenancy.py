import time

import numpy as np
from logger import logger

from perfrunner.helpers.misc import pretty_dict
from perfrunner.settings import TargetSettings
from perfrunner.tests import PerfTest


class EmptyBucketsTest(PerfTest):

    ITERATION_DELAY = 300

    def __index__(self, *args, **kwargs):
        super(EmptyBucketsTest, self).__init__(*args, **kwargs)
        self.results = []

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info(pretty_dict(self.results))

    def create_buckets(self, buckets):
        ram_quota = self.test_config.cluster.mem_quota / len(buckets)
        replica_number = self.test_config.bucket.replica_number
        replica_index = self.test_config.bucket.replica_index
        eviction_policy = self.test_config.bucket.eviction_policy
        threads_number = self.test_config.bucket.threads_number
        password = self.test_config.bucket.password

        for bucket_name in buckets:
            self.rest.create_bucket(host_port=self.master_node,
                                    name=bucket_name,
                                    ram_quota=ram_quota,
                                    replica_number=replica_number,
                                    replica_index=replica_index,
                                    eviction_policy=eviction_policy,
                                    threads_number=threads_number,
                                    password=password)

    def report_stats(self):
        cpu = lambda data: round(np.mean(data), 1)
        rss = lambda data: int(np.mean(data) / 1024 ** 2)
        conn = lambda data: int(np.mean(data))

        summary = {}
        for hostname, s in self.rest.get_node_stats(self.master_node,
                                                    'bucket-1'):
            summary[hostname] = {
                'memcached, MBytes': rss(s['proc/memcached/mem_resident']),
                'beam.smp, MBytes': rss(s['proc/(main)beam.smp/mem_resident']),
                'Total CPU, %': cpu(s['cpu_utilization_rate']),
                'Curr. connections': conn(s['curr_connections']),
            }
        self.results.append(summary)
        logger.info(pretty_dict(summary))

    def delete_buckets(self, buckets):
        for bucket_name in buckets:
            self.rest.delete_bucket(host_port=self.master_node,
                                    name=bucket_name)

    def run(self):
        for num_buckets in range(self.test_config.cluster.min_num_buckets,
                                 self.test_config.cluster.max_num_buckets + 1,
                                 self.test_config.cluster.incr_num_buckets):
            # Buckets
            buckets = ['bucket-{}'.format(i + 1) for i in range(num_buckets)]

            # Create
            self.create_buckets(buckets)
            self.monitor.monitor_node_health(self.master_node)
            logger.info('Sleeping {} seconds'.format(self.ITERATION_DELAY))
            time.sleep(self.ITERATION_DELAY)

            # Monitor
            self.report_stats()

            # Clean up
            self.delete_buckets(buckets)
            self.monitor.monitor_node_health(self.master_node)
            logger.info('Sleeping {} seconds'.format(self.ITERATION_DELAY / 2))
            time.sleep(self.ITERATION_DELAY / 2)


class TargetIterator(object):

    def __init__(self, master_node, buckets):
        self.master_node = master_node
        self.buckets = buckets

    def __iter__(self):
        for bucket in self.buckets:
            yield TargetSettings(self.master_node, bucket, password='password',
                                 prefix=None)


class LoadTest(EmptyBucketsTest):

    def _load(self, buckets):
        load_settings = self.test_config.load_settings
        load_settings.items /= len(buckets)

        target_iterator = TargetIterator(self.master_node, buckets)
        self.worker_manager.run_workload(load_settings, target_iterator)
        self.worker_manager.wait_for_workers()

    def _access(self, buckets):
        access_settings = self.test_config.access_settings
        access_settings.items /= len(buckets)
        access_settings.throughput /= len(buckets)

        target_iterator = TargetIterator(self.master_node, buckets)
        self.worker_manager.run_workload(access_settings, target_iterator,
                                         timer=self.ITERATION_DELAY)
        self.worker_manager.wait_for_workers()

    def _wait_for_persistence(self, buckets):
        for bucket_name in buckets:
            self.monitor.monitor_disk_queue(self.master_node, bucket_name)
            self.monitor.monitor_tap_replication(self.master_node, bucket_name)

    def run(self):
        for num_buckets in range(self.test_config.cluster.min_num_buckets,
                                 self.test_config.cluster.max_num_buckets + 1,
                                 self.test_config.cluster.incr_num_buckets):
            # Buckets
            buckets = ['bucket-{}'.format(i + 1) for i in range(num_buckets)]

            # Create
            self.create_buckets(buckets)
            self.monitor.monitor_node_health(self.master_node)

            # Load and access data
            self._load(buckets)
            self._wait_for_persistence(buckets)
            self._access(buckets)

            # Monitor
            self.report_stats()

            # Clean up
            self.delete_buckets(buckets)
            self.monitor.monitor_node_health(self.master_node)
            logger.info('Sleeping {} seconds'.format(self.ITERATION_DELAY / 2))
            time.sleep(self.ITERATION_DELAY / 2)

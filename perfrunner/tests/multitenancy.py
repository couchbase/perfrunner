import time

import numpy as np
from logger import logger

from perfrunner.helpers.misc import pretty_dict
from perfrunner.tests import PerfTest


class EmptyBucketsTest(PerfTest):

    ITERATION_DELAY = 300

    def create_buckets(self, num_buckets):
        ram_quota = self.test_config.cluster.mem_quota / num_buckets
        replica_number = self.test_config.bucket.replica_number
        replica_index = self.test_config.bucket.replica_index
        eviction_policy = self.test_config.bucket.eviction_policy
        threads_number = self.test_config.bucket.threads_number
        password = self.test_config.bucket.password

        for i in range(num_buckets):
            bucket_name = 'bucket-{}'.format(i + 1)
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

        for hostname, s in self.rest.get_node_stats(self.master_node,
                                                    'bucket-1'):
            summary = {
                'memcached, MBytes': rss(s['proc/memcached/mem_resident']),
                'beam.smp, MBytes': rss(s['proc/(main)beam.smp/mem_resident']),
                'Total CPU, %': cpu(s['cpu_utilization_rate']),
                'Curr. connections': conn(s['curr_connections']),
            }
            logger.info(pretty_dict({hostname: summary}))

    def delete_buckets(self, num_buckets):
        for i in range(num_buckets):
            bucket_name = 'bucket-{}'.format(i + 1)
            self.rest.delete_bucket(host_port=self.master_node,
                                    name=bucket_name)

    def run(self):
        for num_buckets in range(self.test_config.cluster.min_num_buckets,
                                 self.test_config.cluster.max_num_buckets + 1,
                                 self.test_config.cluster.incr_num_buckets):
            # Create
            self.create_buckets(num_buckets)
            self.monitor.monitor_node_health(self.master_node)
            logger.info('Sleeping {} seconds'.format(self.ITERATION_DELAY))
            time.sleep(self.ITERATION_DELAY)

            # Monitor
            self.report_stats()

            # Clean up
            self.delete_buckets(num_buckets)
            self.monitor.monitor_node_health(self.master_node)
            logger.info('Sleeping {} seconds'.format(self.ITERATION_DELAY / 2))
            time.sleep(self.ITERATION_DELAY / 2)

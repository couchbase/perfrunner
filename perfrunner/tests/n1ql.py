import time
from exceptions import NotImplementedError

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest, TargetIterator


class N1QLTest(PerfTest):

    COLLECTORS = {
        'n1ql_latency': True,
        'n1ql_stats': True,
        'secondary_stats': True,
    }

    def __init__(self, *args, **kwargs):
        super(N1QLTest, self).__init__(*args, **kwargs)

    def build_index(self):
        for name, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            if not servers:
                raise Exception('No query servers specified for cluster \"{}\",'
                                ' cannot create indexes'.format(name))

            if not self.test_config.buckets:
                raise Exception('No buckets specified for cluster \"{}\",'
                                ' cannot create indexes'.format(name))

            query_node = servers[0].split(':')[0]
            for bucket in self.test_config.buckets:
                self._build_index(query_node, bucket)

    def _build_index(self, query_node, bucket):

        names = list()
        for index in self.test_config.n1ql_settings.indexes:

            if '{partition_id}' in index:
                for id in range(self.test_config.load_settings.doc_partitions):
                    index_name = index.split('::')[0].format(partition_id=id)
                    index_query = index.split('::')[1]
                    query = index_query.format(name=index_name, bucket=bucket,
                                               partition_id=id)
                    self.rest.n1ql_query(query_node, query)
                    names.append(index_name)
            else:
                index_name = index.split('::')[0]
                index_query = index.split('::')[1]
                query = index_query.format(name=index_name, bucket=bucket)
                self.rest.n1ql_query(query_node, query)
                names.append(index_name)

            for name in names:
                self.rest.wait_for_indexes_to_become_online(host=query_node,
                                                            index_name=name)

    def _create_prepared_statements(self):
        self.n1ql_queries = []
        prepared_stmnts = list()
        for query in self.test_config.access_settings.n1ql_queries:
            if 'prepared' in query and query['prepared']:

                if '{partition_id}' in query['prepared']:
                    for id in range(self.test_config.load_settings.doc_partitions):
                        name = query['prepared'].format(partition_id=id)
                        part_stmt = query['statement'].format(partition_id=id)
                        stmt = 'PREPARE {} AS {}'.format(name, part_stmt)
                        prepared_stmnts.append(stmt)
                else:
                    stmt = 'PREPARE {} AS {}'.format(query['prepared'],
                                                     query['statement'])
                    prepared_stmnts.append(stmt)
                del query['statement']
                query['prepared'] = '"' + query['prepared'] + '"'
            self.n1ql_queries.append(query)

        for name, servers in self.cluster_spec.yield_servers_by_role('n1ql'):
            if not servers:
                raise Exception('No query servers specified for cluster \"{}\",'
                                ' cannot create prepared statement'.format(name))

            if not self.test_config.buckets:
                raise Exception('No buckets specified for cluster \"{}\", '
                                'cannot create prepared statement'.format(name))

            for server in servers:
                query_node = server.split(':')[0]
                for stmt in prepared_stmnts:
                    self.rest.n1ql_query(query_node, stmt)

    @with_stats
    def access(self, access_settings=None):
        super(N1QLTest, self).timer()

    def run(self):
        raise NotImplementedError("N1QLTest is a base test and cannot be run")


class N1QLLatencyTest(N1QLTest):

    def __init__(self, *args, **kwargs):
        super(N1QLLatencyTest, self).__init__(*args, **kwargs)

    def run(self):
        load_settings = self.test_config.load_settings
        load_settings.items /= 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        self.load(load_settings, iterator)

        self.load(load_settings)
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

        self._create_prepared_statements()

        self.workload = self.test_config.access_settings
        self.workload.items /= 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',
                                             self.workload.n1ql_queries)

        self.access_bg(self.workload)
        self.access(self.workload)

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_query_latency(percentile=80)
            )


class N1QLThroughputTest(N1QLTest):

    def __init__(self, *args, **kwargs):
        super(N1QLThroughputTest, self).__init__(*args, **kwargs)

    def run(self):
        load_settings = self.test_config.load_settings
        load_settings.items /= 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')
        self.load(load_settings, iterator)

        self.load(load_settings)
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()
        if self.test_config.access_settings.n1ql_op != "ryow":
            self._create_prepared_statements()

        self.workload = self.test_config.access_settings
        self.workload.items /= 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',
                                             self.workload.n1ql_queries)

        self.access_bg(self.workload)
        self.access(self.workload)

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_avg_n1ql_queries()
            )


class N1QLThroughputLatencyTest(N1QLTest):

    INCREMENT = 0.20

    def __init__(self, *args, **kwargs):
        super(N1QLThroughputLatencyTest, self).__init__(*args, **kwargs)

    @with_stats
    def enable_collectors_and_start_load(self):
        self.access_bg(self.workload)
        super(N1QLThroughputLatencyTest, self).timer()

    def run(self):
        load_settings = self.test_config.load_settings
        load_settings.items /= 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')

        self.load(load_settings, iterator)

        self.load(load_settings)
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

        self._create_prepared_statements()

        self.workload = self.test_config.access_settings
        self.workload.items /= 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries',
                                             self.workload.n1ql_queries)

        self.enable_collectors_and_start_load()

        if self.test_config.stats_settings.enabled:
            orig_throughput, metric, metric_info = self.metric_helper.calc_avg_n1ql_queries()
            self.reporter.post_to_sf(orig_throughput, metric, metric_info)
            self.reporter.post_to_sf(*self.metric_helper.calc_query_latency(percentile=80))

        # run with the maximum throughput, if not specified then pick a big number
        if self.workload.n1ql_throughput_max < float('inf'):
            self.workload.n1ql_throughput = self.workload.n1ql_throughput_max
        else:
            self.workload.n1ql_throughput = 25000
        self.workload.n1ql_workers = int(self.workload.n1ql_throughput / 40)

        self.enable_collectors_and_start_load()

        if self.test_config.stats_settings.enabled:
            throughput, metric, metric_info = self.metric_helper.calc_avg_n1ql_queries()
            # modify with the metric name, de-average out the throughput
            # self.reporter.post_to_sf( throughput , 'max_throughput', metric_info)
            self.reporter.post_to_sf(2 * throughput - orig_throughput, 'max_throughput', metric_info)

    # the following is code to detect max throughput based on a ramp up and ramp down. It will be selectively enabled
    # in this class by changing the name of the parameter

    @with_stats
    def do_run_at_one_level(self):
        self.access_bg(self.workload)
        time.sleep(120)
        # super(N1QLThroughputLatencyTest, self).timer()
        # self.access(self.workload)

        self.cumulative_throughput = self.metric_helper.calc_avg_n1ql_queries()[0]
        self.cumulative_latency = self.metric_helper.calc_query_latency(percentile=80)[0]
        self.reporter.post_to_sf(*self.metric_helper.calc_avg_n1ql_queries())
        self.reporter.post_to_sf(*self.metric_helper.calc_query_latency(percentile=80))

    def run_disabled(self):
        logger.info('\n\nStarting N1QL max throughput identification test')

        load_settings = self.test_config.load_settings
        load_settings.items /= 2

        iterator = TargetIterator(self.cluster_spec, self.test_config, 'n1ql')

        self.load(load_settings, iterator)
        self.load(load_settings)
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

        self._create_prepared_statements()

        self.test_config.access_settings.time = 120
        self.workload = self.test_config.access_settings
        self.workload.items /= 2
        self.workload.n1ql_queries = getattr(self, 'n1ql_queries', self.workload.n1ql_queries)

        # set some hardcoded values
        self.workload.time = 120
        # starting point and worker ratio depends on staleness
        if self.test_config.access_settings.n1ql_queries[0]['scan_consistency'] == 'not_bounded':
            # stale is ok
            # self.workload.n1ql_throughput = 3000
            worker_divisor = 30
        else:
            # self.workload.n1ql_throughput = 300
            worker_divisor = 20

        self.workload.n1ql_throughput = self.workload.n1ql_throughput_max
        self.workload.n1ql_workers = int(self.workload.n1ql_throughput / worker_divisor)

        # do at the initial level
        original_throughput_request = self.workload.n1ql_throughput
        original_workers = self.workload.n1ql_workers
        run_data = []
        run_count = 0

        have_identified_throughput = False

        self.do_run_at_one_level()
        print '\n\nobservedThroughput', self.cumulative_throughput, 'originalThroughputRequest', original_throughput_request
        run_data.append({'requestedThroughput': original_throughput_request,
                         'observedThroughput': self.cumulative_throughput,
                         'latency': self.cumulative_latency})

        if self.cumulative_throughput < original_throughput_request * 0.95:
            logger.info('N1QL: Did not reach the original target, ramp down')
            ramp_up = False
        else:
            logger.info('N1QL: Reached the original target, ramp up')
            ramp_up = True

        running_throughput_average = self.cumulative_throughput

        while not have_identified_throughput and run_count < 10:
            run_count += 1
            if ramp_up:
                self.workload.n1ql_throughput = self.workload.n1ql_throughput + self.INCREMENT * original_throughput_request
                self.workload.n1ql_workers = int(self.workload.n1ql_workers + self.INCREMENT * original_workers)
            else:
                self.workload.n1ql_throughput = self.workload.n1ql_throughput - self.INCREMENT * original_throughput_request
                self.workload.n1ql_workers = int(self.workload.n1ql_workers - self.INCREMENT * original_workers)

            self.do_run_at_one_level()
            observed_throughput = (run_count + 1) * self.cumulative_throughput - running_throughput_average
            running_throughput_average += observed_throughput
            run_data.append({'requestedThroughput': self.workload.n1ql_throughput,
                             'observedThroughput': observed_throughput,
                             'latency': self.cumulative_latency})

            if ramp_up:
                if observed_throughput > self.workload.n1ql_throughput * 0.95:
                    logger.info('N1QL: {0} Met the target, ramp up further'.format(observed_throughput))
                else:
                    logger.info('N1QL: While ramping up, did not reach the desired throughput, requested {0}, actual {1}'.format(self.workload.n1ql_throughput, observed_throughput))
                    have_identified_throughput = True  # hit the max
            else:
                if observed_throughput < self.workload.n1ql_throughput * 0.95:
                    logger.info('N1QL: Did not meet the target {0}, ramp down further'.format(observed_throughput))
                else:
                    logger.info('N1QL: Ramped down to a throughput that was achieved, requested {0}, actual {1}'.format(self.workload.n1ql_throughput, observed_throughput))
                    have_identified_throughput = True  # hit the bottom

        logger.info('N1QL Results Summary:')
        for i in run_data:
            logger.info('Requested throughput {0}, observed throughput {1}, latency {2}'.format(i['requestedThroughput'], i['observedThroughput'], i['latency']))

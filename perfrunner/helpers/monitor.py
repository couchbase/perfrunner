import time

from logger import logger
from perfrunner.helpers import misc
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import (
    DefaultRestHelper,
    KubernetesRestHelper,
    ServerlessRestHelper,
)
from perfrunner.settings import ClusterSpec, TestConfig


class Monitor:

    def __new__(cls,
                cluster_spec: ClusterSpec,
                test_config: TestConfig,
                verbose: bool = False):
        if cluster_spec.dynamic_infrastructure:
            return KubernetesMonitor(cluster_spec, test_config, verbose)
        elif cluster_spec.serverless_infrastructure:
            return ServerlessMonitor(cluster_spec, test_config, verbose)
        else:
            return DefaultMonitor(cluster_spec, test_config, verbose)


class DefaultMonitor(DefaultRestHelper):

    MAX_RETRY = 150
    MAX_RETRY_RECOVERY = 1200
    MAX_RETRY_TIMER_EVENT = 18000
    MAX_RETRY_BOOTSTRAP = 1200

    MONITORING_DELAY = 5

    POLLING_INTERVAL = 1
    POLLING_INTERVAL_INDEXING = 1
    POLLING_INTERVAL_MACHINE_UP = 10
    POLLING_INTERVAL_ANALYTICS = 15
    POLLING_INTERVAL_EVENTING = 1
    POLLING_INTERVAL_FRAGMENTATION = 10
    POLLING_INTERVAL_SGW = 1

    REBALANCE_TIMEOUT = 3600 * 6
    TIMEOUT = 3600 * 12

    DISK_QUEUES = (
        'ep_queue_size',
        'ep_flusher_todo',
        'ep_diskqueue_items',
        'vb_active_queue_size',
        'vb_replica_queue_size',
    )

    DCP_QUEUES = (
        'ep_dcp_replica_items_remaining',
        'ep_dcp_other_items_remaining',
    )

    XDCR_QUEUES = (
        'replication_changes_left',
    )

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = RemoteHelper(cluster_spec, verbose)
        self.master_node = next(cluster_spec.masters)
        self.build = self.get_version(self.master_node)
        version, build_number = self.build.split('-')
        self.build_version_number = tuple(map(int, version.split('.'))) + (int(build_number),)

    def wait_for_rebalance_to_begin(self, host):
        logger.info('Waiting for rebalance to start')

        is_running = False
        start_time = time.time()
        while not is_running:
            is_running, progress = self.get_task_status(host, task_type='rebalance')
            logger.info('Rebalance running: {}'.format(is_running))
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Monitoring got stuck')
            time.sleep(self.POLLING_INTERVAL)

        logger.info('Rebalance started. Rebalance progress: {} %'.format(progress))

    def monitor_rebalance(self, host):
        logger.info('Monitoring rebalance status')

        is_running = True
        last_progress = 0
        last_progress_time = time.time()
        while is_running:
            time.sleep(self.POLLING_INTERVAL)

            is_running, progress = self.get_task_status(host,
                                                        task_type='rebalance')
            if progress == last_progress:
                if time.time() - last_progress_time > self.REBALANCE_TIMEOUT:
                    logger.error('Rebalance hung')
                    break
            else:
                last_progress = progress
                last_progress_time = time.time()

            if progress is not None:
                logger.info('Rebalance progress: {} %'.format(progress))

        logger.info('Rebalance completed')

    def _wait_for_empty_queues(self, host, bucket, queues, stats_function):
        metrics = list(queues)

        start_time = time.time()
        while metrics:
            bucket_stats = stats_function(host, bucket)
            # As we are changing metrics in the loop; take a copy of it to
            # iterate over.
            for metric in list(metrics):
                stats = bucket_stats['op']['samples'].get(metric)
                if stats:
                    last_value = stats[-1]
                    if last_value:
                        logger.info('{} = {:,}'.format(metric, last_value))
                        continue
                    else:
                        logger.info('{} reached 0'.format(metric))
                    metrics.remove(metric)
                else:
                    logger.info('{} reached 0'.format(metric))
                    metrics.remove(metric)
            if metrics:
                time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Monitoring got stuck')

    def _wait_for_empty_dcp_queues(self, host, bucket, stats_function):
        start_time = time.time()
        while True:
            kv_dcp_stats = stats_function(host, bucket)
            stats = int(kv_dcp_stats["data"][0]["values"][-1][1])
            if stats:
                logger.info('{} = {}'.format('ep_dcp_replica_items_remaining', stats))
                if time.time() - start_time > self.TIMEOUT:
                    raise Exception('Monitoring got stuck')
                time.sleep(self.POLLING_INTERVAL)
            else:
                logger.info('{} reached 0'.format('ep_dcp_replica_items_remaining'))
                break

    def _wait_for_replica_count_match(self, host, bucket):
        start_time = time.time()
        bucket_info = self.get_bucket_info(host, bucket)
        replica_number = int(bucket_info['replicaNumber'])
        while replica_number:
            bucket_stats = self.get_bucket_stats(host, bucket)
            curr_items = bucket_stats['op']['samples'].get("curr_items")[-1]
            replica_curr_items = bucket_stats['op']['samples'].get("vb_replica_curr_items")[-1]
            logger.info("curr_items: {}, replica_curr_items: {}".format(curr_items,
                                                                        replica_curr_items))
            if (curr_items * replica_number) == replica_curr_items:
                break
            time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Replica items monitoring got stuck')

    def _wait_for_replication_completion(self, host, bucket, queues, stats_function, link1, link2):
        metrics = list(queues)

        completion_count = 0
        link1_time = 0
        link2_items = 0

        link1_compelteness_str = \
            'replications/{}/bucket-1/bucket-1/percent_completeness'.format(link1)
        link2_compelteness_str = \
            'replications/{}/bucket-1/bucket-1/percent_completeness'.format(link2)
        link2_items_str = \
            'replications/{}/bucket-1/bucket-1/docs_written'.format(link2)

        start_time = time.time()

        while metrics:
            bucket_stats = stats_function(host, bucket)
            # As we are changing metrics in the loop; take a copy of it to
            # iterate over.
            for metric in list(metrics):
                stats = bucket_stats['op']['samples'].get(metric)
                if stats:
                    last_value = stats[-1]
                    if last_value:
                        logger.info('{} = {:,}'.format(metric, last_value))
                        link1_completeness = \
                            bucket_stats['op']['samples'].get(link1_compelteness_str)[-1]
                        link2_completeness = \
                            bucket_stats['op']['samples'].get(link2_compelteness_str)[-1]
                        if link1_completeness == 100 or \
                                link2_completeness == 100:
                            if link1_completeness == 100:
                                if completion_count == 0:
                                    link1_time = time.time()
                                    link2_items = \
                                        bucket_stats['op']['samples'].get(link2_items_str)[-1]
                                    completion_count = completion_count + 1
                            elif link2_completeness == 100:
                                if completion_count == 0:
                                    link1_time = time.time()
                                    link2_items = \
                                        bucket_stats['op']['samples'].get(link2_items_str)[-1]
                                    completion_count = completion_count + 1
                        continue
                    else:
                        logger.info('{} reached 0'.format(metric))
                        if completion_count == 0:
                            link1_time = time.time()
                            link2_items = \
                                bucket_stats['op']['samples'].get(link2_items_str)[-1]
                            completion_count = completion_count + 1
                    metrics.remove(metric)
            if metrics:
                time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Monitoring got stuck')

        return link1_time, link2_items

    def _wait_for_completeness(self, host, bucket, xdcr_link, stats_function):
        metrics = []
        metrics.append(xdcr_link)

        start_time = time.time()

        while metrics:
            bucket_stats = stats_function(host, bucket)

            for metric in metrics:
                stats = bucket_stats['op']['samples'].get(metric)
                if stats:
                    last_value = stats[0]
                    if last_value != 100:
                        logger.info('{} : {}'.format(metric, last_value))
                    elif last_value == 100:
                        logger.info('{} Completed 100 %'.format(metric))
                        metrics.remove(metric)
            if metrics:
                time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Monitoring got stuck')

    def monitor_disk_queues(self, host, bucket):
        logger.info('Monitoring disk queues: {}'.format(bucket))
        self._wait_for_empty_queues(host, bucket, self.DISK_QUEUES,
                                    self.get_bucket_stats)

    def monitor_dcp_queues(self, host, bucket):
        logger.info('Monitoring DCP queues: {}'.format(bucket))
        if self.build_version_number < (1, 0, 0, 0):
            if self.test_config.bucket.replica_number != 0:
                if self.build_version_number < (0, 0, 0, 2106):
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.get_dcp_replication_items)
                else:
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.get_dcp_replication_items_v2)
            self.DCP_QUEUES = (
                'ep_dcp_other_items_remaining',
            )
            self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                        self.get_bucket_stats)
        elif self.build_version_number < (7, 0, 0, 3937):
            self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                        self.get_bucket_stats)
        else:
            if self.test_config.bucket.replica_number != 0:
                if self.build_version_number < (7, 0, 0, 4990):
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.get_dcp_replication_items)
                else:
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.get_dcp_replication_items_v2)
            self.DCP_QUEUES = (
                'ep_dcp_other_items_remaining',
            )
            self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                        self.get_bucket_stats)

    def monitor_replica_count(self, host, bucket):
        logger.info('Monitoring replica count match: {}'.format(bucket))
        self._wait_for_replica_count_match(host, bucket)

    def _wait_for_xdcr_to_start(self, host: str):
        is_running = False
        while not is_running:
            time.sleep(self.POLLING_INTERVAL)
            is_running, _ = self.get_task_status(host, task_type='xdcr')

    def xdcr_link_starttime(self, host: str, uuid: str):
        is_running = False
        while not is_running:
            time.sleep(self.POLLING_INTERVAL)
            is_running, _ = self.get_xdcrlink_status(host, task_type='xdcr', uuid=uuid)
        return time.time()

    def monitor_xdcr_queues(self, host: str, bucket: str):
        logger.info('Monitoring XDCR queues: {}'.format(bucket))
        self._wait_for_xdcr_to_start(host)
        # adding temporary delay to make sure replication_changes_left stats arrives
        time.sleep(20)
        self._wait_for_empty_queues(host, bucket, self.XDCR_QUEUES,
                                    self.get_xdcr_stats)

    def monitor_xdcr_changes_left(self, host: str, bucket: str, xdcrlink1: str, xdcrlink2: str):
        logger.info('Monitoring XDCR queues: {}'.format(bucket))
        self._wait_for_xdcr_to_start(host)
        start_time = time.time()
        link1_time, link2_items = self._wait_for_replication_completion(host, bucket,
                                                                        self.XDCR_QUEUES,
                                                                        self.get_xdcr_stats,
                                                                        xdcrlink1,
                                                                        xdcrlink2)
        return start_time, link1_time, link2_items

    def monitor_xdcr_completeness(self, host: str, bucket: str, xdcr_link: str):
        logger.info('Monitoring XDCR Link Completeness: {}'.format(bucket))
        self._wait_for_completeness(host=host, bucket=bucket, xdcr_link=xdcr_link,
                                    stats_function=self.get_xdcr_stats)
        return time.time()

    def get_num_items(self, host: str, bucket: str):
        num_items = self._get_num_items(host, bucket, total=True)
        return num_items

    def _get_num_items(self, host: str, bucket: str, total: bool = False) -> int:
        stats = self.get_bucket_stats(host=host, bucket=bucket)
        if total:
            curr_items = stats['op']['samples'].get('curr_items_tot')
        else:
            curr_items = stats['op']['samples'].get('curr_items')
        if curr_items:
            return self._ignore_system_collection_items(host=host,
                                                        bucket=bucket, curr_items=curr_items[-1])
        return 0

    def _get_num_items_scope_and_collection(self, host: str, bucket: str, scope: str,
                                            coll: str = None) -> int:
        stats = self.get_scope_and_collection_items(host, bucket, scope, coll)
        if stats:
            return sum(int(d['values'][-1][1]) for d in stats['data'])
        return 0

    def _ignore_system_collection_items(self, host: str, bucket: str, curr_items: int) -> int:
        sys_coll_items = self._get_num_items_scope_and_collection(host, bucket, '_system')
        replica_sys_coll_items = sys_coll_items * (1 + self.test_config.bucket.replica_number)
        logger.info('Ignoring items in _system collection: {} (active), {} (total)'
                    .format(sys_coll_items, replica_sys_coll_items))
        return curr_items - replica_sys_coll_items

    def monitor_num_items(self, host: str, bucket: str, num_items: int, max_retry: int = None):
        logger.info('Checking the number of items in {}'.format(bucket))

        if not max_retry:
            max_retry = self.MAX_RETRY

        retries = 0
        while retries < max_retry:
            curr_items = self._get_num_items(host, bucket, total=True)
            if curr_items == num_items:
                break
            else:
                logger.info('{}(curr_items) != {}(num_items)'.format(curr_items, num_items))

            time.sleep(self.POLLING_INTERVAL)
            retries += 1
        else:
            actual_items = self._get_num_items(host, bucket, total=True)
            raise Exception('Mismatch in the number of items: {}'
                            .format(actual_items))

    def monitor_num_backfill_items(self, host: str, bucket: str, num_items: int):
        logger.info('Checking the number of items in {}'.format(bucket))

        t0 = time.time()
        while True:
            curr_items = self._get_num_items(host, bucket, total=True)

            if curr_items == num_items:
                t1 = time.time()
                break
            else:
                logger.info('{}(curr_items) != {}(num_items)'.format(curr_items, num_items))

            time.sleep(self.POLLING_INTERVAL)
        return t1-t0

    def monitor_task(self, host, task_type):
        logger.info('Monitoring task: {}'.format(task_type))
        time.sleep(self.MONITORING_DELAY * 2)

        while True:
            time.sleep(self.POLLING_INTERVAL)

            tasks = [task for task in self.get_tasks(host)
                     if task.get('type') == task_type]
            if tasks:
                for task in tasks:
                    logger.info('{}: {}%, bucket: {}, ddoc: {}'.format(
                        task_type, task.get('progress'),
                        task.get('bucket'), task.get('designDocument')
                    ))
            else:
                break
        logger.info('Task {} successfully completed'.format(task_type))

    def monitor_warmup(self, memcached, host, bucket):
        logger.info('Monitoring warmup status: {}@{}'.format(bucket,
                                                             host))

        memcached_port = self.get_memcached_port(host)

        while True:
            stats = memcached.get_stats(host, memcached_port, bucket, 'warmup')
            if 'ep_warmup_state' in stats:
                state = stats['ep_warmup_state']
                if state == 'done':
                    return float(stats.get('ep_warmup_time', 0))
                else:
                    logger.info('Warmpup status: {}'.format(state))
                    time.sleep(self.POLLING_INTERVAL)
            else:
                logger.info('No warmup stats are available, continue polling')
                time.sleep(self.POLLING_INTERVAL)

    def monitor_compression(self, memcached, host, bucket):
        logger.info('Monitoring active compression status')

        memcached_port = self.get_memcached_port(host)

        json_docs = -1
        while json_docs:
            stats = memcached.get_stats(host, memcached_port, bucket)
            json_docs = int(stats['ep_active_datatype_json'])
            if json_docs:
                logger.info('Still uncompressed: {:,} items'.format(json_docs))
                time.sleep(self.POLLING_INTERVAL)
        logger.info('All items are compressed')

    def monitor_node_health(self, host):
        logger.info('Monitoring node health')

        for retry in range(self.MAX_RETRY):
            unhealthy_nodes = {
                n for n, status in self.node_statuses(host).items()
                if status != 'healthy'
            } | {
                n for n, status in self.node_statuses_v2(host).items()
                if status != 'healthy'
            }
            if unhealthy_nodes:
                time.sleep(self.POLLING_INTERVAL)
            else:
                break
        else:
            logger.interrupt('Some nodes are not healthy: {}'.format(
                unhealthy_nodes
            ))

    def monitor_analytics_node_active(self, host):
        logger.info('Monitoring analytics node health')

        for retry in range(self.MAX_RETRY):
            active = self.analytics_node_active(host)
            if active:
                break
            else:
                time.sleep(self.POLLING_INTERVAL)
        else:
            logger.interrupt('Analytics node still not healthy: {}'.format(
                host
            ))

    def is_index_ready(self, host: str) -> bool:
        for status in self.get_index_status(host)['status']:
            if status['status'] != 'Ready':
                return False
        return True

    def estimate_pending_docs(self, host: str) -> int:
        stats = self.get_gsi_stats(host)
        pending_docs = 0
        for metric, value in stats.items():
            if 'num_docs_queued' in metric or 'num_docs_pending' in metric:
                pending_docs += value
        return pending_docs

    def monitor_indexing(self, host):
        logger.info('Monitoring indexing progress')

        while not self.is_index_ready(host):
            time.sleep(self.POLLING_INTERVAL_INDEXING * 5)
            pending_docs = self.estimate_pending_docs(host)
            logger.info('Pending docs: {:,}'.format(pending_docs))

        logger.info('Indexing completed')

    def wait_for_all_indexes_dropped(self, index_nodes):
        logger.info('Waiting for all indexes to be dropped')
        indexes_remaining = [1 for _ in index_nodes]

        def update_indexes_remaining():
            for i, node in enumerate(index_nodes):
                indexes_remaining[i] = self.indexes_per_node(node)

        while (sum(indexes_remaining) != 0):
            time.sleep(self.POLLING_INTERVAL_INDEXING)
            update_indexes_remaining()

    def wait_for_secindex_init_build(self, host, indexes):
        # POLL until initial index build is complete
        logger.info(
            "Waiting for the following indexes to be ready: {}".format(indexes))

        indexes_ready = [0 for _ in indexes]

        def get_index_status(json2i, index):
            """Return the index status."""
            for d in json2i["status"]:
                if d["name"] == index:
                    return d["status"]
            return None

        @misc.retry(catch=(KeyError,), iterations=10, wait=30)
        def update_indexes_ready():
            json2i = self.get_index_status(host)
            for i, index in enumerate(indexes):
                status = get_index_status(json2i, index)
                if status == 'Ready':
                    indexes_ready[i] = 1

        init_ts = time.time()
        while sum(indexes_ready) != len(indexes):
            time.sleep(self.POLLING_INTERVAL_INDEXING)
            update_indexes_ready()
        finish_ts = time.time()
        logger.info('secondary index build time: {}'.format(finish_ts - init_ts))
        time_elapsed = round(finish_ts - init_ts)
        return time_elapsed

    def wait_for_secindex_init_build_collections(self, host,
                                                 indexes, recovery=False, created=False):
        # POLL until initial index build is complete
        if created:
            check_for_status = 'Created'
        else:
            check_for_status = 'Ready'

        index_list = []
        for bucket_name, scope_map in indexes.items():
            for scope_name, collection_map in scope_map.items():
                for collection_name, index_map in collection_map.items():
                    for index, index_config in index_map.items():
                        if type(index_config) is dict:
                            num_replica = index_config["num_replica"]
                            index_list.append(index)
                            for i in range(1, num_replica+1):
                                index_list.append("{index_name} (replica {number})"
                                                  .format(index_name=index, number=i))
                        else:
                            for index_name in index_map.keys():
                                index_list.append(index_name)

        indexes = index_list
        logger.info(
            "Waiting for the following indexes to be ready: {}".format(indexes))

        indexes_ready = [0 for _ in indexes]

        def get_index_status(json2i, index):
            """Return the index status."""
            for d in json2i["status"]:
                if d["name"] == index:
                    return d["status"]
            return None

        @misc.retry(catch=(KeyError,), iterations=10, wait=30)
        def update_indexes_ready():
            json2i = self.get_index_status(host)
            for i, index in enumerate(indexes):
                status = get_index_status(json2i, index)
                if status == check_for_status:
                    indexes_ready[i] = 1

        if recovery:
            polling_interval = self.POLLING_INTERVAL
        else:
            polling_interval = self.POLLING_INTERVAL_INDEXING*10

        while sum(indexes_ready) != len(indexes):
            time.sleep(polling_interval)
            update_indexes_ready()
        logger.info('secondary index build complete: {}'.format(indexes))

    def wait_for_secindex_incr_build(self, index_nodes, bucket, indexes, numitems):
        # POLL until incremental index build is complete
        logger.info('expecting {} num_docs_indexed for indexes {}'.format(numitems, indexes))

        # collect num_docs_indexed information globally from all index nodes
        def get_num_docs_indexed():
            data = self.get_index_stats(index_nodes)
            num_indexed = []
            for index in indexes:
                key = "" + bucket + ":" + index + ":num_docs_indexed"
                val = data[key]
                num_indexed.append(val)
            return num_indexed

        def get_num_docs_index_pending():
            data = self.get_index_stats(index_nodes)
            num_pending = []
            for index in indexes:
                key = "" + bucket + ":" + index + ":num_docs_pending"
                val1 = data[key]
                key = "" + bucket + ":" + index + ":num_docs_queued"
                val2 = data[key]
                val = int(val1) + int(val2)
                num_pending.append(val)
            return num_pending

        expected_num_pending = [0] * len(indexes)
        while True:
            time.sleep(self.POLLING_INTERVAL_INDEXING)
            curr_num_pending = get_num_docs_index_pending()
            if curr_num_pending == expected_num_pending:
                break
        curr_num_indexed = get_num_docs_indexed()
        logger.info("Number of Items indexed {}".format(curr_num_indexed))

    def wait_for_secindex_incr_build_collections(self, index_nodes, index_map, expected_num_docs):
        indexes = []
        for bucket_name, scope_map in index_map.items():
            for scope_name, collection_map in scope_map.items():
                for collection_name, coll_index_map in collection_map.items():
                    for index_name, index_def in coll_index_map.items():
                        if scope_name == '_default' \
                                and collection_name == '_default':
                            target_index = "{}:{}".format(
                                bucket_name,
                                index_name)
                        else:
                            target_index = "{}:{}:{}:{}".format(
                                bucket_name,
                                scope_name,
                                collection_name,
                                index_name)
                        indexes.append(target_index)
        logger.info('expecting {} num_docs_indexed for indexes {}'
                    .format(expected_num_docs, indexes))

        # collect num_docs_indexed information globally from all index nodes
        def get_num_docs_indexed():
            data = self.get_index_stats(index_nodes)
            num_indexed = []
            for index in indexes:
                key = index + ":num_docs_indexed"
                val = data[key]
                num_indexed.append(val)
            return num_indexed

        def get_num_docs_index_pending():
            data = self.get_index_stats(index_nodes)
            num_pending = []
            for index in indexes:
                key = index + ":num_docs_pending"
                val1 = data[key]
                key = index + ":num_docs_queued"
                val2 = data[key]
                val = int(val1) + int(val2)
                num_pending.append(val)
            return num_pending

        expected_num_pending = [0] * len(indexes)
        while True:
            time.sleep(self.POLLING_INTERVAL_INDEXING * 10)
            curr_num_pending = get_num_docs_index_pending()
            if curr_num_pending == expected_num_pending:
                break

        curr_num_indexed = get_num_docs_indexed()
        logger.info("Number of Items indexed {}".format(curr_num_indexed))

    def wait_for_num_connections(self, index_node, expected_connections):
        curr_connections = self.get_index_num_connections(index_node)
        retry = 1
        while curr_connections < expected_connections and retry < self.MAX_RETRY:
            time.sleep(self.POLLING_INTERVAL_INDEXING)
            curr_connections = self.get_index_num_connections(index_node)
            logger.info("Got current connections {}".format(curr_connections))
            retry += 1
        if retry == self.MAX_RETRY:
            return False
        return True

    def wait_for_recovery(self, index_nodes, bucket, index):
        time.sleep(self.MONITORING_DELAY)
        for retry in range(self.MAX_RETRY_RECOVERY):
            response = self.get_index_stats(index_nodes)
            item = "{}:{}:disk_load_duration".format(bucket, index)
            if item in response:
                return response[item]
            else:
                time.sleep(self.POLLING_INTERVAL)
        return -1

    def wait_for_servers(self):
        for retry in range(self.MAX_RETRY):
            logger.info('Waiting for all servers to be available')
            time.sleep(self.POLLING_INTERVAL_MACHINE_UP)

            for server in self.cluster_spec.servers:
                if not self.remote.is_up(server):
                    break
            else:
                logger.info('All nodes are up')
                return

        logger.interrupt('Some nodes are still down')

    def monitor_fts_indexing_queue(self, host: str, index: str, items: int, bucket="bucket-1"):
        logger.info('{} : Waiting for indexing to finish'.format(index))
        count = 0
        while count < items:
            count = self.get_fts_doc_count(host, index, bucket)
            logger.info('FTS indexed documents for {}: {:,}'.format(index, count))
            time.sleep(self.POLLING_INTERVAL)

    def monitor_fts_index_persistence(self, hosts: list, index: str, bkt: str = None):
        logger.info('{}: Waiting for index to be persisted'.format(index))
        if not bkt:
            bkt = self.test_config.buckets[0]
        tries = 0
        pending_items = 1
        while pending_items:
            try:
                persist = 0
                compact = 0
                for host in hosts:
                    stats = self.get_fts_stats(host)
                    metric = '{}:{}:{}'.format(bkt, index, 'num_recs_to_persist')
                    persist += stats[metric]

                    metric = '{}:{}:{}'.format(bkt, index, 'total_compactions')
                    compact += stats[metric]
                pending_items = persist or compact
                logger.info('Records to persist: {:,}'.format(persist))
                logger.info('Ongoing compactions: {:,}'.format(compact))
            except KeyError:
                tries += 1
            if tries >= 10:
                raise Exception("cannot get fts stats")
            time.sleep(self.POLLING_INTERVAL)

    def monitor_elastic_indexing_queue(self, host: str, index: str):
        logger.info(' Waiting for indexing to finish')
        items = int(self.test_config.fts_settings.test_total_docs)
        count = 0
        while count < items:
            count = self.get_elastic_doc_count(host, index)
            logger.info('Elasticsearch indexed documents: {:,}'.format(count))
            time.sleep(self.POLLING_INTERVAL)

    def monitor_elastic_index_persistence(self, host: str, index: str):
        logger.info('Waiting for index to be persisted')

        pending_items = -1
        while pending_items:
            stats = self.get_elastic_stats(host)
            pending_items = stats['indices'][index]['total']['translog']['operations']
            logger.info('Records to persist: {:,}'.format(pending_items))
            time.sleep(self.POLLING_INTERVAL)

    def wait_for_bootstrap(self, nodes: list, function: str):
        logger.info('Waiting for bootstrap of eventing function: {} '.format(function))
        for node in nodes:
            retry = 1
            while retry < self.MAX_RETRY_BOOTSTRAP:
                if function in self.get_apps_with_status(node, "deployed"):
                    break
                time.sleep(self.POLLING_INTERVAL)
                retry += 1
            if retry == self.MAX_RETRY_BOOTSTRAP:
                logger.info('Failed to bootstrap function: {}, node: {}'.
                            format(function, node))

    def get_num_analytics_items(self, analytics_node: str, bucket: str) -> int:
        stats_key = '{}:all:incoming_records_count_total'.format(bucket)
        num_items = 0
        for node in self.get_active_nodes_by_role(analytics_node, 'cbas'):
            stats = self.get_analytics_stats(node)
            num_items += stats.get(stats_key, 0)
        return num_items

    def get_ingestion_progress(self, analytics_node: str) -> int:
        stats = self.get_ingestion_v2(analytics_node)
        progress = 0
        number_dataset = 0
        for scope_state in stats["links"][0]["state"]:
            progress += scope_state["progress"] * len(scope_state["scopes"][0]['collections'])
            number_dataset += len(scope_state["scopes"][0]['collections'])
        avg_progress = progress/number_dataset*100
        return avg_progress

    def get_num_remaining_mutations(self, analytics_node: str) -> int:
        while True:
            num_items = 0
            try:
                if self.build_version_number < (7, 0, 0, 4622):
                    stats = self.get_pending_mutations(analytics_node)
                    for dataset in stats['Default']:
                        if self.build_version_number < (7, 0, 0, 4310):
                            num_items += int(stats['Default'][dataset])
                        else:
                            num_items += int(stats['Default'][dataset]['seqnoLag'])
                else:
                    stats = self.get_pending_mutations_v2(analytics_node)
                    for scope in stats['scopes']:
                        for collection in scope['collections']:
                            num_items += int(collection['seqnoLag'])
                break
            except Exception:
                time.sleep(self.POLLING_INTERVAL_ANALYTICS)
        return num_items

    def monitor_data_synced(self, data_node: str, bucket: str, analytics_node: str) -> int:
        logger.info('Waiting for data to be synced from {}'.format(data_node))
        time.sleep(self.MONITORING_DELAY * 3)

        num_items = self._get_num_items(data_node, bucket)
        while True:
            if self.build_version_number < (7, 0, 0, 0):
                num_analytics_items = self.get_num_analytics_items(analytics_node, bucket)
            else:
                if self.build_version_number < (7, 0, 0, 4990):
                    incoming_records = self.get_cbas_incoming_records_count(analytics_node)
                else:
                    incoming_records = self.get_cbas_incoming_records_count_v2(analytics_node)
                num_analytics_items = int(incoming_records["data"][0]["values"][-1][1])

            logger.info('Analytics has {:,} docs (target is {:,})'.format(
                num_analytics_items, num_items))

            if self.build_version_number < (6, 5, 0, 0):
                if num_analytics_items == num_items:
                    break
            else:
                if self.build_version_number > (7, 0, 0, 4853):
                    ingestion_progress = self.get_ingestion_progress(analytics_node)
                    logger.info('Ingestion progress: {:.2f}%'.format(ingestion_progress))
                    if int(ingestion_progress) == 100:
                        break
                else:
                    num_remaining_mutations = self.get_num_remaining_mutations(analytics_node)
                    logger.info('Number of remaining mutations: {}'.format(num_remaining_mutations))
                    if num_remaining_mutations == 0:
                        break

            time.sleep(self.POLLING_INTERVAL_ANALYTICS)

        return num_items

    def monitor_cbas_pending_ops(self, analytics_nodes):
        logger.info('Wait until cbas_pending_ops reaches 0')
        cbas_pending_ops_list = ['cbas_pending_flush_ops',
                                 'cbas_pending_merge_ops',
                                 'cbas_pending_replicate_ops']

        while True:
            pending_ops = 0
            for analytics_node in analytics_nodes:
                api = 'http://{}:8095/_prometheusMetrics'.format(analytics_node)
                api_return = self.get(url=api)
                for line in api_return.text.splitlines():
                    if "#" not in line:
                        metric_line = line.split()
                        metric = metric_line[0]
                        if metric in cbas_pending_ops_list:
                            pending_ops += int(float(metric_line[1]))
            logger.info("cbas pending ops= {}".format(pending_ops))
            if pending_ops == 0:
                break

            time.sleep(self.POLLING_INTERVAL_ANALYTICS)

    def monitor_dataset_drop(self, analytics_node: str, dataset: str):
        while True:
            statement = "SELECT COUNT(*) from `{}`;".format(dataset)
            result = self.exec_analytics_query(analytics_node, statement)
            num_analytics_items = result['results'][0]['$1']
            logger.info("Number of items in dataset {}: {}".
                        format(dataset, num_analytics_items))

            if num_analytics_items == 0:
                break

            time.sleep(self.POLLING_INTERVAL)

    def wait_for_timer_event(self, node: str, function: str, event="timer_events"):
        logger.info('Waiting for timer events to start processing: {} '.format(function))
        retry = 1
        while retry < self.MAX_RETRY_TIMER_EVENT:
            if 0 < self.get_num_events_processed(
                    event=event, node=node, name=function):
                break
            time.sleep(self.POLLING_INTERVAL_EVENTING)
            retry += 1
        if retry == self.MAX_RETRY_TIMER_EVENT:
            logger.info('Failed to get timer event for function: {}'.format(function))

    def wait_for_all_mutations_processed(self, host: str, bucket1: str, bucket2: str):
        logger.info('Waiting for mutations to be processed of eventing function')
        retry = 1
        while retry < self.MAX_RETRY_BOOTSTRAP:
            if self._get_num_items(host=host, bucket=bucket1) == \
                    self._get_num_items(host=host, bucket=bucket2):
                break
            retry += 1
            time.sleep(self.POLLING_INTERVAL_EVENTING)
        if retry == self.MAX_RETRY_BOOTSTRAP:
            logger.info('Failed to process all mutations... TIMEOUT')

    def wait_for_all_timer_creation(self, node: str, function: str):
        logger.info('Waiting for all timers to be created by : {} '.format(function))
        retry = 1
        events_processed = {}
        while retry < self.MAX_RETRY_TIMER_EVENT:
            events_processed = self.get_num_events_processed(event="ALL",
                                                             node=node, name=function)
            if events_processed["dcp_mutation"] == events_processed["timer_responses_received"]:
                break
            time.sleep(self.POLLING_INTERVAL_EVENTING)
            retry += 1
        if retry == self.MAX_RETRY_TIMER_EVENT:
            logger.info('Got only {} timers created for function: {}'.format(
                events_processed["timer_responses_received"], function))

    def wait_for_function_status(self, node: str, function: str, status: str):
        logger.info('Waiting for {} function to {}'.format(function, status))
        retry = 1
        while retry < self.MAX_RETRY_TIMER_EVENT:
            op = self.get_apps_with_status(node, status)
            if function in op:
                break
            time.sleep(self.POLLING_INTERVAL_EVENTING)
            retry += 1
        if retry == self.MAX_RETRY_TIMER_EVENT:
            logger.info('Function {} failed to {}...!!!'.format(function, status))

    def wait_for_fragmentation_stable(self, host: str, bucket: str,
                                      target_fragmentation: int = 50):
        while True:
            stats = self.get_bucket_stats(host=host, bucket=bucket)
            fragmentation = int(stats['op']['samples'].get("couch_docs_fragmentation")[-1])
            logger.info("couch_docs_fragmentation: {}".format(fragmentation))
            if fragmentation <= target_fragmentation:
                break
            time.sleep(self.POLLING_INTERVAL)

    def monitor_sgimport_queues(self, host: str, expected_docs: int):
        logger.info('Monitoring SGImport items:')
        initial_items, start_time = self._wait_for_sg_import_start(host)
        items_in_range = expected_docs - initial_items
        time_taken = self._wait_for_sg_import_complete(host, expected_docs, start_time)
        return time_taken, items_in_range

    def get_import_count(self, host: str):
        stats = self.get_sg_stats(host=host)
        import_count = 0
        if not self.cluster_spec.capella_infrastructure:
            if 'syncGateway_import' in stats.keys():
                import_count = int(stats['syncGateway_import']['import_count'])
            else:
                for count in range(1, self.test_config.cluster.num_buckets + 1):
                    db = 'db-{}'.format(count)
                    if 'shared_bucket_import' in (db_stats :=
                                                  stats['syncgateway']['per_db'][db]):
                        import_count += int(db_stats['shared_bucket_import']['import_count'])
        else:
            stat = stats.find("sgw_shared_bucket_import_import_count")
            stat_list = []
            while stat != -1:
                stat_list.append(stat)
                stat = stats.find("sgw_shared_bucket_import_import_count", stat + 1)
            last = stats.find("# HELP", stat_list[-1] + 1)
            stat_list.append(last)
            for i in range(2, len(stat_list) - 1):
                str = stats[stat_list[i]:stat_list[i+1]]
                a = str.find("}")
                import_count += int(float(str[a+2:]))
            import_count /= 2
        return import_count

    def _wait_for_sg_import_start(self, host: str):
        logger.info('Checking if import process started')

        import_docs = 0
        while True:
            time.sleep(self.POLLING_INTERVAL)
            import_docs = self.get_import_count(host)
            if import_docs >= 1:
                logger.info('importing docs has started')
                return import_docs, time.time()

    def _wait_for_sg_import_complete(self, host: str, expected_docs: int, start_time):
        expected_docs = expected_docs
        start_time = start_time
        logger.info('Monitoring syncgateway import status :')

        imports = 0

        while True:
            time.sleep(self.POLLING_INTERVAL * 4)
            imports = self.get_import_count(host)
            logger.info('Docs imported: {}'.format(imports))
            if imports >= expected_docs:
                end_time = time.time()
                time_taken = end_time - start_time
                return time_taken
            if time.time() - start_time > 1800:
                raise Exception("timeout of 1800 seconds exceeded")

    def monitor_sgreplicate(self, host, expected_docs, replicate_id, version):
        logger.info('Monitoring SGReplicate items:')
        initial_items, start_time = self._wait_for_sg_replicate_start(host, replicate_id, version)
        logger.info('initial items: {} start time: {}'.format(initial_items, start_time))
        items_in_range = expected_docs - initial_items
        logger.info('items in range: {}'.format(items_in_range))
        time_taken, final_items = self._wait_for_sg_replicate_complete(host,
                                                                       expected_docs,
                                                                       start_time,
                                                                       replicate_id,
                                                                       version)

        if replicate_id == 'sgr2_conflict_resolution':
            items_in_range = final_items - initial_items
        return time_taken, items_in_range

    def _wait_for_sg_replicate_start(self, host, replicate_id, version):
        logger.info('Checking if replicate process started')
        logger.info('host: {}'.format(host))
        replicate_docs = 0
        while True:
            if type(host) is list:
                for sg in range(len(host)):
                    stats = self.get_sgreplicate_stats(host=host[sg],
                                                       version=version)
                    for stat in stats:
                        if stat.get('replication_id', '') == replicate_id[sg]:
                            if 'pull' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_read', 0))
                            elif 'push' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_written', 0))
            else:
                stats = self.get_sgreplicate_stats(host=host,
                                                   version=version)
                for stat in stats:
                    if stat.get('replication_id', '') == replicate_id:
                        if replicate_id == 'sgr1_pull' or replicate_id == 'sgr2_pull':
                            replicate_docs = int(stat.get('docs_read', 0))
                        elif replicate_id == 'sgr2_pushAndPull' \
                                or replicate_id == 'sgr2_conflict_resolution':
                            if 'docs_read' in stat:
                                replicate_docs += int(stat.get('docs_read', 0))
                            if 'docs_written' in stat:
                                replicate_docs += int(stat.get('docs_written', 0))
                        elif replicate_id == 'sgr1_push' or replicate_id == 'sgr2_push':
                            replicate_docs = int(stat.get('docs_written', 0))
                        break

                    if replicate_id == 'sgr1_pushAndPull':
                        if stat.get('replication_id', '') == 'sgr1_push':
                            replicate_docs += int(stat.get('docs_written', 0))
                        elif stat.get('replication_id', '') == 'sgr1_pull':
                            replicate_docs += int(stat.get('docs_read', 0))

            if replicate_docs >= 1:
                logger.info('replicating docs has started')
                return replicate_docs, time.time()

            time.sleep(self.POLLING_INTERVAL)

    def _wait_for_sg_replicate_complete(self, host, expected_docs, start_time,
                                        replicate_id, version):
        expected_docs = expected_docs
        start_time = start_time
        logger.info('Monitoring syncgateway replicate status :')
        while True:
            replicate_docs = 0
            if type(host) is list:
                for sg in range(len(host)):
                    stats = self.get_sgreplicate_stats(host=host[sg],
                                                       version=version)
                    for stat in stats:
                        if stat.get('replication_id', '') == replicate_id[sg]:
                            if 'pull' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_read', 0))
                            elif 'push' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_written', 0))
            else:
                stats = self.get_sgreplicate_stats(host=host,
                                                   version=version)
                for stat in stats:
                    if stat.get('replication_id', '') == replicate_id:
                        if replicate_id == 'sgr1_pull' or replicate_id == 'sgr2_pull':
                            replicate_docs = int(stat.get('docs_read', 0))
                        elif replicate_id == 'sgr2_pushAndPull' \
                                or replicate_id == 'sgr2_conflict_resolution':
                            if 'docs_read' in stat:
                                replicate_docs += int(stat.get('docs_read', 0))
                            if 'docs_written' in stat:
                                replicate_docs += int(stat.get('docs_written', 0))
                        elif replicate_id == 'sgr1_push' or replicate_id == 'sgr2_push':
                            replicate_docs = int(stat.get('docs_written', 0))
                        break

                    if replicate_id == 'sgr1_pushAndPull':
                        if stat.get('replication_id', '') == 'sgr1_push':
                            replicate_docs += int(stat.get('docs_written', 0))
                        elif stat['replication_id'] == 'sgr1_pull':
                            replicate_docs += int(stat.get('docs_read', 0))

            logger.info('Docs replicated: {}'.format(replicate_docs))
            if replicate_id == 'sgr2_conflict_resolution':
                sg_stats = self.get_sg_stats(host=host)
                local_count = 0
                remote_count = 0
                merge_count = 0
                num_docs_pushed = 0
                for count in range(1, self.test_config.cluster.num_buckets + 1):
                    db = 'db-{}'.format(count)
                    sgr_stats = sg_stats['syncgateway']['per_db'][db]['replications'][replicate_id]
                    local_count += int(sgr_stats['sgr_conflict_resolved_local_count'])
                    remote_count += int(sgr_stats['sgr_conflict_resolved_remote_count'])
                    merge_count += int(sgr_stats['sgr_conflict_resolved_merge_count'])
                    num_docs_pushed += int(sgr_stats['sgr_num_docs_pushed'])
                if ((local_count + remote_count + merge_count) == int(expected_docs/2)) \
                        and (local_count == num_docs_pushed):
                    end_time = time.time()
                    time_taken = end_time - start_time
                    return time_taken, replicate_docs
            else:
                if replicate_docs >= expected_docs:
                    end_time = time.time()
                    time_taken = end_time - start_time
                    return time_taken, replicate_docs

            time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > 1800:
                raise Exception("timeout of 1800 seconds exceeded")

    def deltasync_stats(self, host: str, db: str):
        stats = self.get_expvar_stats(host)
        if 'delta_sync' in stats['syncgateway']['per_db'][db].keys():
            return stats['syncgateway']['per_db'][db]
        else:
            logger.info('Delta Sync Disabled')
            return stats['syncgateway']['per_db']

    def deltasync_bytes_transfer(self, host: str):
        stats = self.get_expvar_stats(host)
        bytes_transeferred = 0
        if self.test_config.syncgateway_settings.replication_type == 'PUSH':
            replication_type = 'doc_writes_bytes_blip'
        else:
            replication_type = 'doc_reads_bytes_blip'
        for count in range(1, self.test_config.cluster.num_buckets + 1):
            db = 'db-{}'.format(count)
            bytes_transeferred += float(
                    stats['syncgateway']['per_db'][db]['database'][replication_type])
            return bytes_transeferred

    def get_sgw_push_count(self, host):
        sgw_stats = self.get_sg_stats(host)
        push_count = 0
        if not self.cluster_spec.capella_infrastructure:
            for count in range(1, self.test_config.cluster.num_buckets + 1):
                db = 'db-{}'.format(count)
                push_count += \
                    int(sgw_stats['syncgateway']['per_db'][db]
                                 ['cbl_replication_push']['doc_push_count'])
        else:
            stat = sgw_stats.find("sgw_replication_push_doc_push_count")
            stat_list = []
            while stat != -1:
                stat_list.append(stat)
                stat = sgw_stats.find("sgw_replication_push_doc_push_count", stat + 1)
            last = sgw_stats.find("# HELP", stat_list[-1] + 1)
            stat_list.append(last)
            for i in range(2, len(stat_list) - 1):
                str = sgw_stats[stat_list[i]:stat_list[i+1]]
                a = str.find("}")
                push_count += int(float(str[a+2:]))
            push_count /= 2
        return push_count

    def get_sgw_pull_count(self, host):
        pull_count = 0
        sgw_stats = self.get_sg_stats(host)
        if not self.cluster_spec.capella_infrastructure:
            for count in range(1, self.test_config.cluster.num_buckets + 1):
                db = 'db-{}'.format(count)
                pull_count += \
                    int(sgw_stats['syncgateway']['per_db'][db]
                                 ['cbl_replication_pull']['rev_send_count'])
        else:
            stat = sgw_stats.find("sgw_shared_bucket_import_import_count")
            stat_list = []
            while stat != -1:
                stat_list.append(stat)
                stat = sgw_stats.find("sgw_shared_bucket_import_import_count", stat + 1)
            last = sgw_stats.find("# HELP", stat_list[-1] + 1)
            stat_list.append(last)
            for i in range(2, len(stat_list) - 1):
                str = sgw_stats[stat_list[i]:stat_list[i+1]]
                a = str.find("}")
                pull_count += int(float(str[a+2:]))
            pull_count /= 2
        return pull_count

    def wait_sgw_push_start(self, hosts, initial_docs):
        retries = 0
        max_retries = 900
        while True:
            push_count = 0
            start_time = time.time()
            for host in hosts:
                push_count += self.get_sgw_push_count(host)
                if self.cluster_spec.capella_infrastructure:
                    break

            if push_count > initial_docs:
                return start_time, push_count
            retries += 1
            if retries >= max_retries:
                raise Exception(
                    "Push failed to start within {} seconds".format(
                        max_retries*self.POLLING_INTERVAL_SGW)
                )
            time.sleep(self.POLLING_INTERVAL_SGW)

    def wait_sgw_pull_start(self, hosts, initial_docs):
        retries = 0
        max_retries = 900
        while True:
            pull_count = 0
            start_time = time.time()
            for host in hosts:
                pull_count += self.get_sgw_pull_count(host)
                if self.cluster_spec.capella_infrastructure:
                    break

            if pull_count > initial_docs:
                return start_time, pull_count
            retries += 1
            if retries >= max_retries:
                raise Exception(
                    "Pull failed to start within {} seconds".format(
                        max_retries*self.POLLING_INTERVAL_SGW)
                )
            time.sleep(self.POLLING_INTERVAL_SGW)

    def wait_sgw_push_docs(self, hosts, target_docs):
        retries = 0
        max_retries = 360
        last_push_count = 0
        while True:
            push_count = 0
            finished_time = time.time()
            for host in hosts:
                push_count += self.get_sgw_push_count(host)
                if self.cluster_spec.capella_infrastructure:
                    break

            if push_count >= target_docs:
                return finished_time, push_count
            logger.info("push count: {}".format(push_count))
            if push_count == last_push_count:
                retries += 1
                if retries >= max_retries:
                    raise Exception(
                        "Push failed to complete..."
                    )
            last_push_count = push_count
            time.sleep(self.POLLING_INTERVAL_SGW)

    def wait_sgw_pull_docs(self, hosts, target_docs):
        retries = 0
        max_retries = 360
        last_pull_count = 0
        while True:
            pull_count = 0
            finished_time = time.time()
            for host in hosts:
                pull_count += self.get_sgw_pull_count(host)
                if self.cluster_spec.capella_infrastructure:
                    break

            if pull_count >= target_docs:
                return finished_time, pull_count
            logger.info("pull count: {}".format(pull_count))
            if pull_count == last_pull_count:
                retries += 1
                if retries >= max_retries:
                    raise Exception(
                        "Pull failed to complete..."
                    )
            last_pull_count = pull_count
            time.sleep(self.POLLING_INTERVAL_SGW)

    def wait_sgw_push_stop(self, hosts):
        logger.info('Waiting for push to finish')
        last_push_count = 0
        verify_count = 0
        max_verify = 3
        finished_time = time.time()
        maybe_finished_time = time.time()
        while True:
            if verify_count == 0:
                maybe_finished_time = time.time()
            push_count = 0
            for host in hosts:
                push_count += self.get_sgw_push_count(host)
                if self.cluster_spec.capella_infrastructure:
                    break

            logger.info('push count: {}'.format(push_count))
            if push_count == last_push_count:
                logger.info('push maybe completed')
                if verify_count == 0:
                    finished_time = maybe_finished_time
                verify_count += 1
                if verify_count >= max_verify:
                    return finished_time
                last_push_count = push_count
                continue
            else:
                verify_count = 0
            last_push_count = push_count
            time.sleep(self.POLLING_INTERVAL_SGW)


class KubernetesMonitor(KubernetesRestHelper):

    MAX_RETRY = 150
    MAX_RETRY_RECOVERY = 1200
    MAX_RETRY_TIMER_EVENT = 18000
    MAX_RETRY_BOOTSTRAP = 1200

    MONITORING_DELAY = 5

    POLLING_INTERVAL = 2
    POLLING_INTERVAL_INDEXING = 1
    POLLING_INTERVAL_MACHINE_UP = 10
    POLLING_INTERVAL_ANALYTICS = 15
    POLLING_INTERVAL_EVENTING = 1

    REBALANCE_TIMEOUT = 3600 * 6
    TIMEOUT = 3600 * 12

    DISK_QUEUES = (
        'ep_queue_size',
        'ep_flusher_todo',
        'ep_diskqueue_items',
        'vb_active_queue_size',
        'vb_replica_queue_size',
    )

    DCP_QUEUES = (
        'ep_dcp_replica_items_remaining',
        'ep_dcp_other_items_remaining',
    )

    XDCR_QUEUES = (
        'replication_changes_left',
    )

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config)
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    def is_index_ready(self, host: str) -> bool:
        return True

    def monitor_indexing(self, host):
        logger.info('Monitoring indexing progress')

        while not self.is_index_ready(host):
            time.sleep(self.POLLING_INTERVAL_INDEXING * 5)
            pending_docs = self.estimate_pending_docs(host)
            logger.info('Pending docs: {:,}'.format(pending_docs))

        logger.info('Indexing completed')

    def estimate_pending_docs(self, host: str) -> int:
        stats = self.get_gsi_stats(host)
        pending_docs = 0
        for metric, value in stats.items():
            if 'num_docs_queued' in metric or 'num_docs_pending' in metric:
                pending_docs += value
        return pending_docs

    def monitor_disk_queues(self, host, bucket):
        logger.info('Monitoring disk queues: {}'.format(bucket))
        self._wait_for_empty_queues(host, bucket, self.DISK_QUEUES,
                                    self.get_bucket_stats)

    def monitor_dcp_queues(self, host, bucket):
        logger.info('Monitoring DCP queues: {}'.format(bucket))
        self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                    self.get_bucket_stats)

    def monitor_replica_count(self, host, bucket):
        logger.info('Monitoring replica count match: {}'.format(bucket))
        self._wait_for_replica_count_match(host, bucket)

    def _wait_for_empty_queues(self, host, bucket, queues, stats_function):
        metrics = list(queues)

        start_time = time.time()
        while metrics:
            bucket_stats = stats_function(host, bucket)
            # As we are changing metrics in the loop; take a copy of it to
            # iterate over.
            for metric in list(metrics):
                stats = bucket_stats['op']['samples'].get(metric)
                if stats:
                    last_value = stats[-1]
                    if last_value:
                        logger.info('{} = {:,}'.format(metric, last_value))
                        continue
                    else:
                        logger.info('{} reached 0'.format(metric))
                    metrics.remove(metric)
            if metrics:
                time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Monitoring got stuck')

    def _wait_for_replica_count_match(self, host, bucket):
        start_time = time.time()
        bucket_info = self.get_bucket_info(host, bucket)
        replica_number = int(bucket_info['replicaNumber'])
        while replica_number:
            bucket_stats = self.get_bucket_stats(host, bucket)
            curr_items = bucket_stats['op']['samples'].get("curr_items")[-1]
            replica_curr_items = bucket_stats['op']['samples'].get("vb_replica_curr_items")[-1]
            logger.info("curr_items: {}, replica_curr_items: {}".format(curr_items,
                                                                        replica_curr_items))
            if (curr_items * replica_number) == replica_curr_items:
                break
            time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Replica items monitoring got stuck')

    def monitor_num_items(self, host: str, bucket: str, num_items: int, max_retry: int = None):
        logger.info('Checking the number of items in {}'.format(bucket))
        if not max_retry:
            max_retry = self.MAX_RETRY
        retries = 0
        while retries < max_retry:
            curr_items = self._get_num_items(host, bucket, total=True)
            if curr_items == num_items:
                break
            else:
                logger.info('{}(curr_items) != {}(num_items)'.format(curr_items, num_items))
            time.sleep(self.POLLING_INTERVAL)
            retries += 1
        else:
            actual_items = self._get_num_items(host, bucket, total=True)
            raise Exception('Mismatch in the number of items: {}'
                            .format(actual_items))

    def _get_num_items(self, host: str, bucket: str, total: bool = False) -> int:
        stats = self.get_bucket_stats(host=host, bucket=bucket)
        if total:
            curr_items = stats['op']['samples'].get('curr_items_tot')
        else:
            curr_items = stats['op']['samples'].get('curr_items')
        if curr_items:
            return curr_items[-1]
        return 0


class ServerlessMonitor(DefaultMonitor):

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec=cluster_spec, test_config=test_config, verbose=verbose)
        self.rest = ServerlessRestHelper(cluster_spec=cluster_spec, test_config=test_config)

    def monitor_fts_indexing_queue(self, host: str, index: str, items: int, bucket="bucket-1"):
        logger.info('{} : Waiting for indexing to finish'.format(index))
        count = 0
        while count < items:
            count = self.rest.get_fts_doc_count(host, index, bucket)
            logger.info('FTS indexed documents for {}: {:,}'.format(index, count))
            time.sleep(self.POLLING_INTERVAL)

    def monitor_fts_index_persistence(self, hosts: list, index: str, bkt: str = None):
        logger.info('{}: Waiting for index to be persisted'.format(index))
        if not bkt:
            bkt = self.test_config.buckets[0]
        tries = 0
        pending_items = 1
        while pending_items:
            try:
                persist = 0
                compact = 0
                for host in hosts:
                    stats = self.rest.get_fts_stats(host)
                    metric = '{}:{}:{}'.format(bkt, index, 'num_recs_to_persist')
                    persist += stats[metric]

                    metric = '{}:{}:{}'.format(bkt, index, 'total_compactions')
                    compact += stats[metric]
                pending_items = persist or compact
                logger.info('Records to persist: {:,}'.format(persist))
                logger.info('Ongoing compactions: {:,}'.format(compact))
            except KeyError:
                tries += 1
            if tries >= 10:
                raise Exception("cannot get fts stats")
            time.sleep(self.POLLING_INTERVAL)

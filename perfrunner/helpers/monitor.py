import time
from typing import Callable, Optional

from logger import logger
from perfrunner.helpers import misc
from perfrunner.helpers.rest import RestType
from perfrunner.remote import Remote
from perfrunner.settings import ClusterSpec


class Monitor:

    MAX_RETRY = 150
    MAX_RETRY_RECOVERY = 1200
    MAX_RETRY_TIMER_EVENT = 18000
    MAX_RETRY_BOOTSTRAP = 1200
    MAX_RETRY_FOR_FTS_MERGES = 90

    MONITORING_DELAY = 5

    POLLING_INTERVAL = 1
    POLLING_INTERVAL_INDEXING = 1
    POLLING_INTERVAL_MACHINE_UP = 20
    POLLING_INTERVAL_ANALYTICS = 15
    POLLING_INTERVAL_EVENTING = 1
    POLLING_INTERVAL_FRAGMENTATION = 10
    POLLING_INTERVAL_SGW = 1
    POLLING_INTERVAL_SGW_LOGSTREAMING = 5
    POLLING_INTERVAL_SGW_RESYNC = 60  # 1m delay is ok since this takes hours to complete

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

    def __init__(
        self,
        cluster_spec: ClusterSpec,
        rest: RestType,
        remote: Remote,
        build: str,
        awr_bucket: str,
        awr_scope: str,
    ):
        self.cluster_spec = cluster_spec
        self.remote = remote
        self.rest = rest
        self.master_node = next(cluster_spec.masters)
        self.build = build
        self.build_version_number = misc.create_build_tuple(self.build)
        self.is_columnar = self.rest.is_columnar(self.master_node)
        self.awr_bucket = awr_bucket
        self.awr_scope = awr_scope

    def wait_for_rebalance_to_begin(self, host):
        logger.info('Waiting for rebalance to start')

        is_running = False
        start_time = time.time()
        while not is_running:
            is_running, progress = self.rest.get_task_status(host, task_type='rebalance')
            logger.info('Rebalance running: {}'.format(is_running))
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Monitoring got stuck')
            time.sleep(self.POLLING_INTERVAL)

        logger.info('Rebalance started. Rebalance progress: {} %'.format(progress))

    def wait_for_rebalance_task(self, host: str):
        """Wait for rebalance task to complete (successfully or not) on the given host."""
        is_running = True
        last_progress = 0
        last_progress_time = time.time()

        while is_running:
            time.sleep(self.POLLING_INTERVAL)

            is_running, progress = self.rest.get_task_status(host, task_type="rebalance")
            if progress == last_progress:
                if time.time() - last_progress_time > self.REBALANCE_TIMEOUT:
                    logger.interrupt("Rebalance hung")
            else:
                last_progress = progress
                last_progress_time = time.time()

            if progress is not None:
                logger.info(f"Rebalance progress: {progress} %")

        logger.info("Rebalance task stopped running")

    def wait_for_cluster_balanced(self, host: str, timeout_secs: int = 20) -> bool:
        """Wait for the cluster to become balanced."""
        is_balanced = False
        deadline = time.time() + timeout_secs
        while time.time() < deadline and not (is_balanced := self.rest.is_balanced(host)):
            time.sleep(self.POLLING_INTERVAL)

        if is_balanced:
            logger.info("Cluster is balanced")
        else:
            logger.error(f"Cluster did not become balanced within {timeout_secs}s")

        return is_balanced

    def monitor_rebalance(self, host: str):
        """Monitor a rebalance operation until the cluster is balanced."""
        logger.info("Monitoring rebalance status")

        self.wait_for_rebalance_task(host)

        logger.info("Waiting for cluster to become balanced")
        if not self.wait_for_cluster_balanced(self.master_node):
            rebalance_report = self.rest.get_rebalance_report(self.master_node)
            completion_message = rebalance_report["completionMessage"]
            logger.interrupt(f"Rebalance failed with message {completion_message}")

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
            try:
                if stats := int(kv_dcp_stats['data'][0]['values'][-1][1]):
                    logger.info('{} = {}'.format('ep_dcp_replica_items_remaining', stats))
                else:
                    logger.info('{} reached 0'.format('ep_dcp_replica_items_remaining'))
                    break
            except Exception:
                pass

            if time.time() - start_time > self.TIMEOUT:
                raise Exception('DCP queue Monitoring got stuck')
            time.sleep(self.POLLING_INTERVAL)

    def _wait_for_replica_count_match(self, host, bucket):
        start_time = time.time()
        bucket_info = self.rest.get_bucket_info(host, bucket)
        replica_number = int(bucket_info['replicaNumber'])
        while replica_number:
            bucket_stats = self.rest.get_bucket_stats(host, bucket)
            curr_items = bucket_stats['op']['samples'].get("curr_items")[-1]
            replica_curr_items = bucket_stats['op']['samples'].get("vb_replica_curr_items")[-1]
            logger.info("curr_items: {}, replica_curr_items: {}".format(curr_items,
                                                                        replica_curr_items))
            if (curr_items * replica_number) == replica_curr_items:
                break
            time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Replica items monitoring got stuck')

    def _wait_for_replication_completion(self, host: str, bucket: str, link1: str, link2: str):

        completion_count = 0
        link1_time = 0
        link2_items = 0

        start_time = time.time()

        # No need to check for every bucket if we have multiple buckets
        if bucket == "bucket-1":
            logger.info("Sleep until get_xdcr_changes_left_link1 starts to be updated")
            while self.rest.get_xdcr_changes_left_link(host, bucket, link1) <= 0:
                time.sleep(self.POLLING_INTERVAL)
                if time.time() - start_time > self.TIMEOUT:
                    raise Exception('xdcr_changes_left was not updated')
        logger.info("Monitoring queues")
        while True:
            xdcr_changes_left_link1 = self.rest.get_xdcr_changes_left_link(host, bucket, link1)
            logger.info(f'xdcr_changes_left_link1 = {xdcr_changes_left_link1:,}')
            link1_completeness = self.rest.get_xdcr_completeness(host, bucket, link1)
            logger.info(f'link1_completeness = {link1_completeness:,}')
            link2_completeness = self.rest.get_xdcr_completeness(host, bucket, link2)
            logger.info(f'link2_completeness = {link2_completeness:,}')
            if (link1_completeness == 100 or link2_completeness == 100) and completion_count == 0:
                link1_time = time.time()
                link2_items = self.rest.get_xdcr_items(host, bucket, link2)
                completion_count += 1
            if xdcr_changes_left_link1 == 0:
                logger.info('xdcr_changes_left_link1 reached 0')
                break
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
                                    self.rest.get_bucket_stats)

    def monitor_dcp_queues(self, host: str, bucket: str, bucket_replica: int):
        logger.info('Monitoring DCP queues: {}'.format(bucket))
        if self.build_version_number < (1, 0, 0, 0):
            if bucket_replica != 0:
                if self.build_version_number < (0, 0, 0, 2106):
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.rest.get_dcp_replication_items)
                else:
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.rest.get_dcp_replication_items_v2)
            self.DCP_QUEUES = (
                'ep_dcp_other_items_remaining',
            )
            self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                        self.rest.get_bucket_stats)
        elif self.build_version_number < (7, 0, 0, 3937):
            self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                        self.rest.get_bucket_stats)
        else:
            if bucket_replica != 0:
                if self.build_version_number < (7, 0, 0, 4990):
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.rest.get_dcp_replication_items)
                else:
                    self._wait_for_empty_dcp_queues(host, bucket,
                                                    self.rest.get_dcp_replication_items_v2)
            self.DCP_QUEUES = (
                'ep_dcp_other_items_remaining',
            )
            self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                        self.rest.get_bucket_stats)

    def monitor_replica_count(self, host, bucket):
        logger.info('Monitoring replica count match: {}'.format(bucket))
        self._wait_for_replica_count_match(host, bucket)

    def _wait_for_xdcr_to_start(self, host: str):
        is_running = False
        while not is_running:
            time.sleep(self.POLLING_INTERVAL)
            is_running, _ = self.rest.get_task_status(host, task_type='xdcr')

    def xdcr_link_starttime(self, host: str, uuid: str):
        is_running = False
        while not is_running:
            time.sleep(self.POLLING_INTERVAL)
            is_running, _ = self.rest.get_xdcrlink_status(host, task_type='xdcr', uuid=uuid)
        return time.time()

    def monitor_xdcr_queues(
        self, host: str, bucket: str, total_docs: int, mobile: Optional[str] = None
    ):
        logger.info('Monitoring XDCR queues: {}'.format(bucket))
        self._wait_for_xdcr_to_start(host)
        # adding temporary delay to make sure replication_changes_left stats arrives
        time.sleep(20)
        if self.build_version_number < (7, 6, 0, 0):
            self._wait_for_empty_queues(host, bucket, self.XDCR_QUEUES,
                                        self.rest.get_xdcr_stats)
        else:
            self._wait_for_empty_xdcr_queues(host, bucket, total_docs, mobile)

    def _wait_for_empty_xdcr_queues(
        self, host: str, bucket: str, total_docs: int, mobile: Optional[str] = None
    ):
        start_time = time.time()
        # For mobile replication (XDCR with SGW), xdcr_changes_left_total will never reach 0
        # because of sgw heartbeat docs being constantly rewritten. These docs are filtered and
        # are not replicated, but they still count towards xdcr_changes_left_total.
        # So we will consider replication complete if the following happens:
        # - xdcr_docs_written_total >= total number of docs (it can be greater due to import)
        # - xdcr_mobile_docs_filtered_total keeps increasing
        # - xdcr_changes_left_total = 1
        if mobile:
            previous_xdcr_mobile_docs_filtered_total = \
                self.rest.xdcr_mobile_docs_filtered_total(host, bucket)
            logger.info('Initial xdcr_mobile_docs_filtered_total = {:,}'.
                        format(previous_xdcr_mobile_docs_filtered_total))
        # No need to check for every bucket if we have multiple buckets
        if bucket == "bucket-1":
            logger.info("Sleep until xdcr_changes_left_total starts to be updated")
            while self.rest.get_xdcr_changes_left_total(host, bucket) <= 0:
                time.sleep(self.POLLING_INTERVAL)
                if time.time() - start_time > self.TIMEOUT:
                    raise Exception('xdcr_changes_left was not updated')
        while True:
            xdcr_changes_left_total = self.rest.get_xdcr_changes_left_total(host, bucket)
            if xdcr_changes_left_total:
                logger.info('xdcr_changes_left_total = {:,}'.format(xdcr_changes_left_total))
                if xdcr_changes_left_total == 1:
                    xdcr_docs_written_total = self.rest.get_xdcr_docs_written_total(host, bucket)
                    logger.info('xdcr_docs_written_total = {:,}'.format(xdcr_docs_written_total))

                    if xdcr_docs_written_total >= total_docs:
                        xdcr_mobile_docs_filtered_total = \
                            self.rest.xdcr_mobile_docs_filtered_total(host, bucket)
                        logger.info('xdcr_mobile_docs_filtered_total = {:,}'.
                                    format(xdcr_mobile_docs_filtered_total))
                        if xdcr_mobile_docs_filtered_total > \
                           previous_xdcr_mobile_docs_filtered_total:
                            logger.info('Reached the end of the replication')
                            break
                else:
                    previous_xdcr_mobile_docs_filtered_total = \
                        self.rest.xdcr_mobile_docs_filtered_total(host, bucket)
            elif xdcr_changes_left_total == 0:
                logger.info('xdcr_changes_left_total reached 0')
                break
            time.sleep(self.POLLING_INTERVAL)
            if time.time() - start_time > self.TIMEOUT:
                raise Exception('Monitoring got stuck')

    def monitor_xdcr_changes_left(self, host: str, bucket: str, xdcrlink1: str, xdcrlink2: str):
        logger.info('Monitoring XDCR queues: {}'.format(bucket))
        self._wait_for_xdcr_to_start(host)
        start_time = time.time()
        link1_time, link2_items = self._wait_for_replication_completion(host, bucket,
                                                                        xdcrlink1,
                                                                        xdcrlink2)
        return start_time, link1_time, link2_items

    def monitor_xdcr_completeness(self, host: str, bucket: str, xdcr_link: str):
        logger.info('Monitoring XDCR Link Completeness: {}'.format(bucket))
        self._wait_for_completeness(host=host, bucket=bucket, xdcr_link=xdcr_link,
                                    stats_function=self.rest.get_xdcr_stats)
        return time.time()

    def get_num_items(self, host: str, bucket: str, bucket_replica: int):
        num_items = self._get_num_items(host, bucket, bucket_replica, total=True)
        return num_items

    def _get_num_items(
        self, host: str, bucket: str, bucket_replica: int, total: bool = False
    ) -> int:
        stats = self.rest.get_bucket_stats(host=host, bucket=bucket)
        if total:
            curr_items = stats['op']['samples'].get('curr_items_tot')
        else:
            curr_items = stats['op']['samples'].get('curr_items')
        if curr_items:
            return self._ignore_system_collection_items(
                host=host, bucket=bucket, bucket_replica=bucket_replica, curr_items=curr_items[-1]
            )
        return 0

    def _get_num_items_scope_and_collection(self, host: str, bucket: str, scope: str,
                                            coll: str = None) -> int:
        stats = self.rest.get_scope_and_collection_items(host, bucket, scope, coll)
        if stats:
            return sum(int(d['values'][-1][1]) for d in stats['data'])
        return 0

    def _ignore_system_collection_items(
        self, host: str, bucket: str, bucket_replica: int, curr_items: int
    ) -> int:
        sys_coll_items = self._get_num_items_scope_and_collection(host, bucket, '_system')
        replica_sys_coll_items = sys_coll_items * (1 + bucket_replica)
        awr_coll_items = self._get_num_items_scope_and_collection(
            host, self.awr_bucket, self.awr_scope
        )
        replica_awr_coll_items = awr_coll_items * (1 + bucket_replica)
        logger.info(
            f"Ignoring items in _system collection: {sys_coll_items} (active), "
            f"{replica_sys_coll_items} (total)"
        )
        logger.info(
            f"Ignoring items in scope-awr collection: {awr_coll_items} (active), "
            f"{replica_awr_coll_items} (total)"
        )
        return curr_items - replica_sys_coll_items - replica_awr_coll_items

    def monitor_num_items(
        self, host: str, bucket: str, bucket_replica: int, num_items: int, max_retry: int = None
    ):
        logger.info('Checking the number of items in {}'.format(bucket))

        if not max_retry:
            max_retry = self.MAX_RETRY

        retries = 0
        while retries < max_retry:
            curr_items = self._get_num_items(host, bucket, bucket_replica, total=True)
            if curr_items == num_items:
                break
            else:
                logger.info('{}(curr_items) != {}(num_items)'.format(curr_items, num_items))

            time.sleep(self.POLLING_INTERVAL)
            retries += 1
        else:
            actual_items = self._get_num_items(host, bucket, bucket_replica, total=True)
            raise Exception('Mismatch in the number of items: {}'
                            .format(actual_items))

    def monitor_num_backfill_items(
        self, host: str, bucket: str, bucket_replica: int, num_items: int
    ):
        logger.info('Checking the number of items in {}'.format(bucket))

        t0 = time.time()
        while True:
            curr_items = self._get_num_items(host, bucket, bucket_replica, total=True)

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

            tasks = [task for task in self.rest.get_tasks(host)
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
        logger.info('Monitoring warmup status: {}@{}'.format(bucket, host))

        memcached_port = self.rest.get_memcached_port(host)

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

        memcached_port = self.rest.get_memcached_port(host)

        json_docs = -1
        while json_docs:
            stats = memcached.get_stats(host, memcached_port, bucket)
            json_docs = int(stats['ep_active_datatype_json'])
            if json_docs:
                logger.info('Still uncompressed: {:,} items'.format(json_docs))
                time.sleep(self.POLLING_INTERVAL)
        logger.info('All items are compressed')

    def monitor_node_health(
        self,
        host: str,
        polling_interval_secs: Optional[int] = None,
        max_retries: Optional[int] = None,
    ):
        max_retries = max_retries or self.MAX_RETRY
        polling_interval_secs = polling_interval_secs or self.POLLING_INTERVAL
        logger.info('Monitoring node health')

        for _ in range(max_retries):
            unhealthy_nodes = {
                n for n, status in self.rest.node_statuses(host).items()
                if status != 'healthy'
            } | {
                n for n, status in self.rest.node_statuses_v2(host).items()
                if status != 'healthy'
            }
            if not unhealthy_nodes:
                break

            time.sleep(polling_interval_secs)
        else:
            logger.interrupt(f"Some nodes are not healthy: {unhealthy_nodes}")

    def monitor_analytics_node_active(
        self,
        host: str,
        polling_interval_secs: int = 0,
        max_retries: int = 0,
    ):
        max_retries = max_retries or self.MAX_RETRY
        polling_interval_secs = polling_interval_secs or self.POLLING_INTERVAL
        logger.info("Monitoring analytics node health")

        for _ in range(max_retries):
            if self.rest.analytics_node_active(host):
                break
            time.sleep(polling_interval_secs)
        else:
            logger.interrupt(f"Analytics node still not healthy: {host}")

    def is_index_ready(self, host: str) -> bool:
        for status in self.rest.get_index_status(host)['status']:
            if status['status'] != 'Ready':
                return False
        return True

    def estimate_pending_docs(self, host: str) -> int:
        stats = self.rest.get_gsi_stats(host)
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
                indexes_remaining[i] = self.rest.indexes_per_node(node)

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
            json2i = self.rest.get_index_status(host)
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
        for scope_map in indexes.values():
            for collection_map in scope_map.values():
                for index_map in collection_map.values():
                    for index, index_config in index_map.items():
                        if isinstance(index_config, dict):
                            num_replica = index_config["num_replica"]
                            index_list.append(index)
                            for i in range(1, num_replica+1):
                                index_list.append("{index_name} (replica {number})"
                                                  .format(index_name=index, number=i))
                        else:
                            for index_name in index_map:
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
            json2i = self.rest.get_index_status(host)
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
            data = self.rest.get_index_stats(index_nodes)
            num_indexed = []
            for index in indexes:
                key = "" + bucket + ":" + index + ":num_docs_indexed"
                val = data[key]
                num_indexed.append(val)
            return num_indexed

        def get_num_docs_index_pending():
            data = self.rest.get_index_stats(index_nodes)
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
            data = self.rest.get_index_stats(index_nodes)
            num_indexed = []
            for index in indexes:
                key = index + ":num_docs_indexed"
                val = data[key]
                num_indexed.append(val)
            return num_indexed

        def get_num_docs_index_pending():
            data = self.rest.get_index_stats(index_nodes)
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
        curr_connections = self.rest.get_index_num_connections(index_node)
        retry = 1
        while curr_connections < expected_connections and retry < self.MAX_RETRY:
            time.sleep(self.POLLING_INTERVAL_INDEXING)
            curr_connections = self.rest.get_index_num_connections(index_node)
            logger.info("Got current connections {}".format(curr_connections))
            retry += 1
        if retry == self.MAX_RETRY:
            return False
        return True

    def wait_for_recovery(self, index_nodes, bucket, index):
        time.sleep(self.MONITORING_DELAY)
        for retry in range(self.MAX_RETRY_RECOVERY):
            response = self.rest.get_index_stats(index_nodes)
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
            count = self.rest.get_fts_doc_count(host, index, bucket)
            logger.info('FTS indexed documents for {}: {:,}'.format(index, count))
            time.sleep(self.POLLING_INTERVAL)

    def monitor_fts_index_persistence(self, hosts: list[str], index: str, bucket: str):
        logger.info(f"{index}: Waiting for index to be persisted")
        tries = 0
        pending_items = 1
        while pending_items:
            try:
                persist = 0
                compact = 0
                for host in hosts:
                    stats = self.rest.get_fts_stats(host)
                    persist += stats[f"{bucket}:{index}:num_recs_to_persist"]
                    compact += stats[f"{bucket}:{index}:total_compactions"]
                pending_items = persist or compact
                logger.info(f"Records to persist: {persist:,}")
                logger.info(f"Ongoing compactions: {compact:,}")
            except KeyError:
                tries += 1
            if tries >= 10:
                raise Exception("cannot get fts stats")
            time.sleep(self.POLLING_INTERVAL)

    def monitor_fts_index_persistence_and_merges(self, hosts: list, index: str, bucket: str):
        logger.info(f"{index}: Waiting for index to be persisted")

        tries = 0
        retries = 0
        pending_items = 1
        previous_file_merge_total = -1
        previous_mem_merge_total = -1
        file_merge_ops = f"{bucket}:{index}:num_file_merge_ops"
        mem_merge_ops = f"{bucket}:{index}:num_mem_merge_ops"
        metric = f"{bucket}:{index}:num_recs_to_persist"
        while pending_items:
            try:
                persist = 0
                current_file_merge_total = 0
                current_mem_merge_total = 0
                for host in hosts:
                    stats = self.rest.get_fts_stats(host)
                    persist += stats[metric]
                    current_file_merge_total += stats[file_merge_ops]
                    current_mem_merge_total += stats[mem_merge_ops]
                logger.info(f"Records to persist: {persist:,}")
                logger.info(f"Current file merge total: {current_file_merge_total}")
                logger.info(f"Current mem merge total: {current_mem_merge_total}")
                if (current_file_merge_total == previous_file_merge_total and
                    current_mem_merge_total == previous_mem_merge_total):
                    retries+=1
                else:
                    retries=0

                pending_items = persist or max(0, self.MAX_RETRY_FOR_FTS_MERGES-retries)
                previous_file_merge_total = current_file_merge_total
                previous_mem_merge_total = current_mem_merge_total
            except KeyError:
                tries += 1
            if tries >= 10:
                raise Exception("cannot get fts stats")
            time.sleep(self.POLLING_INTERVAL)

    def monitor_elastic_indexing_queue(self, host: str, index: str, total_docs: int):
        logger.info(" Waiting for indexing to finish")
        count = 0
        while count < total_docs:
            count = self.rest.get_elastic_doc_count(host, index)
            logger.info(f"Elasticsearch indexed documents: {count:,}")
            time.sleep(self.POLLING_INTERVAL)

    def monitor_elastic_index_persistence(self, host: str, index: str):
        logger.info('Waiting for index to be persisted')

        pending_items = -1
        while pending_items:
            stats = self.rest.get_elastic_stats(host)
            pending_items = stats['indices'][index]['total']['translog']['operations']
            logger.info('Records to persist: {:,}'.format(pending_items))
            time.sleep(self.POLLING_INTERVAL)

    def wait_for_bootstrap(self, nodes: list, function: str):
        logger.info('Waiting for bootstrap of eventing function: {} '.format(function))
        for node in nodes:
            retry = 1
            while retry < self.MAX_RETRY_BOOTSTRAP:
                if function in self.rest.get_apps_with_status(node, "deployed"):
                    break
                time.sleep(self.POLLING_INTERVAL)
                retry += 1
            if retry == self.MAX_RETRY_BOOTSTRAP:
                logger.info('Failed to bootstrap function: {}, node: {}'.
                            format(function, node))

    def get_num_analytics_items(self, analytics_node: str, bucket: str) -> int:
        stats_key = '{}:all:incoming_records_count_total'.format(bucket)
        num_items = 0
        for node in self.rest.get_active_nodes_by_role(analytics_node, 'cbas'):
            stats = self.rest.get_analytics_stats(node)
            num_items += stats.get(stats_key, 0)
        return num_items

    def query_num_analytics_items(self, analytics_node: str, dataset: str) -> int:
        """Get the number of items in an analytics dataset using an analytics query."""
        statement = "SELECT COUNT(*) from `{}`;".format(dataset)
        result = self.rest.exec_analytics_statement(analytics_node, statement)
        num_analytics_items = result.json()['results'][0]['$1']
        logger.info("Number of items in dataset `{}`: {}".
                    format(dataset, num_analytics_items))
        return num_analytics_items

    def get_ingestion_progress(self, analytics_node: str) -> int:
        stats = self.rest.get_ingestion_v2(analytics_node)
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
                if self.build_version_number < (7, 0, 0, 4622) and not self.is_columnar:
                    stats = self.rest.get_pending_mutations(analytics_node)
                    for dataset in stats['Default']:
                        if self.build_version_number < (7, 0, 0, 4310):
                            num_items += int(stats['Default'][dataset])
                        else:
                            num_items += int(stats['Default'][dataset]['seqnoLag'])
                else:
                    stats = self.rest.get_pending_mutations_v2(analytics_node)
                    for scope in stats['scopes']:
                        for collection in scope['collections']:
                            num_items += int(collection['seqnoLag'])
                break
            except Exception:
                time.sleep(self.POLLING_INTERVAL_ANALYTICS)
        return num_items

    def monitor_data_synced(
        self,
        data_node: str,
        bucket: str,
        bucket_replica: int,
        analytics_node: str,
        sql_suite: str = None,
    ) -> int:
        logger.info('Waiting for data to be synced from {}'.format(data_node))
        time.sleep(self.MONITORING_DELAY * 3)

        analytics_node_version = misc.create_build_tuple(self.rest.get_version(analytics_node))
        is_columnar = self.rest.is_columnar(analytics_node)

        num_items = self._get_num_items(data_node, bucket, bucket_replica)
        while True:
            if (analytics_node_version > (8, 0, 0, 0)) or (
                is_columnar and analytics_node_version >= (1, 0, 1, 0)
            ):
                metric = "cbas_incoming_records_total"
            else:
                metric = "cbas_incoming_records_count"

            if sql_suite is not None:
                incoming_records = self.rest.get_cbas_incoming_records_count(analytics_node, bucket,
                                                                         metric)
            else:
                incoming_records = self.rest.get_cbas_incoming_records_count(analytics_node, None,
                                                                        metric)

            try:
                num_analytics_items = int(incoming_records["data"][0]["values"][-1][1])
            except (IndexError, KeyError):
                num_analytics_items = 0
                logger.error(
                    "Failed to get incoming records count. "
                    f"REST API response: {incoming_records}"
                )

            logger.info(f"Analytics has {num_analytics_items:,} docs (target is {num_items:,})")

            ingestion_progress = self.get_ingestion_progress(analytics_node)
            logger.info(f"Ingestion progress: {ingestion_progress:.2f}%")
            if int(ingestion_progress) == 100:
                break

            time.sleep(self.POLLING_INTERVAL_ANALYTICS)

        return num_items

    def monitor_cbas_pending_ops(self, analytics_nodes: list[str]):
        logger.info("Wait until cbas_pending_ops reaches 0")
        cbas_pending_ops_list = [
            "cbas_pending_flush_ops",
            "cbas_pending_merge_ops",
            "cbas_pending_replicate_ops",
        ]

        while True:
            pending_ops = 0
            for analytics_node in analytics_nodes:
                stats = self.rest.get_analytics_prometheus_stats(analytics_node)
                for metric in cbas_pending_ops_list:
                    pending_ops += int(stats.get(metric, 0))

            logger.info(f"cbas pending ops = {pending_ops}")
            if pending_ops == 0:
                break

            time.sleep(self.POLLING_INTERVAL_ANALYTICS)

    def monitor_cbas_pause_status(self, analytics_node: str) -> str:
        logger.info('Wait until analytics pause operation is completed.')

        start_time = time.time()

        while True:
            resp = self.rest.get_analytics_pause_status(analytics_node)
            pause_status, failure = resp['status'], resp['failure']
            logger.info('Pause status: {}'.format(pause_status))

            if pause_status == 'running':
                time.sleep(self.POLLING_INTERVAL)
            elif pause_status == 'complete':
                break
            elif pause_status == 'notRunning':
                logger.warn('No pause attempts were made.')
                break
            elif pause_status == 'failed':
                logger.error('Pause operation failed: {}'.format(failure))
                break

            if time.time() - start_time > 1800:
                logger.interrupt('Monitoring analytics pause status timed out after 30 mins.')
                break

        return pause_status

    def monitor_cbas_kafka_link_connect_status(self, analytics_node: str, link_name: str,
                                               link_scope: str = 'Default') -> str:
        logger.info('Wait until Kafka Link is connected.')

        start_time = time.time()

        while True:
            resp = self.rest.get_analytics_link_info(analytics_node, link_name, link_scope)
            link_state = resp.get('linkState', None)
            logger.info('Link state: {}'.format(link_state))

            if link_state == 'CONNECTED':
                break
            elif link_state == 'DISCONNECTING':
                logger.interrupt('Link is disconnecting. '
                                 'Is there a problem with one of the connectors?')
                break
            elif link_state == 'DISCONNECTED':
                logger.interrupt('Link is disconnected. Was link connection ever initiated?')
                break
            elif link_state == 'RETRY_FAILED':
                logger.interrupt('Link connection failed after retry.')
                break

            if time.time() - start_time > 1800:
                logger.interrupt('Monitoring Kafka Link connect state timed out after 30 mins.')
                break

            time.sleep(self.POLLING_INTERVAL_ANALYTICS)

        return link_state

    def monitor_cbas_kafka_link_data_ingestion_status(
        self, analytics_node: str, final_dataset_counts: dict[str, int], timeout_mins: int
    ):
        logger.info('Wait until Kafka Link data ingestion is completed.')
        datasets_still_ingesting = set(final_dataset_counts.keys())

        t0 = time.time()
        while datasets_still_ingesting:
            if time.time() - t0 > (timeout_mins * 60):
                logger.interrupt('Monitoring Kafka Link data ingestion timed out after {} mins.'
                                 .format(timeout_mins))
                break

            for dataset in list(datasets_still_ingesting):
                target_items = final_dataset_counts[dataset]
                num_items = self.query_num_analytics_items(analytics_node, dataset)
                if num_items == target_items:
                    logger.info('Dataset `{}` has reached target count {} after ~{}s'
                                .format(dataset, num_items, time.time() - t0))
                    datasets_still_ingesting.remove(dataset)

            time.sleep(self.POLLING_INTERVAL_ANALYTICS)

    def monitor_dataset_drop(self, analytics_node: str, dataset: str):
        while True:
            num_analytics_items = self.query_num_analytics_items(analytics_node, dataset)
            if num_analytics_items == 0:
                break

            time.sleep(self.POLLING_INTERVAL)

    def wait_for_timer_event(self, node: str, function: str, event="timer_events"):
        logger.info('Waiting for timer events to start processing: {} '.format(function))
        retry = 1
        while retry < self.MAX_RETRY_TIMER_EVENT:
            if 0 < self.rest.get_num_events_processed(
                    event=event, node=node, name=function):
                break
            time.sleep(self.POLLING_INTERVAL_EVENTING)
            retry += 1
        if retry == self.MAX_RETRY_TIMER_EVENT:
            logger.info('Failed to get timer event for function: {}'.format(function))

    def wait_for_all_mutations_processed(
        self, host: str, bucket1: str, bucket2: str, bucket_replica: int
    ):
        logger.info('Waiting for mutations to be processed of eventing function')
        retry = 1
        while retry < self.MAX_RETRY_BOOTSTRAP:
            bucket1_items = self._get_num_items(host, bucket1, bucket_replica)
            bucket2_items = self._get_num_items(host, bucket2, bucket_replica)
            if bucket1_items == bucket2_items:
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
            events_processed = self.rest.get_num_events_processed(event="ALL",
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
            op = self.rest.get_apps_with_status(node, status)
            if function in op:
                break
            time.sleep(self.POLLING_INTERVAL_EVENTING)
            retry += 1
        if retry == self.MAX_RETRY_TIMER_EVENT:
            logger.info('Function {} failed to {}...!!!'.format(function, status))

    def monitor_eventing(self, eventing_nodes: list[str], bucket_name: str, bucket_replica: int):
        previous_docs = 0
        while True:
            events_remaining = 0
            for node in eventing_nodes:
                stats = self.rest.get_eventing_stats(node=node)
                for fun_stat in stats:
                    events_remaining += fun_stat.get("events_remaining", {}).get("dcp_backlog", 0)
            docs = self.get_num_items(host=self.cluster_spec.servers[0], bucket=bucket_name,
                                      bucket_replica=bucket_replica)
            logger.info(f"Events remaining: {events_remaining}")
            if events_remaining == 0 and docs == previous_docs:
                return
            previous_docs = docs
            time.sleep(self.POLLING_INTERVAL_EVENTING)

    def wait_for_fragmentation_stable(self, host: str, bucket: str,
                                      target_fragmentation: int = 50):
        while True:
            stats = self.rest.get_bucket_stats(host=host, bucket=bucket)
            fragmentation = int(stats['op']['samples'].get("couch_docs_fragmentation")[-1])
            logger.info("couch_docs_fragmentation: {}".format(fragmentation))
            if fragmentation <= target_fragmentation:
                break
            time.sleep(self.POLLING_INTERVAL)

    def monitor_sgimport_queues(
        self,
        host: str,
        num_buckets: int,
        expected_docs: int,
    ):
        logger.info('Monitoring SGImport items:')
        initial_items, start_time = self._wait_for_sg_import_start(host, num_buckets)
        items_in_range = expected_docs - initial_items
        time_taken = self._wait_for_sg_import_complete(host, num_buckets, expected_docs, start_time)
        return time_taken, items_in_range

    def get_import_count(self, host: str, num_buckets: int):
        stats = self.rest.get_sg_stats(host=host)
        import_count = 0
        if not self.cluster_spec.capella_infrastructure:
            if 'syncGateway_import' in stats.keys():
                import_count = int(stats['syncGateway_import']['import_count'])
            else:
                for count in range(1, num_buckets + 1):
                    db = f'db-{count}'
                    if 'shared_bucket_import' in (db_stats :=
                                                  stats['syncgateway']['per_db'][db]):
                        import_count += int(db_stats['shared_bucket_import']['import_count'])
        else:
            import_count = misc.parse_prometheus_stat(stats,
                                                      "sgw_shared_bucket_import_import_count")
        return import_count

    def _wait_for_sg_import_start(self, host: str, num_buckets: int):
        logger.info('Checking if import process started')

        start_time = time.time()
        import_docs = 0
        while True:
            time.sleep(self.POLLING_INTERVAL)
            import_docs = self.get_import_count(host, num_buckets)
            if import_docs >= 1:
                logger.info('importing docs has started')
                return import_docs, time.time()
            if time.time() - start_time > 300:
                raise Exception("timeout of 300 seconds exceeded")

    def _wait_for_sg_import_complete(
        self, host: str, num_buckets: int, expected_docs: int, start_time
    ):
        expected_docs = expected_docs
        start_time = start_time
        logger.info('Monitoring syncgateway import status :')

        imports = 0

        while True:
            time.sleep(self.POLLING_INTERVAL * 4)
            imports = self.get_import_count(host, num_buckets)
            logger.info('Docs imported: {}'.format(imports))
            if imports >= expected_docs:
                end_time = time.time()
                time_taken = end_time - start_time
                return time_taken
            if time.time() - start_time > 2400:
                raise Exception("timeout of 2400 seconds exceeded")

    def monitor_sgreplicate(
        self, host: str, num_buckets: int, expected_docs: int, replicate_id: str, version: int
    ):
        logger.info('Monitoring SGReplicate items:')
        initial_items, start_time = self._wait_for_sg_replicate_start(host, replicate_id, version)
        logger.info('initial items: {} start time: {}'.format(initial_items, start_time))
        items_in_range = expected_docs - initial_items
        logger.info('items in range: {}'.format(items_in_range))
        time_taken, final_items = self._wait_for_sg_replicate_complete(
            host, num_buckets, expected_docs, start_time, replicate_id, version
        )

        if replicate_id == 'sgr2_conflict_resolution':
            items_in_range = final_items - initial_items
        return time_taken, items_in_range

    def _wait_for_sg_replicate_start(self, host, replicate_id, version):
        logger.info('Checking if replicate process started')
        logger.info('host: {}'.format(host))
        replicate_docs = 0
        while True:
            if isinstance(host, list):
                for sg in range(len(host)):
                    stats = self.rest.get_sgreplicate_stats(host=host[sg],
                                                            version=version)
                    for stat in stats:
                        if stat.get('replication_id', '') == replicate_id[sg]:
                            if 'pull' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_read', 0))
                            elif 'push' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_written', 0))
            else:
                stats = self.rest.get_sgreplicate_stats(host=host,
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

    def _wait_for_sg_replicate_complete(
        self,
        host: str,
        num_buckets: int,
        expected_docs: int,
        start_time: float,
        replicate_id: str,
        version: int,
    ):
        logger.info('Monitoring syncgateway replicate status :')
        while True:
            replicate_docs = 0
            if isinstance(host, list):
                for sg in range(len(host)):
                    stats = self.rest.get_sgreplicate_stats(host=host[sg],
                                                            version=version)
                    for stat in stats:
                        if stat.get('replication_id', '') == replicate_id[sg]:
                            if 'pull' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_read', 0))
                            elif 'push' in replicate_id[sg]:
                                replicate_docs += int(stat.get('docs_written', 0))
            else:
                stats = self.rest.get_sgreplicate_stats(host=host, version=version)
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
                sg_stats = self.rest.get_sg_stats(host=host)
                local_count = 0
                remote_count = 0
                merge_count = 0
                num_docs_pushed = 0
                for count in range(1, num_buckets + 1):
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
        stats = self.rest.get_expvar_stats(host)
        if 'delta_sync' in stats['syncgateway']['per_db'][db].keys():
            return stats['syncgateway']['per_db'][db]
        else:
            logger.info('Delta Sync Disabled')
            return stats['syncgateway']['per_db']

    def deltasync_bytes_transfer(self, host: str, num_buckets: int, replication_mode: str):
        stats = self.rest.get_expvar_stats(host)
        bytes_transeferred = 0
        if replication_mode == "PUSH":
            replication_type = 'doc_writes_bytes_blip'
        else:
            replication_type = 'doc_reads_bytes_blip'
        for count in range(1, num_buckets + 1):
            db = f"db-{count}"
            bytes_transeferred += float(
                    stats['syncgateway']['per_db'][db]['database'][replication_type])
            return bytes_transeferred

    def get_sgw_push_count(self, host: str, num_buckets: int):
        sgw_stats = self.rest.get_sg_stats(host)
        push_count = 0
        if not self.cluster_spec.capella_infrastructure:
            for count in range(1, num_buckets + 1):
                db = f"db-{count}"
                push_count += \
                    int(sgw_stats['syncgateway']['per_db'][db]
                                 ['cbl_replication_push']['doc_push_count'])
        else:
            push_count = misc.parse_prometheus_stat(sgw_stats,
                                                    "sgw_replication_push_doc_push_count")
        return push_count

    def get_sgw_pull_count(self, host: str, num_buckets: int):
        pull_count = 0
        sgw_stats = self.rest.get_sg_stats(host)
        if not self.cluster_spec.capella_infrastructure:
            for count in range(1, num_buckets + 1):
                db = f"db-{count}"
                pull_count += \
                    int(sgw_stats['syncgateway']['per_db'][db]
                                 ['cbl_replication_pull']['rev_send_count'])
        else:
            pull_count = misc.parse_prometheus_stat(sgw_stats,
                                                    "sgw_replication_pull_rev_send_count")
        return pull_count

    def wait_sgw_push_start(self, hosts: list[str], num_buckets: int, initial_docs: int):
        retries = 0
        max_retries = 900
        while True:
            push_count = 0
            start_time = time.time()
            for host in hosts:
                push_count += self.get_sgw_push_count(host, num_buckets)
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

    def wait_sgw_pull_start(self, hosts: list[str], num_buckets: int, initial_docs: int):
        retries = 0
        max_retries = 900
        while True:
            pull_count = 0
            start_time = time.time()
            for host in hosts:
                pull_count += self.get_sgw_pull_count(host, num_buckets)
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

    def wait_sgw_push_docs(self, hosts: list[str], num_buckets: int, target_docs: int):
        retries = 0
        max_retries = 360
        last_push_count = 0
        while True:
            push_count = 0
            finished_time = time.time()
            for host in hosts:
                push_count += self.get_sgw_push_count(host, num_buckets)
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

    def wait_sgw_pull_docs(self, hosts: list[str], num_buckets: int, target_docs: int):
        retries = 0
        max_retries = 360
        last_pull_count = 0
        while True:
            pull_count = 0
            finished_time = time.time()
            for host in hosts:
                pull_count += self.get_sgw_pull_count(host, num_buckets)
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

    def wait_sgw_log_streaming_status(self, desired_status: str):
        retries = 0
        max_retries = 180
        logger.info('Waiting for \'{}\' log-streaming status'.format(desired_status))
        while True:
            current_status = self.rest.get_log_streaming_config().get('data', {}).get('status')
            if current_status == desired_status:
                return
            retries += 1
            if retries >= max_retries:
                raise Exception(
                    "Desired ({}) log-streaming status not reached after {}s. Status: {}".format(
                        desired_status, max_retries*self.POLLING_INTERVAL_SGW_LOGSTREAMING,
                        current_status)
                )
            time.sleep(self.POLLING_INTERVAL_SGW_LOGSTREAMING)

    def monitor_sgw_resync_status(self, host: str, db: str):
        """Monitor the status of resync and return when it is completed."""
        logger.info(f"Waiting for resync to complete for {db}")
        failed_retries = 0
        while True:
            resync_status = self.rest.sgw_get_resync_status(host, db)
            status = resync_status.get("status")
            docs_processed = resync_status.get("docs_processed")
            docs_changed = resync_status.get("docs_changed")
            logger.info(
                f"Resync status: {status}, docs processed: {docs_processed}, "
                f"docs changed: {docs_changed}"
            )
            if status == "completed":
                return
            elif status != "running":
                failed_retries += 1

            if failed_retries >= self.MAX_RETRY:
                raise Exception(f"Resync failed with status: {status}")

            time.sleep(self.POLLING_INTERVAL_SGW_RESYNC)

    def wait_for_snapshot_persistence(self, index_nodes: list[str]):
        """Execute additional steps for shard based rebalance."""
        logger.info("Checking and sleeping until all snapshots are ready")
        is_snapshot_ready = False
        retries = 0
        while not is_snapshot_ready:
            time.sleep(self.MONITORING_DELAY)
            is_snapshot_ready = all(self.rest.is_persistence_active(host=host) == "done" for host
                                    in index_nodes)
            retries += 1
            if retries >= self.MAX_RETRY_RECOVERY:
                raise Exception("Snapshot persistence failed. Not all nodes are done")

    def _wait_for_columnar_instance_state(
        self,
        instance_id: str,
        end_state: str,
        temp_state: str,
        state_func: Optional[Callable[[dict], Optional[str]]] = None,
        state_object: Optional[str] = None,
        poll_interval_secs: Optional[int] = None,
        timeout_secs: Optional[int] = None,
    ):
        state_func = state_func or (lambda info: info.get("data", {}).get("state"))
        state_object = state_object or f"columnar instance {instance_id}"
        poll_interval_secs = poll_interval_secs or self.POLLING_INTERVAL_ANALYTICS
        timeout_secs = timeout_secs or self.TIMEOUT

        logger.info(f'Waiting for {state_object} to be in state "{end_state}"...')
        t0 = time.time()
        while time.time() - t0 < timeout_secs:
            instance_info = self.rest.get_instance_info(instance_id)
            state = state_func(instance_info)
            logger.info(f"State of {state_object}: {state}")

            if state == end_state:
                logger.info(f'{state_object} has reached state "{end_state}"')
                return
            elif state != temp_state:
                logger.interrupt(f"Unexpected state for {state_object}: {state}")
                return

            time.sleep(poll_interval_secs)

        logger.interrupt(
            f"Timed out after {timeout_secs} seconds waiting for {state_object} "
            f'to reach state "{end_state}".'
        )

    def wait_for_columnar_instance_turn_off(
        self,
        instance_id: str,
        poll_interval_secs: Optional[int] = None,
        timeout_secs: Optional[int] = None,
    ):
        self._wait_for_columnar_instance_state(
            instance_id,
            "turned_off",
            "turning_off",
            poll_interval_secs=poll_interval_secs,
            timeout_secs=timeout_secs,
        )

    def wait_for_columnar_instance_turn_on(
        self,
        instance_id: str,
        poll_interval_secs: Optional[int] = None,
        timeout_secs: Optional[int] = None,
    ):
        self._wait_for_columnar_instance_state(
            instance_id,
            "healthy",
            "turning_on",
            poll_interval_secs=poll_interval_secs,
            timeout_secs=timeout_secs,
        )

    def wait_for_columnar_remote_link_ready(
        self,
        instance_id: str,
        link_name: str,
        poll_interval_secs: Optional[int] = None,
        timeout_secs: Optional[int] = None,
    ):
        def get_link_state(info: dict) -> Optional[str]:
            links = info.get("data", {}).get("config", {}).get("links", [])
            link = next((link for link in links if link.get("name") == link_name), None)
            if not link:
                logger.interrupt(
                    f'Link "{link_name}" not found for columnar instance {instance_id}.'
                )
            return link.get("status")

        self._wait_for_columnar_instance_state(
            instance_id,
            "ready",
            "pending",
            state_func=get_link_state,
            state_object=f'link "{link_name}" on columnar instance {instance_id}',
            poll_interval_secs=poll_interval_secs,
            timeout_secs=timeout_secs,
        )

    def monitor_server_upgrade(self):
        """Monitor CAO deployed server upgrade."""
        self.remote.wait_for_cluster_upgrade()
        logger.info(f"Final version: {self.remote.get_current_server_version()}")

    def wait_for_cluster_backup_complete(self, host: str, backup_id: str) -> int:
        """Wait until a specified backup complete or timeout and return its size."""
        retries = 0
        while True:
            is_complete, backup_data = self._get_cluster_backup(host, backup_id)
            if is_complete:
                return backup_data.get("databaseSize", 0)
            retries += 1
            if retries >= self.MAX_RETRY_RECOVERY:
                # If backup timeout, log all backups for debugging
                backups = self.rest.list_cluster_backups(host)
                logger.warn(f"All backups: {misc.pretty_dict(backups)}")
                raise Exception("Backup timeout")

            time.sleep(self.MONITORING_DELAY)

    def _get_cluster_backup(self, host: str, backup_id: str) -> tuple[bool, dict]:
        backups = self.rest.list_cluster_backups(host)
        if not backups:
            return False, {}

        for backup in backups:
            backup_data = backup.get("data", {})
            if backup_data.get("id") != backup_id:
                continue
            status = backup_data.get("progress", {}).get("status")
            complete = status == "complete"
            if complete:
                logger.info(f"Backup completed: {misc.pretty_dict(backup_data)}")
            else:
                logger.info(f"Backup in progress, status {status}")

            return complete, backup_data
        return False, {}

    def monitor_cluster_snapshot_restore(self, host):
        logger.info("Waiting for snapshot restore to start")

        is_running = True
        is_started = False
        last_progress = 0
        last_progress_time = time.time()
        while is_running or not is_started:
            time.sleep(self.MONITORING_DELAY * 3)  # 15sec delays

            is_running, progress = self.rest.get_job_status(host, job_type="snapshot_restore")
            if not progress:
                # Keep waiting until there is some progress
                continue

            is_started = True
            logger.info(f"Snapshot restore progress: {progress} %")

            if progress == last_progress:
                if time.time() - last_progress_time > self.REBALANCE_TIMEOUT:
                    logger.interrupt("Snapshot restore hung")
            else:
                last_progress = progress
                last_progress_time = time.time()

        logger.info("Snapshot restore completed")

    def wait_for_workflow_status(
        self, host: str, workflow_id: str, status: str = "running"
    ) -> Optional[str]:
        logger.info(f"Waiting for workflow {workflow_id} to reach status {status}")
        retries = 0
        while True:
            workflow_details = self.rest.get_workflow_details(host, workflow_id)
            workflow_status = workflow_details.get("status", "").lower()
            if workflow_status == status.lower():
                logger.info(f"Deployed workflow: {workflow_details}")
                eventing_apps = self.rest.get_eventing_apps(host).get("apps", []) or []
                logger.info(f"Eventing apps: {eventing_apps}")
                if len(eventing_apps) > 0:
                    return eventing_apps[0].get("name")
            retries += 1
            if retries % 60 == 0:
                logger.info(f"Workflow status: {workflow_status}")
            if retries >= self.MAX_RETRY:
                raise Exception(
                    f"Workflow {workflow_id} failed to reach the desired status {workflow_details}"
                )

            time.sleep(self.MONITORING_DELAY)

    def monitor_eventing_dcp_mutation(self, eventing_node: str, items: int):
        logger.info("Monitoring eventing backlog")
        dcp_mutation = 0
        while dcp_mutation != items:
            stats = self.rest.get_eventing_stats(eventing_node)
            for stat in stats:
                backlog = stat.get("events_remaining", {}).get("dcp_backlog", 0)
                dcp_mutation = stat.get("event_processing_stats", {}).get("dcp_mutation", 0)
                logger.info(f"{dcp_mutation=}, {backlog=}")
            time.sleep(self.MONITORING_DELAY * 12)

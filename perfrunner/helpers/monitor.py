import time

from logger import logger
from perfrunner.helpers import misc
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper


class Monitor(RestHelper):

    MAX_RETRY = 60
    MAX_RETRY_RECOVERY = 1200
    MONITORING_DELAY = 5
    POLLING_INTERVAL = 2
    POLLING_INTERVAL_INDEXING = 1
    POLLING_INTERVAL_MACHINE_UP = 10
    REBALANCE_TIMEOUT = 3600 * 2
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
        super().__init__(cluster_spec=cluster_spec)
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)

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
        self._wait_for_empty_queues(host, bucket, self.DCP_QUEUES,
                                    self.get_bucket_stats)

    def _wait_for_xdcr_to_start(self, host: str):
        is_running = False
        while not is_running:
            time.sleep(self.POLLING_INTERVAL)
            is_running, _ = self.get_task_status(host, task_type='xdcr')

    def monitor_xdcr_queues(self, host: str, bucket: str):
        logger.info('Monitoring XDCR queues: {}'.format(bucket))
        self._wait_for_xdcr_to_start(host)
        self._wait_for_empty_queues(host, bucket, self.XDCR_QUEUES,
                                    self.get_xdcr_stats)

    def monitor_sgimport_queues(self, host: str, expected_docs: int):
        logger.info('Monitoring SGImport items:')
        initial_items, start_time = self._wait_for_sg_import_start(host)
        items_in_range = expected_docs - initial_items
        time_taken = self._wait_for_sg_import_complete(host, expected_docs, start_time)
        return time_taken, items_in_range

    def _wait_for_sg_import_start(self, host: str):
        logger.info('Checking if import process started')

        import_docs = 0
        while True:
            time.sleep(self.POLLING_INTERVAL)
            stats = self.get_sg_stats(host=host)
            if 'syncGateway_import' in stats.keys():
                import_docs = int(stats['syncGateway_import']['import_count'])
            elif 'shared_bucket_import' in stats['syncgateway']['per_db']['db'].keys():
                import_docs = \
                    int(stats['syncgateway']['per_db']['db']['shared_bucket_import'
                                                             '']['import_count'])
            if import_docs >= 1:
                logger.info('importing docs has started')
                return import_docs, time.time()

    def _wait_for_sg_import_complete(self, host: str, expected_docs: int, start_time):
        expected_docs = expected_docs
        start_time = start_time
        logger.info('Monitoring syncgateway import status :')

        imports = 0

        while True:
            time.sleep(self.POLLING_INTERVAL)
            stats = self.get_sg_stats(host=host)
            if 'syncGateway_import' in stats.keys():
                imports = int(stats['syncGateway_import']['import_count'])
            elif 'shared_bucket_import' in stats['syncgateway']['per_db']['db'].keys():
                imports = \
                    int(stats['syncgateway']['per_db']['db']['shared_bucket_import'
                                                             '']['import_count'])
            logger.info('Docs imported: {}'.format(imports))
            if imports >= expected_docs:
                end_time = time.time()
                time_taken = end_time - start_time
                return time_taken

    def get_import_count(self, host: str):
        stats = self.get_sg_stats(host=host)
        import_count = 0
        if 'syncGateway_import' in stats.keys():
            import_count = int(stats['syncGateway_import']['import_count'])
        elif 'shared_bucket_import' in stats['syncgateway']['per_db']['db'].keys():
            import_count = \
                int(stats['syncgateway']['per_db']['db']['shared_bucket_import'
                                                         '']['import_count'])
        return import_count

    def monitor_sgreplicate(self, host: str, expected_docs: int, replicate_id: str, version: int):
        logger.info('Monitoring SGReplicate items:')
        initial_items, start_time = self._wait_for_sg_replicate_start(host, replicate_id, version)
        items_in_range = expected_docs - initial_items
        time_taken = self._wait_for_sg_replicate_complete(host,
                                                          expected_docs,
                                                          start_time,
                                                          replicate_id,
                                                          version)
        return time_taken, items_in_range

    def _wait_for_sg_replicate_start(self, host: str, replicate_id: str, version: int):
        logger.info('Checking if replicate process started')
        logger.info('host: {}'.format(host))
        replicate_docs = 0
        while True:
            stats = self.get_sgreplicate_stats(host=host,
                                               version=version)
            for stat in stats:
                if stat['replication_id'] == replicate_id:
                    replicate_docs = int(stat['docs_written'])
                    break

            if replicate_docs >= 1:
                logger.info('replicating docs has started')
                return replicate_docs, time.time()

            time.sleep(self.POLLING_INTERVAL)

    def _wait_for_sg_replicate_complete(self, host: str, expected_docs: int, start_time,
                                        replicate_id: str, version: int):
        expected_docs = expected_docs
        start_time = start_time
        logger.info('Monitoring syncgateway replicate status :')
        replicate_docs = 0
        while True:
            stats = self.get_sgreplicate_stats(host=host,
                                               version=version)
            for stat in stats:
                if stat['replication_id'] == replicate_id:
                    replicate_docs = int(stat['docs_written'])
                    break

            logger.info('Docs replicated: {}'.format(replicate_docs))
            if replicate_docs >= expected_docs:
                end_time = time.time()
                time_taken = end_time - start_time
                return time_taken

            time.sleep(self.POLLING_INTERVAL)

    def _get_num_items(self, host: str, bucket: str) -> bool:
        stats = self.get_bucket_stats(host=host, bucket=bucket)
        return stats['op']['samples'].get('curr_items')[-1]

    def monitor_num_items(self, host: str, bucket: str, num_items: int):
        logger.info('Checking the number of items in {}'.format(bucket))
        retries = 0
        while retries < self.MAX_RETRY:
            if self._get_num_items(host, bucket) == num_items:
                break
            time.sleep(self.POLLING_INTERVAL)
            retries += 1
        else:
            raise Exception('Mismatch in the number of items: {}'
                            .format(self._get_num_items(host, bucket)))

    def monitor_task(self, host, task_type):
        logger.info('Monitoring task: {}'.format(task_type))
        time.sleep(self.MONITORING_DELAY)

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
            if b'ep_warmup_state' in stats:
                state = stats[b'ep_warmup_state']
                if state == b'done':
                    return float(stats.get(b'ep_warmup_time', 0))
                else:
                    logger.info('Warmpup status: {}'.format(state))
                    time.sleep(self.POLLING_INTERVAL)
            else:
                    logger.info('No warmup stats are available, continue polling')
                    time.sleep(self.POLLING_INTERVAL)

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
            logger.interrupt('Analytcs node still not health: {}'.format(
                host
            ))

    def monitor_indexing(self, host):
        logger.info('Monitoring indexing progress')

        pending_docs = 1
        while pending_docs:
            time.sleep(self.POLLING_INTERVAL_INDEXING * 5)

            pending_docs = 0
            stats = self.get_gsi_stats(host)
            for metric, value in stats.items():
                if 'num_docs_queued' in metric or 'num_docs_pending' in metric:
                    pending_docs += value
            logger.info('Pending docs: {:,}'.format(pending_docs))

        logger.info('Indexing completed')

    def monitor_index_state(self, host, index_name):
        logger.info('Monitoring index state')

        statement = 'SELECT state FROM system:indexes WHERE name = "{}"'\
            .format(index_name)

        is_building = True
        while is_building:
            time.sleep(self.POLLING_INTERVAL)

            response = self.exec_n1ql_statement(host, statement)
            if response['status'] == 'success':
                for result in response['results']:
                    if result['state'] != 'online':
                        break
                else:
                    is_building = False
            else:
                logger.error(response['status'])

        logger.info('Index "{}" is online'.format(index_name))

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

    def wait_for_secindex_incr_build(self, index_nodes, bucket, indexes, numitems):
        # POLL until incremenal index build is complete
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

    def monitor_fts_indexing_queue(self, host: str, index: str):
        logger.info('Waiting for indexing to finish')

        count = 0
        while count < self.test_config.fts_settings.items:
            count = self.get_fts_doc_count(host, index)
            logger.info('FTS indexed documents: {:,}'.format(count))
            time.sleep(self.POLLING_INTERVAL)

    def monitor_fts_index_persistence(self, host: str, index: str):
        logger.info('Waiting for index to be persisted')

        key = '{}:{}:{}'.format(self.test_config.buckets[0],
                                index,
                                'num_recs_to_persist')
        pending_items = -1
        while pending_items:
            stats = self.get_fts_stats(host)
            pending_items = stats[key]
            logger.info('Records to persist: {:,}'.format(pending_items))
            time.sleep(self.POLLING_INTERVAL)

    def monitor_elastic_indexing_queue(self, host: str, index: str):
        logger.info(' Waiting for indexing to finish')

        count = 0
        while count < self.test_config.fts_settings.items:
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

    def wait_for_bootstrap(self, node: str, function: str):
        logger.info('Waiting for bootstrap of eventing function: {} '.format(function))
        retry = 1
        while retry < self.MAX_RETRY:
            if function in self.get_deployed_apps(node):
                break
            time.sleep(self.POLLING_INTERVAL)
            retry += 1
        if retry == self.MAX_RETRY:
            logger.info('Failed to bootstrap function: {}'.format(function))

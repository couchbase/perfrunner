import socket
import time

from logger import logger
from mc_bin_client.mc_bin_client import MemcachedClient

from perfrunner.helpers.rest import RestHelper


class Monitor(RestHelper):

    POLLING_INTERVAL = 5
    SOCKET_RETRY_INTERVAL = 2
    MAX_RETRY = 10

    DISK_QUEUE_METRICS = (
        'ep_queue_size',
        'ep_flusher_todo',
    )
    TAP_REPLICATION_METRICS = (
        'vb_replica_queue_size',
        'ep_tap_replica_queue_itemondisk',
        'ep_tap_rebalance_queue_backfillremaining',
        'ep_tap_replica_qlen',
    )

    def monitor_rebalance(self, host_port):
        logger.info('Monitoring rebalance status')
        is_running = True
        while is_running:
            time.sleep(self.POLLING_INTERVAL)

            is_running, progress = self.get_rebalance_status(host_port)

            if progress is not None:
                logger.info('Rebalance progress: {} %'.format(progress))
        logger.info('Rebalance successfully completed')

    def _wait_for_null_metric(self, host_port, bucket, metric):
        retry = 0
        while retry < self.MAX_RETRY:
            time.sleep(self.POLLING_INTERVAL)

            bucket_stats = self.get_bucket_stats(host_port, bucket)
            try:
                value = bucket_stats['op']['samples'][metric][-1]
            except KeyError:
                logger.warn('Got broken bucket stats')
                retry += 1
                continue
            else:
                retry = 0

            if value:
                logger.info('Current value of {}: {}'.format(metric, value))
            else:
                logger.info('{} reached 0'.format(metric))
                return
        logger.interrupt('Failed to get bucket stats after {} attempts'.format(
            self.MAX_RETRY
        ))

    def monitor_disk_queue(self, target):
        logger.info('Monitoring disk queue: {}'.format(target.bucket))
        for metric in self.DISK_QUEUE_METRICS:
            self._wait_for_null_metric(target.node, target.bucket, metric)

    def monitor_tap_replication(self, target):
        logger.info('Monitoring TAP replication: {}'.format(target.bucket))
        for metric in self.TAP_REPLICATION_METRICS:
            self._wait_for_null_metric(target.node, target.bucket, metric)

    def monitor_xdcr_replication(self, target):
        logger.info('Monitoring XDCR replication: {}'.format(target.bucket))
        metric = 'replication_changes_left'
        self._wait_for_null_metric(target.node, target.bucket, metric)

    def monitor_bucket_fragmentation(self, target):
        logger.info('Monitoring bucket fragmentation: {}'.format(target.bucket))
        metric = 'couch_docs_fragmentation'
        self._wait_for_null_metric(target.node, target.bucket, metric)

    def monitor_index_fragmentation(self, target):
        logger.info('Monitoring index fragmentation: {}'.format(target.bucket))
        metric = 'couch_views_fragmentation'
        self._wait_for_null_metric(target.node, target.bucket, metric)

    def monitor_task(self, target, task_type):
        logger.info('Monitoring task: {}'.format(task_type))

        while True:
            time.sleep(self.POLLING_INTERVAL)

            tasks = [task for task in self.get_tasks(target.node)
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

    def monitor_warmup(self, target):
        host = target.node.split(':')[0]

        logger.info('Monitoring warmup status')

        while True:
            try:
                mc = MemcachedClient(host=host, port=11210)
                mc.sasl_auth_plain(user=target.bucket, password=target.password)
                stats = mc.stats('warmup')
            except (EOFError, socket.error):
                time.sleep(self.SOCKET_RETRY_INTERVAL)
            else:
                state = stats['ep_warmup_state']
                if state == 'done':
                    return stats['ep_warmup_time']
                else:
                    logger.info('Warmpup status: {}'.format(state))
                    time.sleep(self.POLLING_INTERVAL)

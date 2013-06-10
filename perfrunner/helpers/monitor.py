import time

from logger import logger

from perfrunner.helpers.rest import RestHelper


class Monitor(RestHelper):

    POLLING_INTERVAL = 10
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
                logger.info('Rebalance progress: {0} %'.format(progress))
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
                logger.info('Current value of {0}: {1}'.format(metric, value))
            else:
                logger.info('{0} reached 0'.format(metric))
                return
        logger.interrupt('Failed to get bucket stats after {0} attempts'.format(
            self.MAX_RETRY
        ))

    def monitor_disk_queue(self, target):
        logger.info('Monitoring disk queue: {0}'.format(target.bucket))
        for metric in self.DISK_QUEUE_METRICS:
            self._wait_for_null_metric(target.node, target.bucket, metric)

    def monitor_tap_replication(self, target):
        logger.info('Monitoring TAP replication: {0}'.format(target.bucket))
        for metric in self.TAP_REPLICATION_METRICS:
            self._wait_for_null_metric(target.node, target.bucket, metric)

    def monitor_xdcr_replication(self, target):
        logger.info('Monitoring XDCR replication: {0}'.format(target.bucket))
        metric = 'replication_changes_left'
        self._wait_for_null_metric(target.node, target.bucket, metric)

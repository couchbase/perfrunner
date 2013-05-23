import time

from logger import logger

from perfrunner.helpers.rest import RestHelper


class Monitor(RestHelper):

    DELAY = 10

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
            time.sleep(self.DELAY)

            is_running, progress = self.get_rebalance_status(host_port)

            if progress is not None:
                logger.info('Rebalance progress: {0} %'.format(progress))
        logger.info('Rebalance successfully completed')

    def _monitor_metric_value(self, host_port, bucket, metric):
        value = True
        while value:
            time.sleep(self.DELAY)

            bucket_stats = self.get_bucket_stats(host_port, bucket)
            value = bucket_stats['op']['samples'][metric][-1]

            logger.info('Current value of {0}: {1}'.format(metric, value))
        logger.info('{0} reached 0'.format(metric))

    def monitor_disk_queue(self, target):
        logger.info('Monitoring disk queue: {0}'.format(target.bucket))
        for metric in self.DISK_QUEUE_METRICS:
            self._monitor_metric_value(target.node, target.bucket, metric)

    def monitor_tap_replication(self, target):
        logger.info('Monitoring TAP replication: {0}'.format(target.bucket))
        for metric in self.TAP_REPLICATION_METRICS:
            self._monitor_metric_value(target.node, target.bucket, metric)

    def monitor_xdcr_replication(self, target):
        logger.info('Monitoring XDCR replication: {0}'.format(target.bucket))
        changes_left = True
        while changes_left:
            time.sleep(self.DELAY)

            stats = self.get_bucket_stats(target.node, target.bucket)
            changes_left = stats['op']['samples']['replication_changes_left'][-1]

            logger.info('Changes left: {1}'.format(changes_left))
        logger.info('Replication finished')

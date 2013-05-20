import time

from logger import logger

from perfrunner.helpers.rest import RestHelper


class Monitor(RestHelper):

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

    def __init__(self, cluster_spec, test_config):
        super(RestHelper, self).__init__(cluster_spec, test_config)
        self.test_config = test_config

    def monitor_rebalance(self, host_port):
        logger.info('Monitoring rebalance status')
        while True:
            is_running, progress = self.get_rebalance_status(host_port)
            if is_running:
                logger.info('Rebalance progress: {0} %'.format(progress))
                time.sleep(10)
            else:
                break
        logger.info('Rebalance successfully completed')

    def _monitor_metric_value(self, host_port, bucket, metric):
        while True:
            bucket_stats = self.get_bucket_stats(host_port, bucket)
            curr_value = bucket_stats['op']['samples'][metric][-1]
            if curr_value:
                logger.info('Current value of {0}: {1}'.format(metric,
                                                               curr_value))
                time.sleep(10)
            else:
                logger.info('{0} reached 0'.format(metric))
                break

    def monitor_disk_queue(self, host_port, bucket):
        logger.info('Monitoring disk queue: {0}'.format(bucket))
        for metric in self.DISK_QUEUE_METRICS:
            self._monitor_metric_value(host_port, bucket, metric)

    def monitor_tap_replication(self, host_port, bucket):
        logger.info('Monitoring TAP replication: {0}'.format(bucket))
        for metric in self.TAP_REPLICATION_METRICS:
            self._monitor_metric_value(host_port, bucket, metric)

import time

from logger import logger

from perfrunner.helpers.rest import RestHelper


class Monitor(RestHelper):

    DISK_QUEUE_METRICS = ('ep_queue_size', 'ep_flusher_todo')

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

    def monitor_disk_queue(self, host_port, bucket):
        logger.info('Monitoring disk queue: {0}'.format(bucket))
        for metric in self.DISK_QUEUE_METRICS:
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

import time

from logger import logger

from perfrunner.helpers.rest import RestHelper


class Monitor(RestHelper):

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

import socket
import time

from decorator import decorator
from logger import logger
from mc_bin_client.mc_bin_client import (MemcachedClient, MemcachedError,
                                         memcacheConstants)


SOCKET_RETRY_INTERVAL = 2


@decorator
def retry(method, *args, **kwargs):
    while True:
        try:
            return method(*args, **kwargs)
        except (EOFError, socket.error, MemcachedError):
            time.sleep(SOCKET_RETRY_INTERVAL)


class MemcachedHelper(object):

    @staticmethod
    def calc_watermark(val, mem_quota):
        val = int(val) / 100.0  # string -> ratio
        mem_quota *= 1024 ** 2  # Mb -> bytes
        return str(int(val * mem_quota))

    def __init__(self, test_config):
        self.password = test_config.bucket.password
        self.test_config = test_config

    def set_flusher_param(self, host, port, bucket, key, value):
        logger.info('Changing flusher params: {}={}'.format(key, value))
        mc = MemcachedClient(host=host, port=port)
        mc.sasl_auth_plain(user=bucket, password=self.password)
        mc.set_param(key, value, memcacheConstants.ENGINE_PARAM_FLUSH)

    @retry
    def get_stats(self, host, port, bucket, stats):
        proxyPort = self.test_config.bucket.proxyPort
        if proxyPort is None:
            mc = MemcachedClient(host=host, port=port)
            mc.sasl_auth_plain(user=bucket, password=self.password)
        else:
            mc = MemcachedClient(host=host, port=proxyPort)
        return mc.stats(stats)

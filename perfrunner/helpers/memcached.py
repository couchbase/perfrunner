import socket
import time

from mc_bin_client.mc_bin_client import MemcachedClient

from perfrunner.settings import TestConfig

SOCKET_RETRY_INTERVAL = 2
MAX_RETRY = 600


class MemcachedHelper:

    def __init__(self, test_config: TestConfig):
        self.password = test_config.bucket.password
        if test_config.cluster.ipv6:
            self.family = socket.AF_INET6
        else:
            self.family = socket.AF_INET

    def get_stats(self, host: str, port: int, bucket: str, stats: str = '') -> dict:
        retries = 0
        while True:
            try:
                mc = MemcachedClient(host=host, port=port, family=self.family)
                mc.enable_xerror()
                mc.hello("mc")
                mc.sasl_auth_plain(user=bucket, password=self.password)
                return mc.stats(stats)
            except Exception:
                if retries < MAX_RETRY:
                    retries += 1
                    time.sleep(SOCKET_RETRY_INTERVAL)
                else:
                    raise

    def reset_stats(self, host: str, port: int, bucket: str):
        self.get_stats(host, port, bucket, 'reset')

import socket
import time

from mc_bin_client.mc_bin_client import MemcachedClient

from perfrunner.settings import ClusterSpec, TestConfig

SOCKET_RETRY_INTERVAL = 2
MAX_RETRY = 600


class MemcachedHelper:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        self.username, self.password = cluster_spec.rest_credentials
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
                mc.sasl_auth_plain(user=self.username, password=self.password)
                mc.bucket_select(bucket)
                return mc.stats(stats)
            except Exception:
                if retries < MAX_RETRY:
                    retries += 1
                    time.sleep(SOCKET_RETRY_INTERVAL)
                else:
                    raise

    def reset_stats(self, host: str, port: int, bucket: str):
        self.get_stats(host, port, bucket, 'reset')

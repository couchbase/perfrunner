from time import sleep, time

from cbagent.collectors import Latency
from cbagent.collectors.libstats.pool import Pool
from logger import logger
from perfrunner.helpers.misc import uhex


class XdcrLag(Latency):

    COLLECTOR = "xdcr_lag"

    METRICS = ("xdcr_lag", )

    INITIAL_POLLING_INTERVAL = 0.001  # 1 ms

    TIMEOUT = 600  # 10 minutes

    MAX_SAMPLING_INTERVAL = 0.25  # 250 ms

    def __init__(self, settings):
        super().__init__(settings)

        self.interval = 0

        self.pools = []
        for bucket in self.get_buckets():
            src_pool = Pool(
                bucket=bucket,
                host=settings.master_node,
                username=bucket,
                password=settings.bucket_password,
                quiet=True,
            )
            dst_pool = Pool(
                bucket=bucket,
                host=settings.dest_master_node,
                username=bucket,
                password=settings.bucket_password,
                quiet=True,
                unlock_gil=False,
            )
            self.pools.append((bucket, src_pool, dst_pool))

    def _measure_lags(self, src_pool, dst_pool):
        key = "xdcr_{}".format(uhex())

        src_client = src_pool.get_client()
        dst_client = dst_pool.get_client()

        polling_interval = self.INITIAL_POLLING_INTERVAL

        src_client.set(key, key)

        t0 = time()
        while time() - t0 < self.TIMEOUT:
            if dst_client.get(key).value:
                break
            sleep(polling_interval)
            polling_interval *= 1.1  # increase interval by 10%
        else:
            logger.warn('XDCR sampling timed out after {} seconds'
                        .format(self.TIMEOUT))
        t1 = time()

        src_client.delete(key)

        src_pool.release_client(src_client)
        dst_pool.release_client(dst_client)

        return {"xdcr_lag": (t1 - t0) * 1000}  # s -> ms

    def sample(self):
        t0 = time()
        for bucket, src_pool, dst_pool in self.pools:
            lags = self._measure_lags(src_pool, dst_pool)
            self.store.append(lags,
                              cluster=self.cluster,
                              bucket=bucket,
                              collector=self.COLLECTOR)
        total_sampling_time = time() - t0

        sleep(max(0, self.MAX_SAMPLING_INTERVAL - total_sampling_time))

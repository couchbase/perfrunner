from threading import Thread
from time import sleep, time
from uuid import uuid4

from couchbase.user_constants import OBS_NOTFOUND, OBS_PERSISTED
from decorator import decorator
from logger import logger

from cbagent.collectors import Latency
from cbagent.collectors.libstats.pool import Pool

uhex = lambda: uuid4().hex


@decorator
def timeit(method, *args, **kargs):
    t0 = time()
    method(*args, **kargs)
    t1 = time()
    return t0, t1


class ObserveLatency(Latency):

    COLLECTOR = "observe"

    METRICS = ("latency_observe", )

    NUM_THREADS = 10

    MAX_POLLING_INTERVAL = 1

    INITIAL_REQUEST_INTERVAL = 0.01

    MAX_REQUEST_INTERVAL = 2

    def __init__(self, settings):
        super(Latency, self).__init__(settings)

        self.pools = self.init_pool(settings)

        self.mode = getattr(settings, "observe", "persist")  # replicate | index

    def init_pool(self, settings):
        pools = []
        for bucket in self.get_buckets():
            pool = Pool(
                bucket=bucket,
                host=settings.master_node,
                username=bucket,
                password=settings.bucket_password,
                quiet=True,
            )
            pools.append((bucket, pool))
        return pools

    @timeit
    def _wait_until_persisted(self, client, key):
        req_interval = self.INITIAL_REQUEST_INTERVAL
        while not [v for v in client.observe(key).value
                   if v.flags == OBS_PERSISTED]:
            sleep(req_interval)
            req_interval = min(req_interval * 1.5, self.MAX_REQUEST_INTERVAL)

    @timeit
    def _wait_until_replicated(self, client, key):
        found = lambda client: [
            v for v in client.observe(key).value if v.flags != OBS_NOTFOUND
        ]
        while len(found(client)) != 2:
            sleep(0.002)

    @timeit
    def _wait_until_indexed(self, client, key):
        rows = None
        while not rows:
            rows = tuple(client.query("A", "id_by_city", key=key))

    @timeit
    def _wait_until_secondary_indexed(self, client, key):
        rows = None
        query = "select city from `bucket-1` where city='{}'".format(key)
        while not rows:
            rows = tuple(client.n1ql_query(query))

    def _measure_lags(self, pool):
        client = pool.get_client()

        key = uhex()
        client.set(key, {"city": key})

        if self.mode == "persist":
            t0, t1 = self._wait_until_persisted(client, key)
        elif self.mode == "replicate":
            t0, t1 = self._wait_until_replicated(client, key)
        elif self.mode == "secondary_index":
            t0, t1 = self._wait_until_secondary_indexed(client, key)
        else:
            t0, t1 = self._wait_until_indexed(client, key)
        latency = (t1 - t0) * 1000  # s -> ms
        sleep_time = max(0, self.MAX_POLLING_INTERVAL - (t1 - t0))

        client.delete(key)
        pool.release_client(client)
        return {"latency_observe": latency}, sleep_time

    def sample(self):
        while True:
            try:
                for bucket, pool in self.pools:
                    stats, sleep_time = self._measure_lags(pool)
                    self.store.append(stats,
                                      cluster=self.cluster,
                                      bucket=bucket,
                                      collector=self.COLLECTOR)
                    sleep(sleep_time)
            except Exception as e:
                logger.warn(e)

    def collect(self):
        threads = [Thread(target=self.sample) for _ in range(self.NUM_THREADS)]
        map(lambda t: t.start(), threads)
        map(lambda t: t.join(), threads)

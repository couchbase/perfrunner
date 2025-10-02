from threading import Thread
from time import sleep, time

import numpy
import pkg_resources
from couchbase.bucket import Bucket
from decorator import decorator

from cbagent.collectors import Latency
from cbagent.collectors.libstats.pool import Pool
from logger import logger
from perfrunner.helpers.misc import uhex
from spring.docgen import Document, Key

sdk_major_version = int(pkg_resources.get_distribution("couchbase").version[0])

if sdk_major_version == 2:
    from couchbase.n1ql import N1QLQuery
elif sdk_major_version >= 3:
    from couchbase.n1ql import N1QLRequest as N1QLQuery


@decorator
def timeit(method, *args, **kargs):
    t0 = time()
    method(*args, **kargs)
    t1 = time()
    return t0, t1


class ObserveIndexLatency(Latency):

    COLLECTOR = "observe"

    METRICS = "latency_observe",

    NUM_THREADS = 10

    MAX_POLLING_INTERVAL = 1

    INITIAL_REQUEST_INTERVAL = 0.01

    MAX_REQUEST_INTERVAL = 2

    def __init__(self, settings):
        super().__init__(settings)
        self.pools = []

    def _init_pool(self):
        for bucket in self.get_buckets():
            pool = Pool(
                bucket=bucket,
                host=self.master_node,
                username=bucket,
                password=self.auth[1],
                quiet=True,
            )
            self.pools.append((bucket, pool))

    @timeit
    def _wait_until_indexed(self, client, key):
        rows = None
        while not rows:
            rows = tuple(client.query("A", "id_by_city", key=key))

    def _create_doc(self, pool):
        client = pool.get_client()
        key = uhex()
        client.set(key, {"city": key})
        return client, key

    def _post_wait_operations(self, end_time, start_time, key, client, pool):
        latency = (end_time - start_time) * 1000  # s -> ms
        sleep_time = max(0, self.MAX_POLLING_INTERVAL - (end_time - start_time))

        client.delete(key)
        pool.release_client(client)
        return {"latency_observe": latency}, sleep_time

    def _measure_lags(self, pool):
        client, key = self._create_doc(pool)
        t0, t1 = self._wait_until_indexed(client, key)
        return self._post_wait_operations(end_time=t1, start_time=t0, key=key,
                                          client=client, pool=pool)

    def sample(self):
        while True:
            try:
                for bucket, pool in self.pools:
                    stats, sleep_time = self._measure_lags(pool)
                    self.store.append(
                        stats, cluster=self.cluster, bucket=bucket, collector=self.COLLECTOR
                    )
                    sleep(sleep_time)
            except Exception as e:
                logger.warn(e)

    def collect(self):
        threads = [Thread(target=self.sample) for _ in range(self.NUM_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()


class ObserveSecondaryIndexLatency(ObserveIndexLatency):

    @timeit
    def _wait_until_secondary_indexed(self, key, cb, query):
        row = None
        query.set_option("$c", key)
        while not row:
            row = cb.n1ql_query(query).get_single_result()

    @staticmethod
    def create_alt_mail_doc(pool):
        client = pool.get_client()

        key = uhex()
        client.set(key, {"alt_email": key})
        return client, key

    def _measure_lags(self, pool, cb=None, query=None):
        client, key = self.create_alt_mail_doc(pool)

        t0, t1 = self._wait_until_secondary_indexed(key, cb, query)

        return self._post_wait_operations(end_time=t1, start_time=t0, key=key,
                                          client=client, pool=pool)

    def sample(self):
        connection_string = 'couchbase://{}/{}?password={}'.format(
            self.master_node, self.buckets[0], self.auth[1])
        cb = Bucket(connection_string)
        query = N1QLQuery("select alt_email from `bucket-1` where alt_email=$c", c="abc")
        query.adhoc = False

        while True:
            try:
                for bucket, pool in self.pools:
                    stats, sleep_time = self._measure_lags(pool, cb=cb, query=query)
                    self.store.append(
                        stats, cluster=self.cluster, bucket=bucket, collector=self.COLLECTOR
                    )
                    sleep(sleep_time)
            except Exception as e:
                logger.warn(e)


class DurabilityLatency(ObserveIndexLatency, Latency):

    COLLECTOR = "durability"

    METRICS = "latency_replicate_to", "latency_persist_to"

    DURABILITY_TIMEOUT = 120

    def __init__(self, settings, workload):
        super().__init__(settings)
        self.new_docs = Document(workload.size)
        self.pools = []

    @staticmethod
    def gen_key() -> Key:
        return Key(number=numpy.random.random_integers(0, 10 ** 9),
                   prefix='endure',
                   fmtr='hex')

    def endure(self, pool, metric):
        client = pool.get_client()

        key = self.gen_key()
        doc = self.new_docs.next(key)

        t0 = time()

        client.upsert(key.string, doc)
        if metric == "latency_persist_to":
            client.endure(key.string, persist_to=1, replicate_to=0, interval=0.010,
                          timeout=120)
        else:
            client.endure(key.string, persist_to=0, replicate_to=1, interval=0.001)

        latency = 1000 * (time() - t0)  # Latency in ms

        sleep_time = max(0, self.MAX_POLLING_INTERVAL - latency)

        client.delete(key.string)
        pool.release_client(client)
        return {metric: latency}, sleep_time

    def sample(self):
        while True:
            for bucket, pool in self.pools:
                for metric in self.METRICS:
                    try:
                        stats, sleep_time = self.endure(pool, metric)
                        self.store.append(
                            stats, cluster=self.cluster, bucket=bucket, collector=self.COLLECTOR
                        )
                        sleep(sleep_time)
                    except Exception as e:
                        logger.warn(e)

    def collect(self):
        ObserveIndexLatency.collect(self)

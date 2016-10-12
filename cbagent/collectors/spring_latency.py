from time import sleep, time
from uuid import uuid4

from logger import logger

from cbagent.collectors import Latency, ObserveLatency
from spring.cbgen import CBGen, SubDocGen
from spring.docgen import (
    Document,
    ExistingKey,
    KeyForRemoval,
    NestedDocument,
    NewKey,
)
from spring.querygen import ViewQueryGen, ViewQueryGenByType

uhex = lambda: uuid4().hex


class SpringLatency(Latency):

    COLLECTOR = "spring_latency"

    METRICS = "latency_set", "latency_get"

    def __init__(self, settings, workload, prefix=None):
        super(Latency, self).__init__(settings)

        self.clients = []
        for bucket in self.get_buckets():
            client = CBGen(bucket=bucket, host=settings.master_node,
                           username=bucket, password=settings.bucket_password)
            self.clients.append((bucket, client))

        self.existing_keys = ExistingKey(workload.working_set,
                                         workload.working_set_access,
                                         prefix=prefix)
        self.new_keys = NewKey(prefix=prefix, expiration=workload.expiration)
        self.keys_for_removal = KeyForRemoval(prefix=prefix)

        if not hasattr(workload, 'doc_gen') or workload.doc_gen == 'old':
            self.new_docs = Document(workload.size)
        elif workload.doc_gen == 'new':
            self.new_docs = NestedDocument(workload.size)
        self.items = workload.items

    def measure(self, client, metric, bucket):
        key = self.existing_keys.next(curr_items=self.items, curr_deletes=0)
        doc = self.new_docs.next(key)

        t0 = time()
        if metric == "latency_set":
            client.create(key, doc)
        elif metric == "latency_get":
            client.read(key)
        elif metric == "latency_cas":
            client.cas(key, doc)
        return 1000 * (time() - t0)  # Latency in ms

    def sample(self):
        for bucket, client in self.clients:
            samples = {}
            for metric in self.METRICS:
                samples[metric] = self.measure(client, metric, bucket)
            self.store.append(samples, cluster=self.cluster,
                              bucket=bucket, collector=self.COLLECTOR)


class SpringSubdocLatency(SpringLatency):

    COLLECTOR = "spring_subdoc_latency"

    METRICS = "latency_set", "latency_get"

    def __init__(self, settings, workload, prefix=None):
        super(SpringSubdocLatency, self).__init__(settings, workload, prefix)

        self.clients = []
        self.ws = workload
        for bucket in self.get_buckets():
            client = SubDocGen(bucket=bucket,
                               host=settings.master_node,
                               username=bucket,
                               password=settings.bucket_password)
            self.clients.append((bucket, client))

    def measure(self, client, metric, bucket):
        key = self.existing_keys.next(curr_items=self.items, curr_deletes=0)

        t0 = time()
        if metric == "latency_set":
            client.update(key, self.ws.subdoc_fields, self.ws.size)
        elif metric == "latency_get":
            client.read(key, self.ws.subdoc_fields)
        if metric == "latency_remove":
            client.delete(key, self.ws.subdoc_delete_fields)
        elif metric == "latency_counter":
            client.counter(key, self.ws.subdoc_counter_fields)
        return 1000 * (time() - t0)  # Latency in ms


class SpringCasLatency(SpringLatency):

    METRICS = "latency_set", "latency_get", "latency_cas"


class SpringQueryLatency(SpringLatency):

    COLLECTOR = "spring_query_latency"

    METRICS = "latency_query",

    def __init__(self, settings, workload, ddocs, params, index_type,
                 prefix=None):
        super(SpringQueryLatency, self).__init__(settings, workload, prefix)

        if index_type is None:
            self.new_queries = ViewQueryGen(ddocs, params)
        else:
            self.new_queries = ViewQueryGenByType(index_type, params)

    def measure(self, client, metric, bucket):
        key = self.existing_keys.next(curr_items=self.items, curr_deletes=0)
        doc = self.new_docs.next(key)
        ddoc_name, view_name, query = self.new_queries.next(doc)

        _, latency = client.query(ddoc_name, view_name, query=query)
        return 1000 * latency  # s -> ms


class DurabilityLatency(ObserveLatency, SpringLatency):

    COLLECTOR = "durability"

    METRICS = "latency_replicate_to", "latency_persist_to"

    DURABILITY_TIMEOUT = 120

    def __init__(self, settings, workload, prefix=None):
        SpringLatency.__init__(self, settings, workload, prefix=prefix)

        self.pools = self.init_pool(settings)

    def endure(self, pool, metric):
        client = pool.get_client()

        key = uhex()
        doc = self.new_docs.next(key)

        t0 = time()

        client.upsert(key, doc)
        if metric == "latency_persist_to":
            client.endure(key, persist_to=1, replicate_to=0, interval=0.010,
                          timeout=120)
        else:
            client.endure(key, persist_to=0, replicate_to=1, interval=0.001)

        latency = 1000 * (time() - t0)  # Latency in ms

        sleep_time = max(0, self.MAX_POLLING_INTERVAL - latency)

        client.delete(key)
        pool.release_client(client)
        return {metric: latency}, sleep_time

    def sample(self):
        while True:
            for bucket, pool in self.pools:
                for metric in self.METRICS:
                    try:
                        stats, sleep_time = self.endure(pool, metric)
                        self.store.append(stats,
                                          cluster=self.cluster,
                                          bucket=bucket,
                                          collector=self.COLLECTOR)
                        sleep(sleep_time)
                    except Exception as e:
                        logger.warn(e)

    def collect(self):
        ObserveLatency.collect(self)

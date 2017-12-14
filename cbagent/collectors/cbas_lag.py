from time import sleep, time

import numpy

from cbagent.collectors.latency import Latency
from cbagent.collectors.xdcr_lag import new_client
from logger import logger
from perfrunner.helpers import rest
from spring.docgen import Key


class CBASBigfunMetricInfo:

    """CBASBigfunMetricInfo is the class for cbas_lab collector.

    It is used to get cbas_lag metric related information
    including how to create the test document (including key)
    and the tables used for create test documents
    There will be more such classes created later on
    for CBAS tests for other benchmarks (benchmarks other than
    Bigfun)
    """

    def next_metric_key(self) -> Key:
        return Key(number=numpy.random.random_integers(0, 10 ** 9),
                   prefix='cbas',
                   fmtr='hex')

    def next_metric_doc(self, key) -> dict:
        return {'chirpid': key.string}

    def metric_table(self, bucket):
        return 'ChirpMessages{bucket}'.format(bucket=bucket)


class CBASLag(Latency):

    """CBASLag class is collector for cbas_lab metric.

    It keeps sending test documents to Couchbase data node
    run query against CBAS node to measure the lag of CBAS
    test documents (ms from when it's injested in CB data
    node to when the test document appears in CBAS node).
    Complicated objects: self.rest, self.clients,
    self.cbas_metric are not created during collectors
    initialization in perfrunner process, the are created
    within collector's own process within sample function
    This is because, when i try to create these objects
    with in __init__ (called within perfrunner process)
    these objects will not work properly on MAC OS when
    they are called by collect process within sample
    method (but it works on a Linux OS thouse). After
    moving the creation of these objects to sample method,
    they are working as expected on MAC OS as well.
    """

    COLLECTOR = "cbas_lag"

    METRICS = "cbas_lag",

    INITIAL_POLLING_INTERVAL = 0.01  # 10 ms

    TIMEOUT = 600  # 10 minutes for default cbas lag timeouts.

    MAX_SAMPLING_INTERVAL = 0.25  # 250 ms

    def __init__(self, settings, cbas_test):
        super().__init__(settings)

        self.interval = self.MAX_SAMPLING_INTERVAL
        self.cbas_test = cbas_test
        self.TIMEOUT = cbas_test.test_config.cbas_settings.cbas_lag_timeout
        self.settings = settings
        self.rest = None
        self.cbas_metric = None
        self.clients = None

    def init_by_sample(self):
        if self.rest is None:
            self.rest = rest.RestHelper(self.cbas_test.cluster_spec)
        if self.cbas_metric is None:
            kclass = globals()[(self.cbas_test.CBASMETRIC_CLASSNAME)]
            self.cbas_metric = kclass()
        if self.clients is None:
            self.clients = []
            for bucket in self.get_buckets():
                src_client = new_client(host=self.settings.master_node,
                                        bucket=bucket,
                                        password=self.settings.bucket_password,
                                        timeout=self.TIMEOUT)
                self.clients.append((bucket, src_client, self.settings.cbas_node))

    def measure(self, bucket, src_client, cbas_node):
        key = self.cbas_metric.next_metric_key()
        doc = self.cbas_metric.next_metric_doc(key)

        src_client.upsert(key.string, doc)

        polling_interval = self.INITIAL_POLLING_INTERVAL

        t0 = time()
        while time() - t0 < self.TIMEOUT:
            result = self.rest.run_analytics_query(
                cbas_node,
                'select * from `{metric_table}` t where meta(t).id = "{key}";'.format(
                    metric_table=self.cbas_metric.metric_table(bucket),
                    key=key.string))
            if len(result.get('results')) == 1:
                break
            sleep(polling_interval)
            polling_interval *= 1.05  # increase interval by 5%
        else:
            logger.warn('CBAS sampling timed out after {} seconds when querying for key {}'
                        .format(self.TIMEOUT, key.string))
        t1 = time()
        if t1 - t0 < self.TIMEOUT:
            src_client.remove(key.string, quiet=True)
        else:
            logger.warn('CBAS lag {} over {}'
                        .format(t1 - t0, self.TIMEOUT))
        return {'cbas_lag': (t1 - t0) * 1000}  # s -> ms

    def sample(self):
        self.init_by_sample()
        for bucket, src_client, cbas_node in self.clients:
            lags = self.measure(bucket, src_client, cbas_node)
            self.store.append(lags,
                              cluster=self.cluster,
                              bucket=bucket,
                              collector=self.COLLECTOR)

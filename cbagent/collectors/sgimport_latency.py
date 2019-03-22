import requests
import json

from time import sleep, time

from couchbase.bucket import Bucket

from cbagent.collectors import Latency, Collector
from logger import logger
from perfrunner.helpers.misc import uhex
from spring.docgen import Document
from cbagent.metadata_client import MetadataClient
from cbagent.stores import PerfStore
from perfrunner.settings import (
    ClusterSpec,
    PhaseSettings,
    TargetIterator,
    TestConfig,
)

def new_client(host, bucket, password, timeout):
    connection_string = 'couchbase://{}/{}?password={}'
    connection_string = connection_string.format(host,
                                                 bucket,
                                                 password)
    client = Bucket(connection_string=connection_string)
    client.timeout = timeout
    return client


class SGImport_latency(Collector):

    COLLECTOR = "sgimport_latency"

    METRICS = "sgimport_latency",

    INITIAL_POLLING_INTERVAL = 0.001  # 1 ms

    TIMEOUT = 600  # 10 minutes

    MAX_SAMPLING_INTERVAL = 0.25  # 250 ms

    def __init__(self, settings,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig
                 ):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.mc = MetadataClient(settings)
        self.store = PerfStore(settings.cbmonitor_host)
        self.workload_setting = PhaseSettings

        self.interval = self.MAX_SAMPLING_INTERVAL

        self.cluster = settings.cluster

        self.clients = []

        self.sg_host, self.cb_host = self.cluster_spec.masters

        src_client = new_client(host=self.cb_host,
                                bucket='bucket-1',
                                password='password',
                                timeout=self.TIMEOUT)

        self.clients.append(('bucket-1', src_client))

        self.new_docs = Document(1024)

    def sg_changefeed(self, host: str, key: str):
        sg_db = 'db'
        api = 'http://{}:4985/{}/_changes'.format(host, sg_db)
        key_array = []
        key_array.append(key)
        data = {'limit': 1, 'doc_ids': key_array, 'filter': '_doc_ids', 'feed': 'normal'}
        print('change feed key:', key, data)
        response = requests.post(url=api, data=json.dumps(data))
        print('response:', response)
        if len(response.json()['results']) >= 1:
            if key == response.json()['results'][0]['id']:
                return 1
            else:
                return 0
        else:
            return 0

    def measure(self, src_client):

        key = "sgimport_{}".format(uhex())
        print('key:', key)

        doc = self.new_docs.next(key)

        polling_interval = self.INITIAL_POLLING_INTERVAL

        src_client.upsert(key, doc)


        t0 = time()
        while time() - t0 < self.TIMEOUT:
            if self.sg_changefeed(host=self.sg_host, key=key) == 1:
                break
            sleep(polling_interval)
            polling_interval *= 1.05  # increase interval by 5%
        else:
            logger.warn('SG import sampling timed out after {} seconds'
                        .format(self.TIMEOUT))
        t1 = time()

        src_client.remove(key, quiet=True)

        return {'sgimport_latency': (t1 - t0) * 1000}  # s -> ms

    def sample(self):
        for bucket, src_client in self.clients:
            lags = self.measure(src_client)
            self.store.append(lags,
                              cluster=self.cluster,
                              bucket=bucket,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
        self.mc.add_metric(self.METRICS, server=self.cluster_spec.servers[1], collector=self.COLLECTOR)

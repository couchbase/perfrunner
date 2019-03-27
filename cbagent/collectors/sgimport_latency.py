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

    TIMEOUT = 3600  # 1hr minutes

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

    def sg_changefeed(self, host: str, key: str, last_sequence: int):
        sg_db = 'db'
        api = 'http://{}:4985/{}/_changes'.format(host, sg_db)

        last_sequence_str = "{}".format(last_sequence)

        data = {'filter': 'sync_gateway/bychannel', 'feed': 'normal', "channels": "*", "since": last_sequence_str}

        t1 = time()
        response = requests.post(url=api, data=json.dumps(data))

        last_sequence = last_sequence + len(response.json()['results'])
        print('last_sequence in sg_changefeed', last_sequence)

        record_found = 0
        for record in response.json()['results']:
            if record['id'] == key:
                print(' record found', key)
                record_found = 1
                break
        if record_found == 1:
            return 1, t1, last_sequence
        else:
            return 0, t1, last_sequence

    def get_lastsequence(self, host: str):
        sg_db = 'db'
        api = 'http://{}:4985/{}/_changes'.format(host, sg_db)

        data = {'filter': 'sync_gateway/bychannel', 'feed': 'normal', "channels": '*', "since": '0'}
        response = requests.post(url=api, data=json.dumps(data))

        last_sequence = len(response.json()['results']) + 3
        return last_sequence

    def measure(self, src_client, last_sequence: int):

        key = "sgimport_{}".format(uhex())

        doc = self.new_docs.next(key)

        polling_interval = self.INITIAL_POLLING_INTERVAL

        src_client.upsert(key, doc)

        print('last_sequence at the begining of measure', last_sequence)
        print('key insterted:', key)

        t0 = time()
        t1 = t0
        while time() - t0 < self.TIMEOUT:
            status_code, time_stamp, last_sequence = self.sg_changefeed(host=self.sg_host,
                                                                        key=key,
                                                                        last_sequence=last_sequence)
            print('status_code, time_stamp, last_sequence in while loop', status_code, time_stamp, last_sequence)
            if status_code == 1:
                t1 = time_stamp
                print('t1 after assignment', t1)
                break
            last_sequence = last_sequence
            print('assigning last_sequence to last_sequence', last_sequence)
            sleep(polling_interval)
            polling_interval *= 1.05  # increase interval by 5%
        else:
            logger.warn('SG import sampling timed out after {} seconds'
                        .format(self.TIMEOUT))

        print('time taken (t1 - t0): in ms', (t1 - t0) * 1000)
        return {'sgimport_latency': (t1 - t0) * 1000}  # s -> ms

    def sample(self):
        for bucket, src_client in self.clients:
            last_sequence = self.get_lastsequence(host=self.sg_host)
            print('last sequencid at the begining', last_sequence)
            lags = self.measure(src_client, last_sequence=last_sequence)
            self.store.append(lags,
                              cluster=self.cluster,
                              bucket=bucket,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        self.mc.add_cluster()
        self.mc.add_metric(self.METRICS, server=self.cluster_spec.servers[1], collector=self.COLLECTOR)

import json
from concurrent.futures import ThreadPoolExecutor
from time import time

import requests
from couchbase.bucket import Bucket

from cbagent.collectors import Collector
from cbagent.metadata_client import MetadataClient
from cbagent.stores import PerfStore
from logger import logger
from perfrunner.helpers.misc import uhex
from perfrunner.settings import ClusterSpec, PhaseSettings, TestConfig
from spring.docgen import SGImportLatencyDocument


def new_client(host, bucket, password, timeout):
    connection_string = 'couchbase://{}/{}?password={}'
    connection_string = connection_string.format(host,
                                                 bucket,
                                                 password)
    client = Bucket(connection_string=connection_string)
    client.timeout = timeout
    return client


class SGImportLatency(Collector):

    COLLECTOR = "sgimport_latency"

    METRICS = "sgimport_latency"

    INITIAL_POLLING_INTERVAL = 0.001  # 1 ms

    TIMEOUT = 3600  # 1hr minutes

    MAX_SAMPLING_INTERVAL = 10  # 250 ms

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
        self.cb_host = self.cluster_spec.servers[int(self.test_config.nodes)]
        self.sg_host = next(self.cluster_spec.masters)
        src_client = new_client(host=self.cb_host,
                                bucket='bucket-1',
                                password='password',
                                timeout=self.TIMEOUT)
        self.clients.append(('bucket-1', src_client))
        self.new_docs = SGImportLatencyDocument(1024)

    def check_longpoll_changefeed(self, host: str, key: str, last_sequence: str):
        logger.info("checking longpoll changefeed")
        sg_db = 'db'
        api = 'http://{}:4985/{}/_changes'.format(host, sg_db)
        last_sequence_str = "{}".format(last_sequence)
        data = {'filter': 'sync_gateway/bychannel',
                'feed': 'longpoll',
                "channels": "123",
                "since": last_sequence_str,
                "heartbeat": 3600000}
        logger.info("longpoll data: {}, url: {}".format(data, api))
        try:
            logger.info("longpoll posting")
            response = requests.post(url=api, data=json.dumps(data))
            logger.info("longpoll posted")
        except Exception as ex:
            logger.info("longpoll request failed: {}".format(ex))
            raise ex
        logger.info('got longpoll response: {}'.format(response))
        t1 = time()
        record_found = 0
        logger.info("longpoll response status: {}".format(response.status_code))
        if response.status_code == 200:
            for record in response.json()['results']:
                if record['id'] == key:
                    record_found = 1
                    break
            logger.info("longpoll records found: {}".format(record_found))
            if record_found != 1:
                self.check_longpoll_changefeed(host=host, key=key, last_sequence=last_sequence)
        logger.info("checked longpoll changefeed")
        return t1

    def insert_doc(self, src_client, key: str, doc):
        logger.info("inserting doc")
        src_client.upsert(key, doc)
        logger.info("doc inserted")
        return time()

    def get_lastsequence(self, host: str):
        logger.info("getting last sequence")
        sg_db = 'db'
        api = 'http://{}:4985/{}/_changes'.format(host, sg_db)
        data = {'filter': 'sync_gateway/bychannel',
                'feed': 'normal',
                "channels": "123",
                "since": "0"
                }
        response = requests.post(url=api, data=json.dumps(data))
        last_sequence = response.json()['last_seq']
        logger.info("got last sequence: {}".format(last_sequence))
        return last_sequence

    def measure(self, src_client):
        key = "sgimport_{}".format(uhex())
        logger.info("key: {}".format(key))
        doc = self.new_docs.next(key)
        logger.info("doc: {}".format(doc))
        logger.info("sg_host: {}".format(self.sg_host))
        last_sequence = self.get_lastsequence(host=self.sg_host)
        logger.info("last_sequence: {}".format(last_sequence))
        executor = ThreadPoolExecutor(max_workers=2)
        future1 = executor.submit(self.check_longpoll_changefeed, host=self.sg_host,
                                  key=key,
                                  last_sequence=last_sequence)
        future2 = executor.submit(self.insert_doc, src_client=src_client, key=key, doc=doc)
        logger.info("waiting on t0")
        t0 = future2.result()
        logger.info("t0: {}".format(t0))
        logger.info("waiting on t1")
        t1 = future1.result()
        logger.info("t1; {}".format(t1))
        logger.info('import latency t1, t0', t1, t0, (t1 - t0) * 1000)
        return {'sgimport_latency': (t1 - t0) * 1000}  # s -> ms

    def sample(self):
        for bucket, src_client in self.clients:
            logger.info("bucket: {}, src_client: {}, clients: {}".format(bucket, src_client,
                                                                         self.clients))
            lags = self.measure(src_client)
            logger.info("lags: {}".format(lags))
            self.store.append(lags,
                              cluster=self.cluster,
                              collector=self.COLLECTOR)

    def update_metadata(self):
        logger.info("updating metadata")
        self.mc.add_cluster()
        self.mc.add_metric(self.METRICS, collector=self.COLLECTOR)
        logger.info("metadata updated")

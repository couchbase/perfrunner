import json
import requests
import sys
import time

from logger import logger

from perfrunner.tests import PerfTest
from perfrunner.helpers.cbmonitor import with_stats


FTS_CREATE_HEADERS = {'Content-Type': 'application/json'}
DEFAULT_FTS_CREATE = {
    u'name': None,
    u'params': u'{"mapping":{"types":{},"default_mapping":{"enabled":true,"dynamic":true,"fields":[],"properties":{},"display_order":"0"},"type_field":"_type","default_type":"_default","default_analyzer":"standard","default_datetime_parser":"dateTimeOptional","default_field":"_all","byte_array_converter":"json","analysis":{"analyzers":{},"char_filters":{},"tokenizers":{},"token_filters":{},"token_maps":{}}},"store":{"kvStoreName":"forestdb"}}',
    u'planParams': {   u'hierarchyRules': None,
                       u'maxPartitionsPerPIndex': 20,
                       u'nodePlanParams': None,
                       u'numReplicas': 0,
                       u'pindexWeights': None,
                       u'planFrozen': False},
    u'sourceName': u'bucket-1',
    u'sourceParams': u'{"authUser":"bucket-1","authPassword":"","authSaslUser":"","authSaslPassword":"","clusterManagerBackoffFactor":0,"clusterManagerSleepInitMS":0,"clusterManagerSleepMaxMS":2000,"dataManagerBackoffFactor":0,"dataManagerSleepInitMS":0,"dataManagerSleepMaxMS":2000,"feedBufferSizeBytes":0,"feedBufferAckThreshold":0}',
    u'sourceType': u'couchbase',
    u'sourceUUID': u'',
    u'type': u'fulltext-index',
    u'uuid': u'565cea4df8d4e0ed'}


class FtsIndexTest(PerfTest):
    def create_index(self, fts_node, index_name):
        host_port = [x for x in self.cluster_spec.yield_servers()][0]
        fts_port = self.rest.get_fts_port(host_port, fts_node)
        url = "http://{}:{}/api/index/{}".format(
            fts_node, fts_port, index_name)
        data = dict(DEFAULT_FTS_CREATE)
        data['name'] = index_name
        jstr = json.dumps(data)
        r = requests.put(url, data=jstr, headers=FTS_CREATE_HEADERS)
        if not r.status_code == 200:
            logger.info("URL: %s" % url)
            logger.info("data: %s" % data)
            logger.info("HEADER: %s" % FTS_CREATE_HEADERS)
            logger.error(r.text)
            raise RuntimeError("Failed to create FTS index")

    def wait_for_index(self, fts_node, index_name, num_docs,
                       wait_interval=10, progress_interval=60):
        host_port = [x for x in self.cluster_spec.yield_servers()][0]
        fts_port = self.rest.get_fts_port(host_port, fts_node)
        url = "http://{}:{}/api/index/{}/count".format(
            fts_node, fts_port, index_name)
        last_reported = time.time()
        while True:
            r = requests.get(url)
            if not r.status_code == 200:
                raise RuntimeError(
                    "Failed to fetch document count of index. Status {}".format(
                        r.status_code))
            count = int(r.json()['count'])
            if count >= num_docs:
                logger.info("Done at document count {}".format(count))
                return
            check_report = time.time()
            if check_report - last_reported >= progress_interval:
                last_reported = check_report
                logger.info("(progress) Document count is at {}".format(count))
            time.sleep(wait_interval)

    @with_stats
    def load(self):
        load_settings = self.test_config.load_settings
        if self.test_config.fts_settings:
            load_settings.fts = self.test_config.fts_settings
        else:
            raise RuntimeError("Require [FTS] section in test spec")
        self.worker_manager.run_workload(load_settings, self.target_iterator)
        self.worker_manager.wait_for_workers()

    def run(self):
        logger.info("FtsIndexTest loading database into KV")
        self.load()

        fts_node = self.cluster_spec.yield_servers_by_role(
            'fts').next()[1][0].split(':')[0]
        index_name = self.test_config.fts_settings.name

        start_time = time.time()
        logger.info("Creating FTS index {} on {}".format(index_name, fts_node))
        self.create_index(fts_node, index_name)

        num_docs = self.test_config.fts_settings.items
        logger.info("Timing the time it takes to index {} documents".format(
                    num_docs))
        self.wait_for_index(fts_node, index_name, num_docs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info("FTS initial index build time took {} seconds".format(
            elapsed_time))

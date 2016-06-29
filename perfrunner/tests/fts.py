import json
import time

import requests
from logger import logger
from requests.auth import HTTPBasicAuth

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class INDEXDEF:
    INDEX = {
        "type": "fulltext-index",
        "name": "",
        "uuid": "",
        "sourceType": "couchbase",
        "sourceName": "",
        "sourceUUID": "",
        "planParams": {
            "maxPartitionsPerPIndex": 32,
            "numReplicas": 0,
            "hierarchyRules": None,
            "nodePlanParams": None,
            "pindexWeights": None,
            "planFrozen": False
        },
        "params": {
            "mapping": {
                "analysis": {
                    "analyzers": {},
                    "char_filters": {},
                    "token_filters": {},
                    "token_maps": {},
                    "tokenizers": {}
                },
                "byte_array_converter": "json",
                "default_analyzer": "simple",
                "default_datetime_parser": "dateTimeOptional",
                "default_field": "_all",
                "default_mapping": {
                    "display_order": "0",
                    "dynamic": True,
                    "enabled": True,
                    "fields": [],
                    "properties": {}
                },
                "default_type": "_default",
                "type_field": "type",
                "types": {}
            },
            "store": {
                "kvStoreName": "forestdb"
            }
        },
        "sourceParams": {
            "authPassword": "",
            "authSaslPassword": "",
            "authSaslUser": "",
            "authUser": "default",
            "clusterManagerBackoffFactor": 0,
            "clusterManagerSleepInitMS": 0,
            "clusterManagerSleepMaxMS": 2000,
            "dataManagerBackoffFactor": 0,
            "dataManagerSleepInitMS": 0,
            "dataManagerSleepMaxMS": 2000,
            "feedBufferAckThreshold": 0,
            "feedBufferSizeBytes": 0
        }
    }


class FTStest(PerfTest):

    """
    The most basic FTS workflow:
        Initial data load ->
            Persistence and intra-cluster replication (for consistency) ->
                Data compaction (for consistency) ->
                    "Hot" load or working set warm up ->
                        "access" phase or active workload
    """

    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        super(FTStest, self).__init__(cluster_spec, test_config,
                                      verbose, experiment)
        self.index_definition = INDEXDEF.INDEX
        self.host_port = [x for x in self.cluster_spec.yield_servers()][0]
        self.host = self.host_port.split(':')[0]
        self.fts_port = 8094
        self.host_port = '{}:{}'.format(self.host, self.fts_port)
        self.fts_index = self.test_config.fts_settings.name
        self.header = {'Content-Type': 'application/json'}
        self.requests = requests.session()
        self.wait_time = 30
        self.fts_doccount = self.test_config.fts_settings.items
        self.prepare_index()
        self.index_time_taken = 0
        self.auth = HTTPBasicAuth('Administrator', 'password')

    @with_stats
    def access(self):
        super(FTStest, self).timer()

    @with_stats
    def access_bg_test(self):
        access_settings = self.test_config.access_settings
        access_settings.fts_config = self.test_config.fts_settings
        self.access_bg(access_settings)

    def load(self):
        logger.info('load/restore data to bucket')
        self.remote.cbrestorefts()

    def run(self):
        self.delete_index()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.workload = self.test_config.access_settings

    def delete_index(self):
        self.requests.delete(self.index_url,
                             auth=(self.rest.rest_username,
                                   self.rest.rest_password),
                             headers=self.header)

    def prepare_index(self):
        self.index_definition['name'] = self.fts_index
        self.index_definition["sourceName"] = self.test_config.buckets[0]
        self.index_url = "http://{}/api/index/{}".\
            format(self.host_port, self.fts_index)
        logger.info('Created the Index definition : {}'.
                    format(self.index_definition))

    def create_index(self):
        r = self.requests.put(self.index_url,
                              data=json.dumps(self.index_definition, ensure_ascii=False),
                              auth=(self.rest.rest_username, self.rest.rest_password),
                              headers=self.header)
        if not r.status_code == 200:
            logger.info("URL: %s" % self.index_url)
            logger.info("data: %s" % self.index_definition)
            logger.info("HEADER: %s" % self.header)
            logger.error(r.text)
            raise RuntimeError("Failed to create FTS index")
        time.sleep(self.wait_time)

    def wait_for_index(self, wait_interval=10, progress_interval=60):
        logger.info(' Waiting for Index to be completed')
        last_reported = time.time()
        lastcount = 0
        retry = 0
        while True and (retry != 6):
            r = self.requests.get(url=self.index_url + '/count', auth=self.auth)
            if not r.status_code == 200:
                raise RuntimeError(
                    "Failed to fetch document count of index. Status {}".format(r.status_code))
            count = int(r.json()['count'])
            if lastcount >= count:
                retry += 1
                time.sleep(wait_interval * retry)
                logger.info('count of documents :{} is same or less for retry {}'.format(count, retry))
                continue
            retry = 0
            logger.info("Done at document count {}".
                        format(count))
            if count >= self.fts_doccount:
                logger.info("Finished at document count {}".
                            format(count))
                return
            check_report = time.time()
            if check_report - last_reported >= progress_interval:
                last_reported = check_report
                logger.info("(progress) Document count is at {}".
                            format(count))
            lastcount = count
        if lastcount != self.fts_doccount:
            raise RuntimeError("Failed to create Index")


class FtsIndexTest(FTStest):

        COLLECTORS = {"fts_stats": True}

        @with_stats
        def index_test(self):
            logger.info('running Index Test with stats')
            self.create_index()
            start_time = time.time()
            self.wait_for_index()
            end_time = time.time()
            self.index_time_taken = end_time - start_time

        def run(self):
            super(FtsIndexTest, self).run()
            logger.info("Creating FTS index {} on {}".
                        format(self.fts_index, self.host_port))
            logger.info("Measuring the time it takes to index {} documents".
                        format(self.fts_doccount))
            self.index_test()
            self.reporter.post_to_sf(
                *self.metric_helper.calc_ftses_index(self.index_time_taken)
            )


class FTSLatencyTest(FTStest):
        COLLECTORS = {'fts_latency': True, "fts_query_stats": True, "fts_stats": True}

        def run(self):
            super(FTSLatencyTest, self).run()
            self.create_index()
            self.wait_for_index()
            time.sleep(3 * self.wait_time)
            self.access_bg_test()
            if self.test_config.stats_settings.enabled:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_latency_ftses_queries(percentile=80,
                                                                   dbname='fts_latency',
                                                                   metrics='cbft_latency_get')
                )


class FTSThroughputTest(FTStest):
        COLLECTORS = {'fts_query_stats': True,
                      "fts_stats": True}

        def run(self):
            super(FTSThroughputTest, self).run()
            self.create_index()
            self.wait_for_index()
            time.sleep(3 * self.wait_time)
            self.access_bg_test()
            if self.test_config.stats_settings.enabled:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_avg_fts_queries()
                )

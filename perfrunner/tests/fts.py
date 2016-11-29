import json
import time
import requests
from logger import logger
from requests.auth import HTTPBasicAuth

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import get_json_from_file
from perfrunner.tests import PerfTest


class FTStest(PerfTest):

    WAIT_TIME = 1
    INDEX_WAIT_MAX = 1200

    def __init__(self, cluster_spec, test_config, verbose):
        super(FTStest, self).__init__(cluster_spec, test_config, verbose)

        self.index_definition = get_json_from_file(self.test_config.fts_settings.index_configfile)
        self.host_port = [x for x in self.cluster_spec.yield_servers()][0]
        self.host = self.host_port.split(':')[0]
        self.fts_port = 8094
        self.host_port = '{}:{}'.format(self.host, self.fts_port)
        self.fts_index = self.test_config.fts_settings.name
        self.header = {'Content-Type': 'application/json'}
        self.requests = requests.session()
        self.fts_doccount = self.test_config.fts_settings.items
        self.prepare_index()
        self.index_time_taken = 0
        self.auth = HTTPBasicAuth('Administrator', 'password')
        self.order_by = self.test_config.fts_settings.order_by

    @with_stats
    def access(self, *args):
        super(FTStest, self).timer()

    def access_bg_test(self):
        access_settings = self.test_config.access_settings
        access_settings.fts_config = self.test_config.fts_settings
        self.access_bg(access_settings)
        self.access()

    def load(self, *args):
        logger.info('load/restore data to bucket')
        self.remote.cbrestorefts(self.test_config.fts_settings.storage, self.test_config.fts_settings.repo)

    def run(self):
        self.workload = self.test_config.access_settings
        self.cleanup_and_restore()
        self.create_index()
        self.wait_for_index()
        self.check_rec_presist()
        self.access_bg_test()
        self.report_kpi()

    def cleanup_and_restore(self):
        self.delete_index()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

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

    def check_rec_presist(self):
        rec_memory = self.fts_doccount
        self.fts_url = "http://{}/api/nsstats".format(self.host_port)
        key = ':'.join([self.test_config.buckets[0], self.fts_index, 'num_recs_to_persist'])
        while rec_memory != 0:
            logger.info("Record persists to be expected: %s" % rec_memory)
            r = self.requests.get(url=self.fts_url, auth=self.auth)
            time.sleep(self.WAIT_TIME)
            rec_memory = r.json()[key]

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
        time.sleep(self.WAIT_TIME)

    def wait_for_index(self):
        logger.info(' Waiting for Index to be completed')
        attempts = 0
        while True:
            r = self.requests.get(url=self.index_url + '/count', auth=self.auth)
            if r.status_code != 200:
                raise RuntimeError("Failed to fetch document count of index. Status {}".format(r.status_code))
            count = int(r.json()['count'])
            if count >= self.fts_doccount:
                logger.info("Finished at document count {}".format(count))
                return
            else:
                if not attempts % 10:
                    logger.info("(progress) idexed documents count {}".format(count))
                attempts += 1
                time.sleep(self.WAIT_TIME)
                if (attempts * self.WAIT_TIME) >= self.INDEX_WAIT_MAX:
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
            self.cleanup_and_restore()
            self.index_test()
            self.report_kpi()

        def _report_kpi(self):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_ftses_index(self.index_time_taken,
                                                     order_by=self.order_by)
            )


class FTSLatencyTest(FTStest):
        COLLECTORS = {'fts_latency': True,
                      "fts_query_stats": True,
                      "fts_stats": True}

        def _report_kpi(self):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_latency_ftses_queries(percentile=80,
                                                               dbname='fts_latency',
                                                               metrics='cbft_latency_get',
                                                               order_by=self.order_by)
            )
            self.reporter.post_to_sf(
                *self.metric_helper.calc_latency_ftses_queries(percentile=0,
                                                               dbname='fts_latency',
                                                               metrics='cbft_latency_get',
                                                               order_by=self.order_by)
            )


class FTSThroughputTest(FTStest):
        COLLECTORS = {'fts_query_stats': True,
                      "fts_stats": True}

        def _report_kpi(self):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_avg_fts_queries(order_by=self.order_by)
            )

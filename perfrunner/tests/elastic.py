import json
import time
import requests
from logger import logger
from requests.auth import HTTPBasicAuth


from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import get_json_from_file
from perfrunner.helpers.rest import RestHelper
from perfrunner.tests import PerfTest


class Elastictest(PerfTest):

    WAIT_TIME = 1
    INDEX_WAIT_MAX = 1200

    def __init__(self, cluster_spec, test_config, verbose):
        super(Elastictest, self).__init__(cluster_spec, test_config, verbose)

        self.index_definition = get_json_from_file(self.test_config.fts_settings.index_configfile)
        self.host_port = [x for x in self.cluster_spec.yield_servers()][0]
        self.host = self.host_port.split(':')[0]
        self.url = "{}:{}".format(self.host, "9200")
        self.elastic_host_port = "{}:{}".format(self.host, "9091")
        self.elastic_port = self.test_config.fts_settings.port
        self.elastic_index = self.test_config.fts_settings.name
        self.header = {'Content-Type': 'application/json'}
        self.requests = requests.session()
        self.elastic_doccount = self.test_config.fts_settings.items
        self.index_time_taken = 0
        '''
        This API will be needed for fresh installation
        self.remote.startelasticsearchplugin()
        '''
        self.index_url = "http://{}/{}".format(self.url, self.elastic_index)
        self.rest = RestHelper(cluster_spec)
        self.order_by = self.test_config.fts_settings.order_by

    @with_stats
    def access(self, *args):
        super(Elastictest, self).timer()

    def access_bg_test(self):
        access_settings = self.test_config.access_settings
        access_settings.fts_config = self.test_config.fts_settings
        self.access_bg(access_settings)
        self.access()

    def addelastic(self):
        requests.post(url='http://{}:8091/pools/default/remoteClusters'.format(self.host),
                          data={'username': 'Administrator', 'password': 'password',
                                'hostname': '{}:9091'.format(self.host), 'name': 'Elastic'},
                          auth=HTTPBasicAuth('Administrator', 'password'))
        api = "http://{}/controller/createReplication?fromBucket=bucket-1&" \
              "toCluster=Elastic&toBucket={}&replicationType=continuous&type=capi".\
            format(self.host_port, self.elastic_index)
        resp = requests.post(url=api,
                             auth=('Administrator', 'password'))
        if not resp.status_code == 200:
            raise RuntimeError("Failed to create rebalance")

    def load(self, *args):
        logger.info('load/restore data to bucket')
        self.remote.cbrestorefts(self.test_config.fts_settings.storage, self.test_config.fts_settings.repo)

    def run(self):
        self.cleanup_and_restore()
        self.workload = self.test_config.access_settings
        self.create_index()
        self.addelastic()
        self.wait_for_index()
        self.access_bg_test()
        self.report_kpi()

    def cleanup_and_restore(self):
        self.delete_index()
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

    def delete_index(self):
        logger.info('Deleting Elasticsearch index')
        self.requests.delete(self.index_url)

    def create_index(self):
        logger.info('Creating Elasticsearch index')
        r = self.requests.put(self.index_url,
                              data=json.dumps(self.index_definition, ensure_ascii=False))
        if not r.status_code == 200:
            logger.info("URL: %s" % self.index_url)
            logger.error(r.text)
            raise RuntimeError("Failed to create Elasticsearch index")
        time.sleep(self.WAIT_TIME)

    def wait_for_index(self):
        logger.info(' Waiting for Elasticsearch index to be completed')
        attempts = 0
        while True:
            r = self.requests.get(url=self.index_url + '/_count')
            if r.status_code != 200:
                raise RuntimeError("Failed to fetch document count of index. Status {}".format(r.status_code))
            count = int(r.json()['count'])
            if count >= self.elastic_doccount:
                logger.info("Finished at document count {}".format(count))
                return
            else:
                if not attempts % 10:
                    logger.info("(progress) idexed documents count {}".format(count))
                attempts += 1
                time.sleep(self.WAIT_TIME)
                if (attempts * self.WAIT_TIME) >= self.INDEX_WAIT_MAX:
                    raise RuntimeError("Failed to create index")


class ElasticIndexTest(Elastictest):

        def index_test(self):
            logger.info('running Index Test')
            self.create_index()
            self.addelastic()
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
                                                     order_by=self.order_by,
                                                     name=' Elasticsearch 1.7')
            )


class ElasticLatencyTest(Elastictest):
        COLLECTORS = {"elastic_stats": True}

        def _report_kpi(self):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_latency_ftses_queries(percentile=80,
                                                               dbname='fts_latency',
                                                               metrics='elastic_latency_get',
                                                               order_by=self.order_by,
                                                               name=' Elasticsearch 1.7'
                                                               ))


class ElasticThroughputTest(Elastictest):

        def _report_kpi(self):
            self.reporter.post_to_sf(
                *self.metric_helper.calc_avg_fts_queries(order_by=self.order_by,
                                                         name=' Elasticsearch 1.7')
            )

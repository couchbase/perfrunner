import time

import requests
from logger import logger
from requests.auth import HTTPBasicAuth

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.rest import RestHelper
from perfrunner.tests import PerfTest


class Elastictest(PerfTest):

    """
    The most basic ELASTIC workflow:
        Initial data load ->
            Persistence and intra-cluster replication (for consistency) ->
                Data compaction (for consistency) ->
                    "Hot" load or working set warm up ->
                        "access" phase or active workload
    """

    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        super(Elastictest, self).__init__(cluster_spec, test_config, verbose, experiment)
        self.host_port = [x for x in self.cluster_spec.yield_servers()][0]
        self.host = self.host_port.split(':')[0]
        self.url = "{}:{}".format(self.host, "9200")
        self.elastic_host_port = "{}:{}".format(self.host, "9091")
        self.elastic_port = self.test_config.fts_settings.port
        self.elastic_index = self.test_config.fts_settings.name
        self.header = {'Content-Type': 'application/json'}
        self.requests = requests.session()
        self.wait_time = 60
        self.elastic_doccount = self.test_config.fts_settings.items
        self.index_time_taken = 0
        '''
        This API will be needed for fresh installation
        self.remote.startelasticsearchplugin()
        '''
        self.index_url = "http://{}/{}".format(self.url, self.elastic_index)
        self.rest = RestHelper(cluster_spec)

    @with_stats
    def access_bg_test(self):
        access_settings = self.test_config.access_settings
        access_settings.fts_config = self.test_config.fts_settings
        self.access_bg(access_settings)

    def addelastic(self):
        '''
        self.rest.add_remote_cluster(self.host_port, self.elastic_host_port, "Elastic")
        The above API not working, throwing
        File "/tmp/pip-build-qBV3Lp/requests/requests/models.py", line 597, in apparent_encoding
          File "/tmp/pip-build-qBV3Lp/requests/requests/packages/charade/__init__.py", line 27, in detect
        ImportError: cannot import name universaldetector Error

        api='http://{}/pools/default/remoteClusters'.format(self.host_port)
        print 'api is ', api
        mydata = {'username': 'Administrator', 'password': 'password',
                  'hostname': self.elastic_host_port, 'name': 'Elastic'}
        print 'mydata', mydata
        r=requests.post(url =api,
            data=mydata,
            auth=HTTPBasicAuth('Administrator', 'password'))
        print 'status', r
        '''
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
        logger.info('Deleting elasticsearch Index')
        self.requests.delete(self.index_url)

    def create_index(self):
        logger.info('Creating elastic search Index')
        r = self.requests.put(self.index_url)
        if not r.status_code == 200:
            logger.info("URL: %s" % self.index_url)
            logger.error(r.text)
            raise RuntimeError("Failed to create elastic search index")
        time.sleep(self.wait_time)

    def wait_for_index(self, wait_interval=10, progress_interval=60):
        logger.info(' Waiting for  ELasticSearch plugin Index to be completed')
        last_reported = time.time()
        '''
         To avoid infinite loop in case hangs,
         lastcount variable for that
        '''
        lastcount = 0
        retry = 0
        while True and (retry != 6):
            r = self.requests.get(self.index_url + '/_count')
            if not r.status_code == 200:
                raise RuntimeError(
                    "Failed to fetch document count of index. Status {}".format(r.status_code))
            count = int(r.json()['count'])
            if lastcount >= count:
                retry += 1
                time.sleep(wait_interval * retry)
                logger.info('count of documents :{} is same or less for retry {}'.
                            format(count, retry))
                continue
            retry = 0
            logger.info("Done at document count {}".format(count))
            if count >= self.elastic_doccount:
                logger.info("Finished at document count {}".format(count))
                return
            check_report = time.time()
            if check_report - last_reported >= progress_interval:
                last_reported = check_report
                logger.info("(progress) Document count is at {}".format(count))
            lastcount = count
        if lastcount != self.elastic_doccount:
            raise RuntimeError("Failed to create Index")


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
            super(ElasticIndexTest, self).run()
            logger.info("Measuring the time it takes to index {} documents".format(self.elastic_doccount))
            self.index_test()
            self.reporter.post_to_sf(
                *self.metric_helper.calc_ftses_index(self.index_time_taken)
            )


class ElasticLatencyTest(Elastictest):
        COLLECTORS = {"elastic_stats": True}

        def run(self):
            super(ElasticLatencyTest, self).run()
            self.create_index()
            self.wait_for_index()
            self.access_bg_test()
            if self.test_config.stats_settings.enabled:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_latency_ftses_queries(percentile=95,
                                                                   dbname='elastic_latency',
                                                                   metrics='elastic_latency_get',
                                                                   name='ELASTICSEARCH'
                                                                   ))


class ElasticThroughputTest(Elastictest):

        def run(self):
            super(ElasticThroughputTest, self).run()
            self.create_index()
            self.wait_for_index()
            self.access_bg_test()
            if self.test_config.stats_settings.enabled:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_latency_ftses_queries(percentile=95,
                                                                   dbname='elastic_latency',
                                                                   name='ELASTICSEARCH'
                                                                   ))

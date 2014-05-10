import json
import time

from logger import logger
from seriesly import Seriesly

from perfrunner.helpers.metrics import SgwMetricHelper
from perfrunner.helpers.misc import pretty_dict
from perfrunner.settings import SGW_SERIESLY_HOST
from perfrunner.tests import PerfTest


class SyncGatewayGateloadTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(SyncGatewayGateloadTest, self).__init__(*args, **kwargs)
        self.metric_helper = SgwMetricHelper(self)

    def restart_seriesly(self):
        logger.info('Dropping seriesly dbs')
        seriesly = Seriesly(host='{}'.format(SGW_SERIESLY_HOST))
        db_list = seriesly.list_dbs()
        for db in db_list:
            if ('gateway' in db) or ('gateload' in db):
                seriesly.drop_db(db)

        logger.info('Creating seriesly dbs')
        for i, _ in enumerate(self.cluster_spec.gateways):
            seriesly.create_db('gateway_{}'.format(i + 1))
        for i, _ in enumerate(self.cluster_spec.gateloads):
            seriesly.create_db('gateload_{}'.format(i + 1))

        self.remote.start_sampling()

    def start(self):
        self.remote.clean_seriesly()
        self.restart_seriesly()
        time.sleep(10)
        self.generate_gateload_configs()
        self.remote.start_gateload()

    def generate_gateload_configs(self):
        with open('templates/gateload_config_template.json') as fh:
            template = json.load(fh)

        for idx, _ in enumerate(self.cluster_spec.gateloads):
            template.update({
                'Hostname': self.cluster_spec.gateways[idx],
                'UserOffset': (self.test_config.gateload_settings.pushers +
                               self.test_config.gateload_settings.pullers) * idx,
                'NumPullers': self.test_config.gateload_settings.pullers,
                'NumPushers': self.test_config.gateload_settings.pushers,
                'RunTimeMs': self.test_config.gateload_settings.run_time * 1000,
            })

            config_fname = 'templates/gateload_config_{}.json'.format(idx)
            with open(config_fname, 'w') as fh:
                fh.write(pretty_dict(template))

    def collect_kpi(self):
        logger.info('Collecting KPI')
        for idx, gateload in enumerate(self.cluster_spec.gateloads, start=1):
            logger.info('Test results for {} ({}):'.format(gateload, idx))
            for p in (95, 99):
                latency = self.metric_helper.calc_push_latency(p=p, idx=idx)
                logger.info('\tPushToSubscriberInteractive/p{} average: {}'
                            .format(p, latency))

    def run(self):
        self.start()
        logger.info('Sleep {} seconds waiting for test to finish'.format(
            self.test_config.gateload_settings.run_time
        ))
        time.sleep(self.test_config.gateload_settings.run_time)
        self.collect_kpi()

import json
import time

from seriesly import Seriesly
from logger import logger

from perfrunner.helpers.misc import pretty_dict
from perfrunner.tests import PerfTest
from perfrunner.settings import SERIESLY_HOST


class SyncGatewayGateloadTest(PerfTest):

    def seriesly_create_db(self, seriesly):
        logger.info('gateload_test.py - seriesly_create_db')
        for i, _ in enumerate(self.cluster_spec.gateways):
            seriesly.create_db('gateway_{}'.format(i))
        for i, _ in enumerate(self.cluster_spec.gateloads):
            seriesly.create_db('gateload_{}'.format(i))

    def seriesly_drop_db(self, seriesly):
        logger.info('gateload_test.py - seriesly_drop_db')
        db_list = seriesly.list_dbs()
        for db in db_list:
            seriesly.drop_db(db)

    def start(self):
        self.remote.start_seriesly()
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
        seriesly = Seriesly(host=SERIESLY_HOST)

        logger.info('collect_kpi')
        p_values = ['p99', 'p95']
        for i, _ in enumerate(self.cluster_spec.gateloads):
            target = 'gateload_{}'.format(i + 1)
            logger.info('Test results for {}:'.format(target))
            for p_value in p_values:
                params = {'ptr': '/gateload/ops/PushToSubscriberInteractive/{}'.format(p_value),
                          'reducer': 'avg',
                          'group': 1000000000000}
                data = seriesly[target].query(params)
                value = data.values()[0][0]
                if value is not None:
                    value_sec = float(value) / 10 ** 9
                    logger.info('\tPushToSubscriberInteractive/{} average: {}'
                                .format(p_value, round(value_sec, 2)))
                else:
                    logger.info('\tPushToSubscriberInteractive/{} average does not have expected format: {}'
                                .format(p_value, data))

    def run(self):
        self.start()
        sleep_time = self.test_config.gateload_settings.rampup_interval + self.test_config.gateload_settings.run_time
        logger.info('Sleep {} seconds waiting for test to finish'.format(sleep_time))
        time.sleep(sleep_time)
        self.collect_kpi()

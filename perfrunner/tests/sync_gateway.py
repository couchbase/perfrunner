import json
import time

from logger import logger
from seriesly import Seriesly

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.metrics import SgwMetricHelper
from perfrunner.helpers.misc import log_phase, pretty_dict
from perfrunner.helpers.rest import SyncGatewayRequestHelper
from perfrunner.settings import SGW_SERIESLY_HOST
from perfrunner.tests import PerfTest


class SyncGatewayGateloadTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(SyncGatewayGateloadTest, self).__init__(*args, **kwargs)
        self.metric_helper = SgwMetricHelper(self)
        self.request_helper = SyncGatewayRequestHelper()

    def start_samplers(self):
        logger.info('Creating seriesly dbs')
        seriesly = Seriesly(host='{}'.format(SGW_SERIESLY_HOST))
        for i, _ in enumerate(self.cluster_spec.gateways, start=1):
            seriesly.create_db('gateway_{}'.format(i))
            seriesly.create_db('gateload_{}'.format(i))
        self.remote.start_sampling()

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
        for idx, gateway_ip in enumerate(self.cluster_spec.gateways, start=1):
            expvar = self.request_helper.collect_expvar(gateway_ip)
            fname = 'gateway_expvar_{}.json'.format(idx)
            with open(fname, 'w') as fh:
                fh.write(pretty_dict(expvar))

        for idx, gateload in enumerate(self.cluster_spec.gateloads, start=1):
            logger.info('Test results for {} ({}):'.format(gateload, idx))
            for p in (95, 99):
                latency = self.metric_helper.calc_push_latency(p=p, idx=idx)
                logger.info('\tPushToSubscriberInteractive/p{} average: {}'
                            .format(p, latency))

    @with_stats
    def workload(self):
        logger.info('Sleep {} seconds waiting for test to finish'.format(
            self.test_config.gateload_settings.run_time
        ))
        time.sleep(self.test_config.gateload_settings.run_time)

    def run(self):
        self.generate_gateload_configs()
        self.remote.start_gateload()

        self.remote.restart_seriesly()
        self.start_samplers()

        log_phase('Gateload settings', self.test_config.gateload_settings)
        log_phase('Gateway settings', self.test_config.gateway_settings)
        logger.info('Num Gateways: {}'.format(len(self.cluster_spec.gateways)))

        self.workload()

        self.collect_kpi()
from collections import defaultdict
import json
import time

import numpy as np
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

        latencies = defaultdict(list)
        for idx, gateload in enumerate(self.cluster_spec.gateloads, start=1):
            logger.info('Test results for {} ({}):'.format(gateload, idx))
            for p in (95, 99):
                latency = self.metric_helper.calc_push_latency(p=p, idx=idx)
                latencies[p].append(latency)
                logger.info('\tPushToSubscriberInteractive/p{} average: {}'
                            .format(p, latency))
        criteria = {
            95: self.test_config.gateload_settings.p95_avg_criteria,
            99: self.test_config.gateload_settings.p99_avg_criteria,
        }
        for p in (95, 99):
            average = np.mean(latencies[p])
            if average > criteria[p]:
                logger.warn('\tPushToSubscriberInteractive/p{} average: {} - does not meet the criteria of {}'
                            .format(p, average, criteria[p]))
            else:
                logger.info('\tPushToSubscriberInteractive/p{} average: {} - meet the criteria of {}'
                            .format(p, average, criteria[p]))

    @with_stats
    def workload(self):
        logger.info('Sleep {} seconds waiting for test to finish'.format(
            self.test_config.gateload_settings.run_time
        ))
        time.sleep(self.test_config.gateload_settings.run_time)

    def run(self):
        self.generate_gateload_configs()
        self.remote.start_gateload()

        for idx, gateload in enumerate(self.cluster_spec.gateloads, start=1):
            self.request_helper.wait_for_gateload_to_start(idx, gateload)
        self.remote.restart_seriesly()
        self.request_helper.wait_for_seriesly_to_start(SGW_SERIESLY_HOST)
        self.start_samplers()

        log_phase('Gateload settings', self.test_config.gateload_settings)
        log_phase('Gateway settings', self.test_config.gateway_settings)
        logger.info('Num Gateways: {}'.format(len(self.cluster_spec.gateways)))

        self.workload()

        self.collect_kpi()

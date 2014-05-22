from collections import defaultdict, OrderedDict
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

    KPI = 'PushToSubscriberInteractive/p{} average'

    def __init__(self, *args, **kwargs):
        super(SyncGatewayGateloadTest, self).__init__(*args, **kwargs)
        self.metric_helper = SgwMetricHelper(self)
        self.request_helper = SyncGatewayRequestHelper()

    def create_sgw_test_config(self):
        logger.info('Creating bash configuration')
        with open('scripts/sgw_test_config.sh', 'w') as fh:
            fh.write('#!/bin/sh\n')
            fh.write('gateways_ip="{}"\n'.format(' '.join(self.cluster_spec.gateways)))
            fh.write('gateloads_ip="{}"\n'.format(' '.join(self.cluster_spec.gateloads)))
            fh.write('dbs_ip="{}"\n'.format(' '.join(self.cluster_spec.yield_hostnames())))
            fh.write('seriesly_ip={}\n'.format(SGW_SERIESLY_HOST))

    def start_test_info(self):
        self.create_sgw_test_config()
        self.remote.start_test_info()

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
        logger.info('Collecting Sync Gateway KPI')

        criteria = OrderedDict((
            (95, self.test_config.gateload_settings.p95_avg_criteria),
            (99, self.test_config.gateload_settings.p99_avg_criteria),
        ))

        summary = defaultdict(dict)
        latencies = defaultdict(list)
        for idx, gateload in enumerate(self.cluster_spec.gateloads, start=1):
            for p in criteria:
                kpi = self.KPI.format(p)
                latency = self.metric_helper.calc_push_latency(p=p, idx=idx)
                summary[gateload][kpi] = latency
                latencies[p].append(latency)
        logger.info('Per node summary: {}'.format(pretty_dict(summary)))

        pass_fail = []
        for p, criterion in criteria.items():
            kpi = self.KPI.format(p)
            average = np.mean(latencies[p])
            if average > criterion:
                status = '{}: {} - doesn\'t meet the criteria of {}'\
                    .format(kpi, average, criterion)
            else:
                status = '{}: {} - meets the criteria of {}'\
                    .format(kpi, average, criterion)
            pass_fail.append(status)
        logger.info('Aggregated summary: {}'.format(pretty_dict(pass_fail)))

    @with_stats
    def workload(self):
        logger.info('Sleep {} seconds waiting for test to finish'.format(
            self.test_config.gateload_settings.run_time
        ))
        time.sleep(self.test_config.gateload_settings.run_time)

    def run(self):
        self.start_test_info()

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

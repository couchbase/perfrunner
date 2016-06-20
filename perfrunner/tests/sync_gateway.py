import json
import sys
import time
import traceback
from collections import OrderedDict, defaultdict

import numpy as np
from jinja2 import Environment, FileSystemLoader
from logger import logger
from seriesly import Seriesly

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.metrics import MetricHelper, SgwMetricHelper
from perfrunner.helpers.misc import log_phase, pretty_dict
from perfrunner.helpers.rest import SyncGatewayRequestHelper
from perfrunner.tests import PerfTest


class GateloadTest(PerfTest):

    KPI = 'PushToSubscriberInteractive/p{} average'

    def __init__(self, *args, **kwargs):
        super(GateloadTest, self).__init__(*args, **kwargs)
        self.metric_helper = SgwMetricHelper(self)
        self.metric_db_servers_helper = MetricHelper(self)
        self.request_helper = SyncGatewayRequestHelper()

        loader = FileSystemLoader('templates')
        self.env = Environment(loader=loader)

    def create_sgw_test_config(self):
        logger.info('Creating bash configuration')
        template = self.env.get_template('sgw_test_config.sh')
        with open('scripts/sgw_test_config.sh', 'w') as fh:
            fh.write(template.render(
                gateways_ip=' '.join(self.remote.gateways),
                gateloads_ip=' '.join(self.remote.gateloads),
                dbs_ip=' '.join(self.cluster_spec.yield_hostnames()),
                seriesly_ip=self.test_config.gateload_settings.seriesly_host,
                run_time=self.test_config.gateload_settings.run_time,
            ))

    def start_test_info(self):
        self.create_sgw_test_config()
        self.remote.start_test_info()

    def start_samplers(self):
        logger.info('Creating seriesly dbs')
        seriesly = Seriesly(host='{}'.format(self.test_config.gateload_settings.seriesly_host))
        for i, _ in enumerate(self.remote.gateways, start=1):
            seriesly.create_db('gateway_{}'.format(i))
            seriesly.create_db('gateload_{}'.format(i))
        self.remote.start_sampling()

    def generate_gateload_configs(self):
        template = self.env.get_template('gateload_config_template.json')

        for idx, gateload in enumerate(self.remote.gateloads):
            gateway = self.remote.gateways[idx]
            config_fname = 'templates/gateload_config_{}.json'.format(idx)
            with open(config_fname, 'w') as fh:
                fh.write(template.render(
                    gateway=gateway,
                    pushers=self.test_config.gateload_settings.pushers,
                    pullers=self.test_config.gateload_settings.pullers,
                    doc_size=self.test_config.gateload_settings.doc_size,
                    send_attachment=self.test_config.gateload_settings.send_attachment,
                    channel_active_users=self.test_config.gateload_settings.channel_active_users,
                    channel_concurrent_users=self.test_config.gateload_settings.channel_concurrent_users,
                    sleep_time=self.test_config.gateload_settings.sleep_time * 1000,
                    p95_avg_criteria=self.test_config.gateload_settings.p95_avg_criteria,
                    p99_avg_criteria=self.test_config.gateload_settings.p99_avg_criteria,
                    run_time=self.test_config.gateload_settings.run_time * 1000,
                    rampup_interval=self.test_config.gateload_settings.rampup_interval * 1000,
                    logging_verbose=self.test_config.gateload_settings.logging_verbose,
                    seriesly_host=self.test_config.gateload_settings.seriesly_host,
                    idx=idx,
                    auth_type=self.test_config.gateload_settings.auth_type,
                    password=self.test_config.gateload_settings.password,
                ))

    def collect_kpi(self):
        logger.info('Collecting Sync Gateway KPI')
        try:

            criteria = OrderedDict((
                (95, self.test_config.gateload_settings.p95_avg_criteria),
                (99, self.test_config.gateload_settings.p99_avg_criteria),
            ))

            summary = defaultdict(dict)
            latencies = defaultdict(list)
            all_requests_per_sec = []
            self.errors = []
            for idx, gateload in enumerate(self.remote.gateloads, start=1):
                for p in criteria:
                    kpi = self.KPI.format(p)
                    latency = self.metric_helper.calc_push_latency(p=p, idx=idx)
                    if latency == 0:
                        status = '{}: Failed to get latency data'.format(gateload)
                        self.errors.append(status)
                    summary[gateload][kpi] = latency
                    latencies[p].append(latency)
                requests_per_sec = self.metric_helper.calc_requests_per_sec(idx=idx)
                all_requests_per_sec.append(requests_per_sec)
                summary[gateload]['Average requests per sec'] = requests_per_sec
                doc_counters = self.metric_helper.calc_gateload_doc_counters(idx=idx)
                summary[gateload]['gateload doc counters'] = doc_counters
            logger.info('Per node summary: {}'.format(pretty_dict(summary)))

            self.reporter.post_to_sf(round(np.mean(latencies[99]), 1))

            self.pass_fail = []
            for p, criterion in criteria.items():
                kpi = self.KPI.format(p)
                average = np.mean(latencies[p])
                if average == 0 or average > criterion:
                    status = "{}: {} - doesn't meet the criteria of {}"\
                        .format(kpi, average, criterion)
                else:
                    status = '{}: {} - meets the criteria of {}'\
                        .format(kpi, average, criterion)
                self.pass_fail.append(status)
            logger.info(
                'Aggregated latency: {}'.format(pretty_dict(self.pass_fail))
            )

            network_matrix = self.metric_db_servers_helper.calc_network_throughput
            network_matrix['Avg requests  per sec'] = int(np.average(all_requests_per_sec))
            logger.info(
                'Network throughput: {}'.format(json.dumps(network_matrix, indent=4))
            )

            logger.info('Checking pass or fail')
            if self.errors:
                logger.interrupt('Test failed because of errors: {}'.format(self.errors))
            if "doesn't meet" in ''.join(self.pass_fail):
                logger.interrupt('Test failed: latencies do not meet KPI')
        except:
            traceback.print_exc()
            traceback.print_stack()
            logger.interrupt('Exception running test: {}'.format(str(sys.exc_info()[0])))

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
        for idx, gateload in enumerate(self.remote.gateloads, start=1):
            self.request_helper.wait_for_gateload_to_start(idx, gateload)

        self.remote.restart_seriesly()
        self.request_helper.wait_for_seriesly_to_start(self.test_config.gateload_settings.seriesly_host)
        self.start_samplers()

        log_phase('Gateload settings', self.test_config.gateload_settings)
        log_phase('Gateway settings', self.test_config.gateway_settings)
        log_phase('Stats settings', self.test_config.stats_settings)

        self.workload()

        self.remote.collect_profile_data_gateways()
        self.remote.collect_info_gateway()
        self.remote.collect_info_gateload()
        self.reporter.check_sgw_logs()
        self.reporter.save_expvar()

        return self.collect_kpi()


class PassFailGateloadTest(GateloadTest):

    def run(self):
        super(PassFailGateloadTest, self).run()
        if self.errors:
            logger.interrupt('Test failed because of errors')
        if 'doesn\'t meet' in ''.join(self.pass_fail):
            logger.interrupt('Test failed because at least one of the latencies does not meet KPI')

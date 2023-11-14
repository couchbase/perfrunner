import json
import os
import time
from typing import Any, Dict, List, Union

import requests

from logger import logger
from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.tableau import TableauTerminalHelper
from perfrunner.settings import SHOWFAST_HOST, ClusterSpec, TestConfig

JSON = Dict[str, Any]


class Reporter:

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 build: str,
                 sgw: bool = False):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.build = build + test_config.showfast.build_label
        self.master_node = next(self.cluster_spec.masters)
        self.rest = RestHelper(cluster_spec, test_config)
        self.sgw = sgw


class ShowFastReporter(Reporter):

    def _post_cluster(self):
        cluster = self.cluster_spec.parameters
        cluster['Name'] = self.cluster_spec.name

        logger.info('Adding a cluster: {}'.format(pretty_dict(cluster)))
        requests.post('http://{}/api/v1/clusters'.format(SHOWFAST_HOST),
                      json.dumps(cluster))

    def _post_metric(self, metric: JSON):
        cluster = self.cluster_spec.name
        component = self.test_config.showfast.component
        category = metric.get('category') or self.test_config.showfast.category
        sub_category = metric.get('subCategory') or self.test_config.showfast.sub_category

        if self.cluster_spec.capella_infrastructure:
            sub_category = sub_category.format(provider=self.cluster_spec.capella_backend.upper())
            if self.cluster_spec.serverless_infrastructure:
                metric['provider'] = "serverless"
            else:
                metric['provider'] = self.cluster_spec.cloud_provider.lower()

        metric.update({
            'cluster': cluster,
            'component': component,
            'category': category,
            'subCategory': sub_category,
        })

        logger.info('Adding a metric: {}'.format(pretty_dict(metric)))
        requests.post('http://{}/api/v1/metrics'.format(SHOWFAST_HOST),
                      json.dumps(metric))

    def _generate_benchmark(self,
                            metric: str,
                            value: Union[float, int],
                            snapshots: List[str]) -> JSON:

        build_str = self.build

        if self.test_config.sdktesting_settings.enable_sdktest:
            self.sdk_type = self.test_config.sdktesting_settings.sdk_type[-1]

            if self.sdk_type == 'java':
                # YCSB tests supply the Java SDK version in ycsb setting, use that if provided
                self.sdk_version = self.test_config.ycsb_settings.sdk_version or \
                    self.test_config.client_settings.sdk_version
            else:
                self.sdk_version = self.test_config.client_settings.sdk_version

            build_str = self.sdk_version + ' : ' + build_str

        if not self.cluster_spec.capella_infrastructure:
            if self.test_config.access_settings.show_tls_version or \
               self.test_config.backup_settings.show_tls_version or \
               self.test_config.restore_settings.show_tls_version:
                build_str = self.rest.get_minimum_tls_version(self.master_node) + ' : ' + build_str
                logger.info('build: {}'.format(self.build))

        if self.cluster_spec.capella_infrastructure and \
           self.test_config.cluster.show_cp_version:
            build_str = build_str + ' : ' + self.rest.get_cp_version()

        if self.test_config.tableau_settings.connector_vendor:
            tableau_helper = TableauTerminalHelper(self.test_config)
            connector_version = tableau_helper.get_connector_version()
            build_str = connector_version + ' : ' + build_str

        if (nebula_mode := self.test_config.access_settings.nebula_mode) != 'none':
            db_config = self.rest.get_db_info(self.test_config.buckets[0])
            if nebula_mode == 'nebula':
                nebula_version = db_config['dataplane']['nebula']['image']\
                    .removeprefix('direct-nebula-')
            elif nebula_mode == 'dapi':
                nebula_version = db_config['dataplane']['dataApi']['image']\
                    .removeprefix('couchbase-data-api-')

            build_str = nebula_version + ' : ' + build_str

        return {
            'build': build_str,
            'buildURL': os.environ.get('BUILD_URL'),
            'dateTime': time.strftime('%Y-%m-%d %H:%M'),
            'id': uhex(),
            'metric': metric,
            'snapshots': snapshots,
            'value': value,
        }

    @staticmethod
    def _log_benchmark(benchmark: JSON):
        logger.info('Dry run: {}'.format(pretty_dict(benchmark)))

    @staticmethod
    def _post_benchmark(benchmark: JSON):
        logger.info('Adding a benchmark: {}'.format(pretty_dict(benchmark)))
        requests.post('http://{}/api/v1/benchmarks'.format(SHOWFAST_HOST),
                      json.dumps(benchmark))

    def post(self,
             value: Union[float, int],
             snapshots: List[str],
             metric: JSON):
        metric['id'] = '{}_{}'.format(metric['id'], self.cluster_spec.name)
        benchmark = self._generate_benchmark(metric['id'], value, snapshots)

        if self.test_config.stats_settings.post_to_sf:
            self._post_benchmark(benchmark)
            self._post_metric(metric)
            self._post_cluster()
        else:
            self._log_benchmark(benchmark)
            self._log_benchmark(metric)


class DailyReporter(Reporter):

    @staticmethod
    def _post_daily_benchmark(benchmark: JSON):
        logger.info('Adding a benchmark: {}'.format(pretty_dict(benchmark)))
        requests.post(
            'http://{}/daily/api/v1/benchmarks'.format(SHOWFAST_HOST),
            json.dumps(benchmark))

    @staticmethod
    def _log_daily_benchmark(benchmark: JSON):
        logger.info('Dry run: {}'.format(pretty_dict(benchmark)))

    def post(self,
             metric: str,
             value: Union[float, int],
             snapshots: List[str]):
        benchmark = {
            'build': self.build,
            'buildURL': os.environ.get('BUILD_URL', ''),
            'component': self.test_config.showfast.component,
            'dateTime': time.strftime('%Y-%m-%d %H:%M'),
            'metric': metric,
            'snapshots': snapshots,
            'testCase': self.test_config.showfast.title,
            'threshold': self.test_config.showfast.threshold,
            'value': value,
        }

        if self.test_config.stats_settings.post_to_sf:
            self._post_daily_benchmark(benchmark)
        else:
            self._log_daily_benchmark(benchmark)

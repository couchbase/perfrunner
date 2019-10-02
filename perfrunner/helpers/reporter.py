import json
import os
import time
from typing import Any, Dict, List, Union

import requests

from logger import logger
from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import SHOWFAST_HOST, ClusterSpec, TestConfig

JSON = Dict[str, Any]


class Reporter:

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 build: str):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.build = build + test_config.showfast.build_label


class ShowFastReporter(Reporter):

    def _post_cluster(self):
        cluster = self.cluster_spec.parameters
        cluster['Name'] = self.cluster_spec.name

        logger.info('Adding a cluster: {}'.format(pretty_dict(cluster)))
        requests.post('http://{}/api/v1/clusters'.format(SHOWFAST_HOST),
                      json.dumps(cluster))

    def _post_metric(self, metric: JSON):
        if 'category' not in metric:
            metric['category'] = self.test_config.showfast.category

        metric.update({
            'cluster': self.cluster_spec.name,
            'component': self.test_config.showfast.component,
            'subCategory': self.test_config.showfast.sub_category,
        })

        logger.info('Adding a metric: {}'.format(pretty_dict(metric)))
        requests.post('http://{}/api/v1/metrics'.format(SHOWFAST_HOST),
                      json.dumps(metric))

    def _generate_benchmark(self,
                            metric: str,
                            value: Union[float, int],
                            snapshots: List[str]) -> JSON:

        if self.test_config.sdktesting_settings.enable_sdktest:
            self.sdk_version = self.test_config.ycsb_settings.sdk_version
            self.build = self.sdk_version + ' : ' + self.build

        return {
            'build': self.build,
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

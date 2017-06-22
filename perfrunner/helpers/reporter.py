import json
import os
import time
from typing import Any, Dict, List, Union

import requests
from logger import logger

from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import ClusterSpec, StatsSettings, TestConfig

JSON = Dict[str, Any]


class Reporter:

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 build: str):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.build = build


class ShowFastReporter(Reporter):

    def _post_cluster(self) -> None:
        cluster = self.cluster_spec.parameters
        cluster['Name'] = self.cluster_spec.name

        logger.info('Adding a cluster: {}'.format(pretty_dict(cluster)))
        requests.post('http://{}/api/v1/clusters'.format(StatsSettings.SHOWFAST),
                      json.dumps(cluster))

    def _post_metric(self, metric: JSON) -> None:
        if 'category' not in metric:
            metric['category'] = self.test_config.test_case.category

        metric.update({
            'cluster': self.cluster_spec.name,
            'component': self.test_config.test_case.component,
            'subCategory': self.test_config.test_case.sub_category,
        })

        logger.info('Adding a metric: {}'.format(pretty_dict(metric)))
        requests.post('http://{}/api/v1/metrics'.format(StatsSettings.SHOWFAST),
                      json.dumps(metric))

    def _generate_benchmark(self,
                            metric: str,
                            value: Union[float, int],
                            snapshots: List[str]) -> JSON:
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
    def _log_benchmark(benchmark: JSON) -> None:
        logger.info('Dry run: {}'.format(pretty_dict(benchmark)))

    @staticmethod
    def _post_benchmark(benchmark: JSON) -> None:
        logger.info('Adding a benchmark: {}'.format(pretty_dict(benchmark)))
        requests.post('http://{}/api/v1/benchmarks'.format(StatsSettings.SHOWFAST),
                      json.dumps(benchmark))

    def post(self,
             value: Union[float, int],
             snapshots: List[str],
             metric: JSON) -> None:
        metric['id'] = '{}_{}'.format(metric['id'], self.cluster_spec.name)
        benchmark = self._generate_benchmark(metric['id'], value, snapshots)

        if self.test_config.stats_settings.post_to_sf:
            self._post_benchmark(benchmark)
            self._post_metric(metric)
            self._post_cluster()
        else:
            self._log_benchmark(benchmark)


class DailyReporter(Reporter):

    @staticmethod
    def _post_daily_benchmark(benchmark: JSON) -> None:
        logger.info('Adding a benchmark: {}'.format(pretty_dict(benchmark)))
        requests.post(
            'http://{}/daily/api/v1/benchmarks'.format(StatsSettings.SHOWFAST),
            json.dumps(benchmark))

    @staticmethod
    def _log_daily_benchmark(benchmark: JSON) -> None:
        logger.info('Dry run: {}'.format(pretty_dict(benchmark)))

    def post(self,
             metric: str,
             value: Union[float, int],
             snapshots: List[str]) -> None:
        benchmark = {
            'build': self.build,
            'buildURL': os.environ.get('BUILD_URL', ''),
            'component': self.test_config.test_case.component,
            'dateTime': time.strftime('%Y-%m-%d %H:%M'),
            'metric': metric,
            'snapshots': snapshots,
            'testCase': self.test_config.test_case.title,
            'threshold': self.test_config.test_case.threshold,
            'value': value,
        }

        if self.test_config.stats_settings.post_to_sf:
            self._post_daily_benchmark(benchmark)
        else:
            self._log_daily_benchmark(benchmark)

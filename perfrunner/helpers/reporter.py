import json
import os
import time
from datetime import datetime
from zipfile import ZIP_DEFLATED, ZipFile

import requests
from couchbase.bucket import Bucket
from logger import logger
from pytz import timezone

from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import StatsSettings


class SFReporter(object):

    def __init__(self, test):
        self.test = test

    def _post_cluster(self):
        cluster = self.test.cluster_spec.parameters
        cluster['Name'] = self.test.cluster_spec.name

        logger.info('Adding a cluster: {}'.format(pretty_dict(cluster)))
        requests.post('http://{}/api/v1/clusters'.format(StatsSettings.SHOWFAST),
                      json.dumps(cluster))

    def _post_metric(self, metric, metric_info):
        if metric_info is None:
            metric_info = {
                'cluster': self.test.cluster_spec.name,
                'title': self.test.test_config.test_case.title,
            }
        metric_info['id'] = metric

        logger.info('Adding a metric: {}'.format(pretty_dict(metric_info)))
        requests.post('http://{}/api/v1/metrics'.format(StatsSettings.SHOWFAST),
                      json.dumps(metric_info))

    def _generate_benchmark(self, metric, value):
        return {
            'build': self.test.build,
            'buildURL': os.environ.get('BUILD_URL'),
            'dateTime': time.strftime('%Y-%m-%d %H:%M'),
            'id': uhex(),
            'metric': metric,
            'snapshots': self.test.snapshots,
            'value': value,
        }

    def _log_benchmark(self, metric, value):
        benchmark = self._generate_benchmark(metric, value)

        logger.info('Dry run stats: {}'.format(pretty_dict(benchmark)))

    def _post_benchmark(self, metric, value):
        benchmark = self._generate_benchmark(metric, value)

        logger.info('Adding a benchmark: {}'.format(pretty_dict(benchmark)))
        requests.post('http://{}/api/v1/benchmarks'.format(StatsSettings.SHOWFAST),
                      json.dumps(benchmark))

    def post_to_sf(self, value, metric=None, metric_info=None):
        if metric is None:
            metric = '{}_{}'.format(self.test.test_config.name,
                                    self.test.cluster_spec.name)

        if self.test.test_config.stats_settings.post_to_sf:
            self._post_benchmark(metric, value)
            self._post_metric(metric, metric_info)
            self._post_cluster()
        else:
            self._log_benchmark(metric, value)

    def _upload_test_run_dailyp(self, test_run_dict):
        try:
            bucket = Bucket('couchbase://{}/perf_daily'
                            .format(StatsSettings.CBMONITOR))
        except Exception as e:
            logger.info("Post to Dailyp, DB connection error: {}".format(e.message))
            return False
        doc_id = "{}__{}__{}__{}__{}".format(test_run_dict['category'],
                                             test_run_dict['subcategory'],
                                             test_run_dict['test'],
                                             test_run_dict['build'],
                                             test_run_dict['datetime'])
        bucket.upsert(doc_id, test_run_dict)
        return True

    def post_to_dailyp(self, metrics):
        test_title = self.test.test_config.test_case.title
        test_name = test_title.replace(', ', '_')
        replace_chars = ", =/.`\\"
        for c in replace_chars:
            test_name = test_name.replace(c, "_")

        snapshot_links = list()
        snapshot_host = "http://{}/reports/html/?snapshot=".\
            format(StatsSettings.CBMONITOR)
        for snapshot in self.test.cbagent.snapshots:
            snapshot_link = snapshot_host + snapshot
            snapshot_links.append(snapshot_link)

        if self.test.test_config.dailyp_settings.subcategory is not None:
            category_full_name = "{}-{}".format(self.test.test_config.dailyp_settings.category,
                                                self.test.test_config.dailyp_settings.subcategory)
        else:
            category_full_name = self.test.test_config.dailyp_settings.category

        post_body = {
            "category": category_full_name,
            "subcategory": self.test.test_config.dailyp_settings.subcategory,
            "test_title": test_title,
            "datetime": datetime.now(timezone('US/Pacific')).strftime("%Y_%m_%d-%H:%M"),
            "build": self.test.build,
            "test": test_name,
            "metrics": metrics,
            "snapshots": snapshot_links,
        }

        if self._upload_test_run_dailyp(post_body):
            logger.info("Successfully posted to Dailyp {}".format(post_body))
        else:
            logger.warn("Failed to post to Dailyp {}".format(post_body))


class LogReporter(object):

    def __init__(self, test):
        self.test = test

    def save_master_events(self):
        with ZipFile('master_events.zip', 'w', ZIP_DEFLATED) as zh:
            for master in self.test.cluster_spec.yield_masters():
                master_events = self.test.rest.get_master_events(master)
                self.test.master_events.append(master_events)
                fname = 'master_events_{}.log'.format(master.split(':')[0])
                zh.writestr(zinfo_or_arcname=fname, bytes=master_events)


class Reporter(SFReporter, LogReporter):

    def start(self):
        self.ts = time.time()

    def finish(self, action, time_elapsed=None):
        time_elapsed = time_elapsed or (time.time() - self.ts)
        time_elapsed = round(time_elapsed / 60, 2)
        logger.info(
            'Time taken to perform "{}": {} min'.format(action, time_elapsed)
        )
        return time_elapsed

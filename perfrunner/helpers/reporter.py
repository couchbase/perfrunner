import os
import time
from datetime import datetime
from zipfile import ZIP_DEFLATED, ZipFile

from couchbase import Couchbase
from couchbase.bucket import Bucket
from logger import logger
from pytz import timezone

from perfrunner.helpers.misc import pretty_dict, uhex


class SFReporter(object):

    def __init__(self, test):
        self.test = test

    def _add_cluster(self):
        cluster = self.test.cluster_spec.name
        params = self.test.cluster_spec.parameters
        showfast = self.test.test_config.stats_settings.showfast
        try:
            cb = Couchbase.connect(bucket='clusters', **showfast)
            cb.set(cluster, params)
        except Exception as e:
            logger.warn('Failed to add cluster, {}'.format(e))
        else:
            logger.info('Successfully posted: {}, {}'.format(
                cluster, pretty_dict(params)
            ))

    def _add_metric(self, metric, metric_info):
        if metric_info is None:
            metric_info = {
                'title': self.test.test_config.test_case.metric_title,
                'cluster': self.test.cluster_spec.name,
                'larger_is_better': self.test.test_config.test_case.larger_is_better,
            }
        showfast = self.test.test_config.stats_settings.showfast
        try:
            cb = Couchbase.connect(bucket='metrics', **showfast)
            cb.set(metric, metric_info)
        except Exception as e:
            logger.warn('Failed to add cluster, {}'.format(e))
        else:
            logger.info('Successfully posted: {}, {}'.format(
                metric, pretty_dict(metric_info)
            ))

    def _prepare_data(self, metric, value):
        key = uhex()
        data = {
            'build': self.test.build,
            'metric': metric,
            'value': value,
            'snapshots': self.test.snapshots,
            'build_url': os.environ.get('BUILD_URL'),
            'datetime': time.strftime('%Y-%m-%d %H:%M'),
        }
        return key, data

    @staticmethod
    def _mark_previous_as_obsolete(cb, benchmark):
        for row in cb.query('benchmarks', 'values_by_build_and_metric',
                            key=[benchmark['metric'], benchmark['build']]):
            doc = cb.get(row.docid)
            doc.value.update({'obsolete': True})
            cb.set(row.docid, doc.value)

    def _log_benchmark(self, metric, value):
        key, benchmark = self._prepare_data(metric, value)
        logger.info('Dry run stats: {}'.format(
            pretty_dict(benchmark)
        ))
        return key

    def _post_benchmark(self, metric, value):
        key, benchmark = self._prepare_data(metric, value)
        showfast = self.test.test_config.stats_settings.showfast

        cb = Couchbase.connect(bucket='benchmarks', **showfast)
        try:
            self._mark_previous_as_obsolete(cb, benchmark)
            cb.set(key, benchmark)
        except Exception as e:
            logger.warn('Failed to post benchmark {}: {}'.format(e, benchmark))
        else:
            logger.info('Successfully posted: {}'.format(pretty_dict(benchmark)))

        return key

    def post_to_sf(self, value, metric=None, metric_info=None):
        if metric is None:
            metric = '{}_{}'.format(self.test.test_config.name,
                                    self.test.cluster_spec.name)
        stats_settings = self.test.test_config.stats_settings
        if stats_settings.post_to_sf:
            self._add_metric(metric, metric_info)
            self._add_cluster()
            self._post_benchmark(metric, value)
        else:
            self._log_benchmark(metric, value)
        return value

    def _upload_test_run_dailyp(self, test_run_dict):
        try:
            bucket = Bucket('couchbase://{}/perf_daily'.
                            format(self.test.test_config.stats_settings.cbmonitor['host']))
        except Exception as e:
            logger.info("Post to Dailyp, DB connection error: {}".format(e.message))
            return False
        docid = "{}__{}__{}__{}__{}".format(test_run_dict['category'],
                                            test_run_dict['subcategory'],
                                            test_run_dict['test'],
                                            test_run_dict['build'],
                                            test_run_dict['datetime'])
        bucket.upsert(docid, test_run_dict)
        return True

    def post_to_dailyp(self, metrics):
        test_title = self.test.test_config.test_case.metric_title
        test_name = test_title.replace(', ', '_')
        replace_chars = ", =/.`\\"
        for c in replace_chars:
            test_name = test_name.replace(c, "_")

        snapshot_links = list()
        snapshot_host = "http://{}/reports/html/?snapshot=".\
                        format(self.test.test_config.stats_settings.cbmonitor['host'])
        for snapshot in self.test.cbagent.snapshots:
            snapshot_link = snapshot_host + snapshot
            snapshot_links.append(snapshot_link)

        if self.test.test_config.dailyp_settings.subcategory is not None:
            category_full_name = "{}-{}".format(self.test.test_config.dailyp_settings.category,
                                                self.test.test_config.dailyp_settings.subcategory)
        else:
            category_full_name = self.test.test_config.dailyp_settings.category

        post_body = {"category": category_full_name,
                     "subcategory": self.test.test_config.dailyp_settings.subcategory,
                     "test_title": test_title,
                     "datetime": datetime.now(timezone('US/Pacific')).strftime("%Y_%m_%d-%H:%M"),
                     "build": self.test.build,
                     "test": test_name,
                     "metrics": metrics,
                     "snapshots": snapshot_links
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

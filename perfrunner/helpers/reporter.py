import time
from datetime import datetime
from zipfile import ZipFile, ZIP_DEFLATED

import requests
from btrc import CouchbaseClient, StatsReporter
from couchbase import Couchbase
from logger import logger

from perfrunner.helpers.misc import uhex, pretty_dict
from perfrunner.settings import CBMONITOR_HOST, SF_STORAGE


class BtrcReporter(object):

    def __init__(self, test):
        self.test = test

    def reset_utilzation_stats(self):
        for target in self.test.target_iterator:
            logger.info('Resetting utilization stats from {}/{}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket,
                                 target.username, target.password)
            cb.reset_utilization_stats()

    def save_utilzation_stats(self):
        for target in self.test.target_iterator:
            logger.info('Saving utilization stats from {}/{}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket,
                                 target.username, target.password)
            reporter = StatsReporter(cb)
            reporter.report_stats('util_stats')

    def save_btree_stats(self):
        for target in self.test.target_iterator:
            logger.info('Saving B-tree stats from {}/{}'.format(
                        target.node, target.bucket))
            cb = CouchbaseClient(target.node, target.bucket,
                                 target.username, target.password)
            reporter = StatsReporter(cb)
            reporter.report_stats('btree_stats')


class Comparator(object):

    CONFIDENCE_THRESHOLD = 50

    def get_snapshots(self, benckmark):
        """Get all snapshots ordered by build version for given benchmark"""
        self.snapshots_by_build = dict()
        for row in self.cbb.query('benchmarks', 'build_and_snapshots_by_metric',
                                  key=benckmark['metric'], stale='false'):
            self.snapshots_by_build[row.value[0]] = row.value[1]

    def find_previous(self, new_build):
        """Find previous build within current release and latest build from
        previous release"""
        all_builds = sorted(self.snapshots_by_build.keys(), reverse=True)

        self.prev_release = None
        self.prev_build = None
        for build in all_builds[all_builds.index(new_build) + 1:]:
            if build.startswith(new_build.split('-')[0]):
                if not self.prev_build:
                    self.prev_build = build
            else:
                self.prev_release = build
                break

    def compare(self, new_build):
        """Compare snapshots if possible"""
        api = 'http://{}/reports/compare/'.format(CBMONITOR_HOST)
        snapshot_api = 'http://{}/reports/html/?snapshot={{}}&snapshot={{}}'\
            .format(CBMONITOR_HOST)

        changes = []
        reports = []
        for build in filter(lambda _: _, (self.prev_build, self.prev_release)):
            baselines = self.snapshots_by_build[build]
            targets = self.snapshots_by_build[new_build]
            if baselines and targets and len(baselines) == len(targets):
                for baseline, target in zip(baselines, targets):
                    params = {'baseline': baseline, 'target': target}
                    comparison = requests.get(url=api, params=params).json()
                    diff = tuple({
                        m for m, confidence in comparison
                        if confidence > self.CONFIDENCE_THRESHOLD
                    })
                    changes.append((build, diff))
                    snapshots_url = snapshot_api.format(baseline, target)
                    reports.append((build, snapshots_url))

        # Prefetch (trigger) HTML reports
        for _, url in reports:
            requests.get(url=url)

        return {'changes': changes, 'reports': reports}

    def __call__(self, test, benckmark):
        try:
            self.cbb = Couchbase.connect(bucket='benchmarks', **SF_STORAGE)
            self.cbf = Couchbase.connect(bucket='feed', **SF_STORAGE)
        except Exception, e:
            logger.warn('Failed to connect to database, {}'.format(e))
            return

        self.get_snapshots(benckmark)
        self.find_previous(new_build=benckmark['build'])

        # Feed record
        _id = str(int(time.time() * 10 ** 6))
        base_feed = {
            'build': benckmark['build'],
            'cluster': test.cluster_spec.name,
            'summary': test.test_config.test_summary,
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        changes = self.compare(new_build=benckmark['build'])
        feed = dict(base_feed, **changes)
        self.cbf.set(_id, feed)
        logger.info('Snapshot comparison: {}'.format(pretty_dict(feed)))


class SFReporter(object):

    def __init__(self, test):
        self.test = test

    def _add_cluster(self):
        cluster = self.test.cluster_spec.name
        params = self.test.cluster_spec.parameters
        try:
            cb = Couchbase.connect(bucket='clusters', **SF_STORAGE)
            cb.set(cluster, params)
        except Exception, e:
            logger.warn('Failed to add cluster, {}'.format(e))
        else:
            logger.info('Successfully posted: {}, {}'.format(
                cluster, pretty_dict(params)
            ))

    def _add_metric(self, metric, metric_info):
        if metric_info is None:
            metric_info = {
                'title': self.test.test_config.metric_title,
                'cluster': self.test.cluster_spec.name,
                'larger_is_better': self.test.test_config.regression_criterion,
                'level': self.test.test_config.level,
            }
        try:
            cb = Couchbase.connect(bucket='metrics', **SF_STORAGE)
            cb.set(metric, metric_info)
        except Exception, e:
            logger.warn('Failed to add cluster, {}'.format(e))
        else:
            logger.info('Successfully posted: {}, {}'.format(
                metric, pretty_dict(metric_info)
            ))

    def _prepare_data(self, metric, value):
        key = uhex()
        master = self.test.cluster_spec.yield_masters().next()
        build = self.test.rest.get_version(master)
        data = {'build': build, 'metric': metric, 'value': value,
                'snapshots': self.test.snapshots}
        return key, data

    @staticmethod
    def _mark_previous_as_obsolete(cb, benckmark):
        for row in cb.query('benchmarks', 'values_by_build_and_metric',
                            key=[benckmark['metric'], benckmark['build']]):
            doc = cb.get(row.docid)
            doc.value.update({'obsolete': True})
            cb.set(row.docid, doc.value)

    def _log_benchmark(self, metric, value):
        _, benckmark = self._prepare_data(metric, value)
        logger.info('Dry run stats: {}'.format(
            pretty_dict(benckmark)
        ))

    def _post_benckmark(self, metric, value):
        key, benckmark = self._prepare_data(metric, value)
        try:
            cb = Couchbase.connect(bucket='benchmarks', **SF_STORAGE)
            self._mark_previous_as_obsolete(cb, benckmark)
            cb.set(key, benckmark)
            Comparator()(test=self.test, benckmark=benckmark)
        except Exception, e:
            logger.warn('Failed to post results, {}'.format(e))
        else:
            logger.info('Successfully posted: {}'.format(
                pretty_dict(benckmark)
            ))

    def post_to_sf(self, value, metric=None, metric_info=None):
        if metric is None:
            metric = '{}_{}'.format(self.test.test_config.name,
                                    self.test.cluster_spec.name)

        stats_settings = self.test.test_config.stats_settings

        if stats_settings.post_to_sf:
            self._add_metric(metric, metric_info)
            self._add_cluster()
            self._post_benckmark(metric, value)
        else:
            self._log_benchmark(metric, value)
        return value


class LogReporter(object):

    def __init__(self, test):
        self.test = test

    def save_web_logs(self):
        for master in self.test.cluster_spec.yield_masters():
            logs = self.test.rest.get_logs(master)
            fname = 'web_log_{}.json'.format(master.split(':')[0])
            with open(fname, 'w') as fh:
                fh.write(pretty_dict(logs))

    def save_master_events(self):
        with ZipFile('master_events.zip', 'w', ZIP_DEFLATED) as zh:
            for master in self.test.cluster_spec.yield_masters():
                master_events = self.test.rest.get_master_events(master)
                fname = 'master_events_{}.log'.format(master.split(':')[0])
                zh.writestr(zinfo_or_arcname=fname, bytes=master_events)


class Reporter(BtrcReporter, SFReporter, LogReporter):

    def start(self):
        self.ts = time.time()

    def finish(self, action, time_elapsed=None):
        time_elapsed = time_elapsed or (time.time() - self.ts)
        time_elapsed = round(time_elapsed / 60, 1)
        logger.info(
            'Time taken to perform "{}": {} min'.format(action, time_elapsed)
        )
        return time_elapsed

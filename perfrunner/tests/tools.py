import time

import requests
from logger import logger

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest
from perfrunner.utils.install import CouchbaseInstaller


class Options(object):

    cluster_edition = None
    url = None
    verbose = False
    version = None


class BackupRestoreTest(PerfTest):

    """
    The base CB backup /restore class.
    """

    def download_tools(self):
        options = Options()
        options.version = self.build
        if self.rest.is_community(self.master_node):
            options.cluster_edition = 'community'
        else:
            options.cluster_edition = 'enterprise'

        installer = CouchbaseInstaller(self.cluster_spec, options)

        filename, url = installer.find_package()

        logger.info('Downloading "{}"'.format(url))
        with open(filename, 'w') as fh:
            resp = requests.get(url)
            fh.write(resp.content)

        local.extract_cb(filename)

    def run(self):
        self.download_tools()

        self.load()
        self.wait_for_persistence()


class BackupTest(BackupRestoreTest):

    """
    After a typical workload we backup all nodes and measure time it takes to
    perform backup.
    """

    @with_stats
    def backup(self, mode=None):
        local.backup(
            master_node=self.master_node,
            cluster_spec=self.cluster_spec,
            wrapper=self.rest.is_community(self.master_node),
            mode=mode,
            compression=self.test_config.backup_settings.compression,
            skip_compaction=self.build >= '4.7.0-1082',  # MB-20768
        )

    def _report_kpi(self):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backup_size = local.calc_backup_size(self.cluster_spec)

        self.reporter.post_to_sf(
            *self.metric_helper.calc_bnr_throughput(self.time_elapsed,
                                                    edition,
                                                    tool='backup')
        )

        self.reporter.post_to_sf(
            *self.metric_helper.calc_backup_size(backup_size, edition)
        )

    def run(self):
        super(BackupTest, self).run()

        from_ts, to_ts = self.backup()
        self.time_elapsed = (to_ts - from_ts) / 1000.0  # seconds

        self.report_kpi()


class BackupUnderLoadTest(BackupTest):

    """
    After a typical workload we backup all nodes and measure time it takes to
    perform backup when workload running.
    """

    def run(self):
        super(BackupTest, self).run()

        self.hot_load()

        self.access_bg()

        self.backup()

        self.report_kpi()


class IncrementalBackupUnderLoadTest(BackupTest):

    """
    After a typical workload we backup all nodes then perform incremental backup
    when workload running.
    """

    MUTATION_TIME = 300

    def run(self):
        super(BackupTest, self).run()

        self.backup()

        self.access_bg()
        time.sleep(self.MUTATION_TIME)

        self.backup(mode='diff')

        self.report_kpi()


class RestoreTest(BackupTest):

    """
    After a typical workload we backup all nodes then restore and measure time
    it takes to perform restore.
    """

    @with_stats
    def restore(self):
        local.restore(cluster_spec=self.cluster_spec,
                      master_node=self.master_node,
                      wrapper=self.rest.is_community(self.master_node))

    def flush_buckets(self):
        for i in range(self.test_config.cluster.num_buckets):
            bucket = 'bucket-{}'.format(i + 1)
            self.rest.flush_bucket(host_port=self.master_node, name=bucket)

    def _report_kpi(self):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'

        self.reporter.post_to_sf(
            *self.metric_helper.calc_bnr_throughput(self.time_elapsed,
                                                    edition,
                                                    tool='restore')
        )

    def run(self):
        super(BackupTest, self).run()

        self.backup()

        self.flush_buckets()

        from_ts, to_ts = self.restore()
        self.time_elapsed = (to_ts - from_ts) / 1000.0  # seconds

        self.report_kpi()


class RestoreAfterIncrementalBackupTest(RestoreTest):

    """
    After a typical workload we backup all nodes. After access load we backup
    again (incremental backup).

    Incremental Restore and measure time it takes to perform restore.
    """

    def run(self):
        super(BackupTest, self).run()

        self.backup()

        workload = self.test_config.access_settings
        workload.seq_updates = True
        self.access(workload)

        self.backup(mode='diff')

        self.flush_buckets()

        self.restore()

        self.report_kpi()

import os
import time

import requests
from logger import logger

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest
from perfrunner.utils.install import CouchbaseInstaller


class BackupRestoreTest(PerfTest):

    """
    The base CB backup /restore class.
    """

    def download_tools(self):
        if self.rest.is_community(self.master_node):
            edition = 'community'
        else:
            edition = 'enterprise'

        options = type("Options", (), {"verbose": False, "version": self.build})
        installer = CouchbaseInstaller(self.cluster_spec, options=options)
        url = installer.find_package(edition=edition)

        logger.info('Downloading "{}"'.format(url))
        with open('couchbase.rpm', 'wb') as fh:
            resp = requests.get(url)
            fh.write(resp.content)

        local.extract_cb(filename='couchbase.rpm')

    def flush_buckets(self):
        for i in range(self.test_config.cluster.num_buckets):
            bucket = 'bucket-{}'.format(i + 1)
            self.rest.flush_bucket(host_port=self.master_node, bucket=bucket)

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
    @timeit
    def backup(self, mode=None):
        local.backup(
            master_node=self.master_node,
            cluster_spec=self.cluster_spec,
            wrapper=self.rest.is_community(self.master_node),
            mode=mode,
            compression=self.test_config.backup_settings.compression,
        )

    def _report_kpi(self, time_elapsed):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backup_size = local.calc_backup_size(self.cluster_spec)

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed,
                                         edition,
                                         tool='backup')
        )

        self.reporter.post(
            *self.metrics.backup_size(backup_size, edition)
        )

    def run(self):
        super().run()

        time_elapsed = self.backup()

        self.report_kpi(time_elapsed)


class BackupSizeTest(BackupTest):

    def _report_kpi(self, *args):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backup_size = local.calc_backup_size(self.cluster_spec)

        self.reporter.post(
            *self.metrics.backup_size(backup_size, edition)
        )


class BackupUnderLoadTest(BackupTest):

    """
    After a typical workload we backup all nodes and measure time it takes to
    perform backup when workload running.
    """

    def run(self):
        super(BackupTest, self).run()

        self.hot_load()

        self.access_bg()

        time_elapsed = self.backup()

        self.report_kpi(time_elapsed)


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

        time_elapsed = self.backup(mode='diff')

        self.report_kpi(time_elapsed)


class RestoreTest(BackupTest):

    """
    After a typical workload we backup all nodes then restore and measure time
    it takes to perform restore.
    """

    @with_stats
    @timeit
    def restore(self):
        local.restore(cluster_spec=self.cluster_spec,
                      master_node=self.master_node,
                      wrapper=self.rest.is_community(self.master_node))

    def _report_kpi(self, time_elapsed):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed,
                                         edition,
                                         tool='restore')
        )

    def run(self):
        super(BackupTest, self).run()

        self.backup()

        self.flush_buckets()

        time_elapsed = self.restore()

        self.report_kpi(time_elapsed)


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

        time_elapsed = self.restore()

        self.report_kpi(time_elapsed)


class ExportImportTest(BackupRestoreTest):

    @with_stats
    @timeit
    def export(self):
        local.cbexport(master_node=self.master_node,
                       cluster_spec=self.cluster_spec,
                       bucket=self.test_config.buckets[0],
                       data_format=self.test_config.export_settings.format)

    @with_stats
    @timeit
    def import_data(self):
        import_file = self.test_config.export_settings.import_file
        if import_file is None:
            import_file = 'data.{}'.format(self.test_config.export_settings.type)
            import_file = os.path.join(self.cluster_spec.backup, import_file)
        if self.test_config.export_settings.format != 'sample':
            import_file = 'file://{}'.format(import_file)

        local.cbimport(master_node=self.master_node,
                       cluster_spec=self.cluster_spec,
                       data_type=self.test_config.export_settings.type,
                       data_format=self.test_config.export_settings.format,
                       bucket=self.test_config.buckets[0],
                       import_file=import_file)

    def _report_kpi(self, time_elapsed: float):
        self.reporter.post(
            *self.metrics.import_and_export_throughput(time_elapsed)
        )


class ExportTest(ExportImportTest):

    def run(self):
        super().run()

        time_elapsed = self.export()

        self.report_kpi(time_elapsed)


class ImportTest(ExportImportTest):

    def run(self):
        super().run()

        self.export()

        self.flush_buckets()

        time_elapsed = self.import_data()

        self.report_kpi(time_elapsed)


class ImportSampleDataTest(ExportImportTest):

    def _report_kpi(self, time_elapsed: float):
        self.reporter.post(
            *self.metrics.import_file_throughput(time_elapsed)
        )

    def run(self):
        self.download_tools()

        time_elapsed = self.import_data()

        self.report_kpi(time_elapsed)

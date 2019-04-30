import os

from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest


class BackupRestoreTest(PerfTest):

    def extract_tools(self):
        local.extract_cb(filename='couchbase.rpm')

    def flush_buckets(self):
        for i in range(self.test_config.cluster.num_buckets):
            bucket = 'bucket-{}'.format(i + 1)
            self.rest.flush_bucket(self.master_node, bucket)

    def backup(self, mode=None):
        local.backup(
            master_node=self.master_node,
            cluster_spec=self.cluster_spec,
            threads=self.test_config.backup_settings.threads,
            wrapper=self.rest.is_community(self.master_node),
            mode=mode,
            compression=self.test_config.backup_settings.compression,
            storage_type=self.test_config.backup_settings.storage_type,
            sink_type=self.test_config.backup_settings.sink_type,
            shards=self.test_config.backup_settings.shards
        )

    def compact(self):
        snapshots = local.get_backup_snapshots(self.cluster_spec)
        local.compact(self.cluster_spec,
                      snapshots,
                      self.rest.is_community(self.master_node)
                      )

    def restore(self):
        local.drop_caches()

        local.restore(cluster_spec=self.cluster_spec,
                      master_node=self.master_node,
                      threads=self.test_config.restore_settings.threads,
                      wrapper=self.rest.is_community(self.master_node))

    def run(self):
        self.extract_tools()

        self.load()
        self.wait_for_persistence()


class BackupTest(BackupRestoreTest):

    @with_stats
    @timeit
    def backup(self, mode=None):
        super().backup(mode)

    def _report_kpi(self, time_elapsed):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backup_size = local.calc_backup_size(self.cluster_spec)

        backing_store = self.test_config.backup_settings.storage_type
        sink_type = self.test_config.backup_settings.sink_type

        tool = 'backup'
        if backing_store:
            tool += '-' + backing_store
        elif sink_type:
            tool += '-' + sink_type

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed,
                                         edition,
                                         tool=tool)
        )

        self.reporter.post(
            *self.metrics.backup_size(backup_size,
                                      edition,
                                      tool=tool if backing_store or sink_type
                                      else None)
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


class BackupTestWithCompact(BackupTest):

    @with_stats
    @timeit
    def backup(self, mode=None):
        super().backup(mode)
        super().compact()


class BackupUnderLoadTest(BackupTest):

    def run(self):
        super(BackupTest, self).run()

        self.hot_load()

        self.access_bg()

        time_elapsed = self.backup()

        self.report_kpi(time_elapsed)


class MergeTest(BackupRestoreTest):

    @with_stats
    @timeit
    def merge(self):
        snapshots = local.get_backup_snapshots(self.cluster_spec)

        local.drop_caches()

        local.cbbackupmgr_merge(self.cluster_spec, snapshots)

    def _report_kpi(self, time_elapsed):
        self.reporter.post(
            *self.metrics.merge_throughput(time_elapsed)
        )

    def run(self):
        self.extract_tools()

        self.load()
        self.wait_for_persistence()
        self.backup()  # 1st snapshot

        self.flush_buckets()

        self.load()
        self.wait_for_persistence()
        self.backup(mode=True)  # 2nd snapshot

        time_elapsed = self.merge()

        self.report_kpi(time_elapsed)


class RestoreTest(BackupRestoreTest):

    @with_stats
    @timeit
    def restore(self):
        super().restore()

    def _report_kpi(self, time_elapsed):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'

        backing_store = self.test_config.backup_settings.storage_type
        sink_type = self.test_config.backup_settings.sink_type

        tool = 'restore'
        if backing_store:
            tool += '-' + backing_store
        elif sink_type:
            tool += '-' + sink_type

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed,
                                         edition,
                                         tool=tool)
        )

    def run(self):
        super().run()

        self.backup()

        self.flush_buckets()

        time_elapsed = self.restore()

        self.report_kpi(time_elapsed)


class ExportImportTest(BackupRestoreTest):

    def export(self):
        local.cbexport(master_node=self.master_node,
                       cluster_spec=self.cluster_spec,
                       bucket=self.test_config.buckets[0],
                       data_format=self.test_config.export_settings.format,
                       threads=self.test_config.restore_settings.threads)

    def import_data(self):
        import_file = self.test_config.export_settings.import_file
        if import_file is None:
            import_file = 'data.{}'.format(self.test_config.export_settings.type)
            import_file = os.path.join(self.cluster_spec.backup, import_file)
        if self.test_config.export_settings.format != 'sample':
            import_file = 'file://{}'.format(import_file)

        local.drop_caches()

        local.cbimport(master_node=self.master_node,
                       cluster_spec=self.cluster_spec,
                       data_type=self.test_config.export_settings.type,
                       data_format=self.test_config.export_settings.format,
                       bucket=self.test_config.buckets[0],
                       import_file=import_file,
                       threads=self.test_config.backup_settings.threads)

    def _report_kpi(self, time_elapsed: float):
        self.reporter.post(
            *self.metrics.import_and_export_throughput(time_elapsed)
        )


class ExportTest(ExportImportTest):

    @with_stats
    @timeit
    def export(self):
        super().export()

    def run(self):
        super().run()

        time_elapsed = self.export()

        self.report_kpi(time_elapsed)


class ImportTest(ExportImportTest):

    @with_stats
    @timeit
    def import_data(self):
        super().import_data()

    def run(self):
        super().run()

        self.export()

        self.flush_buckets()

        time_elapsed = self.import_data()

        self.report_kpi(time_elapsed)


class ImportSampleDataTest(ImportTest):

    def _report_kpi(self, time_elapsed: float):
        self.reporter.post(
            *self.metrics.import_file_throughput(time_elapsed)
        )

    def run(self):
        self.extract_tools()

        time_elapsed = self.import_data()

        self.report_kpi(time_elapsed)

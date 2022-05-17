import os

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.settings import LoadSettings, TargetIterator
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
            shards=self.test_config.backup_settings.shards,
            include_data=self.test_config.backup_settings.include_data,
            use_tls=self.test_config.backup_settings.use_tls
        )

    def compact(self):
        # Pre build 6.5.0-3524 there was no threads flag in compact. To ensure
        # tests run across versions, omit this flag pre this build.
        version, build_number = self.build.split('-')
        build = tuple(map(int, version.split('.'))) + (int(build_number),)

        if build < (6, 5, 0, 3524):
            threads = None
        else:
            threads = self.test_config.backup_settings.threads

        snapshots = local.get_backup_snapshots(self.cluster_spec)
        local.compact(self.cluster_spec,
                      snapshots,
                      threads,
                      self.rest.is_community(self.master_node))

    def restore(self):
        local.drop_caches()

        local.restore(cluster_spec=self.cluster_spec,
                      master_node=self.master_node,
                      threads=self.test_config.restore_settings.threads,
                      wrapper=self.rest.is_community(self.master_node),
                      include_data=self.test_config.backup_settings.include_data,
                      use_tls=self.test_config.restore_settings.use_tls)

    def backup_list(self):
        snapshots = local.get_backup_snapshots(self.cluster_spec)
        local.cbbackupmgr_list(cluster_spec=self.cluster_spec,
                               snapshots=snapshots)

    def collectlogs(self):
        local.cbbackupmgr_collectlogs(cluster_spec=self.cluster_spec)

    def get_tool_versions(self):
        local.cbbackupmgr_version()
        local.cbimport_version()
        local.cbexport_version()

    def run(self):
        self.extract_tools()

        if self.test_config.backup_settings.use_tls or \
           self.test_config.restore_settings.use_tls:
            self.download_certificate()

        self.get_tool_versions()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        if self.test_config.compaction.bucket_compaction == 'true':
            self.compact_bucket(wait=True)


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
        storage = None
        if backing_store:
            storage = backing_store
        elif sink_type:
            storage = sink_type

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed, edition, tool, storage)
        )

        if sink_type != 'blackhole':
            self.reporter.post(
                *self.metrics.backup_size(
                    backup_size,
                    edition,
                    tool if backing_store or sink_type else None,
                    storage)
            )

    def run(self):
        super().run()

        try:
            time_elapsed = self.backup()
        finally:
            self.collectlogs()

        self.report_kpi(time_elapsed)


class BackupXATTRTest(BackupTest):

    def run(self):
        self.extract_tools()

        if self.test_config.backup_settings.use_tls or \
           self.test_config.restore_settings.use_tls:
            self.download_certificate()

        self.get_tool_versions()
        self.load()
        self.xattr_load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket(wait=True)

        try:
            time_elapsed = self.backup()
        finally:
            self.collectlogs()

        self.report_kpi(time_elapsed)


class BackupSizeTest(BackupTest):

    def _report_kpi(self, *args):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backup_size = local.calc_backup_size(self.cluster_spec)

        backing_store = self.test_config.backup_settings.storage_type

        tool = 'backup'
        storage = None
        if backing_store:
            storage = backing_store

        self.reporter.post(
            *self.metrics.backup_size(backup_size, edition, tool, storage)
        )


class BackupTestWithCompact(BackupRestoreTest):

    @with_stats
    @timeit
    def compact(self):
        super().compact()

    def _report_kpi(self, time_elapsed: float, backup_size_difference: float):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backing_store = self.test_config.backup_settings.storage_type

        tool = 'compact'
        storage = None
        if backing_store:
            storage = backing_store

        self.reporter.post(
            *self.metrics.tool_size_diff(
                backup_size_difference,
                edition,
                tool,
                storage)
        )

        self.reporter.post(
            *self.metrics.tool_time(
                time_elapsed,
                edition,
                tool,
                storage)
        )

    def run(self):
        super().run()

        try:
            self.backup()
            self.wait_for_persistence()

            initial_size = local.calc_backup_size(self.cluster_spec,
                                                  rounded=False)
            compact_time = self.compact()
            compacted_size = local.calc_backup_size(self.cluster_spec,
                                                    rounded=False)
            # Size differences can be a little small, so go for more precision here
            size_diff = round(initial_size - compacted_size, 2)
        finally:
            self.collectlogs()

        self.report_kpi(compact_time, size_diff)


class BackupUnderLoadTest(BackupTest):

    def run(self):
        super(BackupTest, self).run()

        self.hot_load()

        self.access_bg()

        try:
            time_elapsed = self.backup()
        finally:
            self.collectlogs()

        self.report_kpi(time_elapsed)


class BackupIncrementalTest(BackupRestoreTest):

    @timeit
    @with_stats
    def backup_with_stats(self, mode=False):
        super().backup(mode=mode)

    def _report_kpi(self, time_elapsed: int, backup_size: float):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backing_store = self.test_config.backup_settings.storage_type
        sink_type = self.test_config.backup_settings.sink_type

        tool = 'backup-incremental'
        storage = None
        if backing_store:
            storage = backing_store
        elif sink_type:
            storage = sink_type

        self.reporter.post(
            *self.metrics.tool_time(time_elapsed,
                                    edition,
                                    tool=tool,
                                    storage=storage)
        )

        if sink_type != 'blackhole':
            self.reporter.post(
                *self.metrics.backup_size(
                    backup_size,
                    edition,
                    tool=tool,
                    storage=storage)
            )

    def run(self):
        self.extract_tools()

        if self.test_config.backup_settings.use_tls or self.test_config.restore_settings.use_tls:
            self.download_certificate()

        self.get_tool_versions()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket(wait=True)
        self.backup()

        initial_backup_size = local.calc_backup_size(self.cluster_spec, rounded=False)

        self.access()
        self.wait_for_persistence()

        # Define a secondary load. For this we borrow the 'creates' field,
        # since load doesn't normally use this anyway.
        inc_load = self.test_config.load_settings.additional_items
        workers = self.test_config.load_settings.workers
        size = self.test_config.load_settings.size

        # New key prefix needed to create incremental dataset.
        self.load(
            settings=LoadSettings({"items": inc_load, "workers": workers, "size": size}),
            target_iterator=TargetIterator(self.cluster_spec, self.test_config, prefix='inc-')
        )
        self.wait_for_persistence()

        try:
            inc_backup_time = self.backup_with_stats(mode=True)
            total_backup_size = local.calc_backup_size(self.cluster_spec, rounded=False)
            inc_backup_size = round(total_backup_size - initial_backup_size, 2)
        finally:
            self.collectlogs()

        self._report_kpi(inc_backup_time, inc_backup_size)


class MergeTest(BackupRestoreTest):

    @with_stats
    @timeit
    def merge(self):
        snapshots = local.get_backup_snapshots(self.cluster_spec)

        local.drop_caches()

        # Pre build 6.5.0-3198 there was no threads flag in merge. To ensure
        # tests run across versions, omit this flag pre this build.
        version, build_number = self.build.split('-')
        build = tuple(map(int, version.split('.'))) + (int(build_number),)

        if build < (6, 5, 0, 3198):
            threads = None
        else:
            threads = self.test_config.backup_settings.threads

        local.cbbackupmgr_merge(self.cluster_spec,
                                snapshots,
                                self.test_config.backup_settings.storage_type,
                                threads)

    def _report_kpi(self, time_elapsed):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        tool = 'merge'
        storage = None
        if self.test_config.backup_settings.storage_type:
            storage = self.test_config.backup_settings.storage_type

        self.reporter.post(
            *self.metrics.merge_throughput(
                time_elapsed, edition, tool, storage)
        )

    def run(self):
        self.extract_tools()

        self.get_tool_versions()

        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        try:
            self.backup()  # 1st snapshot

            self.load()
            self.wait_for_persistence()
            self.check_num_items()
            self.backup(mode=True)  # 2nd snapshot

            time_elapsed = self.merge()
        finally:
            self.collectlogs()

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
        storage = None
        if backing_store:
            storage = backing_store
        elif sink_type:
            storage = sink_type

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed, edition, tool, storage)
        )

    def run(self):
        super().run()

        self.backup()
        self.flush_buckets()

        try:
            time_elapsed = self.restore()
        finally:
            self.collectlogs()

        self.report_kpi(time_elapsed)


class RestoreXATTRTest(RestoreTest):

    def run(self):
        self.extract_tools()

        if self.test_config.backup_settings.use_tls or \
           self.test_config.restore_settings.use_tls:
            self.download_certificate()

        self.get_tool_versions()
        self.load()
        self.xattr_load()
        self.wait_for_persistence()
        self.check_num_items()

        self.backup()
        self.flush_buckets()

        try:
            time_elapsed = self.restore()
        finally:
            self.collectlogs()

        self.report_kpi(time_elapsed)


class ListTest(BackupRestoreTest):

    def _report_kpi(self, time_elapsed: float):

        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'
        backing_store = self.test_config.backup_settings.storage_type

        tool = 'list'
        storage = None
        if backing_store:
            storage = backing_store

        self.reporter.post(
            *self.metrics.tool_time(time_elapsed, edition, tool, storage))

    @with_stats
    @timeit
    def backup_list(self):
        super().backup_list()

    def run(self):
        super().run()

        try:
            self.backup()
            local.drop_caches()
            list_time = self.backup_list()
        finally:
            self.collectlogs()

        self.report_kpi(list_time)


class ExportImportTest(BackupRestoreTest):

    def export(self):

        export_settings = self.test_config.export_settings

        local.cbexport(master_node=self.master_node,
                       cluster_spec=self.cluster_spec,
                       bucket=self.test_config.buckets[0],
                       data_format=export_settings.format,
                       threads=export_settings.threads,
                       key_field=export_settings.key_field,
                       log_file=export_settings.log_file,
                       collection_field=export_settings.collection_field,
                       scope_field=export_settings.scope_field)

    def import_data(self):
        import_file = self.test_config.export_settings.import_file
        if import_file is None:
            import_file = 'data.{}'.format(self.test_config.export_settings.type)
            import_file = os.path.join(self.cluster_spec.backup, import_file)
        if self.test_config.export_settings.format != 'sample':
            import_file = 'file://{}'.format(import_file)

        local.drop_caches()

        export_settings = self.test_config.export_settings

        local.cbimport(master_node=self.master_node,
                       cluster_spec=self.cluster_spec,
                       data_type=export_settings.type,
                       data_format=export_settings.format,
                       bucket=self.test_config.buckets[0],
                       import_file=import_file,
                       threads=export_settings.threads,
                       field_separator=export_settings.field_separator,
                       limit_rows=export_settings.limit_rows,
                       skip_rows=export_settings.skip_rows,
                       infer_types=export_settings.infer_types,
                       omit_empty=export_settings.omit_empty,
                       errors_log=export_settings.errors_log,
                       log_file=export_settings.log_file,
                       scope_collection_exp=export_settings.scope_collection_exp)

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
        self.get_tool_versions()
        time_elapsed = self.import_data()

        self.report_kpi(time_elapsed)


class CloudBackupRestoreTest(BackupRestoreTest):
    COLLECTORS = {'iostat': False}

    def backup(self, master_node=None):
        self.remote.backup(
            master_node=master_node if master_node else self.master_node,
            cluster_spec=self.cluster_spec,
            threads=self.test_config.backup_settings.threads,
            worker_home=self.worker_manager.WORKER_HOME,
            compression=self.test_config.backup_settings.compression,
            storage_type=self.test_config.backup_settings.storage_type,
            sink_type=self.test_config.backup_settings.sink_type,
            shards=self.test_config.backup_settings.shards,
            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
            obj_region=self.test_config.backup_settings.obj_region,
            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
            use_tls=self.test_config.backup_settings.use_tls
        )

    def restore(self, master_node=None):
        self.remote.client_drop_caches()

        self.remote.restore(cluster_spec=self.cluster_spec,
                            master_node=master_node if master_node else self.master_node,
                            threads=self.test_config.restore_settings.threads,
                            worker_home=self.worker_manager.WORKER_HOME,
                            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
                            obj_region=self.test_config.backup_settings.obj_region,
                            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
                            use_tls=self.test_config.restore_settings.use_tls)

    def collectlogs(self):
        staging_dir = './stage'
        os.mkdir(staging_dir)
        local.cbbackupmgr_collectlogs(
            cluster_spec=self.cluster_spec,
            obj_staging_dir=staging_dir,
            obj_region=self.test_config.backup_settings.obj_region,
            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id
        )

    def setup_run(self):
        self.remote.extract_cb(filename='couchbase.rpm',
                               worker_home=self.worker_manager.WORKER_HOME)

        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)

        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        if self.test_config.compaction.bucket_compaction == 'true':
            self.compact_bucket(wait=True)


class CloudBackupTest(CloudBackupRestoreTest):

    @with_stats
    @timeit
    def backup(self, master_node=None):
        super().backup(master_node=master_node)

    def _report_kpi(self, time_elapsed):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'

        backing_store = self.test_config.backup_settings.storage_type
        sink_type = self.test_config.backup_settings.sink_type

        tool = 'backup'
        storage = None
        if backing_store:
            storage = backing_store
        elif sink_type:
            storage = sink_type

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed, edition, tool, storage)
        )

    def run(self):
        self.setup_run()

        master_node = None
        if self.cluster_spec.using_private_ips:
            master_node = next(self.cluster_spec.clusters_private)[1][0]

        try:
            time_elapsed = self.backup(master_node=master_node)
        except Exception as e:
            logger.error(e)
        finally:
            self.extract_tools()
            self.collectlogs()

        self.report_kpi(time_elapsed)


class AWSBackupTest(CloudBackupTest):

    def backup(self, master_node=None):
        credential = local.read_aws_credential(self.test_config.backup_settings.aws_credential_path)
        self.remote.create_aws_credential(credential)
        return super().backup(master_node=master_node)


class GCPBackupTest(CloudBackupTest):
    ALL_HOSTNAMES = True


class AzureBackupTest(CloudBackupTest):
    ALL_HOSTNAMES = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        storage_acc = self.cluster_spec.infrastructure_section('storage')['storage_acc']
        self.test_config.config.set('backup', 'obj_access_key_id', storage_acc)


class CloudRestoreTest(CloudBackupRestoreTest):

    @with_stats
    @timeit
    def restore(self, master_node=None):
        super().restore(master_node=master_node)

    def _report_kpi(self, time_elapsed):
        edition = self.rest.is_community(self.master_node) and 'CE' or 'EE'

        backing_store = self.test_config.backup_settings.storage_type
        sink_type = self.test_config.backup_settings.sink_type

        tool = 'restore'
        storage = None
        if backing_store:
            storage = backing_store
        elif sink_type:
            storage = sink_type

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed, edition, tool, storage)
        )

    def run(self):
        self.setup_run()

        master_node = None
        if self.cluster_spec.using_private_ips:
            master_node = next(self.cluster_spec.clusters_private)[1][0]

        try:
            self.backup(master_node=master_node)
            self.flush_buckets()
            time_elapsed = self.restore(master_node=master_node)
        except Exception as e:
            logger.error(e)
        finally:
            self.extract_tools()
            self.collectlogs()

        self.report_kpi(time_elapsed)


class AWSRestoreTest(CloudRestoreTest):

    def backup(self, master_node=None):
        credential = local.read_aws_credential(self.test_config.backup_settings.aws_credential_path)
        self.remote.create_aws_credential(credential)
        super().backup(master_node=master_node)


class GCPRestoreTest(CloudRestoreTest):
    ALL_HOSTNAMES = True


class AzureRestoreTest(CloudRestoreTest):
    ALL_HOSTNAMES = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        storage_acc = self.cluster_spec.infrastructure_section('storage')['storage_acc']
        self.test_config.config.set('backup', 'obj_access_key_id', storage_acc)

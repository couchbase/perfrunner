import os
import time
from typing import Optional

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import create_build_tuple, human_format, pretty_dict
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import LoadSettings, TargetIterator
from perfrunner.tests import PerfTest
from perfrunner.tests.syncgateway import SGRead


class BackupRestoreTest(PerfTest):

    def extract_tools(self):
        local.extract_cb_any(filename='couchbase')

    def flush_buckets(self, master_node: Optional[str] = None):
        for i in range(self.test_config.cluster.num_buckets):
            bucket = 'bucket-{}'.format(i + 1)
            self.rest.flush_bucket(master_node or self.master_node, bucket)

    def backup(self, master_node: Optional[str] = None, mode: Optional[str] = None):
        local.backup(
            master_node=master_node or self.master_node,
            cluster_spec=self.cluster_spec,
            threads=self.test_config.backup_settings.threads,
            wrapper=self.rest.is_community(master_node or self.master_node),
            mode=mode,
            compression=self.test_config.backup_settings.compression,
            storage_type=self.test_config.backup_settings.storage_type,
            sink_type=self.test_config.backup_settings.sink_type,
            shards=self.test_config.backup_settings.shards,
            include_data=self.test_config.backup_settings.include_data,
            use_tls=self.test_config.backup_settings.use_tls,
            encrypted=self.test_config.backup_settings.encrypted,
            passphrase=self.test_config.backup_settings.passphrase,
            env_vars=self.test_config.backup_settings.env_vars,
        )

    def compact(self):
        # Pre build 6.5.0-3524 there was no threads flag in compact. To ensure
        # tests run across versions, omit this flag pre this build.
        if create_build_tuple(self.build) < (6, 5, 0, 3524):
            threads = None
        else:
            threads = self.test_config.backup_settings.threads

        snapshots = local.get_backup_snapshots(self.cluster_spec)
        local.compact(self.cluster_spec,
                      snapshots,
                      threads,
                      self.rest.is_community(self.master_node))

    def restore(self, master_node: Optional[str] = None):
        local.drop_caches()

        local.restore(
            cluster_spec=self.cluster_spec,
            master_node=master_node or self.master_node,
            threads=self.test_config.restore_settings.threads,
            wrapper=self.rest.is_community(master_node or self.master_node),
            include_data=self.test_config.backup_settings.include_data,
            use_tls=self.test_config.restore_settings.use_tls,
            encrypted=self.test_config.backup_settings.encrypted,
            passphrase=self.test_config.backup_settings.passphrase,
            disable_hlv=True if self.test_config.xdcr_settings.mobile else False,
            env_vars=self.test_config.restore_settings.env_vars,
        )

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
        if create_build_tuple(self.build) < (6, 5, 0, 3198):
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
    def restore(self, master_node: Optional[str] = None):
        super().restore(master_node)

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
        import_file = 'file://{}'.format(import_file)

        is_sample_format = self.test_config.export_settings.format == 'sample'

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
                       scope_collection_exp=export_settings.scope_collection_exp,
                       is_sample_format=is_sample_format)

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

    def backup(self, master_node: Optional[str] = None):
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
            use_tls=self.test_config.backup_settings.use_tls,
            encrypted=self.test_config.backup_settings.encrypted,
            passphrase=self.test_config.backup_settings.passphrase,
            env_vars=self.test_config.backup_settings.env_vars,
        )

    def restore(self, master_node=None):
        self.remote.client_drop_caches()

        self.remote.restore(
            cluster_spec=self.cluster_spec,
            master_node=master_node if master_node else self.master_node,
            threads=self.test_config.restore_settings.threads,
            worker_home=self.worker_manager.WORKER_HOME,
            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
            obj_region=self.test_config.backup_settings.obj_region,
            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
            use_tls=self.test_config.restore_settings.use_tls,
            encrypted=self.test_config.backup_settings.encrypted,
            passphrase=self.test_config.backup_settings.passphrase,
            env_vars=self.test_config.restore_settings.env_vars,
        )

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
        self.remote.extract_cb_any(filename='couchbase',
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
        if self.cluster_spec.using_private_cluster_ips:
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
        if self.cluster_spec.using_private_cluster_ips:
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


class ProvisionedCapellaBackup(PerfTest):

    @with_stats
    def backup(self):
        self.rest.backup(self.master_node, self.test_config.buckets[0])
        backup_time = self.rest.wait_for_backup(self.master_node)
        logger.info("Backup took: {}s ".format(backup_time))
        return backup_time

    def _report_kpi(self, time_elapsed):
        edition = 'Capella'
        tool = 'backup'
        storage = 'Rift'

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed, edition, tool, storage)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        time_elapsed = self.backup()
        self.report_kpi(time_elapsed)


class ProvisionedCapellaRestore(ProvisionedCapellaBackup):

    def flush_buckets(self):
        self.rest.flush_bucket(self.master_node, self.test_config.buckets[0])

    def backup(self):
        self.rest.backup(self.master_node, self.test_config.buckets[0])
        self.rest.wait_for_backup(self.master_node)

    def trigger_restore(self):
        self.rest.restore(self.master_node, self.test_config.buckets[0])
        self.rest.wait_for_restore_initialize(self.master_node, self.test_config.buckets[0])

    @with_stats
    @timeit
    def restore(self):
        self.rest.wait_for_restore(self.master_node, self.test_config.buckets[0])

    def _report_kpi(self, time_elapsed):
        edition = 'Capella'
        tool = 'restore'
        storage = 'Rift'

        self.reporter.post(
            *self.metrics.bnr_throughput(time_elapsed, edition, tool, storage)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.backup()
        self.flush_buckets()
        self.trigger_restore()
        time_elapsed = self.restore()
        self.report_kpi(time_elapsed)


class CapellaSnapshotBackupRestoreTest(ProvisionedCapellaRestore):
    """Base class for cluster level backup snapshots."""

    @timeit
    def backup(self):
        self.backup_id = self.rest.start_ondemand_cluster_backup(self.master_node)
        self.backup_size = self.monitor.wait_for_cluster_backup_complete(
            self.master_node, self.backup_id
        )

    @timeit
    def restore(self):
        self.restore_id = self.rest.restore_cluster_backup(self.master_node, self.backup_id)
        self.monitor.monitor_cluster_snapshot_restore(self.master_node)

    @timeit
    def warmup(self):
        self.wait_for_persistence()
        self.check_num_items()

    def run_backup_restore(self):
        self.backup_time = self.backup()
        self._post_backup()
        self.flush_buckets()
        self.restore_time = self.restore()
        self._post_restore()
        self.warmup_time = self.warmup()
        logger.info(
            f"Backup time: {self.backup_time:.2f}s, Restore time: {self.restore_time:.2f}s,"
            f" Warmup time: {self.warmup_time:.2f}s, Backup size: {human_format(self.backup_size)}"
        )

    def _post_backup(self):
        time.sleep(60)

    def _post_restore(self):
        time.sleep(60)
        self._refresh()

    @with_stats
    def access(self) -> dict:
        self.access_bg()
        time.sleep(self.test_config.access_settings.time)
        self.worker_manager.abort_all_tasks()
        latencies = {}

        for operation in ("get", "set", "durable_set"):
            latencies[f"{operation}"] = self.metrics._kv_latency(
                operation, self.test_config.access_settings.latency_percentiles, "spring_latency"
            )
        return latencies

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()

        self.latencies_before = self.access()
        self.run_backup_restore()

        self.hot_load()
        self.latencies_after = self.access()

        self.report_kpi()
        logger.info(f"\n{self.latencies_before=}\n{self.latencies_after=}")

    def _refresh(self):
        self.rest.allow_my_ip_all_clusters()
        self.cluster.capella_allow_client_ips()
        self.rest.create_db_user_all_clusters(*self.cluster_spec.rest_credentials)
        self._maybe_update_master()
        self.monitor.master_node = self.master_node
        self.rest = RestHelper(self.cluster_spec, self.test_config)

    def _maybe_update_master(self):
        # If rebalance removed the master node, get a new one
        try:
            for _, nodes in self.rest.get_all_cluster_nodes().items():
                logger.info(f"New nodes: {nodes}")
                self.master_node = nodes[0].split(":")[0]
                logger.info(f"Master node updated, new master: {self.master_node}")
                return
        except Exception:
            pass

    def _report_kpi(self):
        # Backup time
        self._report_time(self.backup_time, "Backup time", "backup")
        # Restore time
        self._report_time(self.restore_time, "Restore time", "restore")
        # Warmup time
        self._report_time(self.warmup_time, "Warmup time", "warmup")

    def _report_time(self, time_s: float, title: str, category: str):
        value, snapshot, metric = self.metrics.elapsed_time(time_s)
        metric["title"] = f"{title} (min), {metric['title']}"
        metric["category"] = category
        metric["id"] = f"{metric['id']}_{category}"
        self.reporter.post(value, snapshot, metric)


class CapellaSnapshotBackupWithSGWTest(SGRead, CapellaSnapshotBackupRestoreTest):
    COLLECTORS = {
        "disk": False,
        "ns_server": False,
        "active_tasks": False,
        "syncgateway_stats": True,
    }

    ALL_HOSTNAMES = True

    def load_sgw_docs(self):
        self.start_memcached()
        self.load_users()
        self.load_docs()
        self.init_users()
        self.grant_access()

    def sgw_access(self) -> float:
        self.run_test()
        self.collect_execution_logs()
        return self.metrics._parse_sg_throughput()

    @timeit
    def warmup(self):
        self.wait_for_persistence()

    def run(self):
        self.throughputs = []
        self.download_ycsb()
        self.load_sgw_docs()

        self.sgw_throughput_before = self.sgw_access()

        self.run_backup_restore()

        self.sgw_throughput_after = self.sgw_access()

        self.report_kpi()

    def _post_backup(self):
        super()._post_backup()
        self.rest.sgw_delete_backend()
        try:
            self._wait_for_app_service_status(None, None)
        except Exception:
            pass  # Deletion complete

    def _wait_for_app_service_status(
        self, sgw_cluster_id: Optional[str], expected_state: Optional[str]
    ):
        while True:
            status = (
                self.rest.sgw_get_app_service_info(sgw_cluster_id).get("status", {}).get("state")
            )
            logger.info(f"AppService cluster state: {status}")
            if status == "deploymentFailed":
                logger.error(f"Re-deployment failed for cluster {sgw_cluster_id}")

            if status == expected_state:
                break
            time.sleep(30)

    @timeit
    def _wait_for_appservice_ready(self):
        logger.info("Waiting for AppService to be ready")
        # Get new appService Id
        sgw_cluster_id = self.rest.get_cluster_info().get("appService", {}).get("id")
        while not sgw_cluster_id:
            sgw_cluster_id = self.rest.get_cluster_info().get("appService", {}).get("id")
            time.sleep(30)
        self.cluster_spec.config.set("infrastructure", "app_services_cluster", sgw_cluster_id)

        # Wait for AppService redeployment
        self._wait_for_app_service_status(sgw_cluster_id, "healthy")

        # Wait for AppServices endpoint ready
        logger.info("Waiting for AppService dbs to come online")
        db_status = False
        while db_status:
            time.sleep(15)
            dbs = self.rest.sgw_get_app_service_dbs(sgw_cluster_id)
            if not dbs:
                continue
            db_status = all([db.get("data", {}).get("state") == "Online" for db in dbs])
            logger.info(f"AppService DBs: {pretty_dict(dbs)}")

    def _post_restore(self):
        super()._post_restore()
        self.app_service_deployment_time = self._wait_for_appservice_ready()
        self.cluster_spec.update_spec_file()
        logger.info(f"AppService deployment time: {self.app_service_deployment_time:.2f}s")

        client_ips = self.cluster_spec.clients
        if self.cluster_spec.capella_backend == "aws":
            client_ips = [
                dns.split(".")[0].removeprefix("ec2-").replace("-", ".") for dns in client_ips
            ]

        self.rest.sgw_add_allowed_ip(client_ips)

    def _report_kpi(self):
        CapellaSnapshotBackupRestoreTest._report_kpi(self)
        # AppService redeployment time
        self._report_time(
            self.app_service_deployment_time, "AppService re-deployment time", "redeployment"
        )

        # Throughput delta
        throughput_delta = self.sgw_throughput_before - self.sgw_throughput_after
        logger.info(
            f"{self.sgw_throughput_before=}, {self.sgw_throughput_after=}, {throughput_delta=}"
        )

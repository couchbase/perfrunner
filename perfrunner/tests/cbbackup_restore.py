from time import time

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class CBBackupRestoreBase(PerfTest):
    folder_size = 0
    """
    The most basic CB backup /restore class:
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

    def cbbackup(self, wrapper=False, mode=None):
        sizes = self.remote.cbbackup(wrapper, mode)
        total = [v for _, v in sizes.items()]
        self.folder_size = sum(d for d in total) / len(total)

    @with_stats
    def run_backup_with_stats(self, wrapper=False, mode=None):
        self.cbbackup(wrapper, mode)

    @with_stats
    def run_cbrestore_with_stats(self, wrapper=False):
        self.remote.cbrestore(wrapper)

    def flush_buckets(self):
        for num_buckets in range(self.test_config.cluster.num_buckets):
            buckets = ['bucket-{}'.format(i + 1) for i in range(num_buckets)]
        for bucket in buckets:
            self.rest.flush_bucket(host_port=self.master_node, name=bucket)


class BackupTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure time it takes
    to perform backup.
    """
    def run(self):
        super(BackupTest, self).run()

        start = time()
        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        backup_time = int(time() - start)
        logger.info('backup completed in %s sec' % backup_time)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(backup_time)


class BackupWorkloadRunningTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure time it takes
    to perform backup when workload running.
    """

    def run(self):
        super(BackupWorkloadRunningTest, self).run()
        self.access_bg()

        start = time()
        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        t = int(time() - start)
        logger.info('backup completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)


class BackupFolderSizeTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure backup folder size.
    """

    def run(self):
        super(BackupFolderSizeTest, self).run()

        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        logger.info('backup completed, folder size %s' % self.folder_size)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(self.folder_size)


class RestoreTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then restore
    and measure time it takes to perform restore.
    """

    def run(self):
        super(RestoreTest, self).run()
        self.cbbackup(wrapper=self.test_config.test_case.use_backup_wrapper)
        self.flush_buckets()

        start = time()
        self.run_cbrestore_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        t = int(time() - start)
        logger.info('restore completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)


class IncrementalBackupWorkloadRunningTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then perform
    incremental backup when workload running.
    """

    def run(self):
        super(IncrementalBackupWorkloadRunningTest, self).run()
        self.cbbackup(wrapper=self.test_config.test_case.use_backup_wrapper)
        self.access_bg()

        start = time()
        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper, mode='diff')
        t = int(time() - start)
        logger.info('backup completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)


class IncrementalBackupWorkloadRunningFolderSizeTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then perform
    incremental backup when workload running and measure backup folder size.
    """

    def run(self):
        super(IncrementalBackupWorkloadRunningTest, self).run()
        self.cbbackup(wrapper=self.test_config.test_case.use_backup_wrapper)
        self.access_bg()

        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper, mode='diff')
        logger.info('backup completed, folder size %s' % self.folder_size)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(self.folder_size)


class RestoreAfterIncrementalBackupTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes. After access load
    we backup again (incremental backup).
    Incremental Restore and measure time it takes to perform restore.
    """

    def run(self):
        super(RestoreAfterIncrementalBackupTest, self).run()
        self.cbbackup(wrapper=self.test_config.test_case.use_backup_wrapper)
        self.workload = self.test_config.access_settings
        self.workload.seq_updates = True
        self.access(self.workload)
        self.cbbackup(
            wrapper=self.test_config.test_case.use_backup_wrapper, mode='diff')
        self.flush_buckets()

        start = time()
        self.run_cbrestore_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        t = int(time() - start)
        logger.info('restore completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)

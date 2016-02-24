from time import time, sleep

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class CBBackupRestoreBase(PerfTest):
    folder_size = 0
    data_size = 0
    spent_time = 0
    """
    The most basic CB backup /restore class:
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.data_size = (self.test_config.load_settings.items *
                          self.test_config.load_settings.size) / 1000000.0

    def cbbackup(self, wrapper=False, mode=None):
        results = self.remote.cbbackup(wrapper, mode)
        total = [s for s, _ in results.values()]
        self.folder_size = sum(d for d in total) / len(total)
        times = [t for _, t in results.values()]
        self.spent_time = sum(d for d in times) / len(times)

    @with_stats
    def run_backup_with_stats(self, wrapper=False, mode=None):
        self.cbbackup(wrapper, mode)

    @with_stats
    def run_cbrestore_with_stats(self, wrapper=False):
        results = self.remote.cbrestore(wrapper)
        self.spent_time = sum(results.values()) / len(results)

    def flush_buckets(self):
        buckets = ['bucket-{}'.format(i + 1)
                   for i in range(self.test_config.cluster.num_buckets)]
        for bucket in buckets:
            self.rest.flush_bucket(host_port=self.master_node, name=bucket)


class BackupTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure time it takes
    to perform backup.
    """
    def run(self):
        super(BackupTest, self).run()
        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        logger.info('backup completed in %s sec' % self.spent_time)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(round(self.data_size / self.spent_time))


class BackupWorkloadRunningTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure time it takes
    to perform backup when workload running.
    """

    def run(self):
        super(BackupWorkloadRunningTest, self).run()
        self.access_bg()

        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        logger.info('backup completed in %s sec' % self.spent_time)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(round(self.data_size / self.spent_time))


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
            self.reporter.post_to_sf(round(self.data_size / t, 1))


class IncrementalBackupWorkloadRunningTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then perform
    incremental backup when workload running.
    """

    def run(self):
        super(IncrementalBackupWorkloadRunningTest, self).run()
        self.cbbackup(wrapper=self.test_config.test_case.use_backup_wrapper)
        self.access_bg()
        # a delay to give the data update
        sleep(300)
        self.run_backup_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper, mode='diff')
        logger.info('backup completed in %s sec' % self.spent_time)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(round(self.data_size / self.spent_time, 1))


class IncrementalBackupWorkloadRunningFolderSizeTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then perform
    incremental backup when workload running and measure backup folder size.
    """

    def run(self):
        super(IncrementalBackupWorkloadRunningFolderSizeTest, self).run()
        self.cbbackup(wrapper=self.test_config.test_case.use_backup_wrapper)
        self.access_bg()
        # a delay to give the data update
        sleep(300)
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
        self.run_cbrestore_with_stats(
            wrapper=self.test_config.test_case.use_backup_wrapper)
        logger.info('Incremental restore completed in %s sec' % self.spent_time)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(self.spent_time)

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

    def cbbackup(self, mode=None):
        sizes = self.remote.cbbackup(mode)
        total = [v for _, v in sizes.items()]
        self.folder_size = sum(d for d in total) / len(total)

    @with_stats
    def run_backup_with_stats(self, mode=None):
        self.cbbackup(mode=None)

    @with_stats
    def run_cbrestore_with_stats(self):
        self.cbrestore()


class BackupTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure time it takes
    to perform backup.
    """
    def run(self):
        super(BackupTest, self).run()

        start = time()
        self.run_backup_with_stats()
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
        self.run_backup_with_stats()
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

        self.run_backup_with_stats()
        print 'folder_size', self.folder_size
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
        self.cbbackup()

        start = time()
        self.run_cbrestore_with_stats()
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
        self.cbbackup()
        self.access_bg()

        start = time()
        self.run_backup_with_stats(mode='diff')
        t = int(time() - start)
        logger.info('backup completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)


# TODO it's better to change on Incremental case.
# what the scenario should be here?
class RestoreAfterIncrementalBackupTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then perform
    incremental backup when workload running. Restore
    and measure time it takes to perform restore.
    """

    def run(self):
        super(RestoreAfterIncrementalBackupTest, self).run()
        self.cbbackup()
        self.access_bg()
        self.cbbackup(mode='diff')

        start = time()
        self.run_cbrestore_with_stats()
        t = int(time() - start)
        logger.info('restore completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)

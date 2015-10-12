from time import time

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class CBBackupRestoreBase(PerfTest):
    """
    The most basic CB backup /restore class:
    """

    @with_stats
    def access(self):
        super(CBBackupRestoreBase, self).timer()

    def cbbackup(self, mode=None):
        return self.remote.cbbackup(mode)

    def cbrestore(self):
        self.remote.cbrestore()


class BackupTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure time it takes
    to perform backup.
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        start = time()
        self.cbbackup()
        t = int(time() - start)
        logger.info('backup completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)


class BackupWorkloadRunningTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure time it takes
    to perform backup when workload running.
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.access_bg()

        start = time()
        self.cbbackup()
        t = int(time() - start)
        logger.info('backup completed in %s sec' % t)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)


class BackupFolderSizeTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes and measure backup folder size.
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        folder_size = self.cbbackup()
        logger.info('backup completed, folder size %s' % folder_size)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(folder_size)


class RestoreTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then restore
    and measure time it takes to perform restore.
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.cbbackup()

        start = time()
        self.cbrestore()
        t = int(time() - start)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)
        logger.info('restore completed in %s sec' % t)


class IncrementalBackupWorkloadRunningTest(CBBackupRestoreBase):
    """
    After typical workload we backup all nodes then perform
    incremental backup when workload running.
    """

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.cbbackup()
        self.access_bg()

        start = time()
        self.cbbackup(mode='diff')
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
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.cbbackup()
        self.access_bg()
        self.cbbackup(mode='diff')

        start = time()
        self.cbrestore()
        t = int(time() - start)
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(t)
        logger.info('restore completed in %s sec' % t)

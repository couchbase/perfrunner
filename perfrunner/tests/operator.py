import datetime
import time

from logger import logger
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest
from perfrunner.tests.ycsb import YCSBTest


class OperatorTest(PerfTest):

    COLLECTORS = {
        'ns_server_system': True
    }


class OperatorBackupTest(OperatorTest):

    @with_stats
    def backup(self):
        logger.info('Running backup')
        self.remote.create_backup()
        self.remote.wait_for_backup_complete()
        logger.info('Backup complete')

    def parse_backup_status(self, backup_status):
        start_time = backup_status['lastRun']
        end_time = backup_status['lastSuccess']
        start_dt = datetime.datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%SZ')
        end_dt = datetime.datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')
        time_elapsed = end_dt - start_dt
        time_elapsed = time_elapsed.total_seconds()
        capacity_used = backup_status['capacityUsed']
        if "Mi" in capacity_used:
            backup_size = round(float(backup_status['capacityUsed'].strip("Mi"))/1024, 2)
        if "Gi" in capacity_used:
            backup_size = round(float(backup_status['capacityUsed'].strip("Gi")), 2)
        return time_elapsed, backup_size

    def _report_kpi(self, time_elapsed, backup_size):
        edition = 'Operator'
        tool = 'backup'
        storage = None
        self.reporter.post(
            *self.metrics.bnr_throughput(
                time_elapsed,
                edition,
                tool,
                storage)
        )
        self.reporter.post(
            *self.metrics.backup_size(
                backup_size,
                edition,
                tool,
                storage)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.backup()
        backup_status = self.remote.get_backup('my-backup')['status']
        time_elapsed, backup_size = self.parse_backup_status(backup_status)
        self.report_kpi(time_elapsed, backup_size)


class OperatorBackupRestoreTest(OperatorBackupTest):

    @with_stats
    @timeit
    def restore(self):
        logger.info('Running restore')
        self.remote.create_restore()
        self.check_num_items(max_retry=3600)

    def flush_buckets(self):
        logger.info('Flushing bucket')
        for i in range(self.test_config.cluster.num_buckets):
            bucket = 'bucket-{}'.format(i + 1)
            self.rest.flush_bucket(self.master_node, bucket)
            self.check_num_items(bucket_items={bucket: 0})

    def _report_kpi(self, time_elapsed_backup, time_elapsed_restore, backup_size):
        edition = 'Operator'
        storage = None
        tool = 'backup'

        self.reporter.post(
            *self.metrics.bnr_throughput(
                time_elapsed_backup,
                edition,
                tool,
                storage)
        )

        self.reporter.post(
            *self.metrics.backup_size(
                backup_size,
                edition,
                tool,
                storage)
        )

        tool = 'restore'
        self.reporter.post(
            *self.metrics.bnr_throughput(
                time_elapsed_restore,
                edition,
                tool,
                storage)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.backup()
        backup_status = self.remote.get_backup('my-backup')['status']
        time_elapsed_backup, backup_size = self.parse_backup_status(backup_status)
        self.flush_buckets()
        time_elapsed_restore = self.restore()
        self.report_kpi(time_elapsed_backup, time_elapsed_restore, backup_size)


class OperatorBackupRestoreYCSBTest(YCSBTest, OperatorBackupRestoreTest):

    def run(self):
        self.download_ycsb()
        self.load()
        time.sleep(30)
        self.wait_for_persistence()
        self.check_num_items()
        self.backup()
        backup_status = self.remote.get_backup('my-backup')['status']
        time_elapsed_backup, backup_size = self.parse_backup_status(backup_status)
        self.flush_buckets()
        time_elapsed_restore = self.restore()
        self.report_kpi(time_elapsed_backup, time_elapsed_restore, backup_size)

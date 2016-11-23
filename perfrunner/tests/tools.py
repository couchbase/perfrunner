import csv
import json
import os
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

    def flush_buckets(self):
        for i in range(self.test_config.cluster.num_buckets):
            bucket = 'bucket-{}'.format(i + 1)
            self.rest.flush_bucket(host_port=self.master_node, name=bucket)

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


class CbExportImportTest(BackupRestoreTest):

    """
    After a typical workload we export all nodes, measure time it takes to
    perform export.
    Then we flush the bucket and import the data, measure time it takes to
    perform import
    """

    @with_stats
    def export(self):
        t0 = time.time()
        local.export(master_node=self.master_node,
                     cluster_spec=self.cluster_spec,
                     tp=self.test_config.export_import_settings.type,
                     frmt=self.test_config.export_import_settings.format,
                     bucket='bucket-1')
        self.spent_time = time.time() - t0

        self.data_size = local.calc_backup_size(self.cluster_spec)

        logger.info('Export completed in {:.1f} sec, Export size is {} GB'
                    .format(self.spent_time, self.data_size))

    @with_stats
    def import_data(self, tp=None, frmt=None):
        t0 = time.time()
        tp = tp
        frmt = frmt
        if not tp:
            tp = self.test_config.export_import_settings.type
        if frmt is None:
            frmt = self.test_config.export_import_settings.format
        local.import_data(master_node=self.master_node,
                          cluster_spec=self.cluster_spec,
                          tp=tp,
                          frmt=frmt,
                          bucket='bucket-1')
        self.spent_time = time.time() - t0

        logger.info('Import completed in {:.1f} sec, Import size is {} GB'
                    .format(self.spent_time, self.data_size))

    def _report_kpi(self, prefix=''):
        metric_info = {
            'title': prefix + " " + self.test_config.test_case.title,
            'category': prefix.split()[0].lower(),
        }
        metric = self.test_config.name
        # replace 'expimp' on import or export
        metric = metric.replace('expimp', prefix.split()[0].lower())
        if "CSV" in prefix:
            metric = metric.replace('json', 'csv')

        data_size = self.test_config.load_settings.items * \
            self.test_config.load_settings.size / 2.0 ** 20  # MB
        avg_throughput = round(data_size / self.spent_time)

        self.reporter.post_to_sf(avg_throughput, metric=metric,
                                 metric_info=metric_info)

    def _yield_line_delimited_json(self, path):
        """Read a line-delimited json file yielding each row as a record."""
        with open(path, 'r') as f:
            for line in f:
                yield json.loads(line)

    def run(self):
        super(CbExportImportTest, self).run()
        self.export()
        settings = self.test_config.export_import_settings
        self.report_kpi("Export {} {}".format(settings.type.upper(),
                                              settings.format.title()))

        self.flush_buckets()

        self.import_data()

        self.report_kpi("Import {} {}".format(settings.type.upper(),
                                              settings.format.title()))

        if self.test_config.export_import_settings.format == 'lines':
            self.convert_json_in_csv()
            self.flush_buckets()

            self.import_data('csv', '')

            self.report_kpi("Import CSV")

    def convert_json_in_csv(self):
        import_file = "{}/{}.{}".format(
            self.cluster_spec.config.get('storage', 'backup'),
            self.test_config.export_import_settings.format,
            self.test_config.export_import_settings.type)
        data = self._yield_line_delimited_json(import_file)

        with open('{}/export.csv'.format(self.cluster_spec.config.get(
                'storage', 'backup')), 'w') as csvfile:
            output = csv.writer(csvfile)
            header = ""

            for row in data:
                if not header:
                        header = row
                        output.writerow(header.keys())
                output.writerow(row.values())


class CbImportSampleTest(BackupRestoreTest):

    """
    Measure time to perform import zip file with sample.
    """

    @with_stats
    def import_sample_data(self):
        t0 = time.time()
        local.import_sample_data(master_node=self.master_node,
                                 cluster_spec=self.cluster_spec,
                                 bucket='bucket-1')
        self.spent_time = time.time() - t0

        import_file = "{}/../import/beer-sample.zip".\
            format(self.cluster_spec.config.get('storage', 'backup'))

        self.data_size = os.path.getsize(import_file)

        logger.info('Import completed in {:.1f} sec, Import size is {} GB'
                    .format(self.spent_time, self.data_size / 2 ** 30))

    def _report_kpi(self, prefix=''):
        metric_info = {
            'title': prefix + " " + self.test_config.test_case.title,
            'category': prefix.split()[0].lower(),
        }
        metric = self.test_config.name
        metric = metric.replace('expimp', prefix.split()[0].lower())

        data_size = self.data_size / 2.0 ** 20
        avg_throughput = round(data_size / self.spent_time)

        self.reporter.post_to_sf(avg_throughput, metric=metric,
                                 metric_info=metric_info)

    def run(self):
        super(CbImportSampleTest, self)

        self.download_tools()

        settings = self.test_config.export_import_settings

        self.import_sample_data()

        self.report_kpi("Import {} {}".format(settings.type.upper(),
                                              settings.format.title()))

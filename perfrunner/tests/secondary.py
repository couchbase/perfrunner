import time
import urllib2
import base64
import json
import subprocess
import numpy as np

from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.tests import PerfTest


class SecondaryIndexTest(PerfTest):

    """
    The test measures time it takes to build secondary index. This is just a base
    class, actual measurements happen in initial and incremental secondary indexing tests.
    It benchmarks dumb/bulk indexing.
    """

    COLLECTORS = {'secondary_stats': True, 'secondary_debugstats': True}

    def __init__(self, *args):
        super(SecondaryIndexTest, self).__init__(*args)

        """self.test_config.secondaryindex_settings"""
        self.secondaryindex_settings = None
        self.indexnode = None
        self.bucket = None
        self.indexes = []
        self.secondaryDB = ''
        self.configfile = ''

        if self.test_config.secondaryindex_settings.db == 'memdb':
            self.secondaryDB = 'memdb'
        logger.info('secondary storage DB..{}'.format(self.secondaryDB))

        for index in self.test_config.secondaryindex_settings.name.split(','):
            self.indexes.append(index)

        for name, servers in self.cluster_spec.yield_servers_by_role('index'):
            if not servers:
                raise Exception('No index servers specified for cluster \"{}\",'
                                ' cannot create indexes'.format(name))
            self.indexnode = servers[0]

        for testbucket in self.test_config.buckets:
            self.bucket = testbucket

    @with_stats
    def build_secondaryindex(self):
        """call cbindex create command"""
        logger.info('building secondary index..')

        fields = []
        for field in self.test_config.secondaryindex_settings.field.split(','):
            fields.append(field)

        self.remote.build_secondary_index(self.indexnode, self.bucket, self.indexes, fields,
                                          self.secondaryDB)

        rest_username, rest_password = self.cluster_spec.rest_credentials
        time_elapsed = self.rest.wait_for_secindex_init_build(self.indexnode.split(':')[0],
                                                              self.indexes, rest_username, rest_password)
        return time_elapsed


class InitialSecondaryIndexTest(SecondaryIndexTest):

    """
    The test measures time it takes to build index for the first time. Scenario
    is pretty straightforward, there are only two phases:
    -- Initial data load
    -- Index building
    """

    def build_index(self):
        super(InitialSecondaryIndexTest, self).build_secondaryindex()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        init_ts = time.time()
        self.build_secondaryindex()
        finish_ts = time.time()
        time_elapsed = finish_ts - init_ts
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type='Initial')
        )


class InitialandIncrementalSecondaryIndexTest(SecondaryIndexTest):

    """
    The test measures time it takes to build index for the first time as well as
    incremental build. There is no disabling of index updates in incremental building,
    index updating is conurrent to KV incremental load.
    """

    def build_initindex(self):
        self.build_secondaryindex()

    @with_stats
    def build_incrindex(self):
        access_settings = self.test_config.access_settings
        self.worker_manager.run_workload(access_settings, self.target_iterator)
        self.worker_manager.wait_for_workers()
        load_settings = self.test_config.load_settings
        access_settings = self.test_config.access_settings
        numitems = load_settings.items + access_settings.items
        self.rest.wait_for_secindex_incr_build(self.indexnode.split(':')[0], self.bucket,
                                               self.indexes, numitems)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        from_ts, to_ts = self.build_secondaryindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type='Initial')
        )
        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Incremental secondary index', time_elapsed)
        self.reporter.post_to_sf(
            *self.metric_helper.get_indexing_meta(value=time_elapsed,
                                                  index_type='Incremental')
        )


class SecondaryIndexingThroughputTest(SecondaryIndexTest):

    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """

    @with_stats
    def apply_scanworkload(self):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        logger.info('Initiating scan workload')
        if self.test_config.secondaryindex_settings.stale == 'false':
            self.configfile = 'scripts/config_scanthr_sessionconsistent.json'
        else:
            self.configfile = 'scripts/config_scanthr.json'
        cmdstr = "cbindexperf -cluster {} -auth=\"{}:{}\" -configfile {} -resultfile result.json".format(self.indexnode, rest_username, rest_password, self.configfile)
        status = subprocess.call(cmdstr, shell=True)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')

    def read_scanresults(self):
        with open('{}'.format(self.configfile)) as config_file:
            configdata = json.load(config_file)
        numscans = configdata['ScanSpecs'][0]['Repeat']

        with open('result.json') as result_file:
            resdata = json.load(result_file)
        duration_s = (resdata['Duration'])
        numRows = resdata['ScanResults'][0]['Rows']
        """scans and rows per sec"""
        scansps = numscans / duration_s
        rowps = numRows / duration_s
        return scansps, rowps

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        from_ts, to_ts = self.build_secondaryindex()
        self.access_bg()
        self.apply_scanworkload()
        scanthr, rowthr = self.read_scanresults()
        logger.info('Scan throughput: {}'.format(scanthr))
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                round(scanthr, 1)
            )


class SecondaryIndexingScanLatencyTest(SecondaryIndexTest):

    """
    The test applies scan workload against the 2i server and measures
    and reports the average scan throughput
    """
    COLLECTORS = {'secondary_stats': True, 'secondary_latency': True}

    @with_stats
    def apply_scanworkload(self):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        logger.info('Initiating scan workload with stats output')
        if self.test_config.secondaryindex_settings.stale == 'false':
            self.configfile = 'scripts/config_scanlatency_sessionconsistent.json'
        else:
            self.configfile = 'scripts/config_scanlatency.json'
        cmdstr = "cbindexperf -cluster {} -auth=\"{}:{}\" -configfile {} -resultfile result.json -statsfile /root/statsfile".format(self.indexnode, rest_username, rest_password, self.configfile)
        status = subprocess.call(cmdstr, shell=True)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')

    def run(self):
        rmfile = "rm -f {}".format(self.test_config.stats_settings.secondary_statsfile)
        status = subprocess.call(rmfile, shell=True)
        if status != 0:
            raise Exception('existing 2i latency stats file could not be removed')
        else:
            logger.info('Existing 2i latency stats file removed')

        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        from_ts, to_ts = self.build_secondaryindex()
        self.access_bg()
        self.apply_scanworkload()
        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(
                *self.metric_helper.calc_secondaryscan_latency(percentile=80)
            )


class SecondaryIndexingLatencyTest(SecondaryIndexTest):

    """
    This test applies scan workload against a 2i server and measures
    the indexing latency
    """

    @with_stats
    def apply_scanworkload(self):
        rest_username, rest_password = self.cluster_spec.rest_credentials
        logger.info('Initiating the scan workload')
        cmdstr = "cbindexperf -cluster {} -auth=\"{}:{}\" -configfile scripts/config_indexinglatency.json -resultfile result.json".format(self.indexnode, rest_username, rest_password)
        status = subprocess.call(cmdstr, shell=True)
        if status != 0:
            raise Exception('Scan workload could not be applied')
        else:
            logger.info('Scan workload applied')
        return status

    def run(self):
        self.load()

        self.wait_for_persistence()
        self.compact_bucket()

        self.hot_load()

        self.build_secondaryindex()

        num_samples = 100
        samples = []

        while num_samples != 0:
            access_settings = self.test_config.access_settings
            self.worker_manager.run_workload(access_settings, self.target_iterator)
            self.worker_manager.wait_for_workers()
            time_before = time.time()
            status = self.apply_scanworkload()
            time_after = time.time()
            if status == 0:
                num_samples = num_samples - 1
                time_elapsed = (time_after - time_before) / 1000000.0
                samples.append(time_elapsed)

        temp = np.array(samples)
        indexing_latency_percentile_80 = np.percentile(temp, 80)

        logger.info('Indexing latency (80th percentile): {} ms.'.format(indexing_latency_percentile_80))

        if self.test_config.stats_settings.enabled:
            self.reporter.post_to_sf(indexing_latency_percentile_80)

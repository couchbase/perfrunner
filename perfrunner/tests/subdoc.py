import random
from threading import Thread
from time import time, sleep

import numpy as np
from couchbase import Couchbase
from couchbase.user_constants import OBS_NOTFOUND
from logger import logger
from mc_bin_client.mc_bin_client import MemcachedClient, MemcachedError
from tap import TAP

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import pretty_dict, uhex, log_phase
from perfrunner.helpers.worker import run_pillowfight_via_celery
from perfrunner.tests import PerfTest
from perfrunner.workloads.revAB.__main__ import produce_AB
from perfrunner.workloads.revAB.graph import generate_graph, PersonIterator
from perfrunner.workloads.tcmalloc import WorkloadGen
from perfrunner.workloads.pathoGen import PathoGen
from perfrunner.workloads.pillowfight import Pillowfight
from perfrunner.helpers.metrics import SgwMetricHelper, MetricHelper

import json


class SubdocTest(PerfTest):

    """
    The most basic KV workflow:
        Initial data load ->
            Persistence and intra-cluster replication (for consistency) ->
                Data compaction (for consistency) ->
                    "Hot" load or working set warm up ->
                        "access" phase or active workload
    """

    @with_stats
    def access(self):
        super(SubdocTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.compact_bucket()

        self.hot_load()

        self.workload = self.test_config.access_settings
        self.access_bg()
        self.access()


class  SubdocKVTest(SubdocTest):

    def run(self):
        super(SubdocKVTest, self).run()
        self.metric_db_servers_helper = MetricHelper(self)
        network_matrix = self.metric_db_servers_helper.calc_network_bandwidth
        logger.info(
            'Network bandwidth: {}'.format(json.dumps(network_matrix, indent=4))
        )


class SubdocLatencyTest(SubdocTest):

    """
    Enables reporting of GET and SET latency.
    """

    COLLECTORS = {'subdoc_latency': True}
    LATENCY_METRICS = ('get', 'set')

    def run(self):
        super(SubdocLatencyTest, self).run()
        if self.test_config.stats_settings.enabled:
            for operation in self.LATENCY_METRICS:
                self.reporter.post_to_sf(
                    *self.metric_helper.calc_kv_latency(operation=operation,
                                                        percentile=95,
                                                        dbname='spring_subdoc_latency'
                                                        )
                )

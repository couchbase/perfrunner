import copy
import os
import shutil

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import jts_run_task, jts_warmup_task
from perfrunner.tests import PerfTest


class JTSTest(PerfTest):

    result = dict()

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.access = self.test_config.jts_access_settings
        self.showfast = self.test_config.showfast

    def download_jts(self):
        if self.worker_manager.is_remote:
            self.remote.init_jts(repo=self.access.jts_repo,
                                 branch=self.access.jts_repo_branch,
                                 worker_home=self.worker_manager.WORKER_HOME,
                                 jts_home=self.access.jts_home_dir)
        else:
            local.init_jts(repo=self.access.jts_repo,
                           branch=self.access.jts_repo_branch,
                           jts_home=self.access.jts_home_dir)

    @with_stats
    @with_profiles
    def run_test(self):
        self.run_phase('jts run phase', jts_run_task, self.access, self.target_iterator)
        self._download_logs()

    def warmup(self):
        if int(self.access.warmup_query_workers) > 0:
            self.run_phase('jts warmup phase', jts_warmup_task, self.access, self.target_iterator)

    def _download_logs(self):
        local_dir = self.access.jts_logs_dir
        if self.worker_manager.is_remote:
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir, ignore_errors=True)
            os.makedirs(local_dir)
            self.remote.get_jts_logs(self.worker_manager.WORKER_HOME,
                                     self.access.jts_home_dir,
                                     self.access.jts_logs_dir)
        else:
            local.get_jts_logs(self.access.jts_home_dir, local_dir)


class FTSTest(JTSTest):
    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.fts_master_node = self.fts_nodes[0]

    def delete_index(self):
        self.rest.delete_fts_index(self.fts_master_node,
                                   self.access.couchbase_index_name)

    def create_index(self):
        definition = read_json(self.access.couchbase_index_configfile)
        definition.update({
            'name': self.access.couchbase_index_name,
            'sourceName': self.test_config.buckets[0],
        })
        indexed_fld = copy.deepcopy(definition["params"]["mapping"]["default_mapping"])
        num_collections = self.access.collections
        scope = "scope"+str(self.access.scope)
        # Adding the collections index definition here ;
        # -1 is bucket level
        # 0 is default scope
        # non 0  = non default scope
        if self.access.scope != -1:
            types_col = {}
            definition["params"]["mapping"]["default_mapping"]["enabled"] = False
            definition["params"]["doc_config"]["mode"] = "scope.collection.type_field"
            if num_collections == 0:
                types_col["_default._default"] = indexed_fld
            else:
                for coll_num in range(0, num_collections):
                    collection_name = self.access.collection_prefix+str(coll_num)
                    # key_name is the key for the collection type mapping
                    key_name = scope+"."+collection_name
                    types_col[key_name] = indexed_fld
            definition["params"]["mapping"]["types"] = types_col
            logger.info('Index definition: {}'.format(pretty_dict(definition)))
        if self.access.couchbase_index_type:
            definition["params"]["store"]["indexType"] = self.access.couchbase_index_type
        logger.info('Index definition: {}'.format(pretty_dict(definition)))
        self.rest.create_fts_index(self.fts_master_node,
                                   self.access.couchbase_index_name, definition)

    def wait_for_index(self):
        self.monitor.monitor_fts_indexing_queue(self.fts_master_node,
                                                self.access.couchbase_index_name,
                                                int(self.access.test_total_docs))

    def wait_for_index_persistence(self):
        self.monitor.monitor_fts_index_persistence(self.fts_nodes,
                                                   self.access.couchbase_index_name)

    def data_restore(self):
        if self.access.scope != 0:
            self.fts_cbimport()
        else:
            self.restore()

    def cleanup_and_restore(self):
        self.delete_index()
        self.data_restore()
        self.wait_for_persistence()


class FTSThroughputTest(FTSTest):

    COLLECTORS = {'jts_stats': True, 'fts_stats': True}

    def report_kpi(self):
        self.reporter.post(*self.metrics.jts_throughput())

    def run(self):
        self.cleanup_and_restore()
        self.create_index()
        self.download_jts()
        self.wait_for_index()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSLatencyTest(FTSTest):

    COLLECTORS = {'jts_stats': True, 'fts_stats': True}

    def report_kpi(self):
        self.reporter.post(*self.metrics.jts_latency(percentile=80))
        self.reporter.post(*self.metrics.jts_latency(percentile=95))

    def run(self):
        self.cleanup_and_restore()
        self.create_index()
        self.download_jts()
        self.wait_for_index()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSIndexTest(FTSTest):
    COLLECTORS = {'fts_stats': True}

    def report_kpi(self, time_elapsed: int, size: int):
        self.reporter.post(
            *self.metrics.fts_index(time_elapsed)
        )
        self.reporter.post(
            *self.metrics.fts_index_size(size)
        )

    @with_stats
    @timeit
    def build_index(self):
        self.create_index()
        self.wait_for_index()

    def calculate_index_size(self) -> int:
        metric = '{}:{}:{}'.format(self.test_config.buckets[0],
                                   self.access.couchbase_index_name,
                                   'num_bytes_used_disk')
        size = 0
        for host in self.fts_nodes:
            stats = self.rest.get_fts_stats(host)
            size += stats[metric]
        return size

    def run(self):
        self.cleanup_and_restore()
        time_elapsed = self.build_index()
        self.wait_for_index_persistence()
        size = self.calculate_index_size()
        self.report_kpi(time_elapsed, size)

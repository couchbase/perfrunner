import copy
import os
import shutil

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import (
    jts_run_task,
    jts_warmup_task,
    spring_task,
)
from perfrunner.tests import PerfTest


class JTSTest(PerfTest):

    result = dict()

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.access = self.test_config.jts_access_settings
        if self.test_config.collection.collection_map:
            self.access.collections_enabled = True
        self.showfast = self.test_config.showfast
        self.fts_index_map = dict()

    def download_jts(self):
        if self.worker_manager.is_remote:
            self.remote.init_jts(
                repo=self.access.jts_repo,
                branch=self.access.jts_repo_branch,
                worker_home=self.worker_manager.WORKER_HOME,
                jts_home=self.access.jts_home_dir
            )
        else:
            local.init_jts(
                repo=self.access.jts_repo,
                branch=self.access.jts_repo_branch,
                jts_home=self.access.jts_home_dir
            )

    @with_stats
    @with_profiles
    def run_test(self):
        self.run_phase(
            'jts run phase',
            jts_run_task,
            self.access,
            self.target_iterator
        )
        self._download_logs()

    def warmup(self):
        if int(self.access.warmup_query_workers) > 0:
            self.run_phase(
                'jts warmup phase',
                jts_warmup_task,
                self.access,
                self.target_iterator
            )

    def _download_logs(self):
        local_dir = self.access.jts_logs_dir
        if self.worker_manager.is_remote:
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir, ignore_errors=True)
            os.makedirs(local_dir)
            self.remote.get_jts_logs(
                self.worker_manager.WORKER_HOME,
                self.access.jts_home_dir,
                self.access.jts_logs_dir
            )
        else:
            local.get_jts_logs(
                self.access.jts_home_dir,
                local_dir
            )


class FTSTest(JTSTest):

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.fts_master_node = self.fts_nodes[0]

    def delete_index(self):
        self.rest.delete_fts_index(
            self.fts_master_node,
            self.access.couchbase_index_name
        )

    def create_indexes(self):
        if not self.test_config.collection.collection_map:
            index_id = 0
            for bucket in self.test_config.buckets:
                for index_def in self.access.couchbase_index_configfile.split(","):
                    index_name = "{}-{}".format(self.access.couchbase_index_name, index_id)
                    definition = read_json(index_def)
                    definition.update({
                        'name': index_name,
                        'sourceName': bucket,
                    })
                    if self.access.couchbase_index_type:
                        definition["params"]["store"]["indexType"] = \
                            self.access.couchbase_index_type
                    logger.info('Index definition: {}'.format(pretty_dict(definition)))
                    self.rest.create_fts_index(
                        self.fts_master_node,
                        index_name,
                        definition
                    )

                    self.fts_index_map[index_name] = {"bucket": bucket,
                                                      "scope": "_default",
                                                      "collections": ["_default"]}
                    index_id += 1
        else:
            collection_map = self.test_config.collection.collection_map
            index_id = 0
            for bucket in self.test_config.buckets:
                load_targets = []
                for scope in collection_map[bucket].keys():
                    for collection in collection_map[bucket][scope].keys():
                        if collection_map[bucket][scope][collection]['load'] == 1:
                            load_targets += [scope+"."+collection]
                partitions = self.test_config.jts_access_settings.index_groups
                if len(load_targets) >= partitions:
                    k, m = divmod(len(load_targets), partitions)
                    index_groups = (load_targets[i * k + min(i, m):(i + 1) * k + min(i + 1, m)]
                                    for i in range(partitions))
                else:
                    raise Exception("index group partitions must be <= number of collections")
                for index_group in index_groups:
                    for index_def in self.access.couchbase_index_configfile.split(","):
                        index_name = "{}-{}".format(self.access.couchbase_index_name, index_id)
                        definition = read_json(index_def)
                        definition.update({
                            'name': index_name,
                            'sourceName': bucket,
                        })
                        indexed_fld = copy.deepcopy(
                            definition["params"]["mapping"]["default_mapping"])
                        types_col = {}
                        definition["params"]["mapping"]["default_mapping"]["enabled"] = False
                        definition["params"]["doc_config"]["mode"] = "scope.collection.type_field"
                        for collection in index_group:
                            types_col[collection] = indexed_fld
                        definition["params"]["mapping"]["types"] = types_col
                        if self.access.couchbase_index_type:
                            definition["params"]["store"]["indexType"] = \
                                self.access.couchbase_index_type
                        logger.info('Index definition: {}'.format(pretty_dict(definition)))
                        self.rest.create_fts_index(
                            self.fts_master_node,
                            index_name,
                            definition
                        )
                        self.fts_index_map[index_name] = \
                            {"bucket": bucket,
                             "scope": index_group[0].split(".")[0],
                             "collections": [scope_collection.split(".")[1]
                                             for scope_collection in index_group]}
                        index_id += 1
        self.access.fts_index_map = self.fts_index_map

    def wait_for_indexes(self):
        for bucket in self.test_config.buckets:
            if self.test_config.collection.collection_map:
                collection_map = self.test_config.collection.collection_map
                load_targets = []
                for scope in collection_map[bucket].keys():
                    for collection in collection_map[bucket][scope].keys():
                        if collection_map[bucket][scope][collection]['load'] == 1:
                            load_targets += [scope+":"+collection]
                items_per_collections = \
                    len(load_targets) // self.test_config.load_settings.items
                collections_per_index_group = \
                    len(load_targets) // self.test_config.jts_access_settings.index_groups
                items_per_index = items_per_collections * collections_per_index_group
            else:
                items_per_index = self.access.test_total_docs
            for index_name, index_targets in self.fts_index_map.items():
                index_bucket = index_targets["bucket"]
                if index_bucket == bucket:
                    self.monitor.monitor_fts_indexing_queue(
                        self.fts_master_node,
                        index_name,
                        items_per_index
                    )

    def wait_for_index_persistence(self):
        for index_name in self.fts_index_map.keys():
            self.monitor.monitor_fts_index_persistence(
                self.fts_nodes,
                index_name
            )

    def spread_data(self):
        settings = self.test_config.load_settings
        settings.seq_upserts = False
        self.run_phase(
            'data spread',
            spring_task,
            settings,
            self.target_iterator
        )

    def data_restore(self):
        if self.test_config.collection.collection_map:
            self.fts_collections_restore()
            self.wait_for_persistence()
            self.spread_data()
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
        self.create_indexes()
        self.download_jts()
        self.wait_for_indexes()
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
        self.create_indexes()
        self.download_jts()
        self.wait_for_indexes()
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
        self.create_indexes()
        self.wait_for_indexes()

    def calculate_index_size(self) -> int:
        metric = '{}:{}:{}'.format(
            self.test_config.buckets[0],
            self.access.couchbase_index_name,
            'num_bytes_used_disk'
        )
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

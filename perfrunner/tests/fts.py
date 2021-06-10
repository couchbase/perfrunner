import copy
import os
import shutil
import time

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
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
        self.fts_index_defs = dict()

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

    def collection_split(self, collection_map, bucket, scope):
        collection_list_per_index = []
        start = 0
        collection_list = list(collection_map[bucket][scope].keys())
        total_num_collections = len(collection_list)
        collections_per_index = int(total_num_collections / int(self.access.index_groups))
        end = collections_per_index
        end_index = collections_per_index*int(self.access.index_groups)

        if int(self.access.index_groups) == 1:
            # if index is built on all the collections
            return [collection_list]
        while end <= end_index:
            collection_list_per_index.append(collection_list[start:end])
            start = end
            end = end + collections_per_index

        return collection_list_per_index

    def get_collection_index_def(self, collection_list, scope_name, index_type_mapping,
                                 custom_type_mapping_keys=[]):
        coll_type_mapping = []
        for collection_group in collection_list:
            types_col = {}
            for col_name in collection_group:
                key_name = "{}.{}".format(scope_name, col_name)
                if len(custom_type_mapping_keys) != 0:
                    for ind, custom_key in enumerate(custom_type_mapping_keys):
                        key_name = key_name+".{}".format(custom_key)
                        types_col[key_name] = index_type_mapping[ind]
                else:
                    types_col[key_name] = index_type_mapping
            coll_type_mapping.append(types_col)
        return coll_type_mapping

    def create_fts_index_definitions(self):
        logger.info("In the Indexing function:")
        # On a singlee fts index definition
        index_def = read_json(self.access.couchbase_index_configfile)
        collection_map = self.test_config.collection.collection_map
        # this variable will hold the key valuesfor the custom type mapping
        key_values = []
        # index_type_mapping holds the type mapping part of the index
        index_type_mapping = {}
        index_id = 0
        # on a single bucket
        bucket_name = self.test_config.buckets[0]
        index_def.update({
            'sourceName': bucket_name,
        })
        if self.access.couchbase_index_type:
            index_def["params"]["store"]["indexType"] = self.access.couchbase_index_type

        # setting the type mapping for collection indexes
        if collection_map:
            index_def["params"]["doc_config"]["mode"] = "scope.collection.type_field"

        # Geo queries have a slightly different index type with custom type mapping
        if "types" in list(index_def["params"]["mapping"].keys()):
            # if any custom type mapping is present
            key_values = list(index_def["params"]["mapping"]["types"].keys())

        default_index_type_mapping = {}
        # for default collection settings
        if collection_map and len(collection_map[bucket_name].keys()) == 1:
            key_name = "{}.{}".format("_default", "_default")
            if len(key_values) > 0:
                # there is custom mapping
                for type_name in key_values:
                    type_mapping_key_name = key_name + ".{}".format(type_name)
                    default_index_type_mapping[type_mapping_key_name] = \
                        index_def["params"]["mapping"]["types"][type_name]
            else:
                default_index_type_mapping[key_name] = index_type_mapping
                index_def["params"]["mapping"]["default_mapping"]["enabled"] = False

        # if collection map exists , the index is on collections
        if collection_map and len(collection_map[bucket_name].keys()) > 1:
            scope_names = list(collection_map[bucket_name].keys())[1:]
            if "types" in list(index_def["params"]["mapping"].keys()):
                # to get the custom mapping
                temp = index_def["params"]["mapping"]["types"]
                index_type_mapping = [temp[type_key] for type_key in key_values]
            else:
                index_type_mapping = \
                    copy.deepcopy(index_def["params"]["mapping"]["default_mapping"])
                index_def["params"]["mapping"]["default_mapping"]["enabled"] = False
            # on a single scope
            collection_name_list = list(collection_map[bucket_name][scope_names[0]].keys())
            collection_list = self.collection_split(collection_map, bucket_name, scope_names[0])
            index_type_mapping_per_group = self.get_collection_index_def(collection_list,
                                                                         scope_names[0],
                                                                         index_type_mapping,
                                                                         key_values
                                                                         )
            # calculating the doc per collection for persistence
            num_docs_per_collection = \
                self.test_config.load_settings.items // len(collection_name_list)
            items_per_index = num_docs_per_collection * len(collection_list[0])
            # as the test is on a single scope:
            for coll_group_id, collection_type_mapping in enumerate(index_type_mapping_per_group):
                for index_count in range(0, self.access.indexes_per_group):
                    index_name = "{}-{}".format(self.access.couchbase_index_name, index_id)
                    index_def.update({
                        'name': index_name,
                    })
                    index_def["params"]["mapping"]["types"] = collection_type_mapping
                    self.fts_index_map[index_name] = \
                        {
                            "bucket": bucket_name,
                            "scope": scope_names[0],
                            "collections": collection_list[coll_group_id],
                            "total_docs": items_per_index
                        }
                    self.fts_index_defs[index_name] = {
                        "index_def": index_def
                    }
                    index_id += 1

        else:
            # default , multiple indexes with the same index def
            for num_indexes in range(0, self.access.indexes_per_group):
                index_name = "{}-{}".format(self.access.couchbase_index_name, num_indexes)
                index_def.update({
                    'name': index_name,
                })
            # for indexes with default collection and scope in the index
            if collection_map and len(collection_map[bucket_name].keys()) == 1:
                index_def["params"]["mapping"]["types"] = default_index_type_mapping
            self.fts_index_map[index_name] = {
                "bucket": bucket_name,
                "scope": "_default",
                "collections": ["_default"],
                "total_docs": int(self.access.test_total_docs)
            }
            self.fts_index_defs[index_name] = {
                "index_def": index_def
            }

        self.access.fts_index_map = self.fts_index_map

    def create_fts_indexes(self):
        total_time = 0
        for index_name in self.fts_index_defs.keys():
            index_def = self.fts_index_defs[index_name]['index_def']
            logger.info('Index definition: {}'.format(pretty_dict(index_def)))
            t0 = time.time()
            self.rest.create_fts_index(self.fts_master_node, index_name, index_def)
            self.wait_for_index(index_name)
            t1 = time.time()
            logger.info("Time taken by {} is {} s".format(index_name, (t1-t0)))
            total_time = total_time+(t1-t0)
        return total_time

    def wait_for_index(self, index_name):
        self.monitor.monitor_fts_indexing_queue(
            self.fts_master_node,
            index_name,
            self.fts_index_map[index_name]["total_docs"]
        )

    def wait_for_index_persistence(self, fts_nodes=None):
        if fts_nodes is None:
            fts_nodes = self.fts_nodes
        for index_name in self.fts_index_map.keys():
            self.monitor.monitor_fts_index_persistence(
                fts_nodes,
                index_name
            )

    def add_extra_fts_parameters(self):
        logger.info("Adding extra parameter to the fts nodes")
        nodes_before_rebalance = self.test_config.cluster.initial_nodes[0]
        servers_and_roles = self.cluster_spec.servers_and_roles
        fts_nodes_before = []
        for i in range(0, nodes_before_rebalance):
            host = servers_and_roles[i][0]
            roles = servers_and_roles[i][1]
            if "fts" in roles:
                fts_nodes_before.append(host)
        if len(self.access.fts_node_level_parameters.keys()) != 0:
            node_level_params = self.access.fts_node_level_parameters
            for node in fts_nodes_before:
                self.rest.fts_set_node_level_parameters(node_level_params, node)
        return fts_nodes_before

    def calculate_index_size(self) -> int:
        size = 0
        for index_name, index_info in self.fts_index_map.items():
            metric = '{}:{}:{}'.format(
                index_info['bucket'],
                index_name,
                'num_bytes_used_disk'
            )
            for host in self.fts_nodes:
                stats = self.rest.get_fts_stats(host)
                size += stats[metric]
        return size

    @with_stats
    def build_indexes(self):
        elapsed_time = self.create_fts_indexes()
        return elapsed_time

    def spread_data(self):
        settings = self.test_config.load_settings
        #  if self.access.test_collection_query_mode == "collection_specific":
        #      settings.fts_data_spread_worker_type = "collection_specific"
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
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
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
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
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
    def build_index(self, fts_nodes):
        elapsed_time = self.create_fts_indexes()
        self.wait_for_index_persistence(fts_nodes)
        return elapsed_time

    def run(self):
        self.cleanup_and_restore()
        fts_nodes = self.add_extra_fts_parameters()
        self.create_fts_index_definitions()
        time_elapsed = self.build_index(fts_nodes)
        size = self.calculate_index_size()
        self.report_kpi(time_elapsed, size)

import copy
import math
import os
import shutil
import threading
import time
from collections import deque
from typing import Any

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import pretty_dict, read_json, run_aws_cli_command
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import WorkloadPhase, jts_run_task, jts_warmup_task, spring_task
from perfrunner.settings import TargetIterator
from perfrunner.tests import PerfTest
from perfrunner.utils.fts.vector_recall_calculator import VectorRecallCalculator


class JTSTest(PerfTest):

    result = dict()

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.jts_access = self.test_config.jts_access_settings
        if self.test_config.collection.collection_map:
            self.jts_access.collections_enabled = True
        self.showfast = self.test_config.showfast
        self.fts_index_map = dict()
        self.fts_index_defs = dict()
        self.fts_index_name_flag = False
        self.jts_access.capella_infrastructure = self.cluster_spec.capella_infrastructure
        self.jts_access.fts_raw_query_map = None
        if self.jts_access.raw_query_map_file:
            self.jts_access.fts_raw_query_map = read_json(self.jts_access.raw_query_map_file)

    def download_jts(self):
        if self.worker_manager.is_remote:
            self.remote.init_jts(
                repo=self.jts_access.jts_repo,
                branch=self.jts_access.jts_repo_branch,
                worker_home=self.worker_manager.WORKER_HOME,
                jts_home=self.jts_access.jts_home_dir
            )
        else:
            local.init_jts(
                repo=self.jts_access.jts_repo,
                branch=self.jts_access.jts_repo_branch,
                jts_home=self.jts_access.jts_home_dir
            )

    @with_stats
    @with_profiles
    def run_test(self):
        logger.info('Running jts run phase')
        phases = [WorkloadPhase(jts_run_task, self.target_iterator, self.jts_access)]
        self.log_task_settings(phases)
        self.worker_manager.run_fg_phases(phases)
        self._download_logs()

    def warmup(self):
        if int(self.jts_access.warmup_query_workers) > 0:
            logger.info('Running jts warmup phase')
            phases = [WorkloadPhase(jts_warmup_task, self.target_iterator, self.jts_access)]
            self.log_task_settings(phases)
            self.worker_manager.run_fg_phases(phases)

    def _download_logs(self):
        local_dir = self.jts_access.jts_logs_dir
        if self.worker_manager.is_remote:
            if os.path.exists(local_dir):
                shutil.rmtree(local_dir, ignore_errors=True)
            os.makedirs(local_dir)
            self.remote.get_jts_logs(
                self.worker_manager.WORKER_HOME,
                self.jts_access.jts_home_dir,
                self.jts_access.jts_logs_dir
            )
        else:
            local.get_jts_logs(
                self.jts_access.jts_home_dir,
                local_dir
            )

    def custom_target_iterator(self, num_buckets):
        bucket_list = ['bucket-{}'.format(i + 1) for i in range(num_buckets)]
        self.target_iterator = TargetIterator(
            self.cluster_spec, self.test_config, buckets=bucket_list)


class FTSTest(JTSTest):

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose)
        self.fts_master_node = self.fts_nodes[0]

    def delete_index(self):
        self.rest.delete_fts_index(
            self.fts_master_node,
            self.jts_access.couchbase_index_name
        )

    def delete_indexes(self):
        for index_name in self.fts_index_defs.keys():
            fts_updated_name = index_name
            if self.fts_index_name_flag:
                iname = copy.deepcopy(index_name)
                fts_updated_name = "{}.{}.{}".format(
                    self.fts_index_map[index_name]["bucket"],
                    self.fts_index_map[index_name]["scope"],
                    iname)
            self.rest.delete_fts_index(
                self.fts_master_node,
                fts_updated_name
            )
        time.sleep(2)

    def collection_split(self, collection_map, bucket, scope):
        collection_list_per_index = []
        collection_list = list(collection_map[bucket][scope].keys())
        total_num_collections = len(collection_list)
        collections_per_index = int(total_num_collections / self.jts_access.index_groups)

        if int(self.jts_access.index_groups) == 1:
            # if index is built on all the collections
            return [collection_list]
        for i in range(self.jts_access.index_groups):
            start = collections_per_index * i
            end = collections_per_index * (i + 1)
            collection_list_per_index.append(collection_list[start:end])

        return collection_list_per_index

    def update_index_def(self, index_def):
        collection_map = self.test_config.collection.collection_map
        # changing index configuration for serverless mode
        if self.test_config.cluster.serverless_mode:
            index_def["planParams"]["maxPartitionsPerPIndex"] = 1024
            if "indexPartitions" not in list(index_def["planParams"].keys()) \
                    or index_def["planParams"]["indexPartitions"] < 1:
                index_def["planParams"]["indexPartitions"] = 1
            if "numReplicas" not in list(index_def["planParams"].keys()) \
                    or index_def["planParams"]["numReplicas"] < 1:
                index_def["planParams"]["numReplicas"] = 1

        if self.jts_access.couchbase_index_type:
            index_def["params"]["store"]["indexType"] = \
                self.jts_access.couchbase_index_type

        # setting the type mapping for collection indexes
        if collection_map:
            index_def["params"]["doc_config"]["mode"] = "scope.collection.type_field"
        return index_def

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

    def update_key_value_mutate_nested(self, value: dict, target_key: str,
                                       new_value: Any) -> bool:
        if isinstance(value, dict):
            found_occurrence = False
            for key, _value in value.items():
                if key == target_key:
                    value[key] = new_value
                    found_occurrence = True
                else:
                    if self.update_key_value_mutate_nested(_value, target_key, new_value):
                        found_occurrence = True
            return found_occurrence
        elif isinstance(value, list):
            found_occurrence = False
            for element in value:
                if self.update_key_value_mutate_nested(element, target_key, new_value):
                    found_occurrence = True
            return found_occurrence
        return False

    def create_fts_index_definitions(self):
        bucket_name = self.test_config.buckets[0]
        result = self.rest.get_bucket_info(self.fts_nodes[0], bucket_name)
        cb_version = result["nodes"][0]["version"].split("-")[0]
        cb_build = result["nodes"][0]["version"].split("-")[1]
        logger.info("This is the version {} and build {}".format(cb_version, cb_build))
        if cb_version == "7.5.0" and int(cb_build) < 3699:
            self.fts_index_name_flag = True
        else:
            self.fts_index_name_flag = False

        logger.info("Creating indexing definitions:")
        collection_map = self.test_config.collection.collection_map
        index_id = 0

        # this variable will hold the key valuesfor the custom type mapping
        key_values = []

        if self.jts_access.test_query_mode == 'mixed':
            self.mixed_query_map = dict()
            self.mixed_query_map = read_json(self.jts_access.couchbase_index_configmap)
            self.jts_access.mixed_query_map = self.mixed_query_map

        else:
            general_index_def = read_json(self.jts_access.couchbase_index_configfile)
            general_index_def = self.update_index_def(general_index_def)
            if self.jts_access.max_segment_size:
                general_index_def["params"]["store"]["scorchMergePlanOptions"] = {
                    "maxSegmentSize": int(self.jts_access.max_segment_size)
                }
            if self.update_key_value_mutate_nested(general_index_def, "dims",
                                                   self.jts_access.vector_dimension):
                if self.jts_access.vector_dimension == 0:
                    logger.interrupt("vector dimension not provided for vector search test")
                logger.info(f"Vector dimension successful update to \
                            {self.jts_access.vector_dimension}")
                if self.jts_access.vector_similarity_type:
                    self.update_key_value_mutate_nested(general_index_def,
                                                        "similarity",
                                                        self.jts_access.vector_similarity_type)
                    logger.info(f"Vector similarity successful updated to \
                                {self.jts_access.vector_similarity_type}")
            if self.jts_access.vector_index_optimized_for:
                self.update_key_value_mutate_nested(general_index_def,
                                                    "vector_index_optimized_for",
                                                    self.jts_access.vector_index_optimized_for)
            if self.jts_access.index_partitions:
                index_partitions = int(self.jts_access.index_partitions)
                logger.info(f"Successfully updated index partitions to {index_partitions}")
                self.update_key_value_mutate_nested(general_index_def, "indexPartitions",
                                                    index_partitions)
                self.update_key_value_mutate_nested(general_index_def, "maxPartitionsPerPIndex",
                                                    math.ceil(1024/index_partitions))
            if self.jts_access.index_replicas:
                general_index_def["planParams"]["numReplicas"] = int(self.jts_access.index_replicas)

            # Geo queries have a slightly different index type with custom type mapping
            if "types" in list(general_index_def["params"]["mapping"].keys()):
                # if any custom type mapping is present
                key_values = list(general_index_def["params"]["mapping"]["types"].keys())

            if self.jts_access.couchbase_index_type:
                general_index_def["params"]["store"]["indexType"] = \
                    self.jts_access.couchbase_index_type

        # index_type_mapping holds the type mapping part of the index
        index_type_mapping = {}

        for bucket_name in self.test_config.buckets:
            if self.test_config.jts_access_settings.test_query_mode == 'mixed':
                bucket_index_def = read_json(
                    self.mixed_query_map[bucket_name]["couchbase_index_configfile"])
                bucket_index_def = self.update_index_def(bucket_index_def)
                # Geo queries have a slightly different index type with custom type mapping
                if "types" in list(bucket_index_def["params"]["mapping"].keys()):
                    # if any custom type mapping is present
                    key_values = list(bucket_index_def["params"]["mapping"]["types"].keys())

            else:
                bucket_index_def = copy.deepcopy(general_index_def)

            bucket_index_def.update({
                'sourceName': bucket_name,
            })
            # if collection map exists , the index is on collections
            if collection_map and len(collection_map[bucket_name].keys()) > 1:
                scope_names = list(collection_map[bucket_name].keys())[1:]
                if "types" in list(bucket_index_def["params"]["mapping"].keys()):
                    # to get the custom mapping
                    temp = bucket_index_def["params"]["mapping"]["types"]
                    index_type_mapping = [temp[type_key] for type_key in key_values]
                else:
                    index_type_mapping = \
                        copy.deepcopy(bucket_index_def["params"]["mapping"]["default_mapping"])
                    bucket_index_def["params"]["mapping"]["default_mapping"]["enabled"] = False
                    bucket_index_def["params"]["mapping"]["default_mapping"].pop("properties",
                                                                                 None)

                for scope_name in scope_names:
                    collection_name_list = list(collection_map[bucket_name][scope_name].keys())
                    collection_list = self.collection_split(collection_map, bucket_name, scope_name)
                    index_type_mapping_per_group = self.get_collection_index_def(collection_list,
                                                                                 scope_name,
                                                                                 index_type_mapping,
                                                                                 key_values
                                                                                 )

                    # calculating the doc per collection for persistence
                    num_collections = len(collection_name_list) * len(scope_names)
                    num_docs_per_collection = \
                        self.test_config.load_settings.items // num_collections
                    items_per_index = num_docs_per_collection * len(collection_list[0])

                    for coll_group_id, collection_type_mapping in \
                            enumerate(index_type_mapping_per_group):
                        for index_count in range(0, self.jts_access.indexes_per_group):
                            index_name = "{}-{}".format(self.jts_access.couchbase_index_name,
                                                        index_id)
                            collection_index_def = copy.deepcopy(bucket_index_def)
                            collection_index_def.update({
                                'name': index_name,
                            })
                            collection_index_def["params"]["mapping"]["types"] = \
                                collection_type_mapping
                            self.fts_index_map[index_name] = \
                                {
                                    "bucket": bucket_name,
                                    "scope": scope_name,
                                    "collections": collection_list[coll_group_id],
                                    "total_docs": items_per_index
                            }
                            self.fts_index_defs[index_name] = {
                                "index_def": collection_index_def
                            }
                            index_id += 1
            else:
                default_index_type_mapping = {}
                # for default scope and collection settings
                if collection_map and len(collection_map[bucket_name].keys()) == 1:
                    key_name = "{}.{}".format("_default", "_default")
                    if len(key_values) > 0:
                        # there is custom mapping
                        for type_name in key_values:
                            type_mapping_key_name = key_name + ".{}".format(type_name)
                            default_index_type_mapping[type_mapping_key_name] = \
                                bucket_index_def["params"]["mapping"]["types"][type_name]
                    else:
                        default_index_type_mapping[key_name] = index_type_mapping
                        bucket_index_def["params"]["mapping"]["default_mapping"]["enabled"] = False

                # default, multiple indexes with the same index def
                for num_indexes in range(0, self.jts_access.indexes_per_group):
                    index_name = "{}-{}".format(self.jts_access.couchbase_index_name, num_indexes)
                    collection_index_def = copy.deepcopy(bucket_index_def)
                    collection_index_def.update({
                        'name': index_name,
                    })

                    # for indexes with default collection and scope in the index
                    if collection_map and len(collection_map[bucket_name].keys()) == 1:
                        collection_index_def["params"]["mapping"]["types"] = \
                            default_index_type_mapping

                    self.fts_index_map[index_name] = {
                        "bucket": bucket_name,
                        "scope": "_default",
                        "collections": ["_default"],
                        "total_docs": int(self.jts_access.test_total_docs)
                    }

                    self.fts_index_defs[index_name] = {
                        "index_def": collection_index_def
                    }

        self.jts_access.fts_index_map = self.fts_index_map

    def create_fts_index_and_wait(self, index_name, index_def):
        time.sleep(1)
        self.rest.create_fts_index(self.fts_master_node, index_name, index_def)
        self.wait_for_index(index_name)

    def sync_index_create(self, index_def_list):
        for index_name, index_def in index_def_list:
            self.create_fts_index_and_wait(index_name, index_def)

    def async_index_create(self, thread_dict: dict):
        # pushing one index per bucket at a time
        thread_still_remaining = True
        while thread_still_remaining:
            current_thread_queue = []
            for thread_queue in thread_dict.values():
                if len(thread_queue) == 0:
                    # with notion that each queue has equal number of threads
                    thread_still_remaining = False
                    break
                thread = thread_queue.popleft()
                current_thread_queue.append(thread)
                thread.start()

            for current_thread in current_thread_queue:
                current_thread.join()

    def create_fts_indexes(self):
        total_time = 0
        thread_dict = {}
        index_def_list = []
        logger.info("this is the defs : {}".format(self.fts_index_defs))
        for index_name in self.fts_index_defs.keys():
            index_def = self.fts_index_defs[index_name]['index_def']
            logger.info('Index definition: {}'.format(pretty_dict(index_def)))
            if self.fts_index_name_flag:
                iname = copy.deepcopy(index_name)
                fts_updated_name = "{}.{}.{}".format(
                    self.fts_index_map[index_name]["bucket"],
                    self.fts_index_map[index_name]["scope"],
                    iname)
                self.fts_index_map[index_name]["full_index_name"] = fts_updated_name
            bucket_name = self.fts_index_map[index_name]["bucket"]
            if self.jts_access.index_creation_style == 'async':
                if thread_dict.get(bucket_name, None) is None:
                    thread_dict[bucket_name] = deque()
                thread_dict[bucket_name].append(threading.Thread(
                    target=self.create_fts_index_and_wait,
                    args=(index_name, index_def)))
            else:
                index_def_list.append([index_name, index_def])
        logger.info('Index map: {}'.format(pretty_dict(self.fts_index_map)))
        t0 = time.time()
        logger.info("Creating indexes {}hronously.".format(self.jts_access.index_creation_style))
        if self.jts_access.index_creation_style == 'async':
            self.async_index_create(thread_dict)
        else:
            self.sync_index_create(index_def_list)
        t1 = time.time()
        total_time = (t1-t0)
        return total_time

    def wait_for_index(self, index_name):
        if self.fts_index_name_flag is True:
            self.monitor.monitor_fts_indexing_queue(
                self.fts_master_node,
                self.fts_index_map[index_name]["full_index_name"],
                self.fts_index_map[index_name]["total_docs"],
                self.fts_index_map[index_name]["bucket"]
            )

        if self.fts_index_name_flag is False:
            self.monitor.monitor_fts_indexing_queue(
                self.fts_master_node,
                index_name,
                self.fts_index_map[index_name]["total_docs"]
            )

    def wait_for_index_persistence(self, fts_nodes=None):
        if fts_nodes is None:
            fts_nodes = self.fts_nodes
        logger.info("This is the list of hosts: {}".format(fts_nodes))
        if self.fts_index_name_flag is True:
            for index_name in self.fts_index_map.keys():
                self.monitor.monitor_fts_index_persistence(
                    hosts=fts_nodes,
                    index=self.fts_index_map[index_name]["full_index_name"],
                    bkt=self.fts_index_map[index_name]["bucket"]
                )

        if self.fts_index_name_flag is False:
            for index_name in self.fts_index_map.keys():
                self.monitor.monitor_fts_index_persistence(
                    hosts=fts_nodes,
                    index=index_name,
                    bkt=self.fts_index_map[index_name]["bucket"]
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
        if len(self.jts_access.fts_node_level_parameters.keys()) != 0:
            node_level_params = self.jts_access.fts_node_level_parameters
            for node in fts_nodes_before:
                self.rest.fts_set_node_level_parameters(node_level_params, node)
        return fts_nodes_before

    def calculate_index_size(self) -> int:
        size = 0
        for index_name, index_info in self.fts_index_map.items():
            if self.fts_index_name_flag is True:
                metric = '{}:{}:{}'.format(
                    index_info['bucket'],
                    index_info['full_index_name'],
                    'num_bytes_used_disk'
                )
            if self.fts_index_name_flag is False:
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
        #  if self.jts_access.test_collection_query_mode == "collection_specific":
        #      settings.fts_data_spread_worker_type = "collection_specific"
        settings.seq_upserts = False
        logger.info('Running data spread phase')
        phases = [WorkloadPhase(spring_task, self.target_iterator, settings)]
        self.log_task_settings(phases)
        self.worker_manager.run_fg_phases(phases)

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

    def cloud_restore(self):
        self.remote.extract_cb_any(filename='couchbase',
                                   worker_home=self.worker_manager.WORKER_HOME)
        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)

        credential = local.read_aws_credential(
            self.test_config.backup_settings.aws_credential_path, self.cloud_infra)
        self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()
        self.remote.delete_existing_staging_dir(self.test_config.backup_settings.obj_staging_dir)
        collection_map = self.test_config.collection.collection_map
        restore_mapping = self.test_config.restore_settings.map_data
        if restore_mapping is None and collection_map:
            for target in self.target_iterator:
                if not collection_map.get(
                        target.bucket, {}).get("_default", {}).get("_default", {}).get('load', 0):
                    restore_mapping = \
                        "{0}._default._default={0}.scope-1.collection-1"\
                        .format(target.bucket)
        archive = self.test_config.restore_settings.backup_storage
        if self.test_config.restore_settings.modify_storage_dir_name:
            suffix_repo = "aws"
            if self.cluster_spec.capella_infrastructure:
                suffix_repo = self.cluster_spec.capella_backend
            archive = archive + "/" + suffix_repo

        self.remote.restore(cluster_spec=self.cluster_spec,
                            master_node=self.master_node,
                            threads=self.test_config.restore_settings.threads,
                            worker_home=self.worker_manager.WORKER_HOME,
                            archive=archive,
                            repo=self.test_config.restore_settings.backup_repo,
                            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
                            obj_region=self.test_config.backup_settings.obj_region,
                            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
                            use_tls=self.test_config.restore_settings.use_tls,
                            map_data=restore_mapping,
                            encrypted=self.test_config.restore_settings.encrypted,
                            passphrase=self.test_config.restore_settings.passphrase)
        self.wait_for_persistence()
        if self.test_config.collection.collection_map:
            self.spread_data()


class FTSThroughputTest(FTSTest):

    COLLECTORS = {'jts_stats': True, 'fts_stats': True}

    def report_kpi(self):
        self.reporter.post(*self.metrics.jts_throughput())

    def run(self):
        self.cleanup_and_restore()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        index_size = self.calculate_index_size()
        size_final = int(index_size / (1024 ** 2))
        logger.info("The index size is {} MB".format(size_final))
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSLatencyTest(FTSTest):

    COLLECTORS = {'jts_stats': True, 'fts_stats': True}

    def update_settings_for_multi_queries(self, test_config):
        for config in test_config.keys():
            logger.info(test_config[config])
            setattr(self.jts_access, config, test_config[config])
            logger.info(self.jts_access)

    def report_kpi(self):
        logger.info(f"Reporting latencies for percentiles \
                    {self.test_config.jts_access_settings.report_percentiles}")
        for percentile in self.test_config.jts_access_settings.report_percentiles:
            self.reporter.post(*self.metrics.jts_latency(percentile=int(percentile)))

    def run(self):
        self.cleanup_and_restore()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        index_size = self.calculate_index_size()
        size_final = int(index_size / (1024 ** 2))
        logger.info("The index size is {} MB".format(size_final))
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


class FTSIndexTestMultiTenant(FTSIndexTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.wait_for_index_persistence()
        fts_nodes = self.add_extra_fts_parameters()
        self.create_fts_index_definitions()
        time_elapsed = self.build_index(fts_nodes)
        size = self.calculate_index_size()
        self.report_kpi(time_elapsed, size)


class FTSThroughputCloudTest(FTSThroughputTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSLatencyCloudTest(FTSLatencyTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.compact_bucket()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSLatencyCloudBackupTest(FTSLatencyTest):

    def run(self):
        self.cloud_restore()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSThroughputCloudBackupTest(FTSLatencyCloudBackupTest):

    def report_kpi(self):
        self.reporter.post(*self.metrics.jts_throughput())


class FTSIndexBuildCloudBackupTest(FTSIndexTest):

    def cleanup_and_restore(self):
        self.cloud_restore()


class FTSIndexLoadTest(FTSIndexTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        fts_nodes = self.add_extra_fts_parameters()
        self.create_fts_index_definitions()
        time_elapsed = self.build_index(fts_nodes)
        size = self.calculate_index_size()
        self.report_kpi(time_elapsed, size)


class FTSThroughputLoadTest(FTSThroughputTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSThroughputAccessLoadTest(FTSThroughputTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.access_bg()
        self.run_test()
        self.report_kpi()


class FTSThroughputAccessLoadTestWithCustomBucket(FTSThroughputTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.custom_target_iterator(int(self.test_config.jts_access_settings.custom_num_buckets))
        self.warmup()
        self.access_bg()
        self.run_test()
        self.report_kpi()


class FTSLatencyLoadTest(FTSLatencyTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class AutoScalingLoadLatencyTest(FTSLatencyTest):

    def check_autoscaling(self):
        if len(self.last_fts_cluster) < len(self.fts_nodes):
            logger.info('*'*10 + "Autoscaling Happened"+'*'*10)
        else:
            logger.info('*'*10 + "Autoscaling Not Happened"+'*'*10)

    def capture_fts_nodes(self):
        self.last_fts_cluster = self.fts_nodes

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.capture_fts_nodes()
        self.warmup()
        self.run_test()
        self.check_autoscaling()
        self.report_kpi()


class FTSLatencyAccessLoadTestWithCustomBucket(FTSLatencyTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.custom_target_iterator(int(self.test_config.jts_access_settings.custom_num_buckets))
        self.warmup()
        self.access_bg()
        self.run_test()
        self.report_kpi()


class FTSLatencyAccessLoadTest(FTSLatencyTest):

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.access_bg()
        self.run_test()
        self.report_kpi()


class FTSLatencysLoadWithStatsTest(FTSLatencyTest):

    @with_stats
    def load(self):
        super().load()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.check_num_items()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSVectorSearchRecallTest(FTSLatencyTest):

    COLLECTORS = {'fts_stats': False, 'jts_stats': False}

    def report_kpi(self, avg_recall, avg_accuracy):
        self.reporter.post(
            *self.metrics.jts_recall_and_accuracy(avg_recall, "Recall",
                                                  self.jts_access.k_nearest_neighbour)
                        )
        self.reporter.post(
            *self.metrics.jts_recall_and_accuracy(avg_accuracy, "Accuracy",
                                                  self.jts_access.k_nearest_neighbour)
                        )

    @with_profiles
    @with_stats
    def calculate_recall(self):
        self.jts_access.fts_master_node = self.fts_master_node
        recallCalculator = VectorRecallCalculator(self.jts_access, self.rest)
        avg_recall, avg_accuracy = recallCalculator.run()
        return avg_recall, avg_accuracy

    def downloads_ground_truth_file(self):
        logger.info("Downloading ground truth files from aws")
        s3_bucket_path = self.jts_access.ground_truth_s3_path
        ground_truth_file_name = self.jts_access.ground_truth_file_name
        logger.info('Downloading {}'.format(ground_truth_file_name))
        run_aws_cli_command(
            f"s3 cp {s3_bucket_path+ground_truth_file_name} {ground_truth_file_name}")

    def run(self):
        self.cleanup_and_restore()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        self.wait_for_index_persistence()
        self.downloads_ground_truth_file()
        avg_recall, avg_accuracy = self.calculate_recall()
        self.report_kpi(avg_recall, avg_accuracy)


class FTSVectorSearchModifiedDataTest(FTSLatencyTest):

    def run(self):
        self.cleanup_and_restore()
        self.access()
        self.create_fts_index_definitions()
        self.create_fts_indexes()
        index_size = self.calculate_index_size()
        size_final = int(index_size / (1024 ** 2))
        logger.info("The index size is {} MB".format(size_final))
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSLatencyTestWithIndexStats(FTSLatencyTest):

    COLLECTORS = {'fts_stats': True}

    def report_index_stats(self, time_elapsed, size):
        self.reporter.post(
            *self.metrics.fts_index_with_latency(time_elapsed)
        )
        self.reporter.post(
            *self.metrics.fts_size_with_latency(size)
        )
        self.cbmonitor_snapshots = []
        self.cbmonitor_clusters = []
        self.COLLECTORS["jts_stats"] = True

    @with_stats
    def build_index(self, fts_nodes):
        elapsed_time = self.create_fts_indexes()
        self.wait_for_index_persistence(fts_nodes)
        return elapsed_time

    def run(self):
        self.cleanup_and_restore()
        self.create_fts_index_definitions()
        fts_nodes = self.add_extra_fts_parameters()
        time_elapsed = self.build_index(fts_nodes)
        size = self.calculate_index_size()
        self.report_index_stats(time_elapsed, size)
        self.download_jts()
        self.wait_for_index_persistence()
        self.warmup()
        self.run_test()
        self.report_kpi()


class FTSLatencyCloudBackupModifiedDataTest(FTSVectorSearchModifiedDataTest):

    def cleanup_and_restore(self):
        return self.cloud_restore()

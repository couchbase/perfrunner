import copy
import os
import shutil
import threading
import time
from collections import deque

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.helpers.profiler import with_profiles
from perfrunner.helpers.worker import (
    WorkloadPhase,
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
        self.fts_index_name_flag = False
        self.access.capella_infrastructure = self.cluster_spec.capella_infrastructure

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
            [WorkloadPhase(jts_run_task, self.target_iterator, self.access)]
        )
        self._download_logs()

    def warmup(self):
        if int(self.access.warmup_query_workers) > 0:
            self.run_phase(
                'jts warmup phase',
                [WorkloadPhase(jts_warmup_task, self.target_iterator, self.access)]
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
        collections_per_index = int(total_num_collections / self.access.index_groups)

        if int(self.access.index_groups) == 1:
            # if index is built on all the collections
            return [collection_list]
        for i in range(self.access.index_groups):
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

        if self.access.couchbase_index_type:
            index_def["params"]["store"]["indexType"] = \
                self.access.couchbase_index_type

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

    def create_fts_index_definitions(self):
        bucket_name = self.test_config.buckets[0]
        result = self.rest.get_bucket_info(self.fts_nodes[0], bucket_name)
        cb_version = result["nodes"][0]["version"].split("-")[0]
        logger.info("This is the version {}".format(cb_version))
        if cb_version >= "7.2.0":
            self.fts_index_name_flag = True
        if cb_version < "7.2.0":
            self.fts_index_name_flag = False

        logger.info("Creating indexing definitions:")
        collection_map = self.test_config.collection.collection_map
        index_id = 0

        # this variable will hold the key valuesfor the custom type mapping
        key_values = []

        if self.access.test_query_mode == 'mixed':
            self.mixed_query_map = dict()
            self.mixed_query_map = read_json(self.access.couchbase_index_configmap)
            self.access.mixed_query_map = self.mixed_query_map

        else:
            general_index_def = read_json(self.access.couchbase_index_configfile)
            general_index_def = self.update_index_def(general_index_def)

            # Geo queries have a slightly different index type with custom type mapping
            if "types" in list(general_index_def["params"]["mapping"].keys()):
                # if any custom type mapping is present
                key_values = list(general_index_def["params"]["mapping"]["types"].keys())

            if self.access.couchbase_index_type:
                general_index_def["params"]["store"]["indexType"] = \
                    self.access.couchbase_index_type

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
                        for index_count in range(0, self.access.indexes_per_group):
                            index_name = "{}-{}".format(self.access.couchbase_index_name, index_id)
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
                for num_indexes in range(0, self.access.indexes_per_group):
                    index_name = "{}-{}".format(self.access.couchbase_index_name, num_indexes)
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
                        "total_docs": int(self.access.test_total_docs)
                    }

                    self.fts_index_defs[index_name] = {
                        "index_def": collection_index_def
                    }

        self.access.fts_index_map = self.fts_index_map

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
            if self.access.index_creation_style == 'async':
                if thread_dict.get(bucket_name, None) is None:
                    thread_dict[bucket_name] = deque()
                thread_dict[bucket_name].append(threading.Thread(
                                                        target=self.create_fts_index_and_wait,
                                                        args=(index_name, index_def)))
            else:
                index_def_list.append([index_name, index_def])
        logger.info('Index map: {}'.format(pretty_dict(self.fts_index_map)))
        t0 = time.time()
        logger.info("Creating indexes {}hronously.".format(self.access.index_creation_style))
        if self.access.index_creation_style == 'async':
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
                self.fts_index_map[index_name]["total_docs"]
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
        if len(self.access.fts_node_level_parameters.keys()) != 0:
            node_level_params = self.access.fts_node_level_parameters
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
        #  if self.access.test_collection_query_mode == "collection_specific":
        #      settings.fts_data_spread_worker_type = "collection_specific"
        settings.seq_upserts = False
        self.run_phase(
            'data spread',
            [WorkloadPhase(spring_task, self.target_iterator, settings)]
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
            setattr(self.access, config, test_config[config])
            logger.info(self.access)

    def report_kpi(self):
        self.reporter.post(*self.metrics.jts_latency(percentile=80))
        self.reporter.post(*self.metrics.jts_latency(percentile=95))

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

    def restore(self):
        self.remote.extract_cb(filename='couchbase.rpm',
                               worker_home=self.worker_manager.WORKER_HOME)
        self.remote.cbbackupmgr_version(worker_home=self.worker_manager.WORKER_HOME)
        if self.cluster_spec.capella_infrastructure:
            backend = self.cluster_spec.capella_backend
        else:
            backend = self.cluster_spec.cloud_provider
        if backend == 'aws':
            credential = local.read_aws_credential(
                self.test_config.backup_settings.aws_credential_path)
            self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()
        self.remote.restore(cluster_spec=self.cluster_spec,
                            master_node=self.master_node,
                            threads=self.test_config.restore_settings.threads,
                            worker_home=self.worker_manager.WORKER_HOME,
                            archive=self.test_config.restore_settings.backup_storage,
                            repo=self.test_config.restore_settings.backup_repo,
                            obj_staging_dir=self.test_config.backup_settings.obj_staging_dir,
                            obj_region=self.test_config.backup_settings.obj_region,
                            obj_access_key_id=self.test_config.backup_settings.obj_access_key_id,
                            use_tls=self.test_config.restore_settings.use_tls,
                            map_data=self.test_config.restore_settings.map_data)
        self.wait_for_persistence()
        if self.test_config.collection.collection_map:
            self.spread_data()

    def run(self):
        self.restore()
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

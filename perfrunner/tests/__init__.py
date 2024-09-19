import copy
import glob
import shutil
import time
import traceback
from multiprocessing import set_start_method
from typing import Callable, Iterable, List, Optional

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cluster import ClusterManager
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.profiler import ProfilerHelper
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import ShowFastReporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.worker import WorkerManager, WorkloadPhase, spring_task
from perfrunner.settings import (
    CBProfile,
    ClusterSpec,
    PhaseSettings,
    TargetIterator,
    TestConfig,
)

try:
    set_start_method("fork")
except Exception as ex:
    print(ex)


class PerfTest:

    COLLECTORS = {}

    ROOT_CERTIFICATE = 'root.pem'

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.dynamic_infra = self.cluster_spec.dynamic_infrastructure
        self.cloud_infra = self.cluster_spec.cloud_infrastructure
        self.capella_infra = self.cluster_spec.capella_infrastructure
        self.target_iterator = TargetIterator(cluster_spec, test_config)
        self.cluster = ClusterManager(cluster_spec, test_config)
        self.remote = RemoteHelper(cluster_spec, verbose)
        self.profiler = ProfilerHelper(cluster_spec, test_config)
        self.master_node = next(cluster_spec.masters)
        self.memcached = MemcachedHelper(cluster_spec, test_config)
        self.rest = RestHelper(cluster_spec, test_config)
        self.build = self.rest.get_version(self.master_node)
        self.is_columnar = self.rest.is_columnar(self.master_node)
        self.monitor = Monitor(cluster_spec, test_config, self.rest, self.remote, self.build)
        self.metrics = MetricHelper(self)
        self.reporter = ShowFastReporter(cluster_spec, test_config, self.build)

        self.cbmonitor_snapshots = []
        self.cbmonitor_clusters = []

        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config, verbose)

        ssl_enabled_modes = ('data', 'n2n', 'capella', 'nebula', 'dapi')
        need_certificate = (
            self.test_config.cluster.enable_n2n_encryption or
            self.test_config.load_settings.ssl_mode in ssl_enabled_modes or
            self.test_config.access_settings.ssl_mode in ssl_enabled_modes
        )
        if need_certificate:
            self.download_certificate()
            if self.worker_manager.is_remote or self.cluster_spec.cloud_infrastructure:
                self.remote.cloud_put_certificate(self.ROOT_CERTIFICATE,
                                                  self.worker_manager.WORKER_HOME)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or exc_val or exc_tb:
            logger.info(f"{exc_type=}")
            logger.info(f"{exc_val=}")
            logger.info(f"traceback: {''.join(traceback.format_tb(exc_tb))}")

        failure = self.debug()

        self.tear_down()

        if isinstance(exc_type, KeyboardInterrupt):
            logger.warn('The test was interrupted')
            return True

        if failure:
            logger.interrupt(failure)

    @property
    def data_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.master_node, 'kv')

    @property
    def query_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.master_node, 'n1ql')

    @property
    def index_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.master_node, 'index')

    @property
    def fts_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.master_node, 'fts')

    @property
    def analytics_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.master_node, 'cbas')

    @property
    def eventing_nodes(self) -> List[str]:
        return self.rest.get_active_nodes_by_role(self.master_node, 'eventing')

    def tear_down(self):
        if self.test_config.profiling_settings.linux_perf_profile_flag:
            self.collect_linux_perf_profiles()

        if self.test_config.test_case.use_workers:
            self.worker_manager.download_celery_logs()
            self.worker_manager.terminate()

        if self.test_config.cluster.online_cores:
            self.remote.enable_cpu()

        if self.test_config.cluster.kernel_mem_limit:
            self.collect_logs()

            self.reset_memory_settings()

        if self.test_config.cluster.serverless_mode and not self.cloud_infra:
            self.remote.set_cb_profile(CBProfile.DEFAULT)

    def collect_linux_perf_profiles(self):
        self.remote.generate_linux_perf_script()
        self.remote.get_linuxperf_files()

    def collect_logs(self):
        self.remote.collect_info(
            timeout=self.test_config.access_settings.cbcollect_timeout,
            task_regexp=self.test_config.access_settings.cbcollect_regexp,
        )
        for hostname in self.cluster_spec.servers:
            for fname in glob.glob('{}/*.zip'.format(hostname)):
                shutil.move(fname, '{}.zip'.format(hostname))

    def reset_memory_settings(self):
        if self.capella_infra or self.dynamic_infra:
            return

        if self.test_config.cluster.kernel_mem_limit:
            for service in self.test_config.cluster.kernel_mem_limit_services:
                for server in self.cluster_spec.servers_by_role(service):
                    self.remote.reset_memory_settings(host_string=server)
            self.monitor.wait_for_servers()

    def debug(self) -> str:
        failure = self.check_core_dumps()
        failure = self.check_rebalance() or failure
        return self.check_failover() or failure

    def download_certificate(self):
        cert = self.rest.get_certificate(self.master_node)
        with open(self.ROOT_CERTIFICATE, 'w') as fh:
            fh.write(cert)

    def check_rebalance(self) -> str:
        if self.dynamic_infra:
            return

        for master in self.cluster_spec.masters:
            if not self.rest.is_balanced(master):
                return "The cluster is not balanced"

    def check_failover(self) -> Optional[str]:
        if self.dynamic_infra:
            return

        if hasattr(self, 'rebalance_settings'):
            if self.rebalance_settings.failover or \
                    self.rebalance_settings.graceful_failover:
                return

        for master in self.cluster_spec.masters:
            num_failovers = self.rest.get_failover_counter(master)
            if num_failovers:
                return 'Failover happened {} time(s)'.format(num_failovers)

    def check_core_dumps(self) -> str:
        if self.capella_infra:
            return ''
        dumps_per_host = self.remote.detect_core_dumps()
        core_dumps = {
            host: dumps for host, dumps in dumps_per_host.items() if dumps
        }
        if core_dumps:
            return pretty_dict(core_dumps)

    def restore(self):
        logger.info('Restoring data')
        self.remote.purge_restore_progress(
            self.test_config.restore_settings.backup_storage,
            self.test_config.restore_settings.backup_repo
        )
        self.remote.restore_data(
            self.test_config.restore_settings.backup_storage,
            self.test_config.restore_settings.backup_repo,
        )

    def fts_collections_restore(self):
        restore_mapping = None
        collection_map = self.test_config.collection.collection_map
        for target in self.target_iterator:
            if not collection_map.get(
                    target.bucket, {}).get("_default", {}).get("_default", {}).get('load', 0):
                restore_mapping = \
                    "{0}._default._default={0}.scope-1.collection-1"\
                    .format(target.bucket)
            logger.info('Restoring data')
            self.remote.purge_restore_progress(
                self.test_config.restore_settings.backup_storage,
                self.test_config.restore_settings.backup_repo
            )
            self.remote.restore_data(
                self.test_config.restore_settings.backup_storage,
                self.test_config.restore_settings.backup_repo,
                map_data=restore_mapping
            )

    def fts_cbimport(self):
        logger.info('Restoring data into collections')
        num_collections = self.test_config.jts_access_settings.collections_number
        scope_prefix = self.test_config.jts_access_settings.scope_prefix
        collection_prefix = self.test_config.jts_access_settings.collection_prefix
        scope = self.test_config.jts_access_settings.scope_number
        name_of_backup = self.test_config.restore_settings.backup_repo
        self.remote.export_data(num_collections, collection_prefix, scope_prefix,
                                scope, name_of_backup)

    def restore_local(self):
        logger.info('Restoring data')
        local.extract_cb_any(filename='couchbase')
        local.purge_restore_progress(
            self.cluster_spec,
            archive=self.test_config.restore_settings.backup_storage,
            repo=self.test_config.restore_settings.backup_repo
        )
        local.cbbackupmgr_restore(
            master_node=self.master_node,
            cluster_spec=self.cluster_spec,
            threads=self.test_config.restore_settings.threads,
            archive=self.test_config.restore_settings.backup_storage,
            repo=self.test_config.restore_settings.backup_repo,
            include_data=self.test_config.backup_settings.include_data,
            map_data=self.test_config.restore_settings.map_data,
            use_tls=self.test_config.restore_settings.use_tls
        )

    def load_tpcds_json_data(self):
        logger.info('Importing data')
        if self.test_config.collection.collection_map is not None:
            cm = self.test_config.collection.collection_map
            for bucket in self.test_config.buckets:
                for scope in cm[bucket]:
                    for collection in cm[bucket][scope]:
                        if cm[bucket][scope][collection]['load'] == 1:
                            self.remote.load_tpcds_data_json_collection(
                                self.test_config.import_settings.import_file,
                                bucket,
                                scope,
                                collection,
                                self.test_config.import_settings.docs_per_collections
                            )
        else:
            for bucket in self.test_config.buckets:
                self.remote.load_tpcds_data_json(
                    self.test_config.import_settings.import_file, bucket,
                )

    def compact_bucket(self, wait: bool = True):
        if not self.cluster_spec.capella_infrastructure:
            for target in self.target_iterator:
                self.rest.trigger_bucket_compaction(target.node, target.bucket)

            if wait:
                for target in self.target_iterator:
                    self.monitor.monitor_task(target.node, 'bucket_compaction')

    def wait_for_persistence(self):
        for target in self.target_iterator:
            self.monitor.monitor_disk_queues(target.node, target.bucket)
            self.monitor.monitor_dcp_queues(target.node, target.bucket)
            self.monitor.monitor_replica_count(target.node, target.bucket)

    def wait_for_indexing(self, index_nodes: List[str] = []):
        index_nodes = index_nodes or self.index_nodes
        if self.test_config.index_settings.statements:
            for server in index_nodes:
                self.monitor.monitor_indexing(server)

    def check_num_items(self, bucket_items: dict = None, max_retry: int = None,
                        target_iterator: TargetIterator = None):
        target_iterator = target_iterator or self.target_iterator
        if bucket_items:
            for target in target_iterator:
                num_items = bucket_items.get(target.bucket, None)
                if num_items:
                    num_items = num_items * (1 + self.test_config.bucket.replica_number)
                    self.monitor.monitor_num_items(
                        target.node,
                        target.bucket,
                        num_items,
                        max_retry=max_retry
                    )
        elif getattr(self.test_config.load_settings, 'collections', None):
            for target in target_iterator:
                total_load_distribution_ratio = 0
                target_scope_collections = self.test_config.load_settings.collections[target.bucket]
                for scope in target_scope_collections.keys():
                    for options in target_scope_collections[scope].values():
                        if options['load'] == 1:
                            total_load_distribution_ratio += options.get('ratio', 1)
                            # assuming if ratio is not specified default ratio is 1:1
                num_items = \
                    (self.test_config.load_settings.items // total_load_distribution_ratio) * \
                    total_load_distribution_ratio * \
                    (1 + self.test_config.bucket.replica_number)
                self.monitor.monitor_num_items(target.node, target.bucket,
                                               num_items, max_retry=max_retry)
        else:
            num_items = self.test_config.load_settings.items * (
                1 + self.test_config.bucket.replica_number
            )
            for target in target_iterator:
                self.monitor.monitor_num_items(target.node, target.bucket,
                                               num_items, max_retry=max_retry)

    def reset_kv_stats(self):
        master_node = next(self.cluster_spec.masters)
        if self.test_config.cluster.enable_n2n_encryption:
            uname, pwd = self.cluster_spec.rest_credentials
            local.run_cbstats("reset", self.master_node, 11210, uname, pwd)
        else:
            for bucket in self.test_config.buckets:
                for server in self.rest.get_server_list(master_node, bucket):
                    port = self.rest.get_memcached_port(server)
                    self.memcached.reset_stats(server, port, bucket)

    def create_indexes(self, query_node: Optional[str] = None):
        logger.info('Creating and building indexes')
        query_node = query_node or self.query_nodes[0]

        if not self.test_config.index_settings.couchbase_fts_index_name:
            create_statements = []
            build_statements = []

            for statement in self.test_config.index_settings.statements:
                check_stmt = statement.replace(" ", "").upper()
                if 'CREATEINDEX' in check_stmt \
                        or 'CREATEPRIMARYINDEX' in check_stmt:
                    create_statements.append(statement)
                elif 'BUILDINDEX' in check_stmt:
                    build_statements.append(statement)

            for statement in create_statements:
                logger.info('Creating index: ' + statement)
                self.rest.exec_n1ql_statement(query_node, statement)
                cont = False
                while not cont:
                    building = 0
                    index_status = self.rest.get_index_status(self.index_nodes[0])
                    index_list = index_status['status']
                    for index in index_list:
                        if index['status'] != "Ready" and index['status'] != "Created":
                            building += 1
                    if building < 10:
                        cont = True
                    else:
                        time.sleep(10)

            for statement in build_statements:
                logger.info('Building index: ' + statement)
                self.rest.exec_n1ql_statement(query_node, statement)
                cont = False
                while not cont:
                    building = 0
                    index_status = self.rest.get_index_status(self.index_nodes[0])
                    index_list = index_status['status']
                    for index in index_list:
                        if index['status'] != "Ready" and index['status'] != "Created":
                            building += 1
                    if building < 10:
                        cont = True
                    else:
                        time.sleep(10)

            logger.info('Index Create and Build Complete')
        else:
            self.create_fts_index_n1ql()

    def create_fts_index_n1ql(self):
        logger.info("Creating FTS index")
        definition = read_json(self.test_config.index_settings.couchbase_fts_index_configfile)
        bucket_name = self.test_config.buckets[0]
        definition.update({
            'name': self.test_config.index_settings.couchbase_fts_index_name
        })
        if self.test_config.collection.collection_map:
            collection_map = self.test_config.collection.collection_map
            definition["params"]["doc_config"]["mode"] = "scope.collection.type_field"
            scope_name = list(collection_map[bucket_name].keys())[1:][0]
            collection_name = list(collection_map[bucket_name][scope_name].keys())[0]
            ind_type_mapping = \
                copy.deepcopy(definition["params"]["mapping"]["default_mapping"])
            definition["params"]["mapping"]["default_mapping"]["enabled"] = False
            new_type_mapping_name = "{}.{}".format(scope_name, collection_name)
            definition["params"]["mapping"]["types"] = {new_type_mapping_name: ind_type_mapping}

        logger.info('Index definition: {}'.format(pretty_dict(definition)))
        self.rest.create_fts_index(
            self.fts_nodes[0],
            self.test_config.index_settings.couchbase_fts_index_name, definition)
        self.monitor.monitor_fts_indexing_queue(
            self.fts_nodes[0],
            self.test_config.index_settings.couchbase_fts_index_name,
            int(self.test_config.access_settings.items))

    def create_functions(self):
        logger.info('Creating n1ql functions')

        for statement in self.test_config.n1ql_function_settings.statements:
            self.rest.exec_n1ql_statement(self.query_nodes[0], statement)

    def sleep(self):
        access_settings = self.test_config.access_settings
        logger.info('Running phase for {} seconds'.format(access_settings.time))
        time.sleep(access_settings.time)

    def log_task_settings(self, phases: Iterable[WorkloadPhase]):
        if len(phases) > 1:
            common_task_settings, diff_task_settings = PhaseSettings.compare_phase_settings(
                [ph.task_settings for ph in phases]
            )

            # Print out all the settings which are the same for each task
            logger.info('Settings common to each task: {}'
                        .format(pretty_dict(common_task_settings)))

            # For each task, print out the settings which are different
            logger.info('Individual task settings: \n{}'.format(
                '\n'.join(pretty_dict(d) for d in diff_task_settings)
            ))
        else:
            logger.info('Task settings: {}'.format(pretty_dict(phases[0].task_settings)))

    def generic_phase(self,
                      phase_name: str,
                      default_settings: PhaseSettings,
                      default_mixed_settings: Iterable[PhaseSettings],
                      task: Callable = spring_task,
                      settings: PhaseSettings = None,
                      target_iterator: Iterable = None,
                      mixed_tasks: Iterable[Callable] = None,
                      mixed_settings: Iterable[PhaseSettings] = None,
                      mixed_target_iterators: Iterable[Iterable] = None,
                      use_timers: bool = False) -> List[WorkloadPhase]:
        if settings is None:
            settings = default_settings

        phases = []

        if not settings.workload_mix and not mixed_settings:
            # Do non-mixed workload
            if target_iterator is None:
                target_iterator = self.target_iterator

            timer = settings.time if use_timers else None

            phases = [WorkloadPhase(task, target_iterator, settings, timer=timer)]
        else:
            # Do mixed workload
            if mixed_settings is None:
                mixed_settings = default_mixed_settings

            if mixed_target_iterators is None:
                mixed_target_iterators = [
                    TargetIterator(self.cluster_spec, self.test_config,
                                   buckets=task_settings.bucket_list)
                    for task_settings in mixed_settings
                ]

            if mixed_tasks is None:
                mixed_tasks = [task for _ in mixed_settings]

            phases = [
                WorkloadPhase(task, t_iter, task_settings,
                              timer=task_settings.time if use_timers else None)
                for task, t_iter, task_settings in zip(
                    mixed_tasks, mixed_target_iterators, mixed_settings
                )
            ]

        logger.info('Running {} phase'.format(phase_name))
        self.log_task_settings(phases)

        return phases

    def load(self,
             task: Callable = spring_task,
             settings: PhaseSettings = None,
             target_iterator: Iterable = None,
             mixed_tasks: Iterable[Callable] = None,
             mixed_settings: Iterable[PhaseSettings] = None,
             mixed_target_iterators: Iterable[Iterable] = None):
        phases = self.generic_phase(
            phase_name='load',
            default_settings=self.test_config.load_settings,
            default_mixed_settings=self.test_config.mixed_load_settings,
            task=task,
            settings=settings,
            target_iterator=target_iterator,
            mixed_tasks=mixed_tasks,
            mixed_settings=mixed_settings,
            mixed_target_iterators=mixed_target_iterators
        )
        self.worker_manager.run_fg_phases(phases)

    def load_bg(self,
                task: Callable = spring_task,
                settings: PhaseSettings = None,
                target_iterator: Iterable = None,
                mixed_tasks: Iterable[Callable] = None,
                mixed_settings: Iterable[PhaseSettings] = None,
                mixed_target_iterators: Iterable[Iterable] = None):
        phases = self.generic_phase(
            phase_name='background load',
            default_settings=self.test_config.load_settings,
            default_mixed_settings=self.test_config.mixed_load_settings,
            task=task,
            settings=settings,
            target_iterator=target_iterator,
            mixed_tasks=mixed_tasks,
            mixed_settings=mixed_settings,
            mixed_target_iterators=mixed_target_iterators,
        )
        self.worker_manager.run_bg_phases(phases)

    def hot_load(self,
                 task: Callable = spring_task,
                 settings: PhaseSettings = None,
                 target_iterator: Iterable = None,
                 mixed_tasks: Iterable[Callable] = None,
                 mixed_settings: Iterable[PhaseSettings] = None,
                 mixed_target_iterators: Iterable[Iterable] = None):
        phases = self.generic_phase(
            phase_name='hot load',
            default_settings=self.test_config.hot_load_settings,
            default_mixed_settings=self.test_config.mixed_hot_load_settings,
            task=task,
            settings=settings,
            target_iterator=target_iterator,
            mixed_tasks=mixed_tasks,
            mixed_settings=mixed_settings,
            mixed_target_iterators=mixed_target_iterators
        )
        self.worker_manager.run_fg_phases(phases)

    def xattr_load(self,
                   task: Callable = spring_task,
                   target_iterator: Iterable = None,
                   mixed_tasks: Iterable[Callable] = None,
                   mixed_target_iterators: Iterable[Iterable] = None):
        phases = self.generic_phase(
            phase_name='xattr load',
            default_settings=self.test_config.xattr_load_settings,
            default_mixed_settings=self.test_config.mixed_xattr_load_settings,
            task=task,
            settings=None,
            target_iterator=target_iterator,
            mixed_tasks=mixed_tasks,
            mixed_settings=None,
            mixed_target_iterators=mixed_target_iterators
        )
        self.worker_manager.run_fg_phases(phases)

    def access(self,
               task: Callable = spring_task,
               settings: PhaseSettings = None,
               target_iterator: Iterable = None,
               mixed_tasks: Iterable[Callable] = None,
               mixed_settings: Iterable[PhaseSettings] = None,
               mixed_target_iterators: Iterable[Iterable] = None):
        phases = self.generic_phase(
            phase_name='access',
            default_settings=self.test_config.access_settings,
            default_mixed_settings=self.test_config.mixed_access_settings,
            task=task,
            settings=settings,
            target_iterator=target_iterator,
            mixed_tasks=mixed_tasks,
            mixed_settings=mixed_settings,
            mixed_target_iterators=mixed_target_iterators,
            use_timers=True
        )
        self.worker_manager.run_fg_phases(phases)

    def access_bg(self,
                  task: Callable = spring_task,
                  settings: PhaseSettings = None,
                  target_iterator: Iterable = None,
                  mixed_tasks: Iterable[Callable] = None,
                  mixed_settings: Iterable[PhaseSettings] = None,
                  mixed_target_iterators: Iterable[Iterable] = None):
        phases = self.generic_phase(
            phase_name='background access',
            default_settings=self.test_config.access_settings,
            default_mixed_settings=self.test_config.mixed_access_settings,
            task=task,
            settings=settings,
            target_iterator=target_iterator,
            mixed_tasks=mixed_tasks,
            mixed_settings=mixed_settings,
            mixed_target_iterators=mixed_target_iterators,
            use_timers=True
        )
        self.worker_manager.run_bg_phases(phases)

    def report_kpi(self, *args, **kwargs):
        if self.test_config.stats_settings.enabled:
            self._report_kpi(*args, **kwargs)

    def _report_kpi(self, *args, **kwargs):
        pass

    def _measure_curr_ops(self) -> int:
        if self.cluster_spec.serverless_infrastructure:
            return self._measure_curr_ops_rest()
        return self._measure_curr_ops_mc()

    def _measure_curr_ops_rest(self) -> int:
        ops = 0
        samples = self.rest.get_kv_ops(self.master_node)
        for data in samples['data']:
            if data['metric']['op'] in ['get', 'set']:
                ops += int(data['values'][-1][-1])
        return ops

    def _measure_curr_ops_mc(self) -> int:
        ops = 0
        for bucket in self.test_config.buckets:
            for server in self.rest.get_active_nodes_by_role(self.master_node, "kv"):
                port = self.rest.get_memcached_port(server)

                stats = self.memcached.get_stats(server, port, bucket)
                for stat in 'cmd_get', 'cmd_set':
                    ops += int(stats[stat])
        return ops

    def _measure_billable_units(self) -> int:
        bus = 0
        samples = self.rest.get_metering_stats(self.master_node)
        for data in samples:
            for metric in data['data']:
                bus += int(metric['values'][-1][-1])
        return bus

    def _measure_disk_ops(self):
        ret_stats = dict()
        for bucket in self.test_config.buckets:
            for server in self.rest.get_active_nodes_by_role(self.master_node, "kv"):
                ret_stats[server] = dict()
                port = self.rest.get_memcached_port(server)

                stats = self.memcached.get_stats(server, port, bucket)
                ret_stats[server]["get_ops"] = int(stats['ep_bg_fetched'])
                sets = \
                    int(stats['vb_active_ops_create']) \
                    + int(stats['vb_replica_ops_create']) \
                    + int(stats['vb_pending_ops_create']) \
                    + int(stats['vb_active_ops_update']) \
                    + int(stats['vb_replica_ops_update']) \
                    + int(stats['vb_pending_ops_update'])
                ret_stats[server]["set_ops"] = sets
        return ret_stats

    def get_rebalance_timings(self):
        """Get times in seconds for the last successful rebalance/failover task."""
        logger.info("getting rebalance report")
        rebalance_report = self.rest.get_rebalance_report(self.master_node)
        time_map = dict()
        time_map["total_time"] = rebalance_report["timeTaken"]/1000
        stage_info = rebalance_report["stageInfo"]
        for service, progress in stage_info.items():
            time_map[service] = progress["timeTaken"] / 1000
        logger.info("rebalance timing(s): {}".format(pretty_dict(time_map)))
        return time_map

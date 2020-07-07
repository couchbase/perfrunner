import glob
import shutil
import time
from typing import Callable, Iterable, List, Optional

from logger import logger

from perfrunner.helpers import local
from perfrunner.helpers.cluster import ClusterManager
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import pretty_dict, read_json
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.profiler import Profiler
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import ShowFastReporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.worker import spring_task, WorkerManager
from perfrunner.settings import (
    ClusterSpec,
    PhaseSettings,
    TargetIterator,
    TestConfig,
)


class PerfTest:

    COLLECTORS = {}

    ROOT_CERTIFICATE = 'root.pem'

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.target_iterator = TargetIterator(cluster_spec, test_config)

        self.cluster = ClusterManager(cluster_spec, test_config)
        self.memcached = MemcachedHelper(test_config)
        self.monitor = Monitor(cluster_spec, test_config, verbose)
        self.rest = RestHelper(cluster_spec)
        self.remote = RemoteHelper(cluster_spec, verbose)
        self.profiler = Profiler(cluster_spec, test_config)

        self.master_node = next(cluster_spec.masters)
        self.build = self.rest.get_version(self.master_node)

        self.metrics = MetricHelper(self)
        self.reporter = ShowFastReporter(cluster_spec, test_config, self.build)

        self.cbmonitor_snapshots = []
        self.cbmonitor_clusters = []

        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config,
                                                verbose)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        failure = self.debug()

        self.tear_down()

        if exc_type == KeyboardInterrupt:
            logger.warn('The test was interrupted')
            return True

        if failure:
            logger.interrupt(failure)

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
        if self.test_config.test_case.use_workers:
            self.worker_manager.download_celery_logs()
            self.worker_manager.terminate()

        if self.test_config.cluster.online_cores:
            self.remote.enable_cpu()

        if self.test_config.cluster.kernel_mem_limit:
            self.collect_logs()

            self.cluster.reset_memory_settings()

    def collect_logs(self):
        self.remote.collect_info()

        for hostname in self.cluster_spec.servers:
            for fname in glob.glob('{}/*.zip'.format(hostname)):
                shutil.move(fname, '{}.zip'.format(hostname))

    def reset_memory_settings(self):
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
        for master in self.cluster_spec.masters:
            if self.rest.is_not_balanced(master):
                return 'The cluster is not balanced'

    def check_failover(self) -> Optional[str]:
        if hasattr(self, 'rebalance_settings'):
            if self.rebalance_settings.failover or \
                    self.rebalance_settings.graceful_failover:
                return

        for master in self.cluster_spec.masters:
            num_failovers = self.rest.get_failover_counter(master)
            if num_failovers:
                return 'Failover happened {} time(s)'.format(num_failovers)

    def check_core_dumps(self) -> str:
        dumps_per_host = self.remote.detect_core_dumps()
        core_dumps = {
            host: dumps for host, dumps in dumps_per_host.items() if dumps
        }
        if core_dumps:
            return pretty_dict(core_dumps)

    def restore(self):
        logger.info('Restoring data')
        self.remote.restore_data(
            self.test_config.restore_settings.backup_storage,
            self.test_config.restore_settings.backup_repo,
        )

    def restore_local(self):
        logger.info('Restoring data')
        local.extract_cb(filename='couchbase.rpm')
        local.cbbackupmgr_restore(
            master_node=self.master_node,
            cluster_spec=self.cluster_spec,
            threads=self.test_config.restore_settings.threads,
            archive=self.test_config.restore_settings.backup_storage,
            repo=self.test_config.restore_settings.backup_repo,
        )

    def import_data(self):
        logger.info('Importing data')
        for bucket in self.test_config.buckets:
            self.remote.import_data(
                self.test_config.restore_settings.import_file, bucket,
            )

    def compact_bucket(self, wait: bool = True):
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

    def wait_for_indexing(self):
        if self.test_config.index_settings.statements:
            for server in self.index_nodes:
                self.monitor.monitor_indexing(server)

    def check_num_items(self):
        if getattr(self.test_config.load_settings, 'collections', None):
            for target in self.target_iterator:
                num_load_targets = 0
                target_scope_collections = self.test_config.load_settings.collections[target.bucket]
                for scope in target_scope_collections.keys():
                    for collection in target_scope_collections[scope].keys():
                        if target_scope_collections[scope][collection]['load'] == 1:
                            num_load_targets += 1
                num_items = \
                    (self.test_config.load_settings.items // num_load_targets) * \
                    num_load_targets * \
                    (1 + self.test_config.bucket.replica_number)
                self.monitor.monitor_num_items(target.node, target.bucket,
                                               num_items)
        else:
            num_items = self.test_config.load_settings.items * (
                1 + self.test_config.bucket.replica_number
            )
            for target in self.target_iterator:
                self.monitor.monitor_num_items(target.node, target.bucket,
                                               num_items)

    def reset_kv_stats(self):
        master_node = next(self.cluster_spec.masters)
        for bucket in self.test_config.buckets:
            for server in self.rest.get_server_list(master_node, bucket):
                port = self.rest.get_memcached_port(server)
                self.memcached.reset_stats(server, port, bucket)

    def create_indexes(self):
        logger.info('Creating and building indexes')
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
                self.rest.exec_n1ql_statement(self.query_nodes[0], statement)

            for statement in build_statements:
                logger.info('Building index: ' + statement)
                self.rest.exec_n1ql_statement(self.query_nodes[0], statement)

            logger.info('Index Create and Build Complete')
        else:
            self.create_fts_index_n1ql()

    def create_fts_index_n1ql(self):
        definition = read_json(self.test_config.index_settings.couchbase_fts_index_configfile)
        definition.update({
            'name': self.test_config.index_settings.couchbase_fts_index_name
        })
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

    def run_phase(self,
                  phase: str,
                  task: Callable, settings: PhaseSettings,
                  target_iterator: Iterable,
                  timer: int = None,
                  wait: bool = True):
        logger.info('Running {}: {}'.format(phase, pretty_dict(settings)))
        self.worker_manager.run_tasks(task, settings, target_iterator, timer)
        if wait:
            self.worker_manager.wait_for_workers()

    def load(self,
             task: Callable = spring_task,
             settings: PhaseSettings = None,
             target_iterator: Iterable = None):
        if settings is None:
            settings = self.test_config.load_settings
        if target_iterator is None:
            target_iterator = self.target_iterator

        self.run_phase('load phase',
                       task, settings, target_iterator)

    def hot_load(self,
                 task: Callable = spring_task):
        settings = self.test_config.hot_load_settings

        self.run_phase('hot load phase',
                       task, settings, self.target_iterator)

    def xattr_load(self,
                   task: Callable = spring_task,
                   target_iterator: Iterable = None):
        if target_iterator is None:
            target_iterator = self.target_iterator
        settings = self.test_config.xattr_load_settings

        self.run_phase('xattr phase',
                       task, settings, target_iterator)

    def access(self,
               task: Callable = spring_task,
               settings: PhaseSettings = None,
               target_iterator: Iterable = None):
        if settings is None:
            settings = self.test_config.access_settings
        if target_iterator is None:
            target_iterator = self.target_iterator

        self.run_phase('access phase',
                       task, settings, target_iterator,
                       timer=settings.time)

    def access_bg(self,
                  task: Callable = spring_task,
                  settings: PhaseSettings = None,
                  target_iterator: Iterable = None):
        if settings is None:
            settings = self.test_config.access_settings
        if target_iterator is None:
            target_iterator = self.target_iterator

        self.run_phase('background access phase',
                       task, settings, target_iterator,
                       timer=settings.time, wait=False)

    def report_kpi(self, *args, **kwargs):
        if self.test_config.stats_settings.enabled:
            self._report_kpi(*args, **kwargs)

    def _report_kpi(self, *args, **kwargs):
        pass

    def _measure_curr_ops(self) -> int:
        ops = 0
        for bucket in self.test_config.buckets:
            for server in self.rest.get_active_nodes_by_role(self.master_node, "kv"):
                port = self.rest.get_memcached_port(server)

                stats = self.memcached.get_stats(server, port, bucket)
                for stat in 'cmd_get', 'cmd_set':
                    ops += int(stats[stat])
        return ops

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

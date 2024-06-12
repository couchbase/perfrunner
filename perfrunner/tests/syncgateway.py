import copy
import glob
import json
import multiprocessing
import os
import re
import shutil
from multiprocessing import Pool
from time import sleep, time
from typing import Callable, Iterable

from decorator import decorator

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.cluster import ClusterManager
from perfrunner.helpers.config_files import TimeTrackingFile
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import parse_prometheus_stat, pretty_dict, target_hash
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.profiler import ProfilerHelper, with_profiles
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import ShowFastReporter
from perfrunner.helpers.rest import SGW_ADMIN_PORT, SGW_PUBLIC_PORT, RestHelper
from perfrunner.helpers.worker import (
    WorkerManager,
    WorkloadPhase,
    pillowfight_data_load_task,
    syncgateway_bh_puller_task,
    syncgateway_delta_sync_task_load_docs,
    syncgateway_delta_sync_task_run_test,
    syncgateway_e2e_cbl_task_load_docs,
    syncgateway_e2e_cbl_task_run_test,
    syncgateway_e2e_multi_cbl_task_load_docs,
    syncgateway_e2e_multi_cbl_task_run_test,
    syncgateway_new_docpush_task,
    syncgateway_task_grant_access,
    syncgateway_task_init_users,
    syncgateway_task_load_docs,
    syncgateway_task_load_users,
    syncgateway_task_run_test,
    syncgateway_task_start_memcached,
    ycsb_data_load_task,
    ycsb_task,
)
from perfrunner.settings import (
    ClusterSpec,
    PhaseSettings,
    TargetIterator,
    TargetSettings,
    TestConfig,
)
from perfrunner.tests import PerfTest
from perfrunner.tests.eventing import FunctionsTimeTest


@decorator
def with_timer(cblite_replicate, *args, **kwargs):
    test = args[0]

    t0 = time.time()

    cblite_replicate(*args, **kwargs)

    test.replicate_time = time.time() - t0  # Delta Sync time in seconds


class SGPerfTest(PerfTest):

    COLLECTORS = {'disk': False, 'ns_server': False, 'ns_server_overview': False,
                  'active_tasks': False, 'syncgateway_stats': True}

    ALL_HOSTNAMES = True
    LOCAL_DIR = "YCSB"
    CBLITE_LOG_DIR = "cblite_logs"

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.dynamic_infra = self.cluster_spec.dynamic_infrastructure
        self.capella_infra = self.cluster_spec.capella_infrastructure
        self.cloud_infra = self.cluster_spec.cloud_infrastructure
        self.memcached = MemcachedHelper(cluster_spec, test_config)
        self.remote = RemoteHelper(cluster_spec, verbose)
        self.rest = RestHelper(cluster_spec, bool(test_config.cluster.enable_n2n_encryption))
        self.master_node = next(self.cluster_spec.masters)
        self.sgw_master_node = next(cluster_spec.sgw_masters)
        self.metrics = MetricHelper(self)
        if self.capella_infra:
            self.build = f'{self.rest.get_sgversion(self.sgw_master_node)}:' \
                         f'{self.rest.get_version(self.master_node)}'
        else:
            self.build = self.rest.get_sgversion(self.sgw_master_node)
        self.reporter = ShowFastReporter(cluster_spec, test_config, self.build, sgw=True)
        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config, verbose)
        self.settings = self.test_config.access_settings
        self.settings.syncgateway_settings = self.test_config.syncgateway_settings
        self.profiler = ProfilerHelper(cluster_spec, test_config)
        self.cluster = ClusterManager(cluster_spec, test_config)
        self.target_iterator = TargetIterator(cluster_spec, test_config)
        self.monitor = Monitor(
            cluster_spec,
            self.rest,
            self.remote,
            self.rest.get_version(self.master_node),
            test_config.cluster.query_awr_bucket,
            test_config.cluster.query_awr_scope,
        )
        self.sg_settings = self.test_config.syncgateway_settings
        self.collections = self.test_config.collection.collection_map
        if self.test_config.collection.collection_map:
            self.collection_map = self.test_config.collection.collection_map
        else:
            self.collection_map = {}

        # If we dont move to headless service for syncgateway, maybe move this to
        # rest helper in the future
        self.admin_port = SGW_ADMIN_PORT
        self.public_port = SGW_PUBLIC_PORT
        self.memcached_port = "8000"
        if self.dynamic_infra:
            _, self.admin_port = self.rest.translate_host_and_port(
                self.sgw_master_node, SGW_ADMIN_PORT
            )
            _, self.public_port = self.rest.translate_host_and_port(
                self.sgw_master_node, SGW_PUBLIC_PORT
            )
            self.memcached_port = "11211"

        self.use_distributed_load = int(self.sg_settings.load_clients) > 1

        if collector := self.settings.syncgateway_settings.log_streaming:
            self.enable_log_streaming(collector)
        if self.test_config.sgw_audit_settings.enabled:
            self.enable_audit_logging()

    def enable_log_streaming(self, collector: str):

        if not self.capella_infra:
            return

        # Enable and set config for log streaming
        config = {
            'output_type': collector,
            'url': self._get_collector_url(collector)
        }
        if collector != 'generic_http':
            config.update(
                {'api_key': self._get_collector_api_key()}
            )

        # Initialise and enable log streaming with the provided config
        self.rest.create_or_override_log_streaming_config(config)

        # Get the log streaming options
        options = self.rest.get_log_streaming_options()
        logger.info(f'The log streaming options are: {options}')

        # Get logging options
        log_options = self.rest.get_logging_options()
        logger.info(f'The logging options are: {log_options}')

        # Get logging config for the db
        log_config = self.rest.get_logging_config()
        logger.info(f'The deployed logging config for this db is: {log_config}')

    def disable_log_streaming(self):

        if not self.capella_infra:
            return

        logger.info('Disabling log-streaming')
        self.rest.disable_log_streaming()

    def _get_collector_url(self, collector: str) -> str:
        return os.getenv(f'COLLECTOR_URL_{collector.upper()}')

    def _get_collector_api_key(self, collector: str) -> str:
        return os.getenv(f'COLLECTOR_API_KEY_{collector.upper()}')

    def _get_audit_settings(self) -> dict:
        # Get all available audit settings and configure those filterable ones

        current_events = self.rest.sgw_get_audit_settings(self.sgw_master_node, "db-1").get(
            "events", {}
        )

        if extra_events := self.test_config.sgw_audit_settings.extra_events:
            if extra_events == "ALL":
                return dict.fromkeys(current_events.keys(), True)
            for event, value in current_events:
                current_events[event] = (event in extra_events) or value
        return current_events

    def enable_audit_logging(self):
        if not self.capella_infra:
            config = {"enabled": True, "events": self._get_audit_settings()}
            self.rest.sgw_update_audit_logging(self.sgw_master_node, "db-1", config)
            audit_settings = self.rest.sgw_get_audit_settings(
                self.sgw_master_node, "db-1", filterable=False
            )
            logger.info(f"Audit settings: {pretty_dict(audit_settings)}")
            return

        self.rest.sgw_enable_audit_logging()
        sleep(10)
        state = self.rest.sgw_get_audit_logging_state()
        logger.info(f"The state of audit logging is: {state}")

        current_events = self.rest.sgw_get_audit_logging_config("db-1").json()
        logger.info(f"The current audit logging events are: {current_events}")

    def download_ycsb(self):
        if self.worker_manager.is_remote:
            self.remote.clone_ycsb(repo=self.test_config.syncgateway_settings.repo,
                                   branch=self.test_config.syncgateway_settings.branch,
                                   worker_home=self.worker_manager.WORKER_HOME,
                                   ycsb_instances=int(self.test_config.syncgateway_settings.
                                                      instances_per_client))
        else:
            local.clone_ycsb(repo=self.test_config.syncgateway_settings.repo,
                             branch=self.test_config.syncgateway_settings.branch)

    def build_ycsb(self, ycsb_client):
        if self.worker_manager.is_remote:
            self.remote.build_ycsb(self.worker_manager.WORKER_HOME, ycsb_client)
        else:
            local.build_ycsb(ycsb_client)

    def collect_execution_logs(self):
        if self.worker_manager.is_remote:
            if os.path.exists(self.LOCAL_DIR):
                shutil.rmtree(self.LOCAL_DIR, ignore_errors=True)
            os.makedirs(self.LOCAL_DIR)
            self.remote.get_syncgateway_ycsb_logs(self.worker_manager.WORKER_HOME,
                                                  self.test_config.syncgateway_settings,
                                                  self.LOCAL_DIR)

    def collect_cblite_logs(self):
        if self.worker_manager.is_remote:
            if os.path.exists(self.CBLITE_LOG_DIR):
                shutil.rmtree(self.CBLITE_LOG_DIR, ignore_errors=True)
            os.makedirs(self.CBLITE_LOG_DIR)
            for client in self.cluster_spec.workers[:int(
                    self.settings.syncgateway_settings.clients)]:
                for instance_id in range(int(
                                         self.settings.syncgateway_settings.instances_per_client)):
                    self.remote.get_cblite_logs(client, instance_id, self.CBLITE_LOG_DIR)

    def run_sg_phase(self,
                     phase: str,
                     task: Callable,
                     settings: PhaseSettings,
                     target_iterator: Iterable = None,
                     timer: int = None,
                     distribute: bool = False,
                     wait: bool = True) -> None:
        if target_iterator is None:
            target_iterator = self.target_iterator
        settings.admin_port = self.admin_port
        settings.public_port = self.public_port
        settings.memcached_port = self.memcached_port

        logger.info(f"Running {phase}")
        self.log_task_settings([WorkloadPhase(task, target_iterator, settings, timer=timer)])
        self.worker_manager.run_sg_tasks(task, settings, target_iterator, timer, distribute, phase)
        if wait:
            self.worker_manager.wait_for_fg_tasks()

    def start_memcached(self):
        if self.dynamic_infra:
            self.remote.start_memcached()
        else:
            self.run_sg_phase(
                phase="start memcached",
                task=syncgateway_task_start_memcached,
                settings=self.settings,
                target_iterator=None,
                timer=self.settings.time,
                distribute=False,
            )

    @with_stats
    def load_users(self):
        self.run_sg_phase(
            phase="load users",
            task=syncgateway_task_load_users,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=False
        )

    @with_stats
    def init_users(self):
        if self.test_config.syncgateway_settings.auth == 'true':
            self.run_sg_phase(
                phase="init users",
                task=syncgateway_task_init_users,
                settings=self.settings,
                target_iterator=None,
                timer=self.settings.time,
                distribute=False
            )

    @with_stats
    def grant_access(self):
        if self.test_config.syncgateway_settings.grant_access == 'true':
            self.run_sg_phase(
                phase="grant access to users",
                task=syncgateway_task_grant_access,
                settings=self.settings,
                target_iterator=None,
                timer=self.settings.time,
                distribute=False
            )

    @with_stats
    def load_docs(self):
        self.run_sg_phase(
            phase="load docs",
            task=syncgateway_task_load_docs,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=self.use_distributed_load,
        )

    @with_stats
    @with_profiles
    def run_test(self):
        self.run_sg_phase(
            phase="run test",
            task=syncgateway_task_run_test,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=True
        )

    @with_stats
    @with_profiles
    def monitor_sg_replicate(
        self, sg_master: str, expected_docs: int, replication_id: str, version: int
    ):
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(
            sg_master, self.test_config.cluster.num_buckets, expected_docs, replication_id, version
        )
        return time_elapsed, items_in_range

    def compress_sg_logs(self):
        try:
            self.remote.compress_sg_logs_new()
            return
        except Exception as ex:
            logger.warn(ex)
        try:
            self.remote.compress_sg_logs()
        except Exception as ex:
            logger.warn(ex)

    def get_sg_logs(self):
        ssh_user, ssh_pass = self.cluster_spec.ssh_credentials
        for node in range(int(self.sg_settings.nodes)):
            server = self.cluster_spec.sgw_servers[node]
            try:
                local.get_sg_logs_new(host=server, ssh_user=ssh_user, ssh_pass=ssh_pass)
            except Exception as ex:
                logger.warn(ex)
        if self.settings.syncgateway_settings.troublemaker:
            for server in self.cluster_spec.sgw_servers:
                try:
                    local.get_troublemaker_logs(host=server, ssh_user=ssh_user, ssh_pass=ssh_pass)
                    local.rename_troublemaker_logs(from_host=server)
                except Exception as ex:
                    logger.warn(ex)
                try:
                    local.get_default_troublemaker_logs(host=server, ssh_user=ssh_user,
                                                        ssh_pass=ssh_pass)
                except Exception as ex:
                    logger.warn(ex)

    def get_sg_console(self):
        ssh_user, ssh_pass = self.cluster_spec.ssh_credentials
        for node in range(int(self.sg_settings.nodes)):
            server = self.cluster_spec.sgw_servers[node]
            try:
                local.get_sg_console(host=server, ssh_user=ssh_user, ssh_pass=ssh_pass)
            except Exception as ex:
                logger.warn(ex)

    def check_num_warnings(self):
        warn_count = 0
        if not self.capella_infra:
            for host in self.cluster_spec.sgw_servers:
                stats = self.rest.get_sg_stats(host)
                warn_count += int(stats['syncgateway']['global']
                                       ['resource_utilization']['warn_count'])
        else:
            host = self.cluster_spec.sgw_servers[0]
            stats = self.rest.get_sg_stats(host)
            warn_count = parse_prometheus_stat(stats, "sgw_resource_utilization_warn_count")

        if warn_count > 1000:
            return warn_count

    def channel_list(self, number_of_channels: int):
        channels = []
        for number in range(1, number_of_channels + 1):
            channel = "channel-" + str(number)
            channels.append(channel)
        return channels

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.load_docs()
        self.init_users()
        self.grant_access()
        self.run_test()
        self.report_kpi()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.settings.syncgateway_settings.cbl_per_worker:
            self.remote.kill_cblite()
        if self.settings.syncgateway_settings.collect_cbl_logs:
            self.collect_cblite_logs()
        if self.settings.syncgateway_settings.ramdisk_size != 0:
            self.remote.destroy_cblite_ramdisk()
        if self.settings.syncgateway_settings.troublemaker:
            self.remote.kill_troublemaker()

        if self.test_config.test_case.use_workers:
            self.worker_manager.download_celery_logs()
            self.worker_manager.terminate()

        if self.test_config.cluster.online_cores:
            self.remote.enable_cpu()

        if self.test_config.cluster.kernel_mem_limit:
            self.remote.reset_memory_settings()
            self.monitor.wait_for_servers()

        too_many_warnings = self.check_num_warnings()

        if self.dynamic_infra:
            if too_many_warnings:
                raise Exception(f"Too many warnings: {too_many_warnings}")
            return

        if self.settings.syncgateway_settings.collect_sgw_logs or too_many_warnings:
            self.compress_sg_logs()
            self.get_sg_logs()

        if self.settings.syncgateway_settings.collect_sgw_console:
            self.get_sg_console()

        if too_many_warnings:
            raise Exception(f"Too many warnings: {too_many_warnings}")


class SGLoad(SGPerfTest):
    def unlink_collections(self):
        # When the test is done, unlink the collections
        # (update the app endpoint specs with only 1 coll) and time it
        collections_map = self.test_config.collection.collection_map
        if collections_map:
            logger.info("Unlinking collections")
            t0 = time.time()
            for bucket_name in self.test_config.buckets:
                sgw_db_name = f'db-{bucket_name.split("-")[1]}'
                config = {
                    "scopes": {
                        "scope-1": {
                            "collections": {
                                "collection-1": {
                                    "sync": "function(doc1){channel(doc1.channels);}",
                                    "import_filter": "function(doc1){return true;}"
                                }
                            }
                        }
                    }
                }
                self.rest.sgw_update_db(sgw_db_name, config)
            coll_unlink_time = time.time() - t0
            logger.info(f'Coll unlink time is: {coll_unlink_time}')

    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*loaddocs*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        if self.capella_infra and self.test_config.deployment.monitor_deployment_time:
            with TimeTrackingFile() as t:
                deployment_time = t.config.get('app_services_cluster')
                collection_time = t.config.get('app_services_coll')
                creation_time = t.config.get('app_services_db')
            logger.info(f'deployment_time is: {deployment_time}')
            self.reporter.post(
                *self.metrics.cluster_deployment_time(deployment_time, "cluster_deployment_time",
                                                      "App Services Deployment Time (sec)")
            )

            if self.collections:
                logger.info(f'collection_time is: {collection_time}')
                self.reporter.post(
                    *self.metrics.cluster_deployment_time(collection_time,
                                                          "collection_creation_time",
                                                          "Collection Creation Time (sec)")
                )

            logger.info(f'creation_time is: {creation_time}')
            self.reporter.post(
                *self.metrics.cluster_deployment_time(creation_time, "bucket_creation_time",
                                                      "Database Creation Time (sec)")
            )

        self.reporter.post(
            *self.metrics.sg_throughput("Load Throughput (docs/sec)", "load_")
        )

        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    def run(self):
        self.remote.remove_sglogs()

        self.download_ycsb()

        self.start_memcached()
        self.load_users()
        self.load_docs()

        self.unlink_collections()
        # sleep(600)

        self.report_kpi()


class SGRead(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), GET doc by id")
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )


class SGReadLatency(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_latency('[READ], 95thPercentileLatency(us)',
                                     'Latency (ms), GET doc by id, 95 percentile')
        )


class SGAuthThroughput(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), POST auth")
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )


class SGAuthLatency(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_latency('[SCAN], 95thPercentileLatency(us)',
                                     'Latency (ms), POST auth, 95 percentile')
        )


class SGSync(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput('Throughput (req/sec), GET docs via _changes')
        )

        self.reporter.post(
            *self.metrics.sg_latency('[INSERT], 95thPercentileLatency(us)',
                                     'Latency, round-trip write, 95 percentile (ms)')
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )


class SGSyncQueryThroughput(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput('Throughput (req/sec), GET docs via _changes')
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )


class SGSyncQueryLatency(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_latency('[READ], 95thPercentileLatency(us)',
                                     'Latency (ms), GET docs via _changes, 95 percentile')
        )


class SGWrite(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), POST doc")
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )


class SGWriteLatency(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            self.metrics.sg_latency('[INSERT], 95thPercentileLatency(us)',
                                    'Latency (ms), POST doc, 95 percentile')
        )


class SGInsertUsers(SGPerfTest):

    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*loaddocs*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Load Throughput (docs/sec)", "load_users_")
        )

        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    def run(self):
        self.remote.remove_sglogs()

        self.download_ycsb()

        self.start_memcached()
        self.load_users()

        self.report_kpi()


class SGEventingTest(SGPerfTest, FunctionsTimeTest):

    def __init__(self, *args, **kwargs):
        FunctionsTimeTest.__init__(self, *args, **kwargs)
        SGPerfTest.__init__(self, *args, **kwargs)

    @with_stats
    @with_profiles
    @timeit
    def load_access_and_wait(self):
        self.load_docs()
        self.run_test()

    def get_sgw_writes(self, host: str, num_buckets: int):
        stats = self.rest.get_sg_stats(host=host)
        sg_writes = 0
        if not self.cluster_spec.capella_infrastructure:
            for count in range(1, num_buckets + 1):
                db = f'db-{count}'
                if 'database' in (db_stats := stats['syncgateway']['per_db'][db]):
                    sg_writes += int(db_stats['database']['num_doc_writes'])
        else:
            sg_writes = parse_prometheus_stat(stats, "sgw_database_num_doc_writes")
        return sg_writes

    def print_stats(self):
        total_import_count = 0
        total_writes = 0
        num_buckets = self.test_config.cluster.num_buckets
        for i in range(self.test_config.syncgateway_settings.import_nodes):
            server = self.cluster_spec.sgw_servers[i]
            import_count = self.monitor.get_import_count(host=server, num_buckets=num_buckets)
            total_import_count += import_count
        for i in range(self.test_config.syncgateway_settings.nodes):
            server = self.cluster_spec.sgw_servers[i]
            writes = self.get_sgw_writes(host=server, num_buckets=num_buckets)
            total_writes += writes
        logger.info(f"Total docs imported: {total_import_count}")
        logger.info(f"Total SGW docs written: {total_writes}")
        bucket_replica = self.test_config.bucket.replica_number
        num_items = 0
        for bucket_name in self.test_config.buckets:
            num_items += self.monitor.get_num_items(host=self.cluster_spec.servers[0],
                                                    bucket=bucket_name,
                                                    bucket_replica=bucket_replica)
        num_items = num_items / (bucket_replica + 1)

        logger.info(f"Total items in bucket: {num_items}")

    def _report_kpi(self, time_elapsed):
        self.collect_execution_logs()

        events_successfully_processed = self.get_on_update_success()
        logger.info(f"Events successfully processed: {format(events_successfully_processed)}")
        self.print_stats()

        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())
        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), POST doc")
        )

        self.reporter.post(
            *self.metrics.function_throughput_sg(time=time_elapsed,
                                                 event_name=None,
                                                 events_processed=events_successfully_processed)
        )

    def run(self):

        self.download_ycsb()

        self.start_memcached()
        self.load_users()

        self.set_functions()

        time_elapsed = self.load_access_and_wait()

        logger.info(f"time elapsed is: {time_elapsed}")

        self.report_kpi(time_elapsed)


class SGMixQueryThroughput(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec)")
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )


class SGTargetIterator(TargetIterator):

    def __iter__(self):
        username = self.cluster_spec.rest_credentials[0]
        if self.test_config.client_settings.python_client:
            if self.test_config.client_settings.python_client.split('.')[0] == "2":
                password = self.test_config.bucket.password
            else:
                password = self.cluster_spec.rest_credentials[1]
        else:
            password = self.cluster_spec.rest_credentials[1]
        prefix = self.prefix
        src_master = next(self.cluster_spec.masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, username, password, prefix)


class CBTargetIterator(TargetIterator):

    def __iter__(self):
        username = self.cluster_spec.rest_credentials[0]
        if self.test_config.client_settings.python_client:
            if self.test_config.client_settings.python_client.split('.')[0] == "2":
                password = self.test_config.bucket.password
            else:
                password = self.cluster_spec.rest_credentials[1]
        else:
            password = self.cluster_spec.rest_credentials[1]
        prefix = self.prefix
        # masters = self.cluster_spec.masters
        cb_master = self.cluster_spec.servers[0]
        # src_master = next(masters)
        # dest_master = next(masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(cb_master, bucket)
            yield TargetSettings(cb_master, bucket, username, password, prefix)


class SGImportLoad(PerfTest):

    def load(self, *args):
        PerfTest.load(self, task=pillowfight_data_load_task)

    def run(self):
        self.load()


class SGImportThroughputTest(SGPerfTest):

    def load(self, *args, **kwargs):
        PerfTest.load(self, task=ycsb_data_load_task)

    def access(self, *args, **kwargs):
        PerfTest.access(self, task=ycsb_task)

    def access_bg(self, *args, **kwargs):
        PerfTest.access_bg(self, task=ycsb_task)

    COLLECTORS = {'disk': False, 'ns_server': False, 'ns_server_overview': False,
                  'active_tasks': False, 'syncgateway_stats': True}

    def _report_kpi(self, time_elapsed_load, items_in_range_load, time_elapsed_access,
                    items_in_range_access):
        self.collect_execution_logs()
        self.reporter.post(
            *self.metrics.sgimport_items_per_sec(time_elapsed=time_elapsed_load,
                                                 items_in_range=items_in_range_load,
                                                 operation="INSERT")
        )
        self.reporter.post(
            *self.metrics.sgimport_items_per_sec(time_elapsed=time_elapsed_access,
                                                 items_in_range=items_in_range_access,
                                                 operation="UPDATE")
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    @with_stats
    @with_profiles
    def monitor_sg_import(self, phase):
        host = self.cluster_spec.sgw_servers[0]
        num_buckets = self.test_config.cluster.num_buckets
        expected_docs = self.test_config.load_settings.items * num_buckets
        if phase == 'access':
            expected_docs = expected_docs * 2
        logger.info(f'expected docs :{expected_docs}')

        initial_docs = self.initial_import_count()
        logger.info(f'initial docs imported :{initial_docs}')

        remaining_docs = expected_docs - initial_docs
        logger.info(f'remaining_docs :{remaining_docs}')

        time_elapsed, items_in_range = self.monitor.monitor_sgimport_queues(
            host, num_buckets, expected_docs
        )
        return time_elapsed, items_in_range

    def initial_import_count(self):
        total_initial_docs = 0
        num_buckets = self.test_config.cluster.num_buckets
        for i in range(self.test_config.syncgateway_settings.import_nodes):
            server = self.cluster_spec.sgw_servers[i]
            import_count = self.monitor.get_import_count(host=server, num_buckets=num_buckets)
            total_initial_docs += import_count
            if self.capella_infra:
                break
        return total_initial_docs

    @with_stats
    @with_profiles
    def monitor_sg_import_multinode(self, phase: str, timeout: int = 1200, sleep_delay: int = 2):
        expected_docs = self.test_config.load_settings.items * self.test_config.cluster.num_buckets
        if phase == 'access':
            expected_docs = expected_docs * 2
        logger.info(f'expected docs :{expected_docs}')

        initial_docs = self.initial_import_count()
        logger.info(f'initial docs imported :{initial_docs}')

        remaining_docs = expected_docs - initial_docs
        logger.info(f'remaining_docs :{remaining_docs}')

        importing = True

        start_time = time()
        num_buckets = self.test_config.cluster.num_buckets

        while importing:
            sleep(sleep_delay)
            total_count = 0
            for i in range(self.test_config.syncgateway_settings.import_nodes):
                server = self.cluster_spec.sgw_servers[i]
                import_count = self.monitor.get_import_count(host=server, num_buckets=num_buckets)
                logger.info(f'import count : {import_count} , host : {server}')
                total_count += import_count
            if total_count >= expected_docs:
                importing = False
            if time() - start_time > timeout:
                raise Exception(f"timeout of {timeout} exceeded")

        end_time = time()

        time_elapsed = end_time - start_time

        return time_elapsed, remaining_docs

    def run(self):
        self.download_ycsb()
        self.build_ycsb(ycsb_client=self.test_config.load_settings.ycsb_client)

        t0 = time()

        self.load()

        if self.sg_settings.db_config_path:
            with open(self.sg_settings.db_config_path) as file:
                config = json.load(file).get('databases', {})
                for db_name, db_config in config.items():
                    self.rest.sgw_create_db(db_name, db_config, self.sgw_master_node)

        sleep(120)

        if self.test_config.syncgateway_settings.import_nodes == 1 or self.capella_infra:
            time_elapsed_load, items_in_range_load = \
                self.monitor_sg_import(phase='load')
        else:
            time_elapsed_load, items_in_range_load = self.monitor_sg_import_multinode(phase="load")

        if items_in_range_load == 0:
            time_elapsed_load = time() - t0
            items_in_range_load = self.test_config.load_settings.items

        self.access_bg()

        if self.test_config.syncgateway_settings.import_nodes == 1 or self.capella_infra:
            time_elapsed_access, items_in_range_access = \
                self.monitor_sg_import(phase='access')
        else:
            time_elapsed_access, items_in_range_access = self.monitor_sg_import_multinode(
                phase="access"
            )

        self.report_kpi(time_elapsed_load, items_in_range_load,
                        time_elapsed_access, items_in_range_access)


class SGImportLatencyTest(SGPerfTest):

    def download_ycsb(self):
        if self.worker_manager.is_remote:
            self.remote.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                                   branch=self.test_config.ycsb_settings.branch,
                                   worker_home=self.worker_manager.WORKER_HOME,
                                   ycsb_instances=1)
        else:
            local.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                             branch=self.test_config.ycsb_settings.branch)

    COLLECTORS = {'disk': False, 'ns_server': False, 'ns_server_overview': False,
                  'active_tasks': False, 'syncgateway_stats': True,
                  'sgimport_latency': True}

    def _report_kpi(self, *args):
        self.collect_execution_logs()
        self.reporter.post(
            *self.metrics.sgimport_latency()
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    def monitor_sg_import(self):
        host = self.cluster_spec.sgw_servers[0]
        expected_docs = self.test_config.load_settings.items
        self.monitor.monitor_sgimport_queues(
            host, self.test_config.cluster.num_buckets, expected_docs
        )

    @with_stats
    @with_profiles
    def load(self, *args):
        cb_target_iterator = CBTargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')
        super().load(task=ycsb_data_load_task, target_iterator=cb_target_iterator)
        self.monitor_sg_import()

    @with_stats
    @with_profiles
    def access(self, *args):
        cb_target_iterator = CBTargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')
        super().access_bg(task=ycsb_task, target_iterator=cb_target_iterator)

    def run(self):
        self.download_ycsb()
        self.load()
        self.report_kpi()


class SGEventingImportTest(SGEventingTest, SGImportThroughputTest):

    def __init__(self, *args):
        super().__init__(*args)

    def _report_kpi(self, time_elapsed_load, items_in_range_load, time_elapsed,
                    events_successfully_processed):
        self.collect_execution_logs()
        logger.info(f"Events successfully processed: {events_successfully_processed}")
        self.print_stats()

        self.reporter.post(
            *self.metrics.sgimport_items_per_sec(time_elapsed=time_elapsed_load,
                                                 items_in_range=items_in_range_load,
                                                 operation="INSERT")
        )

        self.reporter.post(
            *self.metrics.function_throughput_sg(time=time_elapsed,
                                                 event_name=None,
                                                 events_processed=events_successfully_processed)
        )

    def run(self):


        self.download_ycsb()

        self.set_functions()

        self.load()

        if self.test_config.syncgateway_settings.import_nodes == 1 or self.capella_infra:
            time_elapsed_load, items_in_range_load = \
                self.monitor_sg_import(phase='load')
        else:
            time_elapsed_load, items_in_range_load = self.monitor_sg_import_multinode(phase="load")

        t0 = time()
        self.access()

        sleep(30)
        events_successfully_processed = self.get_on_update_success() - items_in_range_load
        time_elapsed = time() - t0
        logger.info(f"Time elapsed: {time_elapsed}")
        logger.info(f"Events successfully processed: {events_successfully_processed}")

        # Wait for SGW to import docs
        sleep(1200)

        self.report_kpi(time_elapsed_load, items_in_range_load,
                        time_elapsed, events_successfully_processed)


class SGSyncByUserWithAuth(SGSync):

    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput('Throughput (req/sec), SYNC docs via _changes')
        )

        self.reporter.post(
            *self.metrics.sg_latency('[INSERT], 95thPercentileLatency(us)',
                                     'Latency, round-trip write, 95 percentile (ms)')
        )

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.init_users()
        self.run_test()
        self.report_kpi()


class SGSyncByKeyNoAuth(SGSyncByUserWithAuth):

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.run_test()
        self.report_kpi()


class SGSyncInitialLoad(SGSyncByUserWithAuth):

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.load_docs()
        self.init_users()
        self.run_test()
        self.report_kpi()


class SGReplicateThroughputTest1(SGPerfTest):

    def _report_kpi(self, time_elapsed, items_in_range):
        self.reporter.post(
            *self.metrics.sgreplicate_items_per_sec(time_elapsed=time_elapsed,
                                                    items_in_range=items_in_range)
        )

    def start_replication(self, sg1_master, sg2_master):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg1 = f'https://{sg1_master}:4985/db-1'
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg1 = f'http://{sg1_master}:4985/db-1'
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr1_push",
            "source": sg1,
            "target": sg2,
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "changes_feed_limit": 10000,
            "collections_enabled": True if self.collections else False
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr1_pull"
            self.rest.start_sg_replication(sg2_master, data)

    def run(self):
        masters = self.cluster_spec.sgw_masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == "push":
            sgw_node = sg1_master
        elif self.sg_settings.sg_replication_type == "pull":
            sgw_node = sg2_master
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sgw_node,
            self.test_config.load_settings.items,
            f"sgr1_{self.sg_settings.sg_replication_type}",
            1,
        )
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputMultiChannelTest1(SGReplicateThroughputTest1):

    def start_replication(self, sg1_master, sg2_master):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg1 = f'https://{sg1_master}:4985/db-1'
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg1 = f'http://{sg1_master}:4985/db-1'
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr1_push",
            "source": sg1,
            "target": sg2,
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "changes_feed_limit": 10000,
            "collections_enabled": True if self.collections else False
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr1_pull"
            self.rest.start_sg_replication(sg2_master, data)

    def run(self):
        masters = self.cluster_spec.sgw_masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == "push":
            sgw_node = sg1_master
        elif self.sg_settings.sg_replication_type == "pull":
            sgw_node = sg2_master
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sgw_node,
            self.test_config.load_settings.items,
            f"sgr1_{self.sg_settings.sg_replication_type}",
            1,
        )
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputTest2(SGPerfTest):

    def _report_kpi(self, time_elapsed, items_in_range):
        self.reporter.post(
            *self.metrics.sgreplicate_items_per_sec(time_elapsed=time_elapsed,
                                                    items_in_range=items_in_range)
        )

    def start_replication(self, sg1_master, sg2_master):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg1 = f'https://{sg1_master}:4985/db-1'
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg1 = f'http://{sg1_master}:4985/db-1'
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr2_push",
            "remote": sg2,
            "direction": "push",
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "collections_enabled": True if self.collections else False
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication2(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr2_pull"
            data["direction"] = "pull"
            data["remote"] = sg1
            self.rest.start_sg_replication2(sg2_master, data)

    def run(self):
        masters = self.cluster_spec.sgw_masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == "push":
            sgw_node = sg1_master
        elif self.sg_settings.sg_replication_type == "pull":
            sgw_node = sg2_master
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sgw_node,
            self.test_config.load_settings.items,
            f"sgr2_{self.sg_settings.sg_replication_type}",
            2,
        )
        self.report_kpi(time_elapsed, items_in_range)
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )


class SGReplicateThroughputMultiChannelTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg1 = f'https://{sg1_master}:4985/db-1'
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg1 = f'http://{sg1_master}:4985/db-1'
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr2_push",
            "remote": sg2,
            "direction": "push",
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "collections_enabled": True if self.collections else False
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication2(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr2_pull"
            data["direction"] = "pull"
            data["remote"] = sg1
            self.rest.start_sg_replication2(sg2_master, data)

    def run(self):
        masters = self.cluster_spec.sgw_masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == "push":
            sgw_node = sg1_master
        elif self.sg_settings.sg_replication_type == "pull":
            sgw_node = sg2_master
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sgw_node,
            self.test_config.load_settings.items,
            f"sgr2_{self.sg_settings.sg_replication_type}",
            2,
        )
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputConflictResolutionTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr2_conflict_resolution",
            "remote": sg2,
            "direction": "pushAndPull",
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "conflict_resolution_type": "default",
            "collections_enabled": True if self.collections else False
        }

        if self.sg_settings.sg_conflict_resolution == 'custom':
            data["conflict_resolution_type"] = "custom"
            data["custom_conflict_resolver"] = \
                "function(conflict) { return defaultPolicy(conflict);}"

        self.rest.start_sg_replication2(sg1_master, data)

    def run(self):
        masters = self.cluster_spec.sgw_masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.start_replication(sg1_master, sg2_master)
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sg1_master,
            self.test_config.load_settings.items * 2,
            "sgr2_conflict_resolution",
            2,
        )
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateLoad(SGPerfTest):

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()


class SGReplicateThroughputBidirectionalTest1(SGReplicateThroughputTest1):

    def start_replication(self, sg1_master, sg2_master):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg1 = f'https://{sg1_master}:4985/db-1'
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg1 = f'http://{sg1_master}:4985/db-1'
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr1_push",
            "source": sg1,
            "target": sg2,
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "changes_feed_limit": 10000,
            "collections_enabled": True if self.collections else False
        }

        self.rest.start_sg_replication(sg1_master, data)

        data["replication_id"] = "sgr1_pull"
        data["source"] = sg2
        data["target"] = sg1
        self.rest.start_sg_replication(sg1_master, data)

    def run(self):
        masters = self.cluster_spec.sgw_masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.start_replication(sg1_master, sg2_master)
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sg1_master,
            self.test_config.load_settings.items * 2,
            "sgr1_pushAndPull",
            1,
        )
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputBidirectionalTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr2_pushAndPull",
            "remote": sg2,
            "direction": "pushAndPull",
            "continuous": True,
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "collections_enabled": True if self.collections else False
        }

        self.rest.start_sg_replication2(sg1_master, data)

    def run(self):
        masters = self.cluster_spec.sgw_masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.start_replication(sg1_master, sg2_master)

        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sg1_master,
            self.test_config.load_settings.items * 2,
            "sgr2_pushAndPull",
            2,
        )
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputMultiChannelMultiSgTest1(SGReplicateThroughputTest1):

    def start_replication(self, sg1_master, sg2_master, channel):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg1 = f'https://{sg1_master}:4985/db-1'
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg1 = f'http://{sg1_master}:4985/db-1'
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = [("channel-" + str(channel + 1))]
        replication_id = "sgr1_" + self.sg_settings.sg_replication_type + str(channel+1)
        data = {
            "replication_id": replication_id,
            "source": sg1,
            "target": sg2,
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "changes_feed_limit": 10000,
            "collections_enabled": True if self.collections else False
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            self.rest.start_sg_replication(sg2_master, data)
        return data["replication_id"]

    def run(self):
        sg1_node = []
        sg2_node = []
        replication_ids = []
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        for node in range(int(self.sg_settings.nodes)):
            sg1 = self.cluster_spec.sgw_servers[node]
            nodes_per_cluster = int(self.sg_settings.nodes) + \
                int(self.test_config.cluster.initial_nodes[0])
            sg2 = self.cluster_spec.sgw_servers[node+nodes_per_cluster]
            replication_id = self.start_replication(sg1, sg2, node)
            sg1_node.append(sg1)
            sg2_node.append(sg2)
            replication_ids.append(replication_id)

        if self.sg_settings.sg_replication_type == "push":
            sgw_node = sg1_node
        elif self.sg_settings.sg_replication_type == "pull":
            sgw_node = sg2_node
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sgw_node,
            self.test_config.load_settings.items,
            replication_ids,
            1,
        )
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputMultiChannelMultiSgTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master, channel):
        if self.capella_infra or \
           self.test_config.cluster.enable_n2n_encryption:
            sg1 = f'https://{sg1_master}:4985/db-1'
            sg2 = f'https://{sg2_master}:4985/db-1'
        else:
            sg1 = f'http://{sg1_master}:4985/db-1'
            sg2 = f'http://{sg2_master}:4985/db-1'

        channels = [("channel-" + str(channel+1))]
        replication_id = "sgr2_" + self.sg_settings.sg_replication_type + str(channel+1)
        data = {
            "replication_id": replication_id,
            "remote": sg2,
            "direction": "push",
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True,
            "collections_enabled": True if self.collections else False
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication2(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["direction"] = "pull"
            data["remote"] = sg1
            self.rest.start_sg_replication2(sg2_master, data)
        return data["replication_id"]

    def monitor_sg_replicate(
        self, sg_master: str, expected_docs: int, replication_id, version: int
    ):
        # This overrides to avoid triggering profiles and stats twice since
        # `run_replicate` already does this
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(
            sg_master, self.test_config.cluster.num_buckets, expected_docs, replication_id, version
        )
        return time_elapsed, items_in_range

    @with_stats
    @with_profiles
    def run_replicate(self):
        sleep(30)
        sg1_nodes = []
        sg2_nodes = []
        replication_ids = []
        for node in range(int(self.sg_settings.nodes)):
            sg1 = self.cluster_spec.sgw_servers[node]
            nodes_per_cluster = int(self.sg_settings.nodes)
            sg2 = self.cluster_spec.sgw_servers[node+nodes_per_cluster]
            replication_id = self.start_replication(sg1, sg2, node)
            sg1_nodes.append(sg1)
            sg2_nodes.append(sg2)
            replication_ids.append(replication_id)

        if self.sg_settings.sg_replication_type == "push":
            sgw_node = sg1_nodes
        elif self.sg_settings.sg_replication_type == "pull":
            sgw_node = sg2_nodes
        time_elapsed, items_in_range = self.monitor_sg_replicate(
            sgw_node,
            self.test_config.load_settings.items,
            replication_ids,
            2,
        )
        return time_elapsed, items_in_range

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        time_elapsed, items_in_range = self.run_replicate()
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateMultiClusterPull(SGPerfTest):

    def download_blockholepuller_tool(self):
        if self.worker_manager.is_remote:
            self.remote.download_blackholepuller(worker_home=self.worker_manager.WORKER_HOME)
        else:
            local.download_blockholepuller()

    def collect_execution_logs(self):
        if self.worker_manager.is_remote:

            logger.info('removing existing log & stderr files')
            local.remove_sg_bp_logs()
            self.remote.get_sg_blackholepuller_logs(self.worker_manager.WORKER_HOME,
                                                    self.test_config.syncgateway_settings)

    def run_sg_bp_phase(self,
                        phase: str,
                        task: Callable, settings: PhaseSettings,
                        target_iterator: Iterable = None,
                        timer: int = None,
                        distribute: bool = False) -> None:
        if target_iterator is None:
            target_iterator = self.target_iterator
        self.worker_manager.run_sg_bp_tasks(task, settings, target_iterator,
                                            timer, distribute, phase)
        self.worker_manager.wait_for_fg_tasks()

    @with_stats
    @with_profiles
    def run_bp_test(self):
        self.run_sg_bp_phase(
            phase="blackholepuller test",
            task=syncgateway_bh_puller_task,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=True
        )

    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('*sg_stats_blackholepuller_*.json'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_bp_throughput("Average docs/sec per client")
        )

        duration = int(self.test_config.syncgateway_settings.sg_blackholepuller_timeout)

        self.reporter.post(
            *self.metrics.sg_bp_total_docs_pulled(title="Total docs pulled per second",
                                                  duration=duration)
        )

        self.reporter.post(
            *self.metrics.sg_bp_num_replications(title="Number of replications executed",
                                                 documents=int(self.test_config.
                                                               syncgateway_settings.
                                                               documents))
        )

        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    def run(self):

        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.load_docs()
        self.init_users()
        self.grant_access()
        self.download_blockholepuller_tool()
        self.run_bp_test()
        self.report_kpi()


class SGReplicateMultiClusterPush(SGPerfTest):

    def download_newdocpusher_tool(self):
        if self.worker_manager.is_remote:
            self.remote.download_newdocpusher(worker_home=self.worker_manager.WORKER_HOME)
        else:
            local.download_newdocpusher()

    def collect_execution_logs(self):
        if self.worker_manager.is_remote:

            logger.info('removing existing log & stderr files')
            local.remove_sg_newdocpusher_logs()
            self.remote.get_sg_newdocpusher_logs(self.worker_manager.WORKER_HOME,
                                                 self.test_config.syncgateway_settings)

    def run_sg_docpush_phase(self,
                             phase: str,
                             task: Callable, settings: PhaseSettings,
                             target_iterator: Iterable = None,
                             timer: int = None,
                             distribute: bool = False) -> None:
        if target_iterator is None:
            target_iterator = self.target_iterator
        self.worker_manager.run_sg_bp_tasks(task, settings, target_iterator,
                                            timer, distribute, phase)
        self.worker_manager.wait_for_fg_tasks()

    @with_stats
    @with_profiles
    def run_bp_test(self):
        self.run_sg_docpush_phase(
            phase="newDocpusher test",
            task=syncgateway_new_docpush_task,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=True
        )

    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('*sg_stats_newdocpusher_*.json'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_newdocpush_throughput("Average docs/sec per client")
        )

        duration = int(self.test_config.syncgateway_settings.sg_blackholepuller_timeout)

        self.reporter.post(
            *self.metrics.sg_bp_total_docs_pushed(title="Total docs pushed per second",
                                                  duration=duration)
        )

        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.download_newdocpusher_tool()
        self.run_bp_test()
        self.report_kpi()


class DeltaSync(SGPerfTest):

    def load_docs(self):
        self.run_sg_phase(
            phase="load docs",
            task=syncgateway_delta_sync_task_load_docs,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=False
        )

    @with_stats
    @with_profiles
    def run_test(self):
        self.run_sg_phase(
            phase="run test",
            task=syncgateway_delta_sync_task_run_test,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=True
        )

    def start_cblite(self, port: str, db_name: str):
        local.start_cblitedb(port=port, db_name=db_name)

    @with_stats
    @with_profiles
    def cblite_replicate(self, cblite_db: str):
        replication_type = self.test_config.syncgateway_settings.replication_type
        sgw_ip = list(self.cluster_spec.sgw_masters)[0]
        if replication_type == 'PUSH':
            ret_str = local.replicate_push(self.cluster_spec, cblite_db, sgw_ip)
        elif replication_type == 'PULL':
            ret_str = local.replicate_pull(self.cluster_spec, cblite_db, sgw_ip)
        else:
            raise Exception(f'incorrect replication type: {replication_type}')

        if ret_str.find('Completed'):
            logger.info(f'cblite message:{ret_str}')
            replication_time = float((re.search('docs in (.*) secs;', ret_str)).group(1))
            docs_replicated = int((re.search('Completed (.*) docs in', ret_str)).group(1))
            if docs_replicated == int(self.test_config.syncgateway_settings.documents):
                success_code = 'SUCCESS'
            else:
                success_code = 'FAILED'
                logger.info(
                    f'Replication failed due to partial replication. Number of docs replicated: \
                    {docs_replicated}'
                )
        else:
            logger.info(f'Replication failed with error message:{ret_str}')
            replication_time = 0
            docs_replicated = 0
            success_code = 'FAILED'
        return replication_time, docs_replicated, success_code

    def _report_kpi(self,
                    replication_time: float,
                    throughput: int,
                    bandwidth: float,
                    synced_bytes: float):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())
        self.reporter.post(
            *self.metrics.deltasync_time(
                replication_time=replication_time
            )
        )

        self.reporter.post(
            *self.metrics.deltasync_throughput(throughput=throughput)
        )

        self.reporter.post(
            *self.metrics.deltasync_bytes(bytes=synced_bytes)
        )

        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    def db_cleanup(self):
        local.cleanup_cblite_db()

    def post_deltastats(self):
        sg_server = self.cluster_spec.sgw_servers[0]
        for count in range(1, self.test_config.cluster.num_buckets + 1):
            db = f'db-{count}'
            stats = self.monitor.deltasync_stats(host=sg_server, db=db)
            logger.info(f'Sync-gateway Stats for {db} are:{stats}')

    def calc_bandwidth_usage(self, synced_bytes: float, time_taken: float):
        # in Mb
        bandwidth = round((((synced_bytes/time_taken)/1024)/1024), 2)
        return bandwidth

    def get_bytes_transfer(self):
        return self.monitor.deltasync_bytes_transfer(
            host=self.cluster_spec.sgw_servers[0],
            num_buckets=self.test_config.cluster.num_buckets,
            replication_mode=self.test_config.syncgateway_settings.replication_type,
        )

    def run(self):
        self.download_ycsb()

        if self.test_config.syncgateway_settings.deltasync_cachehit_ratio == '100':
            self.start_cblite(port='4985', db_name='db-1')
            self.start_cblite(port='4986', db_name='db-2')
        else:
            self.start_cblite(port='4985', db_name='db-1')
        self.start_memcached()
        self.load_docs()
        sleep(20)
        if self.test_config.syncgateway_settings.deltasync_cachehit_ratio == '100':
            self.cblite_replicate(cblite_db='db-1')
            self.cblite_replicate(cblite_db='db-2')
        else:
            self.cblite_replicate(cblite_db='db-1')
        self.post_deltastats()
        self.run_test()

        if self.test_config.syncgateway_settings.deltasync_cachehit_ratio == '100':
            self.cblite_replicate(cblite_db='db-1')
            bytes_transferred_1 = self.get_bytes_transfer()
            replication_time, docs_replicated, success_code = self.cblite_replicate(
                cblite_db='db-2')
        else:
            bytes_transferred_1 = self.get_bytes_transfer()
            replication_time, docs_replicated, success_code = self.cblite_replicate(
                cblite_db='db-1')

        if success_code == 'SUCCESS':
            self.post_deltastats()
            bytes_transferred_2 = self.get_bytes_transfer()
            byte_transfer = bytes_transferred_2 - bytes_transferred_1
            bandwidth = self.calc_bandwidth_usage(
                synced_bytes=byte_transfer,
                time_taken=replication_time
            )
            throughput = int(docs_replicated/replication_time)
            self.report_kpi(replication_time, throughput, bandwidth, byte_transfer)

            self.db_cleanup()
        else:
            self.db_cleanup()

        if self.settings.syncgateway_settings.collect_sgw_logs:
            self.compress_sg_logs()
            self.get_sg_logs()

        if self.settings.syncgateway_settings.collect_sgw_console:
            self.get_sg_console()


class DeltaSyncParallel(DeltaSync):

    def generate_dbmap(self, num_dbs: int):
        db_map = {}
        for i in range(num_dbs):
            port = str(4985 + i)
            db_name = 'db' + str(i)
            db_map.update({port: db_name})
        return db_map

    def start_multiplecblite(self, db_map: map):
        cblite_dbs = []
        for key in db_map:
            local.start_cblitedb(port=key, db_name=db_map[key])
            cblite_dbs.append(db_map[key])
        return cblite_dbs

    @with_stats
    @with_profiles
    def multiple_replicate(self, num_agents: int, cblite_dbs: list):
        with Pool(processes=num_agents) as pool:
            logger.info(f'starting cb replicate parallel with {num_agents}')
            results = pool.map(func=self.cblite_replicate, iterable=cblite_dbs)
        logger.info(f'end of multiple replicate: {results}')
        return results

    def cblite_replicate(self, cblite_db: str):
        replication_type = self.test_config.syncgateway_settings.replication_type
        sgw_ip = list(self.cluster_spec.sgw_masters)[0]
        if replication_type == 'PUSH':
            ret_str = local.replicate_push(self.cluster_spec, cblite_db, sgw_ip)
        elif replication_type == 'PULL':
            ret_str = local.replicate_pull(self.cluster_spec, cblite_db, sgw_ip)
        else:
            raise Exception(f'incorrect replication type: {replication_type}')

        if ret_str.find('Completed'):
            logger.info(f'cblite message: {ret_str}')
            replication_time = float((re.search('docs in (.*) secs;', ret_str)).group(1))
            docs_replicated = int((re.search('Completed (.*) docs in', ret_str)).group(1))
            if docs_replicated == int(self.test_config.syncgateway_settings.documents):
                success_code = 'SUCCESS'
            else:
                success_code = 'FAILED'
                logger.info(
                    f'Replication failed due to partial replication. Number of docs replicated: \
                    {docs_replicated}'
                )
        else:
            logger.info(f'Replication failed with error message:{ret_str}')
            replication_time = 0
            docs_replicated = 0
            success_code = 'FAILED'
        return replication_time, docs_replicated, success_code

    def get_average_time(self, results: list):
        tmp_sum = 0
        for result in results:
            tmp_sum = tmp_sum + result[0]
        average = tmp_sum/len(results)
        return average

    def get_docs_replicated(self, results: list):
        doc_count = 0
        for result in results:
            doc_count = doc_count + result[1]
        return doc_count

    def check_success(self, results: list):
        success_count = 0
        for result in results:
            if result[2] == 'SUCCESS':
                success_count += 1
        logger.info(f'success_count :{success_count}')
        if success_count == len(results):
            return 1
        else:
            return 0

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        num_dbs = int(self.test_config.syncgateway_settings.replication_concurrency)
        db_map = self.generate_dbmap(num_dbs)
        cblite_dbs = self.start_multiplecblite(db_map)

        if self.test_config.syncgateway_settings.deltasync_cachehit_ratio == '100':
            self.start_cblite(port='4983', db_name='db')

        self.load_docs()

        if self.test_config.syncgateway_settings.deltasync_cachehit_ratio == '100':
            self.cblite_replicate(cblite_db='db')

        num_agents = len(cblite_dbs)

        self.multiple_replicate(num_agents, cblite_dbs)

        bytes_transferred_1 = self.get_bytes_transfer()
        self.post_deltastats()

        self.run_test()

        if self.test_config.syncgateway_settings.deltasync_cachehit_ratio == '100':
            self.cblite_replicate(cblite_db='db')
            bytes_transferred_1 = self.get_bytes_transfer()

        results = self.multiple_replicate(num_agents, cblite_dbs)

        if self.check_success(results) == 1:
            self.post_deltastats()
            bytes_transferred_2 = self.get_bytes_transfer()
            byte_transfer = bytes_transferred_2 - bytes_transferred_1
            average_time = self.get_average_time(results)

            docs_replicated = self.get_docs_replicated(results)

            throughput = int(docs_replicated/average_time)
            bandwidth = self.calc_bandwidth_usage(
                synced_bytes=byte_transfer,
                time_taken=average_time
            )

            self.report_kpi(average_time, throughput, bandwidth, byte_transfer)

            self.db_cleanup()
        else:
            self.db_cleanup()

        if self.settings.syncgateway_settings.collect_sgw_logs:
            self.compress_sg_logs()
            self.get_sg_logs()

        if self.settings.syncgateway_settings.collect_sgw_console:
            self.get_sg_console()


class EndToEndTest(SGPerfTest):

    def start_continuous_replication(self, db):
        replication_type = self.test_config.syncgateway_settings.replication_type
        sgw_ip = list(self.cluster_spec.sgw_masters)[0]
        if replication_type == 'E2E_PUSH':
            local.replicate_push_continuous(self.cluster_spec, db, sgw_ip)
        elif replication_type == 'E2E_PULL':
            local.replicate_pull_continuous(self.cluster_spec, db, sgw_ip)
        else:
            raise Exception(
                f'replication type must be either E2E_PUSH or E2E_PULL: {replication_type}'
            )

    def wait_for_docs_pushed(self, initial_docs, target_docs):
        sgw_servers = self.settings.syncgateway_settings.nodes
        sg_servers = self.cluster_spec.sgw_servers[0:sgw_servers]
        sgw_t0, start_push_count = self.monitor.wait_sgw_push_start(
            sg_servers, self.test_config.cluster.num_buckets, initial_docs
        )
        logger.info("waiting for push complete...")
        sgw_t1, end_push_count = self.monitor.wait_sgw_push_docs(
            sg_servers, self.test_config.cluster.num_buckets, initial_docs + target_docs
        )
        sgw_time = sgw_t1 - sgw_t0
        observed_pushed = end_push_count-start_push_count
        logger.info(f'sgw_time: {sgw_time}, observed_pushed: {observed_pushed}, \
                    throughput: {observed_pushed/sgw_time}')
        return sgw_time, observed_pushed

    def wait_for_docs_pulled(self, initial_docs, target_docs):
        sgw_servers = self.settings.syncgateway_settings.nodes
        sg_servers = self.cluster_spec.sgw_servers[0:sgw_servers]
        logger.info(f'Initial docs: {initial_docs}, Target docs: {target_docs}')
        sgw_t0, start_pull_count = self.monitor.wait_sgw_pull_start(
            sg_servers, self.test_config.cluster.num_buckets, initial_docs
        )
        logger.info("waiting for pull complete...")
        sgw_t1, end_pull_count = self.monitor.wait_sgw_pull_docs(
            sg_servers, self.test_config.cluster.num_buckets, initial_docs + target_docs
        )
        sgw_time = sgw_t1 - sgw_t0
        observed_pulled = end_pull_count - start_pull_count
        logger.info(f'sgw_time: {sgw_time}, observed_pulled: {observed_pulled}, \
                    throughput: {observed_pulled/sgw_time}')
        return sgw_time, observed_pulled

    def post_delta_stats(self):
        sgw_servers = self.settings.syncgateway_settings.nodes
        sg_servers = self.cluster_spec.sgw_servers[0:sgw_servers]
        pull_count = 0
        push_count = 0
        for host in sg_servers:
            sgw_stats = self.rest.get_sg_stats(host)
            if not self.capella_infra:
                logger.info(f'Sync-gateway Stats for host {host}: \
                            {pretty_dict(sgw_stats["syncgateway"]["per_db"])}')
                for count in range(1, self.test_config.cluster.num_buckets + 1):
                    db = f'db-{count}'
                    pull_count += \
                        int(sgw_stats['syncgateway']['per_db'][db]
                                     ['cbl_replication_pull']['rev_send_count'])
                    push_count += \
                        int(sgw_stats['syncgateway']['per_db'][db]
                                     ['cbl_replication_push']['doc_push_count'])
            else:
                stat = sgw_stats.find("sgw_replication_pull_rev_send_count")
                stat_list = []
                while stat != -1:
                    stat_list.append(stat)
                    stat = sgw_stats.find("sgw_replication_pull_rev_send_count", stat + 1)
                last = sgw_stats.find("# HELP", stat_list[-1] + 1)
                stat_list.append(last)
                for i in range(2, len(stat_list) - 1):
                    str = sgw_stats[stat_list[i]:stat_list[i+1]]
                    a = str.find("}")
                    pull_count += int(float(str[a+2:]))

                stat = sgw_stats.find("sgw_replication_push_doc_push_count")
                stat_list = []
                while stat != -1:
                    stat_list.append(stat)
                    stat = sgw_stats.find("sgw_replication_push_doc_push_count", stat + 1)
                last = sgw_stats.find("# HELP", stat_list[-1] + 1)
                stat_list.append(last)
                for i in range(2, len(stat_list) - 1):
                    str = sgw_stats[stat_list[i]:stat_list[i+1]]
                    a = str.find("}")
                    push_count += int(float(str[a+2:]))
                break
        return dict(pull_count=pull_count, push_count=push_count)

    def print_ycsb_logs(self):
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as f_out:
                logger.info(f)
                logger.info(f_out.read())

    def _report_kpi(self, sgw_load_tp: int, sgw_access_tp: int):
        if sgw_load_tp > 0:  # Make E2E tests a bit resilient, collect partial data when available
            self.reporter.post(
                *self.metrics.sgw_e2e_throughput(
                    throughput=sgw_load_tp,
                    operation="INSERT",
                    replication=self.test_config.syncgateway_settings.replication_type
                )
            )
            self.reporter.post(
                *self.metrics.sgw_e2e_throughput_per_cblite(
                    throughput=sgw_load_tp / float(self.settings.syncgateway_settings.threads),
                    operation="INSERT",
                    replication=self.test_config.syncgateway_settings.replication_type
                )
            )

        if sgw_access_tp > 0:
            self.reporter.post(
                *self.metrics.sgw_e2e_throughput(
                    throughput=sgw_access_tp,
                    operation="UPDATE",
                    replication=self.test_config.syncgateway_settings.replication_type
                )
            )
            self.reporter.post(
                *self.metrics.sgw_e2e_throughput_per_cblite(
                    throughput=sgw_access_tp / float(self.settings.syncgateway_settings.threads),
                    operation="UPDATE",
                    replication=self.test_config.syncgateway_settings.replication_type
                )
            )

            if self.sg_settings.mem_cpu_stats:
                self.reporter.post(
                    *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
                )
                self.reporter.post(
                    *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
                )

    @with_stats
    @with_profiles
    def e2e_cbl_load_bg(self, pre_load_writes, load_docs):
        settings = copy.deepcopy(self.settings)
        settings.syncgateway_settings.clients = \
            self.settings.syncgateway_settings.load_clients
        settings.syncgateway_settings.threads = \
            self.settings.syncgateway_settings.load_threads
        settings.syncgateway_settings.instances_per_client = \
            self.settings.syncgateway_settings.load_instances_per_client
        self.run_sg_phase(
            phase="load phase",
            task=syncgateway_e2e_cbl_task_load_docs,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=True,
            wait=False
        )
        sgw_load_time, observed_pushed = \
            self.wait_for_docs_pushed(
                initial_docs=pre_load_writes,
                target_docs=load_docs
            )
        return sgw_load_time, observed_pushed

    @with_stats
    @with_profiles
    def e2e_cbl_update_bg(self, post_load_writes, load_docs):
        self.run_sg_phase(
            phase="access phase",
            task=syncgateway_e2e_cbl_task_run_test,
            settings=self.settings,
            target_iterator=None,
            timer=self.settings.time,
            distribute=True,
            wait=False
        )
        sgw_access_time, observed_pushed = \
            self.wait_for_docs_pushed(
                initial_docs=post_load_writes,
                target_docs=int(load_docs*0.9)
            )
        self.worker_manager.wait_for_bg_tasks()
        return sgw_access_time, observed_pushed

    @with_stats
    @with_profiles
    def e2e_cb_load_bg(self, pre_load_writes, load_docs):
        cb_target_iterator = CBTargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')
        super().load_bg(task=ycsb_data_load_task, target_iterator=cb_target_iterator)
        sgw_load_time = \
            self.wait_for_docs_pulled(
                initial_docs=pre_load_writes,
                target_docs=load_docs
            )
        return sgw_load_time

    @with_stats
    @with_profiles
    def e2e_cb_update_bg(self, post_load_writes, load_docs):
        cb_target_iterator = CBTargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')
        super().access_bg(task=ycsb_task, target_iterator=cb_target_iterator)
        sgw_access_time = \
            self.wait_for_docs_pulled(
                initial_docs=post_load_writes,
                target_docs=load_docs
            )
        return sgw_access_time


class EndToEndSingleCBLTest(EndToEndTest):

    def run_push(self):
        pass

    def run_pull(self):
        pass

    def run_bidi(self):
        pass

    def run(self):
        try:
            local.kill_cblite()
        except Exception as ex:
            logger.warn(ex)
        self.download_ycsb()
        local.clone_cblite()
        local.build_cblite()
        local.cleanup_cblite_db()
        local.start_cblitedb_continuous(port='4985', db_name='db')
        self.start_continuous_replication('db')
        self.start_memcached()
        replication_type = self.test_config.syncgateway_settings.replication_type
        if replication_type == 'E2E_PUSH':
            self.run_push()
        elif replication_type == 'E2E_PULL':
            self.run_pull()
        elif replication_type == "E2E_BIDI":
            self.run_bidi()
        else:
            raise Exception(
                'Replication type must be '
                'E2E_PUSH, E2E_PULL or E2E_BIDI: '
                f'{replication_type}'
            )


class EndToEndSingleCBLPushTest(EndToEndSingleCBLTest):

    def run_push(self):
        load_docs = int(self.settings.syncgateway_settings.documents)
        pre_load_stats = self.post_delta_stats()
        pre_load_writes = pre_load_stats['push_count']
        logger.info(f'initial writes: {pre_load_writes}')

        sgw_load_time, observed_pushed_load = self.e2e_cbl_load_bg(pre_load_writes, load_docs)

        post_load_stats = self.post_delta_stats()
        post_load_writes = post_load_stats['push_count']
        logger.info(f'post load writes: {post_load_writes}')

        sgw_access_time, observed_pushed_access = self.e2e_cbl_update_bg(post_load_writes,
                                                                         load_docs)

        post_access_stats = self.post_delta_stats()
        post_access_writes = post_access_stats['push_count']
        logger.info(f'post access writes: {post_access_writes}')

        sgw_load_tp = observed_pushed_load / sgw_load_time
        sgw_access_tp = observed_pushed_access / sgw_access_time

        self.collect_execution_logs()
        self.print_ycsb_logs()

        self.report_kpi(sgw_load_tp, sgw_access_tp)
        local.cleanup_cblite_db_coninuous()

        if self.settings.syncgateway_settings.collect_sgw_logs:
            self.compress_sg_logs()
            self.get_sg_logs()

        if self.settings.syncgateway_settings.collect_sgw_console:
            self.get_sg_console()


class EndToEndSingleCBLPullTest(EndToEndSingleCBLTest):

    def download_ycsb(self):
        if self.worker_manager.is_remote:
            self.remote.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                                   branch=self.test_config.ycsb_settings.branch,
                                   worker_home=self.worker_manager.WORKER_HOME,
                                   ycsb_instances=1)
        else:
            local.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                             branch=self.test_config.ycsb_settings.branch)

    def run_pull(self):
        load_docs = int(self.settings.syncgateway_settings.documents)
        pre_load_stats = self.post_delta_stats()
        pre_load_writes = pre_load_stats['pull_count']
        logger.info(f'initial writes: {pre_load_writes}')

        sgw_load_time = self.e2e_cb_load_bg(pre_load_writes, load_docs)

        post_load_stats = self.post_delta_stats()
        post_load_writes = post_load_stats['pull_count']
        logger.info(f'post load writes: {post_load_writes}')

        sgw_access_time = self.e2e_cb_update_bg(post_load_writes)

        post_access_stats = self.post_delta_stats()
        post_access_writes = post_access_stats['pull_count']
        logger.info(f'post access writes: {post_access_writes}')

        docs_accessed = post_access_writes - post_load_writes
        sgw_load_tp = load_docs / sgw_load_time
        sgw_access_tp = docs_accessed / sgw_access_time

        self.collect_execution_logs()
        self.print_ycsb_logs()

        self.report_kpi(sgw_load_tp, sgw_access_tp)
        local.cleanup_cblite_db_coninuous()

        if self.settings.syncgateway_settings.collect_sgw_logs:
            self.compress_sg_logs()
            self.get_sg_logs()

        if self.settings.syncgateway_settings.collect_sgw_console:
            self.get_sg_console()


class EndToEndSingleCBLBidiTest(EndToEndSingleCBLTest):

    def run_bidi(self):
        pass


class EndToEndMultiCBLTest(EndToEndTest):

    def get_cblite_info(self):
        for client in self.cluster_spec.workers[:int(self.settings.syncgateway_settings.clients)]:
            for instance_id in range(int(self.settings.syncgateway_settings.instances_per_client)):
                db_name = f'db_{instance_id}'
                port = 4985+instance_id
                info = self.rest.get_cblite_info(
                    client, port, db_name)
                logger.info(f'client: {client}, port: {port}, db: {db_name}, \n info: {info}')

    def push_monitor(self, queue, pre_load_writes, load_docs):
        try:
            sgw_load_time, observed_pushed = \
                self.wait_for_docs_pushed(
                    initial_docs=pre_load_writes,
                    target_docs=load_docs
                )
            queue.put((sgw_load_time, observed_pushed, "push"))
        except Exception as ex:
            logger.info(str(ex))
            queue.put((None, None, "push"))

    def pull_monitor(self, queue, pre_load_writes, load_docs):
        try:
            sgw_load_time, observed_pulled = \
                self.wait_for_docs_pulled(
                    initial_docs=pre_load_writes,
                    target_docs=load_docs
                )
            queue.put((sgw_load_time, observed_pulled, "pull"))
        except Exception as ex:
            logger.info(str(ex))
            queue.put((None, None, "pull"))

    @with_stats
    @with_profiles
    def push_load(self, pre_load_writes, load_docs):
        # settings = copy.deepcopy(self.settings)
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=self.push_monitor, args=(queue, pre_load_writes,
                                                                    load_docs))
        p.daemon = True
        p.start()
        try:
            self.run_sg_phase(
                phase="load phase",
                task=syncgateway_e2e_multi_cbl_task_load_docs,
                settings=self.settings,
                target_iterator=None,
                timer=self.settings.time,
                distribute=True,
                wait=False
            )
            ret = queue.get(timeout=900)
        except Exception as ex:
            logger.info(str(ex))
            p.terminate()
            p.join()
            self.get_cblite_info()
            self.collect_execution_logs()
            raise ex
        else:
            p.join()
            sgw_load_time = ret[0]
            observed_pushed = ret[1]
            if sgw_load_time and observed_pushed:
                return sgw_load_time, observed_pushed
            else:
                self.get_cblite_info()
                self.collect_execution_logs()
                raise Exception("failed to load docs")

    @with_stats
    @with_profiles
    def push_update(self, post_load_writes, load_docs):
        collections_count = len(self.collection_map.get('bucket-1', {'scope-1': {'_default'}})
                                ['scope-1'])
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=self.push_monitor, args=(queue, post_load_writes,
                                    int(load_docs) // collections_count))
        p.daemon = True
        p.start()
        try:
            self.run_sg_phase(
                phase="access phase",
                task=syncgateway_e2e_multi_cbl_task_run_test,
                settings=self.settings,
                target_iterator=None,
                timer=self.settings.time,
                distribute=True,
                wait=False
            )
            ret = queue.get(timeout=900)
        except Exception as ex:
            logger.info(str(ex))
            p.terminate()
            p.join()
            self.get_cblite_info()
            self.collect_execution_logs()
            raise ex
        else:
            p.join()
            sgw_load_time = ret[0]
            observed_pushed = ret[1]
            if sgw_load_time and observed_pushed:
                return sgw_load_time, observed_pushed
            else:
                self.get_cblite_info()
                self.collect_execution_logs()
                raise Exception("failed to update docs")

    @with_stats
    @with_profiles
    def pull_load(self, pre_load_writes, load_docs):
        # settings = copy.deepcopy(self.settings)
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=self.pull_monitor, args=(queue, pre_load_writes,
                                    load_docs))
        p.daemon = True
        p.start()
        try:
            PerfTest.load(self, task=ycsb_data_load_task)
            ret = queue.get(timeout=900)
        except Exception as ex:
            logger.info(str(ex))
            p.terminate()
            p.join()
            self.get_cblite_info()
            self.collect_execution_logs()
            raise ex
        else:
            p.join()
            sgw_load_time = ret[0]
            observed_pushed = ret[1]
            if sgw_load_time and observed_pushed:
                return sgw_load_time, observed_pushed
            else:
                self.get_cblite_info()
                self.collect_execution_logs()
                raise Exception("failed to load docs")

    def cloud_restore(self):
        self.remote.extract_cb_any(filename='couchbase',
                                   worker_home="/tmp")
        self.remote.cbbackupmgr_version(worker_home="/tmp")

        credential = local.read_aws_credential(
            self.test_config.backup_settings.aws_credential_path)
        self.remote.create_aws_credential(credential)
        self.remote.client_drop_caches()
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
            if self.capella_infra:
                suffix_repo = self.cluster_spec.capella_backend
            archive = archive + "/" + suffix_repo

        self.remote.restore(cluster_spec=self.cluster_spec,
                            master_node=self.master_node,
                            threads=self.test_config.restore_settings.threads,
                            worker_home="/tmp",
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

    @with_stats
    @with_profiles
    def pull_restore(self, pre_load_writes, load_docs):
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=self.pull_monitor, args=(queue, pre_load_writes,
                                    load_docs))
        p.daemon = True
        p.start()
        try:
            self.cloud_restore()
            ret = queue.get(timeout=900)
        except Exception as ex:
            logger.info(str(ex))
            p.terminate()
            p.join()
            self.get_cblite_info()
            self.collect_execution_logs()
            raise ex
        else:
            p.join()
            sgw_load_time = ret[0]
            observed_pushed = ret[1]
            if sgw_load_time and observed_pushed:
                return sgw_load_time, observed_pushed
            else:
                self.get_cblite_info()
                self.collect_execution_logs()
                raise Exception("failed to load docs")

    @with_stats
    @with_profiles
    def pull_update(self, post_load_writes, load_docs):
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=self.pull_monitor, args=(queue, post_load_writes,
                                    int(load_docs)))
        p.daemon = True
        p.start()
        try:
            PerfTest.access_bg(self, task=ycsb_task)
            ret = queue.get(timeout=900)
        except Exception as ex:
            logger.info(str(ex))
            p.terminate()
            p.join()
            self.get_cblite_info()
            self.collect_execution_logs()
            raise ex
        else:
            p.join()
            sgw_load_time = ret[0]
            observed_pushed = ret[1]
            if sgw_load_time and observed_pushed:
                return sgw_load_time, observed_pushed
            else:
                self.get_cblite_info()
                self.collect_execution_logs()
                raise Exception("failed to update docs")

    @with_stats
    @with_profiles
    def bidi_load(self, pre_load_writes, pre_load_reads, load_docs):
        settings = copy.deepcopy(self.settings)
        settings.syncgateway_settings.documents = load_docs // 2
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=self.push_monitor, args=(queue,
                                    pre_load_writes,
                                    int(load_docs
                                        * float(settings.syncgateway_settings.pushproportion))))
        p.daemon = True
        p.start()
        q = multiprocessing.Process(target=self.pull_monitor, args=(queue,
                                    pre_load_reads,
                                    int(load_docs
                                        * float(settings.syncgateway_settings.pullproportion))))
        q.daemon = True
        q.start()
        try:
            settings.syncgateway_settings.documents = \
                int(load_docs *
                    float(settings.syncgateway_settings.pushproportion))
            self.run_sg_phase(
                phase="load phase",
                task=syncgateway_e2e_multi_cbl_task_load_docs,
                settings=settings,
                timer=self.settings.time,
                target_iterator=None,
                distribute=True,
                wait=False
            )

            settings.syncgateway_settings.documents = \
                int(load_docs *
                    float(settings.syncgateway_settings.pullproportion))
            PerfTest.load(self, task=ycsb_data_load_task)
            ret1 = queue.get(timeout=900)
            ret2 = queue.get(timeout=900)
        except Exception as ex:
            logger.info(str(ex))
            p.terminate()
            p.join()
            q.terminate()
            q.join()
            self.get_cblite_info()
            self.collect_execution_logs()
            raise ex
        else:
            p.join()
            q.join()
            if ret1[2] == "push":
                sgw_push_load_time = ret1[0]
                sgw_push_load_docs = ret1[1]
                sgw_pull_load_time = ret2[0]
                sgw_pull_load_docs = ret2[1]
            else:
                sgw_pull_load_time = ret1[0]
                sgw_pull_load_docs = ret1[1]
                sgw_push_load_time = ret2[0]
                sgw_push_load_docs = ret2[1]
            if sgw_push_load_time and sgw_push_load_docs and sgw_pull_load_time and \
                    sgw_pull_load_docs:
                return sgw_push_load_time, sgw_push_load_docs, sgw_pull_load_time, \
                    sgw_pull_load_docs
            else:
                logger.info(f'PUSH time: {sgw_push_load_time}, PUSH docs: {sgw_push_load_docs}, \
                             PULL time: {sgw_pull_load_time}, PULL docs: {sgw_pull_load_docs}')
                self.get_cblite_info()
                self.collect_execution_logs()
                raise Exception("failed to load docs")

    @with_stats
    @with_profiles
    def bidi_update(self, post_load_writes, post_load_reads, load_docs):
        settings = copy.deepcopy(self.settings)
        settings.syncgateway_settings.documents = load_docs // 2
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=self.push_monitor, args=(queue,
                                    post_load_writes,
                                    int(load_docs
                                        * float(settings.syncgateway_settings.pushproportion))))
        p.daemon = True
        p.start()
        q = multiprocessing.Process(target=self.pull_monitor, args=(queue,
                                    post_load_reads,
                                    int(load_docs
                                        * float(settings.syncgateway_settings.pullproportion))))
        q.daemon = True
        q.start()
        try:
            settings.syncgateway_settings.documents = \
                int(load_docs *
                    float(settings.syncgateway_settings.pushproportion))
            self.run_sg_phase(
                phase="access phase",
                task=syncgateway_e2e_multi_cbl_task_run_test,
                settings=settings,
                timer=self.settings.time,
                target_iterator=None,
                distribute=True,
                wait=False
            )

            settings.syncgateway_settings.documents = \
                int(load_docs *
                    float(settings.syncgateway_settings.pullproportion))
            PerfTest.access_bg(self, task=ycsb_task)
            ret1 = queue.get(timeout=900)
            ret2 = queue.get(timeout=900)
        except Exception as ex:
            logger.info(str(ex))
            p.terminate()
            p.join()
            q.terminate()
            q.join()
            self.get_cblite_info()
            self.collect_execution_logs()
            raise ex
        else:
            p.join()
            q.join()
            if ret1[2] == "push":
                sgw_push_load_time = ret1[0]
                sgw_push_load_docs = ret1[1]
                sgw_pull_load_time = ret2[0]
                sgw_pull_load_docs = ret2[1]
            else:
                sgw_pull_load_time = ret1[0]
                sgw_pull_load_docs = ret1[1]
                sgw_push_load_time = ret2[0]
                sgw_push_load_docs = ret2[1]
            if sgw_push_load_time and sgw_push_load_docs and sgw_pull_load_time and \
                    sgw_pull_load_docs:
                return sgw_push_load_time, sgw_push_load_docs, sgw_pull_load_time, \
                    sgw_pull_load_docs
            else:
                logger.info(f'PUSH time: {sgw_push_load_time}, PUSH docs: {sgw_push_load_docs}, \
                             PULL time: {sgw_pull_load_time}, PULL docs: {sgw_pull_load_docs}')
                self.get_cblite_info()
                self.collect_execution_logs()
                raise Exception("failed to update docs")

    def start_multi_cblitedb_continuous(self):
        verbose = self.settings.syncgateway_settings.cbl_verbose_logging
        for instance_id in range(int(self.settings.syncgateway_settings.instances_per_client)):
            for client in self.cluster_spec.workers[:int(
                                                    self.settings.syncgateway_settings.clients)]:
                port = 4985 + instance_id
                db_name = f'db_{instance_id}'
                self.remote.start_cblitedb_continuous(client, db_name, port, verbose,
                                                      self.collection_map
                                                      .get('bucket-1', {}))

    def start_multi_continuous_replication(self):
        replication_type = self.test_config.syncgateway_settings.replication_type
        current_sgw = 0
        cblites_on_current_sgw = 0
        sgw_ip = self.cluster_spec.sgw_servers[current_sgw]

        logger.info(f'The sgw_ip is: {sgw_ip}')
        total_users = int(self.settings.syncgateway_settings.users)
        user_id = 0
        logger.info(f'The number of users is: {total_users}')
        instances_per_sgw = int(self.settings.syncgateway_settings.threads) \
            / int(self.settings.syncgateway_settings.nodes)
        if self.capella_infra:
            instances_per_sgw = int(self.settings.syncgateway_settings.threads)
        logger.info(f'Instances per sgw: {instances_per_sgw}')
        for instance_id in range(int(self.settings.syncgateway_settings.instances_per_client)):
            for client in self.cluster_spec.workers[:int(
                                                    self.settings.syncgateway_settings.clients)]:
                db_name = f'db_{instance_id}'
                user_num = user_id % total_users
                user_id += 1
                if self.settings.syncgateway_settings.replication_auth:
                    username = f'sg-user-{user_num}'
                    if total_users > 1:
                        if self.capella_infra:
                            password = "Password123!"
                        else:
                            password = "password"
                    else:
                        password = "guest"
                else:
                    username = None
                    password = None
                port = 4985 + instance_id
                cblites_on_current_sgw += 1
                logger.info(f'Cblites on current sgw: {cblites_on_current_sgw}')
                if cblites_on_current_sgw > instances_per_sgw and \
                        current_sgw < int(self.settings.syncgateway_settings.nodes) - 1:
                    cblites_on_current_sgw = 0
                    current_sgw += 1
                    sgw_ip = self.cluster_spec.sgw_servers[current_sgw]

                scopes = self.collection_map.get('bucket-1', None)
                # Use _default collection to start replication for non collection runs
                collections = ['_default._default']
                if scopes:
                    collections = []
                    for scope, coll in scopes.items():
                        if scope == '_default':
                            continue
                        for col_name, _ in coll.items():
                            collections.append(f'{scope}.{col_name}')

                if replication_type == 'E2E_PUSH':
                    sgw_port = 4900 if self.test_config.syncgateway_settings.troublemaker else 4984
                    self.rest.start_cblite_replication_push(
                        client, port, db_name, sgw_ip, sgw_port, username, password,
                        collections=collections)
                elif replication_type == 'E2E_PULL':
                    sgw_port = 4900 if self.test_config.syncgateway_settings.troublemaker else 4984
                    self.rest.start_cblite_replication_pull(
                        client, port, db_name, sgw_ip, sgw_port, username, password,
                        collections=collections)
                elif replication_type == 'E2E_BIDI':
                    sgw_port = 4900 if self.test_config.syncgateway_settings.troublemaker else 4984
                    self.rest.start_cblite_replication_bidi(
                        client, port, db_name, sgw_ip, sgw_port, username, password,
                        collections=collections)
                else:
                    raise Exception(
                        f'replication type must be either E2E_PUSH or E2E_PULL: {replication_type}'
                    )

    def setup_cblite(self):
        try:
            self.remote.create_cblite_directory()
        except Exception as ex:
            logger.info(ex)
        if self.settings.syncgateway_settings.ramdisk_size != 0:
            try:
                self.remote.create_cblite_ramdisk(self.settings.syncgateway_settings.ramdisk_size)
            except Exception as ex:
                logger.info(ex)
        try:
            self.remote.clone_cblite()
        except Exception as ex:
            logger.info(ex)
        self.remote.build_cblite()

    def run_push(self):
        pass

    def run_pull(self):
        pass

    def run_bidi(self):
        pass

    def run(self):
        try:
            self.remote.kill_cblite()
        except Exception as ex:
            logger.warn(ex)
        self.remote.modify_tcp_settings()
        self.download_ycsb()
        self.remote.build_syncgateway_ycsb(
            worker_home=self.worker_manager.WORKER_HOME,
            ycsb_instances=int(self.test_config.syncgateway_settings.instances_per_client))
        self.setup_cblite()
        if self.test_config.syncgateway_settings.e2e:
            sync_function = (
                'function (doc) {{ channel("channel-".concat((Date.now()) % {0}));}}'.
                format(self.sg_settings.channels)
            )
            collections_map = self.test_config.collection.collection_map
            if collections_map:
                logger.info("Update sync function for db with named collections")
                target_scope_collections = collections_map["bucket-1"]

                for scope, collections in target_scope_collections.items():
                    for collection, options in collections.items():
                        if options['load'] == 1 and options['access'] == 1:
                            logger.info(f"The scope is: {scope}")
                            logger.info(f"The collections is: {collection}")
                            keyspace = f"db-1.{scope}.{collection}"
                            logger.info(f"keyspace is: {keyspace}")
                            self.rest.sgw_update_sync_function(self.sgw_master_node, keyspace,
                                                               sync_function)
            else:
                logger.info("update sync function for default collection")
                self.rest.sgw_update_sync_function(self.sgw_master_node, "db-1", sync_function)
            logger.info("Updated sync function")
        self.start_memcached()
        self.load_users()
        self.init_users()
        self.grant_access()
        self.start_multi_cblitedb_continuous()
        logger.info("CBLites are started")
        if self.test_config.syncgateway_settings.troublemaker:
            logger.info("Troublemaker is building")
            self.remote.build_troublemaker()
            logger.info("Troublemaker is successfully built")
        self.start_multi_continuous_replication()
        logger.info("Replication started")
        replication_type = self.test_config.syncgateway_settings.replication_type
        if replication_type == 'E2E_PUSH':
            self.run_push()
        elif replication_type == 'E2E_PULL':
            self.run_pull()
        elif replication_type == "E2E_BIDI":
            self.run_bidi()
        else:
            raise Exception(
                "Replication type must be "
                "E2E_PUSH, E2E_PULL or E2E_BIDI: "
                f"{replication_type}"
            )


class EndToEndMultiCBLPushTest(EndToEndMultiCBLTest):

    def run_push(self):
        logger.info('Loading Docs')
        load_docs = int(self.settings.syncgateway_settings.documents)
        pre_load_stats = self.post_delta_stats()
        pre_load_writes = pre_load_stats['push_count']
        logger.info(f'initial pushed: {pre_load_writes}')

        sgw_load_time, observed_pushed_load = self.push_load(pre_load_writes, load_docs)
        sgw_load_tp = observed_pushed_load / sgw_load_time

        post_load_stats = self.post_delta_stats()
        post_load_writes = post_load_stats['push_count']
        logger.info(f'post load pushed: {post_load_writes}')

        try:
            sgw_access_time, observed_pushed_access = self.push_update(post_load_writes, load_docs)

            post_access_stats = self.post_delta_stats()
            post_access_writes = post_access_stats['push_count']
            logger.info(f'post access pushed: {post_access_writes}')

            sgw_access_tp = observed_pushed_access / sgw_access_time
        except Exception as ex:
            logger.warn(f'PUSH access phase failed: {ex}')
            sgw_access_tp = 0

        self.collect_execution_logs()
        self.report_kpi(sgw_load_tp, sgw_access_tp)


class EndToEndMultiCBLPullTest(EndToEndMultiCBLTest):

    def run_pull(self):
        logger.info('Loading Docs')
        load_docs = int(self.settings.syncgateway_settings.documents)
        pre_load_stats = self.post_delta_stats()
        pre_load_reads = pre_load_stats['pull_count']
        logger.info(f'initial pulled: {pre_load_reads}')

        sgw_load_time, observed_pulled_load = self.pull_load(pre_load_reads, load_docs)
        sgw_load_tp = observed_pulled_load / sgw_load_time

        post_load_stats = self.post_delta_stats()
        post_load_reads = post_load_stats['pull_count']
        logger.info(f'post load pulled: {post_load_reads}')

        try:
            sgw_access_time, observed_pulled_access = self.pull_update(post_load_reads, load_docs)

            post_access_stats = self.post_delta_stats()
            post_access_reads = post_access_stats['pull_count']
            logger.info(f'post access pulled: {post_access_reads}')

            sgw_access_tp = observed_pulled_access / sgw_access_time
        except Exception as ex:
            logger.warn(f'PULL access phase failed: {ex}')
            sgw_access_tp = 0

        self.collect_execution_logs()
        self.report_kpi(sgw_load_tp, sgw_access_tp)


class EndToEndMultiCBLPullRestoreTest(EndToEndMultiCBLTest):

    def run_pull(self):
        logger.info("Loading Docs Via Restore")
        load_docs = int(self.settings.syncgateway_settings.documents)
        pre_load_stats = self.post_delta_stats()
        pre_load_reads = pre_load_stats['pull_count']
        logger.info("initial pulled: {}".format(pre_load_reads))

        sgw_load_time, observed_pulled_load = self.pull_restore(pre_load_reads, load_docs)
        sgw_load_tp = observed_pulled_load / sgw_load_time

        post_load_stats = self.post_delta_stats()
        post_load_reads = post_load_stats['pull_count']
        logger.info("post load pulled: {}".format(post_load_reads))

        self.collect_execution_logs()
        self.report_kpi(sgw_load_tp, 0)


class EndToEndMultiCBLBidiTest(EndToEndMultiCBLTest):

    def _report_kpi(self, sgw_load_tp: int, operation: str):
        if sgw_load_tp <= 0:
            return
        self.reporter.post(
            *self.metrics.sgw_e2e_throughput(
                throughput=sgw_load_tp,
                operation=operation,
                replication=self.test_config.syncgateway_settings.replication_type
            )
        )
        self.reporter.post(
            *self.metrics.sgw_e2e_throughput_per_cblite(
                throughput=sgw_load_tp / float(self.settings.syncgateway_settings.threads),
                operation=operation,
                replication=self.test_config.syncgateway_settings.replication_type
            )
        )
        if self.sg_settings.mem_cpu_stats:
            self.reporter.post(
                *self.metrics.avg_sg_cpu_usage("Average SGW CPU usage (number vCPUs)")
            )
            self.reporter.post(
                *self.metrics.avg_sg_mem_usage("Average SGW Memory usage (MB)")
            )

    def run_bidi(self):
        load_docs = int(self.settings.syncgateway_settings.documents)
        pre_load_stats = self.post_delta_stats()

        pre_load_writes = pre_load_stats['push_count']
        logger.info(f'initial pushed: {pre_load_writes}')

        pre_load_reads = pre_load_stats['pull_count']
        logger.info(f'initial pulled: {pre_load_reads}')

        try:
            sgw_push_load_time, sgw_push_load_docs, sgw_pull_load_time, sgw_pull_load_docs = \
                self.bidi_load(pre_load_writes, pre_load_reads, load_docs)
            sgw_push_load_tp = sgw_push_load_docs / sgw_push_load_time
            sgw_pull_load_tp = sgw_pull_load_docs / sgw_pull_load_time

            self.report_kpi(sgw_push_load_tp, "PUSH_INSERT")
            self.report_kpi(sgw_pull_load_tp, "PULL_INSERT")
        except Exception as ex:
            logger.warn(f'BIDI load phase failed: {ex}')

        post_load_stats = self.post_delta_stats()
        post_load_writes = post_load_stats['push_count']
        logger.info(f'post load pushed: {post_load_writes}')
        post_load_reads = post_load_stats['pull_count']
        logger.info(f'post load pulled: {post_load_reads}')

        try:
            sgw_push_access_time, sgw_push_access_docs, \
                sgw_pull_access_time, sgw_pull_access_docs = \
                self.bidi_update(post_load_writes, post_load_reads, load_docs)
            sgw_push_access_tp = sgw_push_access_docs / sgw_push_access_time
            sgw_pull_access_tp = sgw_pull_access_docs / sgw_pull_access_time

            post_access_stats = self.post_delta_stats()
            post_access_writes = post_access_stats['push_count']
            logger.info(f'post access pushed: {post_access_writes}')
            post_access_reads = post_access_stats['pull_count']
            logger.info(f'post access pulled: {post_access_reads}')
            self.report_kpi(sgw_push_access_tp, "PUSH_UPDATE")
            self.report_kpi(sgw_pull_access_tp, "PULL_UPDATE")
        except Exception as ex:
            logger.warn(f'BIDI access phase failed: {ex}')

        self.collect_execution_logs()


class LogStreamingTest(SGWrite):

    def access_with_log_streaming(self, collector: str):
        self.enable_log_streaming(collector)
        self.monitor.wait_sgw_log_streaming_status('enabled')
        logger.info('Running access phase with log-streaming enabled')
        self.run_test()
        self.disable_log_streaming()
        self.monitor.wait_sgw_log_streaming_status('disabled')

    def run(self):
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.load_docs()
        self.init_users()
        # Run without log-streaming
        logger.info('Running access phase without log-streaming')
        self.run_test()
        self.report_kpi()

        # Run with log streaming
        if collector := self.settings.syncgateway_settings.log_streaming:
            self.access_with_log_streaming(collector)
            self.report_kpi()

    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob(f'{self.LOCAL_DIR}/*runtest*.result'):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec),")
        )


class ResyncTest(SGImportThroughputTest):

    def run(self):
        self.download_ycsb()
        self.build_ycsb(ycsb_client=self.test_config.load_settings.ycsb_client)
        self.load()
        self.monitor_sg_import_multinode(phase="load", timeout=3600, sleep_delay=30)

        self.rest.sgw_update_sync_function(
            self.sgw_master_node, "db-1", self.test_config.syncgateway_settings.resync_new_function
        )

        self.remote.restart_syncgateway()
        self.rest.sgw_set_db_offline(self.sgw_master_node, "db-1")
        self.resync_time = self.resync()
        self.rest.sgw_set_db_online(self.sgw_master_node, "db-1")

        self.collect_execution_logs()
        self.report_kpi()

    @timeit
    @with_profiles
    @with_stats
    def resync(self):
        self.rest.sgw_start_resync(self.sgw_master_node, "db-1")
        self.monitor.monitor_sgw_resync_status(self.sgw_master_node, "db-1")

    def _report_kpi(self):
        self.reporter.post(*self.metrics.elapsed_time(self.resync_time))

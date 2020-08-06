import glob
import os
import shutil
from time import time
from typing import Callable

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.cluster import ClusterManager
from perfrunner.helpers.memcached import MemcachedHelper
from perfrunner.helpers.metrics import MetricHelper
from perfrunner.helpers.misc import pretty_dict, target_hash
from perfrunner.helpers.monitor import Monitor
from perfrunner.helpers.profiler import Profiler, with_profiles
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.reporter import ShowFastReporter
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.worker import (
    WorkerManager,
    pillowfight_data_load_task,
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


class SGPerfTest(PerfTest):

    COLLECTORS = {'disk': False, 'ns_server': False, 'ns_server_overview': False,
                  'active_tasks': False, 'syncgateway_stats': True}

    ALL_HOSTNAMES = True
    LOCAL_DIR = "YCSB"

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.memcached = MemcachedHelper(test_config)
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)
        self.rest = RestHelper(cluster_spec)
        # self.build = os.environ.get('SGBUILD') or "0.0.0-000"
        self.master_node = next(cluster_spec.masters)
        self.build = self.rest.get_sgversion(self.master_node)
        self.metrics = MetricHelper(self)
        self.reporter = ShowFastReporter(cluster_spec, test_config, self.build)
        if self.test_config.test_case.use_workers:
            self.worker_manager = WorkerManager(cluster_spec, test_config, verbose)
        self.settings = self.test_config.access_settings
        self.settings.syncgateway_settings = self.test_config.syncgateway_settings
        self.profiler = Profiler(cluster_spec, test_config)
        self.cluster = ClusterManager(cluster_spec, test_config)
        self.target_iterator = TargetIterator(cluster_spec, test_config)
        self.monitor = Monitor(cluster_spec, test_config, verbose)
        self.sg_settings = self.test_config.syncgateway_settings

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

    def collect_execution_logs(self):
        if self.worker_manager.is_remote:
            if os.path.exists(self.LOCAL_DIR):
                shutil.rmtree(self.LOCAL_DIR, ignore_errors=True)
            os.makedirs(self.LOCAL_DIR)
            self.remote.get_syncgateway_ycsb_logs(self.worker_manager.WORKER_HOME,
                                                  self.test_config.syncgateway_settings,
                                                  self.LOCAL_DIR)

    def run_sg_phase(self,
                     phase: str,
                     task: Callable, settings: PhaseSettings,
                     timer: int = None,
                     distribute: bool = False) -> None:
        logger.info('Running {}: {}'.format(phase, pretty_dict(settings)))
        self.worker_manager.run_sg_tasks(task, settings, timer, distribute, phase)
        self.worker_manager.wait_for_workers()

    def start_memcached(self):
        self.run_sg_phase("start memcached", syncgateway_task_start_memcached,
                          self.settings, self.settings.time, False)

    def load_users(self):
        self.run_sg_phase("load users", syncgateway_task_load_users,
                          self.settings, self.settings.time, False)

    def init_users(self):
        if self.test_config.syncgateway_settings.auth == 'true':
            self.run_sg_phase("init users", syncgateway_task_init_users,
                              self.settings, self.settings.time, False)

    def grant_access(self):
        if self.test_config.syncgateway_settings.grant_access == 'true':
            self.run_sg_phase("grant access to  users", syncgateway_task_grant_access,
                              self.settings, self.settings.time, False)

    def load_docs(self):
        self.run_sg_phase("load docs", syncgateway_task_load_docs,
                          self.settings, self.settings.time, False)

    @with_stats
    @with_profiles
    def run_test(self):
        self.run_sg_phase("run test", syncgateway_task_run_test,
                          self.settings, self.settings.time, True)

    def compress_sg_logs(self):
        self.remote.compress_sg_logs()

    def get_sg_logs(self):
        initial_nodes = int(self.test_config.syncgateway_settings.nodes)
        ssh_user, ssh_pass = self.cluster_spec.ssh_credentials
        for _server in range(initial_nodes):
            server = self.cluster_spec.servers[_server]
            local.get_sg_logs(host=server, ssh_user=ssh_user, ssh_pass=ssh_pass)

    def channel_list(self, number_of_channels: int):
        channels = []
        for number in range(1, number_of_channels + 1):
            channel = "channel-" + str(number)
            channels.append(channel)
        return channels

    def run(self):
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.load_docs()
        self.init_users()
        self.grant_access()
        self.run_test()
        self.report_kpi()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.test_config.test_case.use_workers:
            self.worker_manager.download_celery_logs()
            self.worker_manager.terminate()

        if self.test_config.cluster.online_cores:
            self.remote.enable_cpu()

        if self.test_config.cluster.kernel_mem_limit:
            self.remote.reset_memory_settings()
            self.monitor.wait_for_servers()


class SGRead(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), GET doc by id")
        )


class SGAuthThroughput(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), POST auth")
        )


class SGAuthLatency(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
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
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
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


class SGSyncQueryThroughput(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput('Throughput (req/sec), GET docs via _changes')
        )


class SGSyncQueryLatency(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
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
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec), POST doc")
        )


class SGMixQueryThroughput(SGPerfTest):
    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
            with open(f, 'r') as fout:
                logger.info(f)
                logger.info(fout.read())

        self.reporter.post(
            *self.metrics.sg_throughput("Throughput (req/sec)")
        )


class SGTargetIterator(TargetIterator):

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        src_master = next(self.cluster_spec.masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(src_master, bucket)
            yield TargetSettings(src_master, bucket, password, prefix)


class CBTargetIterator(TargetIterator):

    def __iter__(self):
        password = self.test_config.bucket.password
        prefix = self.prefix
        # masters = self.cluster_spec.masters
        cb_master = self.cluster_spec.servers[3]
        # src_master = next(masters)
        # dest_master = next(masters)
        for bucket in self.test_config.buckets:
            if self.prefix is None:
                prefix = target_hash(cb_master, bucket)
            yield TargetSettings(cb_master, bucket, password, prefix)


class SGImportLoad(PerfTest):

    def load(self, *args):
        PerfTest.load(self, task=pillowfight_data_load_task)

    def run(self):
        self.remote.remove_sglogs()
        self.load()


class SGImportThroughputTest(SGPerfTest):

    def load(self, *args):
        cb_target_iterator = CBTargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='symmetric')
        super().load(task=pillowfight_data_load_task, target_iterator=cb_target_iterator)

    COLLECTORS = {'disk': False, 'ns_server': False, 'ns_server_overview': False,
                  'active_tasks': False, 'syncgateway_stats': True}

    def _report_kpi(self, time_elapsed, items_in_range):
        self.reporter.post(
            *self.metrics.sgimport_items_per_sec(time_elapsed=time_elapsed,
                                                 items_in_range=items_in_range)
        )

    @with_stats
    @with_profiles
    def monitor_sg_import(self):
        host = self.cluster_spec.servers[0]
        expected_docs = self.test_config.load_settings.items
        time_elapsed, items_in_range = self.monitor.monitor_sgimport_queues(host, expected_docs)
        return time_elapsed, items_in_range

    def initial_import_count(self):
        total_initial_docs = 0
        for i in range(self.test_config.syncgateway_settings.import_nodes):
            server = self.cluster_spec.servers[i]
            import_count = self.monitor.get_import_count(host=server)
            total_initial_docs += import_count
        return total_initial_docs

    @with_stats
    @with_profiles
    def monitor_sg_import_multinode(self):
        expected_docs = self.test_config.load_settings.items
        print("expected docs :", expected_docs)

        initial_docs = self.initial_import_count()
        print("initial docs imported :", initial_docs)

        remaining_docs = expected_docs - initial_docs
        print("remaining_docs : ", remaining_docs)

        importing = True

        start_time = time()

        while importing:
            total_count = 0
            for i in range(self.test_config.syncgateway_settings.import_nodes):
                server = self.cluster_spec.servers[i]
                import_count = self.monitor.get_import_count(host=server)
                print('import count : {} , host : {}'.format(import_count, server))
                total_count += import_count
            if total_count >= expected_docs:
                importing = False

        end_time = time()

        time_elapsed = end_time - start_time

        return time_elapsed, remaining_docs

    def run(self):
        self.remote.remove_sglogs()

        self.load()

        if self.test_config.syncgateway_settings.import_nodes > 1:
            time_elapsed, items_in_range = self.monitor_sg_import_multinode()
        else:
            time_elapsed, items_in_range = self.monitor_sg_import()

        self.report_kpi(time_elapsed, items_in_range)


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
        self.reporter.post(
            *self.metrics.sgimport_latency()
        )

    def monitor_sg_import(self):
        host = self.cluster_spec.servers[0]
        expected_docs = self.test_config.load_settings.items + 360
        self.monitor.monitor_sgimport_queues(host, expected_docs)

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
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.load()
        self.report_kpi()


class SGSyncByUserWithAuth(SGSync):

    def _report_kpi(self):
        self.collect_execution_logs()
        for f in glob.glob('{}/*runtest*.result'.format(self.LOCAL_DIR)):
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
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_users()
        self.init_users()
        self.run_test()
        self.report_kpi()


class SGSyncByKeyNoAuth(SGSyncByUserWithAuth):

    def run(self):
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.run_test()
        self.report_kpi()


class SGSyncInitialLoad(SGSyncByUserWithAuth):

    def run(self):
        self.remote.remove_sglogs()
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
        sg1 = 'http://{}:4985/db'.format(sg1_master)
        sg2 = 'http://{}:4985/db'.format(sg2_master)

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
            "changes_feed_limit": 10000
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr1_pull"
            self.rest.start_sg_replication(sg2_master, data)

    @with_stats
    @with_profiles
    def monitor_sg_replicate(self, replication_id, sg_master):
        expected_docs = self.test_config.load_settings.items
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(sg_master,
                                                                        expected_docs,
                                                                        replication_id,
                                                                        1)
        return time_elapsed, items_in_range

    def run(self):
        masters = self.cluster_spec.masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == 'push':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr1_push", sg1_master)
        elif self.sg_settings.sg_replication_type == 'pull':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr1_pull", sg2_master)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputMultiChannelTest1(SGReplicateThroughputTest1):

    def start_replication(self, sg1_master, sg2_master):
        sg1 = 'http://{}:4985/db'.format(sg1_master)
        sg2 = 'http://{}:4985/db'.format(sg2_master)

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
            "changes_feed_limit": 10000
        }
        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr1_pull"
            self.rest.start_sg_replication(sg2_master, data)

    def run(self):
        masters = self.cluster_spec.masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == 'push':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr1_push", sg1_master)
        elif self.sg_settings.sg_replication_type == 'pull':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr1_pull", sg2_master)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputTest2(SGPerfTest):

    def _report_kpi(self, time_elapsed, items_in_range):
        self.reporter.post(
            *self.metrics.sgreplicate_items_per_sec(time_elapsed=time_elapsed,
                                                    items_in_range=items_in_range)
        )

    def start_replication(self, sg1_master, sg2_master):
        sg1 = 'http://{}:4985/db'.format(sg1_master)
        sg2 = 'http://{}:4985/db'.format(sg2_master)

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr2_push",
            "remote": sg2,
            "direction": "push",
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True
        }
        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication2(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr2_pull"
            data["direction"] = "pull"
            data["remote"] = sg1
            self.rest.start_sg_replication2(sg2_master, data)

    @with_stats
    @with_profiles
    def monitor_sg_replicate(self, replication_id, sg_master):
        expected_docs = self.test_config.load_settings.items
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(sg_master,
                                                                        expected_docs,
                                                                        replication_id,
                                                                        2)
        return time_elapsed, items_in_range

    def run(self):
        masters = self.cluster_spec.masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == 'push':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr2_push", sg1_master)
        elif self.sg_settings.sg_replication_type == 'pull':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr2_pull", sg2_master)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputMultiChannelTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master):
        sg1 = 'http://{}:4985/db'.format(sg1_master)
        sg2 = 'http://{}:4985/db'.format(sg2_master)

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr2_push",
            "remote": sg2,
            "direction": "push",
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True
        }
        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication2(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["replication_id"] = "sgr2_pull"
            data["direction"] = "pull"
            data["remote"] = sg1
            self.rest.start_sg_replication2(sg2_master, data)

    def run(self):
        masters = self.cluster_spec.masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        self.start_replication(sg1_master, sg2_master)
        if self.sg_settings.sg_replication_type == 'push':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr2_push", sg1_master)
        elif self.sg_settings.sg_replication_type == 'pull':
            time_elapsed, items_in_range = self.monitor_sg_replicate("sgr2_pull", sg2_master)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputConflictResolutionTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master):
        sg2 = 'http://{}:4985/db'.format(sg2_master)

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
        }

        if self.sg_settings.sg_conflict_resolution == 'custom':
            data["conflict_resolution_type"] = "custom"
            data["custom_conflict_resolver"] = \
                "function(conflict) { return defaultPolicy(conflict);}"

        self.rest.start_sg_replication2(sg1_master, data)

    @with_stats
    @with_profiles
    def monitor_sg_replicate(self, replication_id, sg_master):
        expected_docs = self.test_config.load_settings.items * 2
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(sg_master,
                                                                        expected_docs,
                                                                        replication_id,
                                                                        2)
        return time_elapsed, items_in_range

    def run(self):
        masters = self.cluster_spec.masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.start_replication(sg1_master, sg2_master)
        time_elapsed, items_in_range = \
            self.monitor_sg_replicate("sgr2_conflict_resolution", sg1_master)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateLoad(SGPerfTest):

    def run(self):
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()


class SGReplicateThroughputBidirectionalTest1(SGReplicateThroughputTest1):

    def start_replication(self, sg1_master, sg2_master):
        sg1 = 'http://{}:4985/db'.format(sg1_master)
        sg2 = 'http://{}:4985/db'.format(sg2_master)

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
            "changes_feed_limit": 10000
        }
        self.rest.start_sg_replication(sg1_master, data)

        data["replication_id"] = "sgr1_pull"
        data["source"] = sg2
        data["target"] = sg1
        self.rest.start_sg_replication(sg1_master, data)

    @with_stats
    @with_profiles
    def monitor_sg_replicate(self, replication_id, sg_master):
        expected_docs = self.test_config.load_settings.items * 2
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(sg_master,
                                                                        expected_docs,
                                                                        replication_id,
                                                                        1)
        return time_elapsed, items_in_range

    def run(self):
        masters = self.cluster_spec.masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.start_replication(sg1_master, sg2_master)
        time_elapsed, items_in_range = self.monitor_sg_replicate("sgr1_pushAndPull", sg1_master)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputBidirectionalTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master):
        sg2 = 'http://{}:4985/db'.format(sg2_master)

        channels = self.channel_list(int(self.sg_settings.channels))

        data = {
            "replication_id": "sgr2_pushAndPull",
            "remote": sg2,
            "direction": "pushAndPull",
            "filter": "sync_gateway/bychannel",
            "query_params": {
                "channels": channels
            },
            "continuous": True
        }
        self.rest.start_sg_replication2(sg1_master, data)

    @with_stats
    @with_profiles
    def monitor_sg_replicate(self, replication_id, sg_master):
        expected_docs = self.test_config.load_settings.items * 2
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(sg_master,
                                                                        expected_docs,
                                                                        replication_id,
                                                                        2)
        return time_elapsed, items_in_range

    def run(self):
        masters = self.cluster_spec.masters
        sg1_master = next(masters)
        sg2_master = next(masters)
        self.start_replication(sg1_master, sg2_master)
        time_elapsed, items_in_range = self.monitor_sg_replicate("sgr2_pushAndPull", sg1_master)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputMultiChannelMultiSgTest1(SGReplicateThroughputTest1):

    def start_replication(self, sg1_master, sg2_master, channel):
        sg1 = 'http://{}:4985/db'.format(sg1_master)
        sg2 = 'http://{}:4985/db'.format(sg2_master)

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
            "changes_feed_limit": 10000
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            self.rest.start_sg_replication(sg2_master, data)
        return data["replication_id"]

    @with_stats
    @with_profiles
    def monitor_sg_replicate(self, replication_id, sg_master):
        expected_docs = self.test_config.load_settings.items
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(sg_master,
                                                                        expected_docs,
                                                                        replication_id,
                                                                        1)
        return time_elapsed, items_in_range

    def run(self):
        sg1_node = []
        sg2_node = []
        replication_ids = []
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        for node in range(int(self.sg_settings.nodes)):
            sg1 = self.cluster_spec.servers[node]
            nodes_per_cluster = int(self.sg_settings.nodes) + \
                int(self.test_config.cluster.initial_nodes[0])
            sg2 = self.cluster_spec.servers[node+nodes_per_cluster]
            replication_id = self.start_replication(sg1, sg2, node)
            sg1_node.append(sg1)
            sg2_node.append(sg2)
            replication_ids.append(replication_id)
        if self.sg_settings.sg_replication_type == 'push':
            time_elapsed, items_in_range = self.monitor_sg_replicate(replication_ids, sg1_node)
        elif self.sg_settings.sg_replication_type == 'pull':
            time_elapsed, items_in_range = self.monitor_sg_replicate(replication_ids, sg2_node)
        self.report_kpi(time_elapsed, items_in_range)


class SGReplicateThroughputMultiChannelMultiSgTest2(SGReplicateThroughputTest2):

    def start_replication(self, sg1_master, sg2_master, channel):
        sg1 = 'http://{}:4985/db'.format(sg1_master)
        sg2 = 'http://{}:4985/db'.format(sg2_master)

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
            "continuous": True
        }

        if self.sg_settings.sg_replication_type == 'push':
            self.rest.start_sg_replication2(sg1_master, data)
        elif self.sg_settings.sg_replication_type == 'pull':
            data["direction"] = "pull"
            data["remote"] = sg1
            self.rest.start_sg_replication2(sg2_master, data)
        return data["replication_id"]

    @with_stats
    @with_profiles
    def monitor_sg_replicate(self, replication_id, sg_master):
        expected_docs = self.test_config.load_settings.items
        time_elapsed, items_in_range = self.monitor.monitor_sgreplicate(sg_master,
                                                                        expected_docs,
                                                                        replication_id,
                                                                        2)
        return time_elapsed, items_in_range

    def run(self):
        sg1_nodes = []
        sg2_nodes = []
        replication_ids = []
        self.remote.remove_sglogs()
        self.download_ycsb()
        self.start_memcached()
        self.load_docs()
        for node in range(int(self.sg_settings.nodes)):
            sg1 = self.cluster_spec.servers[node]
            nodes_per_cluster = int(self.sg_settings.nodes) + \
                int(self.test_config.cluster.initial_nodes[0])
            sg2 = self.cluster_spec.servers[node+nodes_per_cluster]
            replication_id = self.start_replication(sg1, sg2, node)
            sg1_nodes.append(sg1)
            sg2_nodes.append(sg2)
            replication_ids.append(replication_id)
        if self.sg_settings.sg_replication_type == 'push':
            time_elapsed, items_in_range = self.monitor_sg_replicate(replication_ids, sg1_nodes)
        elif self.sg_settings.sg_replication_type == 'pull':
            time_elapsed, items_in_range = self.monitor_sg_replicate(replication_ids, sg2_nodes)
        self.report_kpi(time_elapsed, items_in_range)

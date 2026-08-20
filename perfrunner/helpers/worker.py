import functools
import os
import socket
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import suppress
from itertools import cycle
from multiprocessing import set_start_method
from pathlib import Path
from typing import Optional

import psutil
from celery import Celery, Task, group
from celery.canvas import Signature
from celery.result import AsyncResult
from kombu.serialization import registry
from sqlalchemy import create_engine

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.config_files import CAOWorkerFile
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import (
    CH2,
    CH2ConnectionSettings,
    ClusterSpec,
    PhaseSettings,
    TargetSettings,
    TestConfig,
)
from perfrunner.workloads import spring_workload
from perfrunner.workloads.ai_bench import run_aibench_task
from perfrunner.workloads.analytics.custom.driver import (
    CustomAnalyticsQuery,
    run_custom_analytics_query_task,
)
from perfrunner.workloads.blackholepuller import (
    blackholepuller_runtest,
    newdocpusher_runtest,
)
from perfrunner.workloads.dcp import java_dcp_client, run_dcpdrain
from perfrunner.workloads.jts import jts_run, jts_warmup
from perfrunner.workloads.pillowfight import (
    pillowfight_data_load,
    pillowfight_workload,
)
from perfrunner.workloads.sdks_bench import sdks_benchmark_workload
from perfrunner.workloads.syncgateway import (
    syncgateway_delta_sync_load_docs,
    syncgateway_delta_sync_run_test,
    syncgateway_e2e_cbl_load_docs,
    syncgateway_e2e_cbl_run_test,
    syncgateway_e2e_multi_cb_load_docs,
    syncgateway_e2e_multi_cb_run_test,
    syncgateway_e2e_multi_cbl_load_docs,
    syncgateway_e2e_multi_cbl_run_test,
    syncgateway_grant_access,
    syncgateway_init_users,
    syncgateway_load_docs,
    syncgateway_load_users,
    syncgateway_run_test,
    syncgateway_start_memcached,
    syncgateway_warmup_cache,
)
from perfrunner.workloads.tpcds import (
    tpcds_initial_data_load,
    tpcds_remaining_data_load,
)
from perfrunner.workloads.vectordb_bench import run_vectordb_bench_case
from perfrunner.workloads.xdcr_conflict_sim import run_conflictsim
from perfrunner.workloads.ycsb import (
    ycsb_data_load,
    ycsb_mongo_data_load,
    ycsb_mongo_workload,
    ycsb_workload,
)

try:
    set_start_method("fork")
except Exception as ex:
    print(ex)


COMMON_CELERY_CONFIG = dict(
    task_serializer="pickle",
    result_serializer="pickle",
    accept_content=["pickle", "json", "application/json", "application/data", "application/text"],
    task_protocol=2,
)

LOCAL_BROKER_DB = "perfrunner.db"
LOCAL_RESULTS_DB = "results.db"

LOCAL_CELERY_CONFIG = dict(
    **COMMON_CELERY_CONFIG,
    broker_url=f"sqla+sqlite:///{LOCAL_BROKER_DB}",
    result_backend="database",
    database_url=f"sqlite:///{LOCAL_RESULTS_DB}",
)

REMOTE_CELERY_CONFIG = dict(
    **COMMON_CELERY_CONFIG,
    broker_pool_limit=None,
    worker_hijack_root_logger=False,
    result_backend="rpc://",
    result_persistent=False,
    result_exchange="perf_results",
    broker_connection_retry=True,
)

ON_PREM_BROKER_URL = "amqp://couchbase:couchbase@172.23.96.202:5672/broker"


celery = Celery('workers')

try:
    registry.enable('json')
    registry.enable('application/json')
    registry.enable('application/data')
    registry.enable('application/text')
except Exception as ex:
    print(ex)

if "env/bin/perfrunner" not in sys.argv and "env/bin/nostests" not in sys.argv:
    # configure workers that are started using `env/bin/celery worker`
    worker_type = os.getenv("WORKER_TYPE")
    if worker_type == "local":
        celery.conf.update(LOCAL_CELERY_CONFIG)
    elif worker_type == "remote":
        celery.conf.update(
            REMOTE_CELERY_CONFIG,
            broker_url=os.getenv("BROKER_URL"),
            broker_connection_timeout=30,
            broker_connection_max_retries=10,
        )
    else:
        raise Exception(f"Invalid worker type: {worker_type}")

TASK_PIDFILE_DIR = "celery_task_pids"


def store_pid(func: Callable):
    """Define a decorator to store the PID of a celery task in a file.

    The wrapper function requires the celery task instance as the first argument, meaning this
    decorator must be used on a bound celery task:
    ```
    @celery.task(bind=True)  # creates a bound task
    @store_pid
    def my_task(*args):
        do_something(*args)
    ```
    """

    @functools.wraps(func)
    def wrapper(self: Task, *args, **kwargs):
        pid = os.getpid()
        pidfile = Path(f"{TASK_PIDFILE_DIR}/{self.request.id}.pid")
        pidfile.parent.mkdir(parents=True, exist_ok=True)
        with open(pidfile, "w") as f:
            f.write(str(pid))
        try:
            return func(*args, **kwargs)
        finally:
            # Drop the pidfile once we're done, so that the PID of a finished task can't be
            # recycled by an unrelated process and then signalled by mistake
            pidfile.unlink(missing_ok=True)

    return wrapper


@celery.task(bind=True)
@store_pid
def spring_task(*args):
    spring_workload(*args)


@celery.task(bind=True)
@store_pid
def pillowfight_data_load_task(*args):
    pillowfight_data_load(*args)


@celery.task(bind=True)
@store_pid
def pillowfight_task(*args):
    pillowfight_workload(*args)


@celery.task(bind=True)
@store_pid
def ycsb_data_load_task(*args):
    ycsb_data_load(*args)


@celery.task(bind=True)
@store_pid
def ycsb_mongo_data_load_task(*args):
    ycsb_mongo_data_load(*args)


@celery.task(bind=True)
@store_pid
def ycsb_mongo_workload_task(*args):
    ycsb_mongo_workload(*args)


@celery.task(bind=True)
@store_pid
def ycsb_task(*args):
    ycsb_workload(*args)


@celery.task(bind=True)
@store_pid
def jts_run_task(*args):
    jts_run(*args)


@celery.task(bind=True)
@store_pid
def jts_warmup_task(*args):
    jts_warmup(*args)


@celery.task(bind=True)
@store_pid
def tpcds_initial_data_load_task(*args):
    tpcds_initial_data_load(*args)


@celery.task(bind=True)
@store_pid
def tpcds_remaining_data_load_task(*args):
    tpcds_remaining_data_load(*args)


@celery.task(bind=True)
@store_pid
def java_dcp_client_task(*args):
    java_dcp_client(*args)


@celery.task(bind=True)
@store_pid
def dcpdrain_task(workload_settings, target, timer=None, instance: int = 0):
    """
    Celery wrapper for run_dcpdrain.

    WorkloadPhase will call task.si(workload_settings, target, timer, instance).
    `workload_settings` and `target` are objects (PhaseSettings/TargetSettings).
    Celery in this repo is already configured to handle these task arguments
    (same as other workload tasks).
    """
    # Call implementation in workloads/dcp.py and return its result.
    return run_dcpdrain(workload_settings, target, timer, instance)


@celery.task(bind=True)
@store_pid
def run_conflictsim_task(*args):
    run_conflictsim(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_task_load_users(*args):
    syncgateway_load_users(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_task_init_users(*args):
    syncgateway_init_users(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_task_grant_access(*args):
    syncgateway_grant_access(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_task_warmup_cache(*args):
    syncgateway_warmup_cache(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_task_load_docs(*args):
    syncgateway_load_docs(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_task_run_test(*args):
    syncgateway_run_test(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_task_start_memcached(*args):
    syncgateway_start_memcached(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_bh_puller_task(*args):
    blackholepuller_runtest(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_new_docpush_task(*args):
    newdocpusher_runtest(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_delta_sync_task_load_docs(*args):
    syncgateway_delta_sync_load_docs(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_delta_sync_task_run_test(*args):
    syncgateway_delta_sync_run_test(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_e2e_cbl_task_load_docs(*args):
    syncgateway_e2e_cbl_load_docs(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_e2e_cbl_task_run_test(*args):
    syncgateway_e2e_cbl_run_test(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_e2e_multi_cbl_task_load_docs(*args):
    syncgateway_e2e_multi_cbl_load_docs(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_e2e_multi_cbl_task_run_test(*args):
    syncgateway_e2e_multi_cbl_run_test(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_e2e_multi_cb_task_load_docs(*args):
    syncgateway_e2e_multi_cb_load_docs(*args)


@celery.task(bind=True)
@store_pid
def syncgateway_e2e_multi_cb_task_run_test(*args):
    syncgateway_e2e_multi_cb_run_test(*args)


@celery.task(bind=True)
@store_pid
def sdks_benchmark_task(*args):
    sdks_benchmark_workload(*args)


@celery.task(bind=True)
@store_pid
def vectordb_bench_task(*args):
    run_vectordb_bench_case(*args)


@celery.task(bind=True)
@store_pid
def aibench_task(*args):
    run_aibench_task(*args)


@celery.task(bind=True)
@store_pid
def ch2_load(conn_settings: CH2ConnectionSettings, task_settings: CH2, driver: str, log_file: str):
    local.ch2_load_task(conn_settings, task_settings, driver, log_file)


@celery.task
def custom_analytics_query_task(
    api_url: str,
    api_auth: tuple[str, str],
    queries: list[CustomAnalyticsQuery],
    log_file_path: str,
    request_params: Optional[dict] = None,
):
    return run_custom_analytics_query_task(
        api_url, api_auth, queries, log_file_path, request_params
    )


class WorkloadPhase:

    def __init__(self,
                 task: Callable,
                 target_iterator: Iterable[TargetSettings],
                 base_settings: PhaseSettings,
                 source_iterator: Iterable[TargetSettings] = None,
                 override_settings: dict = {},
                 timer: Optional[int] = None):
        self.task = task
        self.target_iterator = target_iterator
        self.source_iterator = source_iterator
        self.targets = list(target_iterator)
        if source_iterator is not None:
            self.source_targets = list(source_iterator)
            self.source_target = self.source_targets[0]
        self.task_settings = base_settings
        for option, value in override_settings.items():
            if hasattr(self.task_settings, option):
                setattr(self.task_settings, option, value)
        self.task_settings.bucket_list = [t.bucket for t in self.targets]
        self.timer = timer

    def task_sigs(self, workers: Iterator[str]) -> list[tuple[Signature, str]]:
        sigs_with_workers = []

        for target in self.targets:
            for instance in range(self.task_settings.workload_instances):
                worker = next(workers)
                if self.source_iterator is not None:
                    sig = self.task.si(self.task_settings, self.source_target,
                                       target, self.timer).set(
                        queue=worker, expires=self.timer
                    )
                else:
                    sig = self.task.si(self.task_settings, target, self.timer, instance).set(
                        queue=worker, expires=self.timer
                    )
                sigs_with_workers.append((sig, worker))

        return sigs_with_workers


class WorkerManager:

    def __new__(cls, *args, **kwargs):
        if '--remote' in sys.argv:
            return RemoteWorkerManager(*args, **kwargs)
        else:
            return LocalWorkerManager(*args, **kwargs)


class RemoteWorkerManager:

    WORKER_HOME = '/tmp/perfrunner'

    PING_INTERVAL = 1

    # How long to give aborted tasks to shut down gracefully before they get killed
    TASK_TERMINATE_TIMEOUT = 60

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = RemoteHelper(
            cluster_spec, verbose, external_client=self.cluster_spec.external_client
        )

        self.broker_url = ON_PREM_BROKER_URL
        if self.cluster_spec.cloud_infrastructure:
            if (
                self.cluster_spec.kubernetes_infrastructure
                and not self.cluster_spec.external_client
            ):
                self.WORKER_HOME = '/opt/perfrunner'
                self.broker_url = self.remote.get_broker_urls()[0]
                with CAOWorkerFile(self.cluster_spec) as worker_config:
                    worker_config.update_worker_spec()
                    self.worker_path = worker_config.dest_file
            else:
                self.broker_url = (
                    f"amqp://couchbase:couchbase@{self.cluster_spec.utilities[0]}:5672/broker"
                )

        socket_settings = {}
        if hasattr(socket, "TCP_USER_TIMEOUT"):
            socket_settings[socket.TCP_USER_TIMEOUT] = 0

        celery.conf.update(
            REMOTE_CELERY_CONFIG,
            broker_url=self.broker_url,
            broker_connection_timeout=1500,
            broker_connection_max_retries=100,
            broker_transport_options={"socket_settings": socket_settings},
        )
        self.workers = cycle(self.cluster_spec.workers)
        self.terminate()
        self.start()
        self.wait_until_workers_are_ready()
        self.fg_async_results: list[AsyncResult] = []
        self.bg_async_results: list[AsyncResult] = []
        self._aborted_results: list[AsyncResult] = []

    @property
    def is_remote(self) -> bool:
        return True

    def next_worker(self) -> str:
        return next(self.workers)

    def reset_workers(self):
        self.workers = cycle(self.cluster_spec.workers)

    def start(self):
        logger.info('Initializing remote worker environment')
        if self.test_config.client_settings.cherrypick:
            logger.info(f"Using patch on workers: '{self.test_config.client_settings.cherrypick}'")

        if self.cluster_spec.kubernetes_infrastructure and not self.cluster_spec.external_client:
            self.start_kubernetes_workers()
        else:
            self.start_remote_workers()

    def start_remote_workers(self):
        perfrunner_home = os.path.join(self.WORKER_HOME, "perfrunner")
        self.remote.init_repo(self.WORKER_HOME, self.test_config.client_settings.cherrypick)
        need_pymongo = (
            self.cluster_spec.columnar_infrastructure
            and self.test_config.columnar_kafka_links_settings.link_source == "MONGODB"
        )
        self.remote.install_clients(perfrunner_home,
                                    self.test_config.client_settings.python_client,
                                    need_pymongo)
        if '--remote-copy' in sys.argv:
            self.remote.remote_copy(self.WORKER_HOME)
        for worker in self.cluster_spec.workers:
            logger.info(f"Starting remote Celery worker, host={worker}")
            self.remote.start_celery_worker(worker, perfrunner_home, self.broker_url)

    def start_kubernetes_workers(self):
        num_workers = len(self.cluster_spec.workers)
        self.remote.create_from_file(self.worker_path)
        self.remote.wait_for_pods_ready("worker", num_workers)
        # Pull changes to remote workers before starting celery workers on pods
        self.remote.pull_perfrunner_patch(self.test_config.client_settings.cherrypick)
        worker_idx = 0
        for pod in self.remote.get_pods():
            worker_name = pod.get("metadata", {}).get("name", "")
            if "worker" in worker_name:
                self.remote.start_celery_worker(worker_name,
                                                self.cluster_spec.workers[worker_idx],
                                                self.broker_url)
                worker_idx += 1

    def wait_until_workers_are_ready(self):
        workers = [f"celery@{worker}" for worker in self.cluster_spec.workers]
        while True:
            responses = celery.control.ping(workers)
            if len(responses) == len(workers):
                break
            time.sleep(self.PING_INTERVAL)
        logger.info('All remote Celery workers are ready')

    def run_tasks(self, phase: WorkloadPhase) -> list[AsyncResult]:
        if self.test_config.test_case.reset_workers:
            self.reset_workers()

        async_results = []
        for sig, worker in phase.task_sigs(self.workers):
            logger.info(f"Running task on {worker}")
            async_results.append(sig.apply_async())

        logger.info(f"Task results: {async_results}")

        return async_results

    def run_fg_phases(self, phases: Iterable[WorkloadPhase]):
        for phase in phases:
            self.fg_async_results.extend(self.run_tasks(phase))
        self.wait_for_fg_tasks()

    def run_bg_phases(self, phases: Iterable[WorkloadPhase]):
        for phase in phases:
            self.bg_async_results.extend(self.run_tasks(phase))

    def _wait_for_tasks(self, async_results: list[AsyncResult]):
        for res in async_results:
            try:
                res.get()
            except Exception as e:
                logger.info(f"Exception while getting result {e}")
                raise

    def wait_for_fg_tasks(self):
        logger.info('Waiting for foreground tasks to finish')
        self._wait_for_tasks(self.fg_async_results)
        logger.info('All foreground tasks are done')
        self.fg_async_results.clear()

    def wait_for_bg_tasks(self):
        logger.info('Waiting for background tasks to finish')
        self._wait_for_tasks(self.bg_async_results)
        logger.info('All background tasks are done')
        self.bg_async_results.clear()

    def download_celery_logs(self):
        if not os.path.exists('celery'):
            os.mkdir('celery')
        self.remote.get_celery_logs(self.WORKER_HOME)

    def _abort_task(self, task_result: AsyncResult):
        task_result.revoke(terminate=True, signal="SIGTERM")

    def abort_all_tasks(self):
        self._aborted_results = self.fg_async_results + self.bg_async_results
        for result in self._aborted_results:
            logger.info(f"Terminating Celery task (SIGTERM): {result}")
            try:
                self._abort_task(result)
            except Exception as e:
                # Aborting is best-effort: a task we cannot signal must not stop us from
                # signalling the rest, or from finishing the rest of the test teardown.
                logger.warning(f"Failed to terminate Celery task {result}: {e}")
        logger.info('All Celery tasks have been sent SIGTERM')
        # The aborted tasks are no longer ours to track, and re-aborting them later would only
        # produce confusing log lines
        self.fg_async_results.clear()
        self.bg_async_results.clear()
        # Don't return until the tasks have actually stopped. Aborting exists so that workers get
        # to persist their stats (CBPS-430), and a phase is usually aborted from inside a
        # `with_stats` block which reconstructs measurements from those files as soon as it exits,
        # so returning early would race the workers writing them.
        self.wait_for_aborted_tasks()

    def wait_for_aborted_tasks(self):
        """Wait for the aborted tasks to finish, so they can shut down gracefully.

        Bounded by `TASK_TERMINATE_TIMEOUT`, after which the caller is free to kill the workers.
        """
        pending = self._aborted_results
        self._aborted_results = []
        if not pending:
            return

        logger.info(f"Waiting up to {self.TASK_TERMINATE_TIMEOUT}s for aborted tasks to finish")
        deadline = time.time() + self.TASK_TERMINATE_TIMEOUT
        for result in pending:
            try:
                # A task that was aborted mid-flight may well end in failure, which is expected
                # here rather than an error, so don't let `get` re-raise it
                result.get(timeout=max(0, deadline - time.time()), propagate=False)
            except Exception as e:
                # The deadline is shared, so once one task runs out of time the rest have too
                logger.warning(
                    f"Aborted Celery tasks did not all finish within "
                    f"{self.TASK_TERMINATE_TIMEOUT}s, they will be killed: {e}"
                )
                return
        logger.info('All aborted Celery tasks have finished')

    def terminate(self):
        logger.info('Terminating Celery workers')
        if self.cluster_spec.kubernetes_infrastructure and not self.cluster_spec.external_client:
            self.remote.terminate_client_pods(self.worker_path)
        else:
            self.remote.terminate_client_processes()

    def run_sg_tasks(self,
                     task: Callable,
                     task_settings: PhaseSettings,
                     target_iterator: Iterable[TargetSettings],
                     timer: int = None,
                     distribute: bool = False,
                     phase: str = ""):
        self.fg_async_results.clear()
        self.reset_workers()
        for target in target_iterator:
            if distribute:
                total_threads = int(task_settings.syncgateway_settings.threads)
                total_clients = int(task_settings.syncgateway_settings.clients)
                instances_per_client = int(task_settings.syncgateway_settings.instances_per_client)
                total_instances = total_clients * instances_per_client
                threads_per_instance = int(total_threads/total_instances) or 1
                worker_id = 0

                group_tasks = []

                for _ in range(instances_per_client):
                    for client in self.cluster_spec.workers[:total_clients]:
                        worker_id += 1
                        logger.info(
                            f"Running the '{phase}' by worker #{worker_id} on client {client}"
                        )
                        task_settings.syncgateway_settings.threads_per_instance = \
                            str(threads_per_instance)

                        group_tasks.append(
                            task.s(task_settings, target, timer, worker_id, self.cluster_spec).set(
                                queue=client
                            ).set(
                                expires=timer
                            ))

                g = group(group_tasks)
                self.fg_async_results.append(g())
                time.sleep(15)
            else:
                client = self.cluster_spec.workers[0]
                logger.info(f"Running single-instance task '{phase}' on client {client}")
                task_settings.syncgateway_settings.threads_per_instance = \
                    task_settings.syncgateway_settings.threads
                async_result = task.apply_async(
                    args=(task_settings, target, timer, 0, self.cluster_spec),
                    queue=client,
                    expires=timer,
                )
                self.fg_async_results.append(async_result)
                time.sleep(15)
                if task is syncgateway_task_start_memcached:
                    break

    def run_sg_bp_tasks(self,
                        task: Callable,
                        task_settings: PhaseSettings,
                        target_iterator: Iterable[TargetSettings],
                        timer: int = None,
                        distribute: bool = False,
                        phase: str = ""):
        self.fg_async_results.clear()
        self.reset_workers()
        for target in target_iterator:
            if distribute:
                worker_id = 0
                total_clients = int(task_settings.syncgateway_settings.clients)
                for client in self.cluster_spec.workers[:total_clients]:
                    worker_id += 1
                    logger.info(f"Running the '{phase}' by worker #{worker_id} on client {client}")
                    async_result = task.apply_async(
                        args=(task_settings, target, timer, worker_id, self.cluster_spec),
                        queue=client, expires=timer,)
                    self.fg_async_results.append(async_result)
                time.sleep(15)
            else:
                client = self.cluster_spec.workers[0]
                logger.info(f"Running sigle-instance task '{phase}' on client {client}")
                async_result = task.apply_async(
                    args=(task_settings, target, timer, 0, self.cluster_spec),
                    queue=client, expires=timer)
                self.fg_async_results.append(async_result)
                time.sleep(15)


class LocalWorkerManager(RemoteWorkerManager):
    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig, verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        celery.conf.update(LOCAL_CELERY_CONFIG)

        self.workers = cycle(['localhost'])
        self.terminate()
        self.tune_sqlite()
        self.start()
        self.wait_until_workers_are_ready()
        self.fg_async_results = []
        self.bg_async_results = []
        self._aborted_results = []

    @property
    def is_remote(self) -> bool:
        return False

    def tune_sqlite(self):
        for db in LOCAL_BROKER_DB, LOCAL_RESULTS_DB:
            engine = create_engine(f"sqlite:///{db}")
            engine.execute("PRAGMA synchronous=OFF;")

    def wait_until_workers_are_ready(self):
        engine = create_engine(f"sqlite:///{LOCAL_BROKER_DB}")
        query = f'SELECT COUNT(*) FROM kombu_queue WHERE name = "{self.next_worker()}"'

        while True:
            if 'kombu_queue' not in engine.table_names():
                continue

            for count, in engine.execute(query):
                if count:
                    logger.info('Local Celery worker is ready')
                    return

    def start(self):
        logger.info('Starting local Celery worker')
        local.start_celery_worker(queue=self.next_worker())

    def download_celery_logs(self):
        pass

    @staticmethod
    def _task_ids(task_result: AsyncResult) -> list[str]:
        """Return the ids of the tasks covered by a result.

        A group result's own id is a group id, which no task ever writes a pidfile for, so we need
        the ids of the individual tasks it contains instead.
        """
        if members := getattr(task_result, "results", None):
            return [member.id for member in members]
        return [task_result.id]

    @staticmethod
    def _task_process(task_id: str) -> Optional[psutil.Process]:
        """Return the process running a celery task, or None if it isn't running.

        The pidfile is dropped once it has been read. From that point we hold a process handle
        instead, so cleaning it up doesn't depend on the task surviving long enough to do it
        itself: a task killed outright never gets to run the `finally` in `store_pid`.
        """
        pidfile = Path(TASK_PIDFILE_DIR) / f"{task_id}.pid"
        if not pidfile.exists():
            # The task was queued but never started, so there is no process to terminate
            logger.info(f"No pidfile for Celery task {task_id}, nothing to terminate")
            return None

        try:
            return psutil.Process(int(pidfile.read_text()))
        except (ValueError, psutil.Error) as e:
            # The task has most likely already finished
            logger.info(f"No process found for Celery task {task_id}: {e}")
            return None
        finally:
            pidfile.unlink(missing_ok=True)

    def _abort_task(self, task_result: AsyncResult):
        # For local celery workers we don't have remote control because we use SQLAlchemy+SQLite as
        # the broker instead of RabbitMQ, so can't revoke tasks with celery.
        # Instead we manually send SIGTERM to task processes based on their PID.
        #
        # Only the task process itself is signalled: spring installs a SIGTERM handler which shuts
        # its worker processes down gracefully, so that they get to dump their stats. The worker
        # processes don't handle SIGTERM themselves, so signalling them directly would kill them
        # outright. Anything still alive afterwards is dealt with by `terminate`.
        #
        # Note that this process is the celery pool worker running the task, which is the right
        # thing to signal but the wrong thing to wait on: it is long-lived and goes on to run the
        # next task, so it doesn't exit when this one ends. Waiting is done on the task result.
        for task_id in self._task_ids(task_result):
            if process := self._task_process(task_id):
                with suppress(psutil.Error):
                    process.terminate()

    def terminate(self):
        logger.info('Terminating Celery workers')
        local.kill_process('celery')

    def run_sg_tasks(self,
                     task: Callable,
                     task_settings: PhaseSettings,
                     target_iterator: Iterable[TargetSettings],
                     timer: int = None,
                     distribute: bool = False,
                     phase: str = ""):
        self.fg_async_results.clear()
        self.reset_workers()
        for _ in target_iterator:
            if distribute:
                total_threads = int(task_settings.syncgateway_settings.threads)
                total_clients = int(task_settings.syncgateway_settings.clients)
                instances_per_client = int(task_settings.syncgateway_settings.instances_per_client)
                total_instances = total_clients * instances_per_client
                threads_per_instance = int(total_threads/total_instances) or 1
                worker_id = 0
                for _ in range(instances_per_client):
                    for _ in range(total_clients):
                        client = self.next_worker()
                        worker_id += 1
                        logger.info(
                            f"Running the '{phase}' by worker #{worker_id} on client {client}"
                        )
                        task_settings.syncgateway_settings.threads_per_instance = \
                            str(threads_per_instance)
                        async_result = task.apply_async(
                            args=(task_settings, timer, worker_id, self.cluster_spec),
                            queue=client,
                            expires=timer,
                        )
                        self.fg_async_results.append(async_result)
                time.sleep(15)
            else:
                client = self.next_worker()
                logger.info(f"Running single-instance task '{phase}' on client {client}")
                task_settings.syncgateway_settings.threads_per_instance = \
                    task_settings.syncgateway_settings.threads
                async_result = task.apply_async(
                    args=(task_settings, timer, 0, self.cluster_spec),
                    queue=client,
                    expires=timer,
                )
                self.fg_async_results.append(async_result)
                time.sleep(15)

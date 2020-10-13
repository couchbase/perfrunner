import os.path
import sys
import time
from itertools import cycle
from typing import Callable

from celery import Celery
from sqlalchemy import create_engine

from logger import logger
from perfrunner import celerylocal, celeryremote
from perfrunner.helpers import local
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import (
    ClusterSpec,
    PhaseSettings,
    TargetIterator,
    TestConfig,
)
from perfrunner.workloads import spring_workload
from perfrunner.workloads.blackholepuller import (
    blackholepuller_runtest,
    newdocpusher_runtest,
)
from perfrunner.workloads.pillowfight import (
    pillowfight_data_load,
    pillowfight_workload,
)
from perfrunner.workloads.syncgateway import (
    syncgateway_grant_access,
    syncgateway_init_users,
    syncgateway_load_docs,
    syncgateway_load_users,
    syncgateway_run_test,
    syncgateway_start_memcached,
)
from perfrunner.workloads.ycsb import ycsb_data_load, ycsb_workload

celery = Celery('workers')
if '--remote' in sys.argv or '-C' in sys.argv:
    # -C flag is a hack to distinguish local and remote workers!
    celery.config_from_object(celeryremote)
else:
    celery.config_from_object(celerylocal)


@celery.task
def spring_task(*args):
    spring_workload(*args)


@celery.task
def pillowfight_data_load_task(*args):
    pillowfight_data_load(*args)


@celery.task
def pillowfight_task(*args):
    pillowfight_workload(*args)


@celery.task
def ycsb_data_load_task(*args):
    ycsb_data_load(*args)


@celery.task
def ycsb_task(*args):
    ycsb_workload(*args)


@celery.task
def syncgateway_task_load_users(*args):
    syncgateway_load_users(*args)


@celery.task
def syncgateway_task_init_users(*args):
    syncgateway_init_users(*args)


@celery.task
def syncgateway_task_grant_access(*args):
    syncgateway_grant_access(*args)


@celery.task
def syncgateway_task_load_docs(*args):
    syncgateway_load_docs(*args)


@celery.task
def syncgateway_task_run_test(*args):
    syncgateway_run_test(*args)


@celery.task
def syncgateway_task_start_memcached(*args):
    syncgateway_start_memcached(*args)


@celery.task
def syncgateway_bh_puller_task(*args):
    blackholepuller_runtest(*args)


@celery.task
def syncgateway_new_docpush_task(*args):
    newdocpusher_runtest(*args)


class WorkerManager:

    def __new__(cls, *args, **kwargs):
        if '--remote' in sys.argv:
            return RemoteWorkerManager(*args, **kwargs)
        else:
            return LocalWorkerManager(*args, **kwargs)


class RemoteWorkerManager:

    WORKER_HOME = '/tmp/perfrunner'

    PING_INTERVAL = 1

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)
        self.workers = cycle(self.cluster_spec.workers)
        self.terminate()
        self.start()
        self.wait_until_workers_are_ready()

    @property
    def is_remote(self) -> bool:
        return True

    def next_worker(self) -> str:
        return next(self.workers)

    def reset_workers(self):
        self.workers = cycle(self.cluster_spec.workers)

    def start(self):
        logger.info('Initializing remote worker environment')
        self.remote.init_repo(self.WORKER_HOME)

        for worker in self.cluster_spec.workers:
            logger.info('Starting remote Celery worker, host={}'.format(worker))
            perfrunner_home = os.path.join(self.WORKER_HOME, 'perfrunner')
            self.remote.start_celery_worker(worker, perfrunner_home)

    def wait_until_workers_are_ready(self):
        workers = ['celery@{}'.format(worker)
                   for worker in self.cluster_spec.workers]
        while True:
            responses = celery.control.ping(workers)
            if len(responses) == len(workers):
                break
            time.sleep(self.PING_INTERVAL)
        logger.info('All remote Celery workers are ready')

    def run_tasks(self,
                  task: Callable,
                  task_settings: PhaseSettings,
                  target_iterator: TargetIterator,
                  timer: int = None):
        self.async_results = []
        self.reset_workers()
        for target in target_iterator:
            for instance in range(task_settings.worker_instances):
                worker = self.next_worker()
                logger.info('Running the task on {}'.format(worker))
                async_result = task.apply_async(
                    args=(task_settings, target, timer, instance),
                    queue=worker, expires=timer,
                )
                self.async_results.append(async_result)

    def run_sg_tasks(self,
                     task: Callable,
                     task_settings: PhaseSettings,
                     timer: int = None,
                     distribute: bool = False,
                     phase: str = ""):
        self.async_results = []
        self.reset_workers()

        if distribute:
            total_threads = int(task_settings.syncgateway_settings.threads)
            total_clients = int(task_settings.syncgateway_settings.clients)
            instances_per_client = int(task_settings.syncgateway_settings.instances_per_client)
            total_instances = total_clients * instances_per_client
            threads_per_instance = int(total_threads/total_instances) or 1
            worker_id = 0
            for instance in range(instances_per_client):
                for client in self.cluster_spec.workers[:total_clients]:
                    worker_id += 1
                    logger.info('Running the \'{}\' by worker #{} on '
                                'client {}'.format(phase, worker_id, client))
                    task_settings.syncgateway_settings.threads_per_instance = \
                        str(threads_per_instance)
                    async_result = task.apply_async(
                        args=(task_settings, timer, worker_id, self.cluster_spec),
                        queue=client, expires=timer,
                    )
                    self.async_results.append(async_result)
        else:
            client = self.cluster_spec.workers[0]
            logger.info('Running sigle-instance task \'{}\' on client {}'.format(phase, client))
            async_result = task.apply_async(
                args=(task_settings, timer, 0, self.cluster_spec), queue=client, expires=timer,
            )
            self.async_results.append(async_result)

    def run_sg_bp_tasks(self,
                        task: Callable,
                        task_settings: PhaseSettings,
                        timer: int = None,
                        distribute: bool = False,
                        phase: str = ""):

        self.async_results = []
        self.reset_workers()

        if distribute:
            worker_id = 0
            total_clients = int(task_settings.syncgateway_settings.clients)

            for client in self.cluster_spec.workers[:total_clients]:
                worker_id += 1
                logger.info('Running the \'{}\' by worker #{} on'
                            ' client {}'.format(phase, worker_id, client))

                async_result = task.apply_async(
                    args=(task_settings, timer, worker_id, self.cluster_spec),
                    queue=client, expires=timer,)

                self.async_results.append(async_result)
        else:
            client = self.cluster_spec.workers[0]
            logger.info('Running sigle-instance task \'{}\' on client {}'.format(phase, client))
            async_result = task.apply_async(
                args=(task_settings, timer, 0, self.cluster_spec),
                queue=client, expires=timer)
            self.async_results.append(async_result)

    def wait_for_workers(self):
        logger.info('Waiting for all tasks to finish')
        for async_result in self.async_results:
            async_result.get()
        logger.info('All tasks are done')

    def download_celery_logs(self):
        if not os.path.exists('celery'):
            os.mkdir('celery')
        self.remote.get_celery_logs(self.WORKER_HOME)

    def terminate(self):
        logger.info('Terminating Celery workers')
        self.remote.terminate_client_processes()


class LocalWorkerManager(RemoteWorkerManager):

    BROKER_DB = 'perfrunner.db'
    RESULTS_DB = 'results.db'

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.terminate()
        self.tune_sqlite()
        self.start()
        self.wait_until_workers_are_ready()
        self.all_clients = ('localhost',)

    @property
    def is_remote(self) -> bool:
        return False

    def next_worker(self) -> str:
        return 'localhost'

    def tune_sqlite(self):
        for db in self.BROKER_DB, self.RESULTS_DB:
            engine = create_engine('sqlite:///{}'.format(db))
            engine.execute('PRAGMA synchronous=OFF;')

    def wait_until_workers_are_ready(self):
        engine = create_engine('sqlite:///{}'.format(self.BROKER_DB))
        query = 'SELECT COUNT(*) FROM kombu_queue WHERE name = "{}"'\
            .format(self.next_worker())

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

    def terminate(self):
        logger.info('Terminating Celery workers')
        local.kill_process('celery')

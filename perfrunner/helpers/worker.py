import os.path
import sys
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
from perfrunner.workloads.pillowfight import (
    pillowfight_data_load,
    pillowfight_workload,
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


class WorkerManager:

    def __new__(cls, *args, **kwargs):
        if '--remote' in sys.argv:
            return RemoteWorkerManager(*args, **kwargs)
        else:
            return LocalWorkerManager(*args, **kwargs)


class RemoteWorkerManager:

    WORKER_HOME = '/tmp/perfrunner'

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig,
                 verbose: bool):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = RemoteHelper(cluster_spec, test_config, verbose)

        self.queues = cycle(self.cluster_spec.workers)

        self.terminate()
        self.start()

    @property
    def is_remote(self) -> bool:
        return True

    def next_queue(self) -> str:
        return next(self.queues)

    def start(self):
        logger.info('Initializing remote worker environment')
        self.remote.init_repo(self.WORKER_HOME)

        for worker in self.cluster_spec.workers:
            logger.info('Starting remote Celery worker, host={}'.format(worker))
            perfrunner_home = os.path.join(self.WORKER_HOME, 'perfrunner')
            self.remote.start_celery_worker(worker, perfrunner_home)

    def run_tasks(self,
                  task: Callable,
                  task_settings: PhaseSettings,
                  target_iterator: TargetIterator,
                  timer: int = None):
        self.callbacks = []

        for target in target_iterator:
            for instance in range(task_settings.worker_instances):
                callback = task.apply_async(
                    args=(task_settings, target, timer, instance),
                    queue=self.next_queue(), expires=timer,
                )
                self.callbacks.append(callback)

    def wait_for_workers(self):
        logger.info('Waiting for all tasks to finish')
        for callback in self.callbacks:
            callback.wait()
        logger.info('All workers are done')

    def terminate(self):
        logger.info('Terminating Celery workers')
        self.remote.clean_clients(self.WORKER_HOME)


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
        self.ensure_queue_exists()

    @property
    def is_remote(self) -> bool:
        return False

    def next_queue(self) -> str:
        return 'local'

    def tune_sqlite(self):
        for db in self.BROKER_DB, self.RESULTS_DB:
            engine = create_engine('sqlite:///{}'.format(db))
            engine.execute('PRAGMA synchronous=OFF;')

    def ensure_queue_exists(self):
        engine = create_engine('sqlite:///{}'.format(self.BROKER_DB))
        query = 'SELECT COUNT(*) FROM kombu_queue WHERE name = "{}"'\
            .format(self.next_queue())

        while True:
            if 'kombu_queue' not in engine.table_names():
                continue

            for count, in engine.execute(query):
                if count:
                    logger.info('Local Celery worker is ready')
                    return

    def start(self):
        logger.info('Starting local Celery worker')
        local.start_celery_worker(queue=self.next_queue())

    def terminate(self):
        logger.info('Terminating Celery workers')
        local.kill_process('celery')

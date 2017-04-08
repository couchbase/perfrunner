import os.path
import sys
from itertools import cycle
from typing import Callable

from celery import Celery
from logger import logger
from sqlalchemy import create_engine

from perfrunner import celerylocal, celeryremote
from perfrunner.helpers import local
from perfrunner.remote import Remote
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

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 remote_manager: Remote):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.remote = remote_manager

        self.queues = cycle(self.cluster_spec.workers)

        self.terminate()
        self.start()

    def next_queue(self) -> str:
        return next(self.queues)

    def start(self):
        logger.info('Initializing remote worker environment')

        for worker in self.cluster_spec.workers:
            logger.info('Starting remote Celery worker, host={}'.format(worker))

            self.remote.init_repo(worker, self.WORKER_HOME)

            perfrunner_home = os.path.join(self.WORKER_HOME, 'perfrunner')
            self.remote.start_celery_worker(worker, perfrunner_home)

    def init_ycsb_repo(self):
        self.remote.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                               branch=self.test_config.ycsb_settings.branch,
                               worker_home=self.WORKER_HOME)

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

    SQLITE_DBS = 'perfrunner.db', 'results.db'

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 *args):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.terminate()
        self.tune_sqlite()
        self.start()

    def next_queue(self) -> str:
        return 'local'

    def tune_sqlite(self):
        for db in self.SQLITE_DBS:
            engine = create_engine('sqlite:///{}'.format(db))
            engine.execute('PRAGMA synchronous=OFF;')

    def start(self):
        logger.info('Starting local Celery worker')
        local.start_celery_worker(queue='local')

    def init_ycsb_repo(self):
        local.clone_ycsb(repo=self.test_config.ycsb_settings.repo,
                         branch=self.test_config.ycsb_settings.branch)

    def terminate(self):
        logger.info('Terminating Celery workers')
        local.kill_process('celery')

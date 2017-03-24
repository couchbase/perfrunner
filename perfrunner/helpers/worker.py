import os.path
import sys
from itertools import cycle
from time import sleep

from celery import Celery
from kombu import Queue
from logger import logger
from sqlalchemy import create_engine

from perfrunner import celerylocal, celeryremote
from perfrunner.helpers import local
from perfrunner.helpers.misc import log_action, uhex
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
def spring_task(*args, **kwargs):
    spring_workload(*args, **kwargs)


@celery.task
def pillowfight_data_load_task(*args, **kwargs):
    pillowfight_data_load(*args, **kwargs)


@celery.task
def pillowfight_task(*args, **kwargs):
    pillowfight_workload(*args, **kwargs)


@celery.task
def ycsb_data_load_task(*args, **kwargs):
    ycsb_data_load(*args, **kwargs)


@celery.task
def ycsb_task(*args, **kwargs):
    ycsb_workload(*args, **kwargs)


class WorkerManager(object):

    def __new__(cls, *args, **kwargs):
        if '--remote' in sys.argv:
            return RemoteWorkerManager(*args, **kwargs)
        else:
            return LocalWorkerManager(*args, **kwargs)


class RemoteWorkerManager(object):

    RACE_DELAY = 2

    def __init__(self, cluster_spec, test_config, remote_manager):
        self.cluster_spec = cluster_spec
        self.buckets = test_config.buckets
        self.remote = remote_manager

        self.temp_dir = os.path.join('/tmp', uhex())

        self.terminate()
        self.start()

    def new_worker_pool(self):
        return cycle(self.cluster_spec.workers)

    def yield_queues(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.buckets:
                yield '{}-{}'.format(master.split(':')[0], bucket)

    def start(self):
        logger.info('Initializing remote worker environment')

        worker_pool = cycle(self.cluster_spec.workers)

        for queue in self.yield_queues():
            worker = next(worker_pool)
            logger.info('Starting Celery worker, host={}, queue={}'
                        .format(worker, queue))

            worker_home = os.path.join(self.temp_dir, queue)
            perfrunner_home = os.path.join(worker_home, 'perfrunner')

            self.remote.init_repo(worker, worker_home)
            self.remote.start_celery_worker(worker, perfrunner_home, queue)

    def run_tasks(self, task, task_settings, target_iterator, timer=None):
        self.workers = []
        for target in target_iterator:
            log_action('Celery task', task_settings)

            qname = '{}-{}'.format(target.node.split(':')[0], target.bucket)
            queue = Queue(name=qname)
            worker = task.apply_async(
                args=(task_settings, target, timer),
                queue=queue.name, expires=timer,
            )
            self.workers.append(worker)
            sleep(self.RACE_DELAY)

    def wait_for_workers(self):
        logger.info('Waiting for workers to finish')
        for worker in self.workers:
            worker.wait()
        logger.info('All workers are done')

    def terminate(self):
        logger.info('Terminating Celery workers')
        self.remote.clean_clients(self.temp_dir)


class LocalWorkerManager(RemoteWorkerManager):

    SQLITE_DBS = 'perfrunner.db', 'results.db'

    def __init__(self, cluster_spec, test_config, *args):
        self.cluster_spec = cluster_spec
        self.buckets = test_config.buckets

        self.terminate()
        self.tune_sqlite()
        self.start()

    def tune_sqlite(self):
        for db in self.SQLITE_DBS:
            engine = create_engine('sqlite:///{}'.format(db))
            engine.execute('PRAGMA read_uncommitted=1;')
            engine.execute('PRAGMA synchronous=OFF;')

    def start(self):
        for queue in self.yield_queues():
            logger.info('Starting Celery worker, queue={}'.format(queue))
            local.start_celery_worker(queue)
            sleep(self.RACE_DELAY)

    def terminate(self):
        logger.info('Terminating Celery workers')
        local.kill_process('celery')

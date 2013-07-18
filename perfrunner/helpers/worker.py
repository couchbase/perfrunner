from uuid import uuid4

from celery import Celery
from fabric import state
from fabric.api import cd, run
from kombu import Queue
from logger import logger
from spring.wgen import WorkloadGen

from perfrunner.settings import BROKER_URL, REPO

CELERY_QUEUES = (Queue('Q1'), Queue('Q2'))
celery = Celery('workers', backend='amqp', broker=BROKER_URL)


@celery.task
def task_run_workload(settings, target):
    wg = WorkloadGen(settings, target)
    wg.run()


class WorkerManager(object):

    def __init__(self, cluster_spec):
        ssh_username, ssh_password = cluster_spec.get_ssh_credentials()

        self.hosts = cluster_spec.get_workers()
        if self.hosts:
            self.is_remote = True

            state.env.user = ssh_username
            state.env.password = ssh_password
            state.output.running = False
            state.output.stdout = False

            self.temp_dir = '/tmp/{0}'.format(uuid4().hex[:12])
            self._initialize_project()
            self._start()
        else:
            self.is_remote = False

    def _initialize_project(self):
        for i, q in enumerate(CELERY_QUEUES):
            logger.info('Intializing remote worker environment')

            state.env.host_string = self.hosts[i]
            temp_dir = '{0}-{1}'.format(self.temp_dir, q.name)
            run('mkdir {0}'.format(temp_dir))
            with cd(temp_dir):
                run('git clone {0}'.format(REPO))
            with cd('{0}/perfrunner'.format(temp_dir)):
                run('virtualenv env')
                run('env/bin/pip install -r requirements.txt')

    def _start(self):
        for i, q in enumerate(CELERY_QUEUES):
            logger.info('Starting remote Celery worker')

            state.env.host_string = self.hosts[i]
            temp_dir = '{0}-{1}'.format(self.temp_dir, q.name)
            with cd('{0}/perfrunner'.format(temp_dir)):
                run('dtach -n /tmp/perfrunner_{0}.sock '
                    'env/bin/celery worker '
                    '-A perfrunner.helpers.worker -Q {0} -c 4'.format(q.name))

    def run_workload(self, settings, target_iterator):
        queues = (q.name for q in CELERY_QUEUES)
        curr_target = None
        curr_queue = None
        workers = []
        for target in target_iterator:
            if self.is_remote:
                logger.info('Starting workload generator remotely')
                if curr_target != target.node:
                    curr_target = target.node
                    curr_queue = queues.next()
                workers.append(task_run_workload.apply_async(
                    args=(settings, target), queue=curr_queue))
            else:
                logger.info('Starting workload generator locally')
                task_run_workload.apply(args=(settings, target))
        for worker in workers:
            worker.wait()

    def terminate(self):
        if self.is_remote:
            for i, q in enumerate(CELERY_QUEUES):
                state.env.host_string = self.hosts[i]
                temp_dir = '{0}-{1}'.format(self.temp_dir, q.name)
                logger.info('Terminating remote Celery worker')
                run('killall -9 celery; exit 0')
                logger.info('Cleaning up remote worker environment')
                run('rm -fr {0}'.format(temp_dir))

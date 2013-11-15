from celery import Celery
from fabric import state
from fabric.api import cd, run
from kombu import Queue
from logger import logger
from spring.wgen import WorkloadGen

from perfrunner.helpers.misc import uhex
from perfrunner.settings import BROKER_URL, REPO

CELERY_QUEUES = (Queue('Q1'), Queue('Q2'))
NUM_CELERY_WORKERS = 64
celery = Celery('workers', backend='amqp', broker=BROKER_URL)


@celery.task
def task_run_workload(settings, target, shutdown_event, ddocs, neg_coins):
    wg = WorkloadGen(settings, target, shutdown_event, ddocs, neg_coins)
    wg.run()


class WorkerManager(object):

    def __init__(self, cluster_spec):
        self.hosts = cluster_spec.get_workers()
        if self.hosts:
            self.is_remote = True
            state.env.user, state.env.password = \
                cluster_spec.get_ssh_credentials()
            state.output.running = False
            state.output.stdout = False
        else:
            self.is_remote = False

        self.temp_dir = '/tmp/{0}'.format(uhex()[:12])
        self._initialize_project()
        self._start()
 

    def _initialize_project(self):
        for i, q in enumerate(CELERY_QUEUES):
            logger.info('Intializing remote worker environment')

            if self.is_remote:
                state.env.host_string = self.hosts[i]
            else:
                state.env.host_string = "127.0.0.1"
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

            if self.is_remote:
                state.env.host_string = self.hosts[i]
            else:
                state.env.host_string = "127.0.0.1"
            temp_dir = '{0}-{1}'.format(self.temp_dir, q.name)
            with cd('{0}/perfrunner'.format(temp_dir)):
                run('dtach -n /tmp/perfrunner_{0}.sock '
                    'env/bin/celery worker '
                    '-A perfrunner.helpers.worker -Q {0} -c {1}'.format(q.name, NUM_CELERY_WORKERS))

    def run_workload(self, settings, target_iterator, shutdown_event=None,
                     ddocs=None, neg_coins=False):
        targets = list(target_iterator)
        queues = (q.name for q in CELERY_QUEUES)
        curr_target = None
        curr_queue = None
        workers = []
        for target in targets:
            if self.is_remote:
                logger.info('Starting workload generator remotely')
                if len(targets) == 1:
                    for i in range(1, len(self.hosts) + 1):
                        target.prefix = "client-%s" % i
                        workers.append(task_run_workload.apply_async(
                            args=(settings, target, shutdown_event, ddocs, neg_coins),
                            queue='Q%s' % i))
                    break
                if curr_target != target.node:
                    curr_target = target.node
                    curr_queue = queues.next()
                workers.append(task_run_workload.apply_async(
                    args=(settings, target, shutdown_event, ddocs, neg_coins),
                    queue=curr_queue))
            else:
                logger.info('Starting workload generator locally: %s' % (target.bucket))
                workers.append(task_run_workload.apply_async(
                    args=(settings, target, shutdown_event, ddocs, neg_coins),
		            queue='Q1'))
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

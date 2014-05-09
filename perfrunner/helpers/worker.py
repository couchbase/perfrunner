from celery import Celery
from fabric import state
from fabric.api import cd, run, settings
from kombu import Queue
from logger import logger
from spring.wgen import WorkloadGen

from perfrunner.helpers.misc import uhex
from perfrunner.settings import BROKER_URL, REPO

celery = Celery('workers', backend='amqp', broker=BROKER_URL)


@celery.task
def task_run_workload(settings, target, timer):
    wg = WorkloadGen(settings, target, timer=timer)
    wg.run()


class WorkerManager(object):

    def __init__(self, cluster_spec, test_config):
        self.cluster_spec = cluster_spec
        self.test_config = test_config

        self.worker_hosts = cluster_spec.workers
        self.queues = []
        self.workers = []

        self.user, self.password = cluster_spec.client_credentials

        self.temp_dir = '/tmp/{}'.format(uhex()[:12])
        with settings(user=self.user, password=self.password):
            self._initialize_project()
            self._start()

    def _initialize_project(self):
        for i, master in enumerate(self.cluster_spec.yield_masters()):
            state.env.host_string = self.worker_hosts[i]
            run('killall -9 celery; exit 0')
            for bucket in self.test_config.buckets:
                logger.info('Intializing remote worker environment')

                qname = '{}-{}'.format(master.split(':')[0], bucket)
                temp_dir = '{}-{}'.format(self.temp_dir, qname)
                run('mkdir {}'.format(temp_dir))
                with cd(temp_dir):
                    run('git clone {}'.format(REPO))
                with cd('{}/perfrunner'.format(temp_dir)):
                    run('virtualenv -p python2.7 env')
                    run('PATH=/usr/lib/ccache:/usr/lib64/ccache/bin:$PATH '
                        'env/bin/pip install '
                        '--download-cache /tmp/pip -r requirements.txt')

    def _start(self):
        for i, master in enumerate(self.cluster_spec.yield_masters()):
            state.env.host_string = self.worker_hosts[i]
            for bucket in self.test_config.buckets:
                logger.info('Starting remote Celery worker')

                qname = '{}-{}'.format(master.split(':')[0], bucket)
                temp_dir = '{}-{}/perfrunner'.format(self.temp_dir, qname)
                run('cd {0}; nohup env/bin/celery worker '
                    '-A perfrunner.helpers.worker -Q {1} -c 1 '
                    '&>/tmp/worker_{1}.log &'.format(temp_dir, qname),
                    pty=False)

    def run_workload(self, settings, target_iterator, timer=None):
        self.workers = []
        for target in target_iterator:
            logger.info('Starting workload generator remotely')

            qname = '{}-{}'.format(target.node.split(':')[0], target.bucket)
            queue = Queue(name=qname)
            worker = task_run_workload.apply_async(
                args=(settings, target, timer),
                queue=queue.name, expires=timer,
            )
            self.workers.append(worker)
            self.queues.append(queue)

    def wait_for_workers(self):
        for worker in self.workers:
            worker.wait()

    def terminate(self):
        with settings(user=self.user, password=self.password):
            for i, master in enumerate(self.cluster_spec.yield_masters()):
                state.env.host_string = self.worker_hosts[i]
                for bucket in self.test_config.buckets:
                    logger.info('Terminating remote Celery worker')
                    run('killall -9 celery; exit 0')

                    logger.info('Cleaning up remote worker environment')
                    qname = '{}-{}'.format(master.split(':')[0], bucket)
                    temp_dir = '{}-{}'.format(self.temp_dir, qname)
                    run('rm -fr {}'.format(temp_dir))

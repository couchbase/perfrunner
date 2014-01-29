from celery import Celery
from fabric import state
from fabric.api import cd, run
from kombu import Queue
from logger import logger
from spring.wgen import WorkloadGen

from perfrunner.helpers.misc import uhex
from perfrunner.settings import BROKER_URL, REPO

celery = Celery('workers', backend='amqp', broker=BROKER_URL)


@celery.task
def task_run_workload(settings, target, ddocs):
    wg = WorkloadGen(settings, target, None, ddocs)
    wg.run()


class WorkerManager(object):

    def __init__(self, cluster_spec, test_config):
        self.worker_hosts = cluster_spec.get_workers()
        self.queues = []

        state.env.user, state.env.password = cluster_spec.get_ssh_credentials()
        state.output.running = False
        state.output.stdout = False

        self.temp_dir = '/tmp/{}'.format(uhex()[:12])
        self._initialize_project(cluster_spec, test_config)
        self._start(cluster_spec, test_config)

    def _initialize_project(self, cluster_spec, test_config):
        for i, master in enumerate(cluster_spec.yield_masters()):
            state.env.host_string = self.worker_hosts[i]
            run('killall -9 celery; exit 0')
            for bucket in test_config.get_buckets():
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

    def _start(self, cluster_spec, test_config):
        for i, master in enumerate(cluster_spec.yield_masters()):
            state.env.host_string = self.worker_hosts[i]
            for bucket in test_config.get_buckets():
                logger.info('Starting remote Celery worker')

                qname = '{}-{}'.format(master.split(':')[0], bucket)
                temp_dir = '{}-{}'.format(self.temp_dir, qname)
                with cd('{}/perfrunner'.format(temp_dir)):
                    run('dtach -n /tmp/celery_{0}.sock '
                        'env/bin/celery worker -A perfrunner.helpers.worker '
                        '-Q {0} -c 1'.format(qname))

    def run_workload(self, settings, target_iterator, ddocs=None):
        workers = []
        for target in target_iterator:
            logger.info('Starting workload generator remotely')

            qname = '{}-{}'.format(target.node.split(':')[0], target.bucket)
            queue = Queue(name=qname)
            worker = task_run_workload.apply_async(
                args=(settings, target, ddocs), queue=queue.name
            )
            workers.append(worker)
            self.queues.append(queue)
        for worker in workers:
            worker.wait()

    def terminate(self, cluster_spec, test_config):
        for i, master in enumerate(cluster_spec.yield_masters()):
            state.env.host_string = self.worker_hosts[i]
            for bucket in test_config.get_buckets():
                logger.info('Terminating remote Celery worker')
                run('killall -9 celery; exit 0')

                logger.info('Cleaning up remote worker environment')
                qname = '{}-{}'.format(master.split(':')[0], bucket)
                temp_dir = '{}-{}'.format(self.temp_dir, qname)
                run('rm -fr {}'.format(temp_dir))
        for queue in self.queues:
            queue.delete()

from fabric.api import cd, run, settings
from logger import logger

from perfrunner.remote.context import all_clients
from perfrunner.settings import REPO


class Remote(object):

    def __init__(self, cluster_spec, test_config, os):
        self.os = os
        self.hosts = tuple(cluster_spec.yield_hostnames())
        self.kv_hosts = tuple(cluster_spec.yield_kv_servers())
        self.cluster_spec = cluster_spec
        self.test_config = test_config

    @staticmethod
    def wget(url, outdir='/tmp', outfile=None):
        logger.info('Fetching {}'.format(url))
        if outfile is not None:
            run('wget -nc "{}" -P {} -O {}'.format(url, outdir, outfile))
        else:
            run('wget -N "{}" -P {}'.format(url, outdir))

    @all_clients
    def clean_clients(self, temp_dir):
        run('killall -9 celery', quiet=True)
        run('rm -fr {}'.format(temp_dir))

    def init_repo(self, worker, worker_home):
        with settings(host_string=worker):
            run('mkdir {}'.format(worker_home))

            with cd(worker_home):
                run('git clone -q {}'.format(REPO))

                with cd('perfrunner'):
                    run('make')

    def start_celery_worker(self, worker, worker_home, queue):
        with settings(host_string=worker):
            run('cd {0}; ulimit -n 10240; '
                'PYTHONOPTIMIZE=1 C_FORCE_ROOT=1 '
                'nohup env/bin/celery worker '
                '-A perfrunner.helpers.worker -Q {1} -c 1 -n {2} -C '
                '&>worker_{1}.log &'.format(worker_home, queue, worker),
                pty=False)

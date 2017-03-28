from fabric.api import cd, run, settings, shell_env
from logger import logger

from perfrunner.remote.context import all_clients
from perfrunner.settings import REPO


class Remote(object):

    CLIENT_PROCESSES = 'celery', 'cbc-pillowfight'

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
        run('killall -9 {}'.format(' '.join(self.CLIENT_PROCESSES)), quiet=True)

        run('rm -fr {}'.format(temp_dir))

    def init_repo(self, worker, worker_home):
        with settings(host_string=worker):
            run('mkdir -p {}'.format(worker_home))

            with cd(worker_home):
                run('git clone -q {}'.format(REPO))

                with cd('perfrunner'):
                    run('make')

    def start_celery_worker(self, worker, worker_home):
        with settings(host_string=worker):
            with cd(worker_home), shell_env(PYTHONOPTIMIZE='1',
                                            PYTHONWARNINGS='ignore',
                                            C_FORCE_ROOT='1'):
                run('ulimit -n 10240; '
                    'nohup env/bin/celery worker '
                    '-A perfrunner.helpers.worker -Q {0} -n {0} -C '
                    '&>worker_{0}.log &'.format(worker), pty=False)

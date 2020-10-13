from fabric.api import cd, get, run, settings, shell_env

from logger import logger
from perfrunner.remote.context import all_clients
from perfrunner.settings import REPO, BRANCH


class Remote:

    CLIENT_PROCESSES = 'celery', 'cbc-pillowfight', 'memcached'

    def __init__(self, cluster_spec, test_config, os):
        self.os = os
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
    def terminate_client_processes(self):
        run('killall -9 {}'.format(' '.join(self.CLIENT_PROCESSES)), quiet=True)

    @all_clients
    def init_repo(self, worker_home: str):
        run('rm -fr {}'.format(worker_home))
        run('mkdir -p {}'.format(worker_home))

        with cd(worker_home):
            run('git clone -q -b {} {}'.format(BRANCH, REPO))

            with cd('perfrunner'):
                run('make')

    def start_celery_worker(self, worker, worker_home):
        with settings(host_string=worker):
            with cd(worker_home), shell_env(PYTHONOPTIMIZE='1',
                                            PYTHONWARNINGS='ignore',
                                            C_FORCE_ROOT='1'):
                run('ulimit -n 10240; '
                    'nohup env/bin/celery worker -A perfrunner.helpers.worker '
                    '-l INFO -Q {0} -n {0} -C --discard '
                    '&>worker_{0}.log &'.format(worker), pty=False)

    @all_clients
    def clone_ycsb(self, repo: str, branch: str, worker_home: str, ycsb_instances: int):
        logger.info('Cloning YCSB repository: {} branch {}'.format(
            repo, branch))

        for instance in range(ycsb_instances):
            with cd(worker_home), cd('perfrunner'):
                run('git clone -q -b {} {}'.format(branch, repo))
                run('mv YCSB YCSB_{}'.format(instance+1))

        with cd(worker_home), cd('perfrunner'):
            run('git clone -q -b {} {}'.format(branch, repo))

    @all_clients
    def get_celery_logs(self, worker_home: str):
        logger.info('Collecting remote Celery logs')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat worker_*.log', quiet=True)
            if not r.return_code:
                get('worker_*.log', local_path='celery/')

    @all_clients
    def get_export_files(self, worker_home: str):
        logger.info('Collecting YCSB export files')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat YCSB/ycsb_run_*.log', quiet=True)
            if not r.return_code:
                get('YCSB/ycsb_run_*.log', local_path='YCSB/')

    @all_clients
    def get_syncgateway_ycsb_logs(self, worker_home, sgs, local_dir):
        localpath = "{}/".format(local_dir)
        instances = int(sgs.instances_per_client)
        pattern = "{}*".format(sgs.log_title)
        logger.info('Collecting YCSB logs')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat YCSB/{}'.format(pattern), quiet=True)
            if not r.return_code:
                get('YCSB/{}'.format(pattern), local_path=localpath)
            for i in range(instances):
                r = run('stat YCSB_{}/{}'.format(i+1, pattern), quiet=True)
                if not r.return_code:
                    get('YCSB_{}/{}'.format(i+1, pattern), local_path=local_dir)

    @all_clients
    def download_blackholepuller(self, worker_home: str):
        print('download or copy  blackholepuller ')
        with cd('/root/SG_TestingTools'):
            cmd = 'cp blackholePuller /tmp/perfrunner/perfrunner/'
            run(cmd)

    @all_clients
    def download_newdocpusher(self, worker_home: str):
        print('download or copy  blackholepuller ')
        with cd('/root/SG_TestingTools'):
            cmd = 'cp newDocPusher /tmp/perfrunner/perfrunner/'
            run(cmd)

    @all_clients
    def get_sg_blackholepuller_logs(self, worker_home, sgs):
        pattern = "*{}*".format('_blackholepuller_')
        logger.info('Collecting SG Blackhole Puller logs')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat {}'.format(pattern), quiet=True)
            if not r.return_code:
                get('{}'.format(pattern), local_path='.')

    @all_clients
    def get_sgblackholepuller_result_files(self):
        logger.info('Collecting blackholepuller result files')
        with cd('/tmp/perfrunner'), cd('perfrunner'):
            r = run('stat sg_blackholepuller_result*.log', quiet=True)
            if not r.return_code:
                get('sg_blackholepuller_result*.log', local_path='/')

    @all_clients
    def get_sg_newdocpusher_logs(self, worker_home, sgs):
        pattern = "*{}*".format('_newdocpusher_')
        logger.info('Collecting SG Blackhole Puller logs')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat {}'.format(pattern), quiet=True)
            if not r.return_code:
                get('{}'.format(pattern), local_path='.')

    @all_clients
    def get_newdocpusher_result_files(self):
        logger.info('Collecting blackholepuller result files')
        with cd('/tmp/perfrunner'), cd('perfrunner'):
            r = run('stat sg_newdocpusher_result*.log', quiet=True)
            if not r.return_code:
                get('sg_newdocpusher_result*.log', local_path='/')

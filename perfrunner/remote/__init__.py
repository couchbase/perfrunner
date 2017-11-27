from fabric.api import cd, get, run, settings, shell_env

from logger import logger
from perfrunner.remote.context import all_clients
from perfrunner.settings import REPO


class Remote:

    CLIENT_PROCESSES = 'celery', 'cbc-pillowfight', 'memcached'
    CLIENT_PROCESSES_ARGS = ['loader']

    def __init__(self, cluster_spec, os):
        self.os = os
        self.cluster_spec = cluster_spec

    @staticmethod
    def wget(url, outdir='/tmp', outfile=None):
        logger.info('Fetching {}'.format(url))
        if outfile is not None:
            run('wget -nc "{}" -P {} -O {}'.format(url, outdir, outfile))
        else:
            run('wget -N "{}" -P {}'.format(url, outdir))

    @all_clients
    def terminate_client_processes(self):
        for arg in self.CLIENT_PROCESSES_ARGS:
            logger.info('Killing client process with argument {}'.format(arg))
            run('pkill -f {}'.format(arg), warn_only=True, quiet=True)
        run('killall -9 {}'.format(' '.join(self.CLIENT_PROCESSES)), quiet=True)

    @all_clients
    def init_repo(self, worker_home: str):
        run('rm -fr {}'.format(worker_home))
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
                    'nohup env/bin/celery worker -A perfrunner.helpers.worker '
                    '-l INFO -Q {0} -n {0} -C --discard '
                    '&>worker_{0}.log &'.format(worker), pty=False)

    def clone_git_repo(self, repo: str, branch: str, worker_home: str):
        logger.info('Cloning repository: {} branch {}'.format(
            repo, branch))
        with cd(worker_home), cd('perfrunner'):
            run('git clone -q -b {} {}'.format(branch, repo))

    @all_clients
    def clone_ycsb(self, repo: str, branch: str, worker_home: str):
        self.clone_git_repo(repo=repo, branch=branch, worker_home=worker_home)

    @all_clients
    def clone_bigfun(self, socialgen_repo: str, socialgen_branch: str,
                     loader_repo: str, loader_branch: str, worker_home: str):
        self.clone_git_repo(repo=socialgen_repo, branch=socialgen_branch, worker_home=worker_home)
        self.clone_git_repo(repo=loader_repo, branch=loader_branch, worker_home=worker_home)
        with cd(worker_home), cd('perfrunner'):
            with cd('socialGen'):
                cmd = "mvn clean package"
                run(cmd)
            with cd('loader'):
                cmd = "mvn clean package"
                run(cmd)

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
    def get_bigfun_export_files(self, worker_home: str):
        logger.info('Collecting Bigfun export files')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat loader/*.result', quiet=True)
            if not r.return_code:
                get('loader/*.result', local_path='loader/')

    def generate_doctemplates(self, host: str, worker_home: str, workers: int,
                              user_docs: int, clientn: int):
        with settings(host_string=host):
            with cd(worker_home), cd('perfrunner'):
                with cd('socialGen'):
                    cmd = "bash ./scripts/initb.sh /workspace/bigfundata" \
                          " {partitions} {clientn} {users}" \
                          " -f JSON -k STRING#%015d > socialGen.log".format(users=user_docs,
                                                                            partitions=workers,
                                                                            clientn=clientn)
                    run(cmd)

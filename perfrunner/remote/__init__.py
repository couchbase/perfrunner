import os
import shutil

from fabric.api import cd, get, run, settings, shell_env

from logger import logger
from perfrunner.remote.context import all_clients, master_client
from perfrunner.settings import REPO


class Remote:

    CLIENT_PROCESSES = 'celery', 'cbc-pillowfight', 'memcached'

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
        run('killall -9 {}'.format(' '.join(self.CLIENT_PROCESSES)), quiet=True)

    @all_clients
    def init_repo(self, worker_home: str):
        run('rm -fr {}'.format(worker_home))
        run('mkdir -p {}'.format(worker_home))

        with cd(worker_home):
            run('git clone -q {}'.format(REPO))
            with cd('perfrunner'):
                run('make')

    @all_clients
    def install_clients(self, perfrunner_home, test_config):
        client_settings = test_config.client_settings.__dict__
        py_version = client_settings['python_client']
        if py_version is not None:
            with cd(perfrunner_home):
                if 'review.couchbase.org' in py_version or "github" in py_version:
                    run("env/bin/pip install {} --no-cache-dir".format(py_version), quiet=True)
                else:
                    run("env/bin/pip install couchbase=={} "
                        "--no-cache-dir".format(py_version), quiet=True)

    @master_client
    def remote_copy(self, worker_home: str):
        with cd(worker_home):
            with cd('perfrunner'):
                logger.info('move couchbase package to perfrunner')
                run('mv /tmp/couchbase.rpm ./')

    def start_celery_worker(self, worker, worker_home, broker_url):
        with settings(host_string=worker):
            with cd(worker_home), shell_env(PYTHONOPTIMIZE='1',
                                            PYTHONWARNINGS='ignore',
                                            C_FORCE_ROOT='1'):
                run('ulimit -n 10240; '
                    'WORKER_TYPE=remote '
                    'BROKER_URL={1} '
                    'nohup env/bin/celery -A perfrunner.helpers.worker worker '
                    '-l INFO -Q {0} -n {0} --discard '
                    '&>worker_{0}.log &'.format(worker, broker_url), pty=False)

    def clone_git_repo(self, repo: str, branch: str, worker_home: str, commit: str = None):
        logger.info('Cloning repository: {} branch {}'.format(repo, branch))
        with cd(worker_home), cd('perfrunner'):
            run('git clone -q -b {} {}'.format(branch, repo))
        if commit:
            with cd(worker_home), cd('perfrunner'), cd(repo):
                run('git checkout {}'.format(commit))

    @all_clients
    def init_ycsb(self, repo: str, branch: str, worker_home: str, sdk_version: None):
        shutil.rmtree("YCSB", ignore_errors=True)
        self.clone_git_repo(repo=repo, branch=branch, worker_home=worker_home)
        if sdk_version is not None:
            sdk_version = sdk_version.replace(":", ".")
            major_version = sdk_version.split(".")[0]
            with cd(worker_home), cd('perfrunner'), cd('YCSB'):
                cb_version = "couchbase"
                if major_version == "1":
                    cb_version += ""
                else:
                    cb_version += major_version
                original_string = '<{0}.version>*.*.*<\\/{0}.version>'.format(cb_version)
                new_string = '<{0}.version>{1}<\\/{0}.version>'.format(cb_version, sdk_version)
                cmd = "sed -i 's/{}/{}/g' pom.xml".format(original_string, new_string)
                run(cmd)

    @all_clients
    def init_tpcds_couchbase_loader(self, repo: str, branch: str, worker_home: str):
        self.clone_git_repo(repo, branch, worker_home)
        with cd(worker_home), \
                cd('perfrunner'), \
                cd("cbas-perf-support"), \
                cd("tpcds-couchbase-loader"):
            run('mvn install')

    @all_clients
    def init_java_dcp_client(self, repo: str, branch: str, worker_home: str, commit: str = None):
        self.clone_git_repo(repo, branch, worker_home, commit)
        with cd(worker_home), cd('perfrunner'), cd("java-dcp-client"):
            run('perf/build.sh')

    @all_clients
    def init_jts(self, repo: str, branch: str, worker_home: str, jts_home: str):
        self.clone_git_repo(repo, branch, worker_home)
        with cd(worker_home), cd('perfrunner'), cd(jts_home):
            run('mvn install')

    @all_clients
    def get_jts_logs(self, worker_home: str, jts_home: str, local_dir: str):
        logger.info("Collecting remote JTS logs")
        jts_logs_dir = "{}/logs".format(jts_home)
        local_dir = "{}/logs/".format(local_dir)
        if not os.path.exists(local_dir):
            os.mkdir(local_dir)
        with cd(worker_home), cd('perfrunner'), cd(jts_logs_dir):
            directory = run('ls -d */')
            r = run('stat {}*.log'.format(directory), quiet=True)
            if not r.return_code:
                get('{}*.log'.format(directory), local_path=local_dir)

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

    @master_client
    def extract_cb(self, filename: str, worker_home: str):
        logger.info('Extracting couchbase.rpm')
        with cd(worker_home), cd('perfrunner'):
            run('rpm2cpio ./{} | cpio -idm'.format(filename))

    @master_client
    def get_ch2_logfile(self, worker_home: str, logfile: str):
        logger.info('Collecting CH2 log')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat {}*.log'.format(logfile), quiet=True)
            if not r.return_code:
                get('{}*.log'.format(logfile), local_path='./')

    @master_client
    def init_ch2(self, repo: str, branch: str, worker_home: str):
        self.clone_git_repo(repo=repo, branch=branch, worker_home=worker_home)

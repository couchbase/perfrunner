import os
import shutil

from fabric.api import cd, get, run, settings, shell_env

from logger import logger
from perfrunner.helpers.misc import get_python_sdk_installation
from perfrunner.remote.context import (
    all_clients,
    all_clients_batch,
    master_client,
    syncgateway_servers,
)
from perfrunner.settings import REPO, ClusterSpec


class Remote:

    CLIENT_PROCESSES = 'celery', 'cbc-pillowfight', 'memcached', 'cblite'

    def __init__(self, cluster_spec: ClusterSpec):
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
            with cd("perfrunner"):
                run('make')

    @all_clients
    def install_clients(self, perfrunner_home: str, python_client: str, need_pymongo: bool = False):
        if python_client is not None:
            logger.info('Installing Python SDK on remote client: {}'.format(python_client))
            with cd(perfrunner_home):
                package = get_python_sdk_installation(python_client)
                run("PYCBC_USE_CPM_CACHE=OFF env/bin/pip install {} "
                    "--no-cache-dir".format(package), warn_only=True)

                if need_pymongo:
                    run("env/bin/pip install pymongo --no-cache-dir", warn_only=True)


    @master_client
    def remote_copy(self, worker_home: str):
        with cd(worker_home):
            with cd('perfrunner'):
                logger.info('move couchbase package to perfrunner')
                run('mv /tmp/couchbase.deb ./ || mv /tmp/couchbase.rpm ./')

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
        repo = repo.replace("git://", "https://")
        logger.info('Cloning repository: {} branch {}'.format(repo, branch))
        with cd(worker_home), cd('perfrunner'):
            run('git clone -q -b {} {}'.format(branch, repo))
        if commit:
            with cd(worker_home), cd('perfrunner'), cd(repo):
                run('git checkout {}'.format(commit))

    @all_clients
    def build_ycsb(self, worker_home: str, ycsb_client: str):
        cmd = 'pyenv local 2.7.18 && bin/ycsb build {}'.format(ycsb_client)

        logger.info('Running: {}'.format(cmd))
        with cd(worker_home), cd('perfrunner'), cd('YCSB'):
            run(cmd)

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
        with cd(worker_home), cd('perfrunner'), cd(jts_logs_dir):
            directories = run('ls -d */')
            directories = directories.split()
            for directory in directories:
                target_dir = "{}/{}".format(local_dir, directory)
                if not os.path.exists(target_dir):
                    os.mkdir(target_dir)
                r = run('stat {}*.log'.format(directory), quiet=True)
                if not r.return_code:
                    get('{}*.log'.format(directory), local_path=target_dir)

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
            r = run('stat YCSB/ycsb_*.log', quiet=True)
            if not r.return_code:
                get('YCSB/ycsb_*.log', local_path='YCSB/')

    @all_clients
    def get_gsi_measurements(self, worker_home: str):
        logger.info('Collecting GSI measurements')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat result.json', quiet=True)
            if not r.return_code:
                get('result.json', local_path='.')
            r = run('stat /root/statsfile', quiet=True)
            if not r.return_code:
                get('/root/statsfile', local_path='/root/')

    @master_client
    def extract_cb_any(self, filename: str, worker_home: str):
        logger.info('Extracting couchbase archive')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat {}.deb'.format(filename), quiet=True)
            if not r.return_code:
                logger.info('Extracting couchbase.deb')
                with settings(shell='/bin/bash -l -c'):  # Ignore pipefails for this command
                    run('ar p {}.deb data.tar.xz | unxz | tar x'.format(filename))
            else:
                logger.info('Extracting couchbase.rpm')
                run('rpm2cpio ./{}.rpm | cpio -idm'.format(filename), quiet=True)

    @all_clients
    def get_ch2_logfile(self, worker_home: str, logfile: str):
        logger.info('Collecting CH2 log')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat {}*.log'.format(logfile), quiet=True)
            if not r.return_code:
                get('{}*.log'.format(logfile), local_path='./')

    @all_clients
    def init_ch2(self, repo: str, branch: str, worker_home: str):
        self.clone_git_repo(repo=repo, branch=branch, worker_home=worker_home)

    @all_clients
    def clone_ycsb(self, repo: str, branch: str, worker_home: str, ycsb_instances: int):
        repo = repo.replace("git://", "https://")
        logger.info('Cloning YCSB repository: {} branch {}'.format(
            repo, branch))

        for instance in range(ycsb_instances):
            with cd(worker_home), cd('perfrunner'):
                run('git clone -q -b {} {}'.format(branch, repo))
                run('mv YCSB YCSB_{}'.format(instance+1))

        with cd(worker_home), cd('perfrunner'):
            run('git clone -q -b {} {}'.format(branch, repo))

    @all_clients
    def build_syncgateway_ycsb(self, worker_home: str, ycsb_instances: int):
        logger.info('Building YCSB jar...')
        for instance in range(ycsb_instances):
            with cd(worker_home), cd('perfrunner'), cd('YCSB_{}'.format(instance+1)):
                run('mvn -pl com.yahoo.ycsb:syncgateway-binding '
                    '-am package '
                    '-DskipTests dependency:build-classpath '
                    '-DincludeScope=compile '
                    '-Dmdep.outputFilterFile=true')
                run('mvn -pl com.yahoo.ycsb:couchbase2-binding '
                    '-am package '
                    '-DskipTests dependency:build-classpath '
                    '-DincludeScope=compile '
                    '-Dmdep.outputFilterFile=true')

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

    def get_cblite_logs(self, worker, instance_id, local_dir):
        localpath = "{}/".format(local_dir)
        pattern = "serve_db_{}.log".format(instance_id)
        logger.info('Collecting cblite serve logs')
        with settings(host_string=worker):
            with cd('/root'):
                r = run('stat {}'.format(pattern), quiet=True)
                if not r.return_code:
                    get('{}'.format(pattern), local_path=localpath)
                    try:
                        os.rename(localpath+pattern, localpath+"{}_{}".format(worker, pattern))
                    except Exception as ex:
                        print(ex)

    @all_clients
    def download_blackholepuller(self, worker_home: str):
        logger.info("downloading blackholepuller")
        with cd('/root/sg_dev_tools_new/replicator/blackholePuller'):
            run('cp blackholePuller /tmp/perfrunner/perfrunner/')
            run('chmod +x /tmp/perfrunner/perfrunner/blackholePuller')

    @all_clients
    def download_newdocpusher(self, worker_home: str):
        logger.info('downloading newdocpusher')
        with cd('/root/sg_dev_tools_new/replicator/newDocPusher'):
            run('cp newDocPusher /tmp/perfrunner/perfrunner/')
            run('chmod +x /tmp/perfrunner/perfrunner/newDocPusher')

    @all_clients
    def get_sg_blackholepuller_logs(self, worker_home, sgs):
        pattern = "*{}*".format('_blackholepuller_')
        logger.info('Collecting SG blackholepuller logs')
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
        logger.info('Collecting SG newdocpusher logs')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat {}'.format(pattern), quiet=True)
            if not r.return_code:
                get('{}'.format(pattern), local_path='.')

    @all_clients
    def get_newdocpusher_result_files(self):
        logger.info('Collecting newdocpusher result files')
        with cd('/tmp/perfrunner'), cd('perfrunner'):
            r = run('stat sg_newdocpusher_result*.log', quiet=True)
            if not r.return_code:
                get('sg_newdocpusher_result*.log', local_path='/')

    @all_clients
    def create_cblite_directory(self):
        logger.info('Creating cblite directory')
        run('rm -rf /tmp/couchbase-mobile-tools/', quiet=True)
        run('mkdir /tmp/couchbase-mobile-tools/', quiet=True)
        run('chmod 777 /tmp/couchbase-mobile-tools', quiet=True)

    @all_clients
    def create_cblite_ramdisk(self, ramdisk_size):
        logger.info('Creating cblite ramdisk')
        run('mount -t tmpfs -o size={}m cbliteramdisk /tmp/couchbase-mobile-tools'.
            format(ramdisk_size), quiet=True, warn_only=True)

    @all_clients
    def destroy_cblite_ramdisk(self):
        logger.info('Destroying cblite ramdisk')
        run('umount /tmp/couchbase-mobile-tools/', quiet=True, warn_only=True)

    @all_clients
    def clone_cblite(self):
        logger.info('Cloning cblite')
        with cd('/tmp/couchbase-mobile-tools/'):
            run('git clone https://github.com/couchbaselabs/couchbase-mobile-tools .')
            run('git checkout dbfb5f53e57ccdba9bc63693d580a2b7cdc2aa0d')

    def build_cblite(self):
        logger.info('Building cblite: updating submodule...')
        try:
            self.cblite_update_submodule()
        except Exception as ex:
            logger.info("{}".format(ex))
        logger.info('Building cblite: checking out version...')
        try:
            self.cblite_checkout_version()
        except Exception as ex:
            logger.info("{}".format(ex))
        logger.info('Building cblite: creating build directory...')
        try:
            self.cblite_make_build_dir()
        except Exception as ex:
            logger.info("{}".format(ex))
        logger.info('Building cblite: running cmake...')
        try:
            self.cblite_build_cmake()
        except Exception as ex:
            logger.info("{}".format(ex))
        logger.info('Building cblite: running make...')
        try:
            self.cblite_build_make()
        except Exception as ex:
            logger.info("{}".format(ex))

    @all_clients_batch
    def cblite_update_submodule(self):
        with cd('/tmp/couchbase-mobile-tools/'):
            run('git submodule update --init --recursive', quiet=True)

    @all_clients_batch
    def cblite_checkout_version(self):
        with cd('/tmp/couchbase-mobile-tools/vendor/couchbase-lite-core'):
            run('git checkout 8b3bb4f1e0942c596a2fe1a3e903e8f778ba9b66', quiet=True)

    @all_clients_batch
    def cblite_make_build_dir(self):
        with cd('/tmp/couchbase-mobile-tools/cblite/'):
            run('mkdir build_cmake', quiet=True)

    @all_clients_batch
    def cblite_build_cmake(self):
        with cd('/tmp/couchbase-mobile-tools/cblite/build_cmake'):
            run('/usr/bin/cmake ..', quiet=True)

    @all_clients_batch
    def cblite_build_make(self):
        with cd('/tmp/couchbase-mobile-tools/cblite/build_cmake'):
            run('/usr/bin/make -j 5', quiet=True)

    def start_cblitedb_continuous(self, worker: str, db_name: str, port: int, verbose: int = 1,
                                  collection: dict = {}):
        cblite = '/tmp/couchbase-mobile-tools/cblite/build_cmake/cblite'
        db_path = '/tmp/couchbase-mobile-tools/{}.cblite2'.format(db_name)
        cmd = '{} --create serve {} --port {} {} &>serve_{}.log & '\
            .format(cblite, '--verbose' if verbose else '', port, db_path, db_name)
        logger.info(cmd)
        with settings(host_string=worker):
            run(cmd, pty=False)

        # Create collections
        for scope, coll in collection.items():
            if scope == '_default':
                continue
            for col_name, _ in coll.items():
                cmd = '{} mkcoll {} {}/{}'.format(cblite, db_path, scope, col_name)
                with settings(host_string=worker):
                    run(cmd, pty=False)

    def start_cblite_replication_aws_pull(self,
                                          worker,
                                          sgw_host,
                                          sgw_port,
                                          cblite_db,
                                          user,
                                          password):
        cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite pull --replicate'\
              ' --continuous --user {0}:{1} wss://{2}:{3}/db-1'\
              ' /tmp/couchbase-mobile-tools/{4}.cblite2 &>pull_{0}.log &'\
              .format(user, password, sgw_host, sgw_port, cblite_db)
        logger.info(cmd)
        with settings(host_string=worker):
            run(cmd, pty=False)

    def start_cblite_replication_aws_push(self,
                                          worker,
                                          sgw_host,
                                          sgw_port,
                                          cblite_db,
                                          user,
                                          password):
        cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite push --replicate'\
              ' --continuous --user {0}:{1} /tmp/couchbase-mobile-tools/{2}.cblite2'\
              '  wss://{3}:{4}/db-1 &>push_{0}.log &'\
              .format(user, password, cblite_db, sgw_host, sgw_port)
        logger.info(cmd)
        with settings(host_string=worker):
            run(cmd, pty=False)

    def start_cblite_replication_aws_bidi(self,
                                          worker,
                                          sgw_host,
                                          sgw_port,
                                          cblite_db,
                                          user,
                                          password):
        cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite cp --replicate --bidi'\
              ' --continuous --user {0}:{1} wss://{2}:{3}/db-1'\
              ' /tmp/couchbase-mobile-tools/{4}.cblite2 &>bidi_{0}.log &'\
              .format(user, password, sgw_host, sgw_port, cblite_db)
        logger.info(cmd)
        with settings(host_string=worker):
            run(cmd, pty=False)

    @all_clients
    def kill_cblite(self):
        logger.info("cleaning up cblite db continuous")
        cmd = "killall -9 cblite"
        with settings(quiet=True, warn_only=True):
            run(cmd)

    @all_clients
    def modify_fd_limits(self):
        logger.info("modifying fd limits")
        with settings(quiet=True, warn_only=True):
            run("sed -i 's/LimitNOFILE=70000/LimitNOFILE=1000000/")

    @all_clients
    def modify_tcp_settings(self):
        logger.info("modifying tcp settings")
        with settings(quiet=True, warn_only=True):
            run("sysctl net.ipv4.ip_local_port_range=1024")
            run("sysctl net.ipv4.tcp_fin_timeout=15")
            run("sysctl net.ipv4.tcp_tw_recycle=0")
            run("sysctl net.ipv4.tcp_tw_reuse=0")

    def replicate_push_continuous(self, worker: str, db_name: str, sgw_ip: str):
        cmd = '/tmp/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'push --continuous --user guest:guest /tmp/couchbase-mobile-tools/{0}.cblite2 ' \
              'ws://{1}:4984/db-1 &>push_{0}.log &'.format(db_name, sgw_ip)
        logger.info(cmd)
        with settings(host_string=worker):
            run(cmd, pty=False)

    def replicate_pull_continuous(self, worker: str, db_name: str, sgw_ip: str):
        cmd = '/tmp/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'pull --continuous --user guest:guest /tmp/couchbase-mobile-tools/{0}.cblite2 ' \
              'ws://{1}:4984/db-1 &>pull_{0}.log &'.format(db_name, sgw_ip)
        logger.info(cmd)
        with settings(host_string=worker):
            run(cmd, pty=False)

    @all_clients
    def cleanup_cblite_db_coninuous(self):
        logger.info("cleaning up cblite db continuous")
        cmd = 'rm -rf *.cblite2'
        with cd('/tmp/couchbase-mobile-tools/'):
            run(cmd)

    @syncgateway_servers
    def build_troublemaker(self):
        with cd('/tmp/'):
            run('rm -rf troublemaker', quiet=True)
            run('mkdir troublemaker')
        with cd('/tmp/troublemaker/'):
            run('wget '
                'https://github.com/couchbaselabs/TroublemakerProxy/'
                'releases/download/v1.2.4/TroublemakerProxy-v1.2.4-linux-x64.tar.gz')
            run('tar -xvf TroublemakerProxy-v1.2.4-linux-x64.tar.gz ')
        with cd('/tmp/troublemaker/TroublemakerProxy-v1.2.4-linux-x64/'):
            run('mkdir no_compression_plugin')
        with cd('/tmp/troublemaker/TroublemakerProxy-v1.2.4-linux-x64/no_compression_plugin/'):
            run('wget '
                'https://github.com/couchbaselabs/TroublemakerProxy/'
                'releases/download/v1.2.4/NoCompressionPlugin-v1.2.4.zip')
            run('yum install unzip -y')
            run('unzip NoCompressionPlugin-v1.2.4.zip ')
        with cd('/tmp/troublemaker/TroublemakerProxy-v1.2.4-linux-x64/'):
            run('''echo '{"FromPort": 4900, "ToPort": 4984, "Plugins":
                [{"Path": "/tmp/troublemaker/TroublemakerProxy-v1.2.4-linux-x64/
                no_compression_plugin/NoCompressionPlugin.dll"}]}' > troublemaker.json''',
                shell=False)
        run('/tmp/troublemaker/TroublemakerProxy-v1.2.4-linux-x64/TroublemakerProxy '
            '--config /tmp/troublemaker/TroublemakerProxy-v1.2.4-linux-x64/troublemaker.json '
            '&>troublemaker.log &', pty=False)

    @syncgateway_servers
    def kill_troublemaker(self):
        logger.info("killing troublemaker")
        cmd = " killall -9 TroublemakerProxy"
        with settings(quiet=True, warn_only=True):
            run(cmd)

    @all_clients
    def download_vectordb_bench(self, repo: str, branch: str, worker_home: str):
        shutil.rmtree("VectorDBBench", ignore_errors=True)
        self.clone_git_repo(repo=repo, branch=branch, worker_home=worker_home)

    @all_clients
    def build_vectordb_bench(self, worker_home: str):
        logger.info("Building VectorDB Bench")
        with cd(worker_home), cd("perfrunner"), cd("VectorDBBench"):
            run("make")

    @all_clients
    def get_vectordb_result_files(self, worker_home: str, pattern: str):
        logger.info("Collecting VectorDB result files")
        with cd(worker_home), cd("perfrunner"):
            r = run(f"stat {pattern}", quiet=True)
            if not r.return_code:
                get(pattern, local_path="VectorDBBench/")

    @all_clients
    def update_pyenv_and_install_python(self, py_version: str = None):
        """Update pyenv and maybe install another python version if provided."""
        logger.info("Updating pyenv ...")
        with cd("/root/.pyenv/"):
            run("git pull")

        if py_version:
            logger.info(f"Installing python {py_version} using pyenv")
            run(f"pyenv install {py_version} -s")
            # A workaround for ModuleNotFoundError pip._vendor.six issue
            logger.info("Updating virtualenv")
            # Remove any root installation of virtualenv
            run("pip uninstall --yes virtualenv", warn_only=True)
            run("pip3 uninstall --yes virtualenv", warn_only=True)
            run("apt purge -y python3-virtualenv", warn_only=True)
            run(f"pyenv local {py_version} && yes | pip install virtualenv", warn_only=True)

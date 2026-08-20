import os
import shutil
from typing import Optional
from uuid import uuid4

from logger import logger
from perfrunner.helpers.local import _resolve_repo_url, _sanitize_repo_url
from perfrunner.helpers.misc import get_python_sdk_installation
from perfrunner.remote.api import cd, env, get, hide, run, settings, shell_env
from perfrunner.remote.context import (
    all_clients,
    all_clients_batch,
    master_client,
    syncgateway_servers,
)
from perfrunner.settings import REPO, ClusterSpec

# Skip checkstyle incase upstream branch doesn't enforce it.
# Also configure stack size for clients that haven't set anything.
YCSB_MAVEN_OPTS = "-Dcheckstyle.skip=true -Xss16m"


class Remote:
    CLIENT_PROCESSES = "celery", "cbc-pillowfight", "memcached", "cblite", "com.yahoo.ycsb", "tpcc"

    def __init__(self, cluster_spec: ClusterSpec):
        self.cluster_spec = cluster_spec

    @staticmethod
    def wget(url, outdir='/tmp', outfile=None):
        logger.info(f"Fetching {url}")
        if outfile is not None:
            run(f'wget -nc "{url}" -P {outdir} -O {outfile}')
        else:
            run(f'wget -N "{url}" -P {outdir}')

    @all_clients
    def terminate_client_processes(self):
        run(f"killall -9 {' '.join(self.CLIENT_PROCESSES)}", quiet=True)
        # Kill the rest of client processes by pattern matching.
        run(
            f"kill -9 $(ps -eo pid,cmd | grep -E '{'|'.join(self.CLIENT_PROCESSES)}' "
            "| awk '{print $1}')",
            quiet=True,
        )

    @all_clients
    def init_repo(self, worker_home: str, cherrypick: Optional[str] = None):
        run(f"rm -fr {worker_home}")
        run(f"mkdir -p {worker_home}")

        with cd(worker_home):
            run(f"git clone -q {REPO}")
            with cd("perfrunner"):
                if cherrypick:
                    run(cherrypick)
                run('make')

    @all_clients
    def install_clients(self, perfrunner_home: str, python_client: str, need_pymongo: bool = False):
        if python_client is not None:
            logger.info(f"Installing Python SDK on remote client: {python_client}")
            with cd(perfrunner_home):
                package = get_python_sdk_installation(python_client)
                run(
                    f"PYCBC_USE_CPM_CACHE=OFF env/bin/pip install {package} --no-cache-dir",
                    warn_only=True,
                )

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
                run(
                    "ulimit -n 10240; "
                    "WORKER_TYPE=remote "
                    f"BROKER_URL={broker_url} "
                    "nohup env/bin/celery -A perfrunner.helpers.worker worker "
                    f"-l INFO -Q {worker} -n {worker} --discard "
                    f"&>worker_{worker}.log &",
                    pty=False,
                )

    def clone_git_repo(
        self,
        repo: str,
        branch: str,
        worker_home: str,
        commit: Optional[str] = None,
        cherrypick: Optional[str] = None,
        target_dir: Optional[str] = None,
    ):
        repo = repo.replace("git://", "https://")
        repo = _resolve_repo_url(repo)
        logger.info(f"Cloning repository: {_sanitize_repo_url(repo)} branch {branch}")
        repo_name = target_dir or repo.split("/")[-1].split(".")[0]

        with cd(worker_home), cd("perfrunner"):
            clone_cmd = f"git clone -q -b {branch} {repo}"
            if target_dir:
                clone_cmd += f" {target_dir}"
            run(clone_cmd)

        if commit:
            with cd(worker_home), cd("perfrunner"), cd(repo_name):
                run(f"git checkout {commit}")

        if cherrypick:
            with cd(worker_home), cd("perfrunner"), cd(repo_name):
                run(cherrypick)

    @all_clients
    def build_ycsb(self, worker_home: str, ycsb_client: str):
        cmd = f"pyenv local 2.7.18 && bin/ycsb build {ycsb_client}"

        logger.info(f"Running: {cmd}")
        with cd(worker_home), cd("perfrunner"), cd("YCSB"):
            with settings(hide("output", "warnings"), warn_only=True):
                result = run(cmd)
            if result.return_code == 0:
                return

            logger.warning(
                "bin/ycsb build is not supported by this branch; falling back to direct Maven build"
            )
            mvn_cmd = f"mvn -pl ./{ycsb_client} -am package -DskipTests"
            logger.info(f"Running: {mvn_cmd}")
            with shell_env(MAVEN_OPTS=YCSB_MAVEN_OPTS):
                run(mvn_cmd)

    @all_clients
    def init_ycsb(self, repo: str, branch: str, worker_home: str, sdk_version: None):
        shutil.rmtree("YCSB", ignore_errors=True)
        self.clone_git_repo(repo=repo, branch=branch, worker_home=worker_home, target_dir="YCSB")
        if sdk_version is not None:
            sdk_version = sdk_version.replace(":", ".")
            major_version = sdk_version.split(".")[0]
            with cd(worker_home), cd('perfrunner'), cd('YCSB'):
                cb_version = "couchbase"
                if major_version == "1":
                    cb_version += ""
                else:
                    cb_version += major_version
                original_string = f"<{cb_version}.version>*.*.*<\\/{cb_version}.version>"
                new_string = f"<{cb_version}.version>{sdk_version}<\\/{cb_version}.version>"
                cmd = f"sed -i 's/{original_string}/{new_string}/g' pom.xml"
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
    def clear_jts_logs(self, worker_home: str, jts_home: str):
        """Remove previously collected JTS logs on the workers for run isolation."""
        with cd(worker_home), cd('perfrunner'), cd(jts_home):
            before = run("ls logs 2>/dev/null | wc -l", quiet=True)
            run("rm -rf logs", quiet=True)
            run("mkdir -p logs", quiet=True)
            logger.info(f"Cleared JTS logs on worker (had {str(before).strip()} entries)")

    @all_clients
    def get_jts_logs(self, worker_home: str, jts_home: str, local_dir: str):
        logger.info("Collecting remote JTS logs")
        jts_logs_dir = f"{jts_home}/logs"
        with cd(worker_home), cd('perfrunner'), cd(jts_logs_dir):
            directories = run('ls -d */')
            directories = directories.split()
            for directory in directories:
                target_dir = f"{local_dir}/{directory}"
                if not os.path.exists(target_dir):
                    os.mkdir(target_dir)
                r = run(f"stat {directory}*.log", quiet=True)
                if not r.return_code:
                    get(f"{directory}*.log", local_path=target_dir)

    @all_clients
    def get_celery_logs(self, worker_home: str):
        logger.info('Collecting remote Celery logs')
        with cd(worker_home), cd('perfrunner'):
            r = run('stat worker_*.log', quiet=True)
            if not r.return_code:
                get('worker_*.log', local_path='celery/')

    @all_clients
    def get_pprof_files(self, worker_home: str):
        logger.info("Collecting remote pprof files")
        target_dir = "pprof"
        with cd(worker_home), cd("perfrunner"):
            r = run("stat *.pprof", quiet=True)
            if not r.return_code:
                if not os.path.exists(target_dir):
                    os.mkdir(target_dir)
                get("*.pprof", local_path=target_dir)

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
            r = run(f"stat {filename}.deb", quiet=True)
            if not r.return_code:
                logger.info('Extracting couchbase.deb')
                with settings(shell='/bin/bash -l -c'):  # Ignore pipefails for this command
                    run(f"ar p {filename}.deb data.tar.xz | unxz | tar x")
            else:
                logger.info('Extracting couchbase.rpm')
                run(f"rpm2cpio ./{filename}.rpm | cpio -idm", quiet=True)

    @all_clients
    def get_ch2_logfile(self, worker_home: str, logfile: str):
        logger.info('Collecting CH2 log')
        with cd(worker_home), cd('perfrunner'):
            r = run(f"stat {logfile}*.log", quiet=True)
            if not r.return_code:
                get(f"{logfile}*.log", local_path="./")

    @all_clients
    def init_ch2(self, repo: str, branch: str, worker_home: str, cherrypick: Optional[str] = None):
        self.clone_git_repo(
            repo=repo, branch=branch, worker_home=worker_home, cherrypick=cherrypick
        )

    @all_clients
    def clone_ycsb(self, repo: str, branch: str, worker_home: str, ycsb_instances: int):
        repo = repo.replace("git://", "https://")
        repo = _resolve_repo_url(repo)
        logger.info(f"Cloning YCSB repository: {_sanitize_repo_url(repo)} branch: {branch}")

        for instance in range(ycsb_instances):
            with cd(worker_home), cd('perfrunner'):
                run(f"git clone -q -b {branch} {repo} YCSB")
                run(f"mv YCSB YCSB_{instance + 1}")

        with cd(worker_home), cd('perfrunner'):
            run(f"git clone -q -b {branch} {repo} YCSB")

    @all_clients
    def build_syncgateway_ycsb(self, worker_home: str, ycsb_instances: int):
        logger.info('Building YCSB jar...')
        for instance in range(ycsb_instances):
            with cd(worker_home), cd("perfrunner"), cd(f"YCSB_{instance + 1}"):
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
        localpath = f"{local_dir}/"
        instances = int(sgs.instances_per_client)
        pattern = f"{sgs.log_title}*"
        logger.info('Collecting YCSB logs')
        with cd(worker_home), cd('perfrunner'):
            r = run(f"stat YCSB/{pattern}", quiet=True)
            if not r.return_code:
                get(f"YCSB/{pattern}", local_path=localpath)
            for i in range(instances):
                r = run(f"stat YCSB_{i + 1}/{pattern}", quiet=True)
                if not r.return_code:
                    get(f"YCSB_{i + 1}/{pattern}", local_path=local_dir)

    def get_cblite_logs(self, worker, instance_id, local_dir):
        localpath = f"{local_dir}/"
        pattern = f"serve_db_{instance_id}.log"
        logger.info('Collecting cblite serve logs')
        with settings(host_string=worker):
            with cd('/root'):
                r = run(f"stat {pattern}", quiet=True)
                if not r.return_code:
                    get(f"{pattern}", local_path=localpath)
                    try:
                        os.rename(localpath + pattern, localpath + f"{worker}_{pattern}")
                    except Exception as ex:
                        print(ex)

    @all_clients
    def download_blackholepuller(self, worker_home: str):
        logger.info("downloading blackholepuller")
        with cd('/root/sg_dev_tools/replicator/blackholePuller'):
            run('cp blackholePuller /tmp/perfrunner/perfrunner/')
            run('chmod +x /tmp/perfrunner/perfrunner/blackholePuller')

    @all_clients
    def download_newdocpusher(self, worker_home: str):
        logger.info('downloading newdocpusher')
        with cd('/root/sg_dev_tools/replicator/newDocPusher'):
            run('cp newDocPusher /tmp/perfrunner/perfrunner/')
            run('chmod +x /tmp/perfrunner/perfrunner/newDocPusher')

    @all_clients
    def get_sg_blackholepuller_logs(self, worker_home, sgs):
        pattern = "*{}*".format('_blackholepuller_')
        logger.info('Collecting SG blackholepuller logs')
        with cd(worker_home), cd('perfrunner'):
            r = run(f"stat {pattern}", quiet=True)
            if not r.return_code:
                get(f"{pattern}", local_path=".")

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
            r = run(f"stat {pattern}", quiet=True)
            if not r.return_code:
                get(f"{pattern}", local_path=".")

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
        run(
            f"mount -t tmpfs -o size={ramdisk_size}m cbliteramdisk /tmp/couchbase-mobile-tools",
            quiet=True,
            warn_only=True,
        )

    @all_clients
    def destroy_cblite_ramdisk(self):
        logger.info('Destroying cblite ramdisk')
        run('umount /tmp/couchbase-mobile-tools/', quiet=True, warn_only=True)

    @all_clients
    def clone_cblite(self):
        logger.info('Cloning cblite')
        with cd('/tmp/couchbase-mobile-tools/'):
            run('git clone https://github.com/couchbaselabs/couchbase-mobile-tools .')
            run('git checkout 1755d395131073e173f9a4f984e84084c144c18f')

    def build_cblite(self):
        logger.info('Building cblite: updating submodule...')
        try:
            self.cblite_update_submodule()
        except Exception as ex:
            logger.info(f"{ex}")
        logger.info('Building cblite: checking out version...')
        try:
            self.cblite_checkout_version()
        except Exception as ex:
            logger.info(f"{ex}")
        logger.info('Building cblite: creating build directory...')
        try:
            self.cblite_make_build_dir()
        except Exception as ex:
            logger.info(f"{ex}")
        logger.info('Building cblite: running cmake...')
        try:
            self.cblite_build_cmake()
        except Exception as ex:
            logger.info(f"{ex}")
        logger.info('Building cblite: running make...')
        try:
            self.cblite_build_make()
        except Exception as ex:
            logger.info(f"{ex}")

    @all_clients_batch
    def cblite_update_submodule(self):
        with cd('/tmp/couchbase-mobile-tools/'):
            run('git submodule update --init --recursive', quiet=True)

    @all_clients_batch
    def cblite_checkout_version(self):
        with cd('/tmp/couchbase-mobile-tools/vendor/couchbase-lite-core'):
            run('git checkout 91a72069bcb942c9e1bfb39e4f38abd544e6b742', quiet=True)

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
            run('/usr/bin/make -j 5')

    def start_cblitedb_continuous(self, worker: str, db_name: str, port: int, verbose: int = 1,
                                  collection: dict = {}):
        cblite = '/tmp/couchbase-mobile-tools/cblite/build_cmake/cblite'
        db_path = f"/tmp/couchbase-mobile-tools/{db_name}.cblite2"
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
                cmd = f"{cblite} mkcoll {db_path} {scope}/{col_name}"
                with settings(host_string=worker):
                    run(cmd, pty=False)

    def start_cblite_replication_aws_pull(self,
                                          worker,
                                          sgw_host,
                                          sgw_port,
                                          cblite_db,
                                          user,
                                          password):
        cmd = (
            "nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite pull --replicate"
            f" --continuous --user {user}:{password} wss://{sgw_host}:{sgw_port}/db-1"
            f" /tmp/couchbase-mobile-tools/{cblite_db}.cblite2 &>pull_{user}.log &"
        )
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
        cmd = (
            "nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite push --replicate"
            f" --continuous --user {user}:{password}"
            f" /tmp/couchbase-mobile-tools/{cblite_db}.cblite2"
            f"  wss://{sgw_host}:{sgw_port}/db-1 &>push_{user}.log &"
        )
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
        cmd = (
            "nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite cp --replicate --bidi"
            f" --continuous --user {user}:{password} wss://{sgw_host}:{sgw_port}/db-1"
            f" /tmp/couchbase-mobile-tools/{cblite_db}.cblite2 &>bidi_{user}.log &"
        )
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
        cmd = (
            "/tmp/couchbase-mobile-tools/cblite/build_cmake/cblite "
            f"push --continuous --user guest:guest /tmp/couchbase-mobile-tools/{db_name}.cblite2 "
            f"ws://{sgw_ip}:4984/db-1 &>push_{db_name}.log &"
        )
        logger.info(cmd)
        with settings(host_string=worker):
            run(cmd, pty=False)

    def replicate_pull_continuous(self, worker: str, db_name: str, sgw_ip: str):
        cmd = (
            "/tmp/couchbase-mobile-tools/cblite/build_cmake/cblite "
            f"pull --continuous --user guest:guest /tmp/couchbase-mobile-tools/{db_name}.cblite2 "
            f"ws://{sgw_ip}:4984/db-1 &>pull_{db_name}.log &"
        )
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
    def build_aibench(self, worker_home: str):
        logger.info("Building AI Bench")
        with cd(worker_home), cd("ai_bench"):
            run("echo 3.11.8 > .python-version && make", shell_escape=False, pty=True)

    @all_clients
    def get_aibench_result_files(self, worker_home: str):
        logger.info("Collecting AI Bench result files")
        with cd(worker_home), cd("ai_bench"):
            r = run("stat results/*.json", quiet=True)
            if not r.return_code:
                get("results/*.json", local_path="ai_bench/results/")

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
            run("pyenv local 3.9.7 && yes | pip install virtualenv", warn_only=True)

    @all_clients
    def cleanup_spring_data_files(self, worker_home: str, live_dir: str):
        perfrunner_dir = f"{worker_home}/perfrunner"
        logger.info(f"Deleting all files in {perfrunner_dir}/{live_dir}/ on {env.host_string}")
        with cd(perfrunner_dir):
            # `warn_only` so an unreachable worker does not abort the run. Without it
            # fabric calls `abort()`, which raises `SystemExit` - not an `Exception` - and
            # an unhandled `SystemExit` is printed as nothing at all.
            run(f"rm -rf {live_dir}/*", warn_only=True)

    @all_clients
    def get_spring_data_files(
        self, worker_home: str, file_pattern: str, live_dir: str, remote_snapshot_dir: str
    ):
        """
        Fetch latency data files from `live_dir` on all remote workers into local `live_dir`.

        Then move latency data files from remote worker `live_dir`s to snapshot-specific dirs.
        """
        perfrunner_dir = f"{worker_home}/perfrunner"
        logger.info(
            f"Fetching all files matching {perfrunner_dir}/{live_dir}/{file_pattern} "
            f"from {env.host_string}"
        )
        with cd(perfrunner_dir):
            pattern = f"{live_dir}/{file_pattern}"
            r = run(f"stat {pattern}", quiet=True)
            if not r.return_code:
                # Append a uuid to mark files from different workers without invalidating
                # the glob pattern
                run(f"for f in {pattern}; do mv $f $f-{uuid4().hex[:6]}; done")
                get(pattern, local_path=f"./{live_dir}")
                run(f"mkdir -p {remote_snapshot_dir} && mv {pattern} {remote_snapshot_dir}/")

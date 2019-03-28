import os
import socket
import time
import urllib.parse
from datetime import date
from glob import glob
from sys import platform
from typing import List

from fabric.api import lcd, local, quiet, settings, shell_env
from mc_bin_client.mc_bin_client import MemcachedClient, MemcachedError

from logger import logger
from perfrunner.settings import ClusterSpec


def extract_cb(filename: str):
    cmd = 'rpm2cpio ./{} | cpio -idm'.format(filename)
    with quiet():
        local(cmd)


def cleanup(backup_dir: str):
    logger.info("Cleaning the disk before backup/export")

    # Remove files from the directory, if any.
    local('rm -fr {}/*'.format(backup_dir))

    # Discard unused blocks. Twice.
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)  # Otherwise fstrim won't find the device
    if platform == "linux2":
        local('fstrim -v {0} && fstrim -v {0}'.format(backup_dir))


def drop_caches():
    logger.info('Dropping memory cache')
    local('sync && echo 3 > /proc/sys/vm/drop_caches')


def backup(master_node:  str, cluster_spec: ClusterSpec, threads: int,
           wrapper: bool = False, mode: str = None, compression: bool = False):
    logger.info('Creating a new backup: {}'.format(cluster_spec.backup))

    if not mode:
        cleanup(cluster_spec.backup)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, threads, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, threads, mode, compression)


def compact(cluster_spec: ClusterSpec,
            snapshots: List[str],
            wrapper: bool = False):
    if wrapper:
        return
    cbbackupmgr_compact(cluster_spec, snapshots)


def cbbackupwrapper(master_node: str, cluster_spec: ClusterSpec, threads: int,
                    mode: str):
    postfix = ''
    if mode:
        postfix = '-m {}'.format(mode)

    cmd = './cbbackupwrapper http://{}:8091 {} -u {} -p {} -P {} {}'.format(
        master_node,
        cluster_spec.backup,
        cluster_spec.rest_credentials[0],
        cluster_spec.rest_credentials[1],
        threads,
        postfix,
    )
    logger.info('Running: {}'.format(cmd))
    with lcd('./opt/couchbase/bin'):
        local(cmd)


def cbbackupmgr_backup(master_node: str, cluster_spec: ClusterSpec,
                       threads: int, mode: str, compression: bool):
    if not mode:
        local('./opt/couchbase/bin/cbbackupmgr config '
              '--archive {} --repo default'.format(cluster_spec.backup))

    cmd = \
        './opt/couchbase/bin/cbbackupmgr backup ' \
        '--archive {} --repo default --threads {} ' \
        '--host http://{} --username {} --password {}'.format(
            cluster_spec.backup,
            threads,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
        )

    if compression:
        cmd = '{} --value-compression compressed'.format(cmd)

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def get_backup_snapshots(cluster_spec: ClusterSpec) -> List[str]:
    cmd = \
        './opt/couchbase/bin/cbbackupmgr list ' \
        '--archive {} --repo default '.format(
            cluster_spec.backup,
        )

    pattern = '+ {}-'.format(date.today().year)
    snapshots = []
    for line in local(cmd, capture=True).split('\n'):
        if pattern in line:
            snapshot = line.strip().split()[-1]
            snapshots.append(snapshot)
    return snapshots


def cbbackupmgr_merge(cluster_spec: ClusterSpec, snapshots: List[str]):
    cmd = \
        './opt/couchbase/bin/cbbackupmgr merge ' \
        '--archive {} --repo default --start {} --end {}'.format(
            cluster_spec.backup,
            snapshots[0],
            snapshots[1],
        )

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def calc_backup_size(cluster_spec: ClusterSpec) -> float:
    backup_size = local('du -sb0 {}'.format(cluster_spec.backup), capture=True)
    backup_size = backup_size.split()[0]
    backup_size = float(backup_size) / 2 ** 30  # B -> GB

    return round(backup_size)


def restore(master_node: str, cluster_spec: ClusterSpec, threads: int,
            wrapper: bool = False):
    logger.info('Restore from {}'.format(cluster_spec.backup))

    if wrapper:
        cbrestorewrapper(master_node, cluster_spec)
    else:
        cbbackupmgr_restore(master_node, cluster_spec, threads)


def cbrestorewrapper(master_node: str, cluster_spec: ClusterSpec):
    cmd = './cbrestorewrapper {} http://{}:8091 -u {} -p {}'.format(
        cluster_spec.backup,
        master_node,
        cluster_spec.rest_credentials[0],
        cluster_spec.rest_credentials[1],
    )
    logger.info('Running: {}'.format(cmd))
    with lcd('./opt/couchbase/bin'):
        local(cmd)


def cbbackupmgr_restore(master_node: str, cluster_spec: ClusterSpec,
                        threads: int, archive: str = '', repo: str = 'default'):
    cmd = \
        './opt/couchbase/bin/cbbackupmgr restore --force-updates ' \
        '--archive {} --repo {} --threads {} ' \
        '--host http://{} --username {} --password {}'.format(
            archive or cluster_spec.backup,
            repo,
            threads,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
        )
    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbbackupmgr_compact(cluster_spec: ClusterSpec, snapshots: List[str]):
    cmd = \
        './opt/couchbase/bin/cbbackupmgr compact ' \
        '--archive {} --repo default --backup {}'.format(
            cluster_spec.backup,
            snapshots[0]
        )
    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbexport(master_node: str, cluster_spec: ClusterSpec, bucket: str,
             data_format: str, threads: int):
    export_path = os.path.join(cluster_spec.backup, 'data.json')

    cleanup(cluster_spec.backup)

    cmd = \
        './opt/couchbase/bin/cbexport json --format {} ' \
        '--output {} --threads {} ' \
        '--cluster http://{} --username {} --password {} --bucket {}'.format(
            data_format,
            export_path,
            threads,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
            bucket,
        )

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbimport(master_node: str, cluster_spec: ClusterSpec, bucket: str,
             data_type: str, data_format: str, import_file: str, threads: int):
    cmd = \
        './opt/couchbase/bin/cbimport {} ' \
        '--dataset {} --bucket {} ' \
        '--generate-key "#MONO_INCR#" --threads {} ' \
        '--cluster http://{} --username {} --password {} ' \
        ''.format(
            data_type,
            import_file,
            bucket,
            threads,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1])

    if data_type == 'json':
        cmd += ' --format {}'.format(data_format)

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def run_cbc_pillowfight(host: str,
                        bucket: str,
                        password: str,
                        num_items: int,
                        num_threads: int,
                        num_cycles: int,
                        size: int,
                        batch_size: int,
                        writes: int,
                        persist_to: int,
                        replicate_to: int,
                        connstr_params: dict,
                        doc_gen: str = 'binary',
                        ssl_mode: str = 'none',
                        populate: bool = False):
    cmd = 'cbc-pillowfight ' \
        '--password {password} ' \
        '--batch-size {batch_size} ' \
        '--num-items {num_items} ' \
        '--num-threads {num_threads} ' \
        '--min-size {size} ' \
        '--max-size {size} ' \
        '--persist-to {persist_to} ' \
        '--replicate-to {replicate_to} '

    if doc_gen == 'json':
        cmd += '--json '
    elif doc_gen == 'json_snappy':
        cmd += '--json --compress --compress '

    if ssl_mode == 'data':
        cmd += '--spec "couchbases://{host}/{bucket}?{params}" --certpath root.pem '
    else:
        cmd += '--spec "couchbase://{host}/{bucket}?{params}" '

    if populate:
        cmd += '--populate-only '
    else:
        cmd += \
            '--set-pct {writes} ' \
            '--num-cycles {num_cycles} ' \
            '--no-population '

    cmd += ' > /dev/null 2>&1'

    params = urllib.parse.urlencode(connstr_params)

    cmd = cmd.format(host=host,
                     bucket=bucket,
                     password=password,
                     params=params,
                     num_items=num_items,
                     num_threads=num_threads,
                     num_cycles=num_cycles,
                     size=size,
                     batch_size=batch_size,
                     persist_to=persist_to,
                     replicate_to=replicate_to,
                     writes=writes)

    logger.info('Running: {}'.format(cmd))
    local(cmd, shell='/bin/bash')


def run_dcptest(host: str, username: str, password: str, bucket: str,
                num_items: int, num_connections: int):
    cmd = './dcptest ' \
        '-kvaddrs {host}:11210 ' \
        '-buckets {bucket} ' \
        '-nummessages {num_items} ' \
        '-numconnections {num_connections} ' \
        '-outputfile dcpstatsfile ' \
        '{host}:8091 > dcptest.log 2>&1'

    cmd = cmd.format(host=host,
                     bucket=bucket,
                     num_items=num_items,
                     num_connections=num_connections)

    cbauth = 'http://{user}:{password}@{host}:8091'.format(host=host,
                                                           user=username,
                                                           password=password)

    logger.info('Running: {}'.format(cmd))

    with shell_env(CBAUTH_REVRPC_URL=cbauth):
        local(cmd)


def run_kvgen(hostname: str, num_docs: int, prefix: str):
    cmd = './kvgen -hostname {} -docs {} -prefix {}'.format(hostname,
                                                            num_docs,
                                                            prefix)
    logger.info('Running: {}'.format(cmd))
    with shell_env(GOGC='300'):
        local(cmd)


def run_ycsb(host: str,
             bucket: str,
             password: str,
             action: str,
             workload: str,
             items: int,
             workers: int,
             target: int,
             epoll: str,
             boost: int,
             persist_to: int,
             replicate_to: int,
             ssl_keystore_file: str ='',
             ssl_keystore_password: str = '',
             ssl_mode: str = 'none',
             soe_params: dict = None,
             ops: int = None,
             execution_time: int = None,
             cbcollect: int = 0,
             timeseries: int = 0,
             instance: int = 0,
             fieldlength: int = 1024,
             fieldcount: int = 10):
    cmd = 'bin/ycsb {action} couchbase2 ' \
          '-P {workload} ' \
          '-p writeallfields=true ' \
          '-threads {workers} ' \
          '-p target={target} ' \
          '-p fieldlength={fieldlength} ' \
          '-p fieldcount={fieldcount} ' \
          '-p couchbase.host={host} ' \
          '-p couchbase.bucket={bucket} ' \
          '-p couchbase.upsert=true ' \
          '-p couchbase.epoll={epoll} ' \
          '-p couchbase.boost={boost} ' \
          '-p couchbase.persistTo={persist_to} ' \
          '-p couchbase.replicateTo={replicate_to} ' \
          '-p couchbase.sslMode={ssl_mode} ' \
          '-p couchbase.certKeystoreFile=../{ssl_keystore_file} ' \
          '-p couchbase.certKeystorePassword={ssl_keystore_password} ' \
          '-p couchbase.password={password} ' \
          '-p exportfile=ycsb_{action}_{instance}.log '

    if ops is not None:
        cmd += ' -p operationcount={ops} '
    if execution_time is not None:
        cmd += ' -p maxexecutiontime={execution_time} '
    if timeseries or cbcollect:
        cmd += '-p measurementtype=timeseries '

    cmd = cmd.format(host=host,
                     bucket=bucket,
                     action=action,
                     workload=workload,
                     items=items,
                     ops=ops,
                     workers=workers,
                     target=target,
                     execution_time=execution_time,
                     epoll=epoll,
                     boost=boost,
                     persist_to=persist_to,
                     replicate_to=replicate_to,
                     instance=instance,
                     ssl_mode=ssl_mode, password=password,
                     ssl_keystore_file=ssl_keystore_file,
                     ssl_keystore_password=ssl_keystore_password,
                     fieldlength=fieldlength,
                     fieldcount=fieldcount)

    if soe_params is None:
        cmd += ' -p recordcount={items} '.format(items=items)
    else:
        cmd += ' -p totalrecordcount={totalrecordcount} '.format(totalrecordcount=items)
        cmd += ' -p recordcount={items} '.format(items=soe_params['recorded_load_cache_size'])
        cmd += ' -p insertstart={insertstart} '.format(insertstart=soe_params['insertstart'])

    cmd += ' 2>ycsb_{action}_{instance}_stderr.log '.format(action=action, instance=instance)
    logger.info('Running: {}'.format(cmd))
    with lcd('YCSB'):
        local(cmd)


def run_custom_cmd(path: str, binary: str, params: str):
    logger.info("Executing command {} {}".format(binary, params))
    cmd = "{} {}".format(binary, params)
    with lcd(path):
        local(cmd)


def get_jts_logs(jts_home: str, local_dir: str):
    logger.info("Collecting remote JTS logs")
    source_dir = "JTS/logs".format(jts_home)
    for file in glob("{}".format(source_dir)):
        local("cp -r {} {}/".format(file, local_dir))


def restart_memcached(mem_limit: int = 10000, port: int = 8000):
    cmd1 = 'killall -9 memcached'
    logger.info('Running: {}'.format(cmd1))
    with settings(warn_only=True):
        local(cmd1, capture=True)

    for counter in range(5):
        time.sleep(2)
        with settings(warn_only=True):
            result = local('pgrep memcached', capture=True)
        if result.return_code == 1:
            break
        else:
            logger.info('memcached still running')
    else:
        raise Exception('memcached was not kill properly')

    cmd2 = 'memcached -u root -m {mem_limit} -l localhost -p {port} -d'
    cmd2 = cmd2.format(mem_limit=mem_limit, port=port)
    logger.info('Running: {}'.format(cmd2))
    local(cmd2, capture=True)

    for counter in range(5):
        try:
            time.sleep(2)
            mc = MemcachedClient(host='localhost', port=port)
            mc.stats()
            mc.close()
            break
        except (EOFError, socket.error, MemcachedError):
            logger.info('Can not connect to memcached')
    else:
        raise Exception('memcached did not start properly')

    logger.info('memcached restarted')


def run_cbindexperf(path_to_tool: str, node: str, rest_username: str,
                    rest_password: str, configfile: str,
                    run_in_background: bool = False):
    logger.info('Initiating scan workload')
    cmdstr = "{} -cluster {}:8091 -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
             "-statsfile /root/statsfile" \
        .format(path_to_tool, node, rest_username, rest_password, configfile)
    if run_in_background:
        cmdstr += " &"
    logger.info('To be applied: {}'.format(cmdstr))
    ret = local(cmdstr)
    return ret.return_code


def kill_process(process: str):
    logger.info('Killing the following process: {}'.format(process))
    with quiet():
        local("killall -9 {}".format(process))


def start_celery_worker(queue: str):
    with shell_env(PYTHONOPTIMIZE='1', PYTHONWARNINGS='ignore', C_FORCE_ROOT='1'):
        local('nohup env/bin/celery worker '
              '-A perfrunner.helpers.worker -Q {} > worker.log &'.format(queue))


def clone_git_repo(repo: str, branch: str):
    logger.info('Cloning repository: {} branch: {}'.format(repo, branch))
    local('git clone -q -b {} {}'.format(branch, repo))


def init_jts(repo: str, branch: str, jts_home: str):
    clone_git_repo(repo, branch)
    with lcd(jts_home):
        local('mvn install')


def generate_bigfun_data(user_docs: int):
    logger.info('Generating socialGen documents for {} users'.format(user_docs))
    with lcd('socialGen'):
        cmd = \
            "./scripts/initb.sh data 1 0 {users} " \
            "-f JSON -k STRING#%015d > socialGen.log".format(
                users=user_docs)
        local('mvn clean package')
        local(cmd)


def run_loader(hostname: str, bucket: str, password: str, workers: int,
               table: str, path: str = 'socialGen/data/p1'):
    logger.info('Loading socialGen documents ("{}" table)'.format(table))
    cmd = \
        "./loader -hostname {hostname} -bucket {bucket} -password {password} " \
        "-workers {workers} -table {table} -path {path} > loader.log".format(
            hostname=hostname,
            bucket=bucket,
            password=password,
            workers=workers,
            table=table,
            path=path)
    local(cmd)


def load_bigfun_data(hostname: str, bucket: str, password: str, workers: int):
    for table in 'gbook_users', 'gbook_messages', 'chirp_messages':
        run_loader(hostname, bucket, password, workers, table)


def get_indexer_heap_profile(indexer: str, user: str, password: str) -> str:
    cmd = 'go tool pprof --text http://{}:{}@{}:9102/debug/pprof/heap'.format(user,
                                                                              password,
                                                                              indexer)
    return local(cmd, capture=True)


def govendor_fetch(path: str, revision: str, package: str):
    logger.info('Fetching: {} with revision {} and package as {}'.format(path, revision, package))
    local('govendor fetch {}/{}@{}'.format(path, package, revision))


def generate_ssl_keystore(root_certificate: str, keystore_file: str,
                          storepass: str):
    logger.info('Generating SSL keystore')
    with quiet():
        local("keytool -delete -keystore {} -alias couchbase -storepass storepass"
              .format(keystore_file))
    local("keytool -importcert -file {} -storepass {} -trustcacerts "
          "-noprompt -keystore {} -alias couchbase"
          .format(root_certificate, storepass, keystore_file))


def build_java_dcp_client():
    with lcd('java-dcp-client'):
        local('perf/build.sh')


def run_java_dcp_client(connection_string: str, messages: int, config_file: str):
    cmd = 'perf/run.sh {} {} {} > java_dcp.log'.format(connection_string,
                                                       messages,
                                                       config_file)
    with lcd('java-dcp-client'):
        logger.info('Running: {}'.format(cmd))
        local(cmd)

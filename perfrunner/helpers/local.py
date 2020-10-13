import os.path
import socket
import time
from datetime import date
from sys import platform
from typing import List

from fabric.api import lcd, local, quiet, settings, shell_env
from mc_bin_client.mc_bin_client import MemcachedClient, MemcachedError

from logger import logger
from perfrunner.settings import ClusterSpec


def extract_cb(filename):
    cmd = 'rpm2cpio ./{} | cpio -idm'.format(filename)
    with quiet():
        local(cmd)


def cleanup(backup_dir):
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


def backup(master_node, cluster_spec, wrapper=False, mode=None,
           compression=False):
    logger.info('Creating a new backup: {}'.format(cluster_spec.backup))

    if not mode:
        cleanup(cluster_spec.backup)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, mode, compression)


def compact(cluster_spec, snapshots, wrapper=False):
    if wrapper:
        return
    cbbackupmgr_compact(cluster_spec, snapshots)


def cbbackupwrapper(master_node, cluster_spec, mode):
    postfix = ''
    if mode:
        postfix = '-m {}'.format(mode)

    cmd = './cbbackupwrapper http://{}:8091 {} -u {} -p {} -P 16 {}'.format(
        master_node,
        cluster_spec.backup,
        cluster_spec.rest_credentials[0],
        cluster_spec.rest_credentials[1],
        postfix,
    )
    logger.info('Running: {}'.format(cmd))
    with lcd('./opt/couchbase/bin'):
        local(cmd)


def cbbackupmgr_backup(master_node, cluster_spec, mode, compression):
    if not mode:
        local('./opt/couchbase/bin/cbbackupmgr config '
              '--archive {} --repo default'.format(cluster_spec.backup))

    cmd = \
        './opt/couchbase/bin/cbbackupmgr backup ' \
        '--archive {} --repo default  --threads 16 ' \
        '--host http://{} --username {} --password {}'.format(
            cluster_spec.backup,
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


def calc_backup_size(cluster_spec) -> float:
    backup_size = local('du -sb0 {}'.format(cluster_spec.backup), capture=True)
    backup_size = backup_size.split()[0]
    backup_size = float(backup_size) / 2 ** 30  # B -> GB

    return round(backup_size)


def restore(master_node, cluster_spec, wrapper=False):
    logger.info('Restore from {}'.format(cluster_spec.backup))

    if wrapper:
        cbrestorewrapper(master_node, cluster_spec)
    else:
        cbbackupmgr_restore(master_node, cluster_spec)


def cbrestorewrapper(master_node, cluster_spec):
    cmd = './cbrestorewrapper {} http://{}:8091 -u {} -p {}'.format(
        cluster_spec.backup,
        master_node,
        cluster_spec.rest_credentials[0],
        cluster_spec.rest_credentials[1],
    )
    logger.info('Running: {}'.format(cmd))
    with lcd('./opt/couchbase/bin'):
        local(cmd)


def cbbackupmgr_restore(master_node, cluster_spec):
    cmd = \
        './opt/couchbase/bin/cbbackupmgr restore ' \
        '--archive {} --repo default  --threads 16 ' \
        '--host http://{} --username {} --password {}'.format(
            cluster_spec.backup,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
        )
    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbbackupmgr_compact(cluster_spec, snapshots):
    cmd = \
        './opt/couchbase/bin/cbbackupmgr compact ' \
        '--archive {} --repo default --backup {}'.format(
            cluster_spec.backup,
            snapshots[0]
        )
    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbexport(master_node: str, cluster_spec: ClusterSpec, bucket: str,
             data_format: str):
    export_path = os.path.join(cluster_spec.backup, 'data.json')

    cleanup(cluster_spec.backup)

    cmd = \
        './opt/couchbase/bin/cbexport json --format {} ' \
        '--output {} --threads 16 ' \
        '--cluster http://{} --username {} --password {} --bucket {}'.format(
            data_format,
            export_path,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
            bucket,
        )

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbimport(master_node: str, cluster_spec: ClusterSpec, data_type: str,
             data_format: str, bucket: str, import_file: str):
    cmd = \
        './opt/couchbase/bin/cbimport {} ' \
        '--dataset {} --bucket {} ' \
        '--generate-key "#MONO_INCR#" --threads 16 ' \
        '--cluster http://{} --username {} --password {} ' \
        ''.format(
            data_type,
            import_file,
            bucket,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
        )
    if data_type == 'json':
        cmd += ' --format {}'.format(data_format)

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def run_cbc_pillowfight(host, bucket, password,
                        num_items, num_threads, num_cycles, size, writes,
                        populate=False, doc_gen='binary', use_ssl=False):
    cmd = 'cbc-pillowfight ' \
        '--password {password} ' \
        '--batch-size 1000 ' \
        '--num-items {num_items} ' \
        '--num-threads {num_threads} ' \
        '--min-size {size} ' \
        '--max-size {size} ' \

    if doc_gen == 'json':
        cmd += '--json '

    if use_ssl:
        cmd += '--spec couchbases://{host}/{bucket} --certpath root.pem '
    else:
        cmd += '--spec couchbase://{host}/{bucket} '

    if populate:
        cmd += '--populate-only'
    else:
        cmd += \
            '--set-pct {writes} ' \
            '--num-cycles {num_cycles} ' \
            '--no-population'

    cmd = cmd.format(host=host, bucket=bucket, password=password,
                     num_items=num_items, num_threads=num_threads,
                     num_cycles=num_cycles, size=size, writes=writes)

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def run_dcptest_script(host, username, password, bucket,
                       num_items, num_connections, output_file):
    cmd = './dcptest ' \
        '-kvaddrs {host}:11210 ' \
        '-buckets {bucket} ' \
        '-nummessages {num_items} ' \
        '-numconnections {num_connections} ' \
        '-outputfile {outputfile} ' \
        '{host}:8091 > dcptest.log 2>&1'

    cmd = cmd.format(host=host, bucket=bucket, num_items=num_items,
                     num_connections=num_connections, outputfile=output_file)

    cbauth = 'http://{user}:{password}@{host}:8091'.format(host=host,
                                                           user=username,
                                                           password=password)

    logger.info('Running: {}'.format(cmd))

    with shell_env(CBAUTH_REVRPC_URL=cbauth):
        local(cmd)


def run_kvgen(hostname, num_docs, prefix):
    cmd = './kvgen -hostname {} -docs {} -prefix {}'.format(hostname,
                                                            num_docs,
                                                            prefix)
    logger.info('Running: {}'.format(cmd))
    with shell_env(GOGC='300'):
        local(cmd)


def run_ycsb(host, bucket, password, action, workload, items, workers, target, epoll, boost,
             soe_params=None, ops=None, time=None, instance=0):
    cmd = 'bin/ycsb {action} couchbase2 ' \
          '-P {workload} ' \
          '-p writeallfields=true ' \
          '-threads {workers} ' \
          '-p target={target} ' \
          '-p couchbase.host={host} ' \
          '-p couchbase.bucket={bucket} ' \
          '-p couchbase.password={password} ' \
          '-p couchbase.upsert=true ' \
          '-p couchbase.epoll={epoll} ' \
          '-p couchbase.boost={boost} ' \
          '-p couchbase.boost=48 ' \
          '-p couchbase.epoll=true ' \
          '-p exportfile=ycsb_{action}_{instance}.log '

    if ops is not None:
        cmd += ' -p operationcount={ops} '
    if time is not None:
        cmd += ' -p maxexecutiontime={time} '

    cmd = cmd.format(host=host, bucket=bucket, password=password,
                     action=action, workload=workload, target=target, epoll=epoll, boost=boost,
                     items=items, ops=ops, workers=workers, time=time,
                     instance=instance)

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


def run_cmd(path, command, parameters, output_file):
    cmd = "{} {} 2>{}".format(command, parameters, output_file)
    logger.info('Running : {}'.format(cmd))
    with lcd(path):
        local(cmd)


def restart_memcached(mem_limit=10000, port=8000, mem_host='localhost'):
    cmd1 = 'killall -9 memcached'
    logger.info('Running: {}'.format(cmd1))
    with settings(warn_only=True):
        result = local(cmd1, capture=True)

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

    cmd2 = 'memcached -u root -m {mem_limit} -l {memhost} -p {port} -d'
    cmd2 = cmd2.format(mem_limit=mem_limit, port=port, memhost=mem_host)
    logger.info('Running: {}'.format(cmd2))
    result = local(cmd2, capture=True)

    for counter in range(5):
        try:
            time.sleep(2)
            mc = MemcachedClient(host=mem_host, port=port)
            mc.stats()
            mc.close()
            break
        except (EOFError, socket.error, MemcachedError):
            logger.info('Can not connect to memcached')
    else:
        raise Exception('memcached did not start properly')

    logger.info('memcached restarted')


def run_cbindexperf(path_to_tool, node, rest_username, rest_password,
                    configfile, run_in_background=False):
    logger.info('Initiating scan workload')
    cmdstr = "{} -cluster {}:8091 -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
             "-statsfile /root/statsfile" \
        .format(path_to_tool, node, rest_username, rest_password, configfile)
    if run_in_background:
        cmdstr += " &"
    logger.info('To be applied: {}'.format(cmdstr))
    ret = local(cmdstr)
    return ret.return_code


def kill_process(process):
    logger.info('Killing the following process: {}'.format(process))
    with quiet():
        local("killall -9 {}".format(process))


def start_celery_worker(queue):
    with shell_env(PYTHONOPTIMIZE='1', PYTHONWARNINGS='ignore', C_FORCE_ROOT='1'):
        local('nohup env/bin/celery worker '
              '-A perfrunner.helpers.worker -Q {} > worker.log &'.format(queue))


def clone_ycsb(repo: str, branch: str):
    logger.info('Cloning YCSB repository: {} branch: {}'.format(repo, branch))
    local('git clone -q -b {} {}'.format(branch, repo))


def get_indexer_heap_profile(indexer: str) -> str:
    cmd = 'go tool pprof --text http://{}:9102/debug/pprof/heap'.format(indexer)
    return local(cmd, capture=True)


def govendor_fetch(path: str, revision: str, package: str):
    logger.info('Fetching: {} with revision {} and package as {}'.format(path, revision, package))
    local('govendor fetch {}/{}@{}'.format(path, package, revision))


def get_sg_logs(host: str, ssh_user: str, ssh_pass: str):
    os.system('sshpass -p {} scp {}@{}:/home/sync_gateway/*logs.tar.gz ./'.format(ssh_pass,
                                                                                  ssh_user,
                                                                                  host))


def run_blackholepuller(host, clients, timeout, stderr_file_name, log_file_name):
    local('./blackholePuller -url http://sg-user-0:password@{}:4984/db -clients {}'
          ' -timeout {}s 2>{}.log 1>{}.json'.format(host, clients,
                                                    timeout,
                                                    stderr_file_name,
                                                    log_file_name))


def run_blackholepuller_adv(url_str, clients, timeout, stderr_file_name, log_file_name):
    local('./blackholePuller -url {} -clients {}'
          ' -timeout {}s 2>{}.log 1>{}.json'.format(url_str, clients,
                                                    timeout,
                                                    stderr_file_name,
                                                    log_file_name))


def run_newdocpusher(host, changebatchset, clients, timeout, stderr_file_name,
                     log_file_name, doc_id_prefix, doc_size):

    local('./newDocPusher -url http://sg-user-0:password@{}:4984/db -changesBatchSize {} '
          '-clients {} -docSize {} -docIDPrefix {}'
          ' -timeout {}s 2>{}.log 1>{}.json'.format(host, changebatchset, clients,
                                                    doc_size, doc_id_prefix,
                                                    timeout, stderr_file_name,
                                                    log_file_name))


def run_newdocpusher_adv(url_str, changebatchset, clients, timeout, stderr_file_name,
                         log_file_name, doc_id_prefix, doc_size):
    print('printing url string', url_str)
    local('./newDocPusher -url {} -changesBatchSize {} -clients {} '
          '-docSize {} -docIDPrefix {}'
          ' -timeout {}s 2>{}.log 1>{}.json'.format(url_str, changebatchset, clients,
                                                    doc_size, doc_id_prefix, timeout,
                                                    stderr_file_name, log_file_name))


def download_blockholepuller():
    local('curl -o blackholePuller-linux-x64 https://github.com/couchbaselabs/sg_dev_tools/')
    local('chmod +x blackholePuller-linux-x64')


def download_newdocpusher():
    local('curl -o newDocPusher-linux-x64 https://github.com/couchbaselabs/sg_dev_tools/')
    local('chmod +x newDocPusher-linux-x64')


def remove_sg_bp_logs():
    local('rm -rf *blackholepuller*')
    local('rm -rf *newdocpush*')


def remove_sg_newdocpusher_logs():
    local('rm -rf *blackholepuller*')
    local('rm -rf *newdocpush*')

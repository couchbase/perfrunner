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
                        populate=False, use_ssl=False):
    cmd = 'cbc-pillowfight ' \
        '--password {password} ' \
        '--batch-size 1000 ' \
        '--num-items {num_items} ' \
        '--num-threads {num_threads} ' \
        '--min-size {size} ' \
        '--max-size {size} ' \

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


def run_ycsb(host, bucket, password, action, workload, items, workers,
             soe_params=None, ops=None, time=None, instance=0):
    cmd = 'bin/ycsb {action} couchbase2 ' \
          '-P {workload} ' \
          '-p writeallfields=true ' \
          '-threads {workers} ' \
          '-p couchbase.host={host} ' \
          '-p couchbase.bucket={bucket} ' \
          '-p couchbase.password={password} ' \
          '-p couchbase.upsert=true ' \
          '-p couchbase.boost=48 ' \
          '-p couchbase.epoll=true ' \
          '-p exportfile=ycsb_{action}_{instance}.log '

    if ops is not None:
        cmd += ' -p operationcount={ops} '
    if time is not None:
        cmd += ' -p maxexecutiontime={time} '

    cmd = cmd.format(host=host, bucket=bucket, password=password,
                     action=action, workload=workload,
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


def run_bigfun_single_script(host, bucket, password, op, instance, tablename, id, arg):
    with lcd('loader'):
        cmd = "bash ./target/loader-1.0-SNAPSHOT-binary-assembly/bin/loader.sh" \
              " /workspace/bigfundata" \
              " {clientn} {op} {tablename} {id}" \
              " {cbhost} {bucket} {password} {arg}" \
              " > loader_{tablename}_{cbhost}_{bucket}_{clientn}_{op}.log" \
              " 2> loader_{tablename}_{cbhost}_{bucket}_{clientn}_{op}.err".format(
                                                        op=op,
                                                        clientn=instance,
                                                        cbhost=host,
                                                        bucket=bucket,
                                                        password=password,
                                                        tablename=tablename,
                                                        id=id,
                                                        arg=arg)
        local(cmd)


def run_bigfun_mix_mode_script(host, bucket, password,
                               instance, datapath,
                               interval_ms, duration_sec,
                               gudoc, gmdoc, cmdoc,
                               guarg, gmarg, cmarg):
    with lcd('loader'):
        cmd = 'bash ./target/loader-1.0-SNAPSHOT-binary-assembly/bin/mixloadall.sh' \
              ' {host} {bucket} {password}' \
              ' {instance} {datapath}' \
              ' {interval_ms} {duration_sec}' \
              ' {gudoc} {gmdoc} {cmdoc}' \
              ' "{guarg}" "{gmarg}" "{cmarg}"'.format(
                                                host=host,
                                                bucket=bucket,
                                                password=password,
                                                instance=instance,
                                                datapath=datapath,
                                                interval_ms=interval_ms,
                                                duration_sec=duration_sec,
                                                gudoc=gudoc,
                                                gmdoc=gmdoc,
                                                cmdoc=cmdoc,
                                                guarg=guarg,
                                                gmarg=gmarg,
                                                cmarg=cmarg)
        local(cmd)


def run_bigfun_script(host, bucket, password, op, instance, guarg, gmarg, cmarg):
    run_bigfun_single_script(host, bucket, password, op, instance,
                             "gbook_users", "id", guarg)
    run_bigfun_single_script(host, bucket, password, op, instance,
                             "gbook_messages", "message_id", gmarg)
    run_bigfun_single_script(host, bucket, password, op, instance,
                             "chirp_messages", "chirpid", cmarg)


def run_bigfun_query_script(host, bucket, password, instance, concurrent, repeat):
    with lcd('loader'):
        cmd = "bash ./target/loader-1.0-SNAPSHOT-binary-assembly/bin/query.sh" \
              " /workspace/bigfundata" \
              " {clientn} {tablesufix} {host} {user} {password} {concurrent} {repeat}" \
              " > query_{host}_{bucket}_{clientn}_{concurrent}_{repeat}.log" \
              " 2> query_{host}_{bucket}_{clientn}_{concurrent}_{repeat}.err".format(
                                                        clientn=instance,
                                                        tablesufix=bucket,
                                                        host=host,
                                                        user=bucket,
                                                        bucket=bucket,
                                                        password=password,
                                                        concurrent=concurrent,
                                                        repeat=repeat)
        local(cmd)


def run_bigfun(host, bucket, password, action, gudocnum=0, interval=0,
               time=None, instance=0, workers=0, inserts=0, deletes=0, updates=0):
    if action == "insert":
        run_bigfun_script(host, bucket, password, "insert", instance, "", "", "")
    elif action == "delete":
        run_bigfun_script(host, bucket, password, "delete", instance, "", "", "")
    elif action == "ttl":
        run_bigfun_script(host, bucket, password, "ttl", instance, "-t 1#2", "-t 1#2", "-t 1#2")
    elif action == "update_non_index":
        run_bigfun_script(host, bucket, password, "update", instance,
                          "-U alias#string#alias_update%d#1#100 ",
                          "-U message#string#message_update%d#1#100",
                          "-U message_text#string#message_text_update%d#1#100")
    elif action == "update_index":
        run_bigfun_script(host, bucket, password, "update", instance,
                          "-U user_since#time#1992-01-01T00:00:00#1996-01-01T00:00:00",
                          "-U author_id#string#%015d#1#1000",
                          "-U send_time#time#1992-01-01T00:00:00#1996-01-01T00:00:00")
    elif action == "query":
        run_bigfun_query_script(host, bucket, password, instance, workers, 3)
    elif action == "wait":
        with lcd('loader'):
            cmd = "sleep {seconds}".format(seconds=time)
            local(cmd)
    elif action == "mix":
        id_per_table = gudocnum * 20
        gu_insert_start = 4 * id_per_table
        gm_insert_start = 5 * id_per_table
        cm_insert_start = 7 * id_per_table
        inmem_ratio = 0.1
        if int((gudocnum * inmem_ratio) / workers) < 100:
            inmem_ratio = 0.5
        gu_doc_inmem = int(gudocnum * inmem_ratio / workers)
        gm_doc_inmem = int((gudocnum * 5 * inmem_ratio) / workers)
        cm_doc_inmem = int((gudocnum * 10 * inmem_ratio) / workers)
        insert_range_per_partition = int(id_per_table / workers)
        arg = "-t 1#2 -ip {insertportion} -dp {deleteportion}" \
              " -up {updateportion} -tp {ttlportion}" \
              " -qp 0 -md 10 -ir {insertrange}".format(insertportion=inserts,
                                                       deleteportion=deletes,
                                                       updateportion=updates,
                                                       ttlportion=0,
                                                       insertrange=insert_range_per_partition)
        run_bigfun_mix_mode_script(host, bucket, password,
                                   instance, "/workspace/bigfundata",
                                   interval, time,
                                   gu_doc_inmem, gm_doc_inmem, cm_doc_inmem,
                                   arg + " -is {insertstart}".format(insertstart=gu_insert_start),
                                   arg + " -is {insertstart}".format(insertstart=gm_insert_start),
                                   arg + " -is {insertstart}".format(insertstart=cm_insert_start))


def restart_memcached(mem_limit=10000, port=8000):
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

    cmd2 = 'memcached -u root -m {mem_limit} -l localhost -p {port} -d'
    cmd2 = cmd2.format(mem_limit=mem_limit, port=port)
    logger.info('Running: {}'.format(cmd2))
    result = local(cmd2, capture=True)

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


def clone_bigfun(socialgen_repo: str, socialgen_branch: str,
                 loader_repo: str, loader_branch: str):
    logger.info('Cloning socialGen repository: {} branch {}'.format(
        socialgen_repo, socialgen_branch))
    logger.info('Cloning loader repository: {} branch {}'.format(
        loader_repo, loader_branch))
    local('git clone -q -b {} {}'.format(socialgen_branch, socialgen_repo))
    local('git clone -q -b {} {}'.format(loader_branch, loader_repo))
    with lcd('socialGen'):
        cmd = "mvn clean package"
        local(cmd)
    with lcd('loader'):
        cmd = "mvn clean package"
        local(cmd)


def generate_doctemplates(workers: int, user_docs: int, clientn: int):
    logger.info('Generating document templates workers {} docs {} client {}'.format(
        workers, user_docs, clientn))
    with lcd('socialGen'):
        cmd = "bash ./scripts/initb.sh /workspace/bigfundata {partitions} {clientn} {users}" \
              " -f JSON -k STRING#%015d > socialGen.log".format(users=user_docs,
                                                                partitions=workers,
                                                                clientn=clientn)
        local(cmd)


def get_indexer_heap_profile(indexer: str) -> str:
    cmd = 'go tool pprof --text http://{}:9102/debug/pprof/heap'.format(indexer)
    return local(cmd, capture=True)


def govendor_fetch(path: str, revision: str, package: str):
    logger.info('Fetching: {} with revision {} and package as {}'.format(path, revision, package))
    local('govendor fetch {}/{}@{}'.format(path, package, revision))

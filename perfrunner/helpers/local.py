import os.path
from sys import platform

from fabric.api import lcd, local, quiet, shell_env
from logger import logger


def extract_cb(filename):
    cmd = 'rpm2cpio ./{} | cpio -idm'.format(filename)
    with quiet():
        local(cmd)


def cleanup(backup_dir):
    logger.info("Cleaning the disk before backup")

    # Remove files from the directory, if any.
    local('rm -fr {}/*'.format(backup_dir))

    # Discard unused blocks. Twice.
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)  # Otherwise fstrim won't find the device
    if platform == "linux2":
        local('fstrim -v {0} && fstrim -v {0}'.format(backup_dir))


def backup(master_node, cluster_spec, wrapper=False, mode=None,
           compression=False):
    logger.info('Creating a new backup: {}'.format(cluster_spec.backup))

    if not mode:
        cleanup(cluster_spec.backup)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, mode, compression)


def cbbackupwrapper(master_node, cluster_spec, mode):
    postfix = ''
    if mode:
        postfix = '-m {}'.format(mode)

    cmd = './cbbackupwrapper http://{} {} -u {} -p {} -P 16 {}'.format(
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


def calc_backup_size(cluster_spec):
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
    cmd = './cbrestorewrapper {} http://{} -u {} -p {}'.format(
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


def export(master_node, cluster_spec, tp='json', frmt=None, bucket='default'):
    export_file = "{}/{}.{}".format(cluster_spec.backup, frmt, tp)

    cleanup(cluster_spec.backup)

    logger.info('export into: {}'.format(export_file))

    cmd = \
        './opt/couchbase/bin/cbexport {} -c http://{} --username {} ' \
        '--password {} --format {} --output {} -b {} -t 16' \
        .format(tp, master_node, cluster_spec.rest_credentials[0],
                cluster_spec.rest_credentials[1],
                frmt, export_file, bucket)

    logger.info('Running: {}'.format(cmd))
    local(cmd, capture=False)


def import_data(master_node, cluster_spec, tp='json', frmt=None, bucket=''):
    import_file = "{}/{}.{}".format(cluster_spec.backup, frmt, tp)
    if not frmt:
        import_file = "{}/export.{}".format(cluster_spec.backup, tp)

    logger.info('import from: {}'.format(import_file))

    cmd = \
        './opt/couchbase/bin/cbimport {} -c http://{} -u {} -p {} ' \
        '--dataset file://{} -b {} -g "#MONO_INCR#" -l LOG -t 16' \
        .format(tp, master_node, cluster_spec.rest_credentials[0],
                cluster_spec.rest_credentials[1], import_file, bucket)

    if frmt:
        cmd += ' --format {}'.format(frmt)
    logger.info('Running: {}'.format(cmd))
    local(cmd, capture=False)


def import_sample_data(master_node, cluster_spec, bucket='', edition='EE'):
    """
    To generate sample zip with 60m files we need ~10 hours and 250 G of disk.
    Please use generate_samples.py tool to generate and put it under
    /data/import/ folder
    """
    import_file = "/data/import/beer-sample.zip"

    logger.info('import from: {}'.format(import_file))
    if edition == 'EE':
        cmd = \
            './opt/couchbase/bin/cbimport json -c http://{} -u {} ' \
            '-p {} -d {} -b {} -g "#MONO_INCR#" -l LOG -t 16 -f sample' \
            .format(master_node, cluster_spec.rest_credentials[0],
                    cluster_spec.rest_credentials[1], import_file, bucket)
    else:
        # MB-21945 in 4.7
        cmd = \
            './opt/couchbase/bin/cbdocloader -c http://{} -u {} -p {} -b {} -m 40000 -d {}' \
            .format(master_node, cluster_spec.rest_credentials[0],
                    cluster_spec.rest_credentials[1], bucket, import_file)
    logger.info('Running: {}'.format(cmd))
    local(cmd, capture=False)


def cbtransfer_import_data(master_node, cluster_spec, bucket=''):
    bf = "/data/json_lines_ce"
    files = local("ls {} | grep export_csv".format(bf), capture=True).split()

    logger.info('import from: {}/{}'.format(bf, files))
    size = 0
    for f in files:
        cmd = \
            './opt/couchbase/bin/cbtransfer {}/{} http://{} -u {} -p {} -B {}' \
            .format(bf, f, master_node, cluster_spec.rest_credentials[0],
                    cluster_spec.rest_credentials[1], bucket)

        logger.info('Running: {}'.format(cmd))
        local(cmd, capture=False)
        size += os.path.getsize("{}/{}".format(bf, f))
    return size


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
    local(cmd, capture=False)


def run_dcptest_script(host_port, username, password, bucket,
                       num_items, num_connections, output_file):
    cmd = './dcptest ' \
        '-kvaddrs {kvaddrs} ' \
        '-buckets {bucket} ' \
        '-nummessages {num_items} ' \
        '-numconnections {num_connections} ' \
        '-outputfile {outputfile} ' \
        '{host_port} > dcptest.log 2>&1'

    cmd = cmd.format(kvaddrs=host_port.replace('8091', '11210'), bucket=bucket,
                     num_items=num_items, num_connections=num_connections,
                     outputfile=output_file, host_port=host_port)

    cbauth_path = 'http://{user}:{password}@{host}'.format(host=host_port,
                                                           user=username,
                                                           password=password)

    logger.info('Running: {}'.format(cmd))

    with shell_env(CBAUTH_REVRPC_URL=cbauth_path):
        local(cmd, capture=False)


def run_kvgen(hostname, num_docs, prefix):
    cmd = './kvgen -hostname {} -docs {} -prefix {}'.format(hostname,
                                                            num_docs,
                                                            prefix)
    logger.info('Running: {}'.format(cmd))
    with shell_env(GOGC='300'):
        local(cmd, capture=False)


def run_ycsb(host, bucket, password, action, workload, items, workers,
             ops=None, time=None, instance=0):
    cmd = 'bin/ycsb {action} couchbase2 ' \
        '-P {workload} ' \
        '-p recordcount={items} ' \
        '-p threadcount={workers} ' \
        '-p couchbase.host={host} ' \
        '-p couchbase.bucket={bucket} ' \
        '-p couchbase.password={password} ' \
        '-p couchbase.boost=48 ' \
        '-p couchbase.epoll=true ' \
        '-p exportfile=ycsb_{action}_{instance}.log'

    if ops is not None:
        cmd += ' -p operationcount={ops}'
    if time is not None:
        cmd += ' -p maxexecutiontime={time}'

    cmd += ' 2>/dev/null'

    cmd = cmd.format(host=host, bucket=bucket, password=password,
                     action=action, workload=workload,
                     items=items, ops=ops, workers=workers, time=time,
                     instance=instance)

    logger.info('Running: {}'.format(cmd))
    with lcd('YCSB'):
        local(cmd, capture=False)


def run_cbindexperf(path_to_tool, node, rest_username, rest_password, configfile, run_in_background=False):
    logger.info('Initiating scan workload')
    cmdstr = "{} -cluster {} -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
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
    logger.info('Cloning YCSB repository: {}'.format(repo))
    local('git clone -q -b {} {}'.format(branch, repo))

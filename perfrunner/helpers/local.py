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
    backup_dir = cluster_spec.config.get('storage', 'backup')

    logger.info('Creating a new backup: {}'.format(backup_dir))

    if not mode:
        cleanup(backup_dir)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, backup_dir, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, backup_dir, mode,
                           compression)


def cbbackupwrapper(master_node, cluster_spec, backup_dir, mode):
    postfix = ''
    if mode:
        postfix = '-m {}'.format(mode)

    cmd = './cbbackupwrapper http://{} {} -u {} -p {} -P 16 {}'.format(
        master_node,
        backup_dir,
        cluster_spec.rest_credentials[0],
        cluster_spec.rest_credentials[1],
        postfix,
    )
    logger.info('Running: {}'.format(cmd))
    with lcd('./opt/couchbase/bin'):
        local(cmd)


def cbbackupmgr_backup(master_node, cluster_spec, backup_dir, mode,
                       compression):
    if not mode:
        local('./opt/couchbase/bin/cbbackupmgr config '
              '--archive {} --repo default'.format(backup_dir))

    cmd = \
        './opt/couchbase/bin/cbbackupmgr backup ' \
        '--archive {} --repo default  --threads 16 ' \
        '--host http://{} --username {} --password {}'.format(
            backup_dir,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
        )

    if compression:
        cmd = '{} --value-compression compressed'.format(cmd)

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def calc_backup_size(cluster_spec):
    backup_dir = cluster_spec.config.get('storage', 'backup')

    backup_size = local('du -sb0 {}'.format(backup_dir), capture=True)
    backup_size = backup_size.split()[0]
    backup_size = float(backup_size) / 2 ** 30  # B -> GB

    return round(backup_size)


def restore(master_node, cluster_spec, wrapper=False):
    backup_dir = cluster_spec.config.get('storage', 'backup')

    logger.info('Restore from {}'.format(backup_dir))

    if wrapper:
        cbrestorewrapper(master_node, cluster_spec, backup_dir)
    else:
        cbbackupmgr_restore(master_node, cluster_spec, backup_dir)


def cbrestorewrapper(master_node, cluster_spec, backup_dir):
    cmd = './cbrestorewrapper {} http://{} -u {} -p {}'.format(
        backup_dir,
        master_node,
        cluster_spec.rest_credentials[0],
        cluster_spec.rest_credentials[1],
    )
    logger.info('Running: {}'.format(cmd))
    with lcd('./opt/couchbase/bin'):
        local(cmd)


def cbbackupmgr_restore(master_node, cluster_spec, backup_dir):
    cmd = \
        './opt/couchbase/bin/cbbackupmgr restore ' \
        '--archive {} --repo default  --threads 16 ' \
        '--host http://{} --username {} --password {}'.format(
            backup_dir,
            master_node,
            cluster_spec.rest_credentials[0],
            cluster_spec.rest_credentials[1],
        )
    logger.info('Running: {}'.format(cmd))
    local(cmd)


def export(master_node, cluster_spec, tp='json', frmt=None, bucket='default'):
    export_file = "{}/{}.{}".format(
        cluster_spec.config.get('storage', 'backup'), frmt, tp)

    cleanup(cluster_spec.config.get('storage', 'backup'))

    logger.info('export into: {}'.format(export_file))

    if tp == 'json':
        cmd = \
            './opt/couchbase/bin/cbexport {} -c http://{} --username {} ' \
            '--password {} --format {} --output {} -b {} -t 16' \
            .format(tp, master_node, cluster_spec.rest_credentials[0],
                    cluster_spec.rest_credentials[1],
                    frmt, export_file, bucket)
    logger.info('Running: {}'.format(cmd))
    local(cmd, capture=False)


def import_data(master_node, cluster_spec, tp='json', frmt=None, bucket=''):
    import_file = "{}/{}.{}".format(
        cluster_spec.config.get('storage', 'backup'), frmt, tp)
    if not frmt:
        import_file = "{}/export.{}".format(
            cluster_spec.config.get('storage', 'backup'), tp)

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
                        populate=False):
    cmd = 'cbc-pillowfight ' \
        '--spec couchbase://{host}/{bucket} ' \
        '--password {password} ' \
        '--batch-size 1000 ' \
        '--num-items {num_items} ' \
        '--num-threads {num_threads} ' \
        '--min-size {size} ' \
        '--max-size {size} ' \

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


def run_dcptest_script(test):
    command = "./dcptest -auth {user}:{password} -kvaddrs {master_node_ip}:11210 -buckets {bucket} " \
              "-nummessages {items} -numconnections {num_connections} -outputfile {outputfile} {master_node}" \
              " > dcptest.log 2>&1". \
        format(user=test.username, password=test.password, master_node_ip=test.master_node.split(":")[0],
               bucket=test.bucket, items=test.items, num_connections=test.num_connections,
               outputfile=test.OUTPUT_FILE, master_node=test.master_node)
    logger.info("DCP test command: {}".format(command))
    cbauth_path = "http://{user}:{password}@{node}".format(user=test.username, password=test.password,
                                                           node=test.master_node)
    with shell_env(CBAUTH_REVRPC_URL=cbauth_path):
        local(command, capture=False)


def run_kvgen(hostname, num_docs, prefix):
    cmd = './kvgen -hostname {} -docs {} -prefix {}'.format(hostname,
                                                            num_docs,
                                                            prefix)
    logger.info('Running: {}'.format(cmd))
    with shell_env(GOGC='300'):
        local(cmd, capture=False)


def run_ycsb(host, bucket, password, action, workload, items, workers,
             ops=None, time=None):
    cmd = 'bin/ycsb {action} couchbase2 ' \
        '-P {workload} ' \
        '-p recordcount={items} ' \
        '-p threadcount={workers} ' \
        '-p couchbase.host={host} ' \
        '-p couchbase.bucket={bucket} ' \
        '-p couchbase.password={password} ' \
        '-p couchbase.boost=48 ' \
        '-p couchbase.epoll=true ' \
        '-p exportfile=ycsb.log'

    if ops is not None:
        cmd += ' -p operationcount={ops}'
    if time is not None:
        cmd += ' -p maxexecutiontime={time}'

    cmd += ' 2>/dev/null'

    cmd = cmd.format(host=host, bucket=bucket, password=password,
                     action=action, workload=workload,
                     items=items, ops=ops, workers=workers, time=time)

    logger.info('Running: {}'.format(cmd))
    with lcd('YCSB'):
        local(cmd, capture=False)


def run_cbindexperf(path_to_tool, node, rest_username, rest_password, configfile):
    logger.info('Initiating scan workload')
    cmdstr = "{} -cluster {} -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
             "-statsfile /root/statsfile" \
        .format(path_to_tool, node, rest_username, rest_password, configfile)
    logger.info('To be applied: {}'.format(cmdstr))
    ret = local(cmdstr)
    return ret.return_code

import os.path
from sys import platform

from fabric.api import lcd, local, quiet
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
           compression=False, skip_compaction=False):
    backup_dir = cluster_spec.config.get('storage', 'backup')

    logger.info('Creating a new backup: {}'.format(backup_dir))

    if not mode:
        cleanup(backup_dir)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, backup_dir, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, backup_dir, mode,
                           compression, skip_compaction)


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
                       compression, skip_compaction):
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

    if skip_compaction:
        cmd = '{} --skip-last-compaction'.format(cmd)

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
    local(cmd, capture=False)


def import_data(master_node, cluster_spec, tp='json', frmt=None, bucket=''):
    import_file = "{}/{}.{}".format(
        cluster_spec.config.get('storage', 'backup'), frmt, tp)
    if not frmt:
        import_file = "{}/export.{}".format(
            cluster_spec.config.get('storage', 'backup'), tp)

    logger.info('import from: {}'.format(import_file))

    cmd = \
        './opt/couchbase/bin/cbimport {} -c http://{} --username {} --password {} ' \
        '--dataset file://{} -b {} -g "#MONO_INCR#" -l LOG -t 16' \
        .format(tp, master_node, cluster_spec.rest_credentials[0],
                cluster_spec.rest_credentials[1], import_file, bucket)

    if frmt:
        cmd += ' --format {}'.format(frmt)

    local(cmd, capture=False)

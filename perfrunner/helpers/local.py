from fabric.api import lcd, local, quiet
from logger import logger


def extract_cb(filename):
    cmd = 'rpm2cpio ./{} | cpio -idm'.format(filename)
    with quiet():
        local(cmd)


def cleanup(backup_dir):
    with quiet():
        disk = local('df --output=source {} | tail -n1'.format(backup_dir),
                     capture=True)
        local('umount {}'.format(disk))
        local('mkfs.ext4 -F {}'.format(disk))
        local('mount -a'.format(disk))
        local('fstrim {}'.format(backup_dir))


def backup(master_node, cluster_spec, wrapper=False, mode=None, compression=False):
    backup_dir = cluster_spec.config.get('storage', 'backup')

    logger.info('Creating a new backup: {}'.format(backup_dir))

    if not mode:
        cleanup(backup_dir)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, backup_dir, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, backup_dir, mode, compression)


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


def cbbackupmgr_backup(master_node, cluster_spec, backup_dir, mode, compression=False):
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

    return round(backup_size, 1)


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

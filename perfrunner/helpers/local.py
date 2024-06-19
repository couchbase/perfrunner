import os
import shutil
import socket
import time
import urllib.parse
from datetime import date
from glob import glob
from pathlib import Path
from sys import platform
from typing import List, Optional, Tuple

from fabric.api import hide, lcd, local, quiet, settings, shell_env, warn_only
from mc_bin_client.mc_bin_client import MemcachedClient, MemcachedError

from logger import logger
from perfrunner.helpers.misc import SSLCertificate, run_local_shell_command
from perfrunner.settings import (
    CH2,
    CH3,
    CH2ConnectionSettings,
    CH3ConnectionSettings,
    ClusterSpec,
)


def extract_cb(filename: str):
    cmd = 'rpm2cpio ./{} | cpio -idm'.format(filename)
    with quiet():
        local(cmd)


def extract_cb_deb(filename: str):
    cmd = 'ar p {} data.tar.xz | unxz | tar x'.format(filename)
    with quiet():
        local(cmd)


def extract_cb_any(filename: str):
    if os.path.exists("{}.deb".format(filename)):
        extract_cb_deb("{}.deb".format(filename))
    else:
        extract_cb("{}.rpm".format(filename))


def extract_any(filename: str, to_path: str = None, remove_after: bool = True):
    """Extract the given file and store the content to the specified path.

    If `remove_after` is specified, delete the file after extraction.
    """
    cmd = [
        f"tar xfv {filename}",
        f"--one-top-level={to_path} --strip-components 1" if to_path else "",
    ]
    with warn_only():
        local(" ".join(cmd))
        if remove_after:
            local(f"rm -f {filename}")


def cleanup(backup_dir: str):
    logger.info("Clearing the backup directory before backup/export")

    if (path := Path(backup_dir)).exists():
        local("find {} -mindepth 1 -name '*' -delete".format(backup_dir))
    else:
        path.mkdir(parents=True)

    # Discard unused blocks. Twice.
    if platform == "linux2":  # this never gets run? platform is "linux" on linux
        logger.info('Running fstrim on backup directory to discard unused blocks')
        local('fstrim -v {0} && fstrim -v {0}'.format(backup_dir))


def drop_caches():
    logger.info('Dropping memory cache')
    local('sync && echo 3 > /proc/sys/vm/drop_caches')


def backup(master_node: str, cluster_spec: ClusterSpec, threads: int, wrapper: bool = False,
           mode: Optional[str] = None, compression: bool = False,
           storage_type: Optional[str] = None, sink_type: Optional[str] = None,
           shards: Optional[int] = None, include_data: Optional[str] = None, use_tls: bool = False,
           encrypted: bool = False, passphrase: str = 'couchbase'):
    logger.info('Creating a new backup: {}'.format(cluster_spec.backup))

    if not mode:
        cleanup(cluster_spec.backup)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, 16, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, threads, mode, compression, storage_type,
                           sink_type, shards, include_data, use_tls, encrypted, passphrase)


def compact(cluster_spec: ClusterSpec,
            snapshots: List[str],
            threads,
            wrapper: bool = False):
    if wrapper:
        return
    cbbackupmgr_compact(cluster_spec, snapshots, threads)


def cbbackupwrapper(master_node: str, cluster_spec: ClusterSpec,
                    threads: int, mode: str):
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


def cbbackupmgr_version():
    cmd = './opt/couchbase/bin/cbbackupmgr --version'
    logger.info('Running: {}'.format(cmd))
    result = local(cmd, capture=True)
    logger.info(result)


def cbbackupmgr_backup(master_node: str, cluster_spec: ClusterSpec, threads: int, mode: str,
                       compression: bool, storage_type: str, sink_type: str, shards: int,
                       include_data: str, use_tls: bool = False, encrypted: bool = False,
                       passphrase: str = 'couchbase'):
    if not mode:
        flags = ['--archive {}'.format(cluster_spec.backup),
                 '--repo default',
                 '--include-data {}'.format(include_data) if include_data else None,
                 '--encrypted --passphrase {}'.format(passphrase) if encrypted else None]

        cmd = './opt/couchbase/bin/cbbackupmgr config {}'.format(' '.join(filter(None, flags)))
        logger.info('Running: {}'.format(cmd))
        run_local_shell_command(cmd)

    flags = ['--archive {}'.format(cluster_spec.backup),
             '--repo default',
             '--cluster http{}://{}'.format('s' if use_tls else '', master_node),
             '--cacert root.pem' if use_tls else None,
             '--username {}'.format(cluster_spec.rest_credentials[0]),
             '--password {}'.format(cluster_spec.rest_credentials[1]),
             '--threads {}'.format(threads) if threads else None,
             '--storage {}'.format(storage_type) if storage_type else None,
             '--sink {}'.format(sink_type) if sink_type else None,
             '--value-compression compressed' if compression else '--value-compression unchanged',
             '--shards {}'.format(shards) if shards else None,
             '--passphrase {}'.format(passphrase) if encrypted else None]

    cmd = './opt/couchbase/bin/cbbackupmgr backup {}'.format(' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    run_local_shell_command(cmd)


def cbbackupmgr_collectlogs(cluster_spec: ClusterSpec, obj_region: str = None,
                            obj_staging_dir: str = None, obj_access_key_id: str = None):
    logger.info('Running cbbackumgr cbcollect-logs on localhost ')

    flags = [
        '--archive {}'.format(cluster_spec.backup),
        '--output-dir .',
        '--obj-region {}'.format(obj_region) if obj_region else None,
        '--obj-staging-dir {}'.format(obj_staging_dir) if obj_staging_dir else None,
        '--obj-access-key-id {}'.format(obj_access_key_id) if obj_access_key_id else None
    ]

    cmd = './opt/couchbase/bin/cbbackupmgr collect-logs {}'.format(
          ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def get_backup_snapshots(cluster_spec: ClusterSpec) -> List[str]:

    logger.info('running cbbackupmgr info command ')

    cmd = \
        './opt/couchbase/bin/cbbackupmgr info ' \
        '--archive {} --repo default '.format(cluster_spec.backup)

    logger.info('Running: {}'.format(cmd))

    snapshots = []
    pattern = '  {}-'.format(date.today().year)
    for line in local(cmd, capture=True).split('\n'):
        if pattern in line:
            snapshot = line.strip().split()[1]
            snapshots.append(snapshot)
    return snapshots



def cbbackupmgr_merge(cluster_spec: ClusterSpec, snapshots: List[str],
                      storage_type: str, threads: int):

    flags = ['--archive {}'.format(cluster_spec.backup),
             '--repo default',
             '--start {}'.format(snapshots[0]),
             '--end {}'.format(snapshots[1]),
             '--storage {}'.format(storage_type) if storage_type else None,
             '--threads {}'.format(threads) if threads else None]

    cmd = './opt/couchbase/bin/cbbackupmgr merge {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def calc_backup_size(cluster_spec: ClusterSpec,
                     rounded: bool = True) -> float:
    backup_size = local('du -sb0 {}'.format(cluster_spec.backup), capture=True)
    backup_size = backup_size.split()[0]
    backup_size = float(backup_size) / 2 ** 30  # B -> GB

    return round(backup_size, 2) if rounded else backup_size


def restore(master_node: str, cluster_spec: ClusterSpec, threads: int, wrapper: bool = False,
            include_data: Optional[str] = None, use_tls: bool = False, encrypted: bool = False,
            passphrase: str = 'couchbase', disable_hlv: bool = False):

    logger.info('Restore from {}'.format(cluster_spec.backup))

    purge_restore_progress(cluster_spec)

    if wrapper:
        cbrestorewrapper(master_node, cluster_spec)
    else:
        cbbackupmgr_restore(master_node, cluster_spec, threads, include_data, use_tls=use_tls,
                            encrypted=encrypted, passphrase=passphrase, disable_hlv=disable_hlv)


def purge_restore_progress(cluster_spec: ClusterSpec, archive: str = '',
                           repo: str = 'default'):
    archive_path = archive or cluster_spec.backup
    repo_dir = '{}/{}'.format(archive_path.rstrip('/'), repo)
    logger.info('Purging restore progress from {}'.format(repo_dir))
    cmd = 'rm -rf {}/.restore'.format(repo_dir)
    logger.info('Running: {}'.format(cmd))
    local(cmd)


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


def cbbackupmgr_restore(master_node: str, cluster_spec: ClusterSpec, threads: int,
                        include_data: str, archive: str = '', repo: str = 'default',
                        map_data: Optional[str] = None, use_tls: bool = False,
                        encrypted: bool = False, passphrase: str = 'couchbase',
                        disable_hlv: bool = False):

    flags = ['--archive {}'.format(archive or cluster_spec.backup),
             '--repo {}'.format(repo),
             '--include-data {}'.format(include_data) if include_data else None,
             '--threads {}'.format(threads),
             '--cluster http{}://{}'.format('s' if use_tls else '', master_node),
             '--cacert root.pem' if use_tls else None,
             '--username {}'.format(cluster_spec.rest_credentials[0]),
             '--password {}'.format(cluster_spec.rest_credentials[1]),
             '--map-data {}'.format(map_data) if map_data else None,
             '--passphrase {}'.format(passphrase) if encrypted else None,
             '--disable-hlv' if disable_hlv else None]

    cmd = './opt/couchbase/bin/cbbackupmgr restore --force-updates {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    run_local_shell_command(cmd)


def cbbackupmgr_compact(cluster_spec: ClusterSpec, snapshots: List[str],
                        threads: int):

    flags = ['--archive {}'.format(cluster_spec.backup),
             '--repo default',
             '--backup {}'.format(snapshots[0]),
             '--threads {}'.format(threads) if threads else None]

    cmd = './opt/couchbase/bin/cbbackupmgr compact {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbbackupmgr_list(cluster_spec: ClusterSpec, snapshots: List[str],
                     bucket: str = None):

    flags = ['--archive {}'.format(cluster_spec.backup),
             '--repo default',
             '--backup {}'.format(snapshots[0]),
             '--bucket {}'.format(bucket) if bucket else None]

    cmd = './opt/couchbase/bin/cbbackupmgr list {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbexport_version():
    cmd = './opt/couchbase/bin/cbexport --version'
    logger.info('Running: {}'.format(cmd))
    result = local(cmd, capture=True)
    logger.info(result)


def cbexport(master_node: str, cluster_spec: ClusterSpec, bucket: str,
             data_format: str, threads: int, collection_field: str, scope_field: str,
             key_field: str = None,
             log_file: str = None):

    export_path = os.path.join(cluster_spec.backup, 'data.json')

    cleanup(cluster_spec.backup)

    if collection_field and scope_field:
        flags = ['--format {}'.format(data_format),
                 '--output {}'.format(export_path),
                 '--cluster http://{}'.format(master_node),
                 '--bucket {}'.format(bucket),
                 '--username {}'.format(cluster_spec.rest_credentials[0]),
                 '--password {}'.format(cluster_spec.rest_credentials[1]),
                 '--collection-field {}'.format(collection_field),
                 '--scope-field {}'.format(scope_field),
                 '--threads {}'.format(threads) if threads else None,
                 '--include-key key',
                 '--verbose --log-file {}'.format(log_file) if log_file else None]

    else:
        flags = ['--format {}'.format(data_format),
                 '--output {}'.format(export_path),
                 '--cluster http://{}'.format(master_node),
                 '--bucket {}'.format(bucket),
                 '--username {}'.format(cluster_spec.rest_credentials[0]),
                 '--password {}'.format(cluster_spec.rest_credentials[1]),
                 '--threads {}'.format(threads) if threads else None,
                 '--include-key {}'.format(key_field) if key_field else None,
                 '--verbose --log-file {}'.format(log_file) if log_file else None]

    cmd = './opt/couchbase/bin/cbexport json {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbimport_version():
    cmd = './opt/couchbase/bin/cbimport --version'
    logger.info('Running: {}'.format(cmd))
    result = local(cmd, capture=True)
    logger.info(result)


def cbimport(master_node: str, cluster_spec: ClusterSpec, bucket: str,
             data_type: str, data_format: str, import_file: str, threads: int,
             scope_collection_exp: str, field_separator: str = None, limit_rows: int = None,
             skip_rows: int = None, infer_types: int = None,
             omit_empty: int = None, errors_log: str = None,
             log_file: str = None, is_sample_format: bool = False):

    if not scope_collection_exp:

        flags = ['--format {}'.format(data_format) if data_type == 'json'
                 else None,
                 '--dataset {}'.format(import_file),
                 '--cluster http://{}'.format(master_node),
                 '--bucket {}'.format(bucket),
                 '--username {}'.format(cluster_spec.rest_credentials[0]),
                 '--password {}'.format(cluster_spec.rest_credentials[1]),
                 '--generate-key "#MONO_INCR#"' if not is_sample_format else None,
                 '--threads {}'.format(threads) if threads else None,
                 '--field-separator {}'.format(field_separator) if field_separator
                 else None,
                 '--limit-rows {}'.format(limit_rows) if limit_rows else None,
                 '--skip-rows {}'.format(skip_rows) if skip_rows else None,
                 '--infer-types' if infer_types else None,
                 '--omit-empty' if omit_empty else None,
                 '--errors-log {}'.format(errors_log) if errors_log else None,
                 '--verbose --log-file {}'.format(log_file) if log_file else None]

    else:

        flags = ['--format {}'.format(data_format) if data_type == 'json'
                 else None,
                 '--dataset {}'.format(import_file),
                 '--cluster http://{}'.format(master_node),
                 '--bucket {}'.format(bucket),
                 '--username {}'.format(cluster_spec.rest_credentials[0]),
                 '--password {}'.format(cluster_spec.rest_credentials[1]),
                 '--generate-key "#MONO_INCR#"',
                 '--threads {}'.format(threads) if threads else None,
                 '--field-separator {}'.format(field_separator) if field_separator
                 else None,
                 '--limit-rows {}'.format(limit_rows) if limit_rows else None,
                 '--skip-rows {}'.format(skip_rows) if skip_rows else None,
                 '--infer-types' if infer_types else None,
                 '--omit-empty' if omit_empty else None,
                 '--errors-log {}'.format(errors_log) if errors_log else None,
                 '--verbose --log-file {}'.format(log_file) if log_file else None,
                 '--scope-collection-exp {}'.format(scope_collection_exp)]

    cmd = './opt/couchbase/bin/cbimport {} {}'.format(
        data_type, ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def run_cbc_pillowfight(host: str,
                        bucket: str,
                        password: str,
                        username: str,
                        num_items: int,
                        num_threads: int,
                        num_cycles: int,
                        size: int,
                        batch_size: int,
                        writes: int,
                        persist_to: int,
                        replicate_to: int,
                        connstr_params: dict,
                        durability: int,
                        doc_gen: str = 'binary',
                        ssl_mode: str = 'none',
                        populate: bool = False,
                        collections: dict = None,
                        custom_pillowfight: bool = False):

    cmd = 'cbc-pillowfight ' \
        '--password {password} ' \
        '--batch-size {batch_size} ' \
        '--num-items {num_items} ' \
        '--num-threads {num_threads} ' \
        '--min-size {size} ' \
        '--max-size {size} ' \
        '--persist-to {persist_to} ' \
        '--replicate-to {replicate_to} '

    if custom_pillowfight:
        cmd = '/tmp/libcouchbase_custom/libcouchbase/build/bin/'+cmd

    if collections:
        target_scope_collections = collections[bucket]
        for scope in target_scope_collections.keys():
            for collection in target_scope_collections[scope].keys():
                if populate:
                    if target_scope_collections[scope][collection]['load'] == 1:
                        cmd += "--collection " + scope+"."+collection + " "
                else:
                    if target_scope_collections[scope][collection]['access'] == 1:
                        cmd += "--collection " + scope+"."+collection + " "

    if doc_gen == 'json':
        cmd += '--json '
    elif doc_gen == 'json_snappy':
        cmd += '--json --compress --compress '

    if ssl_mode == 'data' or ssl_mode == 'n2n':
        cmd += '--spec "couchbases://{host}/{bucket}?{params}" --certpath root.pem '
    elif ssl_mode == 'capella':
        cmd += '--spec "couchbases://{host}/{bucket}?ssl=no_verify&{params}" -u {username} '
    else:
        cmd += '--spec "couchbase://{host}/{bucket}?{params}" '

    if populate:
        cmd += '--populate-only '
    else:
        cmd += \
            '--set-pct {writes} ' \
            '--num-cycles {num_cycles} ' \
            '--no-population '

    durability_options = {
        0: 'none',
        1: 'majority',
        2: 'majority_and_persist_to_active',
        3: 'persist_to_majority'}

    if durability:
        cmd += '--durability {durability} '

    cmd += ' > /dev/null 2>&1'

    params = urllib.parse.urlencode(connstr_params)

    cmd = cmd.format(host=host,
                     bucket=bucket,
                     password=password,
                     username=username,
                     params=params,
                     num_items=num_items,
                     num_threads=num_threads,
                     num_cycles=num_cycles,
                     size=size,
                     batch_size=batch_size,
                     persist_to=persist_to,
                     replicate_to=replicate_to,
                     writes=writes,
                     durability=durability_options[durability]
                     if durability else None)

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


def build_ycsb(ycsb_client: str):
    cmd = 'pyenv local 2.7.18 && bin/ycsb build {}'.format(ycsb_client)

    logger.info('Running: {}'.format(cmd))
    with lcd('YCSB'):
        local(cmd)


def run_ycsb(host: str,
             bucket: str,
             password: str,
             action: str,
             ycsb_client: str,
             workload: str,
             items: int,
             workers: int,
             target: int,
             epoll: str,
             boost: int,
             persist_to: int,
             replicate_to: int,
             num_atrs: int,
             documentsintransaction: int,
             transactionreadproportion: str,
             transactionupdateproportion: str,
             transactioninsertproportion: str,
             requestdistribution: str,
             ssl_keystore_file: str = '',
             ssl_keystore_password: str = '',
             ssl_mode: str = 'none',
             certificate_file: str = '',
             soe_params: dict = None,
             ops: int = None,
             execution_time: int = None,
             cbcollect: int = 0,
             timeseries: int = 0,
             transactionsenabled: int = 0,
             instance: int = 0,
             fieldlength: int = 1024,
             fieldcount: int = 10,
             durability: int = None,
             kv_endpoints: int = 1,
             enable_mutation_token: str = None,
             retry_strategy: str = 'default',
             retry_lower: int = 1,
             retry_upper: int = 500,
             retry_factor: int = 2,
             range_scan_sampling: bool = None,
             ordered: bool = None,
             prefix_scan: bool = None,
             ycsb_jvm_args: str = None,
             collections_map: dict = None,
             out_of_order: int = 0,
             phase_params: dict = None,
             insert_test_params: dict = None,
             cloud: bool = None):

    cmd = 'bin/ycsb {action} {ycsb_client} ' \
          '-P {workload} ' \
          '-p writeallfields=true ' \
          '-threads {workers} ' \
          '-p target={target} ' \
          '-p fieldlength={fieldlength} ' \
          '-p fieldcount={fieldcount} ' \
          '-p requestdistribution={requestdistribution} ' \
          '-p couchbase.host={host} ' \
          '-p couchbase.bucket={bucket} ' \
          '-p couchbase.upsert=true ' \
          '-p couchbase.epoll={epoll} ' \
          '-p couchbase.boost={boost} ' \
          '-p couchbase.kvEndpoints={kv_endpoints} ' \
          '-p couchbase.sslMode={ssl_mode} ' \
          '-p couchbase.certKeystoreFile=../{ssl_keystore_file} ' \
          '-p couchbase.certKeystorePassword={ssl_keystore_password} ' \
          '-p couchbase.certificateFile=../{certificate_file} ' \
          '-p couchbase.password={password} ' \
          '-p exportfile=ycsb_{action}_{instance}_{bucket}.log ' \
          '-p couchbase.retryStrategy={retry_strategy} ' \
          '-p couchbase.retryLower={retry_lower} ' \
          '-p couchbase.retryUpper={retry_upper} ' \
          '-p couchbase.retryFactor={retry_factor} ' \
          '-p couchbase.rangeScanSampling={range_scan_sampling} ' \
          '-p couchbase.prefixScan={prefix_scan} ' \
          '-p couchbase.ordered={ordered} '

    cmd = 'pyenv local 2.7.18 && ' + cmd

    if durability is None:
        cmd += '-p couchbase.persistTo={persist_to} '
        cmd += '-p couchbase.replicateTo={replicate_to} '
    else:
        cmd += '-p couchbase.durability={durability} '

    if enable_mutation_token is not None:
        cmd += '-p couchbase.enableMutationToken={enable_mutation_token} '

    if ops is not None:
        cmd += ' -p operationcount={ops} '
    if execution_time is not None:
        cmd += ' -p maxexecutiontime={execution_time} '
    if timeseries:
        cmd += '-p measurementtype=timeseries '

    if transactionsenabled:
        cmd += ' -p couchbase.transactionsEnabled=true '
        cmd += ' -p couchbase.atrs={num_atrs} '
        cmd += ' -p documentsintransaction={documentsintransaction} '
        cmd += ' -p transactionreadproportion={transactionreadproportion} '
        cmd += ' -p transactionupdateproportion={transactionupdateproportion} '
        cmd += ' -p transactioninsertproportion={transactioninsertproportion} '

    if ycsb_jvm_args is not None:
        cmd += ' -jvm-args=\'{ycsb_jvm_args}\' '

    cmd = cmd.format(host=host,
                     bucket=bucket,
                     action=action,
                     ycsb_client=ycsb_client,
                     workload=workload,
                     items=items,
                     ops=ops,
                     workers=workers,
                     target=target,
                     execution_time=execution_time,
                     epoll=epoll,
                     documentsintransaction=documentsintransaction,
                     transactionreadproportion=transactionreadproportion,
                     transactionupdateproportion=transactionupdateproportion,
                     transactioninsertproportion=transactioninsertproportion,
                     requestdistribution=requestdistribution,
                     boost=boost,
                     persist_to=persist_to,
                     replicate_to=replicate_to,
                     num_atrs=num_atrs,
                     instance=instance,
                     ssl_mode=ssl_mode, password=password,
                     ssl_keystore_file=ssl_keystore_file,
                     ssl_keystore_password=ssl_keystore_password,
                     certificate_file=certificate_file,
                     fieldlength=fieldlength,
                     fieldcount=fieldcount,
                     durability=durability,
                     kv_endpoints=kv_endpoints,
                     enable_mutation_token=enable_mutation_token,
                     retry_strategy=retry_strategy,
                     retry_lower=retry_lower,
                     retry_upper=retry_upper,
                     retry_factor=retry_factor,
                     range_scan_sampling=range_scan_sampling,
                     prefix_scan=prefix_scan,
                     ordered=ordered,
                     ycsb_jvm_args=ycsb_jvm_args)

    if soe_params is None:
        if phase_params:
            cmd += ' -p totalrecordcount={totalrecordcount} '.format(totalrecordcount=items)
            cmd += ' -p recordcount={items} '.format(
                items=phase_params['inserts_per_workerinstance'])
            cmd += ' -p insertstart={insertstart} '.format(insertstart=phase_params['insertstart'])
        elif insert_test_params:
            cmd += ' -p recordcount={items} '.format(items=insert_test_params['recordcount'])
            cmd += ' -p insertstart={insertstart} '.format(
                insertstart=insert_test_params['insertstart'])
        else:
            cmd += ' -p recordcount={items} '.format(items=items)
    else:
        cmd += ' -p totalrecordcount={totalrecordcount} '.format(totalrecordcount=items)
        cmd += ' -p recordcount={items} '.format(items=soe_params['recorded_load_cache_size'])
        cmd += ' -p insertstart={insertstart} '.format(insertstart=soe_params['insertstart'])

    if out_of_order:
        cmd += ' -p couchbase.outOfOrderExecution=true '

    if collections_map:
        target_scope_collections = collections_map[bucket]
        target_scopes = set()
        target_collections = set()

        for scope in target_scope_collections.keys():
            for collection in target_scope_collections[scope].keys():
                if target_scope_collections[scope][collection]['load'] == 1 \
                        and target_scope_collections[scope][collection]['access'] == 1:
                    target_scopes.add(scope)
                    target_collections.add(collection)

        records_per_collection = items // len(target_collections)
        cmd += ' -p recordspercollection={recordspercollection} '\
            .format(recordspercollection=records_per_collection)
        cmd += ' -p collectioncount={num_of_collections} '\
            .format(num_of_collections=len(target_collections))
        cmd += ' -p scopecount={num_of_scopes} '\
            .format(num_of_scopes=len(target_scopes))

        collection_string = ''

        for coll in list(target_collections):
            collection_string += coll + ","

        collections_param = collection_string[:-1]

        cmd += ' -p collectionsparam={collectionsparam} '.format(collectionsparam=collections_param)

        scope_string = ''

        for scope in list(target_scopes):
            scope_string += scope + ","

        scopes_param = scope_string[:-1]

        cmd += ' -p scopesparam={scopesparam} '.format(scopesparam=scopes_param)

    cmd += ' 2>ycsb_{action}_{instance}_{bucket}_stderr.log '.format(action=action,
                                                                     instance=instance,
                                                                     bucket=bucket)

    logger.info('Running: {}'.format(cmd))
    with lcd('YCSB'):
        local(cmd)


def run_tpcds_loader(host: str,
                     bucket: str,
                     password: str,
                     partitions: int,
                     instance: int,
                     scale_factor: int):

    cmd = "java -jar tpcds.jar" \
          " partitions={partitions}" \
          " partition={partition}" \
          " scalefactor={scalefactor}" \
          " hostname={host}" \
          " bucketname={bucket}" \
          " username={user}" \
          " password={password}"

    cmd = cmd.format(
        partitions=partitions,
        partition=instance+1,
        scalefactor=scale_factor,
        host=host,
        bucket=bucket,
        user="Administrator",
        password=password)

    with lcd("cbas-perf-support"), lcd("tpcds-couchbase-loader"), lcd("target"):
        local(cmd)


def run_custom_cmd(path: str, binary: str, params: str):
    logger.info("Executing command {} {}".format(binary, params))
    cmd = "{} {}".format(binary, params)
    with lcd(path):
        local(cmd)


def get_jts_logs(jts_home: str, local_dir: str):
    logger.info("Collecting remote JTS logs")
    source_dir = "{}/logs".format(jts_home)
    for file in glob("{}".format(source_dir)):
        local("cp -r {} {}/".format(file, local_dir))


def run_cmd(path, command, parameters, output_file):
    cmd = "{} {} 2>{}".format(command, parameters, output_file)
    logger.info('Running : {}'.format(cmd))
    with lcd(path):
        local(cmd)


def restart_memcached(mem_host: str = 'localhost', mem_limit: int = 10000, port: int = 8000):
    cmd1 = 'systemctl stop memcached'
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
        raise Exception('memcached was not killed properly')

    cmd2 = 'memcached -u root -m {mem_limit} -l {memhost} -p {port} -d'
    cmd2 = cmd2.format(mem_limit=mem_limit, port=port, memhost=mem_host)
    logger.info('Running: {}'.format(cmd2))
    local(cmd2, capture=True)

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


def run_cbindexperf(path_to_tool: str, node: str, rest_username: str,
                    rest_password: str, configfile: str,
                    run_in_background: bool = False,
                    collect_profile: bool = True,
                    is_ssl: bool = False,
                    gcpercent: int = None):
    logger.info('Initiating scan workload')
    if gcpercent:
        cmdstr = "{} -cluster {}:8091 -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
            "-statsfile /root/statsfile -gcpercent {}".\
            format(path_to_tool, node, rest_username, rest_password, configfile, gcpercent)
    else:
        cmdstr = "{} -cluster {}:8091 -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
                 "-statsfile /root/statsfile" \
            .format(path_to_tool, node, rest_username, rest_password, configfile)
    if collect_profile:
        cmdstr += " -cpuprofile cpuprofile.prof -memprofile memprofile.prof "
    if is_ssl:
        cmdstr += " -use_tls -cacert ./root.pem"
    if run_in_background:
        cmdstr += " &"
    logger.info('To be applied: {}'.format(cmdstr))
    ret = local(cmdstr)
    return ret.return_code


def run_cbindex(path_to_tool: str, options: str, index_nodes):
    batch_options = \
        "-auth=Administrator:password " \
        "-server {index_node}:8091 " \
        "-type batch_process " \
        "-input /tmp/batch.txt " \
        "-refresh_settings=true".format(index_node=index_nodes[0])
    with open("/tmp/batch.txt", "w+") as bf:
        bf.write(options)
    logger.info('Running cbindex on the client')
    cmdstr = "{} {}"\
             .format(path_to_tool, batch_options)
    logger.info('Running {}'.format(cmdstr))
    ret = local(cmdstr)
    return ret.return_code, ret.stderr


def kill_process(process: str):
    logger.info('Killing the following process: {}'.format(process))
    with quiet():
        local("killall -9 {}".format(process))


def start_celery_worker(queue: str):
    with shell_env(PYTHONOPTIMIZE='1', PYTHONWARNINGS='ignore', C_FORCE_ROOT='1'):
        local('WORKER_TYPE=local '
              'BROKER_URL=sqla+sqlite:///perfrunner.db '
              'nohup env/bin/celery -A perfrunner.helpers.worker worker '
              '-l INFO -Q {} -f worker.log &'.format(queue))


def clone_git_repo(repo: str, branch: str, commit: str = None):
    repo_name = repo.split("/")[-1].split(".")[0]
    logger.info('checking if repo {} exists...'.format(repo_name))
    if os.path.exists("{}".format(repo_name)):
        logger.info('repo {} exists...removing...'.format(repo_name))
        shutil.rmtree(repo_name, ignore_errors=True)
    logger.info('Cloning repository: {} branch: {}'.format(repo, branch))
    local('git clone -q -b {} --single-branch {}'.format(branch, repo))
    if commit:
        with lcd(repo_name):
            local('git checkout {}'.format(commit))


def check_if_remote_branch_exists(repo: str, branch: str) -> bool:
    """Return true if a specified branch exists in the specified repo."""
    logger.info(f"Checking if branch {branch} exists on {repo}")
    ret = local(f"git ls-remote -q --heads {repo} refs/heads/{branch}", capture=True)
    return f"refs/heads/{branch}" in ret.stdout


def init_tpcds_couchbase_loader(repo: str, branch: str):
    clone_git_repo(repo, branch)
    with lcd("cbas-perf-support"), lcd("tpcds-couchbase-loader"):
        local('mvn install')


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
    logger.info('Running: {}'.format(cmd))
    for counter in range(10):
        time.sleep(2)
        with settings(warn_only=True):
            result = local(cmd, capture=True)
        if result.succeeded:
            break
        else:
            logger.info('Error: {}'.format(result.stderr))
    else:
        raise Exception('Command failed: {}'.format(cmd))
    return result


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


def run_java_dcp_client(connection_string: str, messages: int,
                        config_file: str, instance: int = None, collections: list = None):

    cmd = 'perf/run.sh {} {} {} '.format(connection_string,
                                         messages,
                                         config_file)

    if collections:
        for collection in collections:
            cmd += collection+","
        cmd = cmd[:-1]
    if instance:
        cmd += ' > java_dcp_{}.log'.format(str(instance))
    else:
        cmd += ' > java_dcp.log'
    with lcd('java-dcp-client'):
        logger.info('Running: {}'.format(cmd))
        local(cmd)


def detect_ubuntu_release():
    return local('lsb_release -sr', capture=True).strip()


def run_cbstats(command: str, server: str, port: int, username: str, password: str,
                bucket: Optional[str] = None) -> Tuple[str, int]:
    """Run cbstats on the given server, returning (stdout, returncode).

    If `bucket` not provided, then run cbstats with the -a flag (iterating over all buckets).
    """
    args = [
        '{}:{}'.format(server, port),
        '-u {}'.format(username),
        '-p {}'.format(password),
        '-b {}'.format(bucket) if bucket else '-a',
        '-j',
        command
    ]
    cmd = "./opt/couchbase/bin/cbstats {}".format(' '.join(args))
    stdout, _, returncode = run_local_shell_command(cmd)
    return stdout, returncode


def read_aws_credential(credential_path: str, delete_file: bool = True) -> str:
    logger.info("Reading AWS credential")
    cmd = f'cat {credential_path}/aws_credential'
    credential = local(cmd, capture=True)
    if delete_file:
        cmd = f'rm {credential_path}/aws_credential'
        local(cmd)
    return credential


def get_aws_credential(credential_path: str, delete_file: bool = True):
    logger.info("Reading AWS credential")
    with open('{}/aws_credential'.format(credential_path)) as f:
        lines = f.read().splitlines()
    aws_access_key_id = ''
    aws_secret_access_key = ''
    for line in lines:
        if "aws_access_key_id" in line:
            aws_access_key_id = line.replace(" ", "").split("=")[-1]
        if 'aws_secret_access_key' in line:
            aws_secret_access_key = line.replace(" ", "").split("=")[-1]

    if delete_file:
        cmd = 'rm {}/aws_credential'.format(credential_path)
        local(cmd)

    if aws_access_key_id and aws_secret_access_key:
        return aws_access_key_id, aws_secret_access_key
    else:
        raise Exception('cannot get aws credential')


def cbepctl(master_node: str, cluster_spec: ClusterSpec, bucket: str,
            option: str, value: int):
    flags = ['{}:11209'.format(master_node),
             '-u {}'.format(cluster_spec.rest_credentials[0]),
             '-p {}'.format(cluster_spec.rest_credentials[1]),
             '-b {}'.format(bucket),
             'set flush_param {} {}'.format(option, value)]

    cmd = './opt/couchbase/bin/cbepctl {}'.format(
        ' '.join(flags))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def create_remote_link(analytics_link, data_node, analytics_node, username, password):
    logger.info('Create analytics remote link')
    cmd = "curl -v -k -u {}:{} " \
          "-X POST http://{}:8095/analytics/link " \
          "-d dataverse=Default " \
          "-d name={} " \
          "-d type=couchbase " \
          "-d hostname={}:8091 " \
          "-d username={} " \
          "-d password={} " \
          "-d encryption=none ".format(username, password, analytics_node, analytics_link,
                                       data_node, username, password)
    local(cmd)


def download_pytppc(repo: str, branch: str):
    cmd = 'git clone -q -b {} {}'.format(branch, repo)
    local(cmd)


def download_all_s3_logs(path_name: str, file_name: str):
    logger.info('Downloading {}'.format(file_name))
    # <organization name>/<date as YYYY-MM-DD>/<logfile>
    cmd = 'aws s3 cp {} {}'.format(path_name, file_name)
    local(cmd)

def pytpcc_create_collections(collection_config: str, master_node:  str):

    cmd = 'cp py-tpcc/pytpcc/constants.py.collections py-tpcc/pytpcc/constants.py'
    logger.info("Copyied constants.py.collections {}".format(cmd))

    local(cmd)
    cmd = './py-tpcc/pytpcc/util/{} {}'.format(collection_config, master_node)

    logger.info("Creating Collections : {}".format(cmd))
    local(cmd)


def pytpcc_create_indexes(master_node: str, run_sql_shell: str, cbrindex_sql: str,
                          port: str, index_replica: int):

    cmd = './py-tpcc/pytpcc/util/{} {}:{} {} ' \
          '< ./py-tpcc/pytpcc/util/{} '.format(run_sql_shell, master_node, port,
                                               index_replica, cbrindex_sql)

    logger.info("Creating pytpcc Indexes : {}".format(cmd))

    local(cmd)


def pytpcc_create_functions(master_node: str, run_function_shell: str, cbrfunction_sql: str,
                            port: str, index_replica: int):

    cmd = './py-tpcc/pytpcc/util/{} {}:{} {} ' \
          '< ./py-tpcc/pytpcc/util/{} '.format(run_function_shell, master_node, port,
                                               index_replica, cbrfunction_sql)

    logger.info("Creating pytpcc Functions : {}".format(cmd))

    local(cmd)


def pytpcc_load_data(warehouse: int, client_threads: int,
                     master_node: str, port: str,
                     cluster_spec: ClusterSpec,
                     multi_query_node: bool,
                     driver: str,
                     nodes: list):

    if multi_query_node:
        nodes_length = len(nodes)
        multi_query_url = master_node + ':' + port

        for node in range(1, nodes_length):
            multi_query_url = multi_query_url + ',' + nodes[node] + ':' + port

        cmd = '../../env/bin/python3 ./tpcc.py --warehouses {}' \
              ' --clients {} {} --no-execute --query-url {}:{}' \
              ' --multi-query-url {}' \
              ' --userid {} --password {}'.format(warehouse, client_threads, driver,
                                                  master_node,
                                                  port,
                                                  multi_query_url,
                                                  cluster_spec.rest_credentials[0],
                                                  cluster_spec.rest_credentials[1])
    else:

        cmd = '../../env/bin/python3 ./tpcc.py --warehouses {}' \
              ' --clients {} {} --no-execute --query-url {}:{}' \
              ' --userid {} --password {}'.format(warehouse, client_threads, driver,
                                                  master_node,
                                                  port,
                                                  cluster_spec.rest_credentials[0],
                                                  cluster_spec.rest_credentials[1])

    logger.info("Loading Docs : {}".format(cmd))

    with lcd('py-tpcc/pytpcc/'):

        for line in local(cmd, capture=True).split('\n'):
            print(line)


def pytpcc_run_task(warehouse: int, duration: int, client_threads: int,
                    driver: str, master_node:  str,
                    multi_query_node: bool, cluster_spec: ClusterSpec, port: str,
                    nodes: list, durability_level: str, scan_consistency: str, txtimeout: str):

    if multi_query_node:
        nodes_length = len(nodes)
        multi_query_url = master_node + ':' + port

        for node in range(1, nodes_length):
            multi_query_url = multi_query_url + ',' + nodes[node] + ':' + port

        cmd = '../../env/bin/python3 ./tpcc.py --warehouses {} --duration {} ' \
              '--clients {} {} --query-url {}:{} ' \
              '--multi-query-url {} --userid {} --no-load --durability_level {} ' \
              '--password {} --scan_consistency {}' \
              ' --txtimeout {} > pytpcc_run_result.log'.format(warehouse,
                                                               duration,
                                                               client_threads, driver,
                                                               master_node, port,
                                                               multi_query_url,
                                                               cluster_spec.rest_credentials[0],
                                                               durability_level,
                                                               cluster_spec.rest_credentials[1],
                                                               scan_consistency, txtimeout)

    else:
        cmd = '../../env/bin/python3 ./tpcc.py --warehouses {} --duration {} ' \
              '--clients {} {} --query-url {}:{} ' \
              '--no-load --userid {} --password {} --durability_level {} ' \
              '--scan_consistency {} ' \
              '--txtimeout {} > pytpcc_run_result.log'.format(warehouse,
                                                              duration,
                                                              client_threads,
                                                              driver,
                                                              master_node,
                                                              port,
                                                              cluster_spec.rest_credentials[0],
                                                              cluster_spec.rest_credentials[1],
                                                              durability_level,
                                                              scan_consistency, txtimeout)

    logger.info("Running : {}".format(cmd))

    with lcd('py-tpcc/pytpcc/'):
        for line in local(cmd, capture=True).split('\n'):
            print(line)


def copy_pytpcc_run_output():
    cmd = 'cp py-tpcc/pytpcc/pytpcc_run_result.log .'
    local(cmd)


def ch2_run_task(
    conn_settings: CH2ConnectionSettings,
    task_settings: CH2,
    driver: str = "nestcollections",
    log_file: str = "ch2_mixed",
):
    cmd = (
        f"../../../env/bin/python3 ./tpcc.py {driver} "
        f"{conn_settings.cli_args_str_run(task_settings.tclients, task_settings.aclients)} "
        f"{task_settings.cli_args_str_run()} > ../../../{log_file}.log"
    )

    with lcd("ch2/ch2driver/pytpcc/"):
        logger.info(f"Running: {cmd}")
        local(cmd)


def ch2_load_task(
    conn_settings: CH2ConnectionSettings,
    task_settings: CH2,
    driver: str = "nestcollections",
    log_file: str = "ch2_load",
):
    if task_settings.load_mode == "qrysvc-load":
        output_dest = "/dev/null 2>&1"
    else:
        output_dest = f"../../../{log_file}.log"

    cmd = (
        f"../../../env/bin/python3 ./tpcc.py {driver} "
        f"{conn_settings.cli_args_str_load(task_settings.load_mode)} "
        f"{task_settings.cli_args_str_load()} > {output_dest}"
    )

    with lcd("ch2/ch2driver/pytpcc/"):
        logger.info(f"Running: {cmd}")
        local(cmd)


def ch3_load_task(
    conn_settings: CH3ConnectionSettings,
    task_settings: CH3,
    driver: str = 'nestcollections'
):
    if task_settings.load_mode == "qrysvc-load":
        output_dest = '/dev/null 2>&1'
    else:
        output_dest = '../../../ch3_load.log'

    cmd = '../../../env/bin/python3 ./tpcc.py {} {} {} > {}'.format(
        driver,
        conn_settings.cli_args_str_load(task_settings.load_mode),
        task_settings.cli_args_str_load(),
        output_dest
    )

    with lcd('ch3/ch3driver/pytpcc/'):
        logger.info('Running: {}'.format(cmd))
        local(cmd)


def ch3_create_fts_index(cluster_spec: ClusterSpec, fts_node: str):

    cmd = 'sh -x ./cbcrftsindexcollection.sh {} {}:{}'.format(fts_node,
                                                              cluster_spec.rest_credentials[0],
                                                              cluster_spec.rest_credentials[1])

    with lcd('ch3/ch3driver/pytpcc/util'):
        logger.info('Running: {}'.format(cmd))
        local(cmd)


def ch3_run_task(
    conn_settings: CH3ConnectionSettings,
    task_settings: CH3,
    driver: str = 'nestcollections',
    log_file: str = 'ch3_mixed'
):
    cmd = '../../../env/bin/python3 ./tpcc.py {} {} {} > ../../../{}.log'.format(
        driver,
        conn_settings.cli_args_str_run(task_settings.tclients,
                                       task_settings.aclients,
                                       task_settings.fclients),
        task_settings.cli_args_str_run(),
        log_file
    )

    with lcd('ch3/ch3driver/pytpcc/'):
        logger.info('Running: {}'.format(cmd))
        local(cmd)


def get_sg_logs(host: str, ssh_user: str, ssh_pass: str):
    local('sshpass -p {} scp {}@{}:/home/sync_gateway/*logs.tar.gz ./'.format(ssh_pass,
                                                                              ssh_user,
                                                                              host))


def get_sg_logs_new(host: str, ssh_user: str, ssh_pass: str):
    local('sshpass -p {0} scp {1}@{2}:/var/tmp/sglogs/syncgateway_logs.tar.gz '
          './{2}_syncgateway_logs.tar.gz'.format(ssh_pass,
                                                 ssh_user,
                                                 host))


def get_sg_console(host: str, ssh_user: str, ssh_pass: str):
    with settings(warn_only=True):
        local('sshpass -p {0} scp {1}@{2}:/var/tmp/sg_console* ./{2}_sg_console*'.format(ssh_pass,
                                                                                         ssh_user,
                                                                                         host))


def get_troublemaker_logs(host: str, ssh_user: str, ssh_pass: str):
    local('sshpass -p {} scp {}@{}:/root/troublemaker.log ./'.format(ssh_pass,
                                                                     ssh_user,
                                                                     host))


def get_default_troublemaker_logs(host: str, ssh_user: str, ssh_pass: str):
    local('sshpass -p {} scp {}@{}:/tmp/Logs/troublemaker-log.txt ./'.format(ssh_pass,
                                                                             ssh_user,
                                                                             host))


def rename_troublemaker_logs(from_host: str):
    local('cp ./troublemaker.log ./{}_troublemaker.log'.format(from_host))
    local('rm ./troublemaker.log')


def run_blackholepuller(host, clients, timeout, stderr_file_name, log_file_name):
    local('./blackholePuller -url http://sg-user-0:password@{}:4984/db-1 -clients {}'
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


def run_blackholepuller_users(host, clients, timeout, users_file_name, stderr_file_name,
                              log_file_name):
    local('./blackholePuller -url http://sg-user-0:password@{}:4984/db-1 -clients {}'
          ' -timeout {}s -userPasswordFilepath {} 2>{}.log 1>{}.json'.format(host, clients,
                                                                             timeout,
                                                                             users_file_name,
                                                                             stderr_file_name,
                                                                             log_file_name))


def run_blackholepuller_users_adv(url_str, clients, timeout, users_file_name, stderr_file_name,
                                  log_file_name):
    local('./blackholePuller -url {} -clients {}'
          ' -timeout {}s -userPasswordFilepath {} 2>{}.log 1>{}.json'.format(url_str, clients,
                                                                             timeout,
                                                                             users_file_name,
                                                                             stderr_file_name,
                                                                             log_file_name))


def run_newdocpusher(host, changebatchset, clients, timeout, stderr_file_name,
                     log_file_name, doc_id_prefix, doc_size):

    local('./newDocPusher -url http://sg-user-0:password@{}:4984/db-1 -changesBatchSize {} '
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


def replicate_push(cluster_spec: ClusterSpec, cblite_db: str, sgw_ip: str):
    if cluster_spec.capella_infrastructure:
        cmd = '/root/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'push --user guest:guest /root/couchbase-mobile-tools/{0}.cblite2 ' \
              'wss://{1}:4984/db-1'.format(cblite_db, sgw_ip)
    else:
        cmd = '/root/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'push --user guest:guest /root/couchbase-mobile-tools/{0}.cblite2 ' \
              'ws://{1}:4984/db-1'.format(cblite_db, sgw_ip)
    logger.info('Running: {}'.format(cmd))
    with quiet():
        return local(cmd, capture=True)


def replicate_pull(cluster_spec: ClusterSpec, cblite_db: str, sgw_ip: str):
    if cluster_spec.capella_infrastructure:
        cmd = '/root/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'pull --user guest:guest /root/couchbase-mobile-tools/{0}.cblite2 ' \
              'wss://{1}:4984/db-1'.format(cblite_db, sgw_ip)
    else:
        cmd = '/root/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'pull --user guest:guest /root/couchbase-mobile-tools/{0}.cblite2 ' \
              'ws://{1}:4984/db-1'.format(cblite_db, sgw_ip)
    logger.info('Running: {}'.format(cmd))
    with quiet():
        return local(cmd, capture=True)


def start_cblitedb(port: str, db_name: str):
    cmd = 'nohup /root/couchbase-mobile-tools/cblite/build_cmake/cblite --create serve --port' \
          ' {} /root/couchbase-mobile-tools/{}.cblite2 &>/dev/null &'.format(port, db_name)
    logger.info('Running: {}'.format(cmd))
    with lcd('/root/couchbase-mobile-tools/'):
        local(cmd)


def cleanup_cblite_db():
    cmd = 'rm -rf db*'
    with lcd('/root/couchbase-mobile-tools/'):
        local(cmd)


def clone_cblite():
    with lcd('/tmp/'):
        local('rm -rf couchbase-mobile-tools/')
        local('git clone https://github.com/couchbaselabs/couchbase-mobile-tools -b'
              'perf/no_compression')


def build_cblite():
    with lcd('/tmp/couchbase-mobile-tools/'):
        local('git submodule update --init --recursive')
    with lcd('/tmp/couchbase-mobile-tools/cblite/'):
        local('mkdir build_cmake')
    with lcd('/tmp/couchbase-mobile-tools/cblite/build_cmake'):
        local('/snap/bin/cmake ..')
        local('/usr/bin/make -j 5')


def replicate_push_continuous(cluster_spec: ClusterSpec, cblite_db: str, sgw_ip: str):
    if cluster_spec.capella_infrastructure:
        cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'push --continuous --user guest:guest /tmp/couchbase-mobile-tools/{0}.cblite2 ' \
              'wss://{1}:4984/db-1 &>/dev/null &'.format(cblite_db, sgw_ip)
    else:
        cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'push --continuous --user guest:guest /tmp/couchbase-mobile-tools/{0}.cblite2 ' \
              'ws://{1}:4984/db-1 &>/dev/null &'.format(cblite_db, sgw_ip)
    logger.info('Running: {}'.format(cmd))
    with quiet():
        local(cmd)


def replicate_pull_continuous(cluster_spec: ClusterSpec, cblite_db: str, sgw_ip: str):
    if cluster_spec.capella_infrastructure:
        cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'pull --continuous --user guest:guest /tmp/couchbase-mobile-tools/{0}.cblite2 ' \
              'wss://{1}:4984/db-1 &>/dev/null &'.format(cblite_db, sgw_ip)
    else:
        cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite ' \
              'pull --continuous --user guest:guest /tmp/couchbase-mobile-tools/{0}.cblite2 ' \
              'ws://{1}:4984/db-1 &>/dev/null &'.format(cblite_db, sgw_ip)
    logger.info('Running: {}'.format(cmd))
    with quiet():
        local(cmd)


def cleanup_cblite_db_coninuous():
    logger.info("cleaning up cblite db continuous")
    cmd = 'rm -rf db*'
    with lcd('/tmp/couchbase-mobile-tools/'):
        local(cmd)


def start_cblitedb_continuous(port: str, db_name: str):
    cmd = 'nohup /tmp/couchbase-mobile-tools/cblite/build_cmake/cblite --create serve --port' \
          ' {} {}.cblite2 &>/dev/null &'.format(port, db_name)
    logger.info('Running: {}'.format(cmd))
    with lcd('/tmp/couchbase-mobile-tools/'), quiet():
        local(cmd)
    logger.info('cblite started')


def clone_ycsb(repo: str, branch: str):
    repo = repo.replace("git://", "https://")
    logger.info('Cloning YCSB repository: {} branch: {}'.format(repo, branch))
    local('git clone -q -b {} {}'.format(branch, repo))


def kill_cblite():
    logger.info("cleaning up cblite db continuous")
    cmd = "ps auxww | grep 'cblite' | awk '{print $2}' | xargs kill -9"
    with settings(warn_only=True):
        local(cmd)


def set_up_s3_link(username: str, password: str, baseurl: str, external_dataset_type: str,
                   external_dataset_region: str, access_key: str, secret_access_key: str):
    logger.info('Create analytics external link')

    cmd = "curl -v -k -u {}:{} " \
          "-X POST {}/analytics/link " \
          "-d dataverse=Default " \
          "-d name=external_link " \
          "-d type={} " \
          "-d region={} " \
          "-d accessKeyId={} " \
          "--data-urlencode secretAccessKey={}".format(username, password, baseurl.strip('/'),
                                                       external_dataset_type,
                                                       external_dataset_region,
                                                       access_key, secret_access_key)
    local(cmd)


def create_javascript_udf(node, udflib, user, password, security):
    logger.info('Create Javascript UDF function')
    cmd = "curl -k -v -X POST " \
          "http://{}:8093/evaluator/v1/libraries/{} " \
          "-u {}:{}".format(node, udflib, user, password)
    if security:
        cmd = cmd.replace("http", "https")
        cmd = cmd.replace("8093", "18093")
    cmd = cmd + " -d 'function udf(id) { return id}'"
    local(cmd)


def create_x509_certificates(nodes: list[str]):
    ssl = SSLCertificate(nodes)
    ssl.generate()


def build_sdk_benchmark(benchmark_name: str, sdk_type: str, build_cmd: str = None):
    """Build the specified benchmark using the provided command."""
    if not build_cmd:
        return

    # Clean any existing log files
    with settings(hide('everything'), lcd('sdks/{}/'.format(sdk_type)), warn_only=True):
        local('rm {benchmark_name} {benchmark_name}*.log {benchmark_name}*.out'
              .format(benchmark_name=benchmark_name))

    run_cmd('sdks/{}/'.format(sdk_type), build_cmd, '', '{}.out'.format(benchmark_name))


def get_java_classpath(benchmark_name: str) -> str:
    """Return the cp.txt content as generated by maven during `build_sdk_benchmark`."""
    # Filename should be the sameone specified by `-Dmdep.outputFile=` in `get_sdk_build_command`.
    with open('sdks/java/{}/cp.txt'.format(benchmark_name), 'r') as cp:
        first_line = cp.readline()
        return first_line.replace('classpath=', '') if first_line else ''


def download_dotnet_client(version: str):
    """Download dotnet client from source.

    The version can be either a gerrit change reference or a commit/git hash.
    """
    local('git clone https://github.com/couchbase/couchbase-net-client.git')
    with lcd('couchbase-net-client'):
        if 'refs/changes/' in version:
            local('git fetch https://review.couchbase.org/couchbase-net-client '
                  '{} && git cherry-pick FETCH_HEAD'.format(version))
        else:
            local('git checkout {}'.format(version))
        local('dotnet build --configuration Release couchbase-net-client.sln')
        return local('pwd', capture=True)


def build_vectordb_bench():
    # Build using Makefile. When using couchbase-client branch,
    # this file is enhanced for easy of use
    logger.info("Building VectorDB Bench")
    with lcd("VectorDBBench"):
        local("make")


def run_vectordb_bench(options: str, dataset_dir: str, use_shuffled_data: bool = True):
    with lcd("VectorDBBench"):
        # Set `PYTHONPATH` to avoid package not found issues in some setup
        vectordb_path = local("pwd", capture=True)
        with shell_env(
            USE_SHUFFLED_DATA=str(use_shuffled_data),
            PYTHONPATH=vectordb_path,
            DATASET_LOCAL_DIR=dataset_dir,
        ):
            # Run using a command line tool which may not exist in upstream version
            local("env/bin/cmd_run {}".format(options))


def collect_cbopinfo_logs(kubeconfig: str):
    """Collect operator logs using cbopinfo."""
    # If the cao binaries exists, use them to collect extra logs
    with settings(warn_only=True), shell_env(KUBECONFIG=kubeconfig):
        local("couchbase-operator/bin/cao collect-logs --collectinfo --collectinfo-collect=all")

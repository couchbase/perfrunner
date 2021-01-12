import os
import shutil
import socket
import time
import urllib.parse
from datetime import date
from glob import glob
from sys import platform
from typing import List

from fabric.api import hide, lcd, local, quiet, settings, shell_env
from mc_bin_client.mc_bin_client import MemcachedClient, MemcachedError

from logger import logger
from perfrunner.settings import ClusterSpec


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
           wrapper: bool = False, mode: str = None, compression: bool = False,
           storage_type: str = None, sink_type: str = None,
           shards: int = None, include_data: str = None):
    logger.info('Creating a new backup: {}'.format(cluster_spec.backup))

    if not mode:
        cleanup(cluster_spec.backup)

    if wrapper:
        cbbackupwrapper(master_node, cluster_spec, 16, mode)
    else:
        cbbackupmgr_backup(master_node, cluster_spec, threads, mode,
                           compression, storage_type, sink_type, shards, include_data)


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


def cbbackupmgr_backup(master_node: str, cluster_spec: ClusterSpec,
                       threads: int, mode: str, compression: bool,
                       storage_type: str, sink_type: str, shards: int, include_data: str):
    if not mode:
        if include_data:

            logger.info('running ./opt/couchbase/bin/cbbackupmgr config '
                        '--archive {} --repo default --include-data {}'.format(cluster_spec.backup,
                                                                               include_data))

            local('./opt/couchbase/bin/cbbackupmgr config '
                  '--archive {} --repo default --include-data {}'.format(cluster_spec.backup,
                                                                         include_data))

        else:
            logger.info('running ./opt/couchbase/bin/cbbackupmgr config '
                        '--archive {} --repo default'.format(cluster_spec.backup))
            local('./opt/couchbase/bin/cbbackupmgr config '
                  '--archive {} --repo default'.format(cluster_spec.backup))

    flags = ['--archive {}'.format(cluster_spec.backup),
             '--repo default',
             '--host http://{}'.format(master_node),
             '--username {}'.format(cluster_spec.rest_credentials[0]),
             '--password {}'.format(cluster_spec.rest_credentials[1]),
             '--threads {}'.format(threads) if threads else None,
             '--storage {}'.format(storage_type) if storage_type else None,
             '--sink {}'.format(sink_type) if sink_type else None,
             '--value-compression compressed' if compression else None,
             '--shards {}'.format(shards) if shards else None]

    cmd = './opt/couchbase/bin/cbbackupmgr backup {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbbackupmgr_collectinfo(cluster_spec: ClusterSpec, archive: str = ''):
    logger.info('Running cbbackumgr cbcollect_info on local/host ')

    cmd = ('./opt/couchbase/bin/cbbackupmgr collect-logs --archive {} '
           '--output-dir {}'.format(archive or cluster_spec.backup, '.'))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def get_backup_snapshots(cluster_spec: ClusterSpec) -> List[str]:

    logger.info('running cbbackupmgr list/info command ')

    cmd_type = 'list'

    cmd = \
        './opt/couchbase/bin/cbbackupmgr list ' \
        '--archive {} --repo default '.format(cluster_spec.backup)

    for line in local(cmd, capture=True).split('\n'):
        if 'list is deprecated' in line:
            cmd_type = 'info'

    cmd = \
        './opt/couchbase/bin/cbbackupmgr {} ' \
        '--archive {} --repo default '.format(cmd_type,
                                              cluster_spec.backup,
                                              )
    snapshots = []
    if cmd_type == 'info':
        pattern = '+  {}-'.format(date.today().year)
        for line in local(cmd, capture=True).split('\n'):
            if pattern in line:
                snapshot = line.strip().split()[1]
                snapshots.append(snapshot)
        return snapshots

    elif cmd_type == 'list':
        pattern = '+ {}-'.format(date.today().year)
        for line in local(cmd, capture=True).split('\n'):
            if pattern in line:
                snapshot = line.strip().split()[-1]
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

    return round(backup_size) if rounded else backup_size


def restore(master_node: str, cluster_spec: ClusterSpec, threads: int,
            wrapper: bool = False, include_data: str = None):

    logger.info('Restore from {}'.format(cluster_spec.backup))

    if wrapper:
        cbrestorewrapper(master_node, cluster_spec)
    else:
        cbbackupmgr_restore(master_node, cluster_spec, threads, include_data)


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
                        threads: int, include_data: str, archive: str = '',
                        repo: str = 'default', map_data: str = None):

    flags = ['--archive {}'.format(archive or cluster_spec.backup),
             '--repo {}'.format(repo),
             '--include-data {}'.format(include_data) if include_data else None,
             '--threads {}'.format(threads),
             '--host http://{}'.format(master_node),
             '--username {}'.format(cluster_spec.rest_credentials[0]),
             '--password {}'.format(cluster_spec.rest_credentials[1]),
             '--map-data {}'.format(map_data) if map_data else None]

    cmd = './opt/couchbase/bin/cbbackupmgr restore --force-updates {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


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
                 '--log-file {}'.format(log_file) if log_file else None]

    else:
        flags = ['--format {}'.format(data_format),
                 '--output {}'.format(export_path),
                 '--cluster http://{}'.format(master_node),
                 '--bucket {}'.format(bucket),
                 '--username {}'.format(cluster_spec.rest_credentials[0]),
                 '--password {}'.format(cluster_spec.rest_credentials[1]),
                 '--threads {}'.format(threads) if threads else None,
                 '--include-key {}'.format(key_field) if key_field else None,
                 '--log-file {}'.format(log_file) if log_file else None]

    cmd = './opt/couchbase/bin/cbexport json {}'.format(
        ' '.join(filter(None, flags)))

    logger.info('Running: {}'.format(cmd))
    local(cmd)


def cbimport(master_node: str, cluster_spec: ClusterSpec, bucket: str,
             data_type: str, data_format: str, import_file: str, threads: int,
             scope_collection_exp: str, field_separator: str = None, limit_rows: int = None,
             skip_rows: int = None, infer_types: int = None,
             omit_empty: int = None, errors_log: str = None,
             log_file: str = None):

    if not scope_collection_exp:

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
                 '--log-file {}'.format(log_file) if log_file else None]

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
                 '--log-file {}'.format(log_file) if log_file else None,
                 '--scope-collection-exp {}'.format(scope_collection_exp)]

    cmd = './opt/couchbase/bin/cbimport {} {}'.format(
        data_type, ' '.join(filter(None, flags)))

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
             ycsb_jvm_args: str = None,
             collections_map: dict = None,
             out_of_order: int = 0,
             phase_params: dict = None):

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
          '-p exportfile=ycsb_{action}_{instance}.log ' \
          '-p couchbase.retryStrategy={retry_strategy} ' \
          '-p couchbase.retryLower={retry_lower} ' \
          '-p couchbase.retryUpper={retry_upper} ' \
          '-p couchbase.retryFactor={retry_factor} '

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
    if timeseries or cbcollect:
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
                     ycsb_jvm_args=ycsb_jvm_args)

    if soe_params is None:
        if phase_params is None:
            cmd += ' -p recordcount={items} '.format(items=items)
        else:
            cmd += ' -p totalrecordcount={totalrecordcount} '.format(totalrecordcount=items)
            cmd += ' -p recordcount={items} '.format(
                items=phase_params['inserts_per_workerinstance'])
            cmd += ' -p insertstart={insertstart} '.format(insertstart=phase_params['insertstart'])
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

    cmd += ' 2>ycsb_{action}_{instance}_stderr.log '.format(action=action, instance=instance)

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
        raise Exception('memcached was not killed properly')

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
                    run_in_background: bool = False,
                    collect_profile: bool = True):
    logger.info('Initiating scan workload')
    cmdstr = "{} -cluster {}:8091 -auth=\"{}:{}\" -configfile {} -resultfile result.json " \
             "-statsfile /root/statsfile" \
        .format(path_to_tool, node, rest_username, rest_password, configfile)
    if collect_profile:
        cmdstr += " -cpuprofile cpuprofile.prof -memprofile memprofile.prof "
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


def clone_git_repo(repo: str, branch: str, commit: str = None):
    repo_name = repo.split("/")[-1].split(".")[0]
    logger.info('checking if repo {} exists...'.format(repo_name))
    if os.path.exists("{}".format(repo_name)):
        logger.info('repo {} exists...removing...'.format(repo_name))
        shutil.rmtree(repo_name, ignore_errors=True)
    logger.info('Cloning repository: {} branch: {}'.format(repo, branch))
    local('git clone -q -b {} {}'.format(branch, repo))
    if commit:
        with lcd(repo_name):
            local('git checkout {}'.format(commit))


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


def get_cbstats(server: str, port: int, command: str, cluster_spec: ClusterSpec):
    cmd = "./opt/couchbase/bin/cbstats -a {}:{} -u {} -p {} {} -j" \
        .format(server, port, cluster_spec.rest_credentials[0],
                cluster_spec.rest_credentials[1], command)
    with hide('warnings'), settings(warn_only=True):
        result = local(cmd, capture=True)
        if result.return_code == 0:
            return result
        else:
            return False


def read_aws_credential(credential_path: str):
    logger.info("Reading AWS credential")
    cmd = 'cat {}/aws_credential'.format(credential_path)
    credential = local(cmd, capture=True)
    cmd = 'rm {}/aws_credential'.format(credential_path)
    local(cmd)
    return credential


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

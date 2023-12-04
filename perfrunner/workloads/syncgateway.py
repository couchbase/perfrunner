import math

from logger import logger
from perfrunner.helpers.local import restart_memcached, run_cmd
from perfrunner.settings import ClusterSpec, PhaseSettings, TargetSettings

BINARY_NAME = "pyenv local 2.7.18 && bin/ycsb"
BINARY_PATH = "YCSB"

LOAD_USERS_CMD = " load {ycsb_command} -s -P {workload} " \
                 "-p syncgateway.loadmode=users " \
                 "-threads {sg_loader_threads} " \
                 "-p syncgateway.db={db} " \
                 "-p syncgateway.host={hosts} " \
                 "-p memcached.host={memcached_host} " \
                 "-p recordcount={total_users} " \
                 "-p syncgateway.channels={total_channels} " \
                 "-p syncgateway.totalusers={total_users} " \
                 "-p syncgateway.channelsperuser={channels_per_user} " \
                 "-p syncgateway.channelsperdocument={channels_per_doc} " \
                 "-p insertstart={insertstart} " \
                 "-p exportfile={exportfile} " \
                 "-p syncgateway.usecapella={use_capella} " \
                 "-p syncgateway.starchannel={starchannel} " \
                 "-p syncgateway.e2e={e2e}"

LOAD_DOCS_CMD = " load {ycsb_command} -s -P {workload} " \
                "-p recordcount={total_docs} " \
                "-threads {sg_docloader_thread} " \
                "-p syncgateway.db={db} " \
                "-p fieldcount={fieldcount} " \
                "-p fieldlength={fieldlength} " \
                "-p syncgateway.host={hosts} " \
                "-p syncgateway.auth=false " \
                "-target {sg_load_throughput} " \
                "-p syncgateway.basic_auth={basic_auth} " \
                "-p syncgateway.feedmode={feedmode} " \
                "-p syncgateway.replicator2={replicator2} " \
                "-p memcached.host={memcached_host} " \
                "-p syncgateway.insertmode={insert_mode} " \
                "-p syncgateway.roundtrip={roundtrip} " \
                "-p syncgateway.feedmode={feedmode} " \
                "-p syncgateway.totalusers={total_users} " \
                "-p syncgateway.channels={total_channels} " \
                "-p syncgateway.channelsperuser={channels_per_user} " \
                "-p syncgateway.channelsperdocument={channels_per_doc} " \
                "-p insertstart={insertstart} " \
                "-p syncgateway.usecapella={use_capella} " \
                "-p exportfile={exportfile}"

INIT_USERS_CMD = " run {ycsb_command} -s -P {workload} " \
                 "-p recordcount={total_docs} " \
                 "-p syncgateway.db={db} " \
                 "-p operationcount=400 " \
                 "-p maxexecutiontime=36000 " \
                 "-threads {sg_loader_threads} " \
                 "-p syncgateway.host={hosts} " \
                 "-p syncgateway.auth={auth} " \
                 "-p memcached.host={memcached_host} " \
                 "-p syncgateway.totalusers={total_users} " \
                 "-p syncgateway.runmode=changesonly " \
                 "-p syncgateway.sequencestart={sequence_start} " \
                 "-p syncgateway.initusers=true " \
                 "-p insertstart={insertstart} " \
                 "-p readproportion=1 " \
                 "-p syncgateway.feedmode=normal " \
                 "-p syncgateway.usecapella={use_capella} " \
                 "-p exportfile={exportfile}"

GRANT_ACCESS_CMD = " run {ycsb_command} -s -P {workload} " \
                   "-p recordcount={total_docs} " \
                   "-p syncgateway.db={db} " \
                   "-p operationcount=1000 " \
                   "-p maxexecutiontime=36000 " \
                   "-threads 1 " \
                   "-p syncgateway.host={hosts} " \
                   "-p syncgateway.auth={auth} " \
                   "-p memcached.host={memcached_host} " \
                   "-p syncgateway.totalusers={total_users} " \
                   "-p syncgateway.runmode=changesonly " \
                   "-p syncgateway.sequencestart={sequence_start} " \
                   "-p insertstart={insertstart} " \
                   "-p readproportion=1 " \
                   "-p syncgateway.feedmode=normal " \
                   "-p exportfile={exportfile} " \
                   "-p syncgateway.channelspergrant={channels_per_grant} " \
                   "-p syncgateway.usecapella={use_capella} " \
                   "-p syncgateway.grantaccesstoall={grant_access}"

RUN_TEST_CMD = " run {ycsb_command} -s -P {workload} " \
               "-p recordcount={total_docs} " \
               "-p syncgateway.db={db} " \
               "-p operationcount=999999999 " \
               "-p fieldcount={fieldcount} " \
               "-p fieldlength={fieldlength} " \
               "-p maxexecutiontime={time} " \
               "-threads {threads} " \
               "-target {cbl_throughput} " \
               "-p syncgateway.host={hosts} " \
               "-p syncgateway.auth={auth} " \
               "-p syncgateway.channels={total_channels} " \
               "-p syncgateway.channelsperuser={channels_per_user} " \
               "-p syncgateway.channelsperdocument={channels_per_doc} " \
               "-p memcached.host={memcached_host} " \
               "-p syncgateway.totalusers={total_users} " \
               "-p syncgateway.roundtrip={roundtrip} " \
               "-p insertstart={insertstart} " \
               "-p syncgateway.readmode={read_mode} " \
               "-p syncgateway.insertmode={insert_mode} " \
               "-p syncgateway.sequencestart={sequence_start} " \
               "-p syncgateway.initusers=false " \
               "-p readproportion={readproportion} " \
               "-p updateproportion={updateproportion} " \
               "-p scanproportion={scanproportion} " \
               "-p insertproportion={insertproportion} " \
               "-p exportfile={exportfile} " \
               "-p syncgateway.feedmode={feedmode} " \
               "-p syncgateway.basic_auth={basic_auth} " \
               "-p syncgateway.replicator2={replicator2} " \
               "-p syncgateway.readLimit={readLimit} " \
               "-p syncgateway.usecapella={use_capella} " \
               "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

DELTA_SYNC_LOAD_DOCS_CMD = " load {ycsb_command} -s -P {workload} " \
                           "-p recordcount={total_docs} " \
                           "-p syncgateway.db={db} " \
                           "-p fieldcount={fieldcount} " \
                           "-p fieldlength={fieldlength} " \
                           "-p syncgateway.doctype={doctype} " \
                           "-p syncgateway.doc_depth={doc_depth} " \
                           "-threads {threads} " \
                           "-p syncgateway.deltasync={delta_sync} " \
                           "-p syncgateway.host={hosts} " \
                           "-p syncgateway.auth=false " \
                           "-p memcached.host={memcached_host} " \
                           "-p syncgateway.totalusers={total_users} " \
                           "-p syncgateway.channels={total_channels} " \
                           "-p syncgateway.channelsperuser={channels_per_user} " \
                           "-p syncgateway.channelsperdocument={channels_per_doc} " \
                           "-p insertstart={insertstart} " \
                           "-p syncgateway.usecapella={use_capella} " \
                           "-p exportfile={exportfile}"

DELTA_SYNC_RUN_TEST_CMD = " run {ycsb_command} -s -P {workload} " \
                          "-p recordcount={total_docs} " \
                          "-p syncgateway.db={db} " \
                          "-p operationcount={total_docs} " \
                          "-p fieldcount={fieldcount} " \
                          "-p maxexecutiontime={time} " \
                          "-threads {threads} " \
                          "-p syncgateway.deltasync={delta_sync} " \
                          "-p writeallfields={writeallfields} " \
                          "-p fieldlength={fieldlength} " \
                          "-p syncgateway.doctype={doctype} " \
                          "-p syncgateway.doc_depth={doc_depth} " \
                          "-p readallfields={readallfields} " \
                          "-p syncgateway.host={hosts} " \
                          "-p syncgateway.auth={auth} " \
                          "-p memcached.host={memcached_host} " \
                          "-p syncgateway.totalusers={total_users} " \
                          "-p syncgateway.roundtrip={roundtrip} " \
                          "-p insertstart={insertstart} " \
                          "-p syncgateway.readmode={read_mode} " \
                          "-p syncgateway.insertmode={insert_mode} " \
                          "-p syncgateway.sequencestart={sequence_start} " \
                          "-p syncgateway.initusers=false " \
                          "-p readproportion={readproportion} " \
                          "-p updateproportion={updateproportion} " \
                          "-p scanproportion={scanproportion}  " \
                          "-p syncgateway.updatefieldcount={updatefieldcount} " \
                          "-p insertproportion={insertproportion} " \
                          "-p syncgateway.channelsperdocument={channels_per_doc} " \
                          "-p exportfile={exportfile} " \
                          "-p syncgateway.feedmode={feedmode} " \
                          "-p syncgateway.usecapella={use_capella} " \
                          "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

E2E_SINGLE_LOAD_DOCS_CMD = " load {ycsb_command} -s -P {workload} " \
                           "-p recordcount={total_docs} " \
                           "-p syncgateway.db={db} " \
                           "-p fieldcount={fieldcount} " \
                           "-p fieldlength={fieldlength} " \
                           "-p syncgateway.doctype={doctype} " \
                           "-p syncgateway.doc_depth={doc_depth} " \
                           "-target {cbl_throughput} " \
                           "-threads {threads} " \
                           "-p syncgateway.deltasync={delta_sync} " \
                           "-p syncgateway.e2e={e2e} " \
                           "-p syncgateway.host={hosts} " \
                           "-p syncgateway.auth=false " \
                           "-p memcached.host={memcached_host} " \
                           "-p syncgateway.totalusers={total_users} " \
                           "-p syncgateway.channels={total_channels} " \
                           "-p syncgateway.channelsperuser={channels_per_user} " \
                           "-p syncgateway.channelsperdocument={channels_per_doc} " \
                           "-p insertstart={insertstart} " \
                           "-p insertcount={insertcount} " \
                           "-p core_workload_insertion_retry_limit={retry_count} " \
                           "-p core_workload_insertion_retry_interval={retry_interval} " \
                           "-p syncgateway.usecapella={use_capella} " \
                           "-p exportfile={exportfile}"

E2E_SINGLE_RUN_TEST_CMD = " run {ycsb_command} -s -P {workload} " \
                          "-p requestdistribution={request_distribution} " \
                          "-p syncgateway.db={db} " \
                          "-p operationcount={total_docs} " \
                          "-p recordcount={total_docs} " \
                          "-p fieldcount={fieldcount} " \
                          "-target {cbl_throughput} " \
                          "-threads {threads} " \
                          "-p syncgateway.deltasync={delta_sync} " \
                          "-p syncgateway.e2e={e2e} " \
                          "-p writeallfields={writeallfields} " \
                          "-p fieldlength={fieldlength} " \
                          "-p syncgateway.doctype={doctype} " \
                          "-p syncgateway.doc_depth={doc_depth} " \
                          "-p readallfields={readallfields} " \
                          "-p syncgateway.host={hosts} " \
                          "-p syncgateway.auth={auth} " \
                          "-p memcached.host={memcached_host} " \
                          "-p syncgateway.totalusers={total_users} " \
                          "-p syncgateway.roundtrip={roundtrip} " \
                          "-p insertstart={insertstart} " \
                          "-p insertcount={insertcount} " \
                          "-p syncgateway.readmode={read_mode} " \
                          "-p syncgateway.insertmode={insert_mode} " \
                          "-p syncgateway.sequencestart={sequence_start} " \
                          "-p syncgateway.initusers=false " \
                          "-p readproportion={readproportion} " \
                          "-p updateproportion={updateproportion} " \
                          "-p scanproportion={scanproportion}  " \
                          "-p syncgateway.updatefieldcount={updatefieldcount} " \
                          "-p insertproportion={insertproportion} " \
                          "-p syncgateway.channelsperdocument={channels_per_doc} " \
                          "-p exportfile={exportfile} " \
                          "-p syncgateway.feedmode={feedmode} " \
                          "-p syncgateway.maxretry={retry_count} " \
                          "-p syncgateway.retrydelay={retry_interval} " \
                          "-p syncgateway.usecapella={use_capella} " \
                          "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

E2E_MULTI_LOAD_DOCS_CMD = " load {ycsb_command} -s -P {workload} " \
                         "-p recordcount={total_docs} " \
                         "-p dataintegrity={data_integrity} " \
                         "-p syncgateway.db={db} " \
                         "-p syncgateway.user={user} " \
                         "-p syncgateway.channellist={channellist} " \
                         "-p fieldcount={fieldcount} " \
                         "-p fieldlength={fieldlength} " \
                         "-p syncgateway.doctype={doctype} " \
                         "-p syncgateway.doc_depth={doc_depth} " \
                         "-threads {threads} " \
                         "-target {cbl_throughput} " \
                         "-p syncgateway.deltasync={delta_sync} " \
                         "-p syncgateway.e2e={e2e} " \
                         "-p syncgateway.host={hosts} " \
                         "-p syncgateway.port.admin={port} " \
                         "-p syncgateway.auth=false " \
                         "-p memcached.host={memcached_host} " \
                         "-p syncgateway.totalusers={total_users} " \
                         "-p syncgateway.channels={total_channels} " \
                         "-p syncgateway.channelsperuser={channels_per_user} " \
                         "-p syncgateway.channelsperdocument={channels_per_doc} " \
                         "-p insertstart={insertstart} " \
                         "-p insertcount={insertcount} " \
                         "-p core_workload_insertion_retry_limit={retry_count} " \
                         "-p core_workload_insertion_retry_interval={retry_interval} " \
                         "-p exportfile={exportfile}"

E2E_MULTI_RUN_TEST_CMD = " run {ycsb_command} -s -P {workload} " \
                        "-p requestdistribution={request_distribution} " \
                        "-p operationcount={operations} " \
                        "-p recordcount={total_docs} " \
                        "-p dataintegrity={data_integrity} " \
                        "-p syncgateway.user={user} " \
                        "-p syncgateway.channellist={channellist} " \
                        "-p syncgateway.db={db} " \
                        "-p fieldcount={fieldcount} " \
                        "-target {cbl_throughput} " \
                        "-threads {threads} " \
                        "-p syncgateway.deltasync={delta_sync} " \
                        "-p syncgateway.e2e={e2e} " \
                        "-p writeallfields={writeallfields} " \
                        "-p fieldlength={fieldlength} " \
                        "-p syncgateway.doctype={doctype} " \
                        "-p syncgateway.doc_depth={doc_depth} " \
                        "-p readallfields={readallfields} " \
                        "-p syncgateway.host={hosts} " \
                        "-p syncgateway.port.admin={port} " \
                        "-p syncgateway.auth={auth} " \
                        "-p memcached.host={memcached_host} " \
                        "-p syncgateway.totalusers={total_users} " \
                        "-p syncgateway.roundtrip={roundtrip} " \
                        "-p insertstart={insertstart} " \
                        "-p insertcount={insertcount} " \
                        "-p syncgateway.readmode={read_mode} " \
                        "-p syncgateway.insertmode={insert_mode} " \
                        "-p syncgateway.sequencestart={sequence_start} " \
                        "-p syncgateway.initusers=false " \
                        "-p readproportion={readproportion} " \
                        "-p updateproportion={updateproportion} " \
                        "-p scanproportion={scanproportion}  " \
                        "-p syncgateway.updatefieldcount={updatefieldcount} " \
                        "-p insertproportion={insertproportion} " \
                        "-p syncgateway.channelsperdocument={channels_per_doc} " \
                        "-p exportfile={exportfile} " \
                        "-p syncgateway.feedmode={feedmode} " \
                        "-p syncgateway.maxretry={retry_count} " \
                        "-p syncgateway.retrydelay={retry_interval} " \
                        "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

E2E_CB_MULTI_LOAD_DOCS_CMD = " load {ycsb_command} -s -P {workload} " \
                             "-p requestdistribution=sequential " \
                             "-p recordcount={total_docs} " \
                             "-p dataintegrity={data_integrity} " \
                             "-p fieldcount={fieldcount} " \
                             "-p fieldlength={fieldlength} " \
                             '-p writeallfields=true ' \
                             "-threads {threads} " \
                             "-p insertstart={insertstart} " \
                             "-p insertcount={insertcount} " \
                             "-p couchbase.host={host} " \
                             "-p couchbase.bucket={bucket} " \
                             "-p couchbase.password=password " \
                             "-p couchbase.sgw=true " \
                             "-p couchbase.sgwChannels={channels} " \
                             "-p couchbase.upsert=false " \
                             '-p couchbase.epoll=true ' \
                             '-p couchbase.boost=12 ' \
                             '-p couchbase.kvEndpoints=1 ' \
                             '-p couchbase.sslMode={ssl_mode_sgw} ' \
                             '-p couchbase.certKeystoreFile=../certificates/data.keystore ' \
                             '-p couchbase.certKeystorePassword=storepass ' \
                             "-p readproportion=0 " \
                             "-p updateproportion=0 " \
                             "-p scanproportion=0  " \
                             "-p insertproportion=1 " \
                             "-p core_workload_insertion_retry_limit={retry_count} " \
                             "-p core_workload_insertion_retry_interval={retry_interval} " \
                             "-p exportfile={exportfile}"

E2E_CB_MULTI_RUN_TEST_CMD = " run {ycsb_command} -s -P {workload} " \
                             "-p requestdistribution=sequential " \
                             "-p operationcount={operations} " \
                             "-p recordcount={total_docs} " \
                             "-p dataintegrity={data_integrity} " \
                             "-p fieldcount={fieldcount} " \
                             "-p fieldlength={fieldlength} " \
                             '-p writeallfields=true ' \
                             "-threads {threads} " \
                             "-p insertstart={insertstart} " \
                             "-p insertcount={insertcount} " \
                             "-p couchbase.host={host} " \
                             "-p couchbase.bucket={bucket} " \
                             "-p couchbase.password=password " \
                             "-p couchbase.sgw=true " \
                             "-p couchbase.sgwChannels={channels} " \
                             "-p couchbase.upsert=false " \
                             '-p couchbase.epoll=true ' \
                             '-p couchbase.boost=12 ' \
                             '-p couchbase.kvEndpoints=1 ' \
                             '-p couchbase.sslMode={ssl_mode_sgw} ' \
                             '-p couchbase.certKeystoreFile=../certificates/data.keystore ' \
                             '-p couchbase.certKeystorePassword=storepass ' \
                             "-p readproportion=0 " \
                             "-p updateproportion=1 " \
                             "-p scanproportion=0  " \
                             "-p insertproportion=0 " \
                             "-p core_workload_insertion_retry_limit={retry_count} " \
                             "-p core_workload_insertion_retry_interval={retry_interval} " \
                             "-p exportfile={exportfile}"


def get_offset(workload_settings, worker_id):
    max_inserts = int(workload_settings.syncgateway_settings.max_inserts_per_instance)
    local_offset = worker_id * max_inserts
    return int(workload_settings.syncgateway_settings.insertstart) + local_offset


def get_hosts(cluster, workload_settings):
    if cluster.sgw_servers:
        if cluster.capella_infrastructure:
            return cluster.sgw_servers[0]
        else:
            return ','.join(cluster.sgw_servers[:int(workload_settings.syncgateway_settings.nodes)])
    else:
        return ','.join(cluster.servers[:int(workload_settings.syncgateway_settings.nodes)])


def get_cb_hosts(cluster):
    return ','.join(cluster.servers[3])


def get_memcached_host(cluster, workload_settings):
    if cluster.cloud_infrastructure and cluster.infrastructure_settings['provider'] == 'gcp':
        memcached_ip = "10.0.0.{}".format(6 + int(workload_settings.syncgateway_settings.nodes))
    elif cluster.cloud_infrastructure and (cluster.infrastructure_settings['provider'] == 'azure'
                                           or (cluster.infrastructure_settings['provider'] ==
                                               'capella' and
                                               (cluster.capella_backend == 'azure' or
                                                cluster.capella_backend == 'gcp'))):
        memcached_ip = next(cluster.clients_private)[1][0]
    else:
        memcached_ip = cluster.workers[0]
    logger.info("The memcached ip is: {}".format(memcached_ip))
    return memcached_ip


def add_collections(cmd, workload_settings: PhaseSettings, target: TargetSettings,
                    is_e2e_access: bool = False):
    sgs = workload_settings.syncgateway_settings
    collections_map = workload_settings.collections
    bucket = target.bucket
    target_scope_collections = collections_map[bucket]
    target_scopes = set()
    target_collections = set()

    for scope in target_scope_collections.keys():
        for collection in target_scope_collections[scope].keys():
            if target_scope_collections[scope][collection]['load'] == 1 \
                    and target_scope_collections[scope][collection]['access'] == 1:
                target_scopes.add(scope)
                target_collections.add(collection)

    if is_e2e_access:
        records_per_collection = int(sgs.documents)
    else:
        records_per_collection = int(sgs.documents) // len(target_collections)

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
    logger.info("The command is now: {}".format(cmd))
    return cmd


def add_capella_password(cmd):
    cmd += ' -p syncgateway.password=Password123! '
    cmd += ' -p syncgateway.adminname=Administrator '
    cmd += ' -p syncgateway.adminpassword=Password123! '
    return cmd


def syncgateway_start_memcached(workload_settings: PhaseSettings,
                                target: TargetSettings,
                                timer: int,
                                worker_id: int,
                                cluster: ClusterSpec):
    restart_memcached(get_memcached_host(cluster, workload_settings), mem_limit=20000, port=8000)


def syncgateway_load_users(workload_settings: PhaseSettings,
                           target: TargetSettings,
                           timer: int,
                           worker_id: int,
                           cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_loadusers_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_loadusers_{}_{}.result".format(sgs.log_title, worker_id, db)
    params = LOAD_USERS_CMD.format(ycsb_command=sgs.ycsb_command,
                                   workload=sgs.workload,
                                   db=db,
                                   hosts=get_hosts(cluster, workload_settings),
                                   memcached_host=get_memcached_host(cluster, workload_settings),
                                   total_users=sgs.users,
                                   sg_loader_threads=sgs.sg_loader_threads,
                                   total_channels=sgs.channels,
                                   channels_per_user=sgs.channels_per_user,
                                   channels_per_doc=sgs.channels_per_doc,
                                   insertstart=get_offset(workload_settings, worker_id),
                                   exportfile=res_file_name,
                                   use_capella="true"
                                               if cluster.has_any_capella else "false",
                                   starchannel=sgs.starchannel,
                                   e2e=sgs.e2e)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_load_docs(workload_settings: PhaseSettings,
                          target: TargetSettings,
                          timer: int, worker_id: int,
                          cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_loaddocs_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_loaddocs_{}_{}.result".format(sgs.log_title, worker_id, db)
    params = LOAD_DOCS_CMD.format(ycsb_command=sgs.ycsb_command,
                                  workload=sgs.workload,
                                  db=db,
                                  hosts=get_hosts(cluster, workload_settings),
                                  total_docs=sgs.documents,
                                  fieldlength=sgs.fieldlength,
                                  fieldcount=sgs.fieldcount,
                                  memcached_host=get_memcached_host(cluster, workload_settings),
                                  total_users=sgs.users,
                                  sg_load_throughput=sgs.sg_load_throughput,
                                  sg_docloader_thread=sgs.sg_docloader_thread,
                                  roundtrip=sgs.roundtrip_write_load,
                                  feedmode=sgs.feed_mode,
                                  replicator2=sgs.replicator2,
                                  basic_auth=sgs.basic_auth,
                                  total_channels=sgs.channels,
                                  insert_mode=sgs.insert_mode,
                                  channels_per_user=sgs.channels_per_user,
                                  channels_per_doc=sgs.channels_per_doc,
                                  insertstart=get_offset(workload_settings, worker_id),
                                  use_capella="true"
                                              if cluster.has_any_capella else "false",
                                  exportfile=res_file_name)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_init_users(workload_settings: PhaseSettings, target: TargetSettings,
                           timer: int, worker_id: int, cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_initusers_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_initusers_{}_{}.result".format(sgs.log_title, worker_id, db)
    params = INIT_USERS_CMD.format(ycsb_command=sgs.ycsb_command,
                                   workload=sgs.workload,
                                   db=db,
                                   hosts=get_hosts(cluster, workload_settings),
                                   total_docs=sgs.documents,
                                   sg_loader_threads=sgs.sg_loader_threads,
                                   memcached_host=get_memcached_host(cluster, workload_settings),
                                   auth=sgs.auth,
                                   total_users=sgs.users,
                                   insertstart=get_offset(workload_settings, worker_id),
                                   sequence_start=int(sgs.users) + int(sgs.documents) + 1,
                                   use_capella="true"
                                               if cluster.has_any_capella else "false",
                                   exportfile=res_file_name)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_grant_access(workload_settings: PhaseSettings,
                             target: TargetSettings,
                             timer: int,
                             worker_id: int,
                             cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_grantaccess_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_grantaccess_{}_{}.result".format(sgs.log_title, worker_id, db)
    params = GRANT_ACCESS_CMD.format(ycsb_command=sgs.ycsb_command,
                                     workload=sgs.workload,
                                     db=db,
                                     hosts=get_hosts(cluster, workload_settings),
                                     total_docs=sgs.documents,
                                     sg_loader_threads=sgs.sg_loader_threads,
                                     memcached_host=get_memcached_host(cluster, workload_settings),
                                     auth=sgs.auth,
                                     total_users=sgs.users,
                                     insertstart=get_offset(workload_settings, worker_id),
                                     sequence_start=int(sgs.users) + int(sgs.documents) + 1,
                                     exportfile=res_file_name,
                                     grant_access=sgs.grant_access,
                                     use_capella="true"
                                                 if cluster.has_any_capella else "false",
                                     channels_per_grant=sgs.channels_per_grant)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_run_test(workload_settings: PhaseSettings,
                         target: TargetSettings,
                         timer: int,
                         worker_id: int,
                         cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_runtest_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_runtest_{}_{}.result".format(sgs.log_title, worker_id, db)
    params = RUN_TEST_CMD.format(ycsb_command=sgs.ycsb_command,
                                 workload=sgs.workload,
                                 db=db,
                                 hosts=get_hosts(cluster, workload_settings),
                                 total_docs=sgs.documents_workset,
                                 fieldlength=sgs.fieldlength,
                                 fieldcount=sgs.fieldcount,
                                 memcached_host=get_memcached_host(cluster, workload_settings),
                                 auth=sgs.auth,
                                 cbl_throughput=sgs.cbl_throughput,
                                 total_users=sgs.users,
                                 total_channels=sgs.channels,
                                 channels_per_user=sgs.channels_per_user,
                                 channels_per_doc=sgs.channels_per_doc,
                                 insertstart=get_offset(workload_settings, worker_id),
                                 sequence_start=int(sgs.users) + int(sgs.documents) + 1,
                                 read_mode=sgs.read_mode,
                                 insert_mode=sgs.insert_mode,
                                 readLimit=sgs.sg_read_limit,
                                 replicator2=sgs.replicator2,
                                 basic_auth=sgs.basic_auth,
                                 threads=sgs.threads_per_instance,
                                 time=timer,
                                 roundtrip=sgs.roundtrip_write,
                                 readproportion=sgs.readproportion,
                                 updateproportion=sgs.updateproportion,
                                 insertproportion=sgs.insertproportion,
                                 scanproportion=sgs.scanproportion,
                                 exportfile=res_file_name,
                                 feedmode=sgs.feed_mode,
                                 use_capella="true"
                                             if cluster.has_any_capella else "false",
                                 grant_access_in_scan=sgs.grant_access_in_scan)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    logger.info("The command to be run is: {}".format(params))
    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_delta_sync_load_docs(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int, worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_loaddocs_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_loaddocs_{}_{}.result".format(sgs.log_title, worker_id, db)
    if sgs.replication_type == 'PUSH':
        phosts = '172.23.100.194'
    else:
        phosts = get_hosts(cluster, workload_settings)
    params = DELTA_SYNC_LOAD_DOCS_CMD.format(
        ycsb_command=sgs.ycsb_command,
        workload=sgs.workload,
        db=db,
        hosts=phosts,
        delta_sync=sgs.delta_sync,
        threads=sgs.threads_per_instance,
        total_docs=sgs.documents,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        memcached_host=get_memcached_host(cluster, workload_settings),
        total_users=sgs.users,
        total_channels=sgs.channels,
        channels_per_user=sgs.channels_per_user,
        channels_per_doc=sgs.channels_per_doc,
        insertstart=get_offset(workload_settings, worker_id),
        use_capella="true" if cluster.has_any_capella else "false",
        exportfile=res_file_name)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_delta_sync_run_test(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int,
        worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_runtest_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_runtest_{}_{}.result".format(sgs.log_title, worker_id, db)
    if sgs.replication_type == 'PUSH':
        phosts = '172.23.100.194'
    else:
        phosts = get_hosts(cluster, workload_settings)
    params = DELTA_SYNC_RUN_TEST_CMD.format(
        ycsb_command=sgs.ycsb_command,
        workload=sgs.workload,
        db=db,
        hosts=phosts,
        writeallfields=sgs.writeallfields,
        readallfields=sgs.readallfields,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        total_docs=sgs.documents_workset,
        memcached_host=get_memcached_host(cluster, workload_settings),
        auth=sgs.auth,
        total_users=sgs.users,
        insertstart=get_offset(workload_settings, worker_id),
        sequence_start=int(sgs.users) + int(sgs.documents) + 1,
        read_mode=sgs.read_mode,
        insert_mode=sgs.insert_mode,
        delta_sync=sgs.delta_sync,
        threads=sgs.threads_per_instance,
        time=timer,
        roundtrip=sgs.roundtrip_write,
        readproportion=sgs.readproportion,
        updateproportion=sgs.updateproportion,
        insertproportion=sgs.insertproportion,
        scanproportion=sgs.scanproportion,
        updatefieldcount=sgs.updatefieldcount,
        channels_per_doc=sgs.channels_per_doc,
        exportfile=res_file_name,
        feedmode=sgs.feed_mode,
        use_capella="true" if cluster.has_any_capella else "false",
        grant_access_in_scan=sgs.grant_access_in_scan)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_cbl_load_docs(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int, worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_loaddocs_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_loaddocs_{}_{}.result".format(sgs.log_title, worker_id, db)
    if sgs.replication_type == 'E2E_PUSH':
        phosts = '172.23.100.194'
    else:
        phosts = get_cb_hosts(cluster)

    instances = int(sgs.clients) * int(sgs.instances_per_client)
    docs = int(sgs.documents)
    docs_per_instance = math.ceil(docs / instances)
    insert_offset = (worker_id - 1) * docs_per_instance
    if insert_offset + docs_per_instance > docs:
        docs_per_instance = docs - insert_offset

    params = E2E_SINGLE_LOAD_DOCS_CMD.format(
        ycsb_command=sgs.ycsb_command,
        workload=sgs.workload,
        db=db,
        hosts=phosts,
        delta_sync=sgs.delta_sync,
        e2e=sgs.e2e,
        threads=sgs.threads_per_instance,
        total_docs=sgs.documents,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        cbl_throughput=sgs.cbl_throughput,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        memcached_host=get_memcached_host(cluster, workload_settings),
        total_users=sgs.users,
        total_channels=sgs.channels,
        channels_per_user=sgs.channels_per_user,
        channels_per_doc=sgs.channels_per_doc,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        use_capella="true" if cluster.has_any_capella else "false",
        exportfile=res_file_name)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_cbl_run_test(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int,
        worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_runtest_{}_{}.log".format(sgs.log_title, worker_id, db)
    res_file_name = "{}_runtest_{}_{}.result".format(sgs.log_title, worker_id, db)
    if sgs.replication_type == 'E2E_PUSH':
        phosts = '172.23.100.194'
    else:
        phosts = get_cb_hosts(cluster)
    params = E2E_SINGLE_RUN_TEST_CMD.format(
        ycsb_command=sgs.ycsb_command,
        workload=sgs.workload,
        db=db,
        hosts=phosts,
        writeallfields=sgs.writeallfields,
        readallfields=sgs.readallfields,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        cbl_throughput=sgs.cbl_throughput,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        total_docs=sgs.documents,
        memcached_host=get_memcached_host(cluster, workload_settings),
        auth=sgs.auth,
        total_users=sgs.users,
        insertstart=sgs.insertstart,
        insertcount=sgs.documents,
        sequence_start=int(sgs.users) + int(sgs.documents) + 1,
        read_mode=sgs.read_mode,
        insert_mode=sgs.insert_mode,
        delta_sync=sgs.delta_sync,
        e2e=sgs.e2e,
        threads=sgs.threads_per_instance,
        roundtrip=sgs.roundtrip_write,
        readproportion=sgs.readproportion,
        updateproportion=sgs.updateproportion,
        insertproportion=sgs.insertproportion,
        scanproportion=sgs.scanproportion,
        updatefieldcount=sgs.updatefieldcount,
        channels_per_doc=sgs.channels_per_doc,
        exportfile=res_file_name,
        feedmode=sgs.feed_mode,
        grant_access_in_scan=sgs.grant_access_in_scan,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        use_capella="true" if cluster.has_any_capella else "false",
        request_distribution=sgs.requestdistribution)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cbl_load_docs(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int, worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_loaddocs_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_loaddocs_{}.result".format(sgs.log_title, worker_id)
    instances = int(sgs.clients) * int(sgs.instances_per_client)
    docs = int(sgs.documents)
    docs_per_instance = math.ceil(docs / instances)
    insert_offset = (worker_id - 1) * docs_per_instance
    if insert_offset + docs_per_instance > docs:
        docs_per_instance = docs - insert_offset
    q, mod = divmod(worker_id-1, int(sgs.clients))
    target_port = 4985 + q
    params = E2E_MULTI_LOAD_DOCS_CMD.format(
        ycsb_command=sgs.ycsb_command,
        workload=sgs.workload,
        hosts=sgs.cbl_target,
        db="db_{}".format(q),
        user="sg-user-{}".format(worker_id-1),
        channellist="channel-{}".format(worker_id-1),
        port=target_port,
        delta_sync=sgs.delta_sync,
        e2e=sgs.e2e,
        threads=sgs.threads_per_instance,
        total_docs=sgs.documents,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        cbl_throughput=sgs.cbl_throughput,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        memcached_host=get_memcached_host(cluster, workload_settings),
        total_users=sgs.users,
        total_channels=sgs.channels,
        channels_per_user=sgs.channels_per_user,
        channels_per_doc=sgs.channels_per_doc,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        exportfile=res_file_name)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cbl_run_test(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int,
        worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_runtest_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_runtest_{}.result".format(sgs.log_title, worker_id)
    instances = int(sgs.clients) * int(sgs.instances_per_client)
    docs = int(sgs.documents)
    docs_per_instance = math.ceil(docs / instances)
    insert_offset = (worker_id - 1) * docs_per_instance
    if insert_offset + docs_per_instance > docs:
        docs_per_instance = docs - insert_offset
    q, mod = divmod(worker_id-1, int(sgs.clients))
    target_port = 4985 + q
    operations = docs_per_instance
    if workload_settings.collections:
        # When multiple scopes are supported, update this appropriatelly
        collections_count = len(workload_settings.collections[target.bucket]['scope-1'])
        operations = int(docs_per_instance) // collections_count

    params = E2E_MULTI_RUN_TEST_CMD.format(
        ycsb_command=sgs.ycsb_command,
        workload=sgs.workload,
        hosts=sgs.cbl_target,
        db="db_{}".format(q),
        user="sg-user-{}".format(worker_id-1),
        channellist="channel-{}".format(worker_id-1),
        port=target_port,
        writeallfields=sgs.writeallfields,
        readallfields=sgs.readallfields,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        cbl_throughput=sgs.cbl_throughput,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        total_docs=docs_per_instance,
        operations=operations,
        memcached_host=get_memcached_host(cluster, workload_settings),
        auth=sgs.auth,
        total_users=sgs.users,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        sequence_start=int(sgs.users) + int(sgs.documents) + 1,
        read_mode=sgs.read_mode,
        insert_mode=sgs.insert_mode,
        delta_sync=sgs.delta_sync,
        e2e=sgs.e2e,
        threads=sgs.threads_per_instance,
        roundtrip=sgs.roundtrip_write,
        readproportion=sgs.readproportion,
        updateproportion=sgs.updateproportion,
        insertproportion=sgs.insertproportion,
        scanproportion=sgs.scanproportion,
        updatefieldcount=sgs.updatefieldcount,
        channels_per_doc=sgs.channels_per_doc,
        exportfile=res_file_name,
        feedmode=sgs.feed_mode,
        grant_access_in_scan=sgs.grant_access_in_scan,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        request_distribution=sgs.requestdistribution)

    if cluster.has_any_capella:
        params = add_capella_password(params)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target, True)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cb_load_docs(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int, worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    replication_type = sgs.replication_type
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_loaddocs_{}_{}.log".format(sgs.log_title, worker_id+100, db)
    res_file_name = "{}_loaddocs_{}_{}.result".format(sgs.log_title, worker_id+100, db)
    instances = int(sgs.clients) * int(sgs.instances_per_client)
    docs = int(sgs.documents)
    docs_per_instance = math.ceil(docs / instances)
    insert_offset = (worker_id - 1) * docs_per_instance
    if insert_offset + docs_per_instance > docs:
        docs_per_instance = docs - insert_offset
    total_docs = sgs.documents
    if replication_type == "E2E_BIDI":
        total_docs += docs
        insert_offset += docs
    params = E2E_CB_MULTI_LOAD_DOCS_CMD.format(
        ycsb_command=workload_settings.ycsb_client,
        workload=workload_settings.workload_path,
        host=cluster.servers[0],
        bucket=target.bucket,
        threads=sgs.threads_per_instance,
        total_docs=total_docs,
        channels=sgs.channels,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        ssl_mode_sgw=sgs.ssl_mode_sgw,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        exportfile=res_file_name)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cb_run_test(
        workload_settings: PhaseSettings,
        timer: int,
        worker_id: int,
        cluster: ClusterSpec,
        target: TargetSettings):
    sgs = workload_settings.syncgateway_settings
    replication_type = sgs.replication_type
    bucket = target.bucket
    db = 'db-{}'.format(bucket.split('-')[1])
    log_file_name = "{}_runtest_{}_{}.log".format(sgs.log_title, worker_id+100, db)
    res_file_name = "{}_runtest_{}_{}.result".format(sgs.log_title, worker_id+100, db)
    instances = int(sgs.clients) * int(sgs.instances_per_client)
    docs = int(sgs.documents)
    docs_per_instance = math.ceil(docs / instances)
    insert_offset = (worker_id - 1) * docs_per_instance
    if insert_offset + docs_per_instance > docs:
        docs_per_instance = docs - insert_offset
    total_docs = sgs.documents
    if replication_type == "E2E_BIDI":
        total_docs += docs
        insert_offset += docs
    operations = docs_per_instance
    if workload_settings.collections:
        # When multiple scopes are supported, update this appropriatelly
        collections_count = len(workload_settings.collections.collection_map[bucket]['scope-1'])
        operations = int(docs_per_instance) // collections_count

    params = E2E_CB_MULTI_RUN_TEST_CMD.format(
        ycsb_command=workload_settings.ycsb_client,
        workload=workload_settings.workload_path,
        host=cluster.servers[0],
        bucket=target.bucket,
        threads=sgs.threads_per_instance,
        operations=operations,
        total_docs=total_docs,
        channels=sgs.channels,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        ssl_mode_sgw=sgs.ssl_mode_sgw,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        exportfile=res_file_name)

    if workload_settings.collections:
        params = add_collections(params, workload_settings, target, True)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def get_instance_home(workload_settings, worker_id):
    path = BINARY_PATH
    if worker_id:
        instances = int(workload_settings.syncgateway_settings.clients)
        instance_id = int((worker_id + instances - 1) / instances)
        path = "{}_{}".format(path, instance_id)
    return path

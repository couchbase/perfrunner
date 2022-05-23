import math

from perfrunner.helpers.local import restart_memcached, run_cmd
from perfrunner.settings import ClusterSpec, PhaseSettings

BINARY_NAME = "pyenv local system && bin/ycsb"
BINARY_PATH = "YCSB"

LOAD_USERS_CMD = " load syncgateway -s -P {workload} " \
                 "-p syncgateway.loadmode=users " \
                 "-threads {sg_loader_threads} " \
                 "-p syncgateway.host={hosts} " \
                 "-p memcached.host={memcached_host} " \
                 "-p recordcount={total_users} " \
                 "-p syncgateway.channels={total_channels} " \
                 "-p syncgateway.totalusers={total_users} " \
                 "-p syncgateway.channelsperuser={channels_per_user} " \
                 "-p insertstart={insertstart} " \
                 "-p exportfile={exportfile} " \
                 "-p syncgateway.starchannel={starchannel}"

LOAD_DOCS_CMD = " load syncgateway -s -P {workload} " \
                "-p recordcount={total_docs} " \
                "-threads {sg_docloader_thread} " \
                "-p fieldcount={fieldcount} " \
                "-p fieldlength={fieldlength} " \
                "-p syncgateway.host={hosts} " \
                "-p syncgateway.auth=false " \
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
                "-p insertstart={insertstart} " \
                "-p exportfile={exportfile}"

INIT_USERS_CMD = " run syncgateway -s -P {workload} " \
                 "-p recordcount={total_docs} " \
                 "-p operationcount=50 " \
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
                 "-p exportfile={exportfile}"

GRANT_ACCESS_CMD = " run syncgateway -s -P {workload} " \
                   "-p recordcount={total_docs} " \
                   "-p operationcount=50 " \
                   "-p maxexecutiontime=36000 " \
                   "-threads {sg_loader_threads} " \
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
                   "-p syncgateway.grantaccesstoall={grant_access}"

RUN_TEST_CMD = " run syncgateway -s -P {workload} " \
               "-p recordcount={total_docs} " \
               "-p operationcount=999999999 " \
               "-p fieldcount={fieldcount} " \
               "-p fieldlength={fieldlength} " \
               "-p maxexecutiontime={time} " \
               "-threads {threads} " \
               "-p syncgateway.host={hosts} " \
               "-p syncgateway.auth={auth} " \
               "-p syncgateway.channels={total_channels} " \
               "-p syncgateway.channelsperuser={channels_per_user} " \
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
               "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

DELTA_SYNC_LOAD_DOCS_CMD = " load syncgateway -s -P {workload} " \
                           "-p recordcount={total_docs} " \
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
                           "-p insertstart={insertstart} " \
                           "-p exportfile={exportfile}"

DELTA_SYNC_RUN_TEST_CMD = " run syncgateway -s -P {workload} " \
                          "-p recordcount={total_docs} " \
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
                          "-p exportfile={exportfile} " \
                          "-p syncgateway.feedmode={feedmode} " \
                          "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

E2E_SINGLE_LOAD_DOCS_CMD = " load syncgateway -s -P {workload} " \
                           "-p recordcount={total_docs} " \
                           "-p fieldcount={fieldcount} " \
                           "-p fieldlength={fieldlength} " \
                           "-p syncgateway.doctype={doctype} " \
                           "-p syncgateway.doc_depth={doc_depth} " \
                           "-threads {threads} " \
                           "-p syncgateway.deltasync={delta_sync} " \
                           "-p syncgateway.e2e={e2e} " \
                           "-p syncgateway.host={hosts} " \
                           "-p syncgateway.auth=false " \
                           "-p memcached.host={memcached_host} " \
                           "-p syncgateway.totalusers={total_users} " \
                           "-p syncgateway.channels={total_channels} " \
                           "-p syncgateway.channelsperuser={channels_per_user} " \
                           "-p insertstart={insertstart} " \
                           "-p insertcount={insertcount} " \
                           "-p core_workload_insertion_retry_limit={retry_count} " \
                           "-p core_workload_insertion_retry_interval={retry_interval} " \
                           "-p exportfile={exportfile}"

E2E_SINGLE_RUN_TEST_CMD = " run syncgateway -s -P {workload} " \
                          "-p requestdistribution={request_distribution} " \
                          "-p operationcount={total_docs} " \
                          "-p recordcount={total_docs} " \
                          "-p fieldcount={fieldcount} " \
                          "-p target={cbl_throughput} " \
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
                          "-p exportfile={exportfile} " \
                          "-p syncgateway.feedmode={feedmode} " \
                          "-p syncgateway.maxretry={retry_count} " \
                          "-p syncgateway.retrydelay={retry_interval} " \
                          "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

E2E_MULTI_LOAD_DOCS_CMD = " load syncgateway -s -P {workload} " \
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
                         "-p syncgateway.deltasync={delta_sync} " \
                         "-p syncgateway.e2e={e2e} " \
                         "-p syncgateway.host={hosts} " \
                         "-p syncgateway.port.admin={port} " \
                         "-p syncgateway.auth=false " \
                         "-p memcached.host={memcached_host} " \
                         "-p syncgateway.totalusers={total_users} " \
                         "-p syncgateway.channels={total_channels} " \
                         "-p syncgateway.channelsperuser={channels_per_user} " \
                         "-p insertstart={insertstart} " \
                         "-p insertcount={insertcount} " \
                         "-p core_workload_insertion_retry_limit={retry_count} " \
                         "-p core_workload_insertion_retry_interval={retry_interval} " \
                         "-p exportfile={exportfile}"

E2E_MULTI_RUN_TEST_CMD = " run syncgateway -s -P {workload} " \
                        "-p requestdistribution={request_distribution} " \
                        "-p operationcount={operations} " \
                        "-p recordcount={total_docs} " \
                        "-p dataintegrity={data_integrity} " \
                        "-p syncgateway.user={user} " \
                        "-p syncgateway.channellist={channellist} " \
                        "-p syncgateway.db={db} " \
                        "-p fieldcount={fieldcount} " \
                        "-p target={cbl_throughput} " \
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
                        "-p exportfile={exportfile} " \
                        "-p syncgateway.feedmode={feedmode} " \
                        "-p syncgateway.maxretry={retry_count} " \
                        "-p syncgateway.retrydelay={retry_interval} " \
                        "-p syncgateway.grantaccessinscan={grant_access_in_scan}"

E2E_CB_MULTI_LOAD_DOCS_CMD = " load couchbase2 -s -P workloads/syncgateway_blank " \
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
                             "-p couchbase.bucket=bucket-1 " \
                             "-p couchbase.password=password " \
                             "-p couchbase.sgw=true " \
                             "-p couchbase.sgwChannels={channels} " \
                             "-p couchbase.upsert=false " \
                             '-p couchbase.epoll=true ' \
                             '-p couchbase.boost=12 ' \
                             '-p couchbase.kvEndpoints=1 ' \
                             '-p couchbase.sslMode=none ' \
                             '-p couchbase.certKeystoreFile=../certificates/data.keystore ' \
                             '-p couchbase.certKeystorePassword=storepass ' \
                             "-p readproportion=0 " \
                             "-p updateproportion=0 " \
                             "-p scanproportion=0  " \
                             "-p insertproportion=1 " \
                             "-p core_workload_insertion_retry_limit={retry_count} " \
                             "-p core_workload_insertion_retry_interval={retry_interval} " \
                             "-p exportfile={exportfile}"

E2E_CB_MULTI_RUN_TEST_CMD = " run couchbase2 -s -P workloads/syncgateway_blank " \
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
                             "-p couchbase.bucket=bucket-1 " \
                             "-p couchbase.password=password " \
                             "-p couchbase.sgw=true " \
                             "-p couchbase.sgwChannels={channels} " \
                             "-p couchbase.upsert=false " \
                             '-p couchbase.epoll=true ' \
                             '-p couchbase.boost=12 ' \
                             '-p couchbase.kvEndpoints=1 ' \
                             '-p couchbase.sslMode=none ' \
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
        return ','.join(cluster.sgw_servers[:int(workload_settings.syncgateway_settings.nodes)])
    else:
        return ','.join(cluster.servers[:int(workload_settings.syncgateway_settings.nodes)])


def get_cb_hosts(cluster):
    return ','.join(cluster.servers[3])


def syncgateway_start_memcached(workload_settings: PhaseSettings,
                                timer: int,
                                worker_id: int,
                                cluster: ClusterSpec):
    restart_memcached(mem_limit=20000, port=8000, mem_host=cluster.workers[0])


def syncgateway_load_users(workload_settings: PhaseSettings,
                           timer: int,
                           worker_id: int,
                           cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_loadusers_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_loadusers_{}.result".format(sgs.log_title, worker_id)
    params = LOAD_USERS_CMD.format(workload=sgs.workload,
                                   hosts=get_hosts(cluster, workload_settings),
                                   memcached_host=cluster.workers[0],
                                   total_users=sgs.users,
                                   sg_loader_threads=sgs.sg_loader_threads,
                                   total_channels=sgs.channels,
                                   channels_per_user=sgs.channels_per_user,
                                   insertstart=get_offset(workload_settings, worker_id),
                                   exportfile=res_file_name,
                                   starchannel=sgs.starchannel)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_load_docs(workload_settings: PhaseSettings,
                          timer: int, worker_id: int,
                          cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_loaddocs_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_loaddocs_{}.result".format(sgs.log_title, worker_id)
    params = LOAD_DOCS_CMD.format(workload=sgs.workload,
                                  hosts=get_hosts(cluster, workload_settings),
                                  total_docs=sgs.documents,
                                  fieldlength=sgs.fieldlength,
                                  fieldcount=sgs.fieldcount,
                                  memcached_host=cluster.workers[0],
                                  total_users=sgs.users,
                                  sg_docloader_thread=sgs.sg_docloader_thread,
                                  roundtrip=sgs.roundtrip_write_load,
                                  feedmode=sgs.feed_mode,
                                  replicator2=sgs.replicator2,
                                  basic_auth=sgs.basic_auth,
                                  total_channels=sgs.channels,
                                  insert_mode=sgs.insert_mode,
                                  channels_per_user=sgs.channels_per_user,
                                  insertstart=get_offset(workload_settings, worker_id),
                                  exportfile=res_file_name)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_init_users(workload_settings: PhaseSettings, timer: int, worker_id: int,
                           cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_initusers_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_initusers_{}.result".format(sgs.log_title, worker_id)
    params = INIT_USERS_CMD.format(workload=sgs.workload,
                                   hosts=get_hosts(cluster, workload_settings),
                                   total_docs=sgs.documents,
                                   sg_loader_threads=sgs.sg_loader_threads,
                                   memcached_host=cluster.workers[0],
                                   auth=sgs.auth,
                                   total_users=sgs.users,
                                   insertstart=get_offset(workload_settings, worker_id),
                                   sequence_start=int(sgs.users) + int(sgs.documents) + 1,
                                   exportfile=res_file_name)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_grant_access(workload_settings: PhaseSettings,
                             timer: int,
                             worker_id: int,
                             cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_grantaccess_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_grantaccess_{}.result".format(sgs.log_title, worker_id)
    params = GRANT_ACCESS_CMD.format(workload=sgs.workload,
                                     hosts=get_hosts(cluster, workload_settings),
                                     total_docs=sgs.documents,
                                     sg_loader_threads=sgs.sg_loader_threads,
                                     memcached_host=cluster.workers[0],
                                     auth=sgs.auth,
                                     total_users=sgs.users,
                                     insertstart=get_offset(workload_settings, worker_id),
                                     sequence_start=int(sgs.users) + int(sgs.documents) + 1,
                                     exportfile=res_file_name,
                                     grant_access=sgs.grant_access,
                                     channels_per_grant=sgs.channels_per_grant)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_run_test(workload_settings: PhaseSettings,
                         timer: int,
                         worker_id: int,
                         cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_runtest_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_runtest_{}.result".format(sgs.log_title, worker_id)
    params = RUN_TEST_CMD.format(workload=sgs.workload,
                                 hosts=get_hosts(cluster, workload_settings),
                                 total_docs=sgs.documents_workset,
                                 fieldlength=sgs.fieldlength,
                                 fieldcount=sgs.fieldcount,
                                 memcached_host=cluster.workers[0],
                                 auth=sgs.auth,
                                 total_users=sgs.users,
                                 total_channels=sgs.channels,
                                 channels_per_user=sgs.channels_per_user,
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
                                 grant_access_in_scan=sgs.grant_access_in_scan)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_delta_sync_load_docs(
        workload_settings: PhaseSettings,
        timer: int, worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_loaddocs_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_loaddocs_{}.result".format(sgs.log_title, worker_id)
    if sgs.replication_type == 'PUSH':
        phosts = '172.23.100.194'
    else:
        phosts = get_hosts(cluster, workload_settings)
    params = DELTA_SYNC_LOAD_DOCS_CMD.format(
        workload=sgs.workload,
        hosts=phosts,
        delta_sync=sgs.delta_sync,
        threads=sgs.threads_per_instance,
        total_docs=sgs.documents,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        memcached_host=cluster.workers[0],
        total_users=sgs.users,
        total_channels=sgs.channels,
        channels_per_user=sgs.channels_per_user,
        insertstart=get_offset(workload_settings, worker_id),
        exportfile=res_file_name)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_delta_sync_run_test(
        workload_settings: PhaseSettings,
        timer: int,
        worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_runtest_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_runtest_{}.result".format(sgs.log_title, worker_id)
    if sgs.replication_type == 'PUSH':
        phosts = '172.23.100.194'
    else:
        phosts = get_hosts(cluster, workload_settings)
    params = DELTA_SYNC_RUN_TEST_CMD.format(
        workload=sgs.workload,
        hosts=phosts,
        writeallfields=sgs.writeallfields,
        readallfields=sgs.readallfields,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        total_docs=sgs.documents_workset,
        memcached_host=cluster.workers[0],
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
        exportfile=res_file_name,
        feedmode=sgs.feed_mode,
        grant_access_in_scan=sgs.grant_access_in_scan)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_cbl_load_docs(
        workload_settings: PhaseSettings,
        timer: int, worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_loaddocs_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_loaddocs_{}.result".format(sgs.log_title, worker_id)

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
        workload=sgs.workload,
        hosts=phosts,
        delta_sync=sgs.delta_sync,
        e2e=sgs.e2e,
        threads=sgs.threads_per_instance,
        total_docs=sgs.documents,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        memcached_host=cluster.workers[0],
        total_users=sgs.users,
        total_channels=sgs.channels,
        channels_per_user=sgs.channels_per_user,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        exportfile=res_file_name)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_cbl_run_test(
        workload_settings: PhaseSettings,
        timer: int,
        worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_runtest_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_runtest_{}.result".format(sgs.log_title, worker_id)
    if sgs.replication_type == 'E2E_PUSH':
        phosts = '172.23.100.194'
    else:
        phosts = get_cb_hosts(cluster)
    params = E2E_SINGLE_RUN_TEST_CMD.format(
        workload=sgs.workload,
        hosts=phosts,
        writeallfields=sgs.writeallfields,
        readallfields=sgs.readallfields,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        cbl_throughput=sgs.cbl_throughput,
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        total_docs=sgs.documents,
        memcached_host=cluster.workers[0],
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
        exportfile=res_file_name,
        feedmode=sgs.feed_mode,
        grant_access_in_scan=sgs.grant_access_in_scan,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        request_distribution=sgs.requestdistribution)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cbl_load_docs(
        workload_settings: PhaseSettings,
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
        doctype=sgs.doctype,
        doc_depth=sgs.doc_depth,
        memcached_host=cluster.workers[0],
        total_users=sgs.users,
        total_channels=sgs.channels,
        channels_per_user=sgs.channels_per_user,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        exportfile=res_file_name)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cbl_run_test(
        workload_settings: PhaseSettings,
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
    params = E2E_MULTI_RUN_TEST_CMD.format(
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
        total_docs=sgs.documents,
        operations=docs_per_instance,
        memcached_host=cluster.workers[0],
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
        exportfile=res_file_name,
        feedmode=sgs.feed_mode,
        grant_access_in_scan=sgs.grant_access_in_scan,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        request_distribution=sgs.requestdistribution)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cb_load_docs(
        workload_settings: PhaseSettings,
        timer: int, worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    replication_type = sgs.replication_type
    log_file_name = "{}_loaddocs_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_loaddocs_{}.result".format(sgs.log_title, worker_id)
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
        workload=sgs.workload,
        host=cluster.servers[0],
        threads=sgs.threads_per_instance,
        total_docs=total_docs,
        channels=sgs.channels,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        exportfile=res_file_name)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def syncgateway_e2e_multi_cb_run_test(
        workload_settings: PhaseSettings,
        timer: int,
        worker_id: int,
        cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    replication_type = sgs.replication_type
    log_file_name = "{}_runtest_{}.log".format(sgs.log_title, worker_id)
    res_file_name = "{}_runtest_{}.result".format(sgs.log_title, worker_id)
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
    params = E2E_CB_MULTI_RUN_TEST_CMD.format(
        workload=sgs.workload,
        host=cluster.servers[0],
        threads=sgs.threads_per_instance,
        operations=docs_per_instance,
        total_docs=total_docs,
        channels=sgs.channels,
        fieldlength=sgs.fieldlength,
        fieldcount=sgs.fieldcount,
        insertstart=insert_offset,
        insertcount=docs_per_instance,
        retry_count=sgs.ycsb_retry_count,
        retry_interval=sgs.ycsb_retry_interval,
        data_integrity=sgs.data_integrity,
        exportfile=res_file_name)

    path = get_instance_home(workload_settings, worker_id)
    run_cmd(path, BINARY_NAME, params, log_file_name)


def get_instance_home(workload_settings, worker_id):
    path = BINARY_PATH
    if worker_id:
        instances = int(workload_settings.syncgateway_settings.clients)
        instance_id = int((worker_id + instances - 1) / instances)
        path = "{}_{}".format(path, instance_id)
    return path

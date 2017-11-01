from perfrunner.settings import PhaseSettings, ClusterSpec
from perfrunner.helpers.local import run_cmd, restart_memcached


BINARY_NAME = "bin/ycsb"
BINARY_PATH = "YCSB"

LOAD_USERS_CMD = " load syncgateway -s -P {workload} -p syncgateway.loadmode=users -threads 50 " \
                 "-p syncgateway.host={hosts} -p memcached.host={memcached_host} -p recordcount={total_users} " \
                 "-p syncgateway.channels={total_channels} -p syncgateway.channelsperuser={channels_per_user} " \
                 "-p insertstart={insertstart}"

LOAD_DOCS_CMD = " load syncgateway -s -P {workload} -p recordcount={total_docs} -threads 50 " \
                "-p syncgateway.host={hosts} -p syncgateway.auth=false " \
                "-p memcached.host={memcached_host} -p syncgateway.totalusers={total_users} " \
                "-p syncgateway.channels={total_channels} -p syncgateway.channelsperuser={channels_per_user} " \
                "-p insertstart={insertstart}"

INIT_USERS_CMD = " run syncgateway -s -P {workload} -p recordcount={total_docs} -p operationcount=50 " \
                 "-p maxexecutiontime=36000 -threads 50 -p syncgateway.host={hosts} " \
                 "-p syncgateway.auth={auth} -p memcached.host={memcached_host} " \
                 "-p syncgateway.totalusers={total_users} -p syncgateway.runmode=changesonly " \
                 "-p syncgateway.sequencestart={sequence_start} -p syncgateway.initusers=true " \
                 "-p insertstart={insertstart} -p readproportion=1 -p syncgateway.feedmode=normal"

RUN_TEST_CMD = " run syncgateway -s -P {workload} -p recordcount={total_docs} -p operationcount=100000000 " \
               "-p maxexecutiontime={time} -threads {threads} -p syncgateway.host={hosts} -p syncgateway.auth={auth} " \
               "-p memcached.host={memcached_host} -p syncgateway.totalusers={total_users} " \
               "-p syncgateway.roundtrip={roundtrip} -p insertstart={insertstart} " \
               "-p syncgateway.readmode={read_mode} -p syncgateway.insertmode={insert_mode} " \
               "-p syncgateway.sequencestart={sequence_start} -p syncgateway.initusers=false " \
               "-p readproportion={readproportion} -p updateproportion={updateproportion} " \
               "-p insertproportion={insertproportion}"


def get_offset(workload_settings, worker_id):
    max_inserts = int(workload_settings.syncgateway_settings.max_inserts_per_instance);
    local_offset = worker_id * max_inserts
    return int(workload_settings.syncgateway_settings.insertstart) + local_offset


def get_hosts(cluster, workload_settings):
    return ','.join(cluster.servers[:int(workload_settings.syncgateway_settings.nodes)])


def syncgateway_start_memcached(workload_settings: PhaseSettings, timer: int, worker_id: int, cluster: ClusterSpec):
    restart_memcached(mem_limit=20000, port=8000, mem_host=cluster.workers[0])

def syncgateway_load_users(workload_settings: PhaseSettings, timer: int, worker_id: int, cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_loadusers_.log".format(sgs.log_title)
    params = LOAD_USERS_CMD.format(workload=sgs.workload,
                                hosts=get_hosts(cluster, workload_settings),
                                memcached_host=cluster.workers[0],
                                total_users=sgs.users,
                                total_channels=sgs.channels,
                                channels_per_user=sgs.channels_per_user,
                                insertstart=get_offset(workload_settings, worker_id))

    run_cmd(BINARY_PATH, BINARY_NAME, params, log_file_name)


def syncgateway_load_docs(workload_settings: PhaseSettings, timer: int, worker_id: int, cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_loaddocs_.log".format(sgs.log_title)
    params = LOAD_DOCS_CMD.format(workload=sgs.workload,
                                  hosts=get_hosts(cluster, workload_settings),
                                  total_docs=sgs.documents,
                                  memcached_host=cluster.workers[0],
                                  total_users=sgs.users,
                                  total_channels=sgs.channels,
                                  channels_per_user=sgs.channels_per_user,
                                  insertstart=get_offset(workload_settings, worker_id))

    run_cmd(BINARY_PATH, BINARY_NAME, params, log_file_name)


def syncgateway_init_users(workload_settings: PhaseSettings, timer: int, worker_id: int, cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_initusers_.log".format(sgs.log_title)
    params = INIT_USERS_CMD.format(workload=sgs.workload,
                                  hosts=get_hosts(cluster, workload_settings),
                                  total_docs=sgs.documents,
                                  memcached_host=cluster.workers[0],
                                  auth=sgs.auth,
                                  total_users=sgs.users,
                                  insertstart=get_offset(workload_settings, worker_id),
                                  sequence_start = int(sgs.users) + int(sgs.documents) + 1)

    run_cmd(BINARY_PATH, BINARY_NAME, params, log_file_name)



def syncgateway_run_test(workload_settings: PhaseSettings, timer: int, worker_id: int, cluster: ClusterSpec):
    sgs = workload_settings.syncgateway_settings
    log_file_name = "{}_runtest_{}.log".format(sgs.log_title, worker_id)
    params = RUN_TEST_CMD.format(workload=sgs.workload,
                                 hosts=get_hosts(cluster, workload_settings),
                                 total_docs=sgs.documents,
                                 memcached_host=cluster.workers[0],
                                 auth=sgs.auth,
                                 total_users=sgs.users,
                                 insertstart=get_offset(workload_settings, worker_id),
                                 sequence_start=int(sgs.users) + int(sgs.documents) + 1,
                                 read_mode=sgs.read_mode,
                                 insert_mode=sgs.insert_mode,
                                 threads=sgs.threads_per_instance,
                                 time=timer,
                                 roundtrip=sgs.roundtrip_write,
                                 readproportion=sgs.readproportion,
                                 updateproportion=sgs.updateproportion,
                                 insertproportion=sgs.insertproportion)

    run_cmd(BINARY_PATH, BINARY_NAME, params, log_file_name)
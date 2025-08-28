import csv
import random

from logger import logger
from perfrunner.helpers.local import (
    run_blackholepuller,
    run_blackholepuller_adv,
    run_blackholepuller_users,
    run_blackholepuller_users_adv,
    run_newdocpusher,
    run_newdocpusher_adv,
)
from perfrunner.settings import ClusterSpec, PhaseSettings, TargetSettings

BINARY_NAME = "/BlackHolePuller"
BINARY_PATH = "/SG_Tools_1"


def get_hosts(cluster, workload_settings):
    return cluster.sgw_servers


def get_host(cluster, workload_settings):
    return cluster.sgw_servers[0]


def build_multihost_url(cluster, workload_settings):
    url_string = ''
    for server in cluster.sgw_servers:
        url = f'http://sg-user-0:password@{server}:4984/db-1,'
        url_string = url_string + url
    return url_string[:-1]


def generate_csv_file(clients: int, users: int, users_file_name: str):
    with open(users_file_name, 'w') as fh:
        writer = csv.writer(fh)
        for user in range(0, clients):
            current_user = f"sg-user-{random.randint(0, users)}"
            writer.writerow([current_user, "password"])


def blackholepuller_runtest(workload_settings: PhaseSettings, target: TargetSettings,
                            timer: int, worker_id: int, cluster: ClusterSpec):

    sgs = workload_settings.syncgateway_settings

    log_file_name = f"sg_stats_blackholepuller_{worker_id}"
    logger.info(f'printing logfile name {log_file_name}')

    stderr_log_file_name = f'sg_stderr_blackholepuller_{worker_id}'

    users_file_name = 'sg_bhp_users.csv'

    if int(workload_settings.syncgateway_settings.nodes) > 1:
        if sgs.sg_blackholepuller_users == 0:
            run_blackholepuller_adv(url_str=build_multihost_url(cluster, workload_settings),
                                    clients=sgs.sg_blackholepuller_client,
                                    timeout=sgs.sg_blackholepuller_timeout,
                                    subprotocolVersion=sgs.sg_blackholepuller_subprotocol,
                                    stderr_file_name=stderr_log_file_name,
                                    log_file_name=log_file_name)
        else:
            generate_csv_file(int(sgs.sg_blackholepuller_client),
                              int(sgs.sg_blackholepuller_users),
                              users_file_name)
            run_blackholepuller_users_adv(url_str=build_multihost_url(cluster, workload_settings),
                                          clients=sgs.sg_blackholepuller_client,
                                          timeout=sgs.sg_blackholepuller_timeout,
                                          subprotocolVersion=sgs.sg_blackholepuller_subprotocol,
                                          users_file_name=users_file_name,
                                          stderr_file_name=stderr_log_file_name,
                                          log_file_name=log_file_name)

    else:
        if sgs.sg_blackholepuller_users == 0:
            run_blackholepuller(host=get_host(cluster, workload_settings),
                                clients=sgs.sg_blackholepuller_client,
                                timeout=sgs.sg_blackholepuller_timeout,
                                subprotocolVersion=sgs.sg_blackholepuller_subprotocol,
                                stderr_file_name=stderr_log_file_name,
                                log_file_name=log_file_name)
        else:
            generate_csv_file(sgs.sg_blackholepuller_client, sgs.sg_blackholepuller_users,
                              users_file_name)
            run_blackholepuller_users(host=get_host(cluster, workload_settings),
                                      clients=sgs.sg_blackholepuller_client,
                                      timeout=sgs.sg_blackholepuller_timeout,
                                      subprotocolVersion=sgs.sg_blackholepuller_subprotocol,
                                      users_file_name=users_file_name,
                                      stderr_file_name=stderr_log_file_name,
                                      log_file_name=log_file_name)


def newdocpusher_runtest(workload_settings: PhaseSettings, target: TargetSettings,
                         timer: int, worker_id: int, cluster: ClusterSpec):

    sgs = workload_settings.syncgateway_settings

    log_file_name = f"sg_stats_newdocpusher_{worker_id}"
    logger.info(f'printing logfile name {log_file_name}')

    stderr_log_file_name = f'sg_stderr_newdocpusher_{worker_id}'

    doc_id_prefix = 'perf' + str(worker_id)

    if int(workload_settings.syncgateway_settings.nodes) > 1:

        run_newdocpusher_adv(url_str=build_multihost_url(cluster, workload_settings),
                             clients=sgs.sg_blackholepuller_client,
                             doc_size=sgs.sg_docsize,
                             timeout=sgs.sg_blackholepuller_timeout,
                             subprotocolVersion=sgs.sg_blackholepuller_subprotocol,
                             useProposeChanges=sgs.sg_blackholepuller_use_propose_changes,
                             stderr_file_name=stderr_log_file_name,
                             log_file_name=log_file_name,
                             doc_id_prefix=doc_id_prefix,
                             changebatchset=sgs.sgtool_changebatchset)

    else:
        run_newdocpusher(host=get_host(cluster, workload_settings),
                         clients=sgs.sg_blackholepuller_client,
                         doc_size=sgs.sg_docsize,
                         timeout=sgs.sg_blackholepuller_timeout,
                         subprotocolVersion=sgs.sg_blackholepuller_subprotocol,
                         useProposeChanges=sgs.sg_blackholepuller_use_propose_changes,
                         stderr_file_name=stderr_log_file_name,
                         log_file_name=log_file_name,
                         doc_id_prefix=doc_id_prefix,
                         changebatchset=sgs.sgtool_changebatchset)


def get_instance_home(workload_settings, worker_id):
    path = BINARY_PATH
    if worker_id:
        instances = int(workload_settings.syncgateway_settings.clients)
        instance_id = int((worker_id + instances - 1) / instances)
        path = f"{path}_{instance_id}"
    return path

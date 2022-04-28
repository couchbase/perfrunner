from logger import logger
from perfrunner.helpers.local import (
    run_blackholepuller,
    run_blackholepuller_adv,
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
        url = 'http://sg-user-0:password@{}:4984/db-1'.format(server) + ','
        url_string = url_string + url
    return url_string[:-1]


def blackholepuller_runtest(workload_settings: PhaseSettings, target: TargetSettings,
                            timer: int, worker_id: int, cluster: ClusterSpec):

    sgs = workload_settings.syncgateway_settings

    log_file_name = "sg_stats_blackholepuller_{}".format(worker_id)
    logger.info('printing logfile name {}'.format(log_file_name))

    stderr_log_file_name = 'sg_stderr_blackholepuller_{}'.format(worker_id)

    if int(workload_settings.syncgateway_settings.nodes) > 1:
        run_blackholepuller_adv(url_str=build_multihost_url(cluster, workload_settings),
                                clients=sgs.sg_blackholepuller_client,
                                timeout=sgs.sg_blackholepuller_timeout,
                                stderr_file_name=stderr_log_file_name,
                                log_file_name=log_file_name)

    else:
        run_blackholepuller(host=get_host(cluster, workload_settings),
                            clients=sgs.sg_blackholepuller_client,
                            timeout=sgs.sg_blackholepuller_timeout,
                            stderr_file_name=stderr_log_file_name,
                            log_file_name=log_file_name)


def newdocpusher_runtest(workload_settings: PhaseSettings, target: TargetSettings,
                         timer: int, worker_id: int, cluster: ClusterSpec):

    sgs = workload_settings.syncgateway_settings

    log_file_name = "sg_stats_newdocpusher_{}".format(worker_id)
    logger.info('printing logfile name {}'.format(log_file_name))

    stderr_log_file_name = 'sg_stderr_newdocpusher_{}'.format(worker_id)

    doc_id_prefix = 'perf' + str(worker_id)

    if int(workload_settings.syncgateway_settings.nodes) > 1:

        logger.info('entered run_newdocpusher_adv method')
        logger.info('urls: {}'.format(build_multihost_url(cluster, workload_settings)))

        run_newdocpusher_adv(url_str=build_multihost_url(cluster, workload_settings),
                             clients=sgs.sg_blackholepuller_client,
                             doc_size=sgs.sg_docsize,
                             timeout=sgs.sg_blackholepuller_timeout,
                             stderr_file_name=stderr_log_file_name,
                             log_file_name=log_file_name,
                             doc_id_prefix=doc_id_prefix,
                             changebatchset=sgs.sgtool_changebatchset)

    else:
        run_newdocpusher(host=get_host(cluster, workload_settings),
                         clients=sgs.sg_blackholepuller_client,
                         doc_size=sgs.sg_docsize,
                         timeout=sgs.sg_blackholepuller_timeout,
                         stderr_file_name=stderr_log_file_name,
                         log_file_name=log_file_name,
                         doc_id_prefix=doc_id_prefix,
                         changebatchset=sgs.sgtool_changebatchset)


def get_instance_home(workload_settings, worker_id):
    path = BINARY_PATH
    if worker_id:
        instances = int(workload_settings.syncgateway_settings.clients)
        instance_id = int((worker_id + instances - 1) / instances)
        path = "{}_{}".format(path, instance_id)
    return path

from logger import logger
from perfrunner.helpers.local import run_cmd
from perfrunner.settings import PhaseSettings, TargetSettings

BINARY_NAME = "./conflictSim"

CONFLICTSIM_PARAMS = "-source {source} " \
                     "-target {target} " \
                     "-srcBucket {srcBucket} " \
                     "-srcPassword {srcPassword} " \
                     "-srcUsername {srcUsername} " \
                     "-tgtBucket {tgtBucket} " \
                     "-tgtPassword {tgtPassword} " \
                     "-tgtUsername {tgtUsername} " \
                     "-workers {workers} " \
                     "-numConflicts {numConflicts} " \
                     "-gap {gap} " \
                     "-distribution {distribution} " \
                     "-debugMode {debugMode} " \
                     "-batchSize {batchSize} " \


def run_conflictsim(workload_settings: PhaseSettings,
                    source_target: TargetSettings,
                    dest_target: TargetSettings,
                    timer: int):
    logger.info("Entered params in conflictSim")
    num_docs = int(workload_settings.items)
    gap = workload_settings.gap
    distribution = workload_settings.distribution
    debug_mode = workload_settings.debug_mode
    batch_size = workload_settings.batch_size
    workers = workload_settings.workers
    source = f"{source_target.node}:8091"
    target = f"{dest_target.node}:8091"
    bucket = source_target.bucket
    password = source_target.password
    username = source_target.username

    params = CONFLICTSIM_PARAMS.format(source=source,
                                       srcBucket=bucket,
                                       srcPassword=password,
                                       srcUsername=username,
                                       target=target,
                                       tgtBucket=bucket,
                                       tgtPassword=password,
                                       tgtUsername=username,
                                       workers=workers,
                                       numConflicts=num_docs,
                                       gap=gap,
                                       distribution=distribution,
                                       debugMode=debug_mode,
                                       batchSize=batch_size)

    logger.info("Running conflictSim with params: {}".format(params))

    path = "/tmp/conflictsim/"

    log_file_name = 'conflictSim.log'

    run_cmd(path, BINARY_NAME, params, log_file_name)

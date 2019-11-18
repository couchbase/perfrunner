from perfrunner.helpers.local import run_tpcds_loader
from perfrunner.settings import PhaseSettings, TargetSettings


def tpcds_initial_data_load(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int,
        instance: int):

    run_tpcds_loader(
        host=target.node,
        bucket=target.bucket,
        password=target.password,
        partitions=workload_settings.workload_instances * 2,
        instance=instance,
        scale_factor=workload_settings.tpcds_scale_factor)


def tpcds_remaining_data_load(
        workload_settings: PhaseSettings,
        target: TargetSettings,
        timer: int,
        instance: int):

    run_tpcds_loader(
        host=target.node,
        bucket=target.bucket,
        password=target.password,
        partitions=workload_settings.workload_instances * 2,
        instance=workload_settings.workload_instances + instance,
        scale_factor=workload_settings.tpcds_scale_factor)

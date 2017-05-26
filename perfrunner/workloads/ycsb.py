from perfrunner.helpers.local import run_ycsb
from perfrunner.settings import PhaseSettings, TargetSettings


def ycsb_data_load(workload_settings: PhaseSettings,
                   target: TargetSettings,
                   *args):
    run_ycsb(host=target.node,
             bucket=target.bucket,
             password=target.password,
             action='load',
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers)


def ycsb_workload(workload_settings: PhaseSettings,
                  target: TargetSettings,
                  timer: int,
                  instance: int):
    run_ycsb(host=target.node,
             bucket=target.bucket,
             password=target.password,
             action='run',
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers,
             ops=int(workload_settings.ops),
             time=workload_settings.time,
             instance=instance)

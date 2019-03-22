from perfrunner.helpers.local import restart_memcached, run_ycsb
from perfrunner.settings import PhaseSettings, TargetSettings


def ycsb_data_load(workload_settings: PhaseSettings,
                   target: TargetSettings,
                   timer: int,
                   instance: int):
    soe_params = None
    if workload_settings.recorded_load_cache_size:
        restart_memcached()
        soe_params = {
            'insertstart': (instance + 1) * workload_settings.inserts_per_workerinstance,
            'recorded_load_cache_size': workload_settings.recorded_load_cache_size,
        }

    run_ycsb(host=target.node,
             bucket=target.bucket,
             password=target.password,
             action='load',
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers,
             target=int(workload_settings.target),
             soe_params=soe_params,
             epoll=workload_settings.epoll,
             boost=workload_settings.boost,
             instance=instance)


def ycsb_workload(workload_settings: PhaseSettings,
                  target: TargetSettings,
                  timer: int,
                  instance: int):
    soe_params = None
    if workload_settings.recorded_load_cache_size:
        soe_params = {
            'insertstart': (instance + 1) * workload_settings.inserts_per_workerinstance,
            'recorded_load_cache_size': workload_settings.recorded_load_cache_size,
        }

    run_ycsb(host=target.node,
             bucket=target.bucket,
             password=target.password,
             action='run',
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers,
             target=int(workload_settings.target),
             soe_params=soe_params,
             epoll=workload_settings.epoll,
             boost=workload_settings.boost,
             ops=int(workload_settings.ops),
             time=workload_settings.time,
             instance=instance)

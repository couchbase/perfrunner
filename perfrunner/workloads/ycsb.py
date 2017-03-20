from perfrunner.helpers.local import run_ycsb


def ycsb_data_load(workload_settings, target, *args, **kwargs):
    host = target.node.split(':')[0]

    run_ycsb(host=host,
             bucket=target.bucket,
             password=target.password,
             action='load',
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers)


def ycsb_workload(workload_settings, target, *args, **kwargs):
    host = target.node.split(':')[0]

    run_ycsb(host=host,
             bucket=target.bucket,
             password=target.password,
             action='run',
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers,
             ops=int(workload_settings.ops),
             time=workload_settings.time)

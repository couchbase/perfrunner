from perfrunner.helpers.local import run_cbc_pillowfight


def pillowfight_data_load(workload_settings, target, *args, **kwargs):
    host, _ = target.node.split(':')

    run_cbc_pillowfight(host=host,
                        bucket=target.bucket,
                        password=target.password,
                        num_items=workload_settings.items,
                        num_threads=workload_settings.workers,
                        num_cycles=workload_settings.iterations,
                        size=workload_settings.size,
                        writes=workload_settings.creates,
                        populate=True,
                        use_ssl=workload_settings.use_ssl)


def pillowfight_workload(workload_settings, target, *args, **kwargs):
    host, _ = target.node.split(':')

    run_cbc_pillowfight(host=host,
                        bucket=target.bucket,
                        password=target.password,
                        num_items=workload_settings.items,
                        num_threads=workload_settings.workers,
                        num_cycles=workload_settings.iterations,
                        size=workload_settings.size,
                        writes=workload_settings.updates,
                        use_ssl=workload_settings.use_ssl)

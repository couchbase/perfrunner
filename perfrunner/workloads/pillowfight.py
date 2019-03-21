from perfrunner.helpers.local import run_cbc_pillowfight
from perfrunner.settings import PhaseSettings, TargetSettings


def pillowfight_data_load(workload_settings: PhaseSettings,
                          target: TargetSettings,
                          *args):

    run_cbc_pillowfight(host=target.node,
                        bucket=target.bucket,
                        password=target.password,
                        num_items=workload_settings.items,
                        num_threads=workload_settings.workers,
                        num_cycles=workload_settings.iterations,
                        size=workload_settings.size,
                        writes=workload_settings.creates,
                        doc_gen=workload_settings.doc_gen,
                        populate=True,
                        use_ssl=workload_settings.use_ssl)


def pillowfight_workload(workload_settings: PhaseSettings,
                         target: TargetSettings,
                         *args):
    run_cbc_pillowfight(host=target.node,
                        bucket=target.bucket,
                        password=target.password,
                        num_items=workload_settings.items,
                        num_threads=workload_settings.workers,
                        num_cycles=workload_settings.iterations,
                        size=workload_settings.size,
                        writes=workload_settings.updates,
                        doc_gen=workload_settings.doc_gen,
                        use_ssl=workload_settings.use_ssl)

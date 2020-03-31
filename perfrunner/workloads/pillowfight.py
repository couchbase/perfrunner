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
                        batch_size=workload_settings.batch_size,
                        writes=workload_settings.creates,
                        persist_to=workload_settings.persist_to,
                        replicate_to=workload_settings.replicate_to,
                        connstr_params=workload_settings.connstr_params,
                        durability=workload_settings.durability,
                        doc_gen=workload_settings.doc_gen,
                        ssl_mode=workload_settings.ssl_mode,
                        populate=True,
                        collections=workload_settings.collections,
                        custom_pillowfight=workload_settings.custom_pillowfight)


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
                        batch_size=workload_settings.batch_size,
                        writes=workload_settings.updates,
                        persist_to=workload_settings.persist_to,
                        replicate_to=workload_settings.replicate_to,
                        durability=workload_settings.durability,
                        connstr_params=workload_settings.connstr_params,
                        doc_gen=workload_settings.doc_gen,
                        ssl_mode=workload_settings.ssl_mode,
                        collections=workload_settings.collections,
                        custom_pillowfight=workload_settings.custom_pillowfight)

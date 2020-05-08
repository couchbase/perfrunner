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
             ycsb_client=workload_settings.ycsb_client,
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers,
             target=int(workload_settings.target),
             soe_params=soe_params,
             instance=instance,
             epoll=workload_settings.epoll,
             boost=workload_settings.boost,
             persist_to=workload_settings.persist_to,
             replicate_to=workload_settings.replicate_to,
             fieldlength=workload_settings.field_length,
             fieldcount=workload_settings.field_count,
             durability=workload_settings.durability,
             kv_endpoints=workload_settings.kv_endpoints,
             enable_mutation_token=workload_settings.enable_mutation_token,
             transactionsenabled=workload_settings.transactionsenabled,
             documentsintransaction=workload_settings.documentsintransaction,
             transactionreadproportion=workload_settings.transactionreadproportion,
             transactionupdateproportion=workload_settings.transactionupdateproportion,
             transactioninsertproportion=workload_settings.transactioninsertproportion,
             requestdistribution=workload_settings.requestdistribution,
             num_atrs=workload_settings.num_atrs,
             ycsb_jvm_args=workload_settings.ycsb_jvm_args,
             collections_map=workload_settings.collections)


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
             ycsb_client=workload_settings.ycsb_client,
             workload=workload_settings.workload_path,
             items=workload_settings.items,
             workers=workload_settings.workers,
             target=int(workload_settings.target),
             soe_params=soe_params,
             ops=int(workload_settings.ops),
             instance=instance,
             epoll=workload_settings.epoll,
             boost=workload_settings.boost,
             persist_to=workload_settings.persist_to,
             replicate_to=workload_settings.replicate_to,
             execution_time=workload_settings.time,
             ssl_keystore_file=workload_settings.ssl_keystore_file,
             ssl_keystore_password=workload_settings.ssl_keystore_password,
             ssl_mode=workload_settings.ssl_mode,
             timeseries=workload_settings.timeseries,
             cbcollect=workload_settings.cbcollect,
             fieldlength=workload_settings.field_length,
             fieldcount=workload_settings.field_count,
             durability=workload_settings.durability,
             kv_endpoints=workload_settings.kv_endpoints,
             enable_mutation_token=workload_settings.enable_mutation_token,
             retry_strategy=workload_settings.retry_strategy,
             retry_lower=workload_settings.retry_lower,
             retry_upper=workload_settings.retry_upper,
             retry_factor=workload_settings.retry_factor,
             transactionsenabled=workload_settings.transactionsenabled,
             documentsintransaction=workload_settings.documentsintransaction,
             transactionreadproportion=workload_settings.transactionreadproportion,
             transactionupdateproportion=workload_settings.transactionupdateproportion,
             transactioninsertproportion=workload_settings.transactioninsertproportion,
             requestdistribution=workload_settings.requestdistribution,
             num_atrs=workload_settings.num_atrs,
             ycsb_jvm_args=workload_settings.ycsb_jvm_args,
             collections_map=workload_settings.collections)

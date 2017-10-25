from perfrunner.helpers.local import run_bigfun
from perfrunner.settings import PhaseSettings, TargetSettings


def cbas_bigfun_data_mixload(workload_settings: PhaseSettings,
                             target: TargetSettings,
                             timer: int,
                             instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='mix',
               time=workload_settings.time,
               interval=workload_settings.bigfun_mix_interval,
               gudocnum=workload_settings.items,
               instance=instance,
               workers=workload_settings.workers,
               inserts=workload_settings.bigfun_mix_inserts,
               updates=workload_settings.bigfun_mix_updates,
               deletes=workload_settings.bigfun_mix_deletes)


def cbas_bigfun_data_ttl(workload_settings: PhaseSettings,
                         target: TargetSettings,
                         timer: int,
                         instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='ttl',
               instance=instance)


def cbas_bigfun_data_delete(workload_settings: PhaseSettings,
                            target: TargetSettings,
                            timer: int,
                            instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='delete',
               instance=instance)


def cbas_bigfun_data_insert(workload_settings: PhaseSettings,
                            target: TargetSettings,
                            timer: int,
                            instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='insert',
               instance=instance)


def cbas_bigfun_data_query(workload_settings: PhaseSettings,
                           target: TargetSettings,
                           timer: int,
                           instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='query',
               instance=instance,
               workers=workload_settings.bigfun_query_workers)


def cbas_bigfun_data_update_index(workload_settings: PhaseSettings,
                                  target: TargetSettings,
                                  timer: int,
                                  instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='update_index',
               instance=instance)


def cbas_bigfun_data_update_non_index(workload_settings: PhaseSettings,
                                      target: TargetSettings,
                                      timer: int,
                                      instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='update_non_index',
               instance=instance)


def cbas_bigfun_wait(workload_settings: PhaseSettings,
                     target: TargetSettings,
                     timer: int,
                     instance: int):
    run_bigfun(host=target.node,
               bucket=target.bucket,
               password=target.password,
               action='wait',
               time=workload_settings.time,
               instance=instance)

from perfrunner.helpers.local import run_java_dcp_client
from perfrunner.settings import PhaseSettings, TargetSettings


def java_dcp_client(workload_settings: PhaseSettings,
                    target: TargetSettings,
                    timer: int,
                    instance: int):

    connection_string = target.connection_string
    messages = workload_settings.items
    config_file = workload_settings.java_dcp_config
    collections = None

    if workload_settings.collections is not None:
        access_targets = []
        num_load_targets = 0
        target_scope_collections = workload_settings.collections[target.bucket]
        for scope in target_scope_collections.keys():
            for collection in target_scope_collections[scope].keys():
                if target_scope_collections[scope][collection]['load'] == 1:
                    num_load_targets += 1
                    if target_scope_collections[scope][collection]['access'] == 1:
                        access_targets += [scope+":"+collection]

        if workload_settings.java_dcp_stream == "all":
            messages = (workload_settings.items // num_load_targets) * len(access_targets)
            collections = access_targets

        elif workload_settings.java_dcp_stream == "even_split":
            split_access_targets = [access_targets[seq_id]
                                    for seq_id
                                    in range(instance,
                                             len(access_targets),
                                             workload_settings.java_dcp_clients)]
            messages = (workload_settings.items // num_load_targets) * len(split_access_targets)
            collections = split_access_targets

    run_java_dcp_client(connection_string=connection_string,
                        messages=messages,
                        config_file=config_file,
                        instance=instance,
                        collections=collections)

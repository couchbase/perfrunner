import json
from typing import Callable
from uuid import uuid4

from logger import logger
from perfrunner.helpers.local import run_custom_cmd
from perfrunner.helpers.misc import get_max_arg_strlen, read_json
from perfrunner.settings import PhaseSettings, TargetSettings, TestConfig

PARAMS = (
    " -test_duration {test_duration}"
    " -test_total_docs {test_total_docs}"
    " -test_query_workers {test_query_workers}"
    " -test_kv_workers {test_kv_workers}"
    " -test_kv_throughput_goal {test_kv_throughput_goal}"
    " -test_data_file {test_data_file}"
    " -test_driver {test_driver}"
    " -test_stats_limit {test_stats_limit}"
    " -test_stats_aggregation_step {test_stats_aggregation_step}"
    " -test_debug {test_debug}"
    " -test_query_type {test_query_type} "
    " -test_query_limit {test_query_limit}"
    " -test_query_field '{test_query_field}'"
    " -test_worker_type {test_worker_type}"
    " -test_geo_polygon_coord_list {test_geo_polygon_coord_list}"
    " -test_query_lon_width {test_query_lon_width}"
    " -test_query_lat_height {test_query_lat_height}"
    " -test_geo_distance {test_geo_distance}"
    " -test_flex {test_flex}"
    " -test_flex_query_type {test_flex_query_type}"
    " -couchbase_index_name {couchbase_index_name}"
    " -couchbase_cluster_ip {couchbase_cluster_ip}"
    " -couchbase_bucket {couchbase_bucket}"
    " -couchbase_user {couchbase_user}"
    " -couchbase_password '{couchbase_password}'"
)


def create_index_list_by_bucket(settings: PhaseSettings, bucket: str) -> list[str]:
    index_list = []
    for index_name in settings.fts_index_map.keys():
        if bucket == settings.fts_index_map[index_name]["bucket"]:
            index_list.append(index_name)
    return index_list


def update_settings_for_mixed_queries(settings: PhaseSettings, bucket: str) -> PhaseSettings:
    for key in settings.mixed_query_map[bucket].keys():
        setattr(settings, key, settings.mixed_query_map[bucket][key])
    return settings


def update_settings_for_multi_queries(
    settings: PhaseSettings, test_config: TestConfig
) -> PhaseSettings:
    for config in test_config.keys():
        setattr(settings, config, test_config[config])
    return settings


def _execute_jts(
    settings: PhaseSettings,
    target: TargetSettings,
    full_index_name: str,
    index_map: dict,
    test_duration: int,
    test_query_workers: int,
    test_kv_workers: int,
    test_kv_throughput_goal: int,
    test_worker_type: str,
):
    host = target.node
    if target.cloud:
        host = target.cloud.get("cluster_svc", host)

    params = PARAMS.format(
        couchbase_index_name=full_index_name,
        couchbase_cluster_ip=host,
        couchbase_bucket=target.bucket,
        couchbase_user=target.username,
        couchbase_password=target.password,
        test_duration=test_duration,
        test_total_docs=settings.test_total_docs,
        test_query_workers=test_query_workers,
        test_kv_workers=test_kv_workers,
        test_kv_throughput_goal=test_kv_throughput_goal,
        test_data_file=settings.test_data_file,
        test_driver=settings.test_driver,
        test_stats_limit=settings.test_stats_limit,
        test_stats_aggregation_step=settings.test_stats_aggregation_step,
        test_debug=settings.test_debug,
        test_query_type=settings.test_query_type,
        test_query_limit=settings.test_query_limit,
        test_query_field=settings.test_query_field,
        test_worker_type=test_worker_type,
        test_geo_polygon_coord_list=settings.test_geo_polygon_coord_list,
        test_query_lon_width=settings.test_query_lon_width,
        test_query_lat_height=settings.test_query_lat_height,
        test_geo_distance=settings.test_geo_distance,
        test_flex=settings.test_flex,
        test_flex_query_type=settings.test_flex_query_type,
    )
    if settings.collections_enabled:
        params += (
            f" -test_collections_enabled {settings.collections_enabled}"
            f" -test_collection_query_mode {settings.test_collection_query_mode}"
            f" -test_collection_specific_count {settings.test_collection_specific_count}"
        )
        if len(index_map_str := json.dumps(index_map)) > get_max_arg_strlen():
            logger.info(
                "Index map too large to provide via CLI. "
                "Copying into file and providing file path instead."
            )
            filename = f"index_map_{uuid4().hex[:6]}.json"
            with open(f"{settings.jts_home_dir}/{filename}", "w") as f:
                f.write(index_map_str)
            params += f" -test_fts_index_map_file ./{filename}"
        else:
            params += f" -test_fts_index_map '{index_map_str}'"
    if settings.capella_infrastructure:
        params += " -couchbase_ssl_mode capella"
    if settings.test_mutation_field and settings.test_mutation_field != 'None':
        params += f" -test_mutation_field {settings.test_mutation_field}"
    if settings.search_query_timeout_in_sec:
        params += f" -search_query_timeout_in_sec {settings.search_query_timeout_in_sec}"
    if settings.test_geojson_query_type:
        params += f" -test_geojson_query_type {settings.test_geojson_query_type}"
    if settings.test_geojson_query_relation:
        params += f" -test_geojson_query_relation {settings.test_geojson_query_relation}"
    if str(settings.aggregation_buffer_ms) != "1000":
        params += f" -aggregation_buffer_ms {settings.aggregation_buffer_ms}"
    if settings.fts_raw_query_map:
        params += (
            f" -k_nearest_neighbour {settings.k_nearest_neighbour}"
            f" -fts_raw_query_map '{json.dumps(settings.fts_raw_query_map)}'"
        )
    if settings.test_query_field2:
        params += f" -test_query_field2 {settings.test_query_field2}"
    print(params)
    run_custom_cmd(settings.jts_home_dir, settings.jts_run_cmd, params)


def execute_jts_run(
    settings: PhaseSettings, target: TargetSettings, full_index_name: str, index_map: dict
):
    _execute_jts(
        settings,
        target,
        full_index_name,
        index_map,
        test_duration=settings.time,
        test_query_workers=settings.test_query_workers,
        test_kv_workers=settings.test_kv_workers,
        test_kv_throughput_goal=settings.test_kv_throughput_goal,
        test_worker_type=settings.test_worker_type,
    )


def execute_jts_warmup(
    settings: PhaseSettings, target: TargetSettings, full_index_name: str, index_map: dict
):
    _execute_jts(
        settings,
        target,
        full_index_name,
        index_map,
        test_duration=settings.warmup_time,
        test_query_workers=settings.warmup_query_workers,
        test_kv_workers="0",
        test_kv_throughput_goal="0",
        test_worker_type="warmup",
    )


def _jts(
    execute_jts_func: Callable,
    workload_settings: PhaseSettings,
    target: TargetSettings,
):
    settings = workload_settings
    index_list = create_index_list_by_bucket(settings, target.bucket)
    for index_name in index_list:
        full_index_name = (
            settings.fts_index_map[index_name]["full_index_name"]
            if "full_index_name" in settings.fts_index_map[index_name].keys()
            else index_name
        )
        index_map = {full_index_name: settings.fts_index_map[index_name]}

        if settings.test_query_mode == "mixed":
            settings = update_settings_for_mixed_queries(settings, target.bucket)

        if settings.test_query_mode == "multi":
            multi_query_map = read_json(settings.couchbase_index_configmap)
            for test_config in multi_query_map["queries"]:
                settings = update_settings_for_multi_queries(settings, test_config)
                execute_jts_func(settings, target, full_index_name, index_map)
        else:
            execute_jts_func(settings, target, full_index_name, index_map)


def jts_run(workload_settings: PhaseSettings, target: TargetSettings, timer: int, worker_id: int):
    _jts(execute_jts_run, workload_settings, target)


def jts_warmup(
    workload_settings: PhaseSettings, target: TargetSettings, timer: int, worker_id: int
):
    _jts(execute_jts_warmup, workload_settings, target)

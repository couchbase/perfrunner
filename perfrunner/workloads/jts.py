import json

from perfrunner.helpers.local import run_custom_cmd
from perfrunner.settings import PhaseSettings, TargetSettings

CMD = " -test_duration {test_duration}" \
      " -test_total_docs {test_total_docs}" \
      " -test_query_workers {test_query_workers}" \
      " -test_kv_workers {test_kv_workers}" \
      " -test_kv_throughput_goal {test_kv_throughput_goal}" \
      " -test_data_file {test_data_file}" \
      " -test_driver {test_driver}" \
      " -test_stats_limit {test_stats_limit}" \
      " -test_stats_aggregation_step {test_stats_aggregation_step}" \
      " -test_debug {test_debug}" \
      " -test_query_type {test_query_type} " \
      " -test_query_limit {test_query_limit}" \
      " -test_query_field {test_query_field}" \
      " -test_mutation_field {test_mutation_field}" \
      " -test_worker_type {test_worker_type}" \
      " -test_geo_polygon_coord_list {test_geo_polygon_coord_list}"\
      " -test_query_lon_width {test_query_lon_width}"\
      " -test_query_lat_height {test_query_lat_height}"\
      " -test_geo_distance {test_geo_distance}"\
      " -test_flex {test_flex}"\
      " -test_flex_query_type {test_flex_query_type}"\
      " -couchbase_index_name {couchbase_index_name}" \
      " -couchbase_cluster_ip {couchbase_cluster_ip}" \
      " -couchbase_bucket {couchbase_bucket}" \
      " -couchbase_user {couchbase_user}" \
      " -couchbase_password {couchbase_password}"


def jts_run(workload_settings: PhaseSettings, target: TargetSettings,
            timer: int, worker_id: int):
    settings = workload_settings
    index_name = "{}-0".format(settings.couchbase_index_name)
    if "full_index_name" in list(settings.fts_index_map[index_name].keys()):
        full_index_name = settings.fts_index_map[index_name]["full_index_name"]
        settings.fts_index_map[full_index_name] = settings.fts_index_map.pop(index_name)
        index_name = full_index_name

    params = CMD.format(
        couchbase_index_name=index_name,
        couchbase_cluster_ip=target.node,
        couchbase_bucket=target.bucket,
        couchbase_user=target.bucket,
        couchbase_password=target.password,
        test_duration=settings.time,
        test_total_docs=settings.test_total_docs,
        test_query_workers=settings.test_query_workers,
        test_kv_workers=settings.test_kv_workers,
        test_kv_throughput_goal=settings.test_kv_throughput_goal,
        test_data_file=settings.test_data_file,
        test_driver=settings.test_driver,
        test_stats_limit=settings.test_stats_limit,
        test_stats_aggregation_step=settings.test_stats_aggregation_step,
        test_debug=settings.test_debug,
        test_query_type=settings.test_query_type,
        test_query_limit=settings.test_query_limit,
        test_query_field=settings.test_query_field,
        test_mutation_field=settings.test_mutation_field,
        test_worker_type=settings.test_worker_type,
        test_geo_polygon_coord_list=settings.test_geo_polygon_coord_list,
        test_query_lon_width=settings.test_query_lon_width,
        test_query_lat_height=settings.test_query_lat_height,
        test_geo_distance=settings.test_geo_distance,
        test_flex=settings.test_flex,
        test_flex_query_type=settings.test_flex_query_type
    )

    if settings.collections_enabled:
        params += " -test_collections_enabled {test_collections_enabled}"\
            .format(test_collections_enabled=settings.collections_enabled)
        params += " -test_collection_query_mode {test_collection_query_mode}" \
            .format(test_collection_query_mode=settings.test_collection_query_mode)
        params += " -test_collection_specific_count {test_collection_specific_count}" \
            .format(test_collection_specific_count=settings.test_collection_specific_count)
        params += " -test_fts_index_map \'{test_fts_index_map}\'"\
            .format(test_fts_index_map=json.dumps(settings.fts_index_map))
    print(params)
    run_custom_cmd(settings.jts_home_dir, settings.jts_run_cmd, params)


def jts_warmup(workload_settings: PhaseSettings, target: TargetSettings,
               timer: int, worker_id: int):
    settings = workload_settings
    index_name = "{}-0".format(settings.couchbase_index_name)
    if "full_index_name" in list(settings.fts_index_map[index_name].keys()):
        full_index_name = settings.fts_index_map[index_name]["full_index_name"]
        settings.fts_index_map[full_index_name] = settings.fts_index_map.pop(index_name)
        index_name = full_index_name
    params = CMD.format(
        couchbase_index_name=index_name,
        couchbase_cluster_ip=target.node,
        couchbase_bucket=target.bucket,
        couchbase_user=target.bucket,
        couchbase_password=target.password,
        test_duration=settings.warmup_time,
        test_total_docs=settings.test_total_docs,
        test_query_workers=settings.warmup_query_workers,
        test_kv_workers="0",
        test_kv_throughput_goal="0",
        test_data_file=settings.test_data_file,
        test_driver=settings.test_driver,
        test_stats_limit=settings.test_stats_limit,
        test_stats_aggregation_step=settings.test_stats_aggregation_step,
        test_debug=settings.test_debug,
        test_query_type=settings.test_query_type,
        test_query_limit=settings.test_query_limit,
        test_query_field=settings.test_query_field,
        test_mutation_field=settings.test_mutation_field,
        test_worker_type="warmup",
        test_geo_polygon_coord_list=settings.test_geo_polygon_coord_list,
        test_query_lon_width=settings.test_query_lon_width,
        test_query_lat_height=settings.test_query_lat_height,
        test_geo_distance=settings.test_geo_distance,
        test_flex=settings.test_flex,
        test_flex_query_type=settings.test_flex_query_type
    )

    if settings.collections_enabled:
        params += " -test_collections_enabled {test_collections_enabled}" \
            .format(test_collections_enabled=settings.collections_enabled)
        params += " -test_collection_query_mode {test_collection_query_mode}" \
            .format(test_collection_query_mode=settings.test_collection_query_mode)
        params += " -test_collection_specific_count {test_collection_specific_count}" \
            .format(test_collection_specific_count=settings.test_collection_specific_count)
        params += " -test_fts_index_map \'{test_fts_index_map}\'" \
            .format(test_fts_index_map=json.dumps(settings.fts_index_map))
    print(params)
    run_custom_cmd(settings.jts_home_dir, settings.jts_run_cmd, params)

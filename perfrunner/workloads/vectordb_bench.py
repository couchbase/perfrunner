import json

from logger import logger
from perfrunner.helpers.local import run_vectordb_bench
from perfrunner.settings import TargetSettings, VectorDBBenchSettings


def run_vectordb_bench_case(
    workload_settings: VectorDBBenchSettings, target: TargetSettings, *args
):
    options = [
        "--db-config",
        "'{}'".format(
            json.dumps(get_db_config_for(workload_settings.database, target, workload_settings))
        ),
        "--database",
        workload_settings.database,
        "--cases",
        workload_settings.cases,
        "--label",
        workload_settings.label,
    ]
    run_vectordb_bench(
        " ".join(options), workload_settings.dataset_local_path, workload_settings.use_shuffled_data
    )


def get_db_config_for(
    database: str, target: TargetSettings, workload_settings: VectorDBBenchSettings
) -> dict:
    """Provide a client specific configuration in json format.

    Each client has its own configuration it expects to run properly. Such properties are formatted
    here to match the clients naming. If there is no implementation for the database, an empty json
    will be sent and the client defaults will be used.
    """
    if database == "Couchbase":
        return {
            "host": target.node,
            "bucket": target.bucket,
            "username": target.username,
            "password": target.password,
            "ssl_mode": workload_settings.ssl_mode,
        }
    else:
        logger.warn(f"No config for '{database}' client. Defaults will be used")
        return {}

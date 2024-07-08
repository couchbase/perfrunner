import glob
import os
import re
import shutil
import time
import zipfile
from argparse import ArgumentParser
from collections import defaultdict
from collections.abc import Callable
from functools import cached_property
from multiprocessing import set_start_method
from pathlib import Path

import requests

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.misc import pretty_dict, run_local_shell_command
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper, RestType
from perfrunner.remote.linux import RemoteLinux
from perfrunner.settings import ClusterSpec

set_start_method("fork")

GOLANG_LOG_FILES = ("eventing.log",
                    "fts.log",
                    "goxdcr.log",
                    "indexer.log",
                    "projector.log",
                    "query.log")


class LogsVerifier:

    def check_file_for(self, filename: str, predicate_func: Callable,
                       processor_func: Callable) -> list[str]:
        """Check files contained in the zipfile for conditions provided by the functions.

        predicate_func: Condition to match filenames.
        processor_func: Condition to match file contents.
        Return filenames passing both condition functions.
        """
        zf = zipfile.ZipFile(filename)
        error_files = []
        for name in zf.namelist():
            if predicate_func(name) and processor_func(zf.read(name)):
                error_files.append(name)
        return error_files

    def check_for_golang_panic(self, filename: str) -> list[str]:
        return self.check_file_for(filename,
                                   lambda name:
                                   any(log_file in name for log_file in GOLANG_LOG_FILES),
                                   lambda data: ('panic' in str(data)))

    def check_for_crash_files(self, filename: str) -> list[str]:
        return self.check_file_for(filename,
                                   lambda name: name.endswith('.dmp'),
                                   lambda _: True)

    def check_for_storage_corrupted(self, filename: str) -> list[str]:
        return self.check_file_for(filename,
                                   lambda name: 'indexer.log' in name,
                                   lambda data: 'Storage corrupted and unrecoverable' in str(data))

    def process_logs(self, is_capella: bool, remote: RemoteLinux):
        failures = defaultdict(dict)
        for filename in glob.iglob('./*.zip'):
            if panic_files := self.check_for_golang_panic(filename):
                failures['panics'][filename] = panic_files
            if crash_files := self.check_for_crash_files(filename):
                failures['crashes'][filename] = crash_files
            if storage_corrupted_files := self.check_for_storage_corrupted(filename):
                failures['storage_corrupted'][filename] = storage_corrupted_files
                if not is_capella:
                    remote.collect_index_datafiles()

        if failures:
            logger.interrupt(f"Following failures found: {pretty_dict(failures)}")


class LokiLogsProcessor(LogsVerifier):
    # <logger>:<level>,<datetime>,<user@address>:<component><line>:<error_title>:<line>]<error_body>
    # Keep the following groups: logger, title, error_body
    ERROR_LOG_RE = r"\[(\w+):\w+,[^,]+,[^:]+@[^:]+:[^:]+<[^>]*>:([^:]+(?::[^:]+)*?):\d+\](.*)"

    LOKI_PUSH_API = 'http://172.23.123.237/loki/loki/api/v1/push'

    def __init__(self, version: str):
        super().__init__()
        self.logs: list[ErrorEvent] = []
        self.version = version.split("-")[0].strip()

    @cached_property
    def _job_name(self) -> str:
        return os.environ.get('BUILD_TAG', 'local')

    def process_logs(self, is_capella: bool, remote):
        for filename in glob.iglob('./*.zip'):
            try:
                self.check_file_for(filename,
                                    lambda name: 'error.log' in name,
                                    self.collect_errors)
                self.store_logs(filename, is_capella)
                self.logs.clear()
            except Exception as e:
                logger.warn(e)

    def collect_errors(self, data: bytes):
        for error in  re.findall(self.ERROR_LOG_RE, data.decode()):
            self.logs.append(ErrorEvent(error))

    def store_logs(self, filename: str, is_capella: bool):
        # For each error, convert to loki with extra parameters
        # source: ns_server | datadog | others
        # job: jenkins-job-number | local
        # cb_version: 7.1.2
        # level: error
        # capella: True | False
        # node: node name/ip

        node = filename.replace("./", "").replace(".zip", "")
        if not self.logs:
            logger.info(f"No error logs found for node {node}")
            return

        data = {
            "streams": [
                {
                    "stream": {
                        "source": "ns_server",
                        "job": self._job_name,
                        "cb_version": self.version,
                        "level": "error",
                        "capella": str(is_capella).lower(),
                        "node": node,
                    },
                    "values": [log.get_values() for log in self.logs],
                }
            ]
        }

        logger.info(f"Sending {len(self.logs)} error logs to Loki for node {node}")
        try:
            resp = requests.post(url=self.LOKI_PUSH_API, json=data)
            resp.raise_for_status()
        except Exception as e:
            logger.warn(f"{e}. {data}")


class ErrorEvent:
    """Encapsulates an individual error line which will be pushed to Loki."""

    def __init__(self, error_data: tuple[str, ...]):
        # This will change with `ERROR_LOG_RE`. If the regex is updated,
        # then this should be updated too to get new groups
        self.logger, self.title, self.msg, *_ = error_data
        self.timestamp = time.time_ns()

    def get_values(self) -> list:
        return [
            f"{self.timestamp}",
            f"{self.title} - {self.msg}",
            {"logger": self.logger, "error": self.title},
        ]

    def __str__(self) -> str:
        return self.title


def get_args():
    parser = ArgumentParser()

    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster_spec_fname",
        required=True,
        help="path to the cluster specification file",
    )
    parser.add_argument(
        "-b",
        "--s3-bucket",
        dest="s3_bucket_name",
        required=False,
        help="name of the s3 bucket to download Capella cluster logs from",
    )
    return parser.parse_args()


def create_s3_bucket_file_key(log_path: str, log_url: str) -> str:
    date = re.search(r"\d{4}-\d{2}-\d{2}", log_url)
    url_split = log_url.split('/')
    org = re.sub(r'\+', '-', url_split[3].lower())
    log_name = re.split('/', log_path)[-1]
    file_key = f"{org}/{date.group(0)}/{log_name.lower()}"
    return file_key


def create_bucket_hostname(node_name: str) -> str:
    node_name = node_name.split('@')[1].split('.')
    hostname = f"{node_name[0]}.{node_name[1]}"
    return hostname


def check_if_log_file_exists(path_name_pattern: str):
    path_name_pattern = path_name_pattern.removesuffix(".zip")
    file_key_pattern = path_name_pattern.split('/')[-1]
    bucket_key = path_name_pattern.removesuffix(file_key_pattern)
    cmd = f"aws s3 ls "\
        f"{bucket_key} --recursive | grep "\
        f"{file_key_pattern}"
    retries = 3
    while retries > 0:
        stdout, _, returncode = run_local_shell_command(cmd)
        if returncode == 0:
            path_name = stdout.split()[-1]
            return path_name
        retries -= 1
        time.sleep(60)
    logger.interrupt(f"Log file not found due to the following error: {stdout}")


def get_capella_cluster_logs(rest: RestType, s3_bucket_name: str):
    if not (upload_host := os.environ.get("CBCOLLECT_UPLOAD_HOST")):
        logger.error("CBCOLLECT_UPLOAD_HOST needs to be set for collecting Capella cluster logs.")
        return

    for master in rest.cluster_spec.masters:
        rest.start_log_collection(master, upload_host=f"https://{upload_host}/")

    timeout_secs = 600
    interval_secs = 30
    clusters_remaining = list(zip(rest.cluster_spec.capella_cluster_ids, rest.cluster_spec.masters))
    successful_tasks = []

    deadline = time.time() + timeout_secs
    while clusters_remaining and time.time() < deadline:
        time.sleep(interval_secs)

        not_done = []
        for i, (cluster_id, master) in enumerate(clusters_remaining):
            task = next(
                (t for t in rest.get_tasks(master) if t["type"] == "clusterLogsCollection"), None
            )
            if not task:
                logger.error(f"Log collection task not found for cluster {cluster_id}")
            elif (status := task["status"]) == "completed":
                logger.info(f"Log collection finished for cluster {cluster_id}")
                successful_tasks.append(task)
            elif status not in ["running", "pending"]:
                logger.error(f"Unexpected log collection status for cluster {cluster_id}: {status}")
            else:
                logger.info(f"Log collection progress for cluster {cluster_id}: {task['progress']}")
                not_done.append(i)

        clusters_remaining = [c for i, c in enumerate(clusters_remaining) if i in not_done]

    if clusters_remaining:
        logger.interrupt(f"Timed out after {timeout_secs}s waiting for log collection to finish")

    for task in successful_tasks:
        for node_name, log_info in task["perNode"].items():
            path = log_info["path"]
            url = log_info["url"]

            file_key = create_s3_bucket_file_key(path, url)
            path_name_pattern = f"s3://{s3_bucket_name}/{file_key}"
            file_key = check_if_log_file_exists(path_name_pattern)
            path_name = f"s3://{s3_bucket_name}/{file_key}"

            hostname = create_bucket_hostname(node_name)
            if re.search(hostname, url) is not None:
                file_name = f"{hostname}.zip"
                local.download_all_s3_logs(path_name, file_name)


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname)

    remote = RemoteHelper(cluster_spec, verbose=False)
    # In any case, TLS ports are available. We use this for compatibility with tests
    # that may have configured strict N2N encryption.
    rest = RestHelper(cluster_spec, use_tls=True)

    # Collect and upload logs
    if cluster_spec.serverless_infrastructure:
        remote.collect_dn_logs()
        remote.collect_dapi_logs()

        dapi_logs = [
            fname for iid in cluster_spec.dapi_instance_ids for fname in glob.glob(f"{iid}/*.log")
        ]

        dn_logs = [
            fname
            for iid in cluster_spec.direct_nebula_instance_ids
            for fname in glob.glob(f"{iid}/*.log")
        ]

        for zip_name, log_fnames in zip(['dapi', 'direct_nebula'], [dapi_logs, dn_logs]):
            with zipfile.ZipFile(f"{zip_name}.zip", "w", compression=zipfile.ZIP_DEFLATED) as z:
                for fname in log_fnames:
                    z.write(fname, arcname=Path(fname).name)

    if cluster_spec.capella_infrastructure:
        get_capella_cluster_logs(rest, args.s3_bucket_name)
    elif cluster_spec.dynamic_infrastructure:
        remote.collect_k8s_logs()
        local.collect_cbopinfo_logs(remote.kube_config_path)
    else:
        _, _, returncode = run_local_shell_command(f"ls {cluster_spec.servers[0]}.zip")
        if returncode:
            remote.collect_info()
            for hostname in cluster_spec.servers:
                for fname in glob.glob(f"{hostname}/*.zip"):
                    shutil.move(fname, f"{hostname}.zip")
        else:
            logger.info("Logs already collected. Skipping cbcollect..")

    if cluster_spec.backup is not None:
        logs = os.path.join(cluster_spec.backup, 'logs')
        if os.path.exists(logs):
            shutil.make_archive('tools', 'zip', logs)

    # Push log lines to Loki
    loki_manager = LokiLogsProcessor(rest.get_version(cluster_spec.servers[0]))
    loki_manager.process_logs(cluster_spec.capella_infrastructure, remote)
    # Process logs, throw exception if any
    analyser = LogsVerifier()
    analyser.process_logs(cluster_spec.capella_infrastructure, remote)


if __name__ == '__main__':
    main()

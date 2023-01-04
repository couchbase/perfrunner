import glob
import os
import re
import shutil
import zipfile
from argparse import ArgumentParser
from collections import defaultdict
from multiprocessing import set_start_method
from typing import List

from logger import logger
from perfrunner.helpers import local
from perfrunner.helpers.misc import pretty_dict, run_local_shell_command
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import RestHelper
from perfrunner.settings import ClusterSpec, TestConfig

set_start_method("fork")

GOLANG_LOG_FILES = ("eventing.log",
                    "fts.log",
                    "goxdcr.log",
                    "indexer.log",
                    "projector.log",
                    "query.log")


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-c', '--cluster', dest='cluster_spec_fname',
                        required=True,
                        help='path to the cluster specification file')
    parser.add_argument('-b', '--s3-bucket', dest='s3_bucket_name',
                        required=False,
                        help='name of the s3 bucket to download Capella cluster logs from')
    return parser.parse_args()


def check_for_golang_panic(file_name: str) -> List[str]:
    zf = zipfile.ZipFile(file_name)
    panic_files = []
    for name in zf.namelist():
        if any(log_file in name for log_file in GOLANG_LOG_FILES):
            data = zf.read(name)
            if "panic" in str(data):
                panic_files.append(name)
    return panic_files


def check_for_crash_files(file_name: str) -> List[str]:
    zf = zipfile.ZipFile(file_name)
    crash_files = []
    for name in zf.namelist():
        if name.endswith('.dmp'):
            crash_files.append(name)
    return crash_files


def check_for_storage_corrupted(file_name: str) -> List[str]:
    zf = zipfile.ZipFile(file_name)
    storage_corrupted = False
    for name in zf.namelist():
        if "indexer.log" in name:
            data = zf.read(name)
            if "Storage corrupted and unrecoverable" in str(data):
                storage_corrupted = True
    return storage_corrupted


def validate_logs(file_name: str):
    panic_files = check_for_golang_panic(file_name)
    crash_files = check_for_crash_files(file_name)
    storage_corrupted = check_for_storage_corrupted(file_name)
    return panic_files, crash_files, storage_corrupted


def create_s3_bucket_file_key(log_path: str, log_url: str):
    date = re.search(r"\d{4}-\d{2}-\d{2}", log_url)
    url_split = log_url.split('/')
    org = re.sub(r'\+', '-', url_split[3].lower())
    log_name = re.split('/', log_path)[-1]
    file_key = '{}/{}/{}'.format(org, date.group(0), log_name.lower())
    return file_key


def check_if_log_file_exists(bucket_name: str, file_key: str):
    cmd = 'aws s3api wait object-exists \
    --bucket {} \
    --key {}'.format(bucket_name, file_key)
    retries = 3
    while retries > 0:
        stdout, _, returncode = run_local_shell_command(cmd)
        if returncode == 0:
            return
    logger.interupt('Log file not found due to the following error: {}'.format(stdout))


def get_capella_cluster_logs(cluster_spec: ClusterSpec, s3_bucket_name: str):
    test_config = TestConfig()
    rest = RestHelper(cluster_spec, test_config)

    rest.trigger_all_cluster_log_collection()
    rest.wait_until_all_logs_uploaded()
    log_paths_urls = rest.get_all_cluster_log_paths_urls()

    for log_path, log_url in log_paths_urls:
        file_key = create_s3_bucket_file_key(log_path, log_url)
        path_name = 's3://{}/{}'.format(s3_bucket_name, file_key)
        check_if_log_file_exists(s3_bucket_name, file_key)
        for hostname in cluster_spec.servers:
            if re.search(hostname, log_url) is not None:
                file_name = '{}.zip'.format(hostname)

        local.download_all_s3_logs(path_name, file_name)


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname)
    if cluster_spec.capella_infrastructure:
        get_capella_cluster_logs(cluster_spec, args.s3_bucket_name)
    else:
        remote = RemoteHelper(cluster_spec, verbose=False)
        remote.collect_info()
        for hostname in cluster_spec.servers:
            for fname in glob.glob('{}/*.zip'.format(hostname)):
                shutil.move(fname, '{}.zip'.format(hostname))

    if cluster_spec.backup is not None:
        logs = os.path.join(cluster_spec.backup, 'logs')
        if os.path.exists(logs):
            shutil.make_archive('tools', 'zip', logs)

    failures = defaultdict(dict)

    for file_name in glob.iglob('./*.zip'):
        panic_files, crash_files, storage_corrupted = validate_logs(file_name)
        if panic_files:
            failures['panics'][file_name] = panic_files
        if crash_files:
            failures['crashes'][file_name] = crash_files
        if storage_corrupted:
            failures['storage_corrupted'][file_name] = True
            if not cluster_spec.capella_infrastructure:
                remote.collect_index_datafiles()

    if failures:
        logger.interrupt(
            "Following failures found: {}".format(pretty_dict(failures)))


if __name__ == '__main__':
    main()

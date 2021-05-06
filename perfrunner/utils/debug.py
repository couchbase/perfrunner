import glob
import os
import shutil
import zipfile
from argparse import ArgumentParser
from collections import defaultdict
from typing import List

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec

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


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(args.cluster_spec_fname)

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
            remote.collect_index_datafiles()

    if failures:
        logger.interrupt(
            "Following failures found: {}".format(pretty_dict(failures)))


if __name__ == '__main__':
    main()

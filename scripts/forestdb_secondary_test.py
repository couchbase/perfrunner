import argparse
import json
import os
import re
import time
from urllib.parse import urljoin
from urllib.request import urlopen

import requests
import xmltodict
from fabric.api import env, execute, run, shell_env
from fabric.context_managers import cd

from logger import logger
from perfrunner.helpers.misc import pretty_dict, uhex
from perfrunner.settings import StatsSettings

args = None
prog_name = "forestdb_standalone_test"


def iter_urls():
    search_bases = [
        'http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock/{edition}/',
        'http://172.23.120.24/builds/latestbuilds/couchbase-server/watson/{edition}/',
        'http://172.23.120.24/builds/latestbuilds/couchbase-server/spock/{edition}/',
    ]

    patterns = [
        'couchbase-server-{release}-{edition}-manifest.xml'
    ]

    for base in search_bases:
        for pat in patterns:
            url = urljoin(
                base.format(**args.__dict__), pat.format(**args.__dict__))
            yield url


def find_manifest():
    for url in iter_urls():
        try:
            logger.debug("Trying {}".format(url))
            status_code = requests.head(url).status_code
        except requests.ConnectionError:
            continue
        else:
            if status_code == 200:
                logger.info('Found "{}"'.format(url))
                return url
    logger.interrupt("Cannot find the manifest for given version")


def fetch_url(xml_url):
    file = urlopen(xml_url)
    data = file.read()
    file.close()
    xml_data = xmltodict.parse(data)
    return xml_data


def hash_from_xml(xml_data):
    found = list(filter(lambda tup: tup['@name'] == 'forestdb',
                        xml_data['manifest']['project']))[0]
    logger.info("Found revision {}".format(found))
    branch = found.get('@upstream', 'master')
    fdb_hash = found['@revision']
    remote = found.get('@remote', 'couchbase')
    logger.info("Using remote {}, branch {}, hash {}".format(remote, branch, fdb_hash))
    return remote, branch, fdb_hash


def clone_repo(repo):
    cmd = "git clone {}".format(repo)
    logger.info("Running {}".format(cmd))
    run(cmd, pty=False)


def compile_forestdb(remote, branch, fdb_hash):
    with cd(args.remote_workdir):
        git_repo = "https://github.com/{}/forestdb.git".format(remote)
        clone_repo(git_repo)
        with cd("forestdb"):
            # if not master, move to that branch.
            if branch != "master":
                run("git checkout -b {0} origin/{0}".format(branch))
            run("git reset --hard {}".format(fdb_hash))
            logger.warn("Patching CMakeLists.txt to use c++0x instead of c++11")
            run("sed -i 's/c++11/c++0x/' CMakeLists.txt")
            run("mkdir build")
            with cd("build"):
                run("cmake ../")
                run("make all")
                run("ls *so")
    fdb_path = "{}/forestdb/build/libforestdb.so".format(args.remote_workdir)
    run("ls {}".format(fdb_path))
    return fdb_path


def compile_standalone_test():
    with cd(args.remote_workdir):
        clone_repo("https://github.com/couchbaselabs/forestdb-2ibenchmark.git")
        with cd("forestdb-2ibenchmark"):
            fdb_dir = "{}/forestdb".format(args.remote_workdir)
            cmd = "FDBDIR={} make".format(fdb_dir)
            run(cmd)
            run("mv {} ../".format(prog_name))
            if args.iniFile is not None:
                run("ln -sf {0} bench_config.ini".format(args.iniFile))
            run("cp bench_config.ini ../")


def post_benchmark(benchmark):
    if args.post_to_sf <= 0:
        logger.info("Dry run stats: {}\n".format(pretty_dict(benchmark)))
        return

    logger.info('Adding a benchmark: {}'.format(pretty_dict(benchmark)))
    requests.post('http://{}/api/v1/benchmarks'.format(StatsSettings.SHOWFAST),
                  json.dumps(benchmark))


def get_metric_id():
    ret_val = ""
    if args.iniFile is not None:
        ret_val = args.iniFile.replace("bench_config_2i_", "")
        ret_val = ret_val.replace(".ini", "")
    return ret_val


def post_incremental(incremental_time):
    metric_id = get_metric_id()
    data = {
        'build': args.version,
        'buildURL': os.environ.get('BUILD_URL'),
        'dateTime': time.strftime('%Y-%m-%d %H:%M'),
        'id': uhex(),
        'metric': 'secondary_fdb_standalone_{}_inc_nyx'.format(metric_id),
        'value': incremental_time,
    }
    post_benchmark(data)


def post_initial(initial_time):
    metric_id = get_metric_id()
    data = {
        'build': args.version,
        'buildURL': os.environ.get('BUILD_URL'),
        'dateTime': time.strftime('%Y-%m-%d %H:%M'),
        'id': uhex(),
        'metric': 'secondary_fdb_standalone_{}_ini_nyx'.format(metric_id),
        'value': initial_time,
    }
    post_benchmark(data)


def run_standalone_test():
    run("service couchbase-server stop", warn_only=True)
    with shell_env(LD_LIBRARY_PATH="{}/forestdb/build".format(args.remote_workdir)):
        with cd(args.remote_workdir):
            run("rm -rf data/")
            run("mkdir data")
            run("ldd ./{}".format(prog_name))
            run("./{}".format(prog_name))
            run("cat incrementalsecondary.txt")

            # Now for internal processing and posting to showfast
            output_text = run("cat incrementalsecondary.txt")
            groups = re.search(
                r"initial index build time[^\d]*(\d*).*?seconds",
                output_text)
            initial_time = int(groups.group(1))

            groups = re.search(
                r"incrmental index build time[^\d]*(\d*).*?seconds",
                output_text)
            incremental_time = int(groups.group(1))
            logger.info("Grepped initial build time {}".format(initial_time))
            logger.info("Grepped incremental build time {}".format(
                incremental_time))
            if initial_time:
                post_initial(initial_time)
            if incremental_time:
                post_incremental(incremental_time)


def cleanup_remote_workdir():
    run("mkdir -p {}".format(args.remote_workdir))
    run("rm   -rf {}".format(args.remote_workdir))
    run("mkdir -p {}".format(args.remote_workdir))


def main():
    xml_url = find_manifest() if not args.manifest_url else args.manifest_url
    xml_data = fetch_url(xml_url)
    remote, branch, fdb_hash = hash_from_xml(xml_data)

    if not args.run_only:
        execute(cleanup_remote_workdir)
        fdb_path = execute(compile_forestdb, remote, branch, fdb_hash)
        execute(compile_standalone_test, fdb_path)
    execute(run_standalone_test)


def get_args():
    global args
    parser = argparse.ArgumentParser(
        description='forestdb stand alone test'
        ' that intends to mimic secondary workload')

    parser.add_argument('--version', dest="version", required=True)
    parser.add_argument('--host', dest="host", required=True)
    parser.add_argument('--remote_workdir', dest="remote_workdir", default="/tmp/standalone_forestdb")
    parser.add_argument('--run-only', dest="run_only", action="store_true")
    parser.add_argument('--post-to-sf', dest="post_to_sf", type=int, default=0)
    parser.add_argument('--iniFile', dest="iniFile", default=None)
    parser.add_argument('--manifest_url', dest="manifest_url", default=None)

    args = parser.parse_args()

    (args.release, args.edition) = args.version.split('-')

    env.hosts = [args.host]
    env.user = "root"
    env.password = "couchbase"


if __name__ == "__main__":
    get_args()
    main()

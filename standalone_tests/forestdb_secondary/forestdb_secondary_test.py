import argparse
import fabric
import json
import logging
import pprint
import requests
import re
import urllib2
import urlparse
import xmltodict

from fabric.api import run, hosts, env, execute, shell_env
from fabric.context_managers import cd
from logger import logger
from uuid import uuid4
from couchbase import Couchbase


args = None
prog_name = "forestdb_standalone_test"


def iter_urls():
    SHERLOCK_BUILDS = (
        'http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock/{edition}/')

    WATSON_BUILDS = (
        'http://172.23.120.24/builds/latestbuilds/couchbase-server/watson/{edition}/')

    search_bases = [SHERLOCK_BUILDS, WATSON_BUILDS]

    patterns = [
        'couchbase-server-{release}-{edition}-manifest.xml'
    ]

    for base in search_bases:
        for pat in patterns:
            url = urlparse.urljoin(
                base.format(**(args.__dict__)), pat.format(**(args.__dict__)))
            yield url


def find_manifest():
    for url in iter_urls():
        try:
            logger.debug("Trying {}".format(url))
            status_code = requests.head(url).status_code
        except ConnectionError:
            continue
        else:
            if status_code == 200:
                logger.info('Found "{}"'.format(url))
                return url
    logger.interrupt("Cannot find the manifest for given version")


def fetch_url(xml_url):
    file = urllib2.urlopen(xml_url)
    data = file.read()
    file.close()
    xml_data = xmltodict.parse(data)
    return xml_data


def hash_from_xml(xml_data):
    found = filter(
        lambda tup: tup['@name'] == 'forestdb',
        xml_data['manifest']['project'])[0]
    logger.info("Found revision {}".format(found))
    branch = found.get('@upstream', 'master')
    fdb_hash = found['@revision']
    logger.info("Using branch {} hash {}".format(branch, fdb_hash))
    return branch, fdb_hash


def clone_repo(repo):
    CLONE_TIMEOUT = 60
    cmd = "git clone {}".format(repo)
    logger.info("Running {}".format(cmd))
    try:
        run(cmd, pty=False, timeout=CLONE_TIMEOUT)
    except fabric.exceptions.CommandTimeout:
        logger.interrupt(
            "Failed to clone forestdb under {} seconds".format(
                CLONE_TIMEOUT))


def compile_forestdb(branch, fdb_hash):
    with cd(args.remote_workdir):
        clone_repo("https://github.com/couchbase/forestdb.git")
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


def compile_standalone_test(fdb_path):
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


# Borrowed from perfrunner
def mark_previous_as_obsolete(cb, benchmark):
    for row in cb.query('benchmarks', 'values_by_build_and_metric',
                        key=[benchmark['metric'], benchmark['build']]):
        doc = cb.get(row.docid)
        doc.value.update({'obsolete': True})
        cb.set(row.docid, doc.value)


def pretty_dict(d):
    return json.dumps(d, indent=4, sort_keys=True,
                      default=lambda o: o.__dict__)


def post_benchmark(benchmark):
    if args.post_to_sf <= 0:
        logger.info("Dry run stats: {}\n".format(pretty_dict(benchmark)))
        return

    key = uuid4().hex
    try:
        cb = Couchbase.connect(
            bucket="benchmarks", host="ci.sc.couchbase.com", port=8091,
            password="password")
        mark_previous_as_obsolete(cb, benchmark)
        cb.set(key, benchmark)
    except Exception, e:
        logger.warn('Failed to post results, {}'.format(e))
        raise
    else:
        logger.info('Successfully posted: {}'.format(
            pretty_dict(benchmark)
        ))


def post_incremental(incremental_time):
    data = {}
    data["metric"] = "secondary_fdb_inc_secondary"
    data["snapshots"] = []
    data["build_url"] = None
    data["build"] = args.version
    data["value"] = incremental_time
    post_benchmark(data)


def post_initial(initial_time):
    data = {}
    data["metric"] = "secondary_fdb_ini_secondary"
    data["snapshots"] = []
    data["build_url"] = None
    data["build"] = args.version
    data["value"] = initial_time
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
            logger.info("Grepped intial build time {}".format(initial_time))
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
    xml_url = find_manifest()
    xml_data = fetch_url(xml_url)
    branch, fdb_hash = hash_from_xml(xml_data)

    if not args.run_only:
        execute(cleanup_remote_workdir)
    if not args.run_only:
        fdb_path = execute(compile_forestdb, branch, fdb_hash)
    if not args.run_only:
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

    args = parser.parse_args()

    (args.release, args.edition) = args.version.split('-')

    env.hosts = [args.host]
    env.user = "root"
    env.password = "couchbase"

#   logger.setLevel(logging.DEBUG)
#   logger.handlers[0].setLevel(logging.DEBUG)

if __name__ == "__main__":
    get_args()
    main()

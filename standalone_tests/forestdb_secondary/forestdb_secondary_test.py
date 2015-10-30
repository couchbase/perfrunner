import argparse
import fabric
import logging
import pprint
import requests
import urllib2
import urlparse
import xmltodict

from fabric.api import run, hosts, env, execute
from fabric.context_managers import cd
from logger import logger
from StringIO import StringIO

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
        clone_repo("https://github.com/uvenum/forestdb-2ibenchmark.git")
        with cd("forestdb-2ibenchmark"):
            bench_dir = "{}/forestdb-2ibenchmark".format(args.remote_workdir)
            fdb_dir = "{}/forestdb".format(args.remote_workdir)
            cmd = "g++ -o {} -Werror".format(prog_name)
            cmd += " -I{0} -I{0}/utils -I{1}/include/".format(bench_dir, fdb_dir)
            cmd += " -L{}/build".format(fdb_dir)
            cmd += " forestdb_workload.cc strgen.cc utils/iniparser.cc"
            cmd += " -lforestdb -lpthread"
            run(cmd)
            run("mv {} ../".format(prog_name))
            run("cp bench_config.ini ../")


def run_standalone_test():
    run("service couchbase-server stop", warn_only=True)
    with cd(args.remote_workdir):
        run("mkdir data")
        run("./{}".format(prog_name))


def cleanup_remote_workdir():
    run("mkdir -p {}".format(args.remote_workdir))
    run("rm   -rf {}".format(args.remote_workdir))
    run("mkdir -p {}".format(args.remote_workdir))


def main():
    xml_url = find_manifest()
    xml_data = fetch_url(xml_url)
    branch, fdb_hash = hash_from_xml(xml_data)

    execute(cleanup_remote_workdir)
    fdb_path = execute(compile_forestdb, branch, fdb_hash)
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

    args = parser.parse_args()

    (args.release, args.edition) = args.version.split('-')

    env.hosts = [args.host]

#   logger.setLevel(logging.DEBUG)
#   logger.handlers[0].setLevel(logging.DEBUG)

if __name__ == "__main__":
    get_args()
    main()

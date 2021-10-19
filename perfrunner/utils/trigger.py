import os
import sys
from argparse import ArgumentParser
from multiprocessing import set_start_method

import requests

from logger import logger

set_start_method("fork")

BASE_URL = 'http://172.23.126.166/builds/latestbuilds/couchbase-server'

CHECKPOINT_DIR = '/home/'

MAX_MISSING = 3

RELEASES = {
    'spock': '5.0.0',
    'vulcan': '5.5.6',
    'alice': '6.0.4',
    'mad-hatter': '6.5.0',
    'cheshire-cat': '7.0.0',
    'neo': '7.1.0'
}


def read_latest(release: str) -> int:
    checkpoint = os.path.join(CHECKPOINT_DIR, release)
    with open(checkpoint) as f:
        build = f.read()
        return int(build)


def store_latest(release: str, build: int):
    logger.info('Storing build {}'.format(build))

    checkpoint = os.path.join(CHECKPOINT_DIR, release)
    with open(checkpoint, 'w') as f:
        f.write(str(build))


def build_exists(release: str, build: str) -> bool:
    url = '{}/{release}/{build}/'.format(BASE_URL, release=release, build=build)

    r = requests.head(url)
    return r.status_code == 200


def rpm_package_exists(release: str, build: str, semver: str) -> bool:
    package = 'couchbase-server-enterprise-{semver}-{build}-centos7.x86_64.rpm'\
        .format(semver=semver, build=build)
    url = '{}/{release}/{build}/{package}'.format(
        BASE_URL, release=release, build=build, package=package)

    r = requests.head(url)
    return r.status_code == 200


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-r', '--release', dest='release', default='mad-hatter')
    parser.add_argument('-s', '--subrelease', dest='subrelease', default='mad-hatter')
    parser.add_argument('-l', '--relserver', dest='relserver', default='6.5.0')

    return parser.parse_args()


def main():
    args = get_args()

    latest = None
    build = read_latest(release=args.subrelease)
    missing = 0

    while missing < MAX_MISSING:
        build += 1

        logger.info('Checking build {}'.format(build))

        if not build_exists(args.release, build):
            missing += 1
            continue

        if rpm_package_exists(args.release, build, args.relserver):
            latest = build

    if latest:
        store_latest(release=args.subrelease, build=latest)
    else:
        sys.exit('No new build found')


if __name__ == '__main__':
    main()

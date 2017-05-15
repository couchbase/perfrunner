import sys
from optparse import OptionParser

import requests
from logger import logger

BASE_URL = 'http://172.23.120.24/builds/latestbuilds/couchbase-server'

CHECKPOINT = '/home/latest'

RELEASES = {
    'spock': '5.0.0',
}


def read_latest() -> int:
    with open(CHECKPOINT) as f:
        build = f.read()
        return int(build)


def store_latest(build: int):
    logger.info('Storing build {}'.format(build))
    with open(CHECKPOINT, 'w') as f:
        f.write(str(build))


def build_exists(release: str, build: str) -> bool:
    url = '{}/{release}/{build}/'.format(BASE_URL, release=release, build=build)

    r = requests.head(url)
    return r.status_code == 200


def rpm_package_exists(release: str, build: str) -> bool:
    semver = RELEASES[release]
    package = 'couchbase-server-enterprise-{semver}-{build}-centos7.x86_64.rpm'\
        .format(semver=semver, build=build)
    url = '{}/{release}/{build}/{package}'.format(
        BASE_URL, release=release, build=build, package=package)

    r = requests.head(url)
    return r.status_code == 200


def main():
    parser = OptionParser()

    parser.add_option('-r', '--release', dest='release', default='spock')
    options, _ = parser.parse_args()

    latest = None
    build = read_latest() + 1

    while build_exists(options.release, build):
        logger.info('Checking build {}'.format(build))
        if rpm_package_exists(options.release, build):
            latest = build
            build += 1

    if latest:
        store_latest(build=latest)
    else:
        sys.exit('No new build found')


if __name__ == '__main__':
    main()

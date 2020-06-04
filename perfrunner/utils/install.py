import sys
from argparse import ArgumentParser
from collections import namedtuple
from typing import Iterator

import requests
import validators
from fabric.api import cd, run
from requests.exceptions import ConnectionError

from logger import logger
from perfrunner.helpers.local import detect_ubuntu_release
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.remote.context import master_client
from perfrunner.settings import ClusterSpec

LOCATIONS = (
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/magma-preview/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/cheshire-cat/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/mad-hatter/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/alice/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/vulcan/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/spock/{build}/',
    'http://172.23.126.166/builds/latestbuilds/couchbase-server/watson/{build}/',
    'http://172.23.126.166/builds/releases/{release}/',
    'http://172.23.126.166/builds/releases/{release}/ce/',
)

PKG_PATTERNS = {
    'rpm': (
        'couchbase-server-{edition}-{release}-{build}-centos{os}.x86_64.rpm',
        'couchbase-server-{edition}-{release}-centos{os}.x86_64.rpm',
        'couchbase-server-{edition}-{release}-centos6.x86_64.rpm',
    ),
    'deb': (
        'couchbase-server-{edition}_{release}-{build}-ubuntu{os}_amd64.deb',
        'couchbase-server-{edition}_{release}-ubuntu{os}_amd64.deb',
    ),
    'exe': (
        'couchbase-server-{edition}_{release}-{build}-windows_amd64.msi',
        'couchbase-server-{edition}_{release}-{build}-windows_amd64.exe',
        'couchbase-server-{edition}_{release}-windows_amd64.exe',
    ),
}

Build = namedtuple('Build', ['filename', 'url'])


class CouchbaseInstaller:

    def __init__(self, cluster_spec, options):
        self.remote = RemoteHelper(cluster_spec, options.verbose)
        self.options = options
        self.cluster_spec = cluster_spec

    @property
    def url(self) -> str:
        if validators.url(self.options.version):
            return self.options.version
        else:
            return self.find_package(edition=self.options.edition)

    @property
    def release(self) -> str:
        return self.options.version.split('-')[0]

    @property
    def build(self) -> str:
        split = self.options.version.split('-')
        if len(split) > 1:
            return split[1]

    def find_package(self, edition: str,
                     package: str = None, os_release: str = None) -> [str, str]:
        for url in self.url_iterator(edition, package, os_release):
            if self.is_exist(url):
                return url
        logger.interrupt('Target build not found')

    def url_iterator(self, edition: str,
                     package: str = None, os_release: str = None) -> Iterator[str]:
        if package is None:
            if self.remote.package == 'rpm':
                os_release = self.remote.detect_centos_release()
            elif self.remote.package == 'deb':
                os_release = self.remote.detect_ubuntu_release()
            package = self.remote.package

        for pkg_pattern in PKG_PATTERNS[package]:
            for loc_pattern in LOCATIONS:
                url = loc_pattern + pkg_pattern
                yield url.format(release=self.release, build=self.build,
                                 edition=edition, os=os_release)

    @staticmethod
    def is_exist(url):
        try:
            status_code = requests.head(url).status_code
        except ConnectionError:
            return False
        if status_code == 200:
            return True
        return False

    def download(self):
        """Download and save a copy of the specified package."""
        if self.remote.package == 'rpm':
            logger.info('Saving a local copy of {}'.format(self.url))
            with open('couchbase.rpm', 'wb') as fh:
                resp = requests.get(self.url)
                fh.write(resp.content)
        else:
            logger.interrupt('Unsupported package format')

    def download_local(self):
        """Download and save a copy of the specified package."""
        try:
            if RemoteHelper.detect_server_os("127.0.0.1").upper() in ('UBUNTU', 'DEBIAN'):
                os_release = detect_ubuntu_release()

                url = self.find_package(edition=self.options.edition,
                                        package="deb", os_release=os_release)
                logger.info('Saving a local copy of {}'.format(url))
                with open('couchbase.deb', 'wb') as fh:
                    resp = requests.get(url)
                    fh.write(resp.content)
        except (Exception, BaseException):
            logger.info("Saving local copy for ubuntu failed, package may not present")

    def download_remote(self):
        """Download and save a copy of the specified package on a remote client."""
        if self.remote.package == 'rpm':
            logger.info('Saving a remote copy of {}'.format(self.url))
            self.wget(url=self.url)
        else:
            logger.interrupt('Unsupported package format')

    @master_client
    def wget(self, url):
        logger.info('Fetching {}'.format(url))
        with cd('/tmp'):
            run('wget -nc "{}"'.format(url))
            package = url.split('/')[-1]
            run('mv {} couchbase.rpm'.format(package))

    def kill_processes(self):
        self.remote.kill_processes()

    def uninstall_package(self):
        self.remote.uninstall_couchbase()

    def clean_data(self):
        self.remote.clean_data()

    def install_package(self):
        logger.info('Using this URL: {}'.format(self.url))
        self.remote.upload_iss_files(self.release)
        self.remote.install_couchbase(self.url)

    def install(self):
        self.kill_processes()
        self.uninstall_package()
        self.clean_data()
        self.install_package()


def get_args():
    parser = ArgumentParser()

    parser.add_argument('-v', '--version', '--url',
                        required=True,
                        help='the build version or the HTTP URL to a package')
    parser.add_argument('-c', '--cluster',
                        required=True,
                        help='the path to a cluster specification file')
    parser.add_argument('-e', '--edition',
                        choices=['enterprise', 'community'],
                        default='enterprise',
                        help='the cluster edition')
    parser.add_argument('--verbose',
                        action='store_true',
                        help='enable verbose logging')
    parser.add_argument('--local-copy',
                        action='store_true',
                        help='save a local copy of a package')
    parser.add_argument('--remote-copy',
                        action='store_true',
                        help='save a remote copy of a package')

    return parser.parse_args()


def main():
    args = get_args()

    cluster_spec = ClusterSpec()
    cluster_spec.parse(fname=args.cluster)

    installer = CouchbaseInstaller(cluster_spec, args)
    installer.install()

    if args.local_copy:
        installer.download()
        installer.download_local()

    if '--remote-copy' in sys.argv:
        logger.info('Saving a remote copy')
        installer.download_remote()


if __name__ == '__main__':
    main()

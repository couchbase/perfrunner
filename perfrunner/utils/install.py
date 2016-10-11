from collections import namedtuple
from optparse import OptionParser
from urlparse import urlparse

import requests
from logger import logger
from requests.exceptions import ConnectionError

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec

LOCATIONS = (
    'http://latestbuilds.hq.couchbase.com/',
    'http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock/{build}/',
    'http://172.23.120.24/builds/latestbuilds/couchbase-server/watson/{build}/',
    'http://172.23.120.24/builds/latestbuilds/couchbase-server/spock/{build}/',
)

Build = namedtuple(
    'Build',
    ['arch', 'pkg', 'edition', 'version', 'release', 'build', 'url']
)


class CouchbaseInstaller(object):

    def __init__(self, cluster_spec, options):
        self.options = options
        self.remote = RemoteHelper(cluster_spec, None, options.verbose)
        self.cluster_spec = cluster_spec

        arch = self.remote.detect_arch()
        pkg = self.remote.detect_pkg()

        release = None
        build = None
        if options.version:
            release, build = options.version.split('-')

        self.build = Build(arch, pkg, options.cluster_edition, options.version,
                           release, build, options.url)
        logger.info('Target build info: {}'.format(self.build))

    def get_expected_filenames(self):
        if self.build.pkg == 'rpm':
            os_release = self.remote.detect_os_release()
            patterns = (
                'couchbase-server-{{edition}}-{{version}}-centos{}.{{arch}}.{{pkg}}'.format(os_release),
                'couchbase-server-{edition}_centos6_{arch}_{version}-rel.{pkg}',
            )
        elif self.build.pkg == 'deb':
            patterns = (
                'couchbase-server-{edition}_{arch}_{version}-rel.{pkg}',
                'couchbase-server-{edition}_{version}-ubuntu12.04_{arch}.{pkg}',
            )
        elif self.build.pkg == 'exe':
            patterns = (
                'couchbase-server-{edition}_{arch}_{version}-rel.setup.{pkg}',
                'couchbase-server-{edition}_{version}-windows_{arch}.{pkg}',
                'couchbase_server-{edition}-windows-{arch}-{version}.{pkg}',
            )
        else:
            patterns = ()  # Sentinel

        for pattern in patterns:
            yield pattern.format(**self.build.__dict__)

    @staticmethod
    def is_exist(url):
        try:
            status_code = requests.head(url).status_code
        except ConnectionError:
            return False
        if status_code == 200:
            return True
        return False

    def find_package(self):
        for filename in self.get_expected_filenames():
            for location in LOCATIONS:
                url = '{}{}'.format(location.format(**self.build.__dict__),
                                    filename)
                if self.is_exist(url):
                    return filename, url
        logger.interrupt('Target build not found')

    def kill_processes(self):
        self.remote.kill_processes()

    def uninstall_package(self):
        self.remote.uninstall_couchbase(self.build.pkg)

    def clean_data(self):
        self.remote.clean_data()

    def install_package(self):
        if self.options.version:
            filename, url = self.find_package()
        else:
            url = self.options.url
            filename = urlparse(url).path.split('/')[-1]

        logger.info('Using this URL: {}'.format(url))
        self.remote.install_couchbase(self.build.pkg, url, filename,
                                      self.build.release)

    def install(self):
        self.kill_processes()
        self.uninstall_package()
        self.clean_data()
        self.install_package()


def main():
    usage = '%prog -c cluster -v version'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='the path to a cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-e', dest='cluster_edition', default='enterprise',
                      help='the cluster edition (community or enterprise)')
    parser.add_option('-v', dest='version',
                      help='the build version', metavar='2.0.0-1976')
    parser.add_option('--url', dest='url', default=None,
                      help='the HTTP URL to a package that should be installed.')
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')

    options, args = parser.parse_args()

    if options.cluster_edition not in ['community', 'enterprise']:
        # changed to default to enterprise, with no error:
        options.cluster_edition = 'enterprise'

    if not (options.cluster_spec_fname and options.version) and not options.url:
        parser.error('Missing mandatory parameter. Either pecify both cluster '
                     'spec and version, or specify the URL to be installed.')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, args)

    installer = CouchbaseInstaller(cluster_spec, options)
    installer.install()

if __name__ == '__main__':
    main()

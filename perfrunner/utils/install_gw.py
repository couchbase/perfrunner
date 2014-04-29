from collections import namedtuple
from optparse import OptionParser

import requests
from logger import logger

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec

Build = namedtuple('Build', ['arch', 'pkg', 'version'])


class GatewayInstaller(object):

    CBFS = 'http://cbfs-ext.hq.couchbase.com/builds/'

    def __init__(self, cluster_spec, options):
        self.remote_helper = RemoteHelper(cluster_spec)
        self.cluster_spec = cluster_spec

        arch = self.remote_helper.detect_arch()
        pkg = self.remote_helper.detect_pkg()

        self.build = Build(arch, pkg, options.version)
        logger.info('Target build info: {}'.format(self.build))

    def get_expected_filenames(self):
        patterns = (
            'couchbase-sync-gateway_1.0.0-11_{arch}.rpm',
        )
        for pattern in patterns:
            yield pattern.format(**self.build._asdict())

    def find_package(self):
        for filename in self.get_expected_filenames():
                url = '{}{}'.format(self.CBFS, filename)
                try:
                    status_code = requests.head(url).status_code
                except requests.exceptions.ConnectionError:
                    continue
                else:
                    if status_code == 200:
                        logger.info('Found "{}"'.format(url))
                        return filename, url
        logger.interrupt('Target build not found')

    def kill_processes(self):
        self.remote_helper.kill_processes_gw()
        self.remote_helper.kill_processes_gl()

    def uninstall_package(self):
        filename, url = self.find_package()
        self.remote_helper.uninstall_package_gw(self.build.pkg, filename)
        self.remote_helper.uninstall_package_gl()

    def install_package(self):
        filename, url = self.find_package()
        self.remote_helper.install_package_gw(self.build.pkg, url, filename,
                                           self.build.version)
        self.remote_helper.install_package_gl()

    def install(self):
        self.kill_processes()
        self.uninstall_package()
        self.install_package()


def main():
    usage = '%prog -c cluster -v version'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-v', dest='version',
                      help='build version', metavar='1.0.0')


    options, _ = parser.parse_args()
    if not options.cluster_spec_fname or not options.version:
        parser.error('Missing mandatory parameter - cluster_spec_fname and version')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname)

    installer = GatewayInstaller(cluster_spec, options)
    installer.install()

if __name__ == '__main__':
    main()

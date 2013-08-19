from collections import namedtuple
from optparse import OptionParser

import requests
from fabric.api import run
from logger import logger

from perfrunner.helpers.remote import RemoteHelper, all_hosts
from perfrunner.settings import ClusterSpec

Build = namedtuple('Build', ['arch', 'pkg', 'version', 'toy'])


class CouchbaseInstaller(RemoteHelper):

    BUILDER = 'http://builder.hq.couchbase.com/get/'

    @staticmethod
    def get_expected_filename(build):
        if build.toy:
            return 'couchbase-server-community_toy-{0}-{1}_{2}-toy.{3}'.format(
                build.toy, build.arch, build.version, build.pkg
            )
        else:
            return 'couchbase-server-enterprise_{0}_{1}-rel.{2}'.format(
                build.arch, build.version, build.pkg
            )

    def check_if_exists(self, url):
        r = requests.head(url)
        if r.status_code == 200:
            logger.info('Found "{0}"'.format(url))
        else:
            logger.interrupt('Target build not found')

    @all_hosts
    def uninstall_package(self, pkg):
        logger.info('Uninstalling Couchbase Server')
        if pkg == 'deb':
            run('yes | apt-get remove couchbase-server')
        else:
            run('yes | yum remove couchbase-server')
        run('rm -fr /opt/couchbase')

    @all_hosts
    def install_package(self, pkg, filename, url):
        self.wget(url, outdir='/tmp')

        logger.info('Installing Couchbase Server')
        if pkg == 'deb':
            run('yes | apt-get install gdebi')
            run('yes | gdebi /tmp/{0}'.format(filename))
        else:
            run('yes | rpm -i /tmp/{0}'.format(filename))

    def install(self, options):
        arch = self.detect_arch()
        pkg = self.detect_pkg()
        build = Build(arch, pkg, options.version, options.toy)

        filename = self.get_expected_filename(build)
        url = '{0}{1}'.format(self.BUILDER, filename)
        self.check_if_exists(url)

        self.uninstall_package(pkg)
        self.install_package(pkg, filename, url)


def main():
    usage = '%prog -c cluster -v version [-t toy]'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-v', dest='version',
                      help='build version', metavar='2.0.0-1976')
    parser.add_option('-t', dest='toy',
                      help='optional toy build ID', metavar='couchstore')

    options, _ = parser.parse_args()
    if not options.cluster_spec_fname or not options.version:
        parser.error('Missing mandatory parameter')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname)

    installer = CouchbaseInstaller(cluster_spec)
    installer.install(options)

if __name__ == '__main__':
    main()

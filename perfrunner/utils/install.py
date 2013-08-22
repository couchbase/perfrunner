import time
from collections import namedtuple
from optparse import OptionParser

import requests
from fabric.api import run, put
from logger import logger

from perfrunner.helpers.remote import RemoteHelper, all_hosts
from perfrunner.settings import ClusterSpec

Build = namedtuple('Build', ['arch', 'pkg', 'version', 'toy'])


class CouchbaseInstaller(object):

    def __new__(cls, cluster_sepc, options):
        remote_helper = RemoteHelper(cluster_sepc)
        arch = remote_helper.detect_arch()
        pkg = remote_helper.detect_pkg()
        build = Build(arch, pkg, options.version, options.toy)

        if pkg == 'setup.exe':
            return WindowsInstaller(build, cluster_sepc)
        else:
            return LinuxInstaller(build, cluster_sepc)


class Installer(RemoteHelper):

    BUILDER = 'http://builder.hq.couchbase.com/get/'
    LATEST_BUILDS = 'http://builds.hq.northscale.net/latestbuilds/'

    def __init__(self, build, cluster_sepc):
        self.build = build
        self.filename = self.get_expected_filename(build)
        self.url = self.get_url()

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

    def get_url(self):
        for base in (self.BUILDER, self.LATEST_BUILDS):
            url = '{0}{1}'.format(base, self.filename)
            if requests.head(url).status_code == 200:
                logger.info('Found "{0}"'.format(url))
                return url
        logger.interrupt('Target build not found')

    def uninstall_package(self):
        raise NotImplementedError

    def install_package(self):
        raise NotImplementedError

    def install(self):
        self.uninstall_package()
        self.install_package()


class LinuxInstaller(Installer):

    @all_hosts
    def uninstall_package(self):
        logger.info('Uninstalling Couchbase Server on Linux')
        if self.build.pkg == 'deb':
            run('yes | apt-get remove couchbase-server')
        else:
            run('yes | yum remove couchbase-server')
        run('rm -fr /opt/couchbase')

    @all_hosts
    def install_package(self):
        logger.info('Installing Couchbase Server on Linux')

        self.wget(self.url, outdir='/tmp')
        if self.build.pkg == 'deb':
            run('yes | apt-get install gdebi')
            run('yes | gdebi /tmp/{0}'.format(self.filename))
        else:
            run('yes | rpm -i /tmp/{0}'.format(self.filename))


class WindowsInstaller(Installer):

    VERSION_FILE = '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'

    @all_hosts
    def uninstall_package(self):
        logger.info('Uninstalling Couchbase Server on Windows')

        put('scripts/uninstall.iss', '/cygdrive/c')
        run('setup.exe -s -f1"C:\\uninstall.iss"')
        while self.exists(self.VERSION_FILE):
            time.sleep(5)
        time.sleep(30)
        run('rm -fr /cygdrive/c/Program\ Files/Couchbase')

    @all_hosts
    def install_package(self):
        logger.info('Installing Couchbase Server on Windows')

        self.wget(self.url, outdir='/cygdrive/c', outfile='setup.exe')
        put('scripts/install.iss', '/cygdrive/c')
        run('setup.exe -s -f1"C:\\install.iss"')
        while not self.exists(self.VERSION_FILE):
            time.sleep(5)
        time.sleep(60)


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

    installer = CouchbaseInstaller(cluster_spec, options)
    installer.install()

if __name__ == '__main__':
    main()

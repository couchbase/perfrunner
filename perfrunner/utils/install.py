import time
from collections import namedtuple
from optparse import OptionParser

import requests
from fabric.api import run, put
from logger import logger

from perfrunner.helpers.remote import RemoteHelper, all_hosts
from perfrunner.settings import ClusterSpec

Build = namedtuple('Build', ['arch', 'pkg', 'version', 'openssl', 'toy'])


class CouchbaseInstaller(object):

    def __new__(cls, cluster_spec, options):
        remote_helper = RemoteHelper(cluster_spec)
        arch = remote_helper.detect_arch()
        pkg = remote_helper.detect_pkg()
        openssl = remote_helper.detect_openssl(pkg)
        build = Build(arch, pkg, options.version, openssl, options.toy)

        if pkg == 'setup.exe':
            return WindowsInstaller(build, cluster_spec)
        else:
            return LinuxInstaller(build, cluster_spec)


class Installer(RemoteHelper):

    CBFS = 'http://cbfs-ext.hq.couchbase.com/builds/'
    LATEST_BUILDS = 'http://builds.hq.northscale.net/latestbuilds/'

    def __init__(self, build, cluster_spec):
        super(Installer, self).__init__(cluster_spec)
        self.cluster_spec = cluster_spec
        self.build = build

    def get_expected_filenames(self):
        if self.build.toy:
            patterns = (
                'couchbase-server-community_toy-{toy}-{arch}_{version}-toy.{pkg}',
                'couchbase-server-community_toy-{toy}-{version}-toy_{arch}.{pkg}'
            )
        elif self.build.openssl == '0.9.8e':
            patterns = (
                'couchbase-server-enterprise_{arch}_{version}-rel.{pkg}',
                'couchbase-server-enterprise_{version}-rel_{arch}_openssl098.{pkg}'
            )
        else:
            patterns = (
                'couchbase-server-enterprise_{arch}_{version}-rel.{pkg}',
                'couchbase-server-enterprise_{version}-rel_{arch}.{pkg}'
            )
        for pattern in patterns:
            yield pattern.format(**dict(zip(self.build._fields, self.build)))

    def find_package(self):
        for filename in self.get_expected_filenames():
            for base in (self.CBFS, self.LATEST_BUILDS):
                url = '{0}{1}'.format(base, filename)
                if requests.head(url).status_code == 200:
                    logger.info('Found "{0}"'.format(url))
                    self.filename = filename
                    self.url = url
                    return
        logger.interrupt('Target build not found')

    def uninstall_package(self):
        raise NotImplementedError

    def clean_data_path(self):
        raise NotImplementedError

    def install_package(self):
        raise NotImplementedError

    def install(self):
        self.find_package()
        self.uninstall_package()
        self.clean_data_path()
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
    def clean_data_path(self):
        for path in self.cluster_spec.get_paths():
            run('rm -fr {0}/*'.format(path))

    @all_hosts
    def install_package(self):
        self.wget(self.url, outdir='/tmp')

        logger.info('Installing Couchbase Server on Linux')
        if self.build.pkg == 'deb':
            run('yes | apt-get install gdebi')
            run('yes | gdebi /tmp/{0}'.format(self.filename))
        else:
            run('yes | rpm -i /tmp/{0}'.format(self.filename))


class WindowsInstaller(Installer):

    VERSION_FILE = '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'

    @all_hosts
    def clean_data_path(self):
        for path in self.cluster_spec.get_paths():
            path = path.replace(':', '').replace('\\', '/')
            path = '/cygdrive/{0}'.format(path)
            run('rm -fr {0}/*'.format(path))

    @all_hosts
    def uninstall_package(self):
        logger.info('Uninstalling Couchbase Server on Windows')

        if self.exists(self.VERSION_FILE):
            run('./setup.exe -s -f1"C:\\uninstall.iss"')
        while self.exists(self.VERSION_FILE):
            time.sleep(5)
        time.sleep(30)

        self.clean_data_path()
        run('rm -fr /cygdrive/c/Program\ Files/Couchbase')

    def put_iss_files(self):
        version = self.build.version.split('-')[0]

        logger.info('Copying {0} ISS files'.format(version))
        put('scripts/install_{0}.iss'.format(version),
            '/cygdrive/c/install.iss')
        put('scripts/uninstall_{0}.iss'.format(version),
            '/cygdrive/c/uninstall.iss')

    @all_hosts
    def install_package(self):
        run('rm -fr setup.exe')
        self.wget(self.url, outfile='setup.exe')
        run('chmod +x setup.exe')

        self.put_iss_files()

        logger.info('Installing Couchbase Server on Windows')

        run('./setup.exe -s -f1"C:\\install.iss"')
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

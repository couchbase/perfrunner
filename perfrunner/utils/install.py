from collections import namedtuple
from optparse import OptionParser

import requests
from lxml import etree
from fabric.api import run

from perfrunner.logger import logger
from perfrunner.utils.remote import RemoteHelper, all_hosts


Build = namedtuple('Build', ['arch', 'pkg', 'version', 'toy'])


class BuildIterator(object):

    BUILDS_URL = 'http://builds.hq.northscale.net/latestbuilds/'

    def __init__(self):
        pass

    @classmethod
    def _parse_build_tree(cls):
        logger.info('Getting list of builds')
        try:
            headers = {'Accept-Encoding': 'gzip, deflate'}
            html = requests.get(url=cls.BUILDS_URL, headers=headers).text
        except requests.exceptions.ConnectionError:
            logger.interrupt('Could not connect to build repository')
        return etree.HTML(html)

    def __iter__(self):
        tree = self._parse_build_tree()
        for li in tree.xpath('/html/body/ul/li'):
            yield li.xpath('a/@href')[0]


class CouchbaseInstaller(RemoteHelper):

    @staticmethod
    def _get_expected_filename(build):
        if build.toy:
            return 'couchbase-server-community_toy-{0}-{1}_{2}-toy.{3}'.format(
                build.toy, build.arch, build.version, build.pkg
            )
        else:
            return 'couchbase-server-enterprise_{0}_{1}-rel.{2}'.format(
                build.arch, build.version, build.pkg
            )

    @staticmethod
    def _check_if_exists(target):
        for filename in BuildIterator():
            if filename == target:
                logger.info('Found "{0}"'.format(filename))
                break
        else:
            logger.interrupt('Target build not found')

    @all_hosts
    def uninstall_package(self, pkg):
        if pkg == 'deb':
            run('yes | apt-get remove couchbase-server')
        else:
            run('yes | yum remove couchbase-server')
        run('rm -fr /opt/couchbase')

    @all_hosts
    def install_package(self, pkg, filename):
        url = BuildIterator.BUILDS_URL + filename
        self.wget(url, outdir='/tmp')

        if pkg == 'deb':
            run('yes | apt-get install gdebi')
            run('yes | gdebi /tmp/{0}'.format(filename))
        else:
            run('yes | rpm -i /tmp/{0}'.format(filename))

    def install(self, options):
        arch = self.detect_arch()
        pkg = self.detect_pkg()
        build = Build(arch, pkg, options.version, options.toy)

        filename = self._get_expected_filename(build)
        self._check_if_exists(filename)

        self.uninstall_package(pkg)
        self.install_package(pkg, filename)


def main():
    usage = '%prog -c cluster -v version [-t toy]'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster',
                      help='path to cluster specification file',
                      metavar='cluster.spec')
    parser.add_option('-v', dest='version',
                      help='build version', metavar='2.0.0-1976')
    parser.add_option('-t', dest='toy',
                      help='optional toy build ID', metavar='couchstore')

    options, _ = parser.parse_args()
    if not options.cluster or not options.version:
        parser.error('Missing mandatory parameter')

    installer = CouchbaseInstaller(options.cluster)
    installer.install(options)

if __name__ == '__main__':
    main()

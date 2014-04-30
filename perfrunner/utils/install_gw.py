import json
from collections import namedtuple
from optparse import OptionParser

import requests
from logger import logger

from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec
from perfrunner.settings import TestConfigGateway


class GatewayInstaller(object):

    CBFS = 'http://cbfs-ext.hq.couchbase.com/builds/'

    def __init__(self, cluster_spec, test_config_gateway, options):
        self.remote_helper = RemoteHelper(cluster_spec)
        self.cluster_spec = cluster_spec
        self.test_config = test_config_gateway
        self.pkg = self.remote_helper.detect_pkg()
        self.version = options.version

    def find_package(self):
        filename = 'couchbase-sync-gateway_{}_x86_64.rpm'.format(self.version)
        url = '{}{}'.format(self.CBFS, filename)
        try:
            status_code = requests.head(url).status_code
        except requests.exceptions.ConnectionError:
            pass
        else:
            if status_code == 200:
                logger.info('Found "{}"'.format(url))
                return filename, url
        logger.interrupt('Target build not found - {}'.format(url))

    def kill_processes(self):
        self.remote_helper.kill_processes_gw()

    def uninstall_package(self):
        filename, url = self.find_package()
        self.remote_helper.uninstall_package_gw(self.pkg, filename)

    def install_package(self):
        filename, url = self.find_package()
        self.remote_helper.install_package_gw(self.pkg, url, filename,
                                              self.version)

    def start_sync_gateway(self):
        with open('perfrunner/templates/gateway_config_template.json') as fh:
            content = json.load(fh)
        with open('perfrunner/templates/gateway_config.json', 'w') as fh:
            content['maxIncomingConnections'] = self.test_config.gateway_conn_in
            content['maxCouchbaseConnections'] = self.test_config.gateway_conn_db
            content['CompressResponses'] = self.test_config.gateway_compression
            db_master = self.cluster_spec.yield_masters().next()
            content['databases']['db']['server'] = "http://bucket-1:password@{}/".format(db_master)
            fh.write(pretty_dict(content))
        self.remote_helper.start_sync_gateway()

    def install(self):
        self.kill_processes()
        #self.uninstall_package()
        #self.install_package()
        self.start_sync_gateway()


def main():
    usage = '%prog -c cluster -t test_config -v version'

    parser = OptionParser(usage)

    parser.add_option('-c', dest='cluster_spec_fname',
                      help='path to cluster specification file',
                      metavar='ClusterSpecFilePath')
    parser.add_option('-t', dest='test_config_fname',
                      help='path to test config file',
                      metavar='TestConfigFilePath')
    parser.add_option('-v', dest='version',
                      help='build version', metavar='Version')

    options, _ = parser.parse_args()
    if not options.cluster_spec_fname or not options.test_config_fname or not options.version:
        parser.error('Missing mandatory parameter - ClusterSpecFilePath, TestConfigFilePath or Version')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname)

    test_config_gateway = TestConfigGateway()
    test_config_gateway.parse(options.test_config_fname)

    installer = GatewayInstaller(cluster_spec, test_config_gateway, options)
    installer.install()

if __name__ == '__main__':
    main()

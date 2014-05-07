import json
from optparse import OptionParser

import requests
from logger import logger

from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.settings import ClusterSpec
from perfrunner.settings import TestConfig


class GatewayInstaller(object):

    CBFS = 'http://cbfs-ext.hq.couchbase.com/builds/'

    def __init__(self, cluster_spec, test_config, options):
        self.remote_helper = RemoteHelper(cluster_spec)
        self.cluster_spec = cluster_spec
        self.test_config = test_config
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

    def kill_processes_gateway(self):
        self.remote_helper.kill_processes_gateway()

    def kill_processes_gateload(self):
        self.remote_helper.kill_processes_gateload()

    def uninstall_package_gateway(self):
        self.remote_helper.uninstall_package_gateway()

    def uninstall_package_gateload(self):
        self.remote_helper.uninstall_package_gateload()

    def install_package_gateway(self):
        filename, url = self.find_package()
        self.remote_helper.install_package_gateway(url, filename)

    def install_package_gateload(self):
        self.remote_helper.install_package_gateload()

    def start_sync_gateways(self):
        with open('templates/gateway_config_template.json') as fh:
            template = json.load(fh)

        db_master = self.cluster_spec.yield_masters().next()
        template['databases']['db']['server'] = "http://bucket-1:password@{}/".format(db_master)
        template.update({
            'maxIncomingConnections': self.test_config.gateway_settings.conn_in,
            'maxCouchbaseConnections': self.test_config.gateway_settings.conn_db,
            'CompressResponses': self.test_config.gateway_settings.compression
        })

        with open('templates/gateway_config.json', 'w') as fh:
            fh.write(pretty_dict(template))
        self.remote_helper.start_gateway()

    def install(self):
        num_gateways = len(self.cluster_spec.gateways)
        num_gateloads = len(self.cluster_spec.gateloads)
        if num_gateways != num_gateloads:
            logger.interrupt(
                'The cluster config file has different number of gateways({}) and gateloads({})'
                .format(num_gateways, num_gateloads)
            )
        self.kill_processes_gateway()
        self.uninstall_package_gateway()
        self.install_package_gateway()
        self.kill_processes_gateload()
        self.uninstall_package_gateload()
        self.install_package_gateload()
        self.start_sync_gateways()


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
    if not options.cluster_spec_fname or not options.test_config_fname \
            or not options.version:
        parser.error('Missing mandatory parameter')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname)

    test_config = TestConfig()
    test_config.parse(options.test_config_fname)

    installer = GatewayInstaller(cluster_spec, test_config, options)
    installer.install()

if __name__ == '__main__':
    main()

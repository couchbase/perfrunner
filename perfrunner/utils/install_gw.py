import json
from optparse import OptionParser

import requests
from logger import logger
from requests.exceptions import ConnectionError

from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import SyncGatewayRequestHelper
from perfrunner.settings import ClusterSpec
from perfrunner.settings import SGW_SERIESLY_HOST
from perfrunner.settings import TestConfig


class GatewayInstaller(object):

    BUILDS = {
        'http://packages.couchbase.com/builds/mobile/sync_gateway': (
            '0.0.0/{0}/couchbase-sync-gateway_{0}_x86_64-community.rpm',
            '1.0.0/{0}/couchbase-sync-gateway_{0}_x86_64.rpm',
        ),
        'http://cbfs-ext.hq.couchbase.com/builds': (
            'couchbase-sync-gateway_{}_x86_64-community.rpm',
        ),
    }

    def __init__(self, cluster_spec, test_config, options):
        self.remote = RemoteHelper(cluster_spec, options.verbose)
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.version = options.version
        self.request_helper = SyncGatewayRequestHelper()

    def find_package(self):
        for filename, url in self.get_expected_locations():
            try:
                status_code = requests.head(url).status_code
            except ConnectionError:
                pass
            else:
                if status_code == 200:
                    logger.info('Found "{}"'.format(url))
                    return filename, url
        logger.interrupt('Target build not found')

    def get_expected_locations(self):
        for location, patterns in self.BUILDS.items():
            for pattern in patterns:
                url = '{}/{}'.format(location, pattern.format(self.version))
                filename = url.split('/')[-1]
                yield filename, url

    def kill_processes_gateway(self):
        self.remote.kill_processes_gateway()

    def kill_processes_gateload(self):
        self.remote.kill_processes_gateload()

    def uninstall_package_gateway(self):
        self.remote.uninstall_package_gateway()
        self.remote.clean_gateway()

    def uninstall_package_gateload(self):
        self.remote.uninstall_package_gateload()
        self.remote.clean_gateload()

    def install_package_gateway(self):
        filename, url = self.find_package()
        self.remote.install_package_gateway(url, filename)

    def install_package_gateload(self):
        self.remote.install_package_gateload()

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
        self.remote.start_gateway()

    def create_bash_config(self):
        logger.info('Creating bash configuration')
        with open('scripts/sgw_test_config.sh', 'w') as fh:
            fh.write('#!/bin/sh\n')
            fh.write('gateways_ip="{}"\n'.format(' '.join(self.cluster_spec.gateways)))
            fh.write('gateloads_ip="{}"\n'.format(' '.join(self.cluster_spec.gateloads)))
            fh.write('dbs_ip="{}"\n'.format(' '.join(self.cluster_spec.yield_hostnames())))
            fh.write('seriesly_ip={}\n'.format(SGW_SERIESLY_HOST))

    def install(self):
        self.kill_processes_gateway()
        self.uninstall_package_gateway()
        self.install_package_gateway()
        self.kill_processes_gateload()
        self.uninstall_package_gateload()
        self.install_package_gateload()
        self.create_bash_config()
        self.start_sync_gateways()
        for i, gateway_ip in enumerate(self.cluster_spec.gateways, start=1):
            self.request_helper.wait_for_gateway_to_start(i, gateway_ip)
            self.request_helper.turn_off_gateway_logging(i, gateway_ip)
        self.remote.start_test_info()


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
    parser.add_option('--verbose', dest='verbose', action='store_true',
                      help='enable verbose logging')

    options, args = parser.parse_args()
    override = args and [arg.split('.') for arg in ' '.join(args).split(',')]

    if not options.cluster_spec_fname or not options.test_config_fname \
            or not options.version:
        parser.error('Missing mandatory parameter')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, override)
    cluster_spec.verify()

    test_config = TestConfig()
    test_config.parse(options.test_config_fname, override)

    installer = GatewayInstaller(cluster_spec, test_config, options)
    installer.install()

if __name__ == '__main__':
    main()

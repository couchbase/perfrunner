import urllib2
from optparse import OptionParser

import requests
from jinja2 import Environment, FileSystemLoader
from logger import logger
from requests.exceptions import ConnectionError

from perfrunner.helpers.remote import RemoteHelper
from perfrunner.helpers.rest import SyncGatewayRequestHelper
from perfrunner.settings import ClusterSpec, TestConfig


class GatewayInstaller(object):

    BUILDS = {
        'http://packages.couchbase.com/builds/mobile/sync_gateway': (
            '0.0.0/{0}/couchbase-sync-gateway_{0}_x86_64-community.rpm',
            '1.0.0/{0}/couchbase-sync-gateway_{0}_x86_64.rpm',
        ),
        'http://packages.couchbase.com.s3.amazonaws.com/builds/mobile/sync_gateway': (
            '0.0.0/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.0/{0}/couchbase-sync-gateway-enterprise_{0}_x86_64.rpm',
            '1.0.1/{0}/couchbase-sync-gateway-enterprise_{0}_x86_64.rpm',
            '1.0.2/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.3/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.4/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.1.0/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
        ),
        'http://cbfs-ext.hq.couchbase.com/builds': (
            'couchbase-sync-gateway_{}_x86_64-community.rpm',
        ),
        'http://latestbuilds.hq.couchbase.com/couchbase-sync-gateway': (
            '0.0.0/416/couchbase-sync-gateway-enterprise_0.0.0-326_x86_64.rpm',
        ),
        'http://latestbuilds.hq.couchbase.com/couchbase-sync-gateway': (
            '0.0.0/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '0.0.1/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.0/{0}/couchbase-sync-gateway-enterprise_{0}_x86_64.rpm',
            '1.0.1/{0}/couchbase-sync-gateway-enterprise_{0}_x86_64.rpm',
            '1.0.2/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.3/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.0.4/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
            '1.1.0/{0}/couchbase-sync-gateway-community_{0}_x86_64.rpm',
        ),
    }

    def __init__(self, cluster_spec, test_config, options):
        self.remote = RemoteHelper(cluster_spec, test_config, options.verbose)
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

    def uninstall_gateway(self):
        self.remote.uninstall_gateway()
        self.remote.clean_gateway()

    def uninstall_gateload(self):
        self.remote.uninstall_gateload()
        self.remote.clean_gateload()

    def install_gateway(self):
        if self.is_source_build(self.version):
            commit_hash = self.version.split(":")[1]
            self.remote.install_gateway_from_source(commit_hash)
        else:
            filename, url = self.find_package()
            self.remote.install_gateway(url, filename)

    def is_source_build(self, version):
        """
        did the user pass in a version of the form commit:<commit_hash>
        as opposed to the form x.y.z (which represents a jenkins build release)?
        """
        return version.startswith("commit:")

    def install_gateload(self):
        self.remote.install_gateload()

    def choose_template(self):

        config_url_setting = self.test_config.gateway_settings.config_url
        if len(config_url_setting) > 0:

            target_filename = "downloaded_config.json"
            target_path = "templates/{}".format(target_filename)

            # build url to remote template (http://git.io/b9PK)
            config_url = "http://git.io/{}".format(config_url_setting)

            # download to a file in templates directory
            logger.info("Downloading config: {}".format(config_url))
            contents = urllib2.urlopen(config_url).read()
            logger.info("Writing config to: {}".format(target_path))
            f = open(target_path, 'w')
            f.write(contents)
            f.close()

            # return name of file
            return target_filename

        if self.test_config.gateway_settings.shadow == 'true':
            return 'gateway_config_shadow_template.json'
        else:
            return 'gateway_config_template.json'

    def generate_sync_gateways_config(self):
        loader = FileSystemLoader('templates')
        env = Environment(loader=loader)

        template_filename = self.choose_template()
        template = env.get_template(template_filename)

        for idx, gateway_ip in enumerate(self.remote.gateways, start=0):
            output_filename = "templates/gateway_config_{}.json".format(idx)
            cache_writer = "false"
            if idx == 0:
                cache_writer = self.test_config.gateway_settings.node0_cache_writer
            elif idx == 1:
                cache_writer = self.test_config.gateway_settings.node1_cache_writer
            elif idx == 2:
                cache_writer = self.test_config.gateway_settings.node2_cache_writer

            with open(output_filename, 'w') as fh:
                fh.write(template.render(
                    conn_in=self.test_config.gateway_settings.conn_in,
                    conn_db=self.test_config.gateway_settings.conn_db,
                    compression=self.test_config.gateway_settings.compression,
                    bucket=self.test_config.buckets[0],
                    cache_writer=cache_writer,
                    db_master=self.cluster_spec.yield_masters().next(),
                ))

    def start_sync_gateways(self):
        self.generate_sync_gateways_config()
        self.remote.start_gateway()

    def install(self):
        self.kill_processes_gateway()
        self.uninstall_gateway()
        self.install_gateway()

        self.kill_processes_gateload()
        self.uninstall_gateload()
        self.install_gateload()

        self.start_sync_gateways()
        for idx, gateway_ip in enumerate(self.remote.gateways, start=1):
            self.request_helper.wait_for_gateway_to_start(idx, gateway_ip)
            if self.test_config.gateway_settings.logging_verbose == 'false':
                self.request_helper.turn_off_gateway_logging(gateway_ip)


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

    if not options.cluster_spec_fname or not options.test_config_fname \
            or not options.version:
        parser.error('Missing mandatory parameter')

    cluster_spec = ClusterSpec()
    cluster_spec.parse(options.cluster_spec_fname, args)

    test_config = TestConfig()
    test_config.parse(options.test_config_fname, args)

    installer = GatewayInstaller(cluster_spec, test_config, options)
    installer.install()

if __name__ == '__main__':
    main()

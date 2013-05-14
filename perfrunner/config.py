import os.path
from ConfigParser import ConfigParser, NoOptionError, NoSectionError
from operator import add

from perfrunner.logger import logger


class Config(object):

    @staticmethod
    def _read_config(fname):
        logger.info('Reading configuration file: {0}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt('File doesn\'t exist: {0}'.format(fname))
        config = ConfigParser()
        config.read(fname)
        return config


class ClusterSpec(Config):

    def parse(self, fname):
        config = self._read_config(fname)
        try:
            self.clusters = tuple(
                servers.split() for _, servers in config.items('clusters')
            )
            self.hosts = map(self.split_host_port, reduce(add, self.clusters))

            self.data_path = config.get('storage', 'data')
            self.index_path = config.get('storage', 'index')

            self.rest_username, self.rest_password = \
                config.get('credentials', 'rest').split(':')
            self.ssh_username, self.ssh_password = \
                config.get('credentials', 'ssh').split(':')
        except (NoSectionError, NoOptionError), e:
            logger.interrupt('Failed to get option from config: {0}'.format(e))

    @staticmethod
    def split_host_port(server):
        return server.split(':')[0]


class TestConfig(Config):

    def parse(self, fname):
        config = self._read_config(fname)
        try:
            self.mem_quota = config.getint('cluster', 'mem_quota')
            self.initial_nodes = config.getint('cluster', 'initial_nodes')
            self.buckets = config.getint('cluster', 'buckets')
        except (NoSectionError, NoOptionError), e:
            logger.interrupt('Failed to get option from config: {0}'.format(e))

import os.path
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError
from operator import add

from logger import logger


def safe(method):
    def wrapper(*args, **kargs):
        try:
            return method(*args, **kargs)
        except (NoSectionError, NoOptionError), e:
            logger.interrupt('Failed to get option from config: {0}'.format(e))
    return wrapper


class Config(object):

    def parse(self, fname):
        logger.info('Reading configuration file: {0}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt('File doesn\'t exist: {0}'.format(fname))
        self.config = SafeConfigParser()
        self.config.read(fname)

        basename = os.path.basename(fname)
        self.name = os.path.splitext(basename)[0]

    @safe
    def _get_options_as_dict(self, section):
        if section in self.config.sections():
            return dict((p, v) for p, v in self.config.items(section))


class ClusterSpec(Config):

    @safe
    def get_clusters(self):
        return tuple(
            servers.split() for _, servers in self.config.items('clusters')
        )

    @safe
    def get_hosts(self):
        split_host_port = lambda server: server.split(':')[0]
        return map(split_host_port, reduce(add, self.get_clusters()))

    @safe
    def get_paths(self):
        data_path = self.config.get('storage', 'data')
        index_path = self.config.get('storage', 'index')
        return data_path, index_path

    @safe
    def get_rest_credentials(self):
        return self.config.get('credentials', 'rest').split(':')

    @safe
    def get_ssh_credentials(self):
        return self.config.get('credentials', 'ssh').split(':')

    @safe
    def get_parameters(self):
        options = self._get_options_as_dict('parameters')
        return AccessSettings(options)


class TestConfig(Config):

    @safe
    def get_test_module(self):
        return self.config.get('test_case', 'module')

    @safe
    def get_test_class(self):
        return self.config.get('test_case', 'class')

    @safe
    def get_test_descr(self):
        return self.config.get('test_case', 'descr')

    @safe
    def get_mem_quota(self):
        return self.config.getint('cluster', 'mem_quota')

    @safe
    def get_initial_nodes(self):
        return self.config.getint('cluster', 'initial_nodes')

    @safe
    def get_num_buckets(self):
        return self.config.getint('cluster', 'num_buckets')

    def get_buckets(self):
        for i in xrange(self.get_num_buckets()):
            yield 'bucket-{0}'.format(i + 1)

    def get_compaction_settings(self):
        options = self._get_options_as_dict('compaction')
        return CompactionSettings(options)

    def get_load_settings(self):
        options = self._get_options_as_dict('load')
        return LoadSettings(options)

    def get_xdcr_settings(self):
        options = self._get_options_as_dict('xdcr')
        return XDCRSettings(options)

    def get_index_settings(self):
        options = self._get_options_as_dict('index')
        return IndexSettings(options)

    def get_access_settings(self):
        options = self._get_options_as_dict('access')
        return AccessSettings(options)


class CompactionSettings(object):

    DB_PERCENTAGE = 30
    VIEW_PERCENTAGE = 30
    PARALLEL = False

    def __init__(self, options):
        self.db_percentage = options.get('db_percentage', self.DB_PERCENTAGE)
        self.view_percentage = options.get('db_percentage', self.VIEW_PERCENTAGE)
        self.parallel = options.get('parallel', self.PARALLEL)


class TargetSettings(object):

    def __init__(self, host_port, bucket, username, password, prefix):
        self.username = username
        self.password = password
        self.node = host_port
        self.bucket = bucket
        self.prefix = prefix


class PhaseSettings(object):

    ITEMS = 0
    OPS = 0
    RATIO = 1.0
    SIZE = 2048
    WORKERS = 12
    WORKING_SET = 0.2


class LoadSettings(PhaseSettings):

    def __init__(self, options):
        self.items = self.ITEMS
        self.ops = int(options.get('ops', self.OPS))
        self.ratio = self.RATIO
        self.size = int(options.get('size', self.SIZE))
        self.workers = int(options.get('workers', self.WORKERS))
        self.working_set = float(options.get('working_set', self.WORKING_SET))


class XDCRSettings(PhaseSettings):

    XDCR_REPLICATION_TYPE = 'bidir'

    def __init__(self, options):
        self.replication_type = options.get('replication_type',
                                            self.XDCR_REPLICATION_TYPE)


class IndexSettings(PhaseSettings):

    VIEWS = [1]
    DISABLED_UPDATES = False

    def __init__(self, options):
        self.views = eval(options.get('views', self.VIEWS))
        self.disabled_updates = bool(options.get('disabled_updates',
                                                 self.DISABLED_UPDATES))


class AccessSettings(LoadSettings):

    def __init__(self, options):
        super(AccessSettings, self).__init__(options)
        self.items = int(options.get('items', self.ITEMS))


SF_STORAGE = {
    'host': '172.23.96.10', 'port': 8091,
    'username': 'Administrator', 'password': 'password'
}

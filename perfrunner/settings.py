import json
import os.path
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError

from decorator import decorator
from logger import logger


REPO = 'https://github.com/couchbaselabs/perfrunner'

BROKER_URL = 'amqp://couchbase:couchbase@ci.sc.couchbase.com:5672/broker'

SF_STORAGE = {'host': 'showfast.sc.couchbase.com', 'password': 'password'}

CBMONITOR_HOST = 'cbmonitor.sc.couchbase.com'


@decorator
def safe(method, *args, **kargs):
    try:
        return method(*args, **kargs)
    except (NoSectionError, NoOptionError), e:
        logger.warn('Failed to get option from config: {}'.format(e))


class Config(object):

    def __init__(self):
        self.config = SafeConfigParser()
        self.name = ''

    def parse(self, fname, override=()):
        logger.info('Reading configuration file: {}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt('File doesn\'t exist: {}'.format(fname))
        self.config.optionxform = str
        self.config.read(fname)
        for section, option, value in override:
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, option, value)

        basename = os.path.basename(fname)
        self.name = os.path.splitext(basename)[0]

    @safe
    def _get_options_as_dict(self, section):
        if section in self.config.sections():
            return {p: v for p, v in self.config.items(section)}
        else:
            return {}


class ClusterSpec(Config):

    @safe
    def yield_clusters(self):
        for cluster_name, servers in self.config.items('clusters'):
            yield cluster_name, servers.split()

    @safe
    def yield_masters(self):
        for _, servers in self.yield_clusters():
            yield servers[0]

    @safe
    def yield_servers(self):
        for _, servers in self.yield_clusters():
            for server in servers:
                yield server

    @safe
    def yield_hostnames(self):
        for _, servers in self.yield_clusters():
            for server in servers:
                yield server.split(':')[0]

    @safe
    def get_workers(self):
        return self.config.get('clients', 'hosts').split()

    @safe
    def get_client_credentials(self):
        return self.config.get('clients', 'credentials').split(':')

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

    def get_parameters(self):
        return self._get_options_as_dict('parameters')


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

    def get_regression_criterion(self):
        return self.config.get('test_case', 'larger_is_better')

    def get_level(self):
        return self.config.get('test_case', 'level')

    @safe
    def get_mem_quota(self):
        return self.config.getint('cluster', 'mem_quota')

    @safe
    def get_initial_nodes(self):
        initial_nodes = self.config.get('cluster', 'initial_nodes')
        initial_nodes = [int(_) for _ in initial_nodes.split()]
        return initial_nodes

    @safe
    def get_num_buckets(self):
        return self.config.getint('cluster', 'num_buckets')

    @safe
    def get_mrw_threads_number(self):
        return self.config.getint('cluster', 'threads_number')

    @safe
    def get_replica_number(self):
        return self.config.getint('cluster', 'replica_number')

    @safe
    def get_swt(self):
        return self.config.get('cluster', 'swt')

    @safe
    def get_num_vbuckets(self):
        return self.config.getint('cluster', 'num_vbuckets')

    @safe
    def get_group_number(self):
        return self.config.getint('cluster', 'groups')

    def get_buckets(self):
        for i in xrange(self.get_num_buckets()):
            yield 'bucket-{}'.format(i + 1)

    def get_compaction_settings(self):
        options = self._get_options_as_dict('compaction')
        return CompactionSettings(options)

    def get_watermark_settings(self):
        options = self._get_options_as_dict('watermarks')
        return options

    def get_load_settings(self):
        options = self._get_options_as_dict('load')
        return LoadSettings(options)

    def get_hot_load_settings(self):
        options = self._get_options_as_dict('hot_load')
        return HotLoadSettings(options)

    def get_xdcr_settings(self):
        options = self._get_options_as_dict('xdcr')
        return XDCRSettings(options)

    def get_index_settings(self):
        options = self._get_options_as_dict('index')
        return IndexSettings(options)

    def get_access_settings(self):
        options = self._get_options_as_dict('access')
        return AccessSettings(options)

    def get_rebalance_settings(self):
        options = self._get_options_as_dict('rebalance')
        return RebalanceSettings(options)

    def get_stats_settings(self):
        options = self._get_options_as_dict('stats')
        return StatsSettings(options)

    def get_internal_settings(self):
        return self._get_options_as_dict('internal')


class StatsSettings(object):

    ENABLED = 1
    POST_TO_SF = 0
    INTERVAL = 5
    LAT_INTERVAL = 1

    def __init__(self, options):
        self.enabled = int(options.get('enabled', self.ENABLED))
        self.post_to_sf = int(options.get('post_to_sf', self.POST_TO_SF))
        self.interval = int(options.get('interval', self.INTERVAL))
        self.lat_interval = int(options.get('lat_interval', self.LAT_INTERVAL))


class CompactionSettings(object):

    DB_PERCENTAGE = 30
    VIEW_PERCENTAGE = 30
    PARALLEL = False

    def __init__(self, options):
        self.db_percentage = options.get('db_percentage', self.DB_PERCENTAGE)
        self.view_percentage = options.get('view_percentage', self.VIEW_PERCENTAGE)
        self.parallel = options.get('parallel', self.PARALLEL)

    def __str__(self):
        return str(self.__dict__)


class TargetSettings(object):

    def __init__(self, host_port, bucket, username, password, prefix):
        self.username = username
        self.password = password
        self.node = host_port
        self.bucket = bucket
        self.prefix = prefix


class RebalanceSettings(object):

    SWAP = 0  # Don't swap by default
    START_AFTER = 1200
    STOP_AFTER = 1200

    def __init__(self, options):
        self.nodes_after = [int(_) for _ in options.get('nodes_after').split()]
        self.swap = int(options.get('swap', self.SWAP))
        self.start_after = int(options.get('start_after', self.START_AFTER))
        self.stop_after = int(options.get('stop_after', self.STOP_AFTER))


class PhaseSettings(object):

    CREATES = 0
    READS = 0
    UPDATES = 0
    DELETES = 0
    CASES = 0
    OPS = 0
    THROUGHPUT = float('inf')
    QUERY_THROUGHPUT = float('inf')

    ITEMS = 0
    SIZE = 2048
    EXPIRATION = 0
    WORKING_SET = 100
    WORKING_SET_ACCESS = 100

    WORKERS = 12
    QUERY_WORKERS = 0

    SEQ_READS = False
    SEQ_UPDATES = False

    TIME = 0

    def __init__(self, options):
        self.creates = int(options.get('creates', self.CREATES))
        self.reads = int(options.get('reads', self.READS))
        self.updates = int(options.get('updates', self.UPDATES))
        self.deletes = int(options.get('deletes', self.DELETES))
        self.cases = int(options.get('cases', self.CASES))
        self.ops = float(options.get('ops', self.OPS))
        self.throughput = float(options.get('throughput', self.THROUGHPUT))
        self.query_throughput = float(options.get('query_throughput',
                                                  self.QUERY_THROUGHPUT))

        self.size = int(options.get('size', self.SIZE))
        self.items = int(options.get('items', self.ITEMS))
        self.expiration = int(options.get('expiration', self.EXPIRATION))
        self.working_set = float(options.get('working_set', self.WORKING_SET))
        self.working_set_access = int(options.get('working_set_access',
                                                  self.WORKING_SET_ACCESS))

        self.workers = int(options.get('workers', self.WORKERS))
        self.query_workers = int(options.get('query_workers',
                                             self.QUERY_WORKERS))

        self.seq_reads = self.SEQ_READS
        self.seq_updates = self.SEQ_UPDATES

        self.time = int(options.get('time', self.TIME))

    def __str__(self):
        return str(self.__dict__)


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPDATES = True


class HotLoadSettings(PhaseSettings):

    SEQ_READS = True
    SEQ_UPDATES = True


class XDCRSettings(PhaseSettings):

    XDCR_REPLICATION_TYPE = 'bidir'
    XDCR_REPLICATION_PROTOCOL = None
    XDCR_USE_SSL = False

    def __init__(self, options):
        self.replication_type = options.get('replication_type',
                                            self.XDCR_REPLICATION_TYPE)
        self.replication_protocol = options.get('replication_protocol',
                                                self.XDCR_REPLICATION_PROTOCOL)
        self.use_ssl = int(options.get('use_ssl', self.XDCR_USE_SSL))


class IndexSettings(PhaseSettings):

    VIEWS = '[1]'
    DISABLED_UPDATES = 0
    PARAMS = '{}'

    def __init__(self, options):
        self.views = eval(options.get('views', self.VIEWS))
        self.params = eval(options.get('params', self.PARAMS))
        self.disabled_updates = int(options.get('disabled_updates',
                                                self.DISABLED_UPDATES))


class AccessSettings(PhaseSettings):

    OPS = float('inf')


class Experiment(object):

    def __init__(self, fname):
        logger.info('Reading experiment file: {}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt('File doesn\'t exist: {}'.format(fname))
        else:
            self.name = os.path.splitext(os.path.basename(fname))[0]
            with open(fname) as fh:
                self.template = json.load(fh)

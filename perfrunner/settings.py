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

    @property
    @safe
    def workers(self):
        return self.config.get('clients', 'hosts').split()

    @property
    @safe
    def gateways(self):
        return self.config.get('gateways', 'hosts').split()

    @property
    @safe
    def gateloads(self):
        return self.config.get('gateloads', 'hosts').split()

    @property
    @safe
    def client_credentials(self):
        return self.config.get('clients', 'credentials').split(':')

    @property
    @safe
    def paths(self):
        data_path = self.config.get('storage', 'data')
        index_path = self.config.get('storage', 'index')
        return data_path, index_path

    @property
    @safe
    def rest_credentials(self):
        return self.config.get('credentials', 'rest').split(':')

    @property
    @safe
    def ssh_credentials(self):
        return self.config.get('credentials', 'ssh').split(':')

    @property
    def parameters(self):
        return self._get_options_as_dict('parameters')


class TestConfig(Config):

    @property
    def test_case(self):
        options = self._get_options_as_dict('test_case')
        return TestCaseSettings(options)

    @property
    def cluster(self):
        options = self._get_options_as_dict('cluster')
        return ClusterSettings(options)

    @property
    def bucket(self):
        options = self._get_options_as_dict('bucket')
        return BucketSettings(options)

    @property
    def buckets(self):
        return [
            'bucket-{}'.format(i + 1) for i in range(self.cluster.num_buckets)
        ]

    @property
    def compaction(self):
        options = self._get_options_as_dict('compaction')
        return CompactionSettings(options)

    @property
    def watermark_settings(self):
        return self._get_options_as_dict('watermarks')

    @property
    def load_settings(self):
        options = self._get_options_as_dict('load')
        return LoadSettings(options)

    @property
    def hot_load_settings(self):
        options = self._get_options_as_dict('hot_load')
        return HotLoadSettings(options)

    @property
    def xdcr_settings(self):
        options = self._get_options_as_dict('xdcr')
        return XDCRSettings(options)

    @property
    def index_settings(self):
        options = self._get_options_as_dict('index')
        return IndexSettings(options)

    @property
    def access_settings(self):
        options = self._get_options_as_dict('access')
        return AccessSettings(options)

    @property
    def rebalance_settings(self):
        options = self._get_options_as_dict('rebalance')
        return RebalanceSettings(options)

    @property
    def stats_settings(self):
        options = self._get_options_as_dict('stats')
        return StatsSettings(options)

    @property
    def internal_settings(self):
        return self._get_options_as_dict('internal')

    @property
    def gateway_settings(self):
        options = self._get_options_as_dict('gateway')
        return GatewaySettings(options)

    @property
    def gateload_settings(self):
        options = self._get_options_as_dict('gateload')
        return GateloadSettings(options)


class TestCaseSettings(object):

    USE_WORKERS = 1
    LEVEL = 'Basic'  # depricated, alt: Advanced

    def __init__(self, options):
        self.test_module = options.get('module')
        self.test_class = options.get('class')
        self.test_summary = options.get('summary')
        self.metric_title = options.get('title')
        self.larger_is_better = options.get('larger_is_better')
        self.level = options.get('level', self.LEVEL)
        self.use_workers = int(options.get('use_workers', self.USE_WORKERS))


class ClusterSettings(object):

    NUM_BUCKETS = 1
    GROUP_NUMBER = 1
    NUM_CPUS = 0  # Use defaults
    RUN_CBQ = 0

    def __init__(self, options):
        self.mem_quota = int(options.get('mem_quota'))
        self.initial_nodes = [
            int(nodes) for nodes in options.get('initial_nodes').split()
        ]
        self.num_buckets = int(options.get('num_buckets', self.NUM_BUCKETS))
        self.num_vbuckets = options.get('num_vbuckets')
        self.group_number = int(options.get('group_number', self.GROUP_NUMBER))
        self.num_cpus = int(options.get('num_cpus', self.NUM_CPUS))
        self.disable_moxi = options.get('disable_moxi')
        self.run_cbq = options.get('run_cbq', self.RUN_CBQ)


class StatsSettings(object):

    ENABLED = 1
    POST_TO_SF = 0
    INTERVAL = 5
    LAT_INTERVAL = 1
    POST_RSS = 0

    def __init__(self, options):
        self.enabled = int(options.get('enabled', self.ENABLED))
        self.post_to_sf = int(options.get('post_to_sf', self.POST_TO_SF))
        self.interval = int(options.get('interval', self.INTERVAL))
        self.lat_interval = int(options.get('lat_interval', self.LAT_INTERVAL))
        self.post_rss = int(options.get('post_rss', self.POST_RSS))


class BucketSettings(object):

    PASSWORD = 'password'
    MAX_NUM_SHARDS = 0
    MAX_THREADS = 0
    REPLICA_NUMBER = 1
    REPLICA_INDEX = 0
    EVICTION_POLICY = 'valueOnly'  # alt: fullEviction

    def __init__(self, options):
        self.password = options.get('password', self.PASSWORD)
        self.max_num_shards = int(
            options.get('max_num_shards', self.MAX_NUM_SHARDS)
        )
        self.max_threads = int(
            options.get('max_threads', self.MAX_THREADS)
        )
        self.replica_number = int(
            options.get('replica_number', self.REPLICA_NUMBER)
        )
        self.replica_index = int(
            options.get('replica_index', self.REPLICA_INDEX)
        )
        self.eviction_policy = \
            options.get('eviction_policy', self.EVICTION_POLICY)

        self.threads_number = options.get('threads_number')  # 2.x


class CompactionSettings(object):

    DB_PERCENTAGE = 30
    VIEW_PERCENTAGE = 30
    PARALLEL = True

    def __init__(self, options):
        self.db_percentage = options.get('db_percentage', self.DB_PERCENTAGE)
        self.view_percentage = options.get('view_percentage', self.VIEW_PERCENTAGE)
        self.parallel = options.get('parallel', self.PARALLEL)

    def __str__(self):
        return str(self.__dict__)


class TargetSettings(object):

    def __init__(self, host_port, bucket, password, prefix):
        self.password = password
        self.node = host_port
        self.bucket = bucket
        self.prefix = prefix


class RebalanceSettings(object):

    SWAP = 0  # Don't swap by default
    FAILOVER = 0  # No failover by default
    GRACEFUL_FAILOVER = 0
    DELTA_RECOVERY = 0  # Full recovery by default
    SLEEP_AFTER_FAILOVER = 600
    START_AFTER = 1200
    STOP_AFTER = 1200

    def __init__(self, options):
        self.nodes_after = [int(_) for _ in options.get('nodes_after').split()]
        self.swap = int(options.get('swap', self.SWAP))
        self.failover = int(options.get('failover', self.FAILOVER))
        self.graceful_failover = int(options.get('graceful_failover',
                                                 self.GRACEFUL_FAILOVER))
        self.sleep_after_failover = int(options.get('sleep_after_failover',
                                                    self.SLEEP_AFTER_FAILOVER))
        self.delta_recovery = int(options.get('delta_recovery',
                                              self.DELTA_RECOVERY))
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

    DOC_GEN = 'old'
    ITEMS = 0
    SIZE = 2048
    EXPIRATION = 0
    WORKING_SET = 100
    WORKING_SET_ACCESS = 100

    WORKERS = 12
    QUERY_WORKERS = 0

    SEQ_READS = False
    SEQ_UPDATES = False

    TIME = 3600 * 24

    ASYNC = False

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

        self.doc_gen = options.get('doc_gen', self.DOC_GEN)
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

        self.async = bool(int(options.get('async', self.ASYNC)))

    def __str__(self):
        return str(self.__dict__)


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPDATES = True


class HotLoadSettings(PhaseSettings):

    SEQ_READS = True
    SEQ_UPDATES = False


class XDCRSettings(PhaseSettings):

    XDCR_REPLICATION_TYPE = 'bidir'
    XDCR_REPLICATION_PROTOCOL = None
    XDCR_USE_SSL = False
    WAN_ENABLED = False

    def __init__(self, options):
        self.replication_type = options.get('replication_type',
                                            self.XDCR_REPLICATION_TYPE)
        self.replication_protocol = options.get('replication_protocol',
                                                self.XDCR_REPLICATION_PROTOCOL)
        self.use_ssl = int(options.get('use_ssl', self.XDCR_USE_SSL))
        self.wan_enabled = int(options.get('wan_enabled', self.WAN_ENABLED))


class IndexSettings(PhaseSettings):

    VIEWS = '[1]'
    DISABLED_UPDATES = 0
    PARAMS = '{}'

    def __init__(self, options):
        self.views = eval(options.get('views', self.VIEWS))
        self.params = eval(options.get('params', self.PARAMS))
        self.disabled_updates = int(options.get('disabled_updates',
                                                self.DISABLED_UPDATES))
        self.index_type = options.get('index_type')


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


class GatewaySettings(PhaseSettings):

    COMPRESSION = 1
    CONN_IN = 0
    CONN_DB = 16

    def __init__(self, options):
        self.conn_in = int(options.get('conn_in', self.CONN_IN))
        self.conn_db = int(options.get('conn_db', self.CONN_DB))
        self.compression = int(options.get('compression', self.COMPRESSION))


class GateloadSettings(PhaseSettings):

    PULLER = 3500
    PUSHER = 1500

    def __init__(self, options):
        self.pullers = int(options.get('pullers', self.PULLER))
        self.pushers = int(options.get('pushers', self.PUSHER))

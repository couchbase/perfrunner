import csv
import json
import os.path
from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError

from decorator import decorator
from logger import logger

from perfrunner.helpers.misc import uhex


REPO = 'https://github.com/couchbase/perfrunner'


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

    def parse(self, fname, override):
        if override:
            override = [x for x in csv.reader(
                ' '.join(override).split(','), delimiter='.')]

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
            yield cluster_name, [s.split(',', 1)[0] for s in servers.split()]

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
    def yield_servers_by_role(self, role):
        for name, servers in self.config.items('clusters'):
            has_service = []
            for server in servers.split():
                if role in server.split(',')[1:]:
                    has_service.append(server.split(',')[0])
            yield name, has_service

    @property
    @safe
    def roles(self):
        server_roles = {}
        for _, node in self.config.items('clusters'):
            for server in node.split():
                name = server.split(',', 1)[0]
                if ',' in server:
                    server_roles[name] = server.split(',', 1)[1]
                else:  # For backward compatibility, set to kv if not specified
                    server_roles[name] = 'kv'

        return server_roles

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
    def emptybuckets(self):
        return [
            'bucket-{}'.format(i + 1) for i in range(self.cluster.num_buckets,
                                                     self.cluster.num_buckets +
                                                     self.cluster.emptybuckets)
        ]

    @property
    def max_buckets(self):
        return [
            'bucket-{}'.format(i + 1) for i in range(self.cluster.max_num_buckets)
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
        hot_load = HotLoadSettings(options)

        load = self.load_settings
        hot_load.doc_gen = load.doc_gen
        hot_load.doc_partitions = load.doc_partitions
        hot_load.size = load.size
        return hot_load

    @property
    def xdcr_settings(self):
        options = self._get_options_as_dict('xdcr')
        return XDCRSettings(options)

    @property
    def index_settings(self):
        options = self._get_options_as_dict('index')
        return IndexSettings(options)

    @property
    def spatial_settings(self):
        options = self._get_options_as_dict('spatial')
        return SpatialSettings(options)

    @property
    def secondaryindex_settings(self):
        options = self._get_options_as_dict('secondary')
        return SecondaryIndexSettings(options)

    @property
    def n1ql_settings(self):
        options = self._get_options_as_dict('n1ql')
        return N1QLSettings(options)

    @property
    def access_settings(self):
        options = self._get_options_as_dict('access')
        access = AccessSettings(options)
        access.resolve_subcategories(self)

        load = self.load_settings
        access.doc_gen = load.doc_gen
        access.doc_partitions = load.doc_partitions
        access.size = load.size
        return access

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

    @property
    def worker_settings(self):
        options = self._get_options_as_dict('worker_settings')
        return WorkerSettings(options)

    def get_n1ql_query_definition(self, query_name):
        return self._get_options_as_dict('n1ql-{}'.format(query_name))


class TestCaseSettings(object):

    USE_WORKERS = 1
    LEVEL = 'Basic'  # depricated, alt: Advanced

    def __init__(self, options):
        self.test_module = '.'.join(options.get('test').split('.')[:-1])
        self.test_class = options.get('test').split('.')[-1]
        self.test_summary = options.get('summary')
        self.metric_title = options.get('title')
        self.larger_is_better = options.get('larger_is_better')
        self.monitor_clients = options.get('monitor_clients', False)
        self.level = options.get('level', self.LEVEL)
        self.use_workers = int(options.get('use_workers', self.USE_WORKERS))
        self.use_backup_wrapper = options.get('use_backup_wrapper', False)


class ClusterSettings(object):

    NUM_BUCKETS = 1
    NUM_EMPTYBUCKETS = 0
    MIN_NUM_BUCKETS = 1
    MAX_NUM_BUCKETS = 10
    INCR_NUM_BUCKETS = 1
    GROUP_NUMBER = 1
    NUM_CPUS = 0  # Use defaults
    RUN_CBQ = 0
    SFWI = 0
    TCMALLOC_AGGRESSIVE_DECOMMIT = 0
    INDEX_MEM_QUOTA = 256

    def __init__(self, options):
        self.mem_quota = int(options.get('mem_quota'))
        self.index_mem_quota = int(options.get('index_mem_quota', self.INDEX_MEM_QUOTA))
        self.initial_nodes = [
            int(nodes) for nodes in options.get('initial_nodes').split()
        ]
        self.num_buckets = int(options.get('num_buckets', self.NUM_BUCKETS))
        self.emptybuckets = int(options.get('emptybuckets', self.NUM_EMPTYBUCKETS))
        self.min_num_buckets = int(options.get('min_num_buckets',
                                               self.MIN_NUM_BUCKETS))
        self.max_num_buckets = int(options.get('max_num_buckets',
                                               self.MAX_NUM_BUCKETS))
        self.incr_num_buckets = int(options.get('incr_num_buckets',
                                                self.INCR_NUM_BUCKETS))
        self.num_vbuckets = options.get('num_vbuckets')
        self.group_number = int(options.get('group_number', self.GROUP_NUMBER))
        self.num_cpus = int(options.get('num_cpus', self.NUM_CPUS))
        self.disable_moxi = options.get('disable_moxi')
        self.run_cbq = options.get('run_cbq', self.RUN_CBQ)
        self.sfwi = options.get('sfwi', self.SFWI)
        self.tcmalloc_aggressive_decommit = options.get('tcmalloc_aggressive_decommit',
                                                        self.TCMALLOC_AGGRESSIVE_DECOMMIT)


class StatsSettings(object):

    CBMONITOR = {'host': 'cbmonitor.sc.couchbase.com', 'password': 'password'}
    ENABLED = 1
    POST_TO_SF = 0
    INTERVAL = 5
    SECONDARY_STATSFILE = '/root/statsfile'
    LAT_INTERVAL = 1
    POST_RSS = 0
    POST_CPU = 0
    SERIESLY = {'host': 'cbmonitor.sc.couchbase.com'}
    SHOWFAST = {'host': 'showfast.sc.couchbase.com', 'password': 'password'}

    def __init__(self, options):
        self.cbmonitor = {'host': options.get('cbmonitor_host',
                                              self.CBMONITOR['host']),
                          'password': options.get('cbmonitor_password',
                                                  self.CBMONITOR['password'])}
        self.enabled = int(options.get('enabled', self.ENABLED))
        self.post_to_sf = int(options.get('post_to_sf', self.POST_TO_SF))
        self.interval = int(options.get('interval', self.INTERVAL))
        self.lat_interval = int(options.get('lat_interval', self.LAT_INTERVAL))
        self.secondary_statsfile = options.get('secondary_statsfile', self.SECONDARY_STATSFILE)
        self.post_rss = int(options.get('post_rss', self.POST_RSS))
        self.post_cpu = int(options.get('post_cpu', self.POST_CPU))
        self.seriesly = {'host': options.get('seriesly_host',
                                             self.SERIESLY['host'])}
        self.showfast = {'host': options.get('showfast_host',
                                             self.SHOWFAST['host']),
                         'password': options.get('showfast_password',
                                                 self.SHOWFAST['password'])}


class BucketSettings(object):

    PASSWORD = 'password'
    MAX_NUM_SHARDS = -1
    MAX_THREADS = -1
    WARMUP_MIN_MEMORY_THRESHOLD = -1
    REPLICA_NUMBER = 1
    REPLICA_INDEX = 0
    EVICTION_POLICY = 'valueOnly'  # alt: fullEviction
    EXPIRY_PAGER_SLEEP_TIME = -1
    DEFRAGMENTER_ENABLED = -1
    HT_LOCKS = -1
    BFILTER_ENABLED = None

    def __init__(self, options):
        self.password = options.get('password', self.PASSWORD)
        self.max_num_shards = int(
            options.get('max_num_shards', self.MAX_NUM_SHARDS)
        )
        self.max_threads = int(
            options.get('max_threads', self.MAX_THREADS)
        )
        self.warmup_min_memory_threshold = int(
            options.get('warmup_min_memory_threshold',
                        self.WARMUP_MIN_MEMORY_THRESHOLD)
        )
        self.replica_number = int(
            options.get('replica_number', self.REPLICA_NUMBER)
        )
        self.replica_index = int(
            options.get('replica_index', self.REPLICA_INDEX)
        )
        self.eviction_policy = \
            options.get('eviction_policy', self.EVICTION_POLICY)

        self.defragmenter_enabled = options.get('defragmenter_enabled',
                                                self.DEFRAGMENTER_ENABLED)

        self.threads_number = options.get('threads_number')  # 2.x

        self.exp_pager_stime = int(options.get('exp_pager_stime',
                                               self.EXPIRY_PAGER_SLEEP_TIME))
        self.ht_locks = int(options.get('ht_locks', self.HT_LOCKS))
        self.bfilter_enabled = options.get('bfilter_enabled', self.BFILTER_ENABLED)


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
    N1QL_THROUGHPUT = float('inf')

    DOC_GEN = 'old'
    DOC_PARTITIONS = 1

    ITEMS = 0
    SIZE = 2048
    EXPIRATION = 0
    WORKING_SET = 100
    WORKING_SET_ACCESS = 100

    WORKERS = 12
    QUERY_WORKERS = 0
    N1QL_WORKERS = 0
    N1QL_OP = 'read'
    DCP_WORKERS = 0

    SEQ_READS = False
    SEQ_UPDATES = False

    TIME = 3600 * 24

    ASYNC = False

    ITERATIONS = 1

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
        self.n1ql_throughput = float(options.get('n1ql_throughput',
                                                 self.N1QL_THROUGHPUT))

        self.doc_gen = options.get('doc_gen', self.DOC_GEN)
        self.doc_partitions = int(options.get('doc_partitions',
                                              self.DOC_PARTITIONS))
        self.size = int(options.get('size', self.SIZE))
        self.items = int(options.get('items', self.ITEMS))
        self.expiration = int(options.get('expiration', self.EXPIRATION))
        self.working_set = float(options.get('working_set', self.WORKING_SET))
        self.working_set_access = int(options.get('working_set_access',
                                                  self.WORKING_SET_ACCESS))

        self.workers = int(options.get('workers', self.WORKERS))
        self.query_workers = int(options.get('query_workers',
                                             self.QUERY_WORKERS))
        self.n1ql_workers = int(options.get('n1ql_workers',
                                            self.N1QL_WORKERS))
        self.n1ql_op = options.get('n1ql_op', self.N1QL_OP)
        self.dcp_workers = int(options.get('dcp_workers', self.DCP_WORKERS))

        self.n1ql_queries = []
        if 'n1ql_queries' in options:
            self.n1ql_queries = options.get('n1ql_queries').strip().split(',')

        self.seq_reads = self.SEQ_READS
        self.seq_updates = self.SEQ_UPDATES

        self.ddocs = None
        self.index_type = None
        self.qparams = {}

        self.n1ql = None
        self.time = int(options.get('time', self.TIME))

        self.async = bool(int(options.get('async', self.ASYNC)))

        self.iterations = int(options.get('iterations', self.ITERATIONS))

        self.filename = None

    def resolve_subcategories(self, config):
        subcategories = self.n1ql_queries
        query_specs = []
        for subcategory in subcategories:
            query_specs.append(config.get_n1ql_query_definition(subcategory))
        self.n1ql_queries = query_specs

    def __str__(self):
        return str(self.__dict__)


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPDATES = True


class HotLoadSettings(PhaseSettings):

    SEQ_READS = True
    SEQ_UPDATES = False

    def __init__(self, options):
        if 'size' in options:
            logger.interrupt(
                "The document `size` may only be set in the [load] "
                "and not in the [hot_load] section")

        super(HotLoadSettings, self).__init__(options)


class XDCRSettings(object):

    XDCR_REPLICATION_TYPE = 'bidir'
    XDCR_REPLICATION_PROTOCOL = None
    XDCR_USE_SSL = False
    WAN_ENABLED = False
    FILTER_EXPRESSION = None

    def __init__(self, options):
        self.replication_type = options.get('replication_type',
                                            self.XDCR_REPLICATION_TYPE)
        self.replication_protocol = options.get('replication_protocol',
                                                self.XDCR_REPLICATION_PROTOCOL)
        self.use_ssl = int(options.get('use_ssl', self.XDCR_USE_SSL))
        self.wan_enabled = int(options.get('wan_enabled', self.WAN_ENABLED))
        self.filter_expression = options.get('filter_expression', self.FILTER_EXPRESSION)

    def __str__(self):
        return str(self.__dict__)


class IndexSettings(object):

    VIEWS = '[1]'
    DISABLED_UPDATES = 0
    PARAMS = '{}'

    def __init__(self, options):
        self.views = eval(options.get('views', self.VIEWS))
        self.params = eval(options.get('params', self.PARAMS))
        self.disabled_updates = int(options.get('disabled_updates',
                                                self.DISABLED_UPDATES))
        self.index_type = options.get('index_type')

    def __str__(self):
        return str(self.__dict__)


class SpatialSettings(object):

    def __init__(self, options):
        if not options:
            return
        self.indexes = []
        if 'indexes' in options:
            self.indexes = options.get('indexes').strip().split('\n')
        self.disabled_updates = int(options.get('disabled_updates', 0))
        self.dimensionality = int(options.get('dimensionality', 0))
        self.data = options.get('data', None)
        if 'view_names' in options:
            self.view_names = options.get('view_names').strip().split('\n')
        self.queries = options.get('queries', None)
        self.workers = int(options.get('workers', 0))
        self.throughput = float(options.get('throughput', float('inf')))
        self.params = json.loads(options.get('params', "{}"))

    def __str__(self):
        return str(self.__dict__)


class SecondaryIndexSettings(object):

    NAME = 'noname'
    FIELD = 'nofield'
    DB = ''
    STALE = 'true'

    def __init__(self, options):
        self.name = str(options.get('name', self.NAME))
        self.field = str(options.get('field', self.FIELD))
        self.db = str(options.get('db', self.DB))
        self.stale = str(options.get('stale', self.STALE))
        for name in self.name.split(","):
            index_partition_name = "index_{}_partitions".format(name)
            val = str(options.get(index_partition_name, ''))
            if val:
                setattr(self, index_partition_name, val)

        self.settings = {
            'indexer.settings.inmemory_snapshot.interval': 200,
            'indexer.settings.log_level': 'info',
            'indexer.settings.max_cpu_percent': 2400,
            'indexer.settings.persisted_snapshot.interval': 5000,
            'indexer.settings.scan_timeout': 0,
            'projector.settings.log_level': 'info'
        }

        for option in options:
            if option.startswith('indexer.settings') or \
               option.startswith('projector.settings') or \
               option.startswith('queryport.client.settings'):

                value = options.get(option)
                try:
                    if '.' in value:
                        self.settings[option] = float(value)
                    else:
                        self.settings[option] = int(value)
                    continue
                except:
                    pass

                self.settings[option] = value

    def __str__(self):
        return str(self.__dict__)


class N1QLSettings(object):

    def __init__(self, options):
        self.indexes = []
        if 'indexes' in options:
            self.indexes = options.get('indexes').strip().split('\n')

        self.settings = {}
        for option in options:
            if option.startswith('query.settings'):
                key = option.split('.')[2]
                value = options.get(option)
                try:
                    if '.' in value:
                        self.settings[key] = float(value)
                    else:
                        self.settings[key] = int(value)
                    continue
                except:
                    pass

                self.settings[key] = value

    def __str__(self):
        return str(self.__dict__)


class AccessSettings(PhaseSettings):

    OPS = float('inf')

    def __init__(self, options):
        if 'size' in options:
            logger.interrupt(
                "The document `size` may only be set in the [load] "
                "and not in the [access] section")

        super(AccessSettings, self).__init__(options)


class Experiment(object):

    def __init__(self, fname):
        logger.info('Reading experiment file: {}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt('File doesn\'t exist: {}'.format(fname))
        else:
            self.name = os.path.splitext(os.path.basename(fname))[0]
            with open(fname) as fh:
                self.template = json.load(fh)


class GatewaySettings(object):

    COMPRESSION = 'true'
    CONN_IN = 0
    CONN_DB = 16
    NUM_NODES = 1
    LOGGING_VERBOSE = 'false'
    SHADOW = 'false'

    # allow customization of the GODEBUG environment variable
    # see http://golang.org/pkg/runtime/
    GO_DEBUG = ''

    # the only allowed urls are git.io urls, ie: http://git.io/b9PK, and only the
    # the last part should be passed, not the full url.  So to tell it it find the
    # config at http://git.io/b9PK, use gateway.config_url.b9PK
    CONFIG_URL = ''

    def __init__(self, options):
        self.conn_in = int(options.get('conn_in', self.CONN_IN))
        self.conn_db = int(options.get('conn_db', self.CONN_DB))
        self.compression = options.get('compression', self.COMPRESSION)
        self.num_nodes = int(options.get('num_nodes', self.NUM_NODES))
        self.logging_verbose = options.get('logging_verbose', self.LOGGING_VERBOSE)
        self.shadow = options.get('shadow', self.SHADOW)
        self.config_url = options.get('config_url', self.CONFIG_URL)
        self.go_debug = options.get('go_debug', self.GO_DEBUG)
        self.node0_cache_writer = options.get('node0_cache_writer', 'false')
        self.node1_cache_writer = options.get('node1_cache_writer', 'false')
        self.node2_cache_writer = options.get('node2_cache_writer', 'false')

    def __str__(self):
        return str(self.__dict__)


class GateloadSettings(object):

    PULLER = 3500
    PUSHER = 1500
    DOC_SIZE = 0
    SEND_ATTACHMENT = 'false'
    CHANNEL_ACTIVE_USERS = 40
    CHANNEL_CONCURRENT_USERS = 40
    SLEEP_TIME = 10  # In seconds, 10 seconds
    RUN_TIME = 3600  # In seconds.  1 hr
    RAMPUP_INTERVAL = 900  # In seconds, 15 minutes
    P95_AVG_CRITERIA = 3
    P99_AVG_CRITERIA = 5
    SERIESLY_HOST = '172.23.106.228'
    LOGGING_VERBOSE = 'false'
    AUTH_TYPE = 'basic'
    PASSWORD = ''

    def __init__(self, options):
        self.pullers = int(options.get('pullers', self.PULLER))
        self.pushers = int(options.get('pushers', self.PUSHER))
        self.doc_size = int(options.get('doc_size', self.DOC_SIZE))
        self.send_attachment = options.get('send_attachment', self.SEND_ATTACHMENT)
        self.channel_active_users = int(options.get('channel_active_users',
                                                    self.CHANNEL_ACTIVE_USERS))
        self.channel_concurrent_users = int(options.get('channel_concurrent_users',
                                                        self.CHANNEL_CONCURRENT_USERS))
        self.sleep_time = int(options.get('sleep_time', self.SLEEP_TIME))
        self.p95_avg_criteria = int(options.get('p95_avg_criteria',
                                                self.P95_AVG_CRITERIA))
        self.p99_avg_criteria = int(options.get('p99_avg_criteria',
                                                self.P99_AVG_CRITERIA))
        self.run_time = int(options.get('run_time', self.RUN_TIME))
        self.rampup_interval = int(options.get('rampup_interval',
                                               self.RAMPUP_INTERVAL))
        self.logging_verbose = options.get('logging_verbose', self.LOGGING_VERBOSE)
        self.seriesly_host = options.get('seriesly_host', self.SERIESLY_HOST)
        self.auth_type = options.get('auth_type', self.AUTH_TYPE)
        self.password = options.get('password', self.PASSWORD)

    def __str__(self):
        return str(self.__dict__)


class WorkerSettings(object):

    REUSE_WORKSPACE = 'false'
    WORKSPACE_DIR = '/tmp/{}'.format(uhex()[:12])

    def __init__(self, options):
        self.reuse_worker = options.get('reuse_workspace', self.REUSE_WORKSPACE)
        self.worker_dir = options.get('workspace_location', self.WORKSPACE_DIR)

    def __str__(self):
        return str(self.__dict__)

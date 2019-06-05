import csv
import os.path
from configparser import ConfigParser, NoOptionError, NoSectionError
from typing import Dict, Iterator, List, Tuple

from decorator import decorator

from logger import logger
from perfrunner.helpers.misc import target_hash

REPO = 'https://github.com/couchbase/perfrunner'
BRANCH = 'syncgateway'


@decorator
def safe(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except (NoSectionError, NoOptionError) as e:
        logger.warn('Failed to get option from config: {}'.format(e))


class Config:

    def __init__(self):
        self.config = ConfigParser()
        self.name = ''

    def parse(self, fname: str, override=None) -> None:
        logger.info('Reading configuration file: {}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt("File doesn't exist: {}".format(fname))
        self.config.optionxform = str
        self.config.read(fname)

        basename = os.path.basename(fname)
        self.name = os.path.splitext(basename)[0]

        if override is not None:
            self.override(override)

    def override(self, override: List[str]):
        override = [x for x in csv.reader(override, delimiter='.')]

        for section, option, value in override:
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, option, value)

    @safe
    def _get_options_as_dict(self, section: str) -> dict:
        if section in self.config.sections():
            return {p: v for p, v in self.config.items(section)}
        else:
            return {}


class ClusterSpec(Config):

    @property
    def clusters(self) -> Iterator:
        for cluster_name, servers in self.config.items('clusters'):
            hosts = [s.split(':')[0] for s in servers.split()]
            yield cluster_name, hosts

    @property
    def masters(self) -> Iterator[str]:
        for _, servers in self.clusters:
            yield servers[0]

    @property
    def servers(self) -> List[str]:
        servers = []
        for _, cluster_servers in self.clusters:
            for server in cluster_servers:
                servers.append(server)
        return servers

    def servers_by_role(self, role: str) -> List[str]:
        has_service = []
        for _, servers in self.config.items('clusters'):
            for server in servers.split():
                host, roles = server.split(':')
                if role in roles:
                    has_service.append(host)
        return has_service

    @property
    def roles(self) -> Dict[str, str]:
        server_roles = {}
        for _, servers in self.config.items('clusters'):
            for server in servers.split():
                host, roles = server.split(':')
                server_roles[host] = roles
        return server_roles

    @property
    def workers(self) -> List[str]:
        return self.config.get('clients', 'hosts').split()

    @property
    def client_credentials(self) -> List[str]:
        return self.config.get('clients', 'credentials').split(':')

    @property
    def paths(self) -> Tuple[str, str]:
        data_path = self.config.get('storage', 'data')
        index_path = self.config.get('storage', 'index')
        return data_path, index_path

    @property
    @safe
    def backup(self) -> str:
        return self.config.get('storage', 'backup')

    @property
    def rest_credentials(self) -> List[str]:
        return self.config.get('credentials', 'rest').split(':')

    @property
    def ssh_credentials(self) -> List[str]:
        return self.config.get('credentials', 'ssh').split(':')

    @property
    def parameters(self) -> dict:
        return self._get_options_as_dict('parameters')


class TestCaseSettings:

    THRESHOLD = -10

    USE_WORKERS = 1

    def __init__(self, options: dict):
        self.test_module = '.'.join(options.get('test').split('.')[:-1])
        self.test_class = options.get('test').split('.')[-1]

        self.title = options.get('title')
        self.component = options.get('component', '')
        self.category = options.get('category', '')
        self.sub_category = options.get('sub_category', '')

        self.threshold = int(options.get("threshold", self.THRESHOLD))

        self.use_workers = int(options.get('use_workers', self.USE_WORKERS))


class ClusterSettings:

    NUM_BUCKETS = 1

    INDEX_MEM_QUOTA = 256
    FTS_INDEX_MEM_QUOTA = 512
    ANALYTICS_MEM_QUOTA = 0
    ANALYTICS_LOG_LEVEL = "WARNING"

    EVENTING_BUCKET_MEM_QUOTA = 0
    EVENTING_BUCKETS = 0

    KERNEL_MEM_LIMIT = 0
    ONLINE_CORES = 0

    def __init__(self, options: dict):
        self.mem_quota = int(options.get('mem_quota'))
        self.index_mem_quota = int(options.get('index_mem_quota',
                                               self.INDEX_MEM_QUOTA))
        self.fts_index_mem_quota = int(options.get('fts_index_mem_quota',
                                                   self.FTS_INDEX_MEM_QUOTA))
        self.analytics_mem_quota = int(options.get('analytics_mem_quota',
                                                   self.ANALYTICS_MEM_QUOTA))
        self.analytics_log_level = options.get('analytics_log_level',
                                               self.ANALYTICS_LOG_LEVEL)
        self.initial_nodes = [
            int(nodes) for nodes in options.get('initial_nodes').split()
        ]
        self.num_buckets = int(options.get('num_buckets',
                                           self.NUM_BUCKETS))
        self.eventing_bucket_mem_quota = int(options.get('eventing_bucket_mem_quota',
                                                         self.EVENTING_BUCKET_MEM_QUOTA))
        self.eventing_buckets = int(options.get('eventing_buckets',
                                                self.EVENTING_BUCKETS))
        self.num_vbuckets = options.get('num_vbuckets')
        self.online_cores = int(options.get('online_cores',
                                            self.ONLINE_CORES))
        self.kernel_mem_limit = options.get('kernel_mem_limit',
                                            self.KERNEL_MEM_LIMIT)


class StatsSettings:

    ENABLED = 1
    POST_TO_SF = 0

    INTERVAL = 5
    LAT_INTERVAL = 1

    POST_CPU = 0

    SECONDARY_STATSFILE = '/root/statsfile'

    CBMONITOR = 'cbmonitor.sc.couchbase.com'
    SHOWFAST = 'showfast.sc.couchbase.com'

    CLIENT_PROCESSES = []
    SERVER_PROCESSES = ['beam.smp',
                        'cbft',
                        'cbq-engine',
                        'indexer',
                        'memcached',
                        'sync_gateway']
    TRACED_PROCESSES = []

    def __init__(self, options: dict):
        self.enabled = int(options.get('enabled', self.ENABLED))
        self.post_to_sf = int(options.get('post_to_sf', self.POST_TO_SF))

        self.interval = int(options.get('interval', self.INTERVAL))
        self.lat_interval = float(options.get('lat_interval',
                                              self.LAT_INTERVAL))

        self.post_cpu = int(options.get('post_cpu', self.POST_CPU))

        self.secondary_statsfile = options.get('secondary_statsfile',
                                               self.SECONDARY_STATSFILE)

        self.client_processes = self.CLIENT_PROCESSES + \
            options.get('client_processes', '').split()
        self.server_processes = self.SERVER_PROCESSES + \
            options.get('server_processes', '').split()
        self.traced_processes = self.TRACED_PROCESSES + \
            options.get('traced_processes', '').split()


class ProfilingSettings:

    INTERVAL = 300  # 5 minutes

    NUM_PROFILES = 1

    PROFILES = 'cpu'

    SERVICES = ''

    CPU_INTERVAL = 10  # 10 seconds

    def __init__(self, options: dict):
        self.services = options.get('services',
                                    self.SERVICES).split()
        self.interval = int(options.get('interval',
                                        self.INTERVAL))
        self.num_profiles = int(options.get('num_profiles',
                                            self.NUM_PROFILES))
        self.profiles = options.get('profiles',
                                    self.PROFILES).split(',')
        self.cpu_interval = int(options.get('cpu_interval', self.CPU_INTERVAL))


class BucketSettings:

    PASSWORD = 'password'
    REPLICA_NUMBER = 1
    REPLICA_INDEX = 0
    EVICTION_POLICY = 'valueOnly'  # alt: fullEviction
    BUCKET_TYPE = 'membase'  # alt: ephemeral

    def __init__(self, options: dict):
        self.password = options.get('password', self.PASSWORD)
        self.replica_number = int(
            options.get('replica_number', self.REPLICA_NUMBER)
        )
        self.replica_index = int(
            options.get('replica_index', self.REPLICA_INDEX)
        )
        self.eviction_policy = options.get('eviction_policy',
                                           self.EVICTION_POLICY)
        self.bucket_type = options.get('bucket_type',
                                       self.BUCKET_TYPE)

        self.conflict_resolution_type = options.get('conflict_resolution_type')


class CompactionSettings:

    DB_PERCENTAGE = 30
    VIEW_PERCENTAGE = 30
    PARALLEL = True

    def __init__(self, options: dict):
        self.db_percentage = options.get('db_percentage',
                                         self.DB_PERCENTAGE)
        self.view_percentage = options.get('view_percentage',
                                           self.VIEW_PERCENTAGE)
        self.parallel = options.get('parallel', self.PARALLEL)

    def __str__(self):
        return str(self.__dict__)


class RebalanceSettings:

    SWAP = 0
    FAILOVER = 'hard'  # Atl: graceful
    DELTA_RECOVERY = 0  # Full recovery by default
    DELAY_BEFORE_FAILOVER = 600
    START_AFTER = 1200
    STOP_AFTER = 1200

    def __init__(self, options: dict):
        nodes_after = options.get('nodes_after', '').split()
        self.nodes_after = [int(num_nodes) for num_nodes in nodes_after]

        self.swap = int(options.get('swap', self.SWAP))

        self.failed_nodes = int(options.get('failed_nodes', 1))
        self.failover = options.get('failover', self.FAILOVER)
        self.delay_before_failover = int(options.get('delay_before_failover',
                                                     self.DELAY_BEFORE_FAILOVER))
        self.delta_recovery = int(options.get('delta_recovery',
                                              self.DELTA_RECOVERY))

        self.start_after = int(options.get('start_after', self.START_AFTER))
        self.stop_after = int(options.get('stop_after', self.STOP_AFTER))


class PhaseSettings:

    TIME = 3600 * 24

    USE_SSL = 0

    DOC_GEN = 'basic'

    CREATES = 0
    READS = 0
    UPDATES = 0
    DELETES = 0
    FTS_UPDATES = 0

    OPS = 0

    HOT_READS = False
    SEQ_UPSERTS = False
    RAND_UPSERTS = False

    ITERATIONS = 1

    ASYNC = False
    HASH_KEYS = 0
    KEY_LENGTH = 0  # max can be 32

    ITEMS = 0
    EXISTING_ITEMS = 0
    SIZE = 2048

    TARGET = 0

    WORKING_SET = 100
    WORKING_SET_ACCESS = 100
    WORKING_SET_MOVE_TIME = 0
    WORKING_SET_MOVE_DOCS = 0

    THROUGHPUT = float('inf')
    QUERY_THROUGHPUT = float('inf')
    N1QL_THROUGHPUT = float('inf')

    VIEW_QUERY_PARAMS = '{}'

    WORKERS = 0
    QUERY_WORKERS = 0
    N1QL_WORKERS = 0
    WORKER_INSTANCES = 1

    N1QL_OP = 'read'
    N1QL_BATCH_SIZE = 100

    ARRAY_SIZE = 10
    NUM_CATEGORIES = 10 ** 6
    NUM_REPLIES = 100
    RANGE_DISTANCE = 10

    ITEM_SIZE = 64
    SIZE_VARIATION_MIN = 1
    SIZE_VARIATION_MAX = 1024

    RECORDED_LOAD_CACHE_SIZE = 0
    INSERTS_PER_WORKERINSTANCE = 0

    EPOLL = 'true'
    BOOST = 48

    def __init__(self, options: dict):
        # Common settings
        self.time = int(options.get('time', self.TIME))
        self.use_ssl = bool(int(options.get('use_ssl', self.USE_SSL)))

        # KV settings
        self.doc_gen = options.get('doc_gen', self.DOC_GEN)

        self.size = int(options.get('size', self.SIZE))
        self.items = int(options.get('items', self.ITEMS))

        self.creates = int(options.get('creates', self.CREATES))
        self.reads = int(options.get('reads', self.READS))
        self.updates = int(options.get('updates', self.UPDATES))
        self.deletes = int(options.get('deletes', self.DELETES))
        self.fts_updates_swap = int(options.get('fts_updates_swap',
                                                self.FTS_UPDATES))
        self.fts_updates_reverse = int(options.get('fts_updates_reverse',
                                                   self.FTS_UPDATES))

        self.ops = float(options.get('ops', self.OPS))
        self.throughput = float(options.get('throughput', self.THROUGHPUT))

        self.working_set = float(options.get('working_set', self.WORKING_SET))
        self.working_set_access = int(options.get('working_set_access',
                                                  self.WORKING_SET_ACCESS))
        self.working_set_move_time = int(options.get('working_set_move_time',
                                                     self.WORKING_SET_MOVE_TIME))
        self.working_set_moving_docs = int(options.get('working_set_moving_docs',
                                                       self.WORKING_SET_MOVE_DOCS))
        self.workers = int(options.get('workers', self.WORKERS))
        self.async = bool(int(options.get('async', self.ASYNC)))
        self.hash_keys = int(options.get('hash_keys', self.HASH_KEYS))
        self.key_length = int(options.get('key_length', self.KEY_LENGTH))

        self.hot_reads = self.HOT_READS
        self.seq_upserts = self.SEQ_UPSERTS
        self.rand_upserts = bool(int(options.get('rand_upserts',
                                                 self.RAND_UPSERTS)))

        self.iterations = int(options.get('iterations', self.ITERATIONS))

        self.worker_instances = int(options.get('worker_instances',
                                                self.WORKER_INSTANCES))

        # Views settings
        self.ddocs = None
        self.index_type = None
        self.query_params = eval(options.get('query_params',
                                             self.VIEW_QUERY_PARAMS))
        self.query_workers = int(options.get('query_workers',
                                             self.QUERY_WORKERS))
        self.query_throughput = float(options.get('query_throughput',
                                                  self.QUERY_THROUGHPUT))

        # N1QL settings
        self.n1ql_gen = options.get('n1ql_gen')

        self.n1ql_workers = int(options.get('n1ql_workers', self.N1QL_WORKERS))
        self.n1ql_op = options.get('n1ql_op', self.N1QL_OP)
        self.n1ql_throughput = float(options.get('n1ql_throughput',
                                                 self.N1QL_THROUGHPUT))
        self.n1ql_batch_size = int(options.get('n1ql_batch_size',
                                               self.N1QL_BATCH_SIZE))
        self.array_size = int(options.get('array_size', self.ARRAY_SIZE))
        self.num_categories = int(options.get('num_categories',
                                              self.NUM_CATEGORIES))
        self.num_replies = int(options.get('num_replies', self.NUM_REPLIES))
        self.range_distance = int(options.get('range_distance',
                                              self.RANGE_DISTANCE))
        if 'n1ql_queries' in options:
            self.n1ql_queries = options.get('n1ql_queries').strip().split(',')

        # 2i settings
        self.existing_items = int(options.get('existing_items',
                                              self.EXISTING_ITEMS))
        self.item_size = int(options.get('item_size', self.ITEM_SIZE))
        self.size_variation_min = int(options.get('size_variation_min',
                                                  self.SIZE_VARIATION_MIN))
        self.size_variation_max = int(options.get('size_variation_max',
                                                  self.SIZE_VARIATION_MAX))

        # FTS settings
        self.fts_config = None

        # Syncgateway settings
        self.syncgateway_settings = None

        # YCSB settings
        self.workload_path = options.get('workload_path')
        self.recorded_load_cache_size = int(options.get('recorded_load_cache_size',
                                                        self.RECORDED_LOAD_CACHE_SIZE))
        self.inserts_per_workerinstance = int(options.get('inserts_per_workerinstance',
                                                          self.INSERTS_PER_WORKERINSTANCE))
        self.epoll = options.get("epoll", self.EPOLL)
        self.boost = options.get('boost', self.BOOST)

        self.target = float(options.get('target', self.TARGET))

        # Subdoc & XATTR
        self.subdoc_field = options.get('subdoc_field')
        self.xattr_field = options.get('xattr_field')

    def __str__(self) -> str:
        return str(self.__dict__)


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPSERTS = True


class HotLoadSettings(PhaseSettings):

    HOT_READS = True

    def __init__(self, options: dict):
        if 'size' in options:
            logger.interrupt(
                "The document `size` may only be set in the [load] "
                "and not in the [hot_load] section")

        super(HotLoadSettings, self).__init__(options)


class RestoreSettings:

    SNAPSHOT = None
    BACKUP_STORAGE = '/backups'
    BACKUP_REPO = ''

    def __init__(self, options):
        self.snapshot = options.get('snapshot', self.SNAPSHOT)
        self.backup_storage = options.get('backup_storage', self.BACKUP_STORAGE)
        self.backup_repo = options.get('backup_repo', self.BACKUP_REPO)

    def __str__(self) -> str:
        return str(self.__dict__)


class XDCRSettings:

    XDCR_REPLICATION_TYPE = 'unidir'
    XDCR_USE_SSL = False
    WAN_DELAY = 0
    FILTER_EXPRESSION = None

    def __init__(self, options: dict):
        self.replication_type = options.get('replication_type',
                                            self.XDCR_REPLICATION_TYPE)
        self.use_ssl = int(options.get('use_ssl',
                                       self.XDCR_USE_SSL))
        self.wan_delay = int(options.get('wan_delay',
                                         self.WAN_DELAY))
        self.filter_expression = options.get('filter_expression',
                                             self.FILTER_EXPRESSION)

    def __str__(self) -> str:
        return str(self.__dict__)


class IndexSettings:

    VIEWS = '[1]'
    DISABLED_UPDATES = 0

    def __init__(self, options: dict):
        self.views = eval(options.get('views', self.VIEWS))
        self.disabled_updates = int(options.get('disabled_updates',
                                                self.DISABLED_UPDATES))
        self.index_type = options.get('index_type')

    def __str__(self) -> str:
        return str(self.__dict__)


class GSISettings:

    STALE = 'true'
    CBINDEXPERF_CONFIGFILE = ''
    CBINDEXPERF_CONFIGFILES = ''
    INIT_NUM_CONNECTIONS = 0
    STEP_NUM_CONNECTIONS = 0
    MAX_NUM_CONNECTIONS = 0
    RUN_RECOVERY_TEST = 0
    INCREMENTAL_LOAD_ITERATIONS = 0
    SCAN_TIME = 1200
    INCREMENTAL_ONLY = 0

    def __init__(self, options: dict):
        self.indexes = {}
        if options.get('indexes') is not None:
            for index_def in options.get('indexes').split(','):
                name, field = index_def.split(':')
                if field.startswith('"'):
                    field = field.replace('"', '\\\"')
                else:
                    field = ','.join(field.split(' '))
                self.indexes[name] = field

        self.stale = options.get('stale', self.STALE)
        self.cbindexperf_configfile = options.get('cbindexperf_configfile',
                                                  self.CBINDEXPERF_CONFIGFILE)
        self.cbindexperf_configfiles = options.get('cbindexperf_configfiles',
                                                   self.CBINDEXPERF_CONFIGFILES)
        self.init_num_connections = int(options.get('init_num_connections',
                                                    self.INIT_NUM_CONNECTIONS))
        self.step_num_connections = int(options.get('step_num_connections',
                                                    self.STEP_NUM_CONNECTIONS))
        self.max_num_connections = int(options.get('max_num_connections',
                                                   self.MAX_NUM_CONNECTIONS))
        self.run_recovery_test = int(options.get('run_recovery_test',
                                                 self.RUN_RECOVERY_TEST))
        self.incremental_only = int(options.get('incremental_only',
                                                self.INCREMENTAL_ONLY))
        self.incremental_load_iterations = int(options.get('incremental_load_iterations',
                                                           self.INCREMENTAL_LOAD_ITERATIONS))
        self.scan_time = int(options.get('scan_time', self.SCAN_TIME))

        self.settings = {}
        for option in options:
            if option.startswith('indexer') or \
                    option.startswith('projector') or \
                    option.startswith('queryport.client.settings'):
                value = options.get(option)
                try:
                    if '.' in value:
                        self.settings[option] = float(value)
                    else:
                        self.settings[option] = int(value)
                except ValueError:
                    self.settings[option] = value

        if self.settings:
            if self.settings['indexer.settings.storage_mode'] == 'forestdb' or \
                    self.settings['indexer.settings.storage_mode'] == 'plasma':
                self.storage = self.settings['indexer.settings.storage_mode']
            else:
                self.storage = 'memdb'

    def __str__(self) -> str:
        return str(self.__dict__)


class DCPSettings:

    NUM_CONNECTIONS = 4
    BUCKET = "bucket-1"

    def __init__(self, options: dict):
        self.num_connections = int(options.get('num_connections',
                                               self.NUM_CONNECTIONS))
        self.bucket = options.get('bucket', self.BUCKET)

    def __str__(self) -> str:
        return str(self.__dict__)


class N1QLSettings:

    def __init__(self, options: dict):
        self.indexes = []
        if 'indexes' in options:
            self.indexes = options.get('indexes').strip().split('\n')

        self.settings = {}
        for option in options:
            if option.startswith('query.settings'):
                key = option.split('.')[-1]
                value = options.get(option)
                self.settings[key] = int(value)

    def __str__(self) -> str:
        return str(self.__dict__)


class AccessSettings(PhaseSettings):

    OPS = float('inf')

    def __init__(self, options):
        if 'size' in options:
            logger.interrupt(
                "The document `size` may only be set in the [load] "
                "and not in the [access] section")

        super(AccessSettings, self).__init__(options)

    def define_queries(self, config) -> None:
        queries = []
        for query_name in self.n1ql_queries:
            query = config.get_n1ql_query_definition(query_name)
            queries.append(query)
        self.n1ql_queries = queries


class BackupSettings:

    COMPRESSION = False

    def __init__(self, options: dict):
        self.compression = int(options.get('compression', self.COMPRESSION))


class ExportSettings:

    TYPE = 'json'  # csv or json
    FORMAT = 'lines'  # lines, list

    def __init__(self, options: dict):
        self.type = options.get('type', self.TYPE)
        self.format = options.get('format', self.FORMAT)
        self.import_file = options.get('import_file')


class FtsSettings:

    def __init__(self, options: dict):
        self.port = options.get("port", "8094")
        self.name = options.get("name")
        self.items = int(options.get("items", 0))
        self.mutate_items = int(options.get("mutate_items", self.items >> 1))
        self.worker = int(options.get("worker", 0))
        self.query = options.get("query", '')
        self.query_size = int(options.get("query_size", 10))
        self.throughput = 0
        self.elastic = bool(int(options.get("elastic", 0)))
        self.query_file = options.get("query_file", None)
        self.type = options.get("type", "match")
        self.logfile = options.get("logfile", None)
        self.order_by = options.get("orderby", "")
        self.storage = options.get("backup_path")
        self.repo = options.get("repo_path")
        self.field = options.get("field", None)
        self.index_configfile = options.get("index_configfile", None)
        self.username = options.get("username", "Administrator")

    def __str__(self) -> str:
        return str(self.__dict__)


class EventingSettings:

    def __init__(self, options: dict):
        self.functions = {}
        if options.get('functions') is not None:
            for function_def in options.get('functions').split(','):
                name, filename = function_def.split(':')
                self.functions[name.strip()] = filename.strip()

        self.worker_count = int(options.get("worker_count", 3))
        self.cpp_worker_thread_count = int(options.get("cpp_worker_thread_count", 2))

    def __str__(self) -> str:
        return str(self.__dict__)


class YCSBSettings:

    REPO = 'git://github.com/couchbaselabs/YCSB.git'
    BRANCH = 'master'

    def __init__(self, options: dict):
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)

    def __str__(self) -> str:
        return str(self.__dict__)


class SyncgatewaySettings:
    REPO = 'git://github.com/couchbaselabs/YCSB.git'
    BRANCH = 'syncgateway-weekly'
    WORKLOAD = 'workloads/syncgateway_blank'
    USERS = 100
    CHANNELS = 1
    CHANNLES_PER_USER = 1
    CLIENTS = 4
    NODES = 4
    CHANNELS_PER_DOC = 1
    DOCUMENTS = 1000000
    ROUNDTRIP_WRITE = "false"
    READ_MODE = 'documents'          # |documents|changes
    FEED_READING_MODE = 'withdocs'   # |withdocs|idsonly
    FEED_MODE = 'longpoll'           # |longpoll|normal
    INSERT_MODE = 'byuser'           # |byuser|bykey
    AUTH = "true"
    READPROPORTION = 1
    UPDATEPROPORTION = 0
    INSERTPROPORTION = 0
    SCANPROPORTION = 0
    REQUESTDISTRIBUTION = 'zipfian'  # |zipfian|uniform
    LOG_TITE = 'sync_gateway_default'
    THREADS = 10
    INSERTSTART = 0
    MAX_INSERTS_PER_INSTANCE = 1000000
    STAR = "false"
    GRANT_ACCESS = "false"
    GRANT_ACCESS_IN_SCAN = "false"
    CHANNELS_PER_GRANT = 1

    def __init__(self, options: dict):
        self.repo = options.get('ycsb_repo', self.REPO)
        self.branch = options.get('ycsb_branch', self.BRANCH)
        self.workload = options.get('workload', self.WORKLOAD)
        self.users = options.get('users', self.USERS)
        self.channels = options.get('channels', self.CHANNELS)
        self.channels_per_user = options.get('channels_per_user', self.CHANNLES_PER_USER)
        self.channels_per_doc = options.get('channels_per_doc', self.CHANNELS_PER_DOC)
        self.documents = options.get('documents', self.DOCUMENTS)
        self.documents_workset = options.get("documents_workset", self.DOCUMENTS)
        self.roundtrip_write = options.get('roundtrip_write', self.ROUNDTRIP_WRITE)
        self.read_mode = options.get('read_mode', self.READ_MODE)
        self.feed_mode = options.get('feed_mode', self.FEED_MODE)
        self.feed_reading_mode = options.get('feed_reading_mode', self.FEED_READING_MODE)
        self.auth = options.get('auth', self.AUTH)
        self.readproportion = options.get('readproportion', self.READPROPORTION)
        self.updateproportion = options.get('updateproportion', self.UPDATEPROPORTION)
        self.insertproportion = options.get('insertproportion', self.INSERTPROPORTION)
        self.scanproportion = options.get('scanproportion', self.SCANPROPORTION)
        self.requestdistribution = options.get('requestdistribution', self.REQUESTDISTRIBUTION)
        self.log_title = options.get('log_title', self.LOG_TITE)
        self.instances_per_client = options.get('instances_per_client', 1)
        self.threads_per_instance = 1
        self.threads = options.get('threads', self.THREADS)
        self.insertstart = options.get('inserstart', self.INSERTSTART)
        self.max_inserts_per_instance = options.get('max_inserts_per_instance',
                                                    self.MAX_INSERTS_PER_INSTANCE)
        self.insert_mode = options.get('insert_mode', self.INSERT_MODE)
        self.clients = options.get('clients', self.CLIENTS)
        self.nodes = options.get('nodes', self.NODES)
        self.starchannel = options.get('starchannel', self.STAR)
        self.grant_access = options.get('grant_access', self.GRANT_ACCESS)
        self.channels_per_grant = options.get('channels_per_grant', self.CHANNELS_PER_GRANT)
        self.grant_access_in_scan = options.get('grant_access_in_scan', self.GRANT_ACCESS_IN_SCAN)
        self.build_label = options.get('build_label', '')

    def __str__(self) -> str:
        return str(self.__dict_)


class TestConfig(Config):

    @property
    def test_case(self) -> TestCaseSettings:
        options = self._get_options_as_dict('test_case')
        return TestCaseSettings(options)

    @property
    def cluster(self) -> ClusterSettings:
        options = self._get_options_as_dict('cluster')
        return ClusterSettings(options)

    @property
    def bucket(self) -> BucketSettings:
        options = self._get_options_as_dict('bucket')
        return BucketSettings(options)

    @property
    def bucket_extras(self) -> dict:
        return self._get_options_as_dict('bucket_extras')

    @property
    def buckets(self) -> List[str]:
        return [
            'bucket-{}'.format(i + 1) for i in range(self.cluster.num_buckets)
        ]

    @property
    def eventing_buckets(self) -> List[str]:
        return [
            'eventing-bucket-{}'.format(i + 1) for i in range(self.cluster.eventing_buckets)
        ]

    @property
    def compaction(self) -> CompactionSettings:
        options = self._get_options_as_dict('compaction')
        return CompactionSettings(options)

    @property
    def restore_settings(self) -> RestoreSettings:
        options = self._get_options_as_dict('restore')
        return RestoreSettings(options)

    @property
    def load_settings(self):
        options = self._get_options_as_dict('load')
        return LoadSettings(options)

    @property
    def hot_load_settings(self) -> HotLoadSettings:
        options = self._get_options_as_dict('hot_load')
        hot_load = HotLoadSettings(options)

        load = self.load_settings
        hot_load.doc_gen = load.doc_gen
        hot_load.array_size = load.array_size
        hot_load.num_categories = load.num_categories
        hot_load.num_replies = load.num_replies
        hot_load.size = load.size
        return hot_load

    @property
    def xdcr_settings(self) -> XDCRSettings:
        options = self._get_options_as_dict('xdcr')
        return XDCRSettings(options)

    @property
    def index_settings(self) -> IndexSettings:
        options = self._get_options_as_dict('index')
        return IndexSettings(options)

    @property
    def gsi_settings(self) -> GSISettings:
        options = self._get_options_as_dict('secondary')
        return GSISettings(options)

    @property
    def dcp_settings(self) -> DCPSettings:
        options = self._get_options_as_dict('dcp')
        return DCPSettings(options)

    @property
    def n1ql_settings(self) -> N1QLSettings:
        options = self._get_options_as_dict('n1ql')
        return N1QLSettings(options)

    @property
    def backup_settings(self) -> BackupSettings:
        options = self._get_options_as_dict('backup')
        return BackupSettings(options)

    @property
    def export_settings(self) -> ExportSettings:
        options = self._get_options_as_dict('export')
        return ExportSettings(options)

    @property
    def access_settings(self) -> AccessSettings:
        options = self._get_options_as_dict('access')
        access = AccessSettings(options)

        if hasattr(access, 'n1ql_queries'):
            access.define_queries(self)

        load_settings = self.load_settings
        access.doc_gen = load_settings.doc_gen
        access.range_distance = load_settings.range_distance
        access.array_size = load_settings.array_size
        access.num_categories = load_settings.num_categories
        access.num_replies = load_settings.num_replies
        access.size = load_settings.size
        access.hash_keys = load_settings.hash_keys
        access.key_length = load_settings.key_length

        return access

    @property
    def rebalance_settings(self) -> RebalanceSettings:
        options = self._get_options_as_dict('rebalance')
        return RebalanceSettings(options)

    @property
    def stats_settings(self) -> StatsSettings:
        options = self._get_options_as_dict('stats')
        return StatsSettings(options)

    @property
    def profiling_settings(self) -> ProfilingSettings:
        options = self._get_options_as_dict('profiling')
        return ProfilingSettings(options)

    @property
    def internal_settings(self) -> dict:
        return self._get_options_as_dict('internal')

    @property
    def xdcr_cluster_settings(self) -> dict:
        return self._get_options_as_dict('xdcr_cluster')

    @property
    def fts_settings(self) -> FtsSettings:
        options = self._get_options_as_dict('fts')
        return FtsSettings(options)

    @property
    def ycsb_settings(self) -> YCSBSettings:
        options = self._get_options_as_dict('ycsb')
        return YCSBSettings(options)

    @property
    def eventing_settings(self) -> EventingSettings:
        options = self._get_options_as_dict('eventing')
        return EventingSettings(options)

    def get_n1ql_query_definition(self, query_name: str) -> dict:
        return self._get_options_as_dict('n1ql-{}'.format(query_name))

    @property
    def fio(self) -> dict:
        return self._get_options_as_dict('fio')

    @property
    def syncgateway_settings(self) -> SyncgatewaySettings:
        options = self._get_options_as_dict('syncgateway')
        return SyncgatewaySettings(options)


class TargetSettings:

    def __init__(self, host: str, bucket: str, password: str, prefix: str):
        self.password = password
        self.node = host
        self.bucket = bucket
        self.prefix = prefix


class TargetIterator:

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 prefix: str = None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.prefix = prefix

    def __iter__(self) -> Iterator:
        password = self.test_config.bucket.password
        prefix = self.prefix
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                if self.prefix is None:
                    prefix = target_hash(master)
                yield TargetSettings(master, bucket, password, prefix)

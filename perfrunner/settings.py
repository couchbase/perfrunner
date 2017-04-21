import csv
import os.path
from configparser import ConfigParser, NoOptionError, NoSectionError
from typing import Iterator, List, Tuple

from decorator import decorator
from logger import logger

from perfrunner.helpers.misc import target_hash

REPO = 'https://github.com/couchbase/perfrunner'


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

    def parse(self, fname: str, override=()) -> None:
        if override:
            override = [x for x in csv.reader(
                ' '.join(override).split(','), delimiter='.')]

        logger.info('Reading configuration file: {}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt("File doesn't exist: {}".format(fname))
        self.config.optionxform = str
        self.config.read(fname)
        for section, option, value in override:
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, option, value)

        basename = os.path.basename(fname)
        self.name = os.path.splitext(basename)[0]

    @safe
    def _get_options_as_dict(self, section: str) -> dict:
        if section in self.config.sections():
            return {p: v for p, v in self.config.items(section)}
        else:
            return {}


class ClusterSpec(Config):

    @safe
    def yield_clusters(self) -> Iterator:
        for cluster_name, servers in self.config.items('clusters'):
            yield cluster_name, [s.split(',', 1)[0] for s in servers.split()]

    @safe
    def yield_masters(self) -> Iterator:
        for _, servers in self.yield_clusters():
            yield servers[0]

    @safe
    def yield_servers(self) -> Iterator:
        for _, servers in self.yield_clusters():
            for server in servers:
                yield server

    @safe
    def yield_hostnames(self) -> Iterator:
        for _, servers in self.yield_clusters():
            for server in servers:
                yield server.split(':')[0]

    @safe
    def yield_servers_by_role(self, role: str) -> Iterator:
        for name, servers in self.config.items('clusters'):
            has_service = []
            for server in servers.split():
                if role in server.split(',')[1:]:
                    has_service.append(server.split(',')[0])
            yield name, has_service

    @safe
    def yield_fts_servers(self) -> Iterator:
        for name, servers in self.config.items('clusters'):
            for server in servers.split():
                if 'fts' in server.split(',')[1:]:
                    yield server.split(':')[0]

    @safe
    def yield_kv_servers(self) -> Iterator:
        for name, servers in self.config.items('clusters'):
            for server in servers.split():
                if ',' in server:
                    continue
                else:
                    yield server.split(':')[0]

    @property
    @safe
    def roles(self) -> dict:
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
    def workers(self) -> List[str]:
        return self.config.get('clients', 'hosts').split()

    @property
    @safe
    def client_credentials(self) -> List[str]:
        return self.config.get('clients', 'credentials').split(':')

    @property
    @safe
    def paths(self) -> Tuple[str, str]:
        data_path = self.config.get('storage', 'data')
        index_path = self.config.get('storage', 'index')
        return data_path, index_path

    @property
    @safe
    def backup(self) -> str:
        return self.config.get('storage', 'backup')

    @property
    @safe
    def rest_credentials(self) -> List[str]:
        return self.config.get('credentials', 'rest').split(':')

    @property
    @safe
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

    GROUP_NUMBER = 1
    NUM_BUCKETS = 1
    INDEX_MEM_QUOTA = 256
    FTS_INDEX_MEM_QUOTA = 512
    THROTTLE_CPU = 0

    def __init__(self, options: dict):
        self.mem_quota = int(options.get('mem_quota'))
        self.index_mem_quota = int(options.get('index_mem_quota',
                                               self.INDEX_MEM_QUOTA))
        self.fts_index_mem_quota = int(options.get('fts_index_mem_quota',
                                                   self.FTS_INDEX_MEM_QUOTA))
        self.initial_nodes = [
            int(nodes) for nodes in options.get('initial_nodes').split()
        ]
        self.num_buckets = int(options.get('num_buckets',
                                           self.NUM_BUCKETS))
        self.num_vbuckets = options.get('num_vbuckets')
        self.group_number = int(options.get('group_number',
                                            self.GROUP_NUMBER))
        self.throttle_cpu = int(options.get('throttle_cpu',
                                            self.THROTTLE_CPU))


class StatsSettings:

    ENABLED = 1
    POST_TO_SF = 0

    INTERVAL = 5
    LAT_INTERVAL = 1

    POST_CPU = 0

    SECONDARY_STATSFILE = '/root/statsfile'

    CBMONITOR = 'cbmonitor.sc.couchbase.com'
    SERIESLY = 'cbmonitor.sc.couchbase.com'
    SHOWFAST = 'showfast.sc.couchbase.com'

    MONITORED_PROCESSES = ['beam.smp',
                           'cbft',
                           'cbq-engine',
                           'indexer',
                           'memcached']

    def __init__(self, options: dict):
        self.enabled = int(options.get('enabled', self.ENABLED))
        self.post_to_sf = int(options.get('post_to_sf', self.POST_TO_SF))

        self.interval = int(options.get('interval', self.INTERVAL))
        self.lat_interval = float(options.get('lat_interval',
                                              self.LAT_INTERVAL))

        self.post_cpu = int(options.get('post_cpu', self.POST_CPU))

        self.secondary_statsfile = options.get('secondary_statsfile',
                                               self.SECONDARY_STATSFILE)

        self.monitored_processes = self.MONITORED_PROCESSES + \
            options.get('monitored_processes', '').split()


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
    SLEEP_AFTER_FAILOVER = 600
    START_AFTER = 1200
    STOP_AFTER = 1200

    def __init__(self, options: dict):
        nodes_after = options.get('nodes_after', '').split()
        self.nodes_after = [int(num_nodes) for num_nodes in nodes_after]

        self.swap = int(options.get('swap', self.SWAP))

        self.failed_nodes = int(options.get('failed_nodes', 1))
        self.failover = options.get('failover', self.FAILOVER)
        self.sleep_after_failover = int(options.get('sleep_after_failover',
                                                    self.SLEEP_AFTER_FAILOVER))
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

    SEQ_READS = False
    SEQ_UPDATES = False

    ITERATIONS = 1

    ASYNC = False
    HASH_KEYS = 0
    KEY_LENGTH = 0  # max can be 32

    ITEMS = 0
    EXISTING_ITEMS = 0
    SIZE = 2048
    EXPIRATION = 0

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
    SPRING_WORKERS = 100
    WORKER_INSTANCES = 1

    N1QL_OP = 'read'
    N1QL_BATCH_SIZE = 100

    ARRAY_SIZE = 10
    NUM_CATEGORIES = 10 ** 6
    NUM_REPLIES = 100
    RANGE_DISTANCE = 10

    PARALLEL_WORKLOAD = False

    ITEM_SIZE = 64
    SIZE_VARIATION_MIN = 1
    SIZE_VARIATION_MAX = 1024

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

        self.expiration = int(options.get('expiration', self.EXPIRATION))
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

        self.seq_reads = self.SEQ_READS
        self.seq_updates = self.SEQ_UPDATES

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
        self.spring_workers = int(options.get('spring_workers',
                                              self.SPRING_WORKERS))
        self.parallel_workload = bool(int(options.get('parallel_workload',
                                                      self.PARALLEL_WORKLOAD)))

        self.item_size = int(options.get('item_size', self.ITEM_SIZE))
        self.size_variation_min = int(options.get('size_variation_min',
                                                  self.SIZE_VARIATION_MIN))
        self.size_variation_max = int(options.get('size_variation_max',
                                                  self.SIZE_VARIATION_MAX))

        # FTS settings
        self.fts_config = None

        # YCSB settings
        self.workload_path = options.get('workload_path')

        # Subdoc & XATTR
        self.subdoc_field = options.get('subdoc_field')
        self.xattr_field = options.get('xattr_field')

    def __str__(self) -> str:
        return str(self.__dict__)


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPDATES = True


class HotLoadSettings(PhaseSettings):

    SEQ_READS = True
    SEQ_UPDATES = False

    def __init__(self, options: dict):
        if 'size' in options:
            logger.interrupt(
                "The document `size` may only be set in the [load] "
                "and not in the [hot_load] section")

        super(HotLoadSettings, self).__init__(options)


class RestoreSettings:

    SNAPSHOT = None

    def __init__(self, options):
        self.snapshot = options.get('snapshot', self.SNAPSHOT)

    def __str__(self) -> str:
        return str(self.__dict__)


class XDCRSettings:

    XDCR_REPLICATION_TYPE = 'bidir'
    XDCR_REPLICATION_PROTOCOL = None
    XDCR_USE_SSL = False
    WAN_ENABLED = False
    FILTER_EXPRESSION = None

    def __init__(self, options: dict):
        self.replication_type = options.get('replication_type',
                                            self.XDCR_REPLICATION_TYPE)
        self.replication_protocol = options.get('replication_protocol',
                                                self.XDCR_REPLICATION_PROTOCOL)
        self.use_ssl = int(options.get('use_ssl', self.XDCR_USE_SSL))
        self.wan_enabled = int(options.get('wan_enabled', self.WAN_ENABLED))
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
    BLOCK_MEMORY = 0
    RESTRICT_KERNEL_MEMORY = 0
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
        self.block_memory = int(options.get('block_memory', self.BLOCK_MEMORY))
        self.restrict_kernel_memory = options.get('restrict_kernel_memory',
                                                  self.RESTRICT_KERNEL_MEMORY)
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


class ExportImportSettings:

    TYPE = 'json'  # csv or json
    FORMAT = 'lines'  # lines, list, or sample/file

    def __init__(self, options: dict):
        self.type = options.get('type', self.TYPE)
        self.format = options.get('format', self.FORMAT)


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


class YcsbSettings:

    REPO = 'git://github.com/brianfrankcooper/YCSB.git'
    BRANCH = 'master'

    def __init__(self, options: dict):
        self.sdk = options.get("sdk")
        self.bucket = options.get("bucket")
        self.jvm = options.get("jvm-args")
        self.threads = options.get("threads")
        self.parameters = options.get("parameters")
        self.workload = options.get("workload_path")
        self.size = options.get("size")
        self.reccount = options.get("recordcount")
        self.opcount = options.get("operationcount")
        self.path = options.get("path")
        self.log_file = options.get("export_file")
        self.log_path = options.get("export_file_path")
        self.index = options.get("index")

        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)

    def __str__(self) -> str:
        return str(self.__dict__)


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
    def export_import_settings(self) -> ExportImportSettings:
        options = self._get_options_as_dict('export_import')
        return ExportImportSettings(options)

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
    def ycsb_settings(self) -> YcsbSettings:
        options = self._get_options_as_dict('ycsb')
        return YcsbSettings(options)

    def get_n1ql_query_definition(self, query_name: str) -> dict:
        return self._get_options_as_dict('n1ql-{}'.format(query_name))

    @property
    def fio(self) -> dict:
        return self._get_options_as_dict('fio')


class TargetSettings:

    def __init__(self, host_port: str, bucket: str, password: str, prefix: str):
        self.password = password
        self.node = host_port
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
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                if self.prefix is None:
                    prefix = target_hash(master.split(':')[0])
                yield TargetSettings(master, bucket, password, prefix)

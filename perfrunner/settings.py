import csv
import json
import os
import re
from configparser import ConfigParser, NoOptionError, NoSectionError
from itertools import chain, combinations, permutations
from typing import Dict, Iterable, Iterator, List

from decorator import decorator

from logger import logger
from perfrunner.helpers.misc import maybe_atoi, target_hash

CBMONITOR_HOST = 'cbmonitor.sc.couchbase.com'
SHOWFAST_HOST = 'showfast.sc.couchbase.com'  # 'localhost:8000'
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

    def parse(self, fname: str, override=None):
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

    def servers_by_role_from_first_cluster(self, role: str) -> List[str]:
        has_service = []
        servers = self.config.items('clusters')[0][1]
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
    def data_path(self) -> str:
        return self.config.get('storage', 'data')

    @property
    def index_path(self) -> str:
        return self.config.get('storage', 'index',
                               fallback=self.config.get('storage', 'data'))

    @property
    def analytics_paths(self) -> List[str]:
        analytics_paths = self.config.get('storage', 'analytics', fallback=None)
        if analytics_paths is not None:
            return analytics_paths.split()
        return []

    @property
    def paths(self) -> Iterator[str]:
        for path in set([self.data_path, self.index_path] + self.analytics_paths):
            if path is not None:
                yield path

    @property
    def backup(self) -> str:
        return self.config.get('storage', 'backup', fallback=None)

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

    USE_WORKERS = 1
    RESET_WORKERS = 0

    def __init__(self, options: dict):
        self.test_module = '.'.join(options.get('test').split('.')[:-1])
        self.test_class = options.get('test').split('.')[-1]

        self.use_workers = int(options.get('use_workers', self.USE_WORKERS))
        self.reset_workers = int(options.get('reset_workers', self.RESET_WORKERS))


class ShowFastSettings:

    THRESHOLD = -10

    def __init__(self, options: dict):
        self.title = options.get('title')
        self.component = options.get('component', '')
        self.category = options.get('category', '')
        self.sub_category = options.get('sub_category', '')
        self.order_by = options.get('orderby', '')
        self.build_label = options.get('build_label', '')
        self.threshold = int(options.get("threshold", self.THRESHOLD))


class ClusterSettings:

    NUM_BUCKETS = 1

    INDEX_MEM_QUOTA = 256
    FTS_INDEX_MEM_QUOTA = 0
    ANALYTICS_MEM_QUOTA = 0
    EVENTING_MEM_QUOTA = 0

    EVENTING_BUCKET_MEM_QUOTA = 0
    EVENTING_METADATA_BUCKET_MEM_QUOTA = 0
    EVENTING_METADATA_BUCKET_NAME = 'eventing'
    EVENTING_BUCKETS = 0

    KERNEL_MEM_LIMIT = 0
    KERNEL_MEM_LIMIT_SERVICES = 'fts', 'index'
    ONLINE_CORES = 0
    ENABLE_CPU_CORES = 'true'
    ENABLE_N2N_ENCRYPTION = None

    IPv6 = 0

    def __init__(self, options: dict):
        self.mem_quota = int(options.get('mem_quota'))
        self.index_mem_quota = int(options.get('index_mem_quota',
                                               self.INDEX_MEM_QUOTA))
        self.fts_index_mem_quota = int(options.get('fts_index_mem_quota',
                                                   self.FTS_INDEX_MEM_QUOTA))
        self.analytics_mem_quota = int(options.get('analytics_mem_quota',
                                                   self.ANALYTICS_MEM_QUOTA))
        self.eventing_mem_quota = int(options.get('eventing_mem_quota',
                                                  self.EVENTING_MEM_QUOTA))

        self.initial_nodes = [
            int(nodes) for nodes in options.get('initial_nodes').split()
        ]

        self.num_buckets = int(options.get('num_buckets',
                                           self.NUM_BUCKETS))
        self.eventing_bucket_mem_quota = int(options.get('eventing_bucket_mem_quota',
                                                         self.EVENTING_BUCKET_MEM_QUOTA))
        self.eventing_metadata_bucket_mem_quota = \
            int(options.get('eventing_metadata_bucket_mem_quota',
                            self.EVENTING_METADATA_BUCKET_MEM_QUOTA))
        self.eventing_buckets = int(options.get('eventing_buckets',
                                                self.EVENTING_BUCKETS))
        self.num_vbuckets = options.get('num_vbuckets')
        self.online_cores = int(options.get('online_cores',
                                            self.ONLINE_CORES))
        self.enable_cpu_cores = maybe_atoi(options.get('enable_cpu_cores', self.ENABLE_CPU_CORES))
        self.ipv6 = int(options.get('ipv6', self.IPv6))
        self.kernel_mem_limit = options.get('kernel_mem_limit',
                                            self.KERNEL_MEM_LIMIT)
        self.enable_n2n_encryption = options.get('enable_n2n_encryption',
                                                 self.ENABLE_N2N_ENCRYPTION)

        kernel_mem_limit_services = options.get('kernel_mem_limit_services')
        if kernel_mem_limit_services:
            self.kernel_mem_limit_services = kernel_mem_limit_services.split()
        else:
            self.kernel_mem_limit_services = self.KERNEL_MEM_LIMIT_SERVICES


class StatsSettings:

    ENABLED = 1
    POST_TO_SF = 0

    INTERVAL = 5
    LAT_INTERVAL = 1

    POST_CPU = 0

    CLIENT_PROCESSES = []
    SERVER_PROCESSES = ['beam.smp',
                        'cbft',
                        'cbq-engine',
                        'indexer',
                        'memcached']
    TRACED_PROCESSES = []

    def __init__(self, options: dict):
        self.enabled = int(options.get('enabled', self.ENABLED))
        self.post_to_sf = int(options.get('post_to_sf', self.POST_TO_SF))

        self.interval = int(options.get('interval', self.INTERVAL))
        self.lat_interval = float(options.get('lat_interval',
                                              self.LAT_INTERVAL))

        self.post_cpu = int(options.get('post_cpu', self.POST_CPU))

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

    def __init__(self, options: dict):
        self.services = options.get('services',
                                    self.SERVICES).split()
        self.interval = int(options.get('interval',
                                        self.INTERVAL))
        self.num_profiles = int(options.get('num_profiles',
                                            self.NUM_PROFILES))
        self.profiles = options.get('profiles',
                                    self.PROFILES).split(',')


class BucketSettings:

    PASSWORD = 'password'
    REPLICA_NUMBER = 1
    REPLICA_INDEX = 0
    EVICTION_POLICY = 'valueOnly'  # alt: fullEviction
    BUCKET_TYPE = 'membase'  # alt: ephemeral
    AUTOFAILOVER_ENABLED = 'true'
    FAILOVER_MIN = 5
    FAILOVER_MAX = 30
    BACKEND_STORAGE = None

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

        self.compression_mode = options.get('compression_mode')

        if options.get('autofailover_enabled', self.AUTOFAILOVER_ENABLED).lower() == "false":
            self.autofailover_enabled = 'false'
        else:
            self.autofailover_enabled = 'true'

        self.failover_min = int(options.get('failover_min', self.FAILOVER_MIN))

        self.failover_max = int(options.get('failover_max', self.FAILOVER_MAX))

        self.backend_storage = options.get('backend_storage', self.BACKEND_STORAGE)


class CollectionSettings:

    CONFIG = None
    COLLECTION_MAP = None

    def __init__(self, options: dict):
        self.config = options.get('config', self.CONFIG)
        self.collection_map = self.COLLECTION_MAP
        if self.config is not None:
            with open(self.config) as f:
                self.collection_map = json.load(f)


class UserSettings:

    NUM_USERS_PER_BUCKET = 0

    def __init__(self, options: dict):
        self.num_users_per_bucket = int(options.get('num_users_per_bucket',
                                                    self.NUM_USERS_PER_BUCKET))


class CompactionSettings:

    DB_PERCENTAGE = 30
    VIEW_PERCENTAGE = 30
    PARALLEL = True
    BUCKET_COMPACTION = 'true'

    def __init__(self, options: dict):
        self.db_percentage = options.get('db_percentage',
                                         self.DB_PERCENTAGE)
        self.view_percentage = options.get('view_percentage',
                                           self.VIEW_PERCENTAGE)
        self.parallel = options.get('parallel', self.PARALLEL)
        self.bucket_compaction = options.get('bucket_compaction', self.BUCKET_COMPACTION)

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

    DOC_GEN = 'basic'
    POWER_ALPHA = 0
    ZIPF_ALPHA = 0

    CREATES = 0
    READS = 0
    UPDATES = 0
    DELETES = 0
    READS_AND_UPDATES = 0
    FTS_UPDATES = 0
    TTL = 0

    OPS = 0
    TARGET = 0

    HOT_READS = False
    SEQ_UPSERTS = False

    BATCH_SIZE = 1000
    BATCHES = 1
    SPRING_BATCH_SIZE = 100

    ITERATIONS = 1

    ASYNC = False

    KEY_FMTR = 'decimal'

    ITEMS = 0
    SIZE = 2048

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
    WORKLOAD_INSTANCES = 1

    N1QL_OP = 'read'
    N1QL_BATCH_SIZE = 100
    N1QL_TIMEOUT = 0

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

    YCSB_FIELD_COUNT = 10
    YCSB_FIELD_LENGTH = 100

    SSL_MODE = 'none'
    SSL_AUTH_KEYSTORE = "certificates/auth.keystore"
    SSL_DATA_KEYSTORE = "certificates/data.keystore"
    SSL_KEYSTOREPASS = "storepass"
    SHOW_TLS_VERSION = False
    CIPHER_LIST = None
    MIN_TLS_VERSION = None

    PERSIST_TO = 0
    REPLICATE_TO = 0

    TIMESERIES = 0

    CBCOLLECT = 0

    CONNSTR_PARAMS = "{'ipv6': 'allow', 'enable_tracing': 'false'}"

    YCSB_CLIENT = 'couchbase2'

    DURABILITY = None

    YCSB_KV_ENDPOINTS = 1
    YCSB_ENABLE_MUTATION_TOKEN = None

    YCSB_RETRY_STRATEGY = 'default'
    YCSB_RETRY_LOWER = 1
    YCSB_RETRY_UPPER = 500
    YCSB_RETRY_FACTOR = 2
    YCSB_OUT_OF_ORDER = 0

    TRANSACTIONSENABLED = 0

    NUM_ATRS = 1024

    YCSB_JVM_ARGS = None

    TPCDS_SCALE_FACTOR = 1

    DOCUMENTSINTRANSACTION = 4
    TRANSACTIONREADPROPORTION = 0.25
    TRANSACTIONUPDATEPROPORTION = 0.75
    TRANSACTIONINSERTPROPORTION = 0

    REQUESTDISTRIBUTION = 'zipfian'

    ANALYTICS_WARMUP_OPS = 0
    ANALYTICS_WARMUP_WORKERS = 0

    COLLECTION_MAP = None
    CUSTOM_PILLOWFIGHT = False

    USERS = None
    USER_MOD_THROUGHPUT = float('inf')
    USER_MOD_WORKERS = 0
    COLLECTION_MOD_WORKERS = 0
    COLLECTION_MOD_THROUGHPUT = float('inf')

    JAVA_DCP_STREAM = 'all'
    JAVA_DCP_CONFIG = None
    JAVA_DCP_CLIENTS = 0

    def __init__(self, options: dict):
        # Common settings
        self.time = int(options.get('time', self.TIME))

        # KV settings
        self.doc_gen = options.get('doc_gen', self.DOC_GEN)
        self.power_alpha = float(options.get('power_alpha', self.POWER_ALPHA))
        self.zipf_alpha = float(options.get('zipf_alpha', self.ZIPF_ALPHA))

        self.size = int(options.get('size', self.SIZE))
        self.items = int(options.get('items', self.ITEMS))

        self.creates = int(options.get('creates', self.CREATES))
        self.reads = int(options.get('reads', self.READS))
        self.updates = int(options.get('updates', self.UPDATES))
        self.deletes = int(options.get('deletes', self.DELETES))
        self.ttl = int(options.get('ttl', self.TTL))
        self.reads_and_updates = int(options.get('reads_and_updates',
                                                 self.READS_AND_UPDATES))
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
        self.key_fmtr = options.get('key_fmtr', self.KEY_FMTR)

        self.hot_reads = self.HOT_READS
        self.seq_upserts = self.SEQ_UPSERTS

        self.iterations = int(options.get('iterations', self.ITERATIONS))

        self.batch_size = int(options.get('batch_size', self.BATCH_SIZE))
        self.batches = int(options.get('batches', self.BATCHES))
        self.spring_batch_size = int(options.get('spring_batch_size', self.SPRING_BATCH_SIZE))

        self.workload_instances = int(options.get('workload_instances',
                                                  self.WORKLOAD_INSTANCES))

        self.connstr_params = eval(options.get('connstr_params', self.CONNSTR_PARAMS))

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
        self.n1ql_timeout = int(options.get('n1ql_timeout', self.N1QL_TIMEOUT))

        if 'n1ql_queries' in options:
            self.n1ql_queries = options.get('n1ql_queries').strip().split(',')

        # 2i settings
        self.item_size = int(options.get('item_size', self.ITEM_SIZE))
        self.size_variation_min = int(options.get('size_variation_min',
                                                  self.SIZE_VARIATION_MIN))
        self.size_variation_max = int(options.get('size_variation_max',
                                                  self.SIZE_VARIATION_MAX))

        # YCSB settings
        self.workload_path = options.get('workload_path')
        self.recorded_load_cache_size = int(options.get('recorded_load_cache_size',
                                                        self.RECORDED_LOAD_CACHE_SIZE))
        self.inserts_per_workerinstance = int(options.get('inserts_per_workerinstance',
                                                          self.INSERTS_PER_WORKERINSTANCE))
        self.epoll = options.get("epoll", self.EPOLL)
        self.boost = options.get('boost', self.BOOST)
        self.target = float(options.get('target', self.TARGET))
        self.field_count = int(options.get('field_count', self.YCSB_FIELD_COUNT))
        self.field_length = int(options.get('field_length', self.YCSB_FIELD_LENGTH))
        self.kv_endpoints = int(options.get('kv_endpoints', self.YCSB_KV_ENDPOINTS))
        self.enable_mutation_token = options.get('enable_mutation_token',
                                                 self.YCSB_ENABLE_MUTATION_TOKEN)
        self.ycsb_client = options.get('ycsb_client', self.YCSB_CLIENT)
        self.ycsb_out_of_order = int(options.get('out_of_order', self.YCSB_OUT_OF_ORDER))

        # trasnsaction settings
        self.transactionsenabled = int(options.get('transactionsenabled',
                                                   self.TRANSACTIONSENABLED))
        self.documentsintransaction = int(options.get('documentsintransaction',
                                                      self.DOCUMENTSINTRANSACTION))
        self.transactionreadproportion = options.get('transactionreadproportion',
                                                     self.TRANSACTIONREADPROPORTION)
        self.transactionupdateproportion = options.get('transactionupdateproportion',
                                                       self.TRANSACTIONUPDATEPROPORTION)
        self.transactioninsertproportion = options.get('transactioninsertproportion',
                                                       self.TRANSACTIONINSERTPROPORTION)
        self.requestdistribution = options.get('requestdistribution',
                                               self.REQUESTDISTRIBUTION)

        # multiple of 1024
        self.num_atrs = int(options.get('num_atrs', self.NUM_ATRS))

        # Subdoc & XATTR
        self.subdoc_field = options.get('subdoc_field')
        self.xattr_field = options.get('xattr_field')

        # SSL settings
        self.ssl_mode = (options.get('ssl_mode', self.SSL_MODE))
        self.ssl_keystore_password = self.SSL_KEYSTOREPASS
        if self.ssl_mode == 'auth':
            self.ssl_keystore_file = self.SSL_AUTH_KEYSTORE
        else:
            self.ssl_keystore_file = self.SSL_DATA_KEYSTORE
        self.show_tls_version = options.get('show_tls_version', self.SHOW_TLS_VERSION)
        self.cipher_list = options.get('cipher_list', self.CIPHER_LIST)
        if self.cipher_list:
            self.cipher_list = self.cipher_list.split(',')

        self.min_tls_version = options.get('min_tls_version',
                                           self.MIN_TLS_VERSION)

        # Durability settings
        self.persist_to = int(options.get('persist_to',
                                          self.PERSIST_TO))
        self.replicate_to = int(options.get('replicate_to',
                                            self.REPLICATE_TO))
        if options.get('durability', self.DURABILITY) is not None:
            self.durability = int(options.get('durability', self.DURABILITY))
        else:
            self.durability = self.DURABILITY

        # YCSB Retry Strategy settings
        self.retry_strategy = options.get('retry_strategy', self.YCSB_RETRY_STRATEGY)
        self.retry_lower = int(options.get('retry_lower', self.YCSB_RETRY_LOWER))
        self.retry_upper = int(options.get('retry_upper', self.YCSB_RETRY_UPPER))
        self.retry_factor = int(options.get('retry_factor', self.YCSB_RETRY_FACTOR))

        # CbCollect Setting
        self.cbcollect = int(options.get('cbcollect',
                                         self.CBCOLLECT))
        # Latency Setting
        self.timeseries = int(options.get('timeseries',
                                          self.TIMESERIES))

        self.ycsb_jvm_args = options.get('ycsb_jvm_args', self.YCSB_JVM_ARGS)
        self.tpcds_scale_factor = int(options.get('tpcds_scale_factor', self.TPCDS_SCALE_FACTOR))

        self.analytics_warmup_ops = int(options.get('analytics_warmup_ops',
                                                    self.ANALYTICS_WARMUP_OPS))
        self.analytics_warmup_workers = int(options.get('analytics_warmup_workers',
                                                        self.ANALYTICS_WARMUP_WORKERS))

        # collection map placeholder
        self.collections = self.COLLECTION_MAP

        self.custom_pillowfight = self.CUSTOM_PILLOWFIGHT

        self.users = self.USERS

        self.user_mod_workers = int(options.get('user_mod_workers', self.USER_MOD_WORKERS))

        self.user_mod_throughput = float(options.get('user_mod_throughput',
                                                     self.USER_MOD_THROUGHPUT))

        self.collection_mod_workers = int(options.get('collection_mod_workers',
                                                      self.COLLECTION_MOD_WORKERS))
        self.collection_mod_throughput = float(options.get('collection_mod_throughput',
                                                           self.COLLECTION_MOD_THROUGHPUT))
        self.java_dcp_stream = self.JAVA_DCP_STREAM
        self.java_dcp_config = self.JAVA_DCP_CONFIG
        self.java_dcp_clients = self.JAVA_DCP_CLIENTS

    def __str__(self) -> str:
        return str(self.__dict__)


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPSERTS = True


class JTSAccessSettings(PhaseSettings):
    JTS_REPO = "https://github.com/couchbaselabs/JTS"
    JTS_REPO_BRANCH = "master"
    JTS_HOME_DIR = "JTS"
    JTS_RUN_CMD = "java -jar target/JTS-1.0-jar-with-dependencies.jar"
    JTS_LOGS_DIR = "JTSlogs"

    def __init__(self, options: dict):
        self.jts_repo = self.JTS_REPO
        self.jts_home_dir = self.JTS_HOME_DIR
        self.jts_run_cmd = self.JTS_RUN_CMD
        self.jts_logs_dir = self.JTS_LOGS_DIR
        self.jts_repo_branch = options.get("jts_repo_branch", self.JTS_REPO_BRANCH)
        self.jts_instances = options.get("jts_instances", "1")
        self.test_total_docs = options.get("test_total_docs", "1000000")
        self.test_query_workers = options.get("test_query_workers", "10")
        self.test_kv_workers = options.get("test_kv_workers", "0")
        self.test_kv_throughput_goal = options.get("test_kv_throughput_goal", "1000")
        self.test_data_file = options.get("test_data_file", "../tests/fts/low.txt")
        self.test_driver = options.get("test_driver", "couchbase")
        self.test_stats_limit = options.get("test_stats_limit", "1000000")
        self.test_stats_aggregation_step = options.get("test_stats_aggregation_step", "1000")
        self.test_debug = options.get("test_debug", "false")
        self.test_query_type = options.get("test_query_type", "term")
        self.test_query_limit = options.get("test_query_limit", "10")
        self.test_query_field = options.get("test_query_field", "text")
        self.test_mutation_field = options.get("test_mutation_field", "text2")
        self.test_worker_type = options.get("test_worker_type", "latency")
        self.couchbase_index_name = options.get("couchbase_index_name", "perf_fts_index")
        self.couchbase_index_configfile = options.get("couchbase_index_configfile")
        self.couchbase_index_type = options.get("couchbase_index_type")
        self.workload_instances = int(self.jts_instances)
        self.time = options.get('test_duration', "600")
        self.warmup_query_workers = options.get("warmup_query_workers", "0")
        self.warmup_time = options.get('warmup_time', "0")

    def __str__(self) -> str:
        return str(self.__dict__)


class HotLoadSettings(PhaseSettings):

    HOT_READS = True


class XattrLoadSettings(PhaseSettings):

    SEQ_UPSERTS = True


class RestoreSettings:

    BACKUP_STORAGE = '/backups'
    BACKUP_REPO = ''
    IMPORT_FILE = ''

    THREADS = 16

    def __init__(self, options):
        self.backup_storage = options.get('backup_storage', self.BACKUP_STORAGE)
        self.backup_repo = options.get('backup_repo', self.BACKUP_REPO)
        self.import_file = options.get('import_file', self.IMPORT_FILE)

        self.threads = options.get('threads', self.THREADS)

    def __str__(self) -> str:
        return str(self.__dict__)


class XDCRSettings:

    WAN_DELAY = 0
    NUM_XDCR_LINKS = 1
    XDCR_LINKS_PRIORITY = 'HIGH'

    def __init__(self, options: dict):
        self.demand_encryption = options.get('demand_encryption')
        self.filter_expression = options.get('filter_expression')
        self.secure_type = options.get('secure_type')
        self.wan_delay = int(options.get('wan_delay',
                                         self.WAN_DELAY))

        self.num_xdcr_links = int(options.get('num_xdcr_links', self.NUM_XDCR_LINKS))
        self.xdcr_links_priority = options.get('xdcr_links_priority',
                                               self.XDCR_LINKS_PRIORITY).split(',')

    def __str__(self) -> str:
        return str(self.__dict__)


class ViewsSettings:

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

    CBINDEXPERF_CONFIGFILE = ''
    CBINDEXPERF_CONFIGFILES = ''
    RUN_RECOVERY_TEST = 0
    INCREMENTAL_LOAD_ITERATIONS = 0
    SCAN_TIME = 1200
    INCREMENTAL_ONLY = 0
    REPORT_INITIAL_BUILD_TIME = 0

    def __init__(self, options: dict):
        self.indexes = {}
        if options.get('indexes') is not None:
            for index_def in options.get('indexes').split('#'):
                name, field = index_def.split(':')
                if '"' in field:
                    field = field.replace('"', '\\\"')
                self.indexes[name] = field

        self.cbindexperf_configfile = options.get('cbindexperf_configfile',
                                                  self.CBINDEXPERF_CONFIGFILE)
        self.cbindexperf_configfiles = options.get('cbindexperf_configfiles',
                                                   self.CBINDEXPERF_CONFIGFILES)
        self.run_recovery_test = int(options.get('run_recovery_test',
                                                 self.RUN_RECOVERY_TEST))
        self.incremental_only = int(options.get('incremental_only',
                                                self.INCREMENTAL_ONLY))
        self.incremental_load_iterations = int(options.get('incremental_load_iterations',
                                                           self.INCREMENTAL_LOAD_ITERATIONS))
        self.scan_time = int(options.get('scan_time', self.SCAN_TIME))
        self.report_initial_build_time = int(options.get('report_initial_build_time',
                                                         self.REPORT_INITIAL_BUILD_TIME))

        self.settings = {}
        for option in options:
            if option.startswith(('indexer', 'projector', 'queryport')):
                value = options.get(option)
                if '.' in value:
                    self.settings[option] = maybe_atoi(value, t=float)
                else:
                    self.settings[option] = maybe_atoi(value, t=int)

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

    def __init__(self, options: dict):
        self.num_connections = int(options.get('num_connections',
                                               self.NUM_CONNECTIONS))

    def __str__(self) -> str:
        return str(self.__dict__)


class N1QLSettings:

    def __init__(self, options: dict):
        self.cbq_settings = {
            option: maybe_atoi(value) for option, value in options.items()
        }

    def __str__(self) -> str:
        return str(self.__dict__)


class IndexSettings:

    FTS_INDEX_NAME = ''
    FTS_INDEX_CONFIG_FILE = ''
    TOP_DOWN = False
    INDEXES_PER_COLLECTION = 1
    REPLICAS = 0

    def __init__(self, options: dict):
        self.raw_statements = options.get('statements')
        self.fields = options.get('fields')
        self.replicas = int(options.get('replicas', self.REPLICAS))
        self.collection_map = options.get('collection_map')
        self.indexes_per_collection = int(options.get('indexes_per_collection',
                                                      self.INDEXES_PER_COLLECTION))
        self.top_down = bool(options.get('top_down', self.TOP_DOWN))

        self.statements = self.create_index_statements()
        self.couchbase_fts_index_name = options.get('couchbase_fts_index_name',
                                                    self.FTS_INDEX_NAME)
        self.couchbase_fts_index_configfile = options.get('couchbase_fts_index_configfile',
                                                          self.FTS_INDEX_CONFIG_FILE)

    def create_index_statements(self) -> List[str]:
        #  Here we generate all permutations of all subsets of index fields
        #  The total number generate given n fields is the following:
        #
        #  Sum from k=0 to n, n!/k! where
        #
        #  n=3  sum = 16
        #  n=4  sum = 65
        #  n=5  sum = 326
        #  n=6  sum = 1957
        if self.collection_map and self.fields:
            statements = []
            if self.fields.strip() == 'primary':
                for bucket in self.collection_map.keys():
                    for scope in self.collection_map[bucket].keys():
                        for collection in self.collection_map[bucket][scope].keys():
                            if self.collection_map[bucket][scope][collection]['load'] == 1:
                                index_name = 'primary_idx_{}_{}_{}'\
                                    .format(bucket, scope, collection)
                                index_name = index_name.replace("-", "_")
                                new_statement = \
                                    "CREATE PRIMARY INDEX {} ON default:`{}`.`{}`.`{}`". \
                                    format(index_name, bucket, scope, collection)
                                if self.replicas > 0:
                                    new_statement = \
                                        new_statement + \
                                        " WITH {'num_replica': "+str(self.replicas)+"}"
                                statements.append(new_statement)
            else:
                fields = self.fields.strip().split(',')
                field_combos = list(chain.from_iterable(combinations(fields, r)
                                                        for r in range(1, len(fields)+1)))
                if self.top_down:
                    field_combos.reverse()
                for bucket in self.collection_map.keys():
                    for scope in self.collection_map[bucket].keys():
                        for collection in self.collection_map[bucket][scope].keys():
                            if self.collection_map[bucket][scope][collection]['load'] == 1:
                                indexes_created = 0
                                for field_subset in field_combos:
                                    subset_permutations = list(permutations(list(field_subset)))
                                    for permutation in subset_permutations:
                                        index_field_list = list(permutation)
                                        index_name = "_".join(index_field_list)
                                        index_fields = ",".join(index_field_list)
                                        new_statement = \
                                            "CREATE INDEX {} ON default:`{}`.`{}`.`{}`({})".\
                                            format(
                                                index_name,
                                                bucket,
                                                scope,
                                                collection,
                                                index_fields)
                                        if self.replicas > 0:
                                            new_statement = \
                                                new_statement + \
                                                " WITH {'num_replica': "+str(self.replicas)+"}"
                                        statements.append(new_statement)
                                        indexes_created += 1
                                        if indexes_created == self.indexes_per_collection:
                                            break
                                    if indexes_created == self.indexes_per_collection:
                                        break
            return statements
        elif self.raw_statements:
            return self.raw_statements.strip().split('\n')
        elif self.raw_statements is None and self.fields is None:
            return []
        else:
            raise Exception('Index options must include one statement, '
                            'or fields (if collections enabled)')

    @property
    def indexes(self) -> List[str]:
        indexes = []
        for statement in self.statements:
            match = re.search(r'CREATE .*INDEX (.*) ON', statement)
            if match:
                indexes.append(match.group(1))
        return indexes

    def __str__(self) -> str:
        return str(self.__dict__)


class N1QLFunctionSettings(IndexSettings):
    pass


class AccessSettings(PhaseSettings):

    OPS = float('inf')

    def define_queries(self, config):
        queries = []
        for query_name in self.n1ql_queries:
            query = config.get_n1ql_query_definition(query_name)
            queries.append(query)
        self.n1ql_queries = queries


class BackupSettings:

    COMPRESSION = False

    # Undefined test parameters will use backup's default
    THREADS = None
    STORAGE_TYPE = None
    SINK_TYPE = None
    SHARDS = None
    OBJ_STAGING_DIR = None
    OBJ_REGION = None
    AWS_CREDENTIAL_PATH = None

    def __init__(self, options: dict):
        self.compression = int(options.get('compression', self.COMPRESSION))
        self.threads = options.get('threads', self.THREADS)
        self.storage_type = options.get('storage_type', self.STORAGE_TYPE)
        self.sink_type = options.get('sink_type', self.SINK_TYPE)
        self.shards = options.get('shards', self.SHARDS)
        self.obj_staging_dir = options.get('obj_staging_dir', self.OBJ_STAGING_DIR)
        self.obj_region = options.get('obj_region', self.OBJ_REGION)
        self.aws_credential_path = options.get('aws_credential_path', self.AWS_CREDENTIAL_PATH)


class ExportSettings:

    THREADS = None
    IMPORT_FILE = None
    TYPE = 'json'  # csv or json
    FORMAT = 'lines'  # lines, list
    KEY_FIELD = None
    LOG_FILE = None
    FIELD_SEPARATOR = None
    LIMIT_ROWS = False
    SKIP_ROWS = False
    INFER_TYPES = False
    OMIT_EMPTY = False
    ERRORS_LOG = None  # error log file

    def __init__(self, options: dict):
        self.threads = options.get('threads', self.THREADS)
        self.type = options.get('type', self.TYPE)
        self.format = options.get('format', self.FORMAT)
        self.import_file = options.get('import_file', self.IMPORT_FILE)
        self.key_field = options.get('key_field', self.KEY_FIELD)
        self.log_file = options.get('log_file', self.LOG_FILE)
        self.log_file = options.get('log_file', self.LOG_FILE)
        self.field_separator = options.get('field_separator',
                                           self.FIELD_SEPARATOR)
        self.limit_rows = int(options.get('limit_rows', self.LIMIT_ROWS))
        self.skip_rows = int(options.get('skip_rows', self.SKIP_ROWS))
        self.infer_types = int(options.get('infer_types', self.INFER_TYPES))
        self.omit_empty = int(options.get('omit_empty', self.OMIT_EMPTY))
        self.errors_log = options.get('errors_log', self.ERRORS_LOG)


class EventingSettings:
    WORKER_COUNT = 3
    CPP_WORKER_THREAD_COUNT = 2
    TIMER_WORKER_POOL_SIZE = 1
    WORKER_QUEUE_CAP = 100000
    TIMER_TIMEOUT = 0
    TIMER_FUZZ = 0
    CONFIG_FILE = "tests/eventing/config/function_sample.json"
    REQUEST_URL = "http://172.23.96.38/cgi-bin/text/1kb_text_200ms.py"

    def __init__(self, options: dict):
        self.functions = {}
        if options.get('functions') is not None:
            for function_def in options.get('functions').split(','):
                name, filename = function_def.split(':')
                self.functions[name.strip()] = filename.strip()

        self.worker_count = int(options.get("worker_count", self.WORKER_COUNT))
        self.cpp_worker_thread_count = int(options.get("cpp_worker_thread_count",
                                                       self.CPP_WORKER_THREAD_COUNT))
        self.timer_worker_pool_size = int(options.get("timer_worker_pool_size",
                                                      self.TIMER_WORKER_POOL_SIZE))
        self.worker_queue_cap = int(options.get("worker_queue_cap",
                                                self.WORKER_QUEUE_CAP))
        self.timer_timeout = int(options.get("timer_timeout",
                                             self.TIMER_TIMEOUT))
        self.timer_fuzz = int(options.get("timer_fuzz",
                                          self.TIMER_FUZZ))
        self.config_file = options.get("config_file", self.CONFIG_FILE)
        self.request_url = options.get("request_url", self.REQUEST_URL)

    def __str__(self) -> str:
        return str(self.__dict__)


class MagmaSettings:
    COLLECT_PER_SERVER_STATS = 0

    def __init__(self, options: dict):
        self.collect_per_server_stats = int(options.get("collect_per_server_stats",
                                                        self.COLLECT_PER_SERVER_STATS))


class AnalyticsSettings:

    NUM_IO_DEVICES = 1
    DEFAULT_LOG_LEVEL = "DEBUG"
    CACHE_PAGE_SIZE = 131072
    STORAGE_COMPRESSION_BLOCK = None

    def __init__(self, options: dict):
        self.num_io_devices = int(options.get('num_io_devices',
                                              self.NUM_IO_DEVICES))
        self.log_level = options.get("log_level", self.DEFAULT_LOG_LEVEL)
        self.storage_buffer_cache_pagesize = options.get("cache_page_size", self.CACHE_PAGE_SIZE)
        self.storage_compression_block = options.get("storage_compression_block",
                                                     self.STORAGE_COMPRESSION_BLOCK)


class AuditSettings:

    ENABLED = True

    EXTRA_EVENTS = ''

    def __init__(self, options: dict):
        self.enabled = bool(options.get('enabled', self.ENABLED))
        self.extra_events = set(options.get('extra_events',
                                            self.EXTRA_EVENTS).split())


class YCSBSettings:

    REPO = 'git://github.com/couchbaselabs/YCSB.git'
    BRANCH = 'master'
    SDK_VERSION = None
    LATENCY_PERCENTILES = [98]
    AVERAGE_LATENCY = 0

    def __init__(self, options: dict):
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)
        self.sdk_version = options.get('sdk_version', self.SDK_VERSION)
        self.latency_percentiles = options.get('latency_percentiles', self.LATENCY_PERCENTILES)
        if isinstance(self.latency_percentiles, str):
            self.latency_percentiles = [int(x) for x in self.latency_percentiles.split(',')]
        self.average_latency = int(options.get('average_latency', self.AVERAGE_LATENCY))

    def __str__(self) -> str:
        return str(self.__dict__)


class SDKTestingSettings:

    ENABLE_SDKTEST = 0
    SDK_TYPE = ['java', 'libc', 'python']

    def __init__(self, options: dict):
        self.enable_sdktest = int(options.get('enable_sdktest', self.ENABLE_SDKTEST))
        self.sdk_type = self.SDK_TYPE + options.get('sdk_type', '').split()

    def __str__(self) -> str:
        return str(self.__dict__)


class ClientSettings:

    LIBCOUCHBASE = '2.9.3'
    PYTHON_CLIENT = '2.5.0'
    PILLOWFIGHT = '0.0.0'

    def __init__(self, options: dict):
        self.libcouchbase = options.get('libcouchbase', self.LIBCOUCHBASE)
        self.python_client = options.get('python_client', self.PYTHON_CLIENT)
        if options.get('pillowfight'):
            self.pillowfight = options.get('pillowfight')

    def __str__(self) -> str:
        return str(self.__dict__)


class JavaDCPSettings:

    REPO = 'git://github.com/couchbase/java-dcp-client.git'

    BRANCH = 'master'

    COMMIT = None

    STREAM = 'all'

    CLIENTS = 1

    def __init__(self, options: dict):
        self.config = options.get('config')
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)
        self.commit = options.get('commit', self.COMMIT)
        self.stream = options.get('stream', self.STREAM)
        self.clients = int(options.get('clients', self.CLIENTS))

    def __str__(self) -> str:
        return str(self.__dict__)


class MagmaBenchmarkSettings:

    NUM_KVSTORES = 1
    WRITE_BATCHSIZE = 1000
    KEY_LEN = 40
    DOC_SIZE = 1024
    NUM_DOCS = 100000000
    NUM_WRITES = 100000000
    NUM_READS = 10000000
    NUM_READERS = 32
    MEM_QUOTA = 1048576
    FS_CACHE_SIZE = 5368709120
    WRITE_MULTIPLIER = 5
    DATA_DIR = "/data"
    ENGINE = "magma"
    ENGINE_CONFIG = ""

    def __init__(self, options: dict):
        self.num_kvstores = int(options.get('num_kvstores', self.NUM_KVSTORES))
        self.write_batchsize = int(options.get('write_batchsize', self.WRITE_BATCHSIZE))
        self.key_len = int(options.get('key_len', self.KEY_LEN))
        self.doc_size = int(options.get('doc_size', self.DOC_SIZE))
        self.num_docs = int(options.get('num_docs', self.NUM_DOCS))
        self.num_writes = int(options.get('num_writes', self.NUM_WRITES))
        self.num_reads = int(options.get('num_reads', self.NUM_READS))
        self.num_readers = int(options.get('num_readers', self.NUM_READERS))
        self.memquota = int(options.get('memquota', self.MEM_QUOTA))
        self.fs_cache_size = int(options.get('fs_cache_size', self.FS_CACHE_SIZE))
        self.write_multiplier = int(options.get('write_multiplier', self.WRITE_MULTIPLIER))
        self.data_dir = options.get('data_dir', self.DATA_DIR)
        self.engine = options.get('engine', self.ENGINE)
        self.engine_config = options.get('engine_config', self.ENGINE_CONFIG)

    def __str__(self) -> str:
        return str(self.__dict__)


class TPCDSLoaderSettings:

    REPO = 'git://github.com/couchbaselabs/cbas-perf-support.git'
    BRANCH = 'master'

    def __init__(self, options: dict):
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
    def showfast(self) -> ShowFastSettings:
        options = self._get_options_as_dict('showfast')
        return ShowFastSettings(options)

    @property
    def cluster(self) -> ClusterSettings:
        options = self._get_options_as_dict('cluster')
        return ClusterSettings(options)

    @property
    def bucket(self) -> BucketSettings:
        options = self._get_options_as_dict('bucket')
        return BucketSettings(options)

    @property
    def collection(self) -> CollectionSettings:
        options = self._get_options_as_dict('collection')
        return CollectionSettings(options)

    @property
    def users(self) -> UserSettings:
        options = self._get_options_as_dict('users')
        return UserSettings(options)

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
        load_options = self._get_options_as_dict('load')
        load_settings = LoadSettings(load_options)

        client_options = self._get_options_as_dict('clients')
        client_settings = ClientSettings(client_options)
        if hasattr(client_settings, "pillowfight"):
            load_settings.custom_pillowfight = True

        collection_options = self._get_options_as_dict('collection')
        collection_settings = CollectionSettings(collection_options)
        if collection_settings.collection_map is not None:
            load_settings.collections = collection_settings.collection_map

        return load_settings

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
        hot_load.key_fmtr = load.key_fmtr

        client_options = self._get_options_as_dict('clients')
        client_settings = ClientSettings(client_options)
        if hasattr(client_settings, "pillowfight"):
            hot_load.custom_pillowfight = True

        collection_options = self._get_options_as_dict('collection')
        collection_settings = CollectionSettings(collection_options)
        if collection_settings.collection_map is not None:
            hot_load.collections = collection_settings.collection_map
        return hot_load

    @property
    def xattr_load_settings(self) -> XattrLoadSettings:
        options = self._get_options_as_dict('xattr_load')
        return XattrLoadSettings(options)

    @property
    def xdcr_settings(self) -> XDCRSettings:
        options = self._get_options_as_dict('xdcr')
        return XDCRSettings(options)

    @property
    def views_settings(self) -> ViewsSettings:
        options = self._get_options_as_dict('views')
        return ViewsSettings(options)

    @property
    def gsi_settings(self) -> GSISettings:
        options = self._get_options_as_dict('secondary')
        return GSISettings(options)

    @property
    def dcp_settings(self) -> DCPSettings:
        options = self._get_options_as_dict('dcp')
        return DCPSettings(options)

    @property
    def index_settings(self) -> IndexSettings:
        options = self._get_options_as_dict('index')
        collection_options = self._get_options_as_dict('collection')
        collection_settings = CollectionSettings(collection_options)
        if collection_settings.collection_map is not None:
            options['collection_map'] = collection_settings.collection_map
        return IndexSettings(options)

    @property
    def n1ql_function_settings(self) -> N1QLFunctionSettings:
        options = self._get_options_as_dict('n1ql_function')
        return N1QLFunctionSettings(options)

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

        java_dcp_options = self._get_options_as_dict('java_dcp')
        java_dcp_settings = JavaDCPSettings(java_dcp_options)
        access.java_dcp_config = java_dcp_settings.config
        access.java_dcp_clients = java_dcp_settings.clients
        access.java_dcp_stream = java_dcp_settings.stream

        client_options = self._get_options_as_dict('clients')
        client_settings = ClientSettings(client_options)
        if hasattr(client_settings, "pillowfight"):
            access.custom_pillowfight = True

        user_options = self._get_options_as_dict('users')
        user_settings = UserSettings(user_options)
        access.users = user_settings.num_users_per_bucket

        collection_options = self._get_options_as_dict('collection')
        collection_settings = CollectionSettings(collection_options)
        if collection_settings.collection_map is not None:
            access.collections = collection_settings.collection_map

        if hasattr(access, 'n1ql_queries'):
            access.define_queries(self)

        load_settings = self.load_settings
        access.doc_gen = load_settings.doc_gen
        access.range_distance = load_settings.range_distance
        access.array_size = load_settings.array_size
        access.num_categories = load_settings.num_categories
        access.num_replies = load_settings.num_replies
        access.size = load_settings.size
        access.key_fmtr = load_settings.key_fmtr
        access.field_count = load_settings.field_count
        access.field_length = load_settings.field_length
        access.bucket_list = self.buckets

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
    def jts_access_settings(self) -> JTSAccessSettings:
        options = self._get_options_as_dict('jts')
        return JTSAccessSettings(options)

    @property
    def ycsb_settings(self) -> YCSBSettings:
        options = self._get_options_as_dict('ycsb')
        return YCSBSettings(options)

    @property
    def sdktesting_settings(self) -> SDKTestingSettings:
        options = self._get_options_as_dict('sdktesting')
        return SDKTestingSettings(options)

    @property
    def eventing_settings(self) -> EventingSettings:
        options = self._get_options_as_dict('eventing')
        return EventingSettings(options)

    @property
    def magma_settings(self) -> MagmaSettings:
        options = self._get_options_as_dict('magma')
        return MagmaSettings(options)

    @property
    def analytics_settings(self) -> AnalyticsSettings:
        options = self._get_options_as_dict('analytics')
        return AnalyticsSettings(options)

    @property
    def audit_settings(self) -> AuditSettings:
        options = self._get_options_as_dict('audit')
        return AuditSettings(options)

    def get_n1ql_query_definition(self, query_name: str) -> dict:
        return self._get_options_as_dict('n1ql-{}'.format(query_name))

    @property
    def fio(self) -> dict:
        return self._get_options_as_dict('fio')

    @property
    def java_dcp_settings(self) -> JavaDCPSettings:
        options = self._get_options_as_dict('java_dcp')
        return JavaDCPSettings(options)

    @property
    def client_settings(self) -> ClientSettings:
        options = self._get_options_as_dict('clients')
        return ClientSettings(options)

    @property
    def magma_benchmark_settings(self) -> MagmaBenchmarkSettings:
        options = self._get_options_as_dict('magma_benchmark')
        return MagmaBenchmarkSettings(options)

    @property
    def tpcds_loader_settings(self) -> TPCDSLoaderSettings:
        options = self._get_options_as_dict('TPCDSLoader')
        return TPCDSLoaderSettings(options)


class TargetSettings:

    def __init__(self, host: str, bucket: str, password: str, prefix: str):
        self.password = password
        self.node = host
        self.bucket = bucket
        self.prefix = prefix

    @property
    def connection_string(self) -> str:
        return 'couchbase://{username}:{password}@{host}/{bucket}'.format(
            username=self.bucket,  # Backward compatibility
            password=self.password,
            host=self.node,
            bucket=self.bucket,
        )


class TargetIterator(Iterable):

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 prefix: str = None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.prefix = prefix

    def __iter__(self) -> Iterator[TargetSettings]:
        password = self.test_config.bucket.password
        prefix = self.prefix
        for master in self.cluster_spec.masters:
            for bucket in self.test_config.buckets:
                if self.prefix is None:
                    prefix = target_hash(master)
                yield TargetSettings(master, bucket, password, prefix)

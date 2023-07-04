import csv
import json
import os
import re
from configparser import ConfigParser, NoOptionError, NoSectionError
from itertools import chain, combinations, permutations
from typing import Dict, Iterable, Iterator, List, Tuple

from decorator import decorator

from logger import logger
from perfrunner.helpers.misc import (
    maybe_atoi,
    run_local_shell_command,
    target_hash,
)

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
        self.fname = ''

    def parse(self, fname: str, override=None):
        logger.info('Reading configuration file: {}'.format(fname))
        if not os.path.isfile(fname):
            logger.interrupt("File doesn't exist: {}".format(fname))
        self.config.optionxform = str
        self.config.read(fname)

        self.fname = fname
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

    def update_spec_file(self):
        with open(self.fname, 'w') as f:
            self.config.write(f)

    @safe
    def _get_options_as_dict(self, section: str) -> dict:
        if section in self.config.sections():
            return {p: v for p, v in self.config.items(section)}
        else:
            return {}


class ClusterSpec(Config):

    @property
    def dynamic_infrastructure(self):
        return self.cloud_infrastructure and self.kubernetes_infrastructure

    @property
    def cloud_infrastructure(self):
        if 'infrastructure' in self.config.sections():
            return True
        else:
            return False

    @property
    def cloud_provider(self):
        return self.config.get('infrastructure', 'provider', fallback='')

    @property
    def capella_backend(self):
        return self.config.get('infrastructure', 'backend', fallback='')

    @property
    def app_services(self):
        return self.config.get('infrastructure', 'app_services', fallback='')

    @property
    def kubernetes_infrastructure(self):
        if self.cloud_infrastructure and not self.capella_infrastructure:
            return self.infrastructure_settings.get("type", "kubernetes") == "kubernetes"
        return False

    @property
    def capella_infrastructure(self):
        if self.cloud_infrastructure:
            return self.infrastructure_settings.get("provider", "aws") == "capella"
        return False

    @property
    def serverless_infrastructure(self):
        return self.capella_infrastructure and \
            self.infrastructure_settings.get('capella_arch', 'dedicated') == 'serverless'

    @property
    def generated_cloud_config_path(self):
        if self.cloud_infrastructure:
            return "cloud/infrastructure/generated/infrastructure_config.json"
        else:
            return None

    @property
    def infrastructure_settings(self):
        return {k: v for k, v in self.config.items('infrastructure')}

    @property
    def infrastructure_clusters(self):
        return {k: v for k, v in self.config.items('clusters')}

    @property
    def infrastructure_clients(self):
        return {k: v for k, v in self.config.items('clients')}

    @property
    def infrastructure_syncgateways(self):
        if self.config.has_section('syncgateways'):
            return {k: v for k, v in self.config.items('syncgateways')}
        else:
            return {}

    @property
    def infrastructure_utilities(self):
        return {k: v for k, v in self.config.items('utilities')}

    def kubernetes_version(self, cluster_name):
        return self.infrastructure_section(cluster_name)\
            .get('version', '1.17')

    def istio_enabled(self, cluster_name):
        istio_enabled = self.infrastructure_section(cluster_name).get('istio_enabled', 0)
        istio_enabled = bool(int(istio_enabled))
        return istio_enabled

    def kubernetes_storage_class(self, cluster_name):
        return self.infrastructure_section(cluster_name) \
            .get('storage_class', 'default')

    def kubernetes_clusters(self):
        k8s_clusters = []
        if 'k8s' in self.config.sections():
            for k, v in self.config.items('k8s'):
                k8s_clusters += v.split(",")
        return k8s_clusters

    def infrastructure_section(self, section: str):
        if section in self.config.sections():
            return {k: v for k, v in self.config.items(section)}
        else:
            return {}

    def infrastructure_config(self):
        infra_config = {}
        for section in self.config.sections():
            infra_config[section] = {p: v for p, v in self.config.items(section)}
        return infra_config

    @property
    def clusters(self) -> Iterator:
        for cluster_name, servers in self.config.items('clusters'):
            hosts = [s.split(':')[0] for s in servers.split()]
            yield cluster_name, hosts

    @property
    def clusters_schemas(self) -> Iterator:
        if self.capella_infrastructure and not self.serverless_infrastructure:
            for cluster_name, servers in self.config.items('clusters_schemas'):
                schemas = servers.split()
                yield cluster_name, schemas

    @property
    def sgw_clusters(self) -> Iterator:
        for cluster_name, servers in self.config.items('syncgateways'):
            hosts = [s.split(':')[0] for s in servers.split()]
            yield cluster_name, hosts

    @property
    def masters(self) -> Iterator[str]:
        for _, servers in self.clusters:
            yield servers[0]

    @property
    def sgw_masters(self) -> Iterator[str]:
        if self.config.has_section('syncgateways'):
            for _, servers in self.sgw_clusters:
                yield servers[0]
        else:
            for _, servers in self.clusters:
                yield servers[0]

    @property
    def servers(self) -> List[str]:
        servers = [node for _, cluster_servers in self.clusters for node in cluster_servers]
        return servers

    @property
    def clients(self) -> List[str]:
        clients = []
        for client_servers in self.infrastructure_clients.values():
            clients += client_servers.strip().split('\n')
        return clients

    @property
    def utilities(self) -> List[str]:
        utilities = []
        for utility_servers in self.infrastructure_utilities.values():
            utilities += utility_servers.strip().split('\n')
        return utilities

    @property
    def sgw_servers(self) -> List[str]:
        servers = []
        if self.config.has_section('syncgateways'):
            for _, cluster_servers in self.sgw_clusters:
                for server in cluster_servers:
                    servers.append(server)
        return servers

    @property
    def using_private_cluster_ips(self) -> bool:
        return self.config.has_section('cluster_private_ips')

    @property
    def using_private_client_ips(self) -> bool:
        return self.config.has_section('client_private_ips')

    @property
    def using_private_utility_ips(self) -> bool:
        return self.config.has_section('utility_private_ips')

    @property
    def using_private_syncgateway_ips(self) -> bool:
        return self.config.has_section('syncgateway_private_ips')

    @property
    def clusters_private(self) -> Iterator:
        if self.using_private_cluster_ips:
            for cluster_name, private_ips in self.config.items('cluster_private_ips'):
                yield cluster_name, private_ips.split()

    @property
    def clients_private(self) -> Iterator:
        if self.using_private_client_ips:
            for cluster_name, private_ips in self.config.items('client_private_ips'):
                yield cluster_name, private_ips.split()

    @property
    def utilities_private(self) -> Iterator:
        if self.using_private_utility_ips:
            for cluster_name, private_ips in self.config.items('utility_private_ips'):
                yield cluster_name, private_ips.split()

    @property
    def syncgateways_private(self) -> Iterator:
        if self.using_private_syncgateway_ips:
            for cluster_name, private_ips in self.config.items('syncgateway_private_ips'):
                yield cluster_name, private_ips.split()

    @property
    def servers_public_to_private_ip(self) -> dict:
        private_clusters = dict(self.clusters_private)
        ip_map = {}
        for cluster, public_ips in self.clusters:
            private_ips = private_clusters.get(cluster, [])
            for i, public_ip in enumerate(public_ips):
                private_ip = private_ips[i] if private_ips else None
                ip_map[public_ip] = private_ip
        return ip_map

    @property
    def using_instance_ids(self):
        return self.config.has_section('cluster_instance_ids')

    @property
    def instance_ids_per_cluster(self):
        iids_per_cluster = {}
        if self.using_instance_ids:
            iids_per_cluster = {k: v.split() for k, v in self.config.items('cluster_instance_ids')}
        return iids_per_cluster

    @property
    def servers_hostname_to_instance_id(self) -> dict:
        iids_per_cluster = self.instance_ids_per_cluster
        host_map = {}
        for cluster, hostnames in self.clusters:
            iids = iids_per_cluster.get(cluster, [])
            for i, hostname in enumerate(hostnames):
                iid = iids[i] if iids else None
                host_map[hostname] = iid
        return host_map

    @property
    def instance_ids_per_nebula_cluster(self):
        return {k: v.split() for k, v in self.config.items('nebula_instance_ids')}

    @property
    def cluster_instance_ids(self) -> List[str]:
        iids = []
        for cluster_iids in self.instance_ids_per_cluster.values():
            iids += cluster_iids
        return iids

    @property
    def direct_nebula_instance_ids(self) -> List[str]:
        return self.instance_ids_per_nebula_cluster.get('direct_nebula', [])

    @property
    def dapi_instance_ids(self) -> List[str]:
        return self.instance_ids_per_nebula_cluster.get('dapi', [])

    @property
    def sgw_instance_ids_per_cluster(self):
        return {k: v.split() for k, v in self.config.items('sgw_instance_ids')}

    @property
    def sgw_instance_ids(self) -> List[str]:
        iids = []
        for sgw_iids in self.sgw_instance_ids_per_cluster.values():
            iids += sgw_iids
        return iids

    @property
    def server_instance_ids_by_role(self, role: str) -> List[str]:
        has_service = []
        if self.using_instance_ids:
            for cluster_name, servers in self.config.items('clusters'):
                for i, server in enumerate(servers.split()):
                    _, roles = server.split(':')
                    if role in roles:
                        has_service.append(self.instance_ids_per_cluster[cluster_name][i])
        return has_service

    def servers_by_role(self, role: str) -> List[str]:
        has_service = []
        for _, servers in self.config.items('clusters'):
            for server in servers.split():
                host, roles, *group = server.split(':')
                if role in roles:
                    has_service.append(host)
        return has_service

    def servers_by_cluster_and_role(self, role: str) -> List[str]:
        has_service = []
        for _, servers in self.config.items('clusters'):
            cluster_has_service = []
            for server in servers.split():
                host, roles, *group = server.split(':')
                if role in roles:
                    cluster_has_service.append(host)
            has_service.append(cluster_has_service)
        return has_service

    def servers_by_role_from_first_cluster(self, role: str) -> List[str]:
        has_service = []
        servers = self.config.items('clusters')[0][1]
        for server in servers.split():
            host, roles, *group = server.split(':')
            if role in roles:
                has_service.append(host)
        return has_service

    @property
    def roles(self) -> Dict[str, str]:
        server_roles = {}
        for _, servers in self.config.items('clusters'):
            for server in servers.split():
                host, roles, *group = server.split(':')
                server_roles[host] = roles
        return server_roles

    @property
    def servers_and_roles(self) -> List[Tuple[str, str]]:
        server_and_roles = []
        for _, servers in self.config.items('clusters'):
            for server in servers.split():
                host, roles, *group = server.split(':')
                server_and_roles.append((host, roles))
        return server_and_roles

    @property
    def workers(self) -> List[str]:
        if self.cloud_infrastructure:
            if self.kubernetes_infrastructure:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += ["{}.{}".format(k, host) for host in v.split()]
                return clients
            else:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += [host for host in v.split()]
                return clients
        else:
            return self.config.get('clients', 'hosts').split()

    @property
    def syncgateways(self) -> List[str]:
        if self.cloud_infrastructure:
            if self.kubernetes_infrastructure:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += ["{}.{}".format(k, host) for host in v.split()]
                return clients
            else:
                client_map = self.infrastructure_clients
                clients = []
                for k, v in client_map.items():
                    if "workers" in k:
                        clients += [host for host in v.split()]
                return clients
        else:
            return self.config.get('syncgateways', 'hosts').split()

    @property
    def brokers(self) -> List[str]:
        if self.cloud_infrastructure:
            if not self.kubernetes_infrastructure:
                util_map = self.infrastructure_utilities
                brokers = []
                for k, v in util_map.items():
                    if "brokers" in k:
                        brokers += [host for host in v.split()]
                return brokers
        return []

    @property
    def client_credentials(self) -> List[str]:
        if self.config.has_option('clients', 'credentials'):
            return self.config.get('clients', 'credentials').split(':')
        else:
            return self.ssh_credentials

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
    def capella_admin_credentials(self) -> List[List[str]]:
        return [
            creds.split(':')
            for creds in self.config.get('credentials', 'admin', fallback='').split()
        ]

    @property
    def ssh_credentials(self) -> List[str]:
        return self.config.get('credentials', 'ssh').split(':')

    @property
    def aws_key_name(self) -> List[str]:
        return self.config.get('credentials', 'aws_key_name')

    @property
    def parameters(self) -> dict:
        return self._get_options_as_dict('parameters')

    @property
    def server_group_map(self) -> dict:
        server_grp_map = {}
        for cluster_name, servers in self.config.items('clusters'):
            for server in servers.split():
                host, roles, *group = server.split(':')
                if len(group):
                    server_grp_map[host] = group[0]
        return server_grp_map

    def get_aws_iid(self, hostname: str, region: str) -> str:
        iid, _, _ = run_local_shell_command(
            (
                "env/bin/aws ec2 describe-instances --region {} "
                "--filter \"Name=ip-address,Values=$(dig +short {})\" "
                "--query \"Reservations[].Instances[].InstanceId\" "
                "--output text"
            ).format(region, hostname)
        )
        return iid.strip()

    def set_capella_instance_ids(self) -> None:
        if self.capella_backend == 'aws':
            logger.info('Getting cluster instance IDs')

            if not self.config.has_section('cluster_instance_ids'):
                self.config.add_section('cluster_instance_ids')

            region = os.environ.get('AWS_REGION', 'us-east-1')

            for cluster_name, hosts in self.clusters:
                iids = []
                for host in hosts:
                    iid = self.get_aws_iid(host, region)
                    logger.info('Instance ID for {}: {}'.format(host, iid))
                    iids.append(iid)
                self.config.set('cluster_instance_ids', cluster_name, '\n' + '\n'.join(iids))
            self.update_spec_file()

    def set_nebula_instance_ids(self) -> None:
        if self.capella_backend == 'aws':
            logger.info('Getting Nebula instance IDs')

            if not self.config.has_section('nebula_instance_ids'):
                self.config.add_section('nebula_instance_ids')

            region = os.environ.get('AWS_REGION', 'us-east-1')

            query = (
                "env/bin/aws ec2 describe-instances --region {} "
                "--filters \"Name=tag-key,Values={{}}\" "
                "\"Name=tag:couchbase-cloud-dataplane-id,Values={}\" "
                "--query \"Reservations[].Instances[].InstanceId\" "
                "--output text"
            ).format(region, self.infrastructure_settings['cbc_dataplane'])

            stdout, _, _ = run_local_shell_command(query.format('couchbase-cloud-nebula'))
            dn_iids = stdout.strip().split()
            logger.info('Found DN instance IDs: {}'.format(', '.join(dn_iids)))
            self.config.set('nebula_instance_ids', 'direct_nebula', '\n' + '\n'.join(dn_iids))

            stdout, _, _ = run_local_shell_command(query.format('couchbase-cloud-data-api'))
            dapi_iids = stdout.strip().split()
            logger.info('Found DAPI instance IDs: {}'.format(', '.join(dapi_iids)))
            self.config.set('nebula_instance_ids', 'dapi', '\n' + '\n'.join(dapi_iids))

        self.update_spec_file()

    def set_sgw_instance_ids(self) -> None:
        if not self.config.has_section('sgw_instance_ids'):
            self.config.add_section('sgw_instance_ids')

        if self.capella_backend == 'aws':
            region = os.environ.get('AWS_REGION', 'us-east-1')
            stdout, _, _ = run_local_shell_command(
                (
                    "env/bin/aws ec2 describe-instances --region {} "
                    "--filters \"Name=tag-key,Values=couchbase-cloud-syncgateway-id\" "
                    "\"Name=tag:couchbase-app-services,Values={}\" "
                    "--query \"Reservations[].Instances[].InstanceId\" "
                    "--output text"
                ).format(region, self.infrastructure_settings['cbc_cluster']),
            )
            sgids = stdout.strip().split()
            logger.info("Found Instance IDs for sgw: {}".format(', '.join(sgids)))
            self.config.set('sgw_instance_ids', 'sync_gateways', '\n', + '\n'.join(sgids))
            self.update_spec_file()

    @property
    def direct_nebula(self) -> dict:
        return self._get_options_as_dict('direct_nebula')

    @property
    def data_api(self) -> dict:
        return self._get_options_as_dict('data_api')


class TestCaseSettings:

    USE_WORKERS = 1
    RESET_WORKERS = 0
    THRESHOLD = -10

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

    MEM_QUOTA = 0
    INDEX_MEM_QUOTA = 256
    FTS_INDEX_MEM_QUOTA = 0
    ANALYTICS_MEM_QUOTA = 0
    EVENTING_MEM_QUOTA = 0

    EVENTING_BUCKET_MEM_QUOTA = 0
    EVENTING_METADATA_BUCKET_MEM_QUOTA = 0
    EVENTING_METADATA_BUCKET_NAME = 'eventing'
    EVENTING_BUCKETS = 0

    KERNEL_MEM_LIMIT = 0
    KV_KERNEL_MEM_LIMIT = 0
    KERNEL_MEM_LIMIT_SERVICES = 'fts', 'index'
    ONLINE_CORES = 0
    SGW_ONLINE_CORES = 0
    ENABLE_CPU_CORES = 'true'
    ENABLE_N2N_ENCRYPTION = None
    BUCKET_NAME = 'bucket-1'
    DISABLE_UI_HTTP = None
    SERVERLESS_MODE = None

    IPv6 = 0

    def __init__(self, options: dict):
        self.mem_quota = int(options.get('mem_quota', self.MEM_QUOTA))
        self.index_mem_quota = int(options.get('index_mem_quota',
                                               self.INDEX_MEM_QUOTA))
        self.fts_index_mem_quota = int(options.get('fts_index_mem_quota',
                                                   self.FTS_INDEX_MEM_QUOTA))
        self.analytics_mem_quota = int(options.get('analytics_mem_quota',
                                                   self.ANALYTICS_MEM_QUOTA))
        self.eventing_mem_quota = int(options.get('eventing_mem_quota',
                                                  self.EVENTING_MEM_QUOTA))

        self.initial_nodes = [
            int(nodes) for nodes in options.get('initial_nodes', '1').split()
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
        self.num_vbuckets = options.get('num_vbuckets', None)
        self.online_cores = int(options.get('online_cores',
                                            self.ONLINE_CORES))
        self.sgw_online_cores = int(options.get('sgw_online_cores',
                                                self.SGW_ONLINE_CORES))
        self.enable_cpu_cores = maybe_atoi(options.get('enable_cpu_cores', self.ENABLE_CPU_CORES))
        self.ipv6 = int(options.get('ipv6', self.IPv6))
        self.kernel_mem_limit = options.get('kernel_mem_limit',
                                            self.KERNEL_MEM_LIMIT)
        self.kv_kernel_mem_limit = options.get('kv_kernel_mem_limit',
                                               self.KV_KERNEL_MEM_LIMIT)
        self.enable_n2n_encryption = options.get('enable_n2n_encryption',
                                                 self.ENABLE_N2N_ENCRYPTION)
        self.ui_http = options.get('ui_http', self.DISABLE_UI_HTTP)
        self.serverless_mode = options.get('serverless_mode', self.SERVERLESS_MODE)

        self.serverless_throttle = {'dataThrottleLimit': int(options.get('data_throttle',
                                                                         0)),
                                    'indexThrottleLimit': int(options.get('index_throttle',
                                                                          0)),
                                    'searchThrottleLimit': int(options.get('search_throttle',
                                                                           0)),
                                    'queryThrottleLimit': int(options.get('query_throttle',
                                                                          0))}

        kernel_mem_limit_services = options.get('kernel_mem_limit_services')
        if kernel_mem_limit_services:
            self.kernel_mem_limit_services = kernel_mem_limit_services.split()
        else:
            self.kernel_mem_limit_services = self.KERNEL_MEM_LIMIT_SERVICES

        self.bucket_name = options.get('bucket_name', self.BUCKET_NAME)

        self.cloud_server_groups = options.get('bucket_name', self.BUCKET_NAME)


class DirectNebulaSettings:

    LOG_LEVEL = None

    def __init__(self, options: dict):
        self.log_level = options.get('log_level', self.LOG_LEVEL)


class DataApiSettings:

    LOG_LEVEL = None

    def __init__(self, options: dict):
        self.log_level = options.get('log_level', self.LOG_LEVEL)


class StatsSettings:

    ENABLED = 1
    POST_TO_SF = 0

    INTERVAL = 5
    LAT_INTERVAL = 1

    POST_CPU = 0

    CLIENT_PROCESSES = []
    SERVER_PROCESSES = ['beam.smp',
                        'ns_server',  # For metrics REST API collector
                        'cbft',
                        'cbq-engine',
                        'indexer',
                        'memcached',
                        'sync_gateway']
    TRACED_PROCESSES = []

    SECONDARY_STATSFILE = '/root/statsfile'

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
        self.secondary_statsfile = options.get('secondary_statsfile',
                                               self.SECONDARY_STATSFILE)


class ProfilingSettings:

    INTERVAL = 300  # 5 minutes

    NUM_PROFILES = 1

    PROFILES = 'cpu'

    SERVICES = ''

    CPU_INTERVAL = 10  # 10 seconds

    LINUX_PERF_PROFILE_DURATION = 10  # seconds

    LINUX_PERF_FREQUENCY = 99

    LINUX_PERF_CALLGRAPH = 'lbr'     # optional lbr, dwarf

    LINUX_PERF_DELAY_MULTIPLIER = 2

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
        self.linux_perf_profile_duration = int(options.get('linux_perf_profile_duration',
                                                           self.LINUX_PERF_PROFILE_DURATION))

        self.linux_perf_profile_flag = bool(options.get('linux_perf_profile_flag'))

        self.linux_perf_frequency = int(options.get('linux_perf_frequency',
                                                    self.LINUX_PERF_FREQUENCY))

        self.linux_perf_callgraph = options.get('linux_perf_callgraph',
                                                self.LINUX_PERF_CALLGRAPH)

        self.linux_perf_delay_multiplier = int(options.get('linux_perf_delay_multiplier',
                                                           self.LINUX_PERF_DELAY_MULTIPLIER))


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
    CONFLICT_RESOLUTION_TYPE = 'seqno'
    FLUSH = True
    MIN_DURABILITY = 'none'
    DOC_TTL_UNIT = None
    DOC_TTL_VALUE = 0
    MAGMA_SEQ_TREE_DATA_BLOCK_SIZE = 0
    HISTORY_SECONDS = 0
    HISTORY_BYTES = 0
    MAX_TTL = 0

    def __init__(self, options: dict):
        self.password = options.get('password', self.PASSWORD)

        self.replica_number = int(options.get('replica_number', self.REPLICA_NUMBER))

        self.replica_index = int(options.get('replica_index', self.REPLICA_INDEX))

        self.eviction_policy = options.get('eviction_policy', self.EVICTION_POLICY)

        self.bucket_type = options.get('bucket_type', self.BUCKET_TYPE)

        self.conflict_resolution_type = options.get('conflict_resolution_type',
                                                    self.CONFLICT_RESOLUTION_TYPE)

        self.compression_mode = options.get('compression_mode')

        if options.get('autofailover_enabled', self.AUTOFAILOVER_ENABLED).lower() == "false":
            self.autofailover_enabled = 'false'
        else:
            self.autofailover_enabled = 'true'

        self.failover_min = int(options.get('failover_min', self.FAILOVER_MIN))

        self.failover_max = int(options.get('failover_max', self.FAILOVER_MAX))

        self.backend_storage = options.get('backend_storage', self.BACKEND_STORAGE)

        self.flush = bool(options.get('flush', self.FLUSH))

        self.min_durability = options.get('min_durability', self.MIN_DURABILITY)

        self.doc_ttl_unit = options.get('doc_ttl_unit', self.DOC_TTL_UNIT)

        self.doc_ttl_value = options.get('doc_ttl_value', self.DOC_TTL_VALUE)

        self.magma_seq_tree_data_block_size = int(options.get('magma_seq_tree_data_block_size',
                                                              self.MAGMA_SEQ_TREE_DATA_BLOCK_SIZE))

        self.history_seconds = int(options.get('history_seconds', self.HISTORY_SECONDS))

        self.history_bytes = int(options.get('history_bytes', self.HISTORY_BYTES))

        self.max_ttl = int(options.get('max_ttl', self.MAX_TTL))


class CollectionSettings:

    CONFIG = None
    COLLECTION_MAP = None
    USE_BULK_API = 1
    SCOPES_PER_BUCKET = 0
    COLLECTIONS_PER_SCOPE = 0

    def __init__(self, options: dict, buckets: Iterable[str] = None):
        self.config = options.get('config', self.CONFIG)
        self.collection_map = self.COLLECTION_MAP
        self.use_bulk_api = int(options.get('use_bulk_api', self.USE_BULK_API))
        self.scopes_per_bucket = int(options.get('scopes_per_bucket', self.SCOPES_PER_BUCKET))
        self.collections_per_scope = int(options.get('collections_per_scope',
                                                     self.COLLECTIONS_PER_SCOPE))
        if self.config is not None:
            with open(self.config) as f:
                self.collection_map = json.load(f)
        elif self.scopes_per_bucket > 0 and buckets:
            self.collection_map = self.create_uniform_collection_map(buckets)

    def create_uniform_collection_map(self, buckets: Iterable[str]):
        coll_map = {
            bucket: {
                'scope-{}'.format(i + 1): {
                    'collection-{}'.format(j + 1): {
                        'access': 1,
                        'load': 1
                    }
                    for j in range(self.collections_per_scope)
                }
                for i in range(self.scopes_per_bucket)
            }
            for bucket in buckets
        }

        for bucket_scopes in coll_map.values():
            bucket_scopes['_default'] = {}

        return coll_map


class ServerlessDBSettings:

    INIT_CONFIG = None
    CONFIG = 'cloud/serverless_db/generated/db_config.json'
    INIT_DB_MAP = {}
    DB_MAP = {}

    def __init__(self, options: dict):
        self.init_config = options.get('init_config', self.INIT_CONFIG)
        self.config = options.get('config', self.CONFIG)
        self.init_db_map = self.INIT_DB_MAP
        self.db_map = self.DB_MAP

        if self.init_config is not None:
            with open(self.init_config) as f:
                self.init_db_map = json.load(f)

        if self.config is not None:
            with open(self.config) as f:
                try:
                    self.db_map = json.load(f)
                except json.JSONDecodeError:
                    self.db_map = {}

    def bucket_creds(self, bucket: str) -> Tuple[str, str]:
        access = self.db_map[bucket]['access']
        secret = self.db_map[bucket]['secret']
        return (access, secret)

    def update_db_map(self, db_map):
        with open(self.config, 'w') as f:
            json.dump(db_map, f, indent=4)


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
    MAGMA_FRAGMENTATION_PERCENTAGE = 50

    def __init__(self, options: dict):
        self.db_percentage = options.get('db_percentage',
                                         self.DB_PERCENTAGE)
        self.view_percentage = options.get('view_percentage',
                                           self.VIEW_PERCENTAGE)
        self.parallel = options.get('parallel', self.PARALLEL)
        self.bucket_compaction = options.get('bucket_compaction', self.BUCKET_COMPACTION)
        self.magma_fragmentation_percentage = options.get('magma_fragmentation_percentage',
                                                          self.MAGMA_FRAGMENTATION_PERCENTAGE)

    def __str__(self):
        return str(self.__dict__)


class RebalanceSettings:

    SWAP = 0
    FAILOVER = 'hard'  # Atl: graceful
    DELTA_RECOVERY = 0  # Full recovery by default
    DELAY_BEFORE_FAILOVER = 600
    START_AFTER = 1200
    STOP_AFTER = 1200
    FTS_PARTITIONS = "1"
    FTS_MAX_DCP_PARTITIONS = "0"

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
        self.services = options.get('services', 'kv')
        self.rebalance_config = options.get('rebalance_config', None)

        # The reblance settings for FTS
        self.ftspartitions = options.get('ftspartitions', self.FTS_PARTITIONS)
        self.fts_max_dcp_partitions = options.get('fts_max_dcp_partitions',
                                                  self.FTS_MAX_DCP_PARTITIONS)
        self.fts_node_level_parameters = {}
        if self.ftspartitions != self.FTS_PARTITIONS:
            self.fts_node_level_parameters["maxConcurrentPartitionMovesPerNode"] = \
                self.ftspartitions
        if self.fts_max_dcp_partitions != self.FTS_MAX_DCP_PARTITIONS:
            self.fts_node_level_parameters["maxFeedsPerDCPAgent"] = self.fts_max_dcp_partitions


class PhaseSettings:

    TIME = 3600 * 24
    USE_BACKUP = 'false'
    RESET_THROTTLE_LIMIT = 'true'

    DOC_GEN = 'basic'
    POWER_ALPHA = 0
    ZIPF_ALPHA = 0
    KEY_PREFIX = None

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

    TIMESERIES_REGULAR = 'false'
    TIMESERIES_ENABLE = 'true'
    TIMESERIES_START = 0
    TIMESERIES_HOURS_PER_DOC = 1
    TIMESERIES_DOCS_PER_DEVICE = 1
    TIMESERIES_TOTAL_DAYS = 1

    BATCH_SIZE = 1000
    BATCHES = 1
    SPRING_BATCH_SIZE = 100

    ITERATIONS = 1

    ASYNC = False

    KEY_FMTR = 'decimal'

    ITEMS = 0
    SIZE = 2048
    ADDITIONAL_ITEMS = 0

    PHASE = 0
    INSERT_TEST_FLAG = 0

    MEM_LOW_WAT = 0
    MEM_HIGH_WAT = 0

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
    FTS_DATA_SPREAD_WORKERS = None
    FTS_DATA_SPREAD_WORKER_TYPE = "default"
    WORKLOAD_INSTANCES = 1

    N1QL_OP = 'read'
    N1QL_BATCH_SIZE = 100
    N1QL_TIMEOUT = 0
    N1QL_QUERY_WEIGHT = ""

    ARRAY_SIZE = 10
    NUM_CATEGORIES = 10 ** 6
    NUM_REPLIES = 100
    RANGE_DISTANCE = 10

    ITEM_SIZE = 64
    SIZE_VARIATION_MIN = 1
    SIZE_VARIATION_MAX = 1024

    RECORDED_LOAD_CACHE_SIZE = 0
    INSERTS_PER_WORKERINSTANCE = 0

    RUN_EXTRA_ACCESS = 'false'

    EPOLL = 'true'
    BOOST = 48

    YCSB_FIELD_COUNT = 10
    YCSB_FIELD_LENGTH = 100
    YCSB_INSERTSTART = 0

    SSL_MODE = 'none'
    SSL_AUTH_KEYSTORE = "certificates/auth.keystore"
    SSL_DATA_KEYSTORE = "certificates/data.keystore"
    SSL_KEYSTOREPASS = "storepass"
    CERTIFICATE_FILE = "root.pem"
    SHOW_TLS_VERSION = False
    CIPHER_LIST = None
    MIN_TLS_VERSION = None

    PERSIST_TO = 0
    REPLICATE_TO = 0

    TIMESERIES = 0

    CBCOLLECT = 0

    CONNSTR_PARAMS = "{'ipv6': 'allow', 'enable_tracing': 'false'}"

    YCSB_CLIENT = 'couchbase2'
    YCSB_DEFAULT_WORKLOAD_PATH = 'workloads/workloada'

    DURABILITY = None

    YCSB_KV_ENDPOINTS = 1
    YCSB_ENABLE_MUTATION_TOKEN = None

    YCSB_RETRY_STRATEGY = 'default'
    YCSB_RETRY_LOWER = 1
    YCSB_RETRY_UPPER = 500
    YCSB_RETRY_FACTOR = 2
    YCSB_OUT_OF_ORDER = 0
    YCSB_SPLIT_WORKLOAD = 0

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
    SPLIT_WORKLOAD = None
    SPLIT_WORKLOAD_THROUGHPUT = 0
    SPLIT_WORKLOAD_WORKERS = 0

    DOCUMENT_GROUPS = 1
    N1QL_SHUTDOWN_TYPE = None

    LATENCY_PERCENTILES = [99.9]

    WORKLOAD_MIX = None
    NUM_BUCKETS = 0
    NEBULA_MODE = 'none'  # options: none, nebula, dapi

    DAPI_REQUEST_META = 'false'
    DAPI_REQUEST_LOGS = 'false'

    def __init__(self, options: dict):
        # Common settings
        self.time = int(options.get('time', self.TIME))
        self.use_backup = maybe_atoi(options.get('use_backup', self.USE_BACKUP))
        self.reset_throttle_limit = maybe_atoi(options.get('reset_throttle_limit',
                                                           self.RESET_THROTTLE_LIMIT))

        # KV settings
        self.doc_gen = options.get('doc_gen', self.DOC_GEN)
        self.power_alpha = float(options.get('power_alpha', self.POWER_ALPHA))
        self.zipf_alpha = float(options.get('zipf_alpha', self.ZIPF_ALPHA))
        self.key_prefix = options.get('key_prefix', self.KEY_PREFIX)

        self.size = int(options.get('size', self.SIZE))
        self.items = int(options.get('items', self.ITEMS))
        self.additional_items = int(options.get('additional_items', self.ADDITIONAL_ITEMS))

        self.phase = int(options.get('phase', self.PHASE))
        self.insert_test_flag = int(options.get('insert_test_flag', self.INSERT_TEST_FLAG))

        self.mem_low_wat = int(options.get('mem_low_wat', self.MEM_LOW_WAT))
        self.mem_high_wat = int(options.get('mem_high_wat', self.MEM_HIGH_WAT))

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
        self.run_async = bool(int(options.get('async', self.ASYNC)))
        self.key_fmtr = options.get('key_fmtr', self.KEY_FMTR)

        self.hot_reads = self.HOT_READS
        self.seq_upserts = self.SEQ_UPSERTS

        self.timeseries_regular = maybe_atoi(options.get('timeseries_regular',
                                                         self.TIMESERIES_REGULAR))
        self.timeseries_enable = maybe_atoi(options.get('timeseries_enable',
                                                        self.TIMESERIES_ENABLE))
        self.timeseries_start = int(options.get('timeseries_start', self.TIMESERIES_START))
        self.timeseries_hours_per_doc = int(options.get('timeseries_hours_per_doc',
                                                        self.TIMESERIES_HOURS_PER_DOC))
        self.timeseries_docs_per_device = int(options.get('timeseries_docs_per_device',
                                                          self.TIMESERIES_DOCS_PER_DEVICE))
        self.timeseries_total_days = int(options.get('timeseries_total_days',
                                                     self.TIMESERIES_TOTAL_DAYS))

        self.iterations = int(options.get('iterations', self.ITERATIONS))

        self.batch_size = int(options.get('batch_size', self.BATCH_SIZE))
        self.batches = int(options.get('batches', self.BATCHES))
        self.spring_batch_size = int(options.get('spring_batch_size', self.SPRING_BATCH_SIZE))

        self.workload_instances = int(options.get('workload_instances',
                                                  self.WORKLOAD_INSTANCES))

        self.connstr_params = eval(options.get('connstr_params', self.CONNSTR_PARAMS))

        self.run_extra_access = maybe_atoi(options.get('run_extra_access', self.RUN_EXTRA_ACCESS))

        self.latency_percentiles = options.get('latency_percentiles', self.LATENCY_PERCENTILES)
        if isinstance(self.latency_percentiles, str):
            self.latency_percentiles = [float(x) for x in self.latency_percentiles.split(',')]

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
            n1ql_query_weight = options.get('n1ql_query_weight', self.N1QL_QUERY_WEIGHT).strip()
            if len(n1ql_query_weight):
                self.n1ql_query_weight = [int(w) for w in n1ql_query_weight.split(',')]
            else:
                self.n1ql_query_weight = [1] * len(self.n1ql_queries)

        # 2i settings
        self.item_size = int(options.get('item_size', self.ITEM_SIZE))
        self.size_variation_min = int(options.get('size_variation_min',
                                                  self.SIZE_VARIATION_MIN))
        self.size_variation_max = int(options.get('size_variation_max',
                                                  self.SIZE_VARIATION_MAX))

        # Syncgateway settings
        self.syncgateway_settings = None

        # YCSB settings
        self.workload_path = options.get('workload_path', self.YCSB_DEFAULT_WORKLOAD_PATH)
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
        self.insertstart = int(options.get('insertstart', self.YCSB_INSERTSTART))
        self.ycsb_split_workload = int(options.get('ycsb_split_workload', self.YCSB_SPLIT_WORKLOAD))

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
        self.certificate_file = self.CERTIFICATE_FILE
        self.show_tls_version = options.get('show_tls_version', self.SHOW_TLS_VERSION)
        self.cipher_list = options.get('cipher_list', self.CIPHER_LIST)
        if self.cipher_list:
            self.cipher_list = self.cipher_list.split(',')

        self.min_tls_version = options.get('min_tls_version',
                                           self.MIN_TLS_VERSION)

        # Durability settings

        self.durability_set = False
        if options.get('persist_to', None) or \
                options.get('replicate_to', None) or \
                options.get('durability', None):
            self.durability_set = True

        self.replicate_to = int(options.get('replicate_to', self.REPLICATE_TO))
        self.persist_to = int(options.get('persist_to', self.PERSIST_TO))
        if options.get('durability', self.DURABILITY) is not None:
            self.durability = int(options.get('durability'))
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

        self.doc_groups = int(options.get('doc_groups', self.DOCUMENT_GROUPS))

        self.fts_data_spread_workers = options.get(
            'fts_data_spread_workers',
            self.FTS_DATA_SPREAD_WORKERS
        )
        if self.fts_data_spread_workers is not None:
            self.fts_data_spread_workers = int(self.fts_data_spread_workers)

        self.fts_data_spread_worker_type = "default"
        self.split_workload = options.get('split_workload', self.SPLIT_WORKLOAD)
        self.split_workload_throughput = options.get('split_workload_throughput',
                                                     self.SPLIT_WORKLOAD_THROUGHPUT)
        self.split_workload_workers = options.get('split_workload_throughput',
                                                  self.SPLIT_WORKLOAD_WORKERS)
        self.n1ql_shutdown_type = options.get('n1ql_shutdown_type', self.N1QL_SHUTDOWN_TYPE)

        self.workload_mix = []
        workload_mix = options.get('workload_mix', self.WORKLOAD_MIX)
        if workload_mix:
            self.workload_mix = workload_mix.split(',')
        self.num_buckets = int(options.get('num_buckets', self.NUM_BUCKETS))
        self.nebula_mode = options.get('nebula_mode', self.NEBULA_MODE)

        self.dapi_request_meta = maybe_atoi(
            options.get('dapi_request_meta', self.DAPI_REQUEST_META)
        )
        self.dapi_request_logs = maybe_atoi(
            options.get('dapi_request_logs', self.DAPI_REQUEST_LOGS)
        )

    def __str__(self) -> str:
        return str(self.__dict__)

    def configure_doc_settings(self, load_settings):
        self.doc_gen = load_settings.doc_gen
        self.array_size = load_settings.array_size
        self.num_categories = load_settings.num_categories
        self.num_replies = load_settings.num_replies
        self.size = load_settings.size
        self.key_fmtr = load_settings.key_fmtr
        self.size_variation_max = load_settings.size_variation_max
        self.size_variation_min = load_settings.size_variation_min

    def configure_client_settings(self, client_settings):
        if hasattr(client_settings, "pillowfight"):
            self.custom_pillowfight = True

    def configure_collection_settings(self, collection_settings):
        if collection_settings.collection_map is not None:
            self.collections = collection_settings.collection_map

    def configure_user_settings(self, user_settings):
        self.users = user_settings.num_users_per_bucket

    def configure_java_dcp_settings(self, java_dcp_settings):
        self.java_dcp_config = java_dcp_settings.config
        self.java_dcp_clients = java_dcp_settings.clients
        self.java_dcp_stream = java_dcp_settings.stream

    def configure(self, test_config):
        raise NotImplementedError()

    @staticmethod
    def compare_phase_settings(settings_list) -> Tuple[dict, list[dict]]:
        options = [set(s.__dict__) for s in settings_list]
        all_options = set.union(*options)
        diff_options = all_options - set.intersection(*options)
        for option in all_options:
            values = []
            for settings in settings_list:
                if hasattr(settings, option):
                    v = getattr(settings, option)
                    if not values:
                        values.append(v)
                    else:
                        if v != values[-1]:
                            diff_options.add(option)
                            break
                else:
                    diff_options.add(option)

        common_settings = {
            option: getattr(settings_list[0], option)
            for option in all_options - diff_options
        }

        # For each task, print out the settings which are different
        diff_settings = [
            {option: getattr(settings, option, None) for option in diff_options}
            for settings in settings_list
        ]

        return common_settings, diff_settings


class LoadSettings(PhaseSettings):

    CREATES = 100
    SEQ_UPSERTS = True

    UNIFORM_COLLECTION_LOAD_TIME = 0
    CONCURRENT_COLLECTION_LOAD = 0

    def __init__(self, options: dict):
        super().__init__(options)
        self.uniform_collection_load_time = int(options.get('uniform_collection_load_time',
                                                            self.UNIFORM_COLLECTION_LOAD_TIME))
        self.concurrent_collection_load = (
            self.uniform_collection_load_time or
            int(options.get('concurrent_collection_load', self.CONCURRENT_COLLECTION_LOAD))
        )

    def configure(self, test_config):
        self.configure_client_settings(test_config.client_settings)
        self.configure_collection_settings(test_config.collection)
        self.bucket_list = test_config.buckets


class JTSAccessSettings(PhaseSettings):

    JTS_REPO = "https://github.com/couchbaselabs/JTS"
    JTS_REPO_BRANCH = "master"
    JTS_HOME_DIR = "JTS"
    JTS_RUN_CMD = "java -jar target/JTS-1.0-jar-with-dependencies.jar"
    JTS_LOGS_DIR = "JTSlogs"
    FTS_PARTITIONS = "1"
    FTS_MAX_DCP_PARTITIONS = "0"
    FTS_FILE_BASED_REBAL_DISABLED = "true"

    def __init__(self, options: dict):
        super().__init__(options)
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
        self.test_mutation_field = options.get("test_mutation_field", None)
        self.test_worker_type = options.get("test_worker_type", "latency")
        self.couchbase_index_name = options.get("couchbase_index_name", "perf_fts_index")
        self.couchbase_index_configfile = options.get("couchbase_index_configfile")
        self.couchbase_index_type = options.get("couchbase_index_type")
        self.workload_instances = int(self.jts_instances)
        self.time = options.get('test_duration', "600")
        self.warmup_query_workers = options.get("warmup_query_workers", "0")
        self.warmup_time = options.get('warmup_time', "0")
        self.custom_num_buckets = options.get('custom_num_buckets', "0")
        # index creation - async or sync
        self.index_creation_style = options.get('index_creation_style', 'sync')
        # Geo Queries parameters
        self.test_geo_polygon_coord_list = options.get("test_geo_polygon_coord_list", "")
        self.test_query_lon_width = options.get("test_query_lon_width", "2")
        self.test_query_lat_height = options.get("test_query_lat_height", "2")
        self.test_geo_distance = options.get("test_geo_distance", "5mi")
        # File based rebalance parameter
        self.fts_file_based_rebal_disabled = options.get('fts_file_based_rebal_disabled',
                                                         self.FTS_FILE_BASED_REBAL_DISABLED)
        # Flex Queries parameters
        self.test_flex = options.get("test_flex", 'false')
        self.test_flex_query_type = options.get('test_flex_query_type', 'array_predicate')
        # Collection settings
        self.test_collection_query_mode = options.get('test_collection_query_mode', 'default')
        self.couchbase_index_configmap = options.get('couchbase_index_configmap', None)
        # Test query mode for mixed query
        self.test_query_mode = options.get('test_query_mode', 'default')
        # Number of indexes per index - group
        self.indexes_per_group = int(options.get('indexes_per_group', '1'))
        # index_group is the number of collections per index
        # if index_group is 1; all the collections are present in the index_def type mapping
        self.index_groups = int(options.get('index_groups', '1'))
        self.fts_index_map = {}
        self.collections_enabled = False
        self.test_collection_specific_count = \
            int(options.get('test_collection_specific_count', '1'))

        # Extra parameters for the FTS debugging
        self.ftspartitions = options.get('ftspartitions', self.FTS_PARTITIONS)
        self.fts_max_dcp_partitions = options.get('fts_max_dcp_partitions',
                                                  self.FTS_MAX_DCP_PARTITIONS)
        self.fts_node_level_parameters = {}
        # Adding bucket wise latency logger for fts
        # (please use this only with multi_query_support JTS branch)
        self.logging_method = options.get('logging_method', None)
        if self.ftspartitions != self.FTS_PARTITIONS:
            self.fts_node_level_parameters["maxConcurrentPartitionMovesPerNode"] = \
                self.ftspartitions
        if self.fts_max_dcp_partitions != self.FTS_MAX_DCP_PARTITIONS:
            self.fts_node_level_parameters["maxFeedsPerDCPAgent"] = self.fts_max_dcp_partitions

        if self.fts_file_based_rebal_disabled != self.FTS_FILE_BASED_REBAL_DISABLED:
            self.fts_node_level_parameters["disableFileTransferRebalance"] = \
                self.fts_file_based_rebal_disabled

    def __str__(self) -> str:
        return str(self.__dict__)

    def configure(self, test_config):
        pass


class HotLoadSettings(PhaseSettings):

    HOT_READS = True

    def configure(self, test_config):
        self.configure_doc_settings(test_config.load_settings)
        self.configure_client_settings(test_config.client_settings)
        self.configure_collection_settings(test_config.collection)
        self.bucket_list = test_config.buckets


class XattrLoadSettings(PhaseSettings):

    SEQ_UPSERTS = True

    def configure(self, test_config):
        self.bucket_list = test_config.buckets


class RestoreSettings:

    BACKUP_STORAGE = '/backups'
    BACKUP_REPO = ''
    IMPORT_FILE = ''
    DOCS_PER_COLLECTION = 0
    THREADS = 16
    MAP_DATA = None
    USE_TLS = False
    SHOW_TLS_VERSION = False
    MIN_TLS_VERSION = None
    ENCRYPTED = False
    PASSPHRASE = 'couchbase'
    CLOUD = None

    def __init__(self, options):
        self.docs_per_collections = int(options.get('docs_per_collection',
                                                    self.DOCS_PER_COLLECTION))
        self.backup_storage = options.get('backup_storage', self.BACKUP_STORAGE)
        self.backup_repo = options.get('backup_repo', self.BACKUP_REPO)
        self.import_file = options.get('import_file', self.IMPORT_FILE)
        self.threads = options.get('threads', self.THREADS)
        self.map_data = options.get('map_data', self.MAP_DATA)
        self.use_tls = int(options.get('use_tls', self.USE_TLS))
        self.show_tls_version = int(options.get('show_tls_version', self.SHOW_TLS_VERSION))
        self.min_tls_version = options.get('min_tls_version', self.MIN_TLS_VERSION)
        self.encrypted = int(options.get('encrypted', self.ENCRYPTED))
        self.passphrase = options.get('passphrase', self.PASSPHRASE)

        self.cloud = self.CLOUD
        if self.backup_storage:
            if self.backup_storage.startswith('s3://'):
                self.cloud = 'aws'
            elif self.backup_storage.startswith('gs://'):
                self.cloud = 'gcp'
            elif self.backup_storage.startswith('az://'):
                self.cloud = 'azure'

    def __str__(self) -> str:
        return str(self.__dict__)


class ImportSettings:

    IMPORT_FILE = ''
    DOCS_PER_COLLECTION = 0

    def __init__(self, options):
        self.docs_per_collections = int(options.get('docs_per_collection',
                                                    self.DOCS_PER_COLLECTION))
        self.import_file = options.get('import_file', self.IMPORT_FILE)

    def __str__(self) -> str:
        return str(self.__dict__)


class XDCRSettings:

    WAN_DELAY = 0
    NUM_XDCR_LINKS = 1
    XDCR_LINKS_PRIORITY = 'HIGH'
    INITIAL_COLLECTION_MAPPING = ''    # std format {"scope-1:collection-1":"scope-1:collection-1"}
    BACKFILL_COLLECTION_MAPPING = ''   # ----------------------"----------------------------------
    XDCR_LINK_DIRECTIONS = 'one-way'
    XDCR_LINK_NETWORK_LIMITS = '0'

    def __init__(self, options: dict):
        self.demand_encryption = options.get('demand_encryption')
        self.filter_expression = options.get('filter_expression')
        self.secure_type = options.get('secure_type')
        self.wan_delay = int(options.get('wan_delay',
                                         self.WAN_DELAY))

        self.num_xdcr_links = int(options.get('num_xdcr_links', self.NUM_XDCR_LINKS))
        self.xdcr_links_priority = options.get('xdcr_links_priority',
                                               self.XDCR_LINKS_PRIORITY).split(',')
        self.initial_collection_mapping = options.get('initial_collection_mapping',
                                                      self.INITIAL_COLLECTION_MAPPING)
        self.backfill_collection_mapping = options.get('backfill_collection_mapping',
                                                       self.BACKFILL_COLLECTION_MAPPING)
        self.collections_oso_mode = bool(options.get('collections_oso_mode'))

        # Capella-specific settings
        self.xdcr_link_directions = options.get('xdcr_link_directions',
                                                self.XDCR_LINK_DIRECTIONS).split(',')
        self.xdcr_link_network_limits = [
            int(x) for x in options.get('xdcr_link_network_limits',
                                        self.XDCR_LINK_NETWORK_LIMITS).split(',')
        ]

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
    CBINDEXPERF_CONCURRENCY = 0
    CBINDEXPERF_CLIENTS = 5
    CBINDEXPERF_LIMIT = 0
    CBINDEXPERF_REPEAT = 0
    CBINDEXPERF_CONFIGFILES = ''
    CBINDEXPERF_GCPERCENT = 100
    RUN_RECOVERY_TEST = 0
    INCREMENTAL_LOAD_ITERATIONS = 0
    SCAN_TIME = 1200
    INCREMENTAL_ONLY = 0
    REPORT_INITIAL_BUILD_TIME = 0
    DISABLE_PERINDEX_STATS = False
    AWS_CREDENTIAL_PATH = None

    def __init__(self, options: dict):
        self.indexes = {}
        if options.get('indexes') is not None:
            myindexes = options.get('indexes')
            if ".json" in myindexes:
                # index definitions passed in as json file
                with open(myindexes) as f:
                    self.indexes = json.load(f)
            else:
                for index_def in myindexes.split('#'):
                    name, field = index_def.split(':')
                    if '"' in field:
                        field = field.replace('"', '\\\"')
                    self.indexes[name] = field

        self.cbindexperf_configfile = options.get('cbindexperf_configfile',
                                                  self.CBINDEXPERF_CONFIGFILE)
        self.cbindexperf_concurrency = int(options.get('cbindexperf_concurrency',
                                                       self.CBINDEXPERF_CONCURRENCY))
        self.cbindexperf_repeat = int(options.get('cbindexperf_repeat',
                                                  self.CBINDEXPERF_REPEAT))
        self.cbindexperf_configfiles = options.get('cbindexperf_configfiles',
                                                   self.CBINDEXPERF_CONFIGFILES)
        self.cbindexperf_gcpercent = int(options.get('cbindexperf_gcpercent',
                                         self.CBINDEXPERF_GCPERCENT))
        self.cbindexperf_clients = int(options.get('cbindexperf_clients',
                                       self.CBINDEXPERF_CLIENTS))
        self.cbindexperf_limit = int(options.get('cbindexperf_limit',
                                                 self.CBINDEXPERF_LIMIT))
        self.run_recovery_test = int(options.get('run_recovery_test',
                                                 self.RUN_RECOVERY_TEST))
        self.incremental_only = int(options.get('incremental_only',
                                                self.INCREMENTAL_ONLY))
        self.incremental_load_iterations = int(options.get('incremental_load_iterations',
                                                           self.INCREMENTAL_LOAD_ITERATIONS))
        self.scan_time = int(options.get('scan_time', self.SCAN_TIME))
        self.report_initial_build_time = int(options.get('report_initial_build_time',
                                                         self.REPORT_INITIAL_BUILD_TIME))

        self.disable_perindex_stats = options.get('disable_perindex_stats',
                                                  self.DISABLE_PERINDEX_STATS)
        self.aws_credential_path = options.get('aws_credential_path', self.AWS_CREDENTIAL_PATH)

        self.settings = {}
        for option in options:
            if option.startswith(('indexer', 'projector', 'queryport', 'planner')):
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
        self.settings['indexer.cpu.throttle.target'] = \
            self.settings.get('indexer.cpu.throttle.target', 1.00)

        self.excludeNode = None
        if self.settings.get('planner.excludeNode'):
            self.excludeNode = self.settings.get('planner.excludeNode')
            self.settings.pop('planner.excludeNode')

    def __str__(self) -> str:
        return str(self.__dict__)


class DCPSettings:

    NUM_CONNECTIONS = 4
    INVOKE_WARM_UP = 0

    def __init__(self, options: dict):
        self.num_connections = int(options.get('num_connections',
                                               self.NUM_CONNECTIONS))
        self.invoke_warm_up = int(options.get('invoke_warm_up',
                                              self.INVOKE_WARM_UP))

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
        self.couchbase_fts_index_name = options.get('couchbase_fts_index_name',
                                                    self.FTS_INDEX_NAME)
        self.couchbase_fts_index_configfile = options.get('couchbase_fts_index_configfile',
                                                          self.FTS_INDEX_CONFIG_FILE)
        self.statements = self.create_index_statements()

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
            build_statements = []
            if self.fields.strip() == 'primary':
                for bucket in self.collection_map.keys():
                    for scope in self.collection_map[bucket].keys():
                        for collection in self.collection_map[bucket][scope].keys():
                            index_num = 1
                            if self.collection_map[bucket][scope][collection]['load'] == 1:
                                collection_num = collection.replace("collection-", "")
                                index_name = 'pi{}_{}'\
                                    .format(collection_num, index_num)
                                new_statement = \
                                    "CREATE PRIMARY INDEX {} ON default:`{}`.`{}`.`{}`". \
                                    format(index_name, bucket, scope, collection)
                                with_clause = " WITH {'defer_build': 'true',"
                                if self.replicas > 0:
                                    with_clause += "'num_replica': " + str(self.replicas) + ","
                                with_clause = with_clause[:-1]
                                with_clause += "}"
                                new_statement += with_clause
                                statements.append(new_statement)
                                build_statement = "BUILD INDEX ON default:`{}`.`{}`.`{}`('{}')" \
                                    .format(bucket, scope, collection, index_name)
                                build_statements.append(build_statement)
                                index_num += 1
            else:
                fields = self.fields.strip().split(',')
                parsed_fields = []
                index = 1
                for field in fields:
                    while field.count("(") != field.count(")"):
                        field = ",".join([field, fields[index]])
                        del fields[index]
                    index += 1
                    parsed_fields.append(field)
                fields = parsed_fields
                field_combos = list(chain.from_iterable(combinations(fields, r)
                                                        for r in range(1, len(fields)+1)))
                if self.top_down:
                    field_combos.reverse()
                for bucket in self.collection_map.keys():
                    for scope in self.collection_map[bucket].keys():
                        for collection in self.collection_map[bucket][scope].keys():
                            if self.collection_map[bucket][scope][collection]['load'] == 1:
                                indexes_created = 0
                                collection_num = collection.replace("collection-", "")
                                for field_subset in field_combos:
                                    subset_permutations = list(permutations(list(field_subset)))
                                    for permutation in subset_permutations:
                                        index_field_list = list(permutation)

                                        index_name = "i{}_{}".format(collection_num,
                                                                     str(indexes_created+1))
                                        index_fields = ",".join(index_field_list)
                                        new_statement = \
                                            "CREATE INDEX {} ON default:`{}`.`{}`.`{}`({})".\
                                            format(
                                                index_name,
                                                bucket,
                                                scope,
                                                collection,
                                                index_fields)
                                        with_clause = " WITH {'defer_build': 'true',"
                                        if self.replicas > 0:
                                            with_clause += \
                                                "'num_replica': " + str(self.replicas) + ","
                                        with_clause = with_clause[:-1]
                                        with_clause += "}"
                                        new_statement += with_clause
                                        statements.append(new_statement)
                                        build_statement = \
                                            "BUILD INDEX ON default:`{}`.`{}`.`{}`('{}')" \
                                            .format(bucket, scope, collection, index_name)
                                        build_statements.append(build_statement)
                                        indexes_created += 1
                                        if indexes_created == self.indexes_per_collection:
                                            break
                                    if indexes_created == self.indexes_per_collection:
                                        break
            statements = statements + build_statements
            return statements
        elif self.raw_statements:
            return self.raw_statements.strip().split('\n')
        elif self.raw_statements is None and self.fields is None:
            return []
        else:
            raise Exception('Index options must include one statement, '
                            'or fields (if collections enabled)')

    @property
    def indexes(self):
        if self.collection_map:
            indexes = []
            for statement in self.statements:
                match = re.search(r'CREATE .*INDEX (.*) ON', statement)
                if match:
                    indexes.append(match.group(1))
            indexes_per_collection = set(indexes)
            index_map = {}
            for bucket in self.collection_map.keys():
                for scope in self.collection_map[bucket].keys():
                    for collection in self.collection_map[bucket][scope].keys():
                        if self.collection_map[bucket][scope][collection]['load'] == 1:
                            bucket_map = index_map.get(bucket, {})
                            if bucket_map == {}:
                                index_map[bucket] = {}
                            scope_map = index_map[bucket].get(scope, {})
                            if scope_map == {}:
                                index_map[bucket][scope] = {}
                            coll_map = index_map[bucket][scope].get(collection, {})
                            if coll_map == {}:
                                index_map[bucket][scope][collection] = {}
                            for index_name in list(indexes_per_collection):
                                index_map[bucket][scope][collection][index_name] = ""
            return index_map
        else:
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

    def configure(self, test_config):
        self.configure_java_dcp_settings(test_config.java_dcp_settings)
        self.configure_client_settings(test_config.client_settings)
        self.configure_user_settings(test_config.users)
        self.configure_collection_settings(test_config.collection)

        load_settings = test_config.load_settings
        self.configure_doc_settings(load_settings)
        self.doc_groups = load_settings.doc_groups
        self.range_distance = load_settings.range_distance

        if self.split_workload is not None:
            with open(self.split_workload) as f:
                self.split_workload = json.load(f)

        if hasattr(self, 'n1ql_queries'):
            self.define_queries(test_config)

        self.bucket_list = test_config.buckets


class ExtraAccessSettings(PhaseSettings):

    OPS = float('inf')

    def configure(self, test_config):
        self.configure_java_dcp_settings(test_config.java_dcp_settings)
        self.configure_client_settings(test_config.client_settings)
        self.configure_user_settings(test_config.users)
        self.configure_collection_settings(test_config.collection)

        load_settings = test_config.load_settings
        self.configure_doc_settings(load_settings)
        self.doc_groups = load_settings.doc_groups
        self.range_distance = load_settings.range_distance

        self.bucket_list = test_config.buckets


class BackupSettings:

    COMPRESSION = False
    USE_TLS = False
    SHOW_TLS_VERSION = False
    MIN_TLS_VERSION = None
    ENCRYPTED = False
    PASSPHRASE = 'couchbase'

    # Undefined test parameters will use backup's default
    THREADS = None
    STORAGE_TYPE = None
    SINK_TYPE = None
    SHARDS = None
    OBJ_STAGING_DIR = None
    OBJ_REGION = None
    OBJ_ACCESS_KEY_ID = None
    AWS_CREDENTIAL_PATH = None
    INCLUDE_DATA = None
    BACKUP_DIRECTORY = None
    CLOUD = None

    def __init__(self, options: dict):
        self.compression = int(options.get('compression', self.COMPRESSION))
        self.threads = options.get('threads', self.THREADS)
        self.storage_type = options.get('storage_type', self.STORAGE_TYPE)
        self.sink_type = options.get('sink_type', self.SINK_TYPE)
        self.shards = options.get('shards', self.SHARDS)
        self.obj_staging_dir = options.get('obj_staging_dir', self.OBJ_STAGING_DIR)
        self.obj_region = options.get('obj_region', self.OBJ_REGION)
        self.obj_access_key_id = options.get('obj_access_key_id', self.OBJ_ACCESS_KEY_ID)
        self.aws_credential_path = options.get('aws_credential_path', self.AWS_CREDENTIAL_PATH)
        self.include_data = options.get('include_data', self.INCLUDE_DATA)
        self.backup_directory = options.get('backup_directory', self.BACKUP_DIRECTORY)
        self.use_tls = int(options.get('use_tls', self.USE_TLS))
        self.show_tls_version = int(options.get('show_tls_version', self.SHOW_TLS_VERSION))
        self.min_tls_version = options.get('min_tls_version', self.MIN_TLS_VERSION)
        self.encrypted = int(options.get('encrypted', self.ENCRYPTED))
        self.passphrase = options.get('passphrase', self.PASSPHRASE)

        self.cloud = self.CLOUD
        if self.backup_directory:
            if self.backup_directory.startswith('s3://'):
                self.cloud = 'aws'
            elif self.backup_directory.startswith('gs://'):
                self.cloud = 'gcp'
            elif self.backup_directory.startswith('az://'):
                self.cloud = 'azure'


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
    COLLECTION_FIELD = None
    SCOPE_FEILD = None
    SCOPE_COLLECTION_EXP = None

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
        self.collection_field = options.get('collection_field', self.COLLECTION_FIELD)
        self.scope_field = options.get('scope_field', self.SCOPE_FEILD)
        self.scope_collection_exp = options.get('scope_collection_exp', self.SCOPE_COLLECTION_EXP)


class EventingSettings:
    WORKER_COUNT = 3
    CPP_WORKER_THREAD_COUNT = 2
    TIMER_WORKER_POOL_SIZE = 1
    WORKER_QUEUE_CAP = 100000
    TIMER_TIMEOUT = 0
    TIMER_FUZZ = 0
    CONFIG_FILE = "tests/eventing/config/function_sample.json"
    REQUEST_URL = "http://172.23.99.247/cgi-bin/text/1kb_text_200ms.py"
    EVENTING_DEST_BKT_DOC_GEN = "basic"

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
        self.eventing_dest_bkt_doc_gen = options.get("eventing_dest_bkt_doc_gen",
                                                     self.EVENTING_DEST_BKT_DOC_GEN)

    def __str__(self) -> str:
        return str(self.__dict__)


class MagmaSettings:
    COLLECT_PER_SERVER_STATS = 0
    STORAGE_QUOTA_PERCENTAGE = 0
    MAGMA_MIN_MEMORY_QUOTA = 0

    def __init__(self, options: dict):
        self.collect_per_server_stats = int(options.get("collect_per_server_stats",
                                                        self.COLLECT_PER_SERVER_STATS))
        self.storage_quota_percentage = int(options.get("storage_quota_percentage",
                                                        self.STORAGE_QUOTA_PERCENTAGE))
        self.magma_min_memory_quota = int(options.get("magma_min_memory_quota",
                                                      self.MAGMA_MIN_MEMORY_QUOTA))


class AnalyticsSettings:

    NUM_IO_DEVICES = 1
    REPLICA_ANALYTICS = 0
    DEFAULT_LOG_LEVEL = "DEBUG"
    CACHE_PAGE_SIZE = 131072
    STORAGE_COMPRESSION_BLOCK = None
    QUERIES = ""
    ANALYTICS_CONFIG_FILE = ""
    DROP_DATASET = ""
    ANALYTICS_LINK = "Local"
    EXTERNAL_DATASET_TYPE = "s3"
    EXTERNAL_DATASET_REGION = "us-east-1"
    EXTERNAL_BUCKET = None
    EXTERNAL_FILE_FORMAT = 'json'
    EXTERNAL_FILE_INCLUDE = 'json'
    AWS_CREDENTIAL_PATH = None
    STORAGE_FORMAT = ""

    def __init__(self, options: dict):
        self.num_io_devices = int(options.get('num_io_devices',
                                              self.NUM_IO_DEVICES))
        self.replica_analytics = int(
            options.get('replica_analytics', self.REPLICA_ANALYTICS)
        )
        self.log_level = options.get("log_level", self.DEFAULT_LOG_LEVEL)
        self.storage_buffer_cache_pagesize = options.get("cache_page_size", self.CACHE_PAGE_SIZE)
        self.storage_compression_block = options.get("storage_compression_block",
                                                     self.STORAGE_COMPRESSION_BLOCK)
        self.queries = options.get("queries", self.QUERIES)
        self.analytics_config_file = options.get("analytics_config_file",
                                                 self.ANALYTICS_CONFIG_FILE)
        self.drop_dataset = options.get("drop_dataset", self.DROP_DATASET)
        self.analytics_link = options.get("analytics_link", self.ANALYTICS_LINK)
        self.external_dataset_type = options.get("external_dataset_type",
                                                 self.EXTERNAL_DATASET_TYPE)
        self.external_dataset_region = options.get("external_dataset_region",
                                                   self.EXTERNAL_DATASET_REGION)
        self.external_bucket = options.get("external_bucket", self.EXTERNAL_BUCKET)
        self.external_file_format = options.get("external_file_format", self.EXTERNAL_FILE_FORMAT)
        self.external_file_include = options.get("external_file_include",
                                                 self.EXTERNAL_FILE_INCLUDE)
        self.aws_credential_path = options.get('aws_credential_path', self.AWS_CREDENTIAL_PATH)
        self.storage_format = options.get('storage_format', self.STORAGE_FORMAT)


class AuditSettings:

    ENABLED = True

    EXTRA_EVENTS = ''

    def __init__(self, options: dict):
        self.enabled = bool(options.get('enabled', self.ENABLED))
        self.extra_events = set(options.get('extra_events',
                                            self.EXTRA_EVENTS).split())


class YCSBSettings:

    REPO = 'https://github.com/couchbaselabs/YCSB.git'
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

    LIBCOUCHBASE = None
    PYTHON_CLIENT = None
    TABLEAU_CONNECTOR = None

    def __init__(self, options: dict):
        self.libcouchbase = options.get('libcouchbase', self.LIBCOUCHBASE)
        self.python_client = options.get('python_client', self.PYTHON_CLIENT)
        self.tableau_connector = options.get('tableau_connector', self.TABLEAU_CONNECTOR)

    def __str__(self) -> str:
        return str(self.__dict__)


class JavaDCPSettings:

    REPO = 'https://github.com/couchbase/java-dcp-client.git'

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
    NUM_WRITES = 1000000000
    NUM_READS = 1000000000
    NUM_READERS = 32
    NUM_WRITERS = 128
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
        self.num_writers = int(options.get('num_writers', self.NUM_WRITERS))
        self.memquota = int(options.get('memquota', self.MEM_QUOTA))
        self.fs_cache_size = int(options.get('fs_cache_size', self.FS_CACHE_SIZE))
        self.write_multiplier = int(options.get('write_multiplier', self.WRITE_MULTIPLIER))
        self.data_dir = options.get('data_dir', self.DATA_DIR)
        self.engine = options.get('engine', self.ENGINE)
        self.engine_config = options.get('engine_config', self.ENGINE_CONFIG)

    def __str__(self) -> str:
        return str(self.__dict__)


class TPCDSLoaderSettings:

    REPO = 'https://github.com/couchbaselabs/cbas-perf-support.git'
    BRANCH = 'master'

    def __init__(self, options: dict):
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)

    def __str__(self) -> str:
        return str(self.__dict__)


class CH2:

    REPO = 'https://github.com/couchbaselabs2/ch2.git'
    BRANCH = 'main'
    WAREHOUSES = 1000
    ACLIENTS = 0
    TCLIENTS = 0
    ITERATIONS = 1
    WARMUP_ITERATIONS = 0
    WARMUP_DURATION = 0
    DURATION = 0
    WORKLOAD = 'ch2_mixed'
    ANALYTICS_STATEMENTS = ''
    USE_BACKUP = 'true'
    LOAD_TCLIENTS = 0
    LOAD_MODE = 'datasvc-bulkload'

    def __init__(self, options: dict):
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)
        self.warehouses = int(options.get('warehouses', self.WAREHOUSES))
        self.aclients = int(options.get('aclients', self.ACLIENTS))
        self.tclients = int(options.get('tclients', self.TCLIENTS))
        self.load_tclients = int(options.get('load_tclients', self.LOAD_TCLIENTS))
        self.load_mode = options.get('load_mode', self.LOAD_MODE)
        self.iterations = int(options.get('iterations', self.ITERATIONS))
        self.warmup_iterations = int(options.get('warmup_iterations', self.WARMUP_ITERATIONS))
        self.warmup_duration = int(options.get('warmup_duration', self.WARMUP_DURATION))
        self.duration = int(options.get('duration', self.DURATION))
        self.workload = options.get('workload', self.WORKLOAD)
        self.use_backup = maybe_atoi(options.get('use_backup', self.USE_BACKUP))
        self.raw_analytics_statements = options.get('analytics_statements',
                                                    self.ANALYTICS_STATEMENTS)
        if self.raw_analytics_statements:
            self.analytics_statements = self.raw_analytics_statements.strip().split('\n')
        else:
            self.analytics_statements = ''

    def __str__(self) -> str:
        return str(self.__dict__)


class CH3:

    REPO = 'https://github.com/couchbaselabs/ch3.git'
    BRANCH = 'main'
    WAREHOUSES = 1000
    ACLIENTS = 0
    TCLIENTS = 0
    FCLIENTS = 0
    ITERATIONS = 1
    WARMUP_ITERATIONS = 0
    WARMUP_DURATION = 0
    DURATION = 0
    WORKLOAD = 'ch3_mixed'
    ANALYTICS_STATEMENTS = ''
    USE_BACKUP = 'true'
    DEBUG = 'false'
    LOAD_TCLIENTS = 0
    LOAD_MODE = 'datasvc-bulkload'

    def __init__(self, options: dict):
        self.repo = options.get('repo', self.REPO)
        self.branch = options.get('branch', self.BRANCH)
        self.warehouses = int(options.get('warehouses', self.WAREHOUSES))
        self.aclients = int(options.get('aclients', self.ACLIENTS))
        self.tclients = int(options.get('tclients', self.TCLIENTS))
        self.fclients = int(options.get('fclients', self.FCLIENTS))
        self.load_tclients = int(options.get('load_tclients', self.LOAD_TCLIENTS))
        self.load_mode = options.get('load_mode', self.LOAD_MODE)
        self.iterations = int(options.get('iterations', self.ITERATIONS))
        self.warmup_iterations = int(options.get('warmup_iterations', self.WARMUP_ITERATIONS))
        self.warmup_duration = int(options.get('warmup_duration', self.WARMUP_DURATION))
        self.duration = int(options.get('duration', self.DURATION))
        self.workload = options.get('workload', self.WORKLOAD)
        self.use_backup = maybe_atoi(options.get('use_backup', self.USE_BACKUP))
        self.debug = maybe_atoi(options.get('debug', self.DEBUG))
        self.raw_analytics_statements = options.get('analytics_statements',
                                                    self.ANALYTICS_STATEMENTS)
        if self.raw_analytics_statements:
            self.analytics_statements = self.raw_analytics_statements.strip().split('\n')
        else:
            self.analytics_statements = ''

    def __str__(self) -> str:
        return str(self.__dict__)


class PYTPCCSettings:

    WAREHOUSE = 1
    CLIENT_THREADS = 1
    DURATION = 600
    MULTI_QUERY_NODE = 0
    DRIVER = 'n1ql'
    QUERY_PORT = '8093'
    KV_PORT = '8091'
    RUN_SQL_SHELL = 'run_sqlcollections.sh'
    RUN_FUNCTION_SHELL = 'run_sqlfunctions.sh'
    CBRINDEX_SQL = 'cbcrindexcollection_replicas3.sql'
    CBRFUNCTION_SQL = 'cbcrjsfunctions.sql'
    COLLECTION_CONFIG = 'cbcrbucketcollection_20GB.sh'
    DURABILITY_LEVEL = 'majority'
    SCAN_CONSISTENCY = 'not_bounded'
    TXTIMEOUT = 3.0
    TXT_CLEANUP_WINDOW = 0
    PYTPCC_BRANCH = 'py3'
    PYTPCC_REPO = 'https://github.com/couchbaselabs/py-tpcc.git'
    INDEX_REPLICAS = 0

    def __init__(self, options: dict):
        self.warehouse = int(options.get('warehouse', self.WAREHOUSE))
        self.client_threads = int(options.get('client_threads',
                                              self.CLIENT_THREADS))
        self.duration = int(options.get('duration', self.DURATION))
        self.multi_query_node = int(options.get('multi_query_node',
                                                self.MULTI_QUERY_NODE))
        self.driver = options.get('driver', self.DRIVER)
        self.query_port = options.get('query_port', self.QUERY_PORT)
        self.kv_port = options.get('kv_port', self.KV_PORT)
        self.run_sql_shell = options.get('run_sql_shell', self.RUN_SQL_SHELL)
        self.run_function_shell = options.get('run_function_shell', self.RUN_FUNCTION_SHELL)
        self.cbrindex_sql = options.get('cbrindex_sql', self.CBRINDEX_SQL)
        self.cbrfunction_sql = options.get('cbrfunction_sql', self.CBRFUNCTION_SQL)
        self.collection_config = options.get('collection_config',
                                             self.COLLECTION_CONFIG)
        self.durability_level = options.get('durability_level',
                                            self.DURABILITY_LEVEL)
        self.scan_consistency = options.get('scan_consistency',
                                            self.SCAN_CONSISTENCY)
        self.txtimeout = options.get('txtimeout', self.TXTIMEOUT)
        self.txt_cleanup_window = int(options.get('txt_cleanup_window',
                                                  self.TXT_CLEANUP_WINDOW))
        self.pytpcc_branch = options.get('pytpcc_branch', self.PYTPCC_BRANCH)
        self.pytpcc_repo = options.get('pytpcc_repo', self.PYTPCC_REPO)
        self.use_pytpcc_backup = bool(options.get('use_pytpcc_backup'))
        self.index_replicas = int(options.get('index_replicas',
                                              self.INDEX_REPLICAS))

    def __str__(self) -> str:
        return str(self.__dict__)


class AutoscalingSettings:

    ENABLED = False
    MIN_NODES = 0
    MAX_NODES = 0
    SERVER_GROUP = None
    TARGET_METRIC = None
    TARGET_TYPE = None
    TARGET_VALUE = None

    def __init__(self, options: dict):
        self.min_nodes = options.get('min_nodes', self.MIN_NODES)
        self.max_nodes = options.get('max_nodes', self.MAX_NODES)
        self.server_group = options.get('server_group', self.SERVER_GROUP)
        self.target_metric = options.get('target_metric', self.TARGET_METRIC)
        self.target_type = options.get('target_type', self.TARGET_TYPE)
        self.target_value = options.get('target_value', self.TARGET_VALUE)
        self.enabled = self.ENABLED
        if self.min_nodes and self.max_nodes:
            self.enabled = True

    def __str__(self) -> str:
        return str(self.__dict__)


class TableauSettings:

    HOST = 'localhost'
    API_VERSION = '3.14'
    CREDENTIALS = {
        'username': 'admin',
        'password': 'password'
    }
    DATASOURCE = None
    CONNECTOR_VENDOR = None

    def __init__(self, options: dict):
        self.host = options.get('host', self.HOST)
        self.api_version = options.get('api_version', self.API_VERSION)
        self.datasource = options.get('datasource', self.DATASOURCE)
        self.connector_vendor = options.get('connector_vendor', self.CONNECTOR_VENDOR)
        credentials = options.get('credentials')
        if credentials:
            uname, password = credentials.split(':')
            self.credentials = {'username': uname, 'password': password}
        else:
            self.credentials = self.CREDENTIALS


class SyncgatewaySettings:
    REPO = 'https://github.com/couchbaselabs/YCSB.git'
    YCSB_COMMAND = 'syncgateway'
    BRANCH = 'tmp-sqw-weekly-updated-c3'
    WORKLOAD = 'workloads/syncgateway_blank'
    USERS = 100
    CHANNELS = 1
    CHANNLES_PER_USER = 1
    LOAD_CLIENTS = 1
    CLIENTS = 4
    NODES = 4
    CHANNELS_PER_DOC = 1
    DOCUMENTS = 1000000
    DOCUMENTSPULL = 500000
    DOCUMENTSPUSH = 500000
    ROUNDTRIP_WRITE = "false"
    READ_MODE = 'documents'          # |documents|changes
    FEED_READING_MODE = 'withdocs'   # |withdocs|idsonly
    FEED_MODE = 'longpoll'           # |longpoll|normal
    INSERT_MODE = 'byuser'           # |byuser|bykey
    AUTH = "true"
    PULLPROPORTION = 0.5
    PUSHPROPORTION = 0.5
    READPROPORTION = 1
    UPDATEPROPORTION = 0
    INSERTPROPORTION = 0
    SCANPROPORTION = 0
    REQUESTDISTRIBUTION = 'zipfian'  # |zipfian|uniform
    LOG_TITE = 'sync_gateway_default'
    LOAD_THREADS = 1
    THREADS = 10
    INSERTSTART = 0
    MAX_INSERTS_PER_INSTANCE = 1000000
    STAR = "false"
    GRANT_ACCESS = "false"
    GRANT_ACCESS_IN_SCAN = "false"
    CHANNELS_PER_GRANT = 1
    FIELDCOUNT = 10
    FIELDLENGTH = 100
    REPLICATOR2 = "false"
    BASIC_AUTH = "false"
    IMPORT_NODES = 1
    SSL_MODE_SGW = 'none'
    ROUNDTRIP_WRITE_LOAD = "false"
    SG_REPLICATION_TYPE = "push"
    SG_CONFLICT_RESOLUTION = "default"
    SG_READ_LIMIT = 1
    SG_LOADER_THREADS = 50
    SG_DOCLOADER_THREAD = 50
    SG_BLACKHOLEPULLER_CLIENTS = 6
    SG_BLACKHOLEPULLER_USERS = 0
    SG_BLACKHOLEPULLER_TIMEOUT = 600    # in seconds
    SG_DOCSIZE = 10240
    SGTOOL_CHANGEBATCHSET = 200

    REPLICATION_TYPE = None
    REPLICATION_CONCURRENCY = 1
    DELTA_SYNC = ''
    DELTASYNC_CACHEHIT_RATIO = 0
    DOCTYPE = 'simple'
    DOC_DEPTH = '1'
    WRITEALLFIELDS = 'true'
    READALLFIELDS = 'true'
    UPDATEFIELDCOUNT = 1

    E2E = ''
    YCSB_RETRY_COUNT = 5
    YCSB_RETRY_INTERVAL = 1
    CBL_PER_WORKER = 0
    CBL_TARGET = "127.0.0.1"
    RAMDISK_SIZE = 0
    CBL_THROUGHPUT = 0
    COLLECT_CBL_LOGS = 0
    CBL_VERBOSE_LOGGING = 0
    TROUBLEMAKER = None
    COLLECT_SGW_LOGS = 0
    COLLECT_SGW_CONSOLE = 0
    DATA_INTEGRITY = 'false'
    REPLICATION_AUTH = 1

    def __init__(self, options: dict):
        self.repo = options.get('ycsb_repo', self.REPO)
        self.branch = options.get('ycsb_branch', self.BRANCH)
        self.ycsb_command = options.get('ycsb_command', self.YCSB_COMMAND)
        self.workload = options.get('workload_path', self.WORKLOAD)
        self.users = options.get('users', self.USERS)
        self.channels = options.get('channels', self.CHANNELS)
        self.channels_per_user = options.get('channels_per_user', self.CHANNLES_PER_USER)
        self.channels_per_doc = options.get('channels_per_doc', self.CHANNELS_PER_DOC)
        self.documents = options.get('documents', self.DOCUMENTS)
        self.documents_workset = options.get("documents_workset", self.documents)
        self.documentspull = options.get('documentspull', self.DOCUMENTSPULL)
        self.documentspush = options.get('documentspush', self.DOCUMENTSPUSH)
        self.roundtrip_write = options.get('roundtrip_write', self.ROUNDTRIP_WRITE)
        self.read_mode = options.get('read_mode', self.READ_MODE)
        self.feed_mode = options.get('feed_mode', self.FEED_MODE)
        self.feed_reading_mode = options.get('feed_reading_mode', self.FEED_READING_MODE)
        self.auth = options.get('auth', self.AUTH)
        self.pullproportion = options.get('pullproportion', self.PULLPROPORTION)
        self.pushproportion = options.get('pushproportion', self.PUSHPROPORTION)
        self.readproportion = options.get('readproportion', self.READPROPORTION)
        self.updateproportion = options.get('updateproportion', self.UPDATEPROPORTION)
        self.insertproportion = options.get('insertproportion', self.INSERTPROPORTION)
        self.scanproportion = options.get('scanproportion', self.SCANPROPORTION)
        self.requestdistribution = options.get('requestdistribution', self.REQUESTDISTRIBUTION)
        self.log_title = options.get('log_title', self.LOG_TITE)
        self.instances_per_client = options.get('instances_per_client', 1)
        self.load_instances_per_client = options.get('load_instances_per_client', 1)
        self.instance = options.get('instance', '')
        self.threads_per_instance = 1
        self.load_threads = options.get('load_threads', self.LOAD_THREADS)
        self.threads = options.get('threads', self.THREADS)
        self.insertstart = options.get('insertstart', self.INSERTSTART)
        self.max_inserts_per_instance = options.get('max_inserts_per_instance',
                                                    self.MAX_INSERTS_PER_INSTANCE)
        self.insert_mode = options.get('insert_mode', self.INSERT_MODE)
        self.load_clients = options.get('load_clients', self.LOAD_CLIENTS)
        self.clients = options.get('clients', self.CLIENTS)
        self.nodes = int(options.get('nodes', self.NODES))
        self.starchannel = options.get('starchannel', self.STAR)
        self.grant_access = options.get('grant_access', self.GRANT_ACCESS)
        self.channels_per_grant = options.get('channels_per_grant', self.CHANNELS_PER_GRANT)
        self.grant_access_in_scan = options.get('grant_access_in_scan', self.GRANT_ACCESS_IN_SCAN)
        self.build_label = options.get('build_label', '')
        self.fieldcount = options.get('fieldcount', self.FIELDCOUNT)
        self.fieldlength = options.get('fieldlength', self.FIELDLENGTH)

        self.replicator2 = options.get('replicator2', self.REPLICATOR2)
        self.basic_auth = options.get('basic_auth', self.BASIC_AUTH)

        self.import_nodes = int(options.get('import_nodes', self.IMPORT_NODES))
        self.ssl_mode_sgw = (options.get('ssl_mode_sgw', self.SSL_MODE_SGW))

        self.roundtrip_write_load = options.get('roundtrip_write_load', self.ROUNDTRIP_WRITE_LOAD)
        self.sg_replication_type = options.get('sg_replication_type', self.SG_REPLICATION_TYPE)
        self.sg_conflict_resolution = options.get('sg_conflict_resolution',
                                                  self.SG_CONFLICT_RESOLUTION)
        self.sg_read_limit = int(options.get('sg_read_limit', self.SG_READ_LIMIT))
        self.sg_loader_threads = int(options.get("sg_loader_threads", self.SG_LOADER_THREADS))
        self.sg_docloader_thread = int(options.get("sg_docloader_thread", self.SG_DOCLOADER_THREAD))
        self.sg_blackholepuller_client = int(options.get("sg_blackholepuller_client",
                                                         self.SG_BLACKHOLEPULLER_CLIENTS))
        self.sg_blackholepuller_users = int(options.get("sg_blackholepuller_users",
                                                        self.SG_BLACKHOLEPULLER_USERS))
        self.sg_blackholepuller_timeout = options.get("sg_blackholepuller_timeout",
                                                      self.SG_BLACKHOLEPULLER_TIMEOUT)
        self.sg_docsize = int(options.get("sg_docsize", self.SG_DOCSIZE))
        self.sgtool_changebatchset = int(options.get("sgtool_changebatchset",
                                                     self.SGTOOL_CHANGEBATCHSET))

        self.delta_sync = self.DELTA_SYNC
        self.e2e = self.E2E
        self.replication_type = options.get('replication_type', self.REPLICATION_TYPE)
        if self.replication_type:
            if self.replication_type in ["PUSH", "PULL"]:
                self.delta_sync = 'true'
            if self.replication_type in ["E2E_PUSH", "E2E_PULL", "E2E_BIDI"]:
                self.e2e = 'true'
        self.deltasync_cachehit_ratio = options.get(
            'deltasync_cachehit_ratio', self.DELTASYNC_CACHEHIT_RATIO)
        self.replication_concurrency = options.get(
            'replication_concurrency', self.REPLICATION_CONCURRENCY)
        self.doctype = options.get('doctype', self.DOCTYPE)
        self.doc_depth = options.get('doc_depth', self.DOC_DEPTH)
        self.writeallfields = options.get('writeallfields', self.WRITEALLFIELDS)
        self.readallfields = options.get('readallfields', self.READALLFIELDS)
        self.updatefieldcount = options.get('updatefieldcount', self.UPDATEFIELDCOUNT)
        self.ycsb_retry_count = int(options.get('ycsb_retry_count', self.YCSB_RETRY_COUNT))
        self.ycsb_retry_interval = int(options.get('ycsb_retry_interval', self.YCSB_RETRY_INTERVAL))
        self.cbl_per_worker = int(options.get('cbl_per_worker', self.CBL_PER_WORKER))
        self.cbl_target = self.CBL_TARGET
        self.ramdisk_size = int(options.get('ramdisk_size', self.RAMDISK_SIZE))
        self.cbl_throughput = int(options.get('cbl_throughput', self.CBL_THROUGHPUT))
        self.collect_cbl_logs = int(options.get('collect_cbl_logs', self.COLLECT_CBL_LOGS))
        self.cbl_verbose_logging = int(options.get('cbl_verbose_logging', self.CBL_VERBOSE_LOGGING))
        self.troublemaker = options.get('troublemaker', self.TROUBLEMAKER)
        self.collect_sgw_logs = int(options.get('collect_sgw_logs', self.COLLECT_SGW_LOGS))
        self.collect_sgw_console = int(options.get('collect_sgw_console', self.COLLECT_SGW_CONSOLE))
        self.data_integrity = options.get('data_integrity', self.DATA_INTEGRITY)
        self.replication_auth = int(options.get('replication_auth', self.REPLICATION_AUTH))

    def __str__(self) -> str:
        return str(self.__dict_)


class DiagEvalSettings:

    DEFAULT_RESTART_DELAY = 5  # seconds

    def __init__(self, options: dict, enable_nonlocal_diag_eval: bool):
        self.restart_delay = int(options.get('restart_delay', self.DEFAULT_RESTART_DELAY))
        payloads = options.get('payloads')
        if payloads:
            self.payloads = payloads.strip().split('\n')
        else:
            self.payloads = None
        self.enable_nonlocal_diag_eval = enable_nonlocal_diag_eval


class TestConfig(Config):

    def _configure_phase_settings(method):  # noqa: N805
        """Decorate phase settings properties to configure them."""
        def wrapper(self):
            phase_settings = method(self)
            phase_settings.configure(self)
            return phase_settings

        return wrapper

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
    def direct_nebula(self) -> DirectNebulaSettings:
        options = self._get_options_as_dict('direct_nebula')
        return DirectNebulaSettings(options)

    @property
    def data_api(self) -> DataApiSettings:
        options = self._get_options_as_dict('data_api')
        return DataApiSettings(options)

    @property
    def bucket(self) -> BucketSettings:
        options = self._get_options_as_dict('bucket')
        return BucketSettings(options)

    @property
    def collection(self) -> CollectionSettings:
        options = self._get_options_as_dict('collection')
        settings = CollectionSettings(options, self.buckets)
        if settings.config is not None and (db_map := self.serverless_db.db_map):
            coll_map = settings.collection_map
            new_coll_map = {}
            for db_id in db_map:
                new_coll_map[db_id] = coll_map[db_map[db_id]['name']]
            settings.collection_map = new_coll_map
        return settings

    @property
    def serverless_db(self) -> ServerlessDBSettings:
        options = self._get_options_as_dict('serverless_db')
        return ServerlessDBSettings(options)

    @property
    def users(self) -> UserSettings:
        options = self._get_options_as_dict('users')
        return UserSettings(options)

    @property
    def diag_eval(self) -> DiagEvalSettings:
        """Specify arbitrary diag/eval payload to be run during cluster configuration.

        This can be specified as follows
        ```
        [diag_eval]
        payloads =
                  ns_config:set(option, value).
                  ns_config:command(option, value).
        restart_delay = N
        ```
        Multiple command can also be specified on one line separated by comma.
        """
        # If there are bucket_extras, then nonlocal diag/eval is already enabled
        enable_nonlocal_diag_eval = False if self.bucket_extras else True
        options = self._get_options_as_dict('diag_eval')
        return DiagEvalSettings(options, enable_nonlocal_diag_eval)

    @property
    def bucket_extras(self) -> dict:
        bucket_extras = self._get_options_as_dict('bucket_extras')
        options = self._get_options_as_dict('access')
        access = AccessSettings(options)
        if access.durability_set:
            if "num_writer_threads" not in bucket_extras:
                bucket_extras["num_writer_threads"] = "disk_io_optimized"
        return bucket_extras

    @property
    def buckets(self) -> List[str]:
        if self.serverless_db.db_map:
            return list(self.serverless_db.db_map.keys())
        elif self.cluster.num_buckets == 1 and self.cluster.bucket_name != 'bucket-1':
            return [self.cluster.bucket_name]
        else:
            return [
                'bucket-{}'.format(i + 1) for i in range(self.cluster.num_buckets)
            ]

    @property
    def eventing_buckets(self) -> List[str]:
        return [
            'eventing-bucket-{}'.format(i + 1) for i in range(self.cluster.eventing_buckets)
        ]

    @property
    def eventing_metadata_bucket(self) -> List[str]:
        return [
            'eventing'
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
    def import_settings(self) -> ImportSettings:
        options = self._get_options_as_dict('import')
        return ImportSettings(options)

    @property
    @_configure_phase_settings
    def load_settings(self):
        load_options = self._get_options_as_dict('load')
        load_settings = LoadSettings(load_options)
        return load_settings

    @property
    def mixed_load_settings(self) -> List[LoadSettings]:
        base_settings = self.load_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('load', base_settings)
        return mix

    @property
    @_configure_phase_settings
    def hot_load_settings(self) -> HotLoadSettings:
        options = self._get_options_as_dict('hot_load')
        hot_load = HotLoadSettings(options)
        return hot_load

    @property
    def mixed_hot_load_settings(self) -> List[HotLoadSettings]:
        base_settings = self.hot_load_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('hot_load', base_settings)
        return mix

    @property
    @_configure_phase_settings
    def xattr_load_settings(self) -> XattrLoadSettings:
        options = self._get_options_as_dict('xattr_load')
        xattr_settings = XattrLoadSettings(options)
        return xattr_settings

    @property
    def mixed_xattr_load_settings(self) -> List[XattrLoadSettings]:
        base_settings = self.xattr_load_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('xattr_load', base_settings)
        return mix

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
        collection_settings = self.collection
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
    @_configure_phase_settings
    def access_settings(self) -> AccessSettings:
        options = self._get_options_as_dict('access')
        access = AccessSettings(options)
        return access

    @property
    def mixed_access_settings(self) -> List[AccessSettings]:
        base_settings = self.access_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('access', base_settings)
        return mix

    @property
    @_configure_phase_settings
    def extra_access_settings(self) -> ExtraAccessSettings:
        options = self._get_options_as_dict('extra_access')
        extra_access = ExtraAccessSettings(options)
        return extra_access

    @property
    def mixed_extra_access_settings(self) -> List[ExtraAccessSettings]:
        base_settings = self.extra_access_settings
        mix = []
        if base_settings.workload_mix:
            mix = self._get_mixed_phase_settings('extra_access', base_settings)
        return mix

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

    def get_sever_group_definition(self, server_group_name: str) -> dict:
        return self._get_options_as_dict('sg-{}'.format(server_group_name))

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

    @property
    def ch2_settings(self) -> CH2:
        options = self._get_options_as_dict('ch2')
        return CH2(options)

    @property
    def ch3_settings(self) -> CH3:
        options = self._get_options_as_dict('ch3')
        return CH3(options)

    @property
    def pytpcc_settings(self) -> PYTPCCSettings:
        options = self._get_options_as_dict('py_tpcc')
        return PYTPCCSettings(options)

    @property
    def autoscaling_setting(self) -> AutoscalingSettings:
        options = self._get_options_as_dict('autoscaling')
        return AutoscalingSettings(options)

    @property
    def tableau_settings(self) -> TableauSettings:
        options = self._get_options_as_dict('tableau')
        return TableauSettings(options)

    @property
    def syncgateway_settings(self) -> SyncgatewaySettings:
        options = self._get_options_as_dict('syncgateway')
        return SyncgatewaySettings(options)

    def _get_mixed_phase_settings(self, base_section, base_settings):
        settings_cls = type(base_settings)
        mix = []

        bucket_offset = 0
        for section in base_settings.workload_mix:
            phase_options = self._get_options_as_dict(base_section)
            override_options = self._get_options_as_dict('{}-{}'.format(base_section, section))
            phase_options.update(override_options)
            phase = settings_cls(phase_options)
            phase.configure(self)
            if bucket_offset >= len(self.buckets):
                break
            phase.bucket_list = self.buckets[bucket_offset:bucket_offset + phase.num_buckets]
            bucket_offset += phase.num_buckets
            mix.append(phase)

        return mix


class TargetSettings:

    def __init__(self, host: str, bucket: str, username: str, password: str,
                 prefix: str = None, cloud: dict = {}):
        self.password = password
        self.node = host
        self.bucket = bucket
        self.prefix = prefix
        self.cloud = cloud
        self.username = username

    @property
    def connection_string(self) -> str:
        return 'couchbase://{username}:{password}@{host}/{bucket}'.format(
            username=self.username,
            password=self.password,
            host=self.node,
            bucket=self.bucket,
        )


class TargetIterator(Iterable):

    def __init__(self,
                 cluster_spec: ClusterSpec,
                 test_config: TestConfig,
                 prefix: str = None,
                 buckets: Iterable[str] = None):
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.prefix = prefix
        self.buckets = list(buckets) if buckets else self.test_config.buckets

    def __iter__(self) -> Iterator[TargetSettings]:
        username = self.cluster_spec.rest_credentials[0]
        if self.test_config.client_settings.python_client:
            if self.test_config.client_settings.python_client.split('.')[0] == "2":
                password = self.test_config.bucket.password
            else:
                password = self.cluster_spec.rest_credentials[1]
        else:
            password = self.cluster_spec.rest_credentials[1]

        prefix = self.prefix

        for master in self.cluster_spec.masters:
            if self.prefix is None:
                prefix = target_hash(master)

            if self.cluster_spec.serverless_infrastructure:
                for bucket in self.buckets:
                    params = self.test_config.serverless_db.db_map[bucket]
                    access = params['access']
                    secret = params['secret']
                    cloud = {
                        'serverless': True,
                        'nebula_uri': params['nebula_uri'],
                        'dapi_uri': params['dapi_uri']
                    }

                    yield TargetSettings(host=master, bucket=bucket, username=access,
                                         password=secret, prefix=prefix, cloud=cloud)
            else:
                for bucket in self.buckets:
                    if "perfrunner.tests.views" in self.test_config.test_case.test_module:
                        username = bucket
                        password = self.test_config.bucket.password

                    cloud = {}

                    if self.cluster_spec.dynamic_infrastructure:
                        cloud = {'cluster_svc': 'cb-example-perf'}
                    elif self.test_config.cluster.serverless_mode == 'enabled':
                        cloud = {'serverless': True}

                    yield TargetSettings(host=master, bucket=bucket, username=username,
                                         password=password, prefix=prefix, cloud=cloud)

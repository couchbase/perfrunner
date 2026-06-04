import copy
import json
import threading
import time
from typing import Callable

import paramiko
import requests
from decorator import decorator
from sshtunnel import SSHTunnelForwarder

from logger import logger
from perfrunner.helpers.misc import uhex
from perfrunner.helpers.rest import RestHelper
from perfrunner.helpers.server import ServerInfoManager
from perfrunner.settings import ClusterSpec, TestConfig


@decorator
def with_profiles(method: Callable, *args, **kwargs):
    test = args[0]
    test.profiler.schedule()
    return method(*args, **kwargs)


class Timer(threading.Timer):

    def __init__(self, interval, function, num_runs=1, args=None, kwargs=None):
        super().__init__(interval, function, args, kwargs)
        self.num_runs = num_runs
        self.daemon = True

    def run(self):
        super().run()
        self.repeat()

    def repeat(self):
        self.num_runs -= 1
        if self.num_runs:
            self.finished.clear()
            self.run()


class ProfilerHelper:
    def __new__(cls, cluster_spec: ClusterSpec, test_config: TestConfig):
        if test_config.profiling_settings.linux_perf_profile_flag:
            return LinuxPerfProfiler(cluster_spec, test_config)
        else:
            return GoDebugProfiler(cluster_spec, test_config)


class ProfilerBase:

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        self.test_config = test_config
        self.cluster_spec = cluster_spec
        self.rest = RestHelper(cluster_spec, bool(test_config.cluster.enable_n2n_encryption))
        self.server_info = ServerInfoManager().get_server_info()
        self.master_node = self.server_info.master_node
        self.ssh_username, self.ssh_password = cluster_spec.ssh_credentials
        self.profiling_settings = copy.deepcopy(test_config.profiling_settings)

    def timer(self, **kwargs):
        timer = Timer(
            function=self.profile,
            interval=self.profiling_settings.interval,
            num_runs=self.profiling_settings.num_profiles,
            kwargs=kwargs,
        )
        timer.start()

    def profile(self, host: str, service: str, profile: str):
        logger.warning("Unknown profiler, doing nothing")

    def _schedule_service(self, service, nodes):
        logger.info(f'Scheduling "{service}" service profiling')
        for server in nodes:
            for profile in self.profiling_settings.profiles:
                self.timer(host=server, service=service, profile=profile)

    def schedule(self):
        for service in self.profiling_settings.services:
            if service == 'syncgateway':
                initial_nodes = int(self.test_config.syncgateway_settings.nodes)
                self._schedule_service(service, self.cluster_spec.sgw_servers[:initial_nodes])
            else:
                role = 'kv' if (service == 'projector' or service == 'goxdcr') else service
                for _, servers in self.cluster_spec.clusters:
                    self.master_node = servers[0]
                    logger.info("The current master node is: {}".format(self.master_node))
                    active_nodes_by_role = self.rest.get_active_nodes_by_role(self.master_node,
                                                                            role=role)
                    self._schedule_service(service, active_nodes_by_role)
                    self.master_node = next(self.cluster_spec.masters)


class GoDebugProfiler (ProfilerBase):

    """Go profiler profiles using existing debug ports from each service."""

    # (connect, read) seconds for a single profile fetch. The CPU profile blocks
    # ~30s while it captures, so the read budget must comfortably exceed that;
    # anything longer than this is a stall and is skipped rather than hung on.
    PROFILE_TIMEOUT = (10, 90)

    DEBUG_PORTS = {
        'fts':   8094,
        'index': 9102,
        'goxdcr': 9998,
        'n1ql':  8093,
        'eventing': 8096,
        'projector': 9999,
        'syncgateway': 4985,
    }

    ENDPOINTS = {
        'cpu':  'http://127.0.0.1:{}/debug/pprof/profile',
        'heap': 'http://127.0.0.1:{}/debug/pprof/heap',
        'goroutine': 'http://127.0.0.1:{}/debug/pprof/goroutine?debug=2',
        'sg_cpu': 'http://127.0.0.1:{}/_profile',
        'sg_heap': 'http://127.0.0.1:{}/_heap',
        'sg_block': 'http://127.0.0.1:{}/debug/pprof/block',
        'sg_mutex': 'http://127.0.0.1:{}/debug/pprof/mutex',
    }

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        super().__init__(cluster_spec, test_config)

    def profile(self, host: str, service: str, profile: str):
        logger.info(f'Collecting {profile} profile on {host}')
        if 'syncgateway' in self.test_config.profiling_settings.services:
            if profile == 'sg_cpu':
                url = f'http://{host}:4985/_profile'
                filename = (f'{host}_{service}_{profile}_'
                           f'{time.strftime("%y%m%d%H%M%S")}_{uhex()[:6]}.pprof')
                requests.post(url=url, data=json.dumps({"file": filename}))
                time.sleep(self.test_config.profiling_settings.cpu_interval)
                requests.post(url=url, data=json.dumps({}))

            if profile == 'sg_heap':
                filename = (f'{host}_{service}_{profile}_'
                           f'{time.strftime("%y%m%d%H%M%S")}_{uhex()[:6]}.pprof')
                url = f'http://{host}:4985/_heap'
                requests.post(url=url, data=json.dumps({"file": filename}))

            if profile == 'goroutine':
                url = f'http://{host}:4985/_debug/pprof/goroutine'
                response = requests.get(url=url)
                self.save(host, service, profile, response.content)

            if profile == 'sg_block':
                url = f'http://{host}:4985/_debug/pprof/block'
                response = requests.get(url=url)
                self.save(host, service, profile, response.content)

            if profile == 'sg_mutex':
                url = f'http://{host}:4985/_debug/pprof/mutex'
                response = requests.get(url=url)
                self.save(host, service, profile, response.content)

            if profile == 'sg_fgprof':
                url = f'http://{host}:4985/_debug/fgprof'
                response = requests.get(url=url)
                self.save(host, service, profile, response.content)

            self.copy_profiles(host=host)

        elif self.cluster_spec.capella_infrastructure:
            endpoint = self.ENDPOINTS[profile]
            port = 10000 + self.DEBUG_PORTS[service]
            logger.info(f'Collecting {profile} profile on {host}')
            url = endpoint.format(str(port))
            url = url.replace("http://127.0.0.1", f"https://{host}")
            response = self.rest.get(url=url)
            self.save(host, service, profile, response.content)

        else:
            endpoint = self.ENDPOINTS[profile]
            port = self.DEBUG_PORTS[service]

            logger.info(f'Collecting {profile} profile on {host}')
            # Fetch directly over HTTP; SSH tunnel is a fallback only
            # (SSHTunnelForwarder.stop() can block forever on a half-dead connection).
            try:
                url = endpoint.format(port).replace('127.0.0.1', host)
                response = requests.get(url=url, auth=self.rest.auth,
                                        timeout=self.PROFILE_TIMEOUT)
                self.save(host, service, profile, response.content)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                try:
                    with self.new_tunnel(host, port) as tunnel:
                        url = endpoint.format(tunnel.local_bind_port)
                        response = requests.get(url=url, auth=self.rest.auth,
                                                timeout=self.PROFILE_TIMEOUT)
                        self.save(host, service, profile, response.content)
                except Exception as e:
                    logger.warning(
                        f'Profile collection failed over tunnel ({service} {profile} '
                        f'on {host}): {e} -- skipping this sample'
                    )
            except Exception as e:
                logger.warning(
                    f'Profile collection failed/timed out ({service} {profile} '
                    f'on {host}): {e} -- skipping this sample'
                )

    def new_tunnel(self, host: str, port: int) -> SSHTunnelForwarder:
        return SSHTunnelForwarder(
            ssh_address_or_host=host,
            ssh_username=self.ssh_username,
            ssh_password=self.ssh_password,
            remote_bind_address=('127.0.0.1', port),
        )

    def copy_profiles(self, host: str):
        logger.info("Copying profile files from SG servers")
        transport = paramiko.Transport((host, 22))
        transport.connect(username=self.ssh_username, password=self.ssh_password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            for fname in sftp.listdir('/home/sync_gateway/'):
                if fname.endswith('.pprof'):
                    sftp.get(f'/home/sync_gateway/{fname}', f'./{fname}')
        finally:
            sftp.close()
            transport.close()

    def save(self, host: str, service: str, profile: str, content: bytes):
        fname = '{}_{}_{}_{}_{}.pprof'.format(
            host, service, profile, time.strftime("%y%m%d%H%M%S"), uhex()[:6])
        logger.info('Collected {} '.format(fname))
        with open(fname, 'wb') as fh:
            fh.write(content)


class LinuxPerfProfiler (ProfilerBase):

    """Linux perf profiler profile individual services using `perf` subcommands.

    `profiling.profiles` setting can be one of the following:
        1. 'cpu': (Default if nothing is specified) runs `perf record`
        2. 'c2c': runs `perf c2c record`
        3. 'mem': runs `perf mem record`
    """

    # perf (record | mem | c2c)
    # Specific functions probe are not supported yet
    PERF_COMMANDS = {
        'cpu': 'record',
        'c2c': 'c2c record',
        'mem': 'mem record'
    }

    SERVICE_MAP = {  # For backward compatibility
        'kv': 'memcached'
    }

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        super().__init__(cluster_spec, test_config)
        self.linux_perf_path = '/opt/couchbase/var/lib/couchbase/logs/'
        if not self.profiling_settings.services:
            self.profiling_settings.services = ['kv']

    def profile(self, host: str, service: str, profile: str):
        fname = 'linux_{}_{}_{}_{}_perf.data'.format(host, service, profile, uhex()[:4])

        service = self.SERVICE_MAP.get(service, service)
        perf_profile = self.PERF_COMMANDS.get(profile, profile)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            client.connect(hostname=host, username=self.ssh_username,
                           password=self.ssh_password)

        except Exception:
            logger.info('Cannot connect to the "{}" via SSH Server'.format(host))
            exit()

        logger.info('Capturing linux `perf {}` profile on {}'.format(profile, host))

        args = [
            'perf {} -a'.format(perf_profile),
            '-F {}'.format(self.profiling_settings.linux_perf_frequency),
            '-g --call-graph {}'.format(self.profiling_settings.linux_perf_callgraph),
            '-p $(pgrep {})'.format(service) if 'mem' not in profile else '',
            '-o {}{}'.format(self.linux_perf_path, fname),
            '-- sleep {}'.format(self.profiling_settings.linux_perf_profile_duration)
        ]
        cmd = '{}'.format(' '.join(args))
        _, stdout, _ = client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            logger.info('linux perf record: linux perf profile capture completed')
        else:
            logger.info('perf record failed , exit_status : {}'.format(exit_status))

        client.close()

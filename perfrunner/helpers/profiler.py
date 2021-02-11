import threading
from typing import Callable

import paramiko
import requests
from decorator import decorator
from sshtunnel import SSHTunnelForwarder

from logger import logger
from perfrunner.helpers.misc import uhex
from perfrunner.helpers.rest import RestHelper
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


class Profiler:

    DEBUG_PORTS = {
        'fts':   8094,
        'index': 9102,
        'goxdcr': 9998,
        'kv': 9998,           # will be deprecated in future
        'n1ql':  8093,
    }

    ENDPOINTS = {
        'cpu':  'http://127.0.0.1:{}/debug/pprof/profile',
        'heap': 'http://127.0.0.1:{}/debug/pprof/heap',
        'goroutine': 'http://127.0.0.1:{}/debug/pprof/goroutine?debug=2',
    }

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        self.test_config = test_config

        self.rest = RestHelper(cluster_spec)

        self.master_node = next(cluster_spec.masters)

        self.ssh_username, self.ssh_password = cluster_spec.ssh_credentials

        self.cluster_spec = cluster_spec

        self.profiling_settings = test_config.profiling_settings

        self.linux_perf_path = '/opt/couchbase/var/lib/couchbase/logs/'

    def new_tunnel(self, host: str, port: int) -> SSHTunnelForwarder:
        return SSHTunnelForwarder(
            ssh_address_or_host=host,
            ssh_username=self.ssh_username,
            ssh_password=self.ssh_password,
            remote_bind_address=('127.0.0.1', port),
        )

    def save(self, host: str, service: str, profile: str, content: bytes):
        fname = '{}_{}_{}_{}.pprof'.format(host, service, profile, uhex()[:6])
        logger.info('Collected {} '.format(fname))
        with open(fname, 'wb') as fh:
            fh.write(content)

    def linux_perf_profile(self, host: str, fname: str, path: str):

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            client.connect(hostname=host, username=self.ssh_username,
                           password=self.ssh_password)

        except Exception:
            logger.info('Cannot connect to the "{}" via SSH Server'.format(host))
            exit()

        logger.info('Capturing linux profile using linux perf record ')

        cmd = 'perf record -a -F {} -g --call-graph {} ' \
              '-p $(pgrep memcached) -o {}{} ' \
              '-- sleep {}'.format(self.profiling_settings.linux_perf_frequency,
                                   self.profiling_settings.linux_perf_callgraph,
                                   path,
                                   fname,
                                   self.profiling_settings.linux_perf_profile_duration)
        stdin, stdout, stderr = client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            logger.info("linux perf record: linux perf profile capture completed")
        else:
            logger.info("perf record failed , exit_status :  ", exit_status)

        client.close()

    def profile(self, host: str, service: str, profile: str):
        logger.info('Collecting {} profile on {}'.format(profile, host))

        endpoint = self.ENDPOINTS[profile]
        port = self.DEBUG_PORTS[service]

        if self.profiling_settings.linux_perf_profile_flag:
            logger.info('Collecting {} profile on {} using linux perf '
                        'reccord'.format(profile, host))

            fname = 'linux_{}_{}_{}_perf.data'.format(host, profile, uhex()[:4])
            self.linux_perf_profile(host=host, fname=fname, path=self.linux_perf_path)

        else:
            logger.info('Collecting {} profile on {}'.format(profile, host))

            with self.new_tunnel(host, port) as tunnel:
                url = endpoint.format(tunnel.local_bind_port)
                response = requests.get(url=url, auth=self.rest.auth)
                self.save(host, service, profile, response.content)

    def timer(self, **kwargs):
        timer = Timer(
            function=self.profile,
            interval=self.test_config.profiling_settings.interval,
            num_runs=self.test_config.profiling_settings.num_profiles,
            kwargs=kwargs,
        )
        timer.start()

    def schedule(self):
        for service in self.test_config.profiling_settings.services:
            logger.info('Scheduling profiling of "{}" services'.format(service))
            for server in self.rest.get_active_nodes_by_role(self.master_node,
                                                             role=service):
                for profile in self.test_config.profiling_settings.profiles:
                    self.timer(host=server, service=service, profile=profile)

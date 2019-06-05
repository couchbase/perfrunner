import json
import os
import threading
import time
from typing import Callable

import requests
from decorator import decorator

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
        self.cluster_spec = ClusterSpec

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
        'kv':    9998,  # goxdcr
        'n1ql':  8093,
        'syncgateway': 4985,
    }

    ENDPOINTS = {
        'cpu':  'http://127.0.0.1:{}/debug/pprof/profile',
        'heap': 'http://127.0.0.1:{}/debug/pprof/heap',
        'goroutine': 'http://127.0.0.1:{}/debug/pprof/goroutine?debug=2',
        'sg_cpu': 'http://127.0.0.1:{}/_profile',
        'sg_heap': 'http://127.0.0.1:{}/_heap',
    }

    def __init__(self, cluster_spec: ClusterSpec, test_config: TestConfig):
        self.test_config = test_config
        self.rest = RestHelper(cluster_spec)
        self.master_node = next(cluster_spec.masters)
        self.ssh_username, self.ssh_password = cluster_spec.ssh_credentials
        self.cluster_spec = cluster_spec

    def save(self, host: str, service: str, profile: str, content: bytes):
        fname = '{}_{}_{}_{}.pprof'.format(host, service, profile, uhex()[:6])
        with open(fname, 'wb') as fh:
            fh.write(content)

    def copy_profiles(self, host: str):
        print("Copying profile files from SG servers")
        os.system('sshpass -p couchbase scp root@{}:/home/sync_gateway/*.pprof ./'.format(host))

    def profile(self, host: str, service: str, profile: str):
        logger.info('Collecting {} profile on {}'.format(profile, host))

        if 'syncgateway' in self.test_config.profiling_settings.services:
            if profile == 'sg_cpu':
                url = 'http://{}:4985/_profile'.format(host)
                filename = '{}_{}_{}_{}.pprof'.format(host, service, profile, uhex()[:6])
                requests.post(url=url, data=json.dumps({"file": filename}))
                time.sleep(self.test_config.profiling_settings.cpu_interval)
                requests.post(url=url, data=json.dumps({}))

            if profile == 'sg_heap':
                filename = '{}_{}_{}_{}.pprof'.format(host, service, profile, uhex()[:6])
                url = 'http://{}:4985/_heap'.format(host)
                requests.post(url=url, data=json.dumps({"file": filename}))

            if profile == 'goroutine':
                url = 'http://{}:4985/_debug/pprof/goroutine'.format(host)
                response = requests.get(url=url)
                self.save(host, service, profile, response.content)

            self.copy_profiles(host=host)

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
            if self.test_config.profiling_settings.services == 'sg_cpu' or 'sg_heap':
                initial_nodes = int(self.test_config.syncgateway_settings.nodes)
                print("number of syncgateway nodes :", initial_nodes)
                for _server in range(initial_nodes):
                    server = self.cluster_spec.servers[_server]
                    for profile in self.test_config.profiling_settings.profiles:
                        self.timer(host=server, service=service, profile=profile)

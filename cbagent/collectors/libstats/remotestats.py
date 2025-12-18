import sys

from decorator import decorator

from perfrunner.remote.api import env, execute, hide, parallel, run, settings

env.shell = '/bin/bash -l -c -o pipefail'
env.keepalive = 60
env.timeout = 60


def parallel_task(server_side=True, stellar_gateway=False):

    @decorator
    def _parallel_task(task, *args, **kargs):
        self = args[0]

        if server_side:
            hosts = self.hosts
        elif stellar_gateway:
            hosts = self.stellar_gateways
        else:
            hosts = self.workers

        with settings(user=self.user, password=self.password, warn_only=True):
            with hide("running", "output"):
                return execute(parallel(task), *args, hosts=hosts, **kargs)

    return _parallel_task


class RemoteStats:

    def __init__(self, hosts, workers, user, password, interval=None, stellar_gateways=None):
        self.hosts = hosts
        self.user = user
        self.password = password
        self.workers = workers
        self.interval = interval
        self.stellar_gateways = stellar_gateways

    def run(self, *args, **kwargs):
        try:
            return run(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit()

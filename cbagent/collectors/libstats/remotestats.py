import sys

from decorator import decorator
from fabric.api import env, hide, parallel, run, settings
from fabric.tasks import execute

env.shell = '/bin/bash -l -c -o pipefail'
env.keepalive = 60
env.timeout = 60


def parallel_task(server_side=True):

    @decorator
    def _parallel_task(task, *args, **kargs):
        self = args[0]

        if server_side:
            hosts = self.hosts
        else:
            hosts = self.workers

        with settings(user=self.user, password=self.password, warn_only=True):
            with hide("running", "output"):
                return execute(parallel(task), *args, hosts=hosts, **kargs)

    return _parallel_task


class RemoteStats:

    def __init__(self, hosts, workers, user, password, interval=None):
        self.hosts = hosts
        self.user = user
        self.password = password
        self.workers = workers
        self.interval = interval

    def run(self, *args, **kwargs):
        try:
            return run(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit()

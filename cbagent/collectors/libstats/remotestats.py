from decorator import decorator

from fabric.api import hide, local, parallel, quiet, run, settings
from fabric.tasks import execute


@decorator
def multi_node_task(task, *args, **kargs):
    self = args[0]
    with settings(user=self.user, password=self.password, warn_only=True):
        with hide("running", "output"):
            return execute(parallel(task), *args, hosts=self.hosts, **kargs)


@decorator
def single_node_task(task, *args, **kargs):
    self = args[0]
    with settings(user=self.user, password=self.password, warn_only=True,
                  host_string=self.hosts[0]):
        with hide("running", "output"):
            return task(*args, **kargs)


def run_local(*args, **kwargs):
    """Run a local command with the same API as a remote command

    Don't pass on the keyword arguments as the ones for the remote call
    (`run`) are not compatible with the `local` call.
    """
    if kwargs.get('quiet', None):
        with quiet():
            return local(*args, capture=True)
    else:
        return local(*args, capture=True)


class RemoteStats(object):

    def __init__(self, hosts, user, password):
        self.hosts = hosts
        self.user = user
        self.password = password
        # Run commands locally if no remote credentials were given
        if user is not None and password is not None:
            self.run = run
        else:
            self.run = run_local

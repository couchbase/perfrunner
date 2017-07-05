from decorator import decorator
from fabric.api import execute, parallel, settings


@decorator
def all_hosts(task, *args, **kwargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.hosts, **kwargs)


@decorator
def single_host(task, *args, **kwargs):
    self = args[0]
    with settings(host_string=self.hosts[0]):
        return task(*args, **kwargs)


@decorator
def all_clients(task, *args, **kwargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.cluster_spec.workers, **kwargs)


@decorator
def single_client(task, *args, **kwargs):
    self = args[0]
    with settings(host_string=self.cluster_spec.workers[0]):
        return task(*args, **kwargs)


@decorator
def index_node(task, *args, **kwargs):
    self = args[0]
    # Get first cluster, its index nodes
    servers = next(self.cluster_spec.servers_by_role('index'))
    if not servers:
        raise RuntimeError("No index nodes specified for cluster")
    with settings(host_string=servers[0]):
        return task(*args, **kwargs)

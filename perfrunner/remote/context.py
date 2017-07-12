from decorator import decorator
from fabric.api import execute, parallel, settings


@decorator
def all_hosts(task, *args, **kwargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.cluster_spec.servers, **kwargs)


@decorator
def single_host(task, *args, **kwargs):
    self = args[0]
    with settings(host_string=self.cluster_spec.servers[0]):
        return task(*args, **kwargs)


@decorator
def all_clients(task, *args, **kwargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.cluster_spec.workers, **kwargs)


@decorator
def index_node(task, *args, **kwargs):
    self = args[0]
    index_node = self.cluster_spec.servers_by_role('index')[0]
    with settings(host_string=index_node):
        return task(*args, **kwargs)

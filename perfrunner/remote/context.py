from decorator import decorator
from fabric.api import execute, parallel, settings


@decorator
def all_hosts(task, *args, **kargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.hosts, **kargs)


@decorator
def single_host(task, *args, **kargs):
    self = args[0]
    with settings(host_string=self.hosts[0]):
        return task(*args, **kargs)


@decorator
def single_client(task, *args, **kargs):
    self = args[0]
    with settings(host_string=self.cluster_spec.workers[0]):
        return task(*args, **kargs)


@decorator
def all_kv_nodes(task, *args, **kargs):
    self = args[0]
    self.host_index = 0
    return execute(parallel(task), *args, hosts=self.kv_hosts, **kargs)

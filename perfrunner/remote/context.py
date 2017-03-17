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
def kv_node_cbindexperf(task, *args, **kargs):
    def _kv_node_cbindexperf(task, *args, **kargs):
        count = 0
        self = args[0]
        for host in self.kv_hosts:
            count += 1
            with settings(host_string=host):
                kargs["host_num"] = count
                task(*args, **kargs)
    return _kv_node_cbindexperf(task, *args, **kargs)


@decorator
def index_node(task, *args, **kargs):
    self = args[0]
    # Get first cluster, its index nodes
    (cluster_name, servers) = \
        self.cluster_spec.yield_servers_by_role('index').next()
    if not servers:
        raise RuntimeError(
            "No index nodes specified for cluster {}".format(cluster_name))
    with settings(host_string=servers[0].split(':')[0]):
        return task(*args, **kargs)

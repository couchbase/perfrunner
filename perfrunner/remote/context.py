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
def kv_node_cbindexperf(task, *args, **kwargs):
    def _kv_node_cbindexperf(task, *args, **kwargs):
        count = 0
        self = args[0]
        for host in self.kv_hosts:
            count += 1
            with settings(host_string=host):
                kwargs["host_num"] = count
                task(*args, **kwargs)
    return _kv_node_cbindexperf(task, *args, **kwargs)


@decorator
def index_node(task, *args, **kwargs):
    self = args[0]
    # Get first cluster, its index nodes
    (cluster_name, servers) = \
        self.cluster_spec.yield_servers_by_role('index').next()
    if not servers:
        raise RuntimeError(
            "No index nodes specified for cluster {}".format(cluster_name))
    with settings(host_string=servers[0].split(':')[0]):
        return task(*args, **kwargs)

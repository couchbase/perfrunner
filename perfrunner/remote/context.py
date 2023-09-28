from typing import Callable, List

from decorator import decorator
from fabric.api import execute, parallel, settings


@decorator
def all_servers(task, *args, **kwargs):
    """Execute the decorated function on all remote server nodes."""
    helper = args[0]

    hosts = helper.cluster_spec.servers

    return execute(parallel(task), *args, hosts=hosts, **kwargs)


@decorator
def master_server(task: Callable, *args, **kwargs):
    """Execute the decorated function on master node."""
    self = args[0]

    hosts = self.cluster_spec.servers

    with settings(host_string=hosts[0]):
        return task(*args, **kwargs)


@decorator
def syncgateway_master_server(task: Callable, *args, **kwargs):
    """Execute the decorated function on master node."""
    self = args[0]
    if self.cluster_spec.sgw_servers:
        if self.cluster_spec.capella_infrastructure and self.cluster_spec.capella_backend == 'aws':
            hosts = self.cluster_spec.sgw_instance_ids
        else:
            hosts = self.cluster_spec.sgw_servers
        with settings(host_string=hosts[0]):
            return task(*args, **kwargs)
    else:
        if self.cluster_spec.capella_infrastructure and self.cluster_spec.capella_backend == 'aws':
            hosts = self.cluster_spec.instance_ids
        else:
            hosts = self.cluster_spec.servers
        with settings(host_string=hosts[0]):
            return task(*args, **kwargs)


@decorator
def syncgateway_servers(task, *args, **kwargs):
    """Execute the decorated function on all remote server nodes."""
    helper = args[0]

    hosts = helper.cluster_spec.sgw_servers

    return execute(parallel(task), *args, hosts=hosts, **kwargs)


def servers_by_role(roles: List[str]):
    """Execute the decorated function on remote server nodes filtered by role."""
    @decorator
    def wrapper(task, *args, **kwargs):
        helper = args[0]

        hosts = []
        for role in roles:
            hosts += helper.cluster_spec.servers_by_role(role=role)

        execute(parallel(task), *args, hosts=hosts, **kwargs)

    return wrapper


@decorator
def all_clients(task: Callable, *args, **kwargs):
    """Execute the decorated function on all remote client machines."""
    helper = args[0]

    hosts = helper.cluster_spec.workers

    return execute(parallel(task), *args, hosts=hosts, **kwargs)


@decorator
def all_clients_batch(task: Callable, *args, **kwargs):
    """Execute the decorated function on all remote client machines."""
    helper = args[0]

    hosts = helper.cluster_spec.workers
    with settings(pool_size=40):
        return execute(parallel(task), *args, hosts=hosts, **kwargs)


@decorator
def master_client(task: Callable, *args, **kwargs):
    """Execute the decorated function on master client."""
    self = args[0]
    with settings(host_string=self.cluster_spec.workers[0]):
        return task(*args, **kwargs)


@decorator
def all_dn_nodes(task, *args, **kwargs):
    """Execute the decorated function on all remote Direct Nebula nodes."""
    helper = args[0]

    hosts = helper.cluster_spec.direct_nebula_instance_ids

    return execute(parallel(task), *args, hosts=hosts, **kwargs)


@decorator
def all_dapi_nodes(task, *args, **kwargs):
    """Execute the decorated function on all remote Data API nodes."""
    helper = args[0]

    hosts = helper.cluster_spec.dapi_instance_ids

    return execute(parallel(task), *args, hosts=hosts, **kwargs)


@decorator
def all_kafka_nodes(task, *args, **kwargs):
    """Execute the decorated function on all remote Kafka nodes."""
    helper = args[0]

    jump_host = helper.cluster_spec.servers[0]
    hosts = helper.cluster_spec.kafka_servers

    # Kafka nodes are in private subnets, so we need to use a jump host that is publicly accessible
    with settings(gateway=jump_host):
        return execute(parallel(task), *args, hosts=hosts, **kwargs)


@decorator
def kafka_zookeepers(task, *args, **kwargs):
    """Execute the decorated function on all remote Kafka Zookeeper nodes."""
    helper = args[0]

    jump_host = helper.cluster_spec.servers[0]
    hosts = helper.cluster_spec.kafka_zookeepers

    # Kafka nodes are in private subnets, so we need to use a jump host that is publicly accessible
    with settings(gateway=jump_host):
        return execute(parallel(task), *args, hosts=hosts, **kwargs)


@decorator
def kafka_brokers(task, *args, **kwargs):
    """Execute the decorated function on all remote Kafka broker nodes."""
    helper = args[0]

    jump_host = helper.cluster_spec.servers[0]
    hosts = helper.cluster_spec.kafka_brokers

    # Kafka nodes are in private subnets, so we need to use a jump host that is publicly accessible
    with settings(gateway=jump_host):
        return execute(parallel(task), *args, hosts=hosts, **kwargs)

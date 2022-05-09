from fabric import state
from fabric.api import run, settings

from logger import logger
from perfrunner.remote.kubernetes import RemoteKubernetes
from perfrunner.remote.linux import RemoteLinux
from perfrunner.remote.windows import RemoteWindows
from perfrunner.settings import ClusterSpec


class RemoteHelper:

    def __new__(cls, cluster_spec: ClusterSpec, verbose: bool = False):
        if not cluster_spec.ssh_credentials:
            return None
        if cluster_spec.dynamic_infrastructure:
            return RemoteKubernetes(cluster_spec, 'kubernetes')

        state.env.user, state.env.password = cluster_spec.ssh_credentials
        state.output.running = verbose
        state.output.stdout = verbose

        if cluster_spec.cloud_infrastructure:
            if cluster_spec.infrastructure_settings.get('os_arch', 'x86_64') == 'arm':
                os = 'arm'
            elif cluster_spec.infrastructure_settings.get('os_arch', 'x86_64') == 'al2':
                os = 'al2'
            else:
                os = cls.detect_server_os(cluster_spec.servers[0], cluster_spec)
        else:
            os = cls.detect_server_os(cluster_spec.servers[0], cluster_spec)
        if os == 'Cygwin':
            return RemoteWindows(cluster_spec, os)
        else:
            return RemoteLinux(cluster_spec, os)

    @staticmethod
    def detect_server_os(server: str, cluster_spec: ClusterSpec):
        state.env.user, state.env.password = cluster_spec.ssh_credentials
        logger.info('Detecting OS on server {}'.format(server))
        with settings(host_string=server):
            os = run('egrep "^(ID)=" /etc/os-release')
            os = os.replace("\"", "").split("=")[-1]
        if os:
            return os
        else:
            return 'Cygwin'

    @staticmethod
    def detect_client_os(server: str, cluster_spec: ClusterSpec):
        state.env.user, state.env.password = cluster_spec.client_credentials
        logger.info('Detecting OS on client {}'.format(server))
        with settings(host_string=server):
            os = run('egrep "^(ID)=" /etc/os-release')
            os = os.replace("\"", "").split("=")[-1]
        return os

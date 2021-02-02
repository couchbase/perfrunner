from fabric import state
from fabric.api import run, settings

from logger import logger
from perfrunner.remote.linux import RemoteLinux
from perfrunner.remote.windows import RemoteWindows
from perfrunner.settings import ClusterSpec


class RemoteHelper:

    def __new__(cls, cluster_spec: ClusterSpec, verbose: bool = False):
        if not cluster_spec.ssh_credentials:
            return None

        state.env.user, state.env.password = cluster_spec.ssh_credentials
        state.output.running = verbose
        state.output.stdout = verbose

        os = cls.detect_os(cluster_spec)
        if os == 'Cygwin':
            return RemoteWindows(cluster_spec, os)
        else:
            return RemoteLinux(cluster_spec, os)

    @staticmethod
    def detect_os(cluster_spec: ClusterSpec):
        logger.info('Detecting OS')
        return RemoteHelper.detect_server_os(cluster_spec.servers[0], cluster_spec)

    @staticmethod
    def detect_server_os(server: str, cluster_spec: ClusterSpec):
        state.env.user, state.env.password = cluster_spec.ssh_credentials
        logger.info('Detecting OS on server {}'.format(server))
        with settings(host_string=server):
            os = run('python -c "import platform; print platform.dist()[0]"')
        if os:
            return os
        else:
            return 'Cygwin'

    @staticmethod
    def detect_client_os(server: str, cluster_spec: ClusterSpec):
        state.env.user, state.env.password = cluster_spec.client_credentials
        logger.info('Detecting OS on server {}'.format(server))
        with settings(host_string=server):
            os = run('python -c "import platform; print platform.dist()[0]"')
        return os

    @staticmethod
    def detect_client_release(server: str):
        logger.info('Detecting OS on client {}'.format(server))
        with settings(host_string=server):
            return run('lsb_release -sr').strip()

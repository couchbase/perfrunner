from typing import Tuple

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
            return RemoteKubernetes(cluster_spec)

        state.env.user, state.env.password = cluster_spec.ssh_credentials
        state.output.running = verbose
        state.output.stdout = verbose

        if cluster_spec.has_any_capella:
            state.env.use_ssh_config = True
            return RemoteLinux(cluster_spec)

        os_platform = cls.detect_os_platform(cluster_spec.servers[0], cluster_spec.ssh_credentials)

        if os_platform.startswith('cygwin'):
            return RemoteWindows(cluster_spec)

        return RemoteLinux(cluster_spec)

    @staticmethod
    def detect_os_platform(host: str, credentials: Tuple[str, str]) -> str:
        state.env.user, state.env.password = credentials
        logger.info('Detecting OS platform on host {} using Python'.format(host))

        with settings(host_string=host):
            platform = run('python3 -c "import platform; print(platform.system())"', quiet=True)
            if platform.return_code != 0:
                platform = run('python -c "import platform; print platform.system()"',
                               warn_only=True)

        if not platform:
            logger.warn('Could not determine OS platform on host {}. Assuming "linux" by default.'
                        .format(host))
            platform = 'linux'

        platform = platform.lower()
        logger.info('Detected OS platform is {}'.format(platform))

        return platform

    @staticmethod
    def detect_client_os(server: str, cluster_spec: ClusterSpec):
        state.env.user, state.env.password = cluster_spec.client_credentials
        logger.info('Detecting OS on client {}'.format(server))
        with settings(host_string=server):
            os = run('egrep "^(ID)=" /etc/os-release')
            os = os.replace("\"", "").split("=")[-1]
        return os

from fabric.api import execute, hide, run, parallel, settings

from perfrunner.logger import logger
from perfrunner.utils.config import ClusterSpec


def all_hosts(task):
    def wrapper(*args, **kargs):
        self = args[0]
        with hide('output', 'running'):
            with settings(user=self.ssh_username, password=self.ssh_password,
                          warn_only=True):
                return execute(parallel(task), *args, hosts=self.hosts, **kargs)
    return wrapper


def single_host(task):
    def wrapper(*args, **kargs):
        self = args[0]
        with hide('output', 'running'):
            with settings(host_string=self.hosts[0],
                          user=self.ssh_username, password=self.ssh_password,
                          warn_only=True):
                return task(*args, **kargs)
    return wrapper


class RemoteHelper(object):

    ARCH = {'i686': 'x86', 'i386': 'x86', 'x86_64': 'x86_64'}

    def __init__(self, spec_file):
        config = ClusterSpec()
        config.parse(spec_file)
        self.__dict__ = config.__dict__

    def wget(self, url, outdir='/tmp'):
        logger.info('Fetching {0}'.format(url))
        run('wget "{0}" -P {1}'.format(url, outdir))

    @single_host
    def detect_pkg(self):
        logger.info('Detecting package manager')
        dist = run('python -c "import platform; print platform.dist()[0]"')
        if dist in ('Ubuntu', 'Debian'):
            return 'deb'
        else:
            return 'rpm'

    @single_host
    def detect_arch(self):
        logger.info('Detecting platform architecture')
        arch = run('arch')
        return self.ARCH[arch]

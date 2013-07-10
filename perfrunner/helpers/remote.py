from uuid import uuid4

from fabric.api import execute, hide, get, run, parallel, settings
from logger import logger

from perfrunner.helpers import Helper


def all_hosts(task):
    def wrapper(*args, **kargs):
        self = args[0]
        with hide('output', 'running'):
            with settings(user=self.ssh_username, password=self.ssh_password):
                return execute(parallel(task), *args, hosts=self.hosts, **kargs)
    return wrapper


def single_host(task):
    def wrapper(*args, **kargs):
        self = args[0]
        with hide('output', 'running'):
            with settings(host_string=self.hosts[0],
                          user=self.ssh_username, password=self.ssh_password):
                return task(*args, **kargs)
    return wrapper


class RemoteHelper(Helper):

    ARCH = {'i686': 'x86', 'i386': 'x86', 'x86_64': 'x86_64'}

    def wget(self, url, outdir='/tmp'):
        logger.info('Fetching {0}'.format(url))
        run('wget -nc "{0}" -P {1}'.format(url, outdir))

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

    @all_hosts
    def reset_swap(self):
        logger.info('Resetting swap')
        run('swapoff --all && swapon --all')

    @all_hosts
    def drop_caches(self):
        logger.info('Dropping memory cache')
        run('sync && echo 3 > /proc/sys/vm/drop_caches')

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')
        fname = '/tmp/{0}.zip'.format(uuid4().hex)
        run('/opt/couchbase/bin/cbcollect_info {0}'.format(fname))
        get('{0}'.format(fname))
        run('rm -f {0}'.format(fname))

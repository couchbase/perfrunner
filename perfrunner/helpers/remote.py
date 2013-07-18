from uuid import uuid4

from fabric import state
from fabric.api import execute, get, run, parallel, settings
from logger import logger


def all_hosts(task):
    def wrapper(self, *args, **kargs):
        return execute(parallel(task), self, *args, hosts=self.hosts, **kargs)
    return wrapper


def single_host(task):
    def wrapper(self, *args, **kargs):
        with settings(host_string=self.hosts[0]):
            return task(self, *args, **kargs)
    return wrapper


class RemoteHelper(object):

    ARCH = {'i686': 'x86', 'i386': 'x86', 'x86_64': 'x86_64'}

    def __init__(self, cluster_spec):
        self.hosts = cluster_spec.get_hosts()
        state.env.user, state.env.password = cluster_spec.get_ssh_credentials()
        state.output.running = False
        state.output.stdout = False

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
    def clean_data_path(self, data_path, index_path):
        for path in (data_path, index_path):
            run('rm -fr {0}/*'.format(path))

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

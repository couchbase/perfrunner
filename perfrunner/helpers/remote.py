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

    ARCH = {'i386': 'x86', 'x86_64': 'x86_64', 'unknown': 'x86_64'}

    def __init__(self, cluster_spec):
        self.hosts = cluster_spec.get_all_hosts()
        state.env.user, state.env.password = cluster_spec.get_ssh_credentials()
        state.output.running = False
        state.output.stdout = False

    def wget(self, url, outdir='/tmp', outfile=None):
        logger.info('Fetching {0}'.format(url))
        if outfile is not None:
            run('wget -nc "{0}" -P {1} -O {2}'.format(url, outdir, outfile))
        else:
            run('wget -nc "{0}" -P {1}'.format(url, outdir))

    def exists(self, fname):
        r = run('test -f "{0}"'.format(fname), warn_only=True, quiet=True)
        return not r.return_code

    @single_host
    def detect_pkg(self):
        logger.info('Detecting package manager')
        dist = run('python -c "import platform; print platform.dist()[0]"',
                   pty=False)
        if not dist:
            return 'setup.exe'
        elif dist in ('Ubuntu', 'Debian'):
            return 'deb'
        else:
            return 'rpm'

    @single_host
    def detect_arch(self):
        logger.info('Detecting platform architecture')
        arch = run('uname -i', pty=False)
        return self.ARCH[arch]

    @single_host
    def detect_openssl(self, pkg):
        logger.info('Detecting openssl version')
        if pkg == 'rpm':
            return run('rpm -q --qf "%{VERSION}" openssl.x86_64')

    @all_hosts
    def reset_swap(self):
        logger.info('Resetting swap')
        run('swapoff --all && swapon --all', warn_only=True, quiet=True)

    @all_hosts
    def drop_caches(self):
        logger.info('Dropping memory cache')
        run('sync && echo 3 > /proc/sys/vm/drop_caches',
            warn_only=True, quiet=True)

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')
        fname = '/tmp/{0}.zip'.format(uuid4().hex)
        r = run('/opt/couchbase/bin/cbcollect_info {0}'.format(fname),
                warn_only=True, quiet=True)
        if not r.return_code:
            get('{0}'.format(fname))
            run('rm -f {0}'.format(fname))

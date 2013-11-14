import time
from decorator import decorator

from fabric import state
from fabric.api import execute, get, put, run, sudo, parallel, settings
from logger import logger

from perfrunner.helpers.misc import uhex


@decorator
def all_hosts(task, *args, **kargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.hosts, **kargs)


@decorator
def single_host(task, *args, **kargs):
    self = args[0]
    with settings(host_string=self.hosts[0]):
        return task(*args, **kargs)


class RemoteHelper(object):

    def __new__(cls, cluster_spec):
        state.env.user, state.env.password = cluster_spec.get_ssh_credentials()
        state.output.running = False
        state.output.stdout = False

        os = cls.detect_os(cluster_spec)
        if os == 'Cygwin':
            return RemoteWindowsHelper(cluster_spec, os)
        else:
            return RemoteLinuxHelper(cluster_spec, os)

    @staticmethod
    def detect_os(cluster_spec):
        logger.info('Detecting OS')
        with settings(host_string=cluster_spec.get_all_hosts()[0]):
            os = run('python -c "import platform; print platform.dist()[0]"',
                     pty=False)
        if os:
            return os
        else:
            return 'Cygwin'


class RemoteLinuxHelper(object):

    ARCH = {'i386': 'x86', 'x86_64': 'x86_64', 'unknown': 'x86_64'}

    ROOT_DIR = '/opt/couchbase'

    def __init__(self, cluster_spec, os):
        self.os = os
        self.hosts = cluster_spec.get_all_hosts()
        self.cluster_spec = cluster_spec

    def wget(self, url, outdir='/tmp', outfile=None):
        logger.info('Fetching {0}'.format(url))
        if outfile is not None:
            run('wget -nc "{0}" -P {1} -O {2}'.format(url, outdir, outfile))
        else:
            run('wget -nc "{0}" -P {1}'.format(url, outdir))

    @single_host
    def detect_pkg(self):
        logger.info('Detecting package manager')
        if self.os in ('Ubuntu', 'Debian'):
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
        sudo('swapoff --all && swapon --all')

    @all_hosts
    def drop_caches(self):
        logger.info('Dropping memory cache')
        sudo('sync && echo 3 > /proc/sys/vm/drop_caches')

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')

        sudo('rm -f /tmp/*.zip')

        fname = '/tmp/{0}.zip'.format(uhex())
        r = run('{0}/bin/cbcollect_info {1}'.format(self.ROOT_DIR, fname),
                warn_only=True)
        if not r.return_code:
            get('{0}'.format(fname))
            sudo('rm -f {0}'.format(fname))

    @all_hosts
    def clean_data(self):
        for path in self.cluster_spec.get_paths():
            sudo('rm -fr {0}/*'.format(path))
        sudo('rm -fr {0}'.format(self.ROOT_DIR))

    @all_hosts
    def uninstall_package(self, pkg):
        logger.info('Uninstalling Couchbase Server')
        if pkg == 'deb':
            sudo('yes | apt-get remove couchbase-server')
        else:
            sudo('yes | yum remove couchbase-server')

    @all_hosts
    def install_package(self, pkg, url, filename, version=None):
        self.wget(url, outdir='/tmp')

        logger.info('Installing Couchbase Server')
        if pkg == 'deb':
            sudo('yes | apt-get install gdebi')
            sudo('yes | gdebi /tmp/{0}'.format(filename))
        else:
            sudo('yes | rpm -i /tmp/{0}'.format(filename))

    @all_hosts
    def change_num_vbuckets(self, num_vbuckets):
        logger.info('Changing num of vbuckets to %s' % num_vbuckets)

        sudo('yes | /etc/init.d/couchbase-server stop')
        sudo("yes | sed -i 's/ulimit -c unlimited/ulimit -c unlimited\\n "
             "  export COUCHBASE_NUM_VBUCKETS=%s/' /opt/couchbase/etc/couchbase_init.d" % num_vbuckets)
        sudo('yes | /etc/init.d/couchbase-server start')

    @all_hosts
    def kill_processes(self):
        logger.info('Killing beam.smp, memcached and epmd')
        run('killall -9 beam.smp memcached epmd', warn_only=True, quiet=True)

class RemoteWindowsHelper(RemoteLinuxHelper):

    ROOT_DIR = '/cygdrive/c/Program\ Files/Couchbase/Server'

    VERSION_FILE = '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'

    def exists(self, fname):
        r = run('test -f "{0}"'.format(fname), warn_only=True, quiet=True)
        return not r.return_code

    @single_host
    def detect_pkg(self):
        logger.info('Detecting package manager')
        return 'setup.exe'

    @single_host
    def detect_openssl(self, pkg):
        pass

    @all_hosts
    def reset_swap(self):
        pass

    @all_hosts
    def drop_caches(self):
        pass

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')

        run('rm -f *.zip')

        fname = '{0}.zip'.format(uhex())
        r = run('{0}/bin/cbcollect_info.exe {1}'.format(self.ROOT_DIR, fname),
                warn_only=True)
        if not r.return_code:
            get('{0}'.format(fname))
            run('rm -f {0}'.format(fname))

    @all_hosts
    def clean_data(self):
        for path in self.cluster_spec.get_paths():
            path = path.replace(':', '').replace('\\', '/')
            path = '/cygdrive/{0}'.format(path)
            run('rm -fr {0}/*'.format(path))
        run('rm -fr {0}'.format(self.ROOT_DIR))

    @all_hosts
    def uninstall_package(self, *args, **kwargs):
        logger.info('Uninstalling Couchbase Server')

        if self.exists(self.VERSION_FILE):
            run('./setup.exe -s -f1"C:\\uninstall.iss"')
        while self.exists(self.VERSION_FILE):
            time.sleep(5)
        time.sleep(30)

    def put_iss_files(self, version):
        logger.info('Copying {0} ISS files'.format(version))
        put('scripts/install_{0}.iss'.format(version),
            '/cygdrive/c/install.iss')
        put('scripts/uninstall_{0}.iss'.format(version),
            '/cygdrive/c/uninstall.iss')

    @all_hosts
    def install_package(self, pkg, url, filename, version=None):
        run('rm -fr setup.exe')
        self.wget(url, outfile='setup.exe')
        run('chmod +x setup.exe')

        self.put_iss_files(version)

        logger.info('Installing Couchbase Server')

        run('./setup.exe -s -f1"C:\\install.iss"')
        while not self.exists(self.VERSION_FILE):
            time.sleep(5)
        time.sleep(60)


    @all_hosts
    def change_num_vbuckets(self, num_vbuckets):
        logger.info('Changing num of vbuckets in Windows is not supported yet.')
        return 

    @all_hosts
    def kill_processes(self):
        logger.info('Killing processes in Windows is not supported yet.')
        return

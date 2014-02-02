import time

from decorator import decorator
from fabric import state
from fabric.api import execute, get, put, run, parallel, settings
from fabric.exceptions import CommandTimeout
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
        state.env.user, state.env.password = cluster_spec.ssh_credentials
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
        with settings(host_string=cluster_spec.yield_hostnames().next()):
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
        self.hosts = tuple(cluster_spec.yield_hostnames())
        self.cluster_spec = cluster_spec

    @staticmethod
    def wget(url, outdir='/tmp', outfile=None):
        logger.info('Fetching {}'.format(url))
        if outfile is not None:
            run('wget -nc "{}" -P {} -O {}'.format(url, outdir, outfile))
        else:
            run('wget -nc "{}" -P {}'.format(url, outdir))

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
        run('swapoff --all && swapon --all')

    @all_hosts
    def drop_caches(self):
        logger.info('Dropping memory cache')
        run('sync && echo 3 > /proc/sys/vm/drop_caches')

    @all_hosts
    def set_swappiness(self):
        logger.info('Changing swappiness to 0')
        run('sysctl vm.swappiness=0')

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')

        run('rm -f /tmp/*.zip')

        fname = '/tmp/{}.zip'.format(uhex())
        try:
            r = run('{}/bin/cbcollect_info {}'.format(self.ROOT_DIR, fname),
                    warn_only=True, timeout=1200)
        except CommandTimeout:
            logger.error('cbcollect_info timed out')
            return
        if not r.return_code:
            get('{}'.format(fname))
            run('rm -f {}'.format(fname))

    @all_hosts
    def clean_data(self):
        for path in self.cluster_spec.paths:
            run('rm -fr {}/*'.format(path))
        run('rm -fr {}'.format(self.ROOT_DIR))

    @all_hosts
    def kill_processes(self):
        logger.info('Killing beam.smp, memcached and epmd')
        run('killall -9 beam.smp memcached epmd', warn_only=True, quiet=True)

    @all_hosts
    def uninstall_package(self, pkg):
        logger.info('Uninstalling Couchbase Server')
        if pkg == 'deb':
            run('yes | apt-get remove couchbase-server')
        else:
            run('yes | yum remove couchbase-server')

    @all_hosts
    def install_package(self, pkg, url, filename, version=None):
        self.wget(url, outdir='/tmp')

        logger.info('Installing Couchbase Server')
        if pkg == 'deb':
            run('yes | apt-get install gdebi')
            run('yes | gdebi /tmp/{}'.format(filename))
        else:
            run('yes | rpm -i /tmp/{}'.format(filename))

    @all_hosts
    def restart_with_alternative_swt(self, swt='medium'):
        """+swt very_low|low|medium|high|very_high"""
        logger.info('Change +swt to {} and restart server'.format(swt))

        run('ERL_AFLAGS="+swt {}" /etc/init.d/couchbase-server restart'
            .format(swt))

    @all_hosts
    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        logger.info('Change number of vbuckets to {}'.format(num_vbuckets))
        run('COUCHBASE_NUM_VBUCKETS={} /etc/init.d/couchbase-server restart'
            .format(num_vbuckets))

    @all_hosts
    def stop_server(self):
        logger.info('Stopping Couchbase Server')
        run('/etc/init.d/couchbase-server stop')

    @all_hosts
    def start_server(self):
        logger.info('Starting Couchbase Server')
        run('/etc/init.d/couchbase-server start')


class RemoteWindowsHelper(RemoteLinuxHelper):

    ROOT_DIR = '/cygdrive/c/Program\ Files/Couchbase/Server'

    VERSION_FILE = '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'

    MAX_RETRIES = 5

    TIMEOUT = 600

    SLEEP_TIME = 60  # crutch

    @staticmethod
    def exists(fname):
        r = run('test -f "{}"'.format(fname), warn_only=True, quiet=True)
        return not r.return_code

    @single_host
    def detect_pkg(self):
        logger.info('Detecting package manager')
        return 'setup.exe'

    @single_host
    def detect_openssl(self, pkg):
        pass

    def reset_swap(self):
        pass

    def drop_caches(self):
        pass

    def set_swappiness(self):
        pass

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')

        run('rm -f *.zip')

        fname = '{}.zip'.format(uhex())
        r = run('{}/bin/cbcollect_info.exe {}'.format(self.ROOT_DIR, fname),
                warn_only=True)
        if not r.return_code:
            get('{}'.format(fname))
            run('rm -f {}'.format(fname))

    @all_hosts
    def clean_data(self):
        for path in self.cluster_spec.paths:
            path = path.replace(':', '').replace('\\', '/')
            path = '/cygdrive/{}'.format(path)
            run('rm -fr {}/*'.format(path))

    def kill_processes(self):
        pass

    def kill_installer(self):
        output = run('ps -ef', warn_only=True, quiet=True)
        for line in output.split('\n'):
            if '/home/Administrator/setup' in line:
                pid = line.split()[1]
                logger.info('Killing running setup.exe, pid={}'.format(pid))
                run('kill {}'.format(pid), warn_only=True, quiet=True)

    def clean_installation(self):
        put('scripts/clean.reg', '/cygdrive/c/clean.reg')
        run('regedit.exe /s "C:\\clean.reg"', warn_only=True)
        run('rm -fr {}'.format(self.ROOT_DIR))

    @all_hosts
    def uninstall_package(self, pkg):
        logger.info('Uninstalling Couchbase Server')

        if self.exists(self.VERSION_FILE):
            for retry in range(self.MAX_RETRIES):
                self.kill_installer()
                try:
                    r = run('./setup.exe -s -f1"C:\\uninstall.iss"',
                            warn_only=True, quiet=True, timeout=self.TIMEOUT)
                    if not r.return_code:
                        while self.exists(self.VERSION_FILE):
                            logger.info('Waiting for Uninstaller to finish')
                            time.sleep(5)
                        break
                except CommandTimeout:
                    continue
            else:
                logger.warn('Failed to uninstall package, cleaning registry')
        self.clean_installation()

    @staticmethod
    def put_iss_files(version):
        logger.info('Copying {} ISS files'.format(version))
        put('scripts/install_{}.iss'.format(version),
            '/cygdrive/c/install.iss')
        put('scripts/uninstall_{}.iss'.format(version),
            '/cygdrive/c/uninstall.iss')

    @all_hosts
    def install_package(self, pkg, url, filename, version=None):
        self.kill_installer()
        run('rm -fr setup.exe')
        self.wget(url, outfile='setup.exe')
        run('chmod +x setup.exe')

        self.put_iss_files(version)

        logger.info('Installing Couchbase Server')

        run('./setup.exe -s -f1"C:\\install.iss"')
        while not self.exists(self.VERSION_FILE):
            logger.info('Waiting for Installer to finish')
            time.sleep(5)
        logger.info('Sleeping for {} seconds'.format(self.SLEEP_TIME))
        time.sleep(self.SLEEP_TIME)

    def restart_with_alternative_swt(self, swt='medium'):
        pass

    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        pass

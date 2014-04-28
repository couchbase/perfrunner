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
    def disable_thp(self):
        run('echo never > /sys/kernel/mm/transparent_hugepage/enabled')

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
        run('killall -9 beam.smp memcached epmd cbq-engine',
            warn_only=True, quiet=True)

    @all_hosts
    def uninstall_package(self, pkg):
        logger.info('Uninstalling Couchbase Server')
        if pkg == 'deb':
            run('yes | apt-get remove couchbase-server', quiet=True)
        else:
            run('yes | yum remove couchbase-server', quiet=True)

    @all_hosts
    def install_package(self, pkg, url, filename, version=None):
        self.wget(url, outdir='/tmp')

        logger.info('Installing Couchbase Server')
        if pkg == 'deb':
            run('yes | apt-get install gdebi')
            run('yes | numactl --interleave=all gdebi /tmp/{}'.format(filename))
        else:
            run('yes | numactl --interleave=all rpm -i /tmp/{}'.format(filename))

    @all_hosts
    def restart(self):
        logger.info('Restarting server')
        run('/etc/init.d/couchbase-server restart')

    @all_hosts
    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        logger.info('Changing number of vbuckets to {}'.format(num_vbuckets))
        run('COUCHBASE_NUM_VBUCKETS={} /etc/init.d/couchbase-server restart'
            .format(num_vbuckets))

    @all_hosts
    def restart_with_alternative_num_cpus(self, num_cpus):
        logger.info('Changing number of vbuckets to {}'.format(num_cpus))
        run('MEMCACHED_NUM_CPUS={} '
            'numactl --interleave=all '
            '/etc/init.d/couchbase-server restart'
            .format(num_cpus))

    @all_hosts
    def disable_moxi(self):
        logger.info('Disabling moxi')
        run('rm /opt/couchbase/bin/moxi')
        run('killall -9 moxi')

    @all_hosts
    def stop_server(self):
        logger.info('Stopping Couchbase Server')
        run('/etc/init.d/couchbase-server stop')

    @all_hosts
    def start_server(self):
        logger.info('Starting Couchbase Server')
        run('/etc/init.d/couchbase-server start')

    def detect_if(self):
        for iface in ('eth0', 'em1'):
            result = run('grep {} /proc/net/dev'.format(iface),
                         warn_only=True, quiet=True)
            if not result.return_code:
                return iface

    def detect_ip(self, _if):
        ifconfing = run('ifconfig {} | grep "inet addr"'.format(_if))
        return ifconfing.split()[1].split(':')[1]

    @all_hosts
    def disable_wan(self):
        logger.info('Disabling WAN effects')
        _if = self.detect_if()
        run('tc qdisc del dev {} root'.format(_if), warn_only=True, quiet=True)

    @all_hosts
    def enable_wan(self):
        logger.info('Enabling WAN effects')
        _if = self.detect_if()
        for cmd in (
            'tc qdisc add dev {} handle 1: root htb',
            'tc class add dev {} parent 1: classid 1:1 htb rate 1gbit',
            'tc class add dev {} parent 1:1 classid 1:11 htb rate 1gbit',
            'tc qdisc add dev {} parent 1:11 handle 10: netem delay 40ms 2ms '
            'loss 0.005% 50% duplicate 0.005% corrupt 0.005%',
        ):
            run(cmd.format(_if))

    @all_hosts
    def filter_wan(self, src_list, dest_list):
        logger.info('Filtering WAN effects')
        _if = self.detect_if()

        if self.detect_ip(_if) in src_list:
            _filter = dest_list
        else:
            _filter = src_list

        for ip in _filter:
            run('tc filter add dev {} protocol ip prio 1 u32 '
                'match ip dst {} flowid 1:11'.format(_if, ip))

    @single_host
    def detect_number_cores(self):
        logger.info('Detecting number of cores')
        return int(run('nproc', pty=False))

    @all_hosts
    def start_cbq(self):
        logger.info('Starting cbq-engine')
        url = 'http://{}:{}@127.0.0.1:8091'.format(
            *self.cluster_spec.rest_credentials)
        return run('dtach -n /tmp/cbq-engine.sock '
                   'cbq-engine -couchbase={}'.format(url))


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

    def disable_thp(self):
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
        run('taskkill /F /T /IM setup.exe', warn_only=True, quiet=True)

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
                        t0 = time.time()
                        while self.exists(self.VERSION_FILE) and \
                                time.time() - t0 < self.TIMEOUT:
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

    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        pass

    def disable_wan(self):
        pass

    def enable_wan(self):
        pass

    def filter_wan(self, *args):
        pass

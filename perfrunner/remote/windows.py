import time

from fabric.api import get, put, run, settings, show
from fabric.exceptions import CommandTimeout

from logger import logger
from perfrunner.helpers.misc import uhex
from perfrunner.remote import Remote
from perfrunner.remote.context import all_servers, master_server


class RemoteWindows(Remote):

    CB_DIR = '/cygdrive/c/Program\\ Files/Couchbase/Server'

    VERSION_FILE = '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'

    MAX_RETRIES = 5

    TIMEOUT = 300

    SLEEP_TIME = 30  # crutch

    PROCESSES = ('erl*', 'epmd*', 'memcached')

    @staticmethod
    def exists(fname):
        r = run('test -f "{}"'.format(fname), warn_only=True, quiet=True)
        return not r.return_code

    @property
    def package(self):
        return 'exe'

    def reset_swap(self):
        pass

    def drop_caches(self):
        pass

    def set_swappiness(self):
        pass

    def disable_thp(self):
        pass

    def flush_iptables(self):
        pass

    def detect_ip(self):
        return run('ipconfig | findstr IPv4').split(': ')[1]

    @all_servers
    def collect_info(self):
        logger.info('Running cbcollect_info')

        run('rm -f *.zip')

        fname = '{}.zip'.format(uhex())
        r = run('{}/bin/cbcollect_info.exe {}'.format(self.CB_DIR, fname),
                warn_only=True)
        if not r.return_code:
            get('{}'.format(fname))
            run('rm -f {}'.format(fname))

    @all_servers
    def clean_data(self):
        for path in self.cluster_spec.paths:
            path = path.replace(':', '').replace('\\', '/')
            path = '/cygdrive/{}'.format(path)
            run('rm -fr {}/*'.format(path))

    @all_servers
    def kill_processes(self):
        logger.info('Killing {}'.format(', '.join(self.PROCESSES)))
        run('taskkill /F /T /IM {}'.format(' /IM '.join(self.PROCESSES)),
            warn_only=True, quiet=True)

    def shutdown(self, host):
        with settings(host_string=host):
            logger.info('Killing {}'.format(', '.join(self.PROCESSES)))
            run('taskkill /F /T /IM {}'.format(' /IM '.join(self.PROCESSES)),
                warn_only=True, quiet=True)

    def kill_installer(self):
        run('taskkill /F /T /IM setup.exe', warn_only=True, quiet=True)

    def uninstall_exe(self, local_ip: str):
        script = './setup.exe -s -f1"C:\\uninstall.iss"'

        for retry in range(self.MAX_RETRIES):
            self.kill_installer()

            try:
                r = run(script, quiet=True, timeout=self.TIMEOUT)
            except CommandTimeout:
                logger.warn("Script timed out on {}. Retrying.".format(local_ip))
                continue

            if r.return_code:  # Non-zero return code
                logger.warn('Script failed on {}. Retrying.'.format(local_ip))
                continue

            return

        logger.warn('Script failed with no more retries on {}'.format(local_ip))

    def uninstall_msi(self):
        run('msiexec /x setup.msi /n /q', warn_only=True, timeout=self.TIMEOUT)

    def monitor_remaining_files(self, local_ip: str):
        t0 = time.time()
        while self.exists(self.VERSION_FILE) and \
                time.time() - t0 < self.TIMEOUT:
            logger.info('Waiting for all files to be removed on {}'
                        .format(local_ip))
            time.sleep(5)

    def clean_installation(self):
        with settings(warn_only=True):
            run('rm -fr {}'.format(self.CB_DIR))

    @all_servers
    def uninstall_couchbase(self):
        logger.info('Uninstalling Couchbase Server')
        local_ip = self.detect_ip()

        if self.exists(self.VERSION_FILE):
            if self.exists('setup.msi'):
                self.uninstall_msi()
            else:
                self.uninstall_exe(local_ip)
            self.monitor_remaining_files(local_ip)
        else:
            logger.info('Package not present on {}'.format(local_ip))

        logger.info('Removing files on {}'.format(local_ip))
        self.clean_installation()

    @all_servers
    def upload_iss_files(self, release: str):
        if release > "5.0":
            logger.info('Copying ISS files skipped for release {}'.format(release))
            return
        logger.info('Copying {} ISS files'.format(release))
        put('iss/install_{}.iss'.format(release),
            '/cygdrive/c/install.iss')
        put('iss/uninstall_{}.iss'.format(release),
            '/cygdrive/c/uninstall.iss')

    def download_package(self, url: str, ext: str):
        run('rm -fr setup.*')
        self.wget(url, outfile='setup.{}'.format(ext))

    def install_exe(self):
        run('chmod +x setup.exe', quiet=True)
        run('./setup.exe -s -f1"C:\\install.iss"')

    def install_msi(self):
        run('msiexec /i setup.msi /n /q')

    def monitor_new_files(self, local_ip: str):
        while not self.exists(self.VERSION_FILE):
            logger.info('Checking files on {}'.format(local_ip))
            time.sleep(5)

    @all_servers
    def install_couchbase(self, url: str):
        logger.info('Installing the package')
        local_ip = self.detect_ip()

        self.kill_installer()

        if 'msi' in url:
            self.download_package(url, ext='msi')
            self.install_msi()
        else:
            self.download_package(url, ext='exe')
            self.install_exe()

        self.monitor_new_files(local_ip)

        logger.info('Sleeping for {} seconds'.format(self.SLEEP_TIME))
        time.sleep(self.SLEEP_TIME)

    def restart(self):
        pass

    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        pass

    @master_server
    def enable_nonlocal_diag_eval(self):
        pass

    def disable_wan(self):
        pass

    def enable_wan(self, *args):
        pass

    def filter_wan(self, *args):
        pass

    @all_servers
    def detect_core_dumps(self):
        return []

    def tune_log_rotation(self):
        pass

    @all_servers
    def stop_server(self):
        logger.info('Stopping Couchbase Server')
        run('net stop CouchbaseServer')

    @all_servers
    def start_server(self):
        logger.info('Starting Couchbase Server')
        run('net start CouchbaseServer')

    @all_servers
    def get_system_backup_version(self):
        # Return version of the latest system state backup
        stdout = run('wbadmin get versions | grep identifier')
        return stdout.split()[-1]

    def start_system_state_recovery(self, host, version):
        # Performs a system state recovery to a specified version
        with settings(show('output'), host_string=host):
            run('wbadmin start systemstaterecovery -version:{} -autoReboot -quiet'
                .format(version), warn_only=True)

    def enable_secrets(self, *args, **kwargs):
        pass

    def enable_cpu(self):
        pass

    def clear_wtmp(self):
        pass

    def enable_ipv6(self):
        pass

    def change_owner(self, *args):
        pass

    def detect_auto_failover(self, host):
        with settings(host_string=host):
            r = run('grep "Starting failing over" '
                    '{}/var/lib/couchbase/logs/info.log'.format(self.CB_DIR), quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    def detect_hard_failover_start(self, host):
        with settings(host_string=host):
            r = run('grep "Starting failing" '
                    '{}/var/lib/couchbase/logs/info.log'.format(self.CB_DIR), quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    def detect_graceful_failover_start(self, host):
        with settings(host_string=host):
            r = run('grep "Starting vbucket moves" '
                    '{}/var/lib/couchbase/logs/info.log'.format(self.CB_DIR), quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    def detect_failover_end(self, host):
        with settings(host_string=host):
            r = run('grep "Failed over .*: ok" '
                    '{}/var/lib/couchbase/logs/info.log'.format(self.CB_DIR), quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

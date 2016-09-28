import time

from fabric.api import get, put, run, settings, show
from fabric.exceptions import CommandTimeout
from logger import logger

from perfrunner.helpers.misc import uhex
from perfrunner.remote import Remote
from perfrunner.remote.context import all_hosts, single_host


class RemoteWindows(Remote):

    CB_DIR = '/cygdrive/c/Program\ Files/Couchbase/Server'

    VERSION_FILE = '/cygdrive/c/Program Files/Couchbase/Server/VERSION.txt'

    MAX_RETRIES = 5

    TIMEOUT = 600

    SLEEP_TIME = 60  # crutch

    PROCESSES = ('erl*', 'epmd*')

    @staticmethod
    def exists(fname):
        r = run('test -f "{}"'.format(fname), warn_only=True, quiet=True)
        return not r.return_code

    @single_host
    def detect_pkg(self):
        return 'exe'

    def detect_arch(self):
        return 'amd64'

    def reset_swap(self):
        pass

    def drop_caches(self):
        pass

    def set_swappiness(self):
        pass

    def disable_thp(self):
        pass

    def detect_ip(self):
        return run('ipconfig | findstr IPv4').split(': ')[1]

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')

        run('rm -f *.zip')

        fname = '{}.zip'.format(uhex())
        r = run('{}/bin/cbcollect_info.exe {}'.format(self.CB_DIR, fname),
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

    @all_hosts
    def kill_processes(self):
        logger.info('Killing {}'.format(', '.join(self.PROCESSES)))
        run('taskkill /F /T /IM {}'.format(' /IM '.join(self.PROCESSES)),
            warn_only=True, quiet=True)

    def kill_installer(self):
        run('taskkill /F /T /IM setup.exe', warn_only=True, quiet=True)

    def clean_installation(self):
        with settings(warn_only=True):
            run('rm -fr {}'.format(self.CB_DIR))

    @all_hosts
    def uninstall_couchbase(self, pkg):
        local_ip = self.detect_ip()
        logger.info('Uninstalling Package on {}'.format(local_ip))

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
                            logger.info('Waiting for Uninstaller to finish on {}'.format(local_ip))
                            time.sleep(5)
                        break
                    else:
                        logger.warn('Uninstall script failed to run on {}'.format(local_ip))
                except CommandTimeout:
                    logger.warn("Uninstall command timed out - retrying on {} ({} of {})"
                                .format(local_ip, retry, self.MAX_RETRIES))
                    continue
            else:
                logger.warn('Uninstaller failed with no more retries on {}'
                            .format(local_ip))
        else:
            logger.info('Package not present on {}'.format(local_ip))

        logger.info('Removing files on {}'.format(local_ip))
        self.clean_installation()

    @staticmethod
    def put_iss_files(version):
        logger.info('Copying {} ISS files'.format(version))
        put('iss/install_{}.iss'.format(version),
            '/cygdrive/c/install.iss')
        put('iss/uninstall_{}.iss'.format(version),
            '/cygdrive/c/uninstall.iss')

    @all_hosts
    def install_couchbase(self, pkg, url, filename, version=None):
        self.kill_installer()
        run('rm -fr setup.exe')
        self.wget(url, outfile='setup.exe')
        run('chmod +x setup.exe')

        self.put_iss_files(version)

        local_ip = self.detect_ip()

        logger.info('Installing Package on {}'.format(local_ip))
        try:
            run('./setup.exe -s -f1"C:\\install.iss"')
        except:
            logger.error('Install script failed on {}'.format(local_ip))
            raise

        while not self.exists(self.VERSION_FILE):
            logger.info('Waiting for Installer to finish on {}'.format(local_ip))
            time.sleep(5)

        logger.info('Sleeping for {} seconds'.format(self.SLEEP_TIME))
        time.sleep(self.SLEEP_TIME)

    def restart(self):
        pass

    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        pass

    def disable_wan(self):
        pass

    def enable_wan(self):
        pass

    def filter_wan(self, *args):
        pass

    def detect_core_dumps(self):
        pass

    def tune_log_rotation(self):
        pass

    @all_hosts
    def stop_server(self):
        logger.info('Stopping Couchbase Server')
        run('net stop CouchbaseServer')

    @all_hosts
    def start_server(self):
        logger.info('Starting Couchbase Server')
        run('net start CouchbaseServer')

    @all_hosts
    def get_system_backup_version(self):
        # Return version of the latest system state backup
        stdout = run('wbadmin get versions | grep identifier')
        return stdout.split()[-1]

    def start_system_state_recovery(self, host, version):
        # Performs a system state recovery to a specified version
        with settings(show('output'), host_string=host):
            run('wbadmin start systemstaterecovery -version:{} -autoReboot -quiet'
                .format(version), warn_only=True)

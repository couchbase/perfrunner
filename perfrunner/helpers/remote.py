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


@decorator
def seriesly_host(task, *args, **kargs):
    self = args[0]
    with settings(host_string=self.test_config.gateload_settings.seriesly_host):
        return task(*args, **kargs)


@decorator
def all_gateways(task, *args, **kargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.gateways, **kargs)


@decorator
def all_gateloads(task, *args, **kargs):
    self = args[0]
    return execute(parallel(task), *args, hosts=self.gateloads, **kargs)


class RemoteHelper(object):

    def __new__(cls, cluster_spec, test_config, verbose=False):
        if not cluster_spec.ssh_credentials:
            return None

        state.env.user, state.env.password = cluster_spec.ssh_credentials
        state.output.running = verbose
        state.output.stdout = verbose

        os = cls.detect_os(cluster_spec)
        if os == 'Cygwin':
            return RemoteWindowsHelper(cluster_spec, test_config, os)
        else:
            return RemoteLinuxHelper(cluster_spec, test_config, os)

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

    CB_DIR = '/opt/couchbase'
    MONGO_DIR = '/opt/mongodb'

    PROCESSES = ('beam.smp', 'memcached', 'epmd', 'cbq-engine', 'mongod')

    def __init__(self, cluster_spec, test_config, os):
        self.os = os
        self.hosts = tuple(cluster_spec.yield_hostnames())
        self.cluster_spec = cluster_spec
        self.test_config = test_config
        self.env = {}

        if self.cluster_spec.gateways and test_config is not None:
            num_nodes = self.test_config.gateway_settings.num_nodes
            self.gateways = self.cluster_spec.gateways[:num_nodes]
            self.gateloads = self.cluster_spec.gateloads[:num_nodes]
        else:
            self.gateways = self.gateloads = None

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
    def build_secondary_index(self, index_nodes, bucket, indexes, fields,
                              secondarydb, where_map):
        logger.info('building secondary indexes')

        # Remember what bucket:index was created
        bucket_indexes = []

        for index, field in zip(indexes, fields):
            cmd = "/opt/couchbase/bin/cbindex"
            cmd += ' -auth=Administrator:password'
            cmd += ' -server {}'.format(index_nodes[0])
            cmd += ' -type create -bucket {}'.format(bucket)
            cmd += ' -fields={}'.format(field)

            if secondarydb:
                cmd += ' -using {}'.format(secondarydb)

            if index in where_map and field in where_map[index]:
                # Partition indexes over index nodes by deploying index with
                # where clause on the corresponding index node
                where_list = where_map[index][field]
                for i, (index_node, where) in enumerate(
                        zip(index_nodes, where_list)):
                    # don't taint cmd itself because we need to reuse it.
                    final_cmd = cmd
                    index_i = index + "_{}".format(i)
                    final_cmd += ' -index {}'.format(index_i)
                    final_cmd += " -where='{}'".format(where)

                    # Since .format() is sensitive to {}, use % formatting
                    with_str_template = \
                        r'{\\\"defer_build\\\":true, \\\"nodes\\\":[\\\"%s\\\"]}'
                    with_str = with_str_template % index_node

                    final_cmd += ' -with=\\\"{}\\\"'.format(with_str)
                    bucket_indexes.append("{}:{}".format(bucket, index_i))
                    logger.info('submitting cbindex command {}'.format(final_cmd))
                    status = run(final_cmd, shell_escape=False, pty=False)
                    if status:
                        logger.info('cbindex status {}'.format(status))
            else:
                # no partitions, no where clause
                final_cmd = cmd
                final_cmd += ' -index {}'.format(index)
                with_str = r'{\\\"defer_build\\\":true}'
                final_cmd += ' -with=\\\"{}\\\"'.format(with_str)
                bucket_indexes.append("{}:{}".format(bucket, index))

                logger.info('submitting cbindex command {}'.format(final_cmd))
                status = run(final_cmd, shell_escape=False, pty=False)
                if status:
                    logger.info('cbindex status {}'.format(status))

        time.sleep(10)

        # build indexes
        cmdstr = '/opt/couchbase/bin/cbindex -auth="Administrator:password"'
        cmdstr += ' -server {}'.format(index_nodes[0])
        cmdstr += ' -type build'
        cmdstr += ' -indexes {}'.format(",".join(bucket_indexes))
        logger.info('cbindex build command {}'.format(cmdstr))
        status = run(cmdstr, shell_escape=False, pty=False)
        if status:
            logger.info('cbindex status {}'.format(status))

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
        for path in (
            '/sys/kernel/mm/transparent_hugepage/enabled',
            '/sys/kernel/mm/redhat_transparent_hugepage/enabled',
        ):
            run('echo never > {}'.format(path), quiet=True)

    @all_hosts
    def collect_info(self):
        logger.info('Running cbcollect_info')

        run('rm -f /tmp/*.zip')

        fname = '/tmp/{}.zip'.format(uhex())
        try:
            r = run('{}/bin/cbcollect_info {}'.format(self.CB_DIR, fname),
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
        run('rm -fr {}'.format(self.CB_DIR))

    @all_hosts
    def kill_processes(self):
        logger.info('Killing {}'.format(', '.join(self.PROCESSES)))
        run('killall -9 {}'.format(' '.join(self.PROCESSES)),
            warn_only=True, quiet=True)

    @all_hosts
    def uninstall_couchbase(self, pkg):
        logger.info('Uninstalling Couchbase Server')
        if pkg == 'deb':
            run('yes | apt-get remove couchbase-server', quiet=True)
            run('yes | apt-get remove couchbase-server-community', quiet=True)
        else:
            run('yes | yum remove couchbase-server', quiet=True)
            run('yes | yum remove couchbase-server-community', quiet=True)

    @all_hosts
    def install_couchbase(self, pkg, url, filename, version=None):
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
        environ = ' '.join('{}={}'.format(k, v) for (k, v) in self.env.items())
        run(environ +
            ' numactl --interleave=all /etc/init.d/couchbase-server restart',
            pty=False)

    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        logger.info('Changing number of vbuckets to {}'.format(num_vbuckets))
        self.env['COUCHBASE_NUM_VBUCKETS'] = num_vbuckets
        self.restart()

    def restart_with_alternative_num_cpus(self, num_cpus):
        logger.info('Changing number of front-end memcached threads to {}'
                    .format(num_cpus))
        self.env['MEMCACHED_NUM_CPUS'] = num_cpus
        self.restart()

    def restart_with_sfwi(self):
        logger.info('Enabling +sfwi')
        self.env['COUCHBASE_NS_SERVER_VM_EXTRA_ARGS'] = '["+sfwi", "100", "+sbwt", "long"]'
        self.restart()

    def restart_with_tcmalloc_aggressive_decommit(self):
        logger.info('Enabling TCMalloc aggressive decommit')
        self.env['TCMALLOC_AGGRESSIVE_DECOMMIT'] = 't'
        self.restart()

    @all_hosts
    def disable_moxi(self):
        logger.info('Disabling moxi')
        run('rm /opt/couchbase/bin/moxi')
        run('killall -9 moxi')

    @all_hosts
    def stop_server(self):
        logger.info('Stopping Couchbase Server')
        getosname = run('uname -a|cut -c1-6')
        if(getosname.find("CYGWIN") != -1):
            run('net stop CouchbaseServer')
        else:
            run('/etc/init.d/couchbase-server stop', pty=False)

    @all_hosts
    def start_server(self):
        logger.info('Starting Couchbase Server')
        getosname = run('uname -a|cut -c1-6')
        if(getosname.find("CYGWIN") != -1):
            run('net start CouchbaseServer')
        else:
            run('/etc/init.d/couchbase-server start', pty=False)

    def detect_if(self):
        for iface in ('em1', 'eth5', 'eth0'):
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
    def detect_core_dumps(self):
        # Based on kernel.core_pattern = /tmp/core.%e.%p.%h.%t
        r = run('ls /tmp/core*', quiet=True)
        if not r.return_code:
            return r.split()
        else:
            return []

    @all_hosts
    def tune_log_rotation(self):
        logger.info('Tune log rotation so that it happens less frequently')
        run('sed -i "s/num_files, [0-9]*/num_files, 50/" '
            '/opt/couchbase/etc/couchbase/static_config')

    @all_hosts
    def start_cbq(self):
        logger.info('Starting cbq-engine')
        return run('nohup cbq-engine '
                   '-couchbase=http://127.0.0.1:8091 -dev=true -log=HTTP '
                   '&> /tmp/cbq.log &', pty=False)

    @all_hosts
    def collect_cbq_logs(self):
        logger.info('Getting cbq-engine logs')
        get('/tmp/cbq.log')

    @seriesly_host
    def restart_seriesly(self):
        logger.info('Cleaning up and restarting seriesly')
        run('killall -9 sample seriesly', quiet=True)
        run('rm -f *.txt *.log *.gz *.json *.out /root/seriesly-data/*',
            warn_only=True)
        run('nohup seriesly -flushDelay=1s -root=/root/seriesly-data '
            '&> seriesly.log &', pty=False)

    @seriesly_host
    def start_sampling(self):
        for i, gateway_ip in enumerate(self.gateways, start=1):
            logger.info('Starting sampling gateway_{}'.format(i))
            run('nohup sample -v '
                'http://{}:4985/_expvar http://localhost:3133/gateway_{} '
                '&> sample.log &'.format(gateway_ip, i), pty=False)
        for i, gateload_ip in enumerate(self.gateloads, start=1):
            logger.info('Starting sampling gateload_{}'.format(i))
            run('nohup sample -v '
                'http://{}:9876/debug/vars http://localhost:3133/gateload_{} '
                '&> sample.log &'.format(gateload_ip, i), pty=False)

    @all_gateways
    def install_gateway(self, url, filename):
        logger.info('Installing Sync Gateway package - {}'.format(filename))
        self.wget(url, outdir='/tmp')
        run('yes | rpm -i /tmp/{}'.format(filename))

    @all_gateways
    def install_gateway_from_source(self, commit_hash):
        logger.info('Installing Sync Gateway from source - {}'.format(commit_hash))
        put('scripts/install_sgw_from_source.sh', '/root/install_sgw_from_source.sh')
        run('chmod 777 /root/install_sgw_from_source.sh')
        run('/root/install_sgw_from_source.sh {}'.format(commit_hash), pty=False)

    @all_gateways
    def uninstall_gateway(self):
        logger.info('Uninstalling Sync Gateway package')
        run('yes | yum remove couchbase-sync-gateway')

    @all_gateways
    def kill_processes_gateway(self):
        logger.info('Killing Sync Gateway')
        run('killall -9 sync_gateway sgw_test_info.sh sar', quiet=True)

    @all_gateways
    def clean_gateway(self):
        logger.info('Cleaning up Gateway')
        run('rm -f *.txt *.log *.gz *.json *.out *.prof', quiet=True)

    @all_gateways
    def start_gateway(self):
        logger.info('Starting Sync Gateway instances')
        _if = self.detect_if()
        local_ip = self.detect_ip(_if)
        index = self.gateways.index(local_ip)
        source_config = 'templates/gateway_config_{}.json'.format(index)
        put(source_config, '/root/gateway_config.json')
        godebug = self.test_config.gateway_settings.go_debug
        args = {
            'ulimit': 'ulimit -n 65536',
            'godebug': godebug,
            'sgw': '/opt/couchbase-sync-gateway/bin/sync_gateway',
            'config': '/root/gateway_config.json',
            'log': '/root/gateway.log',
        }
        command = '{ulimit}; GODEBUG={godebug} nohup {sgw} {config} > {log} 2>&1 &'.format(**args)
        logger.info("Command: {}".format(command))
        run(command, pty=False)

    @all_gateways
    def start_test_info(self):
        logger.info('Starting Sync Gateway sgw_test_info.sh')
        put('scripts/sgw_test_config.sh', '/root/sgw_test_config.sh')
        put('scripts/sgw_test_info.sh', '/root/sgw_test_info.sh')
        run('chmod 777 /root/sgw_*.sh')
        run('nohup /root/sgw_test_info.sh &> sgw_test_info.txt &', pty=False)

    @all_gateways
    def collect_info_gateway(self):
        _if = self.detect_if()
        local_ip = self.detect_ip(_if)
        index = self.gateways.index(local_ip)
        logger.info('Collecting diagnostic information from sync gateway_{} {}'.format(index, local_ip))
        run('rm -f gateway.log.gz', warn_only=True)
        run('gzip gateway.log', warn_only=True)
        put('scripts/sgw_check_logs.sh', '/root/sgw_check_logs.sh')
        run('chmod 777 /root/sgw_*.sh')
        run('/root/sgw_check_logs.sh gateway > sgw_check_logs.out', warn_only=True)
        self.try_get('gateway.log.gz', 'gateway.log_{}.gz'.format(index))
        self.try_get('test_info.txt', 'test_info_{}.txt'.format(index))
        self.try_get('test_info_sar.txt', 'test_info_sar_{}.txt'.format(index))
        self.try_get('sgw_test_info.txt', 'sgw_test_info_{}.txt'.format(index))
        self.try_get('gateway_config.json', 'gateway_config_{}.json'.format(index))
        self.try_get('sgw_check_logs.out', 'sgw_check_logs_gateway_{}.out'.format(index))

    @all_gateloads
    def uninstall_gateload(self):
        logger.info('Removing Gateload binaries')
        run('rm -f /opt/gocode/bin/gateload', quiet=True)

    @all_gateloads
    def install_gateload(self):
        logger.info('Installing Gateload')
        run('go get -u github.com/couchbaselabs/gateload')

    @all_gateloads
    def kill_processes_gateload(self):
        logger.info('Killing Gateload processes')
        run('killall -9 gateload', quiet=True)

    @all_gateloads
    def clean_gateload(self):
        logger.info('Cleaning up Gateload')
        run('rm -f *.txt *.log *.gz *.json *.out', quiet=True)

    @all_gateloads
    def start_gateload(self):
        logger.info('Starting Gateload')
        _if = self.detect_if()
        local_ip = self.detect_ip(_if)
        idx = self.gateloads.index(local_ip)

        config_fname = 'templates/gateload_config_{}.json'.format(idx)
        put(config_fname, '/root/gateload_config.json')
        put('scripts/sgw_check_logs.sh', '/root/sgw_check_logs.sh')
        run('chmod 777 /root/sgw_*.sh')
        run('ulimit -n 65536; nohup /opt/gocode/bin/gateload '
            '-workload /root/gateload_config.json &>/root/gateload.log&',
            pty=False)

    @all_gateloads
    def collect_info_gateload(self):
        _if = self.detect_if()
        local_ip = self.detect_ip(_if)
        idx = self.gateloads.index(local_ip)

        logger.info('Collecting diagnostic information from gateload_{} {}'.format(idx, local_ip))
        run('rm -f gateload.log.gz', warn_only=True)
        run('gzip gateload.log', warn_only=True)
        put('scripts/sgw_check_logs.sh', '/root/sgw_check_logs.sh')
        run('chmod 777 /root/sgw_*.sh')
        run('/root/sgw_check_logs.sh gateload > sgw_check_logs.out', warn_only=True)
        self.try_get('gateload.log.gz', 'gateload.log-{}.gz'.format(idx))
        self.try_get('gateload_config.json', 'gateload_config_{}.json'.format(idx))
        self.try_get('gateload_expvars.json', 'gateload_expvar_{}.json'.format(idx))
        self.try_get('sgw_check_logs.out', 'sgw_check_logs_gateload_{}.out'.format(idx))

    @all_gateways
    def collect_profile_data_gateways(self):
        """
        Collect CPU and heap profile raw data as well as rendered pdfs
        from go tool pprof
        """
        _if = self.detect_if()
        local_ip = self.detect_ip(_if)
        idx = self.gateways.index(local_ip)

        logger.info('Collecting profiling data from gateway_{} {}'.format(idx, local_ip))

        put('scripts/sgw_collect_profile.sh', '/root/sgw_collect_profile.sh')
        run('chmod 777 /root/sgw_collect_profile.sh')
        run('/root/sgw_collect_profile.sh /opt/couchbase-sync-gateway/bin/sync_gateway /root', pty=False)
        self.try_get('profile_data.tar.gz', 'profile_data.tar-{}.gz'.format(idx))

    @all_hosts
    def clean_mongodb(self):
        for path in self.cluster_spec.paths:
            run('rm -fr {}/*'.format(path))
        run('rm -fr {}'.format(self.MONGO_DIR))

    @all_hosts
    def install_mongodb(self, url):
        self.wget(url, outdir='/tmp')
        archive = url.split('/')[-1]

        logger.info('Installing MongoDB')

        run('mkdir {}'.format(self.MONGO_DIR))
        run('tar xzf {} -C {} --strip-components 1'.format(archive,
                                                           self.MONGO_DIR))
        run('numactl --interleave=all {}/bin/mongod '
            '--dbpath={} --fork --logpath /tmp/mongodb.log'
            .format(self.MONGO_DIR, self.cluster_spec.paths[0]))

    def try_get(self, remote_path, local_path=None):
        try:
            get(remote_path, local_path)
        except:
            logger.warn("Exception calling get({}, {}).  Ignoring.".format(remote_path, local_path))


class RemoteWindowsHelper(RemoteLinuxHelper):

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
        logger.info('Detecting package manager')
        return 'exe'

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

        logger.info('Cleaning registry on {}'.format(local_ip))
        self.clean_installation()

    @staticmethod
    def put_iss_files(version):
        logger.info('Copying {} ISS files'.format(version))
        put('scripts/install_{}.iss'.format(version),
            '/cygdrive/c/install.iss')
        put('scripts/uninstall_{}.iss'.format(version),
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

    def tune_log_rotation(self):
        pass

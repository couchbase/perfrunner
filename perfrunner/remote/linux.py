import os
from collections import defaultdict
from typing import Dict, List
from urllib.parse import urlparse

from fabric.api import get, put, quiet, run, settings
from fabric.exceptions import CommandTimeout, NetworkError

from logger import logger
from perfrunner.helpers.misc import uhex
from perfrunner.remote import Remote
from perfrunner.remote.context import (
    all_clients,
    all_servers,
    master_server,
    servers_by_role,
)


class RemoteLinux(Remote):

    CB_DIR = '/opt/couchbase'

    PROCESSES = ('beam.smp', 'memcached', 'epmd', 'cbq-engine', 'indexer',
                 'cbft', 'goport', 'goxdcr', 'couch_view_index_updater',
                 'moxi', 'spring', 'sync_gateway')

    PROCESS_PATTERNS = ('cbas', )

    @property
    def package(self):
        if self.os.upper() in ('UBUNTU', 'DEBIAN'):
            return 'deb'
        else:
            return 'rpm'

    @master_server
    def detect_centos_release(self) -> str:
        """Detect CentOS release (e.g., 6 or 7).

        Possible values:
        - CentOS release 6.x (Final)
        - CentOS Linux release 7.2.1511 (Core)
        """
        return run('cat /etc/redhat-release').split()[-2][0]

    @master_server
    def detect_ubuntu_release(self):
        return run('lsb_release -sr').strip()

    def run_cbindex_command(self, options):
        cmd = "/opt/couchbase/bin/cbindex {options}".format(options=options)

        logger.info('Running: {}'.format(cmd))
        run(cmd, shell_escape=False, pty=False)

    def build_index(self, index_node, bucket_indexes):
        all_indexes = ",".join(bucket_indexes)
        options = \
            "-auth=Administrator:password " \
            "-server {index_node}:8091 " \
            "-type build " \
            "-indexes {all_indexes}".format(index_node=index_node,
                                            all_indexes=all_indexes)

        self.run_cbindex_command(options)

    def create_index(self, index_nodes, bucket, indexes, storage):
        # Remember what bucket:index was created
        bucket_indexes = []

        for index, field in indexes.items():
            options = \
                "-auth=Administrator:password " \
                "-server {index_node}:8091 " \
                "-type create " \
                "-bucket {bucket} " \
                "-fields={field}".format(index_node=index_nodes[0],
                                         bucket=bucket,
                                         field=field)

            if storage == 'memdb' or storage == 'plasma':
                options = '{options} -using {db}'.format(options=options,
                                                         db=storage)

            with_str = r'{\\\"defer_build\\\":true}'
            options = "{options} -index {index} -with=\\\"{with_str}\\\"" \
                .format(options=options, index=index, with_str=with_str)

            bucket_indexes.append("{}:{}".format(bucket, index))
            self.run_cbindex_command(options)

        return bucket_indexes

    @master_server
    def build_secondary_index(self, index_nodes, bucket, indexes, storage):
        logger.info('building secondary indexes')

        # Create index but do not build
        bucket_indexes = self.create_index(index_nodes, bucket, indexes, storage)

        # build indexes
        self.build_index(index_nodes[0], bucket_indexes)

    @all_servers
    def reset_swap(self):
        logger.info('Resetting swap')
        run('swapoff --all && swapon --all')

    @all_servers
    def drop_caches(self):
        logger.info('Dropping memory cache')
        run('sync && echo 3 > /proc/sys/vm/drop_caches')

    @all_servers
    def set_swappiness(self):
        logger.info('Changing swappiness to 0')
        run('sysctl vm.swappiness=0')

    @all_servers
    def disable_thp(self):
        for path in (
            '/sys/kernel/mm/transparent_hugepage/enabled',
            '/sys/kernel/mm/redhat_transparent_hugepage/enabled',
            '/sys/kernel/mm/transparent_hugepage/defrag',
        ):
            run('echo never > {}'.format(path), quiet=True)

    @all_servers
    def flush_iptables(self):
        logger.info('Flushing iptables rules')
        run('iptables -F && ip6tables -F')

    @all_servers
    def collect_info(self):
        logger.info('Running cbcollect_info with redaction')

        run('rm -f /tmp/*.zip')

        fname = '/tmp/{}.zip'.format(uhex())
        try:
            r = run('{}/bin/cbcollect_info {}'
                    .format(self.CB_DIR, fname), warn_only=True, timeout=1200)
        except CommandTimeout:
            logger.error('cbcollect_info timed out')
            return
        if not r.return_code:
            get('{}'.format(fname))
            run('rm -f {}'.format(fname))

    @all_servers
    def clean_data(self):
        for path in self.cluster_spec.paths:
            run('rm -fr {}/*'.format(path))
        run('rm -fr {}'.format(self.CB_DIR))

    @all_servers
    def kill_processes(self):
        logger.info('Killing processes that match pattern: {}'
                    .format(', '.join(self.PROCESS_PATTERNS)))
        for pattern in self.PROCESS_PATTERNS:
            run('pkill -9 -f {}'.format(pattern), quiet=True)

        logger.info('Killing processes: {}'.format(', '.join(self.PROCESSES)))
        run('killall -9 {}'.format(' '.join(self.PROCESSES)), quiet=True)

    def shutdown(self, host):
        with settings(host_string=host):
            logger.info('Killing {}'.format(', '.join(self.PROCESSES)))
            run('killall -9 {}'.format(' '.join(self.PROCESSES)),
                warn_only=True, quiet=True)

    def set_write_permissions(self, mask: str, host: str, path: str):
        with settings(host_string=host):
            logger.info('Setting permissions {} to CB data folder {} on host {}'
                        .format(mask, path, host))
            run('chmod {} {}'.format(mask, path))

    @all_servers
    def uninstall_couchbase(self):
        logger.info('Uninstalling Couchbase Server')
        if self.package == 'deb':
            run('yes | apt-get remove couchbase-server', quiet=True)
            run('yes | apt-get remove couchbase-server-dbg', quiet=True)
            run('yes | apt-get remove couchbase-server-community', quiet=True)
            run('yes | apt-get remove couchbase-server-analytics', quiet=True)
        else:
            run('yes | yum remove couchbase-server', quiet=True)
            run('yes | yum remove couchbase-server-debuginfo', quiet=True)
            run('yes | yum remove couchbase-server-community', quiet=True)
            run('yes | yum remove couchbase-server-analytics', quiet=True)

    def upload_iss_files(self, release: str):
        pass

    @all_servers
    def install_couchbase(self, url: str):
        self.wget(url, outdir='/tmp')
        filename = urlparse(url).path.split('/')[-1]

        logger.info('Installing Couchbase Server')
        if self.package == 'deb':
            run('yes | apt-get install gdebi')
            run('yes | apt install -y ./tmp/{}'.format(filename))
        else:
            run('yes | yum localinstall -y /tmp/{}'.format(filename))

    @all_servers
    def restart(self):
        logger.info('Restarting server')
        run('systemctl restart couchbase-server', pty=False)

    @all_servers
    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        logger.info('Changing number of vbuckets to {}'.format(num_vbuckets))
        run('systemctl set-environment COUCHBASE_NUM_VBUCKETS={}'
            .format(num_vbuckets))
        run('systemctl restart couchbase-server', pty=False)
        run('systemctl unset-environment COUCHBASE_NUM_VBUCKETS')

    @master_server
    def enable_nonlocal_diag_eval(self):
        logger.info('Enabling non-local diag/eval')
        run("curl localhost:8091/diag/eval -u Administrator:password "
            "-d 'ns_config:set(allow_nonlocal_eval, true).'", pty=False)

    @all_servers
    def stop_server(self):
        logger.info('Stopping Couchbase Server')
        run('systemctl stop couchbase-server', pty=False)

    @all_servers
    def start_server(self):
        logger.info('Starting Couchbase Server')
        run('systemctl start couchbase-server', pty=False)

    def detect_if(self):
        stdout = run("ip route list | grep default")
        return stdout.strip().split()[4]

    def detect_ip(self, _if):
        stdout = run('ifdata -pa {}'.format(_if))
        return stdout.strip()

    @all_servers
    def disable_wan(self):
        logger.info('Disabling WAN effects')
        _if = self.detect_if()
        run('tc qdisc del dev {} root'.format(_if), warn_only=True, quiet=True)

    @all_servers
    def enable_wan(self, delay: int):
        logger.info('Enabling WAN effects')
        _if = self.detect_if()
        for cmd in (
            'tc qdisc add dev {} handle 1: root htb',
            'tc class add dev {} parent 1: classid 1:1 htb rate 1gbit',
            'tc class add dev {} parent 1:1 classid 1:11 htb rate 1gbit',
            'tc qdisc add dev {} parent 1:11 handle 10: netem delay {}ms 2ms '
            'loss 0.010% 50% duplicate 0.005%',
        ):
            run(cmd.format(_if, delay))

    @all_servers
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

    @all_servers
    def detect_core_dumps(self):
        # Based on kernel.core_pattern = /data/core-%e-%p
        r = run('ls /data/core*', quiet=True)
        if not r.return_code:
            return r.split()
        else:
            return []

    @all_servers
    def tune_log_rotation(self):
        logger.info('Tune log rotation so that it happens less frequently')
        run('sed -i "s/num_files, [0-9]*/num_files, 50/" '
            '/opt/couchbase/etc/couchbase/static_config')

    @master_server
    def restore_data(self, archive_path: str, repo_path: str):
        cmd = \
            "/opt/couchbase/bin/cbbackupmgr restore " \
            "--archive {} --repo {} --threads 24 " \
            "--cluster http://localhost:8091 " \
            "--username Administrator --password password " \
            "--disable-ft-indexes --disable-gsi-indexes".format(
                archive_path,
                repo_path,
            )

        logger.info("Running: {}".format(cmd))
        run(cmd)

    @master_server
    def import_data(self, import_file: str, bucket: str):
        cmd = \
            "/opt/couchbase/bin/cbimport json " \
            "--dataset file://{} --threads 24 " \
            "--cluster http://localhost:8091 " \
            "--bucket {} " \
            "--username Administrator --password password " \
            "--generate-key '#MONO_INCR#' --format lines".format(
                import_file,
                bucket,
            )

        logger.info("Running: {}".format(cmd))
        run(cmd)

    @all_servers
    def fio(self, config):
        logger.info('Running fio job: {}'.format(config))
        filename = os.path.basename(config)
        remote_path = os.path.join('/tmp', filename)
        put(config, remote_path)
        return run('fio --minimal {}'.format(remote_path))

    def detect_auto_failover(self, host):
        with settings(host_string=host):
            r = run('grep "Starting failing over" '
                    '/opt/couchbase/var/lib/couchbase/logs/info.log', quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    def detect_hard_failover_start(self, host):
        with settings(host_string=host):
            r = run('grep "Starting failing" '
                    '/opt/couchbase/var/lib/couchbase/logs/info.log', quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    def detect_graceful_failover_start(self, host):
        with settings(host_string=host):
            r = run('grep "Starting vbucket moves" '
                    '/opt/couchbase/var/lib/couchbase/logs/info.log', quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    def detect_failover_end(self, host):
        with settings(host_string=host):
            r = run('grep "Failed over .*: ok" '
                    '/opt/couchbase/var/lib/couchbase/logs/info.log', quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    @property
    def num_vcpu(self):
        return int(run('lscpu --all --extended | wc -l')) - 1  # Minus header

    def get_cpu_map(self) -> Dict[str, List]:
        cores = run('lscpu --all --parse=socket,core,cpu | grep -v "#"')

        cpu_map = defaultdict(list)
        for cpu_info in cores.split():
            socket, core, cpu = cpu_info.split(',')
            core_id = (int(socket) << 10) + int(core)  # sort by socket, core
            cpu_map[core_id].append(cpu)
        return cpu_map

    @all_servers
    def disable_cpu(self, online_cores: int):
        logger.info('Throttling CPU resources')

        cpu_map = self.get_cpu_map()
        offline_cores = sorted(cpu_map)[online_cores:]

        for core in offline_cores:
            for cpu in cpu_map[core]:
                run('echo 0 > /sys/devices/system/cpu/cpu{}/online'.format(cpu))

    @all_servers
    def enable_cpu(self):
        logger.info('Enabling all CPU cores')
        for i in range(self.num_vcpu):
            run('echo 1 > /sys/devices/system/cpu/cpu{}/online'.format(i))

    @servers_by_role(roles=['index'])
    def kill_process_on_index_node(self, process):
        logger.info('Killing following process on index node: {}'.format(process))
        run("killall {}".format(process))

    def change_owner(self, host, path, owner='couchbase'):
        with settings(host_string=host):
            run('chown -R {0}:{0} {1}'.format(owner, path))

    def get_disk_usage(self, host, path, human_readable=True):
        with settings(host_string=host):
            if human_readable:
                return run("du -h {}".format(path))
            data = run("du -sb {}/@2i".format(path))
            return int(data.split()[0])

    def get_indexer_rss(self, host):
        with settings(host_string=host):
            data = run("ps -eo rss,comm | grep indexer")
            return int(data.split()[0])

    def grub_config(self):
        logger.info('Changing GRUB configuration')
        run('grub2-mkconfig -o /boot/grub2/grub.cfg')
        run('grub2-mkconfig -o /boot/efi/EFI/centos/grub.cfg', warn_only=True)

    def reboot(self):
        logger.info('Rebooting the node')
        run('reboot', quiet=True, pty=False)

    def tune_memory_settings(self, host_string: str, size: str):
        logger.info('Changing kernel memory to {} on {}'.format(size, host_string))
        with settings(host_string=host_string):
            run("sed -i 's/quiet/quiet mem={}/' /etc/default/grub".format(size))
            self.grub_config()
            self.reboot()

    def reset_memory_settings(self, host_string: str):
        logger.info('Resetting kernel memory settings')
        with settings(host_string=host_string):
            run("sed -ir 's/ mem=[0-9]*[kmgKMG]//' /etc/default/grub")
            self.grub_config()
            self.reboot()

    def is_up(self, host_string: str) -> bool:
        with settings(host_string=host_string):
            try:
                result = run(":")
                return result.return_code == 0  # 0 means success
            except NetworkError:
                return False

    @master_server
    def get_manifest(self):
        logger.info('Getting manifest from host node')
        get("{}/manifest.xml".format(self.CB_DIR), local_path="./")

    @all_servers
    def clear_wtmp(self):
        run('echo > /var/log/wtmp')

    @all_servers
    def enable_ipv6(self):
        logger.info('Enabling IPv6')
        run('sed -i "s/{ipv6, false}/{ipv6, true}/" '
            '/opt/couchbase/etc/couchbase/static_config')

    @all_servers
    def setup_x509(self):
        logger.info('Setting up x.509 certificates')
        put("certificates/inbox", "/opt/couchbase/var/lib/couchbase/")
        run('chmod a+x /opt/couchbase/var/lib/couchbase/inbox/chain.pem')
        run('chmod a+x /opt/couchbase/var/lib/couchbase/inbox/pkey.key')

    @all_clients
    def generate_ssl_keystore(self, root_certificate, keystore_file, storepass, worker_home):
        logger.info('Generating SSL keystore')
        remote_keystore = "{}/perfrunner/{}".format(worker_home, keystore_file)
        remote_root_cert = "{}/perfrunner/{}".format(worker_home, root_certificate)
        put(root_certificate, remote_root_cert)

        with quiet():
            run("keytool -delete -keystore {} -alias couchbase -storepass storepass"
                .format(remote_keystore))
        run("keytool -importcert -file {} -storepass {} -trustcacerts "
            "-noprompt -keystore {} -alias couchbase"
            .format(remote_root_cert, storepass, remote_keystore))

    def set_auto_failover(self, host: str, enable: bool, timeout: int = 5):
        logger.info('Setting auto failover: {} on {}'.format(enable, host))
        cmd = "/opt/couchbase/bin/couchbase-cli setting-autofailover"
        details = "--cluster {}:8091 -u Administrator -p password".format(host)
        if enable:
            logger.info("Enabling auto failover")
            run("{} --enable-auto-failover 1 --auto-failover-timeout {} {}"
                .format(cmd, timeout, details))
        else:
            logger.info("Disabling auto failover")
            run("{} --enable-auto-failover 0 {}".format(cmd, details))

    @master_server
    def enable_n2n_encryption(self, host: str, level: str = None):
        logger.info('Enabling node to node encryption on {}'.format(host))
        self.set_auto_failover(host, False)
        run("/opt/couchbase/bin/couchbase-cli node-to-node-encryption --enable "
            "--cluster {}:8091 -u Administrator -p password".format(host))
        self.set_encryption_level(host, level)

    def set_encryption_level(self, host: str, level: str = "control"):
        logger.info('Setting node to node encryption level: {} on {}'.format(level, host))
        run("/opt/couchbase/bin/couchbase-cli setting-security --cluster-encryption-level {} --set"
            " --cluster {}:8091 -u Administrator -p password".format(level, host))

    @master_server
    def run_magma_benchmark(self, cmd: str, stats_file: str):
        logger.info('Running magma benchmark cmd: {}'.format(cmd))
        stdout = run(cmd)
        logger.info(stdout)
        get('{}'.format(stats_file), local_path="./")

    def get_disk_stats(self, server: str):
        logger.info("Getting disk stats for {}".format(server))
        stats = ""
        with settings(host_string=server):
            stats = run("cat /proc/diskstats")
        return stats

    def get_device(self, server: str):
        logger.info("Getting device for {}".format(server))
        device = ""
        with settings(host_string=server):
            device = run("realpath $(df -P /data | awk 'END{print $1}')")
        return device

    def get_device_sector_size(self, server: str, device: str):
        logger.info("Getting device sector size for {} {}".format(server, device))
        sector_size = 0
        with settings(host_string=server):
            sector_size = run("blockdev --getss {}".format(device))
        return sector_size

    def get_memcached_io_stats(self, server: str):
        logger.info("Getting memcached stats for {}".format(server))
        stats = ""
        with settings(host_string=server):
            stats = run("cat /proc/`pidof memcached`/io")
        return stats

import os
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from fabric.api import cd, get, put, quiet, run, settings
from fabric.exceptions import CommandTimeout, NetworkError

from logger import logger
from perfrunner.helpers.misc import run_local_shell_command, uhex
from perfrunner.remote import Remote
from perfrunner.remote.context import (
    all_clients,
    all_dapi_nodes,
    all_dn_nodes,
    all_servers,
    master_client,
    master_server,
    servers_by_role,
    syncgateway_servers,
)
from perfrunner.settings import ClusterSpec


class RemoteLinux(Remote):

    PLATFORM = 'linux'

    CB_DIR = '/opt/couchbase'

    PROCESSES = ('beam.smp', 'memcached', 'epmd', 'cbq-engine', 'indexer',
                 'cbft', 'goport', 'goxdcr', 'couch_view_index_updater',
                 'moxi', 'spring', 'sync_gateway')

    PROCESS_PATTERNS = ('cbas', )

    LINUX_PERF_PROFILES_PATH = '/opt/couchbase/var/lib/couchbase/logs/'

    LINUX_PERF_DELAY = 30

    def __init__(self, cluster_spec: ClusterSpec):
        super().__init__(cluster_spec)
        if not cluster_spec.capella_infrastructure:
            self.distro, self.distro_version = self.detect_distro()

    @property
    def package(self):
        if self.distro.upper() in ('UBUNTU', 'DEBIAN'):
            return 'deb'
        else:
            return 'rpm'

    @master_server
    def detect_distro(self) -> Tuple[str, str]:
        logger.info('Detecting Linux distribution on master node')
        cmd = 'grep ^{}= /etc/os-release | cut -d= -f2 | tr -d \'"\''
        distro_id = run(cmd.format('ID'))
        distro_version = run(cmd.format('VERSION_ID'))
        logger.info('Detected Linux distribution: {} {}'.format(distro_id, distro_version))
        return distro_id, distro_version

    def run_cbindex_command(self, options, worker_home='/tmp/perfrunner'):
        cmd = "/opt/couchbase/bin/cbindex {options}".format(options=options)
        logger.info('Running: {}'.format(cmd))
        run(cmd, shell_escape=False, pty=False)

    def run_cbindex_command_cloud(self, options, worker_home='/tmp/perfrunner'):
        with cd(worker_home), cd('perfrunner'):
            cmd = "ulimit -n 20000 && opt/couchbase/bin/cbindex {options}".format(options=options)
            logger.info('Running: {}'.format(cmd))
            run(cmd, shell_escape=False, pty=False)

    def build_index(self, index_node, indexes, is_ssl, auth):
        port = 8091
        if is_ssl:
            port = 18091
        options = \
            "-auth='{}:{}' " \
            "-server {index_node}:{port} " \
            "-type build " \
            "-indexes {all_indexes}".format(auth[0], auth[1], port=port, index_node=index_node,
                                            all_indexes=",".join(indexes))
        if is_ssl:
            put("root.pem", "/root/root.pem")
            options = options + " -use_tools=true -cacert ./root.pem"

        output = self.run_cbindex_command(options)
        logger.info("Output of command {}".format(output))

    def create_index(self, index_nodes, bucket, indexes, storage, is_ssl, auth):
        # Remember what bucket:index was created
        bucket_indexes = []

        for index, index_def in indexes.items():
            where = None
            if ':' in index_def:
                fields, where = index_def.split(":")
            else:
                fields = index_def
            fields_list = fields.split(",")
            port = 8091
            if is_ssl:
                port = 18091
            options = \
                "-auth='{}:{}' " \
                "-server {index_node}:{port} " \
                "-type create " \
                "-bucket {bucket} ".format(auth[0], auth[1], port=port, index_node=index_nodes[0],
                                           bucket=bucket)
            options += "-fields "
            for field in fields_list:
                options += "\\\\\\`{}\\\\\\`,".format(field)

            options = options.rstrip(",")
            options = options + " "

            if where is not None:
                options = '{options} -where \\\"{where_clause}\\\"'\
                    .format(options=options, where_clause=where)

            if storage == 'memdb' or storage == 'plasma':
                options = '{options} -using {db}'.format(options=options,
                                                         db=storage)

            options = "{options} -index {index} " \
                .format(options=options, index=index)

            options = options + '-with {\\\\\\"defer_build\\\\\\":true}'

            bucket_indexes.append("{}:{}".format(bucket, index))
            if is_ssl:
                put("root.pem", "/root/root.pem")
                options = options + " -use_tools=true -cacert ./root.pem"
            self.run_cbindex_command(options)

        return bucket_indexes

    def batch_create_index_collection(self, index_nodes, options, is_ssl, auth, is_cloud):
        port = 8091
        if is_ssl:
            port = 18091
        batch_options = \
            "-auth='{}:{}' " \
            "-server {index_node}:{port} " \
            "-type batch_process " \
            "-input /tmp/batch.txt ".format(auth[0], auth[1], port=port, index_node=index_nodes[0])
        with open("/tmp/batch.txt", "w+") as bf:
            bf.write(options)
        put("/tmp/batch.txt", "/tmp/batch.txt")
        if is_ssl and not is_cloud:
            put("root.pem", "/root/root.pem")
            batch_options = batch_options + " -use_tls -cacert ./root.pem"
            self.run_cbindex_command(batch_options)
        elif is_cloud:
            put("root.pem", "/root/root.pem")
            batch_options = batch_options + " -use_tools=true -cacert ./root.pem"
            self.run_cbindex_command_cloud(batch_options)
        else:
            self.run_cbindex_command(batch_options)

    def batch_build_index_collection(self, index_nodes, options, is_ssl, auth, is_cloud):
        port = 8091
        if is_ssl:
            port = 18091
        batch_options = \
            "-auth='{}:{}' " \
            "-server {index_node}:{port} " \
            "-type batch_build " \
            "-input /tmp/batch.txt ".format(auth[0], auth[1], port=port, index_node=index_nodes[0])
        with open("/tmp/batch.txt", "w+") as bf:
            bf.write(options)
        put("/tmp/batch.txt", "/tmp/batch.txt")
        if is_ssl and not is_cloud:
            put("root.pem", "/root/root.pem")
            batch_options = batch_options + " -use_tls -cacert ./root.pem"
        if not is_cloud:
            output = self.run_cbindex_command(batch_options)
        else:
            put("root.pem", "/root/root.pem")
            batch_options = batch_options + " -use_tools=true -cacert ./root.pem"
            output = self.run_cbindex_command_cloud(batch_options)
        logger.info("Output of command {}".format(output))

    @master_server
    def build_secondary_index(self, index_nodes, bucket, indexes, storage, is_ssl, auth):
        logger.info('building secondary indexes')

        # Create index but do not build
        bucket_indexes = self.create_index(index_nodes, bucket, indexes, storage, is_ssl, auth)

        # build indexes
        self.build_index(index_nodes[0], bucket_indexes, is_ssl, auth)

    @master_server
    def create_secondary_index_collections(self, index_nodes, options, is_ssl, auth):
        logger.info('creating secondary indexes')
        self.batch_create_index_collection(index_nodes, options, is_ssl, auth, is_cloud=False)

    @master_client
    def create_secondary_index_collections_cloud(self, index_nodes, options, is_ssl, auth):
        logger.info('creating secondary indexes')
        self.batch_create_index_collection(index_nodes, options, is_ssl, auth, is_cloud=True)

    @master_server
    def build_secondary_index_collections(self, index_nodes, options, is_ssl, auth):
        logger.info('building secondary indexes')
        self.batch_build_index_collection(index_nodes, options, is_ssl, auth, is_cloud=False)

    @master_client
    def build_secondary_index_collections_cloud(self, index_nodes, options, is_ssl, auth):
        logger.info('building secondary indexes')
        self.batch_build_index_collection(index_nodes, options, is_ssl, auth, is_cloud=True)

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

    @all_dn_nodes
    def collect_dn_logs(self):
        logger.info('Collecting Direct Nebula logs')

        r = run('curl http://169.254.169.254/latest/meta-data/instance-id')
        fname = 'dn_{}.log'.format(r.stdout)
        run('journalctl -u direct-nebula > {}'.format(fname))
        get('{}'.format(fname))
        run('rm -f {}'.format(fname))

    @all_dapi_nodes
    def collect_dapi_logs(self):
        logger.info('Collecting Data API logs')

        r = run('curl http://169.254.169.254/latest/meta-data/instance-id')
        fname = 'dapi_{}.log'.format(r.stdout)
        run('journalctl -u couchbase-data-api > {}'.format(fname))
        get('{}'.format(fname))
        run('rm -f {}'.format(fname))

    @all_servers
    def collect_index_datafiles(self):
        logger.info('Archiving Index Data Files')

        fname = '/data/@2i'
        cmd_zip = 'zip -rq @2i.zip /data/@2i'
        r = run('stat {}'.format(fname), quiet=True, warn_only=True)
        if not r.return_code:
            run(cmd_zip, warn_only=True, pty=False)
            get('{}'.format('@2i.zip'), local_path='.')
            run('rm -f {}'.format('@2i.zip'))

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

        self._killall(self.PROCESSES)

    def shutdown(self, host):
        with settings(host_string=host):
            self._killall(self.PROCESSES)

    def kill_memcached(self, host: str):
        # Kill memcached with SIGSTOP to ensure it doesnt get restarted before the failover
        with settings(host_string=host):
            self._killall(('memcached',), signal='SIGSTOP')

    def _killall(self, processes, signal: str = 'SIGKILL'):
        logger.info('Killing {}'.format(', '.join(processes)))
        run('killall -{} {}'.format(signal, ' '.join(processes)), warn_only=True, quiet=True)

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
            run('yes | apt install -y /tmp/{}'.format(filename))
        else:
            run('yes | yum localinstall -y /tmp/{}'.format(filename))

    @all_servers
    def install_uploaded_couchbase(self, filename: str):
        logger.info('Installing Couchbase Server')
        if self.package == 'deb':
            run('yes | apt install -y /tmp/{}'.format(filename))
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

    @all_servers
    def reset_num_vbuckets(self):
        logger.info('Resetting number of vbuckets')
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
        target_dir = '/data' if not self.cluster_spec.capella_infrastructure else '/var/cb/data'
        r = run('ls {}/core*'.format(target_dir), quiet=True)
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
    def purge_restore_progress(self, archive: str, repo: str):
        repo_dir = '{}/{}'.format(archive.rstrip('/'), repo)
        logger.info('Purging restore progress from {}'.format(repo_dir))
        cmd = 'rm -rf {}/.restore'.format(repo_dir)
        logger.info('Running: {}'.format(cmd))
        run(cmd)

    @master_server
    def restore_data(self, archive_path: str, repo_path: str, map_data: str = None):
        cmd = "/opt/couchbase/bin/cbbackupmgr restore " \
              "--archive {} --repo {} --threads 24 " \
              "--cluster http://localhost:8091 " \
              "--username Administrator --password password " \
              "--disable-ft-indexes --disable-gsi-indexes".format(archive_path, repo_path)

        if map_data:
            cmd += " --map-data {}".format(map_data)

        logger.info("Running: {}".format(cmd))
        run(cmd)

    @master_server
    def export_data(self, num_collections, collection_prefix, scope_prefix, scope, name_of_backup):
        logger.info("Loading data into the collections")
        scope_name = scope_prefix + str(scope)
        name_of_backup = "/fts/backup/exportFiles/" + name_of_backup + ".json"
        cmd = "python3 /fts/backup/exportFiles/splitData.py --num_col {} " \
              "--collection_prefix {} " \
              "--scope_name {} " \
              "--data_file {}".format(num_collections, collection_prefix,
                                      scope_name, name_of_backup)
        logger.info("the command {}".format(cmd))
        run(cmd)

    @master_server
    def load_tpcds_data_json(self, import_file: str, bucket: str):
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

    @master_server
    def load_tpcds_data_json_collection(self,
                                        import_file: str,
                                        bucket: str,
                                        scope: str,
                                        collection: str,
                                        docs_per_collection: int = 0):
        cmd = \
            "/opt/couchbase/bin/cbimport json " \
            "--dataset file://{} --threads 24 " \
            "--cluster http://localhost:8091 " \
            "--bucket {} " \
            "--scope-collection-exp {}.{} " \
            "--username Administrator --password password " \
            "--generate-key '#MONO_INCR#' --format lines".format(
                import_file,
                bucket,
                scope,
                collection
            )

        if docs_per_collection > 0:
            cmd += " --limit-docs {}".format(docs_per_collection)

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

    @syncgateway_servers
    def disable_cpu_sgw(self, sgw_online_cores: int):
        logger.info('Throttling CPU resources')

        cpu_map = self.get_cpu_map()
        offline_cores = sorted(cpu_map)[sgw_online_cores:]

        for core in offline_cores:
            for cpu in cpu_map[core]:
                run('echo 0 > /sys/devices/system/cpu/cpu{}/online'.format(cpu))

    @all_servers
    @syncgateway_servers
    def enable_cpu(self):
        logger.info('Enabling all CPU cores')
        for i in range(1, self.num_vcpu):
            run('echo 1 > /sys/devices/system/cpu/cpu{}/online'.format(i))

    @servers_by_role(roles=['index'])
    def kill_process_on_index_node(self, process):
        logger.info('Killing following process on index node: {}'.format(process))
        run("killall {}".format(process), warn_only=True)

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
        run('update-grub')  # This is for BIOS mode
        run('grub-mkconfig -o /boot/efi/EFI/ubuntu/grub.cfg', warn_only=True)
        # This line cover the machine running in EFI mode

    def reboot(self):
        logger.info('Rebooting the node')
        run('reboot', quiet=True, pty=False)

    def tune_memory_settings(self, host_string: str, size: str):
        logger.info('Changing kernel memory to {} on {}'.format(size, host_string))
        with settings(host_string=host_string):
            run("sed -ir 's/mem=[0-9]*[kmgKMG]//g' /etc/default/grub")
            run("sed -i 's/GRUB_CMDLINE_LINUX=\"\"/GRUB_CMDLINE_LINUX=\"mem={}\"/'\
                 /etc/default/grub".format(size))
            self.grub_config()
            self.reboot()

    def reset_memory_settings(self, host_string: str):
        logger.info('Resetting kernel memory settings')
        with settings(host_string=host_string):
            run("sed -ir 's/mem=[0-9]*[kmgKMG]//' /etc/default/grub")
            self.grub_config()
            self.reboot()

    def is_up(self, host_string: str) -> bool:
        with settings(host_string=host_string):
            try:
                logger.info('checking {}'.format(host_string))
                result = run(":", timeout=30)
                logger.info('done {}'.format(host_string))
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
    def update_ip_family_cli(self):
        logger.info('Updating IP family')
        cmd = \
            "/opt/couchbase/bin/couchbase-cli ip-family "\
            "-c http://localhost:8091 -u Administrator "\
            "-p password --set --ipv6"
        logger.info("Running: {}".format(cmd))
        run(cmd)

    @all_servers
    def update_ip_family_rest(self):
        logger.info('Updating IP family')
        cmd = \
            "curl -u Administrator:password -d 'afamily=ipv6' " \
            "http://localhost:8091/node/controller/setupNetConfig"
        logger.info("Running: {}".format(cmd))
        run(cmd)

    @all_servers
    def setup_x509(self):
        logger.info('Setting up x.509 certificates')
        put("certificates/inbox", "/opt/couchbase/var/lib/couchbase/")
        run('chmod a+x /opt/couchbase/var/lib/couchbase/inbox/chain.pem')
        run('chmod a+x /opt/couchbase/var/lib/couchbase/inbox/pkey.key')

    @master_server
    def allow_non_local_ca_upload(self):
        logger.info('Enabling non-local CA upload')
        command = ("curl -X POST -v -u Administrator:password http://localhost:8091" +
                   "/settings/security/allowNonLocalCACertUpload " +
                   "-H 'Content-Type:application/x-www-form-urlencoded" +
                   "' -d 'true'")
        run(command)

    @all_clients
    def generate_ssl_keystore(self, root_certificate, keystore_file, storepass, worker_home):
        logger.info('Generating SSL keystore')
        remote_keystore = "{}/perfrunner/{}".format(worker_home, keystore_file)
        remote_root_cert = "{}/perfrunner/{}".format(worker_home, root_certificate)

        with quiet():
            run("keytool -delete -keystore {} -alias couchbase -storepass storepass"
                .format(remote_keystore))
        run("keytool -importcert -file {} -storepass {} -trustcacerts "
            "-noprompt -keystore {} -alias couchbase"
            .format(remote_root_cert, storepass, remote_keystore))

    @all_clients
    def cloud_put_certificate(self, cert, worker_home):
        put(cert, worker_home + '/perfrunner')

    @all_clients
    def cloud_put_scanfile(self, file, file_path):
        put(file, file_path + '/perfrunner/' + file)

    @master_client
    def run_cbindexperf(self, path_to_tool: str, node: str,
                        rest_username: str,
                        rest_password: str, configfile: str,
                        worker_home: str,
                        run_in_background: bool = False,
                        collect_profile: bool = True,
                        is_ssl: bool = False):
        with cd(worker_home), cd('perfrunner'):
            logger.info('Initiating scan workload')
            cmdstr = "export CBAUTH_REVRPC_URL=http://Administrator:password@{}" \
                     ":8091/query;" \
                     " {} -cluster {}:8091 -auth=\"{}:{}\" -configfile {} " \
                     "-resultfile result.json " \
                     "-statsfile /root/statsfile" \
                .format(node, path_to_tool, node, rest_username, rest_password, configfile)
            if collect_profile:
                cmdstr += " -cpuprofile cpuprofile.prof -memprofile memprofile.prof "
            if is_ssl:
                cmdstr += " -use_tls -cacert ./root.pem"
            if run_in_background:
                cmdstr += " &"
            logger.info('To be applied: {}'.format(cmdstr))
            status = run(cmdstr)
            return status

    @master_client
    def kill_client_process(self, process: str):
        logger.info('Killing the following process: {}'.format(process))
        with quiet():
            run("killall -9 {}".format(process))

    @master_client
    def run_cbindexperf_cloud(self, path_to_tool: str, node: str,
                              rest_username: str,
                              rest_password: str, configfile: str,
                              worker_home: str,
                              run_in_background: bool = False,
                              collect_profile: bool = True,
                              is_ssl: bool = False):
        with cd(worker_home), cd('perfrunner'):
            logger.info('Initiating scan workload')
            port = 8091
            if is_ssl:
                port = 18091
            ulimitcmdstr = "ulimit -n 20000 && "
            cmdstr = "{} -cluster {}:{} -auth='{}:{}' -configfile {} " \
                     "-resultfile result.json " \
                     "-statsfile /root/statsfile" \
                .format(path_to_tool, node, port, rest_username, rest_password, configfile)
            if collect_profile:
                cmdstr += " -cpuprofile cpuprofile.prof -memprofile memprofile.prof "
            if is_ssl:
                cmdstr += " -use_tools=true -cacert ./root.pem"
            if run_in_background:
                cmdstr = ulimitcmdstr + "nohup " + \
                        cmdstr + " > /tmp/cbindexperf.log < /tmp/cbindexperf.log & sleep 5"
            else:
                cmdstr = ulimitcmdstr + cmdstr
            logger.info('To be applied: {}'.format(cmdstr))
            status = run(cmdstr)
            return status.return_code

    @master_client
    def get_indexer_heap_profile(self, indexer: str):
        cmd = 'export GOPATH=$HOME/go; ' \
              'export GOROOT=/usr/local/go; ' \
              'export PATH=$PATH:$GOROOT/bin; ' \
              'go tool pprof --text http://Administrator:password@{}:9102' \
              '/debug/pprof/heap'.format(indexer)
        logger.info('Running: {}'.format(cmd))
        result = run(cmd)
        return result

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
    def ui_http_off(self, host: str):
        logger.info('Disabling UI over http.')
        run("/opt/couchbase/bin/couchbase-cli setting-security --set --disable-http-ui 1 --cluster "
            "{}:8091 -u Administrator -p password".format(host))

    @all_servers
    def enable_serverless_mode(self):
        # Enable serverless profile.
        run('systemctl set-environment CB_FORCE_PROFILE=serverless')

    @all_servers
    def disable_serverless_mode(self):
        # Disabling serverless mode.
        run('systemctl unset-environment CB_FORCE_PROFILE')

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

    @master_client
    def calc_backup_size(self, cluster_spec: ClusterSpec,
                         rounded: bool = True) -> float:
        backup_size = run('du -sb0 {}'.format(cluster_spec.backup))
        backup_size = backup_size.split()[0]
        backup_size = float(backup_size) / 2 ** 30  # B -> GB

        return round(backup_size, 2) if rounded else backup_size

    @master_client
    def backup(self, master_node: str, cluster_spec: ClusterSpec, threads: int, worker_home: str,
               compression: bool = False, storage_type: Optional[str] = None,
               sink_type: Optional[str] = None, shards: Optional[int] = None,
               obj_staging_dir: Optional[str] = None, obj_region: Optional[str] = None,
               obj_access_key_id: Optional[str] = None, use_tls: bool = False,
               encrypted: bool = False, passphrase: str = 'couchbase'):
        logger.info('Creating a new backup: {}'.format(cluster_spec.backup))

        self.cbbackupmgr_config(cluster_spec, worker_home, obj_staging_dir, obj_region,
                                obj_access_key_id, encrypted, passphrase)

        self.cbbackupmgr_backup(master_node, cluster_spec, threads, compression, storage_type,
                                sink_type, shards, worker_home, obj_staging_dir, obj_region,
                                obj_access_key_id, use_tls, encrypted, passphrase)

    @master_client
    def cleanup(self, backup_dir: str):
        logger.info("Cleaning the disk before backup/export")
        run('rm -fr {}/*'.format(backup_dir))
        run('mkdir -p {}'.format(backup_dir))

    @master_client
    def create_aws_credential(self, credential):
        logger.info("Creating AWS credential")
        with cd('~/'):
            run('mkdir -p .aws')
            with cd('.aws'):
                cmd = 'echo "{}" > credentials'.format(credential)
                run(cmd)

    @all_servers
    def create_aws_config_gsi(self, credential):
        logger.info("Creating AWS config")
        with cd('/home/couchbase/'):
            run('mkdir -p .aws')
            with cd('.aws'):
                cmd = 'echo "{}" > credentials'.format(credential)
                run(cmd)
                cmd = 'echo -e "[default] \nregion=us-east-1 \noutput=json" > config'
                run(cmd)

    @master_client
    def cbbackupmgr_version(self, worker_home: str):
        with cd(worker_home), cd('perfrunner'):
            cmd = './opt/couchbase/bin/cbbackupmgr --version'
            logger.info('Running: {}'.format(cmd))
            result = run(cmd)
            logger.info(result)

    @master_client
    def cbbackupmgr_config(self, cluster_spec: ClusterSpec, worker_home: str,
                           obj_staging_dir: Optional[str] = None, obj_region: Optional[str] = None,
                           obj_access_key_id: Optional[str] = None, encrypted: bool = False,
                           passphrase: str = 'couchbase'):
        with cd(worker_home), cd('perfrunner'):
            flags = [
                '--archive {}'.format(cluster_spec.backup),
                '--repo default',
                '--obj-region {}'.format(obj_region) if obj_region else None,
                '--obj-staging-dir {}'.format(obj_staging_dir) if obj_staging_dir else None,
                '--obj-access-key-id {}'.format(obj_access_key_id) if obj_access_key_id else None,
                '--encrypted --passphrase {}'.format(passphrase) if encrypted else None
            ]

            cmd = './opt/couchbase/bin/cbbackupmgr config {}'.format(
                ' '.join(filter(None, flags)))

            logger.info('Running: {}'.format(cmd))
            run(cmd)

    @master_client
    def cbbackupmgr_backup(self, master_node: str, cluster_spec: ClusterSpec, threads: int,
                           compression: bool, storage_type: str, sink_type: str, shards: int,
                           worker_home: str, obj_staging_dir: Optional[str] = None,
                           obj_region: Optional[str] = None,
                           obj_access_key_id: Optional[str] = None, use_tls: bool = False,
                           encrypted: bool = False, passphrase: str = 'couchbase'):
        with cd(worker_home), cd('perfrunner'):
            flags = [
                '--archive {}'.format(cluster_spec.backup),
                '--repo default',
                '--cluster http{}://{}'.format('s' if use_tls else '', master_node),
                '--cacert root.pem' if use_tls else None,
                '--username {}'.format(cluster_spec.rest_credentials[0]),
                '--password {}'.format(cluster_spec.rest_credentials[1]),
                '--threads {}'.format(threads) if threads else None,
                '--storage {}'.format(storage_type) if storage_type else None,
                '--sink {}'.format(sink_type) if sink_type else None,
                '--value-compression compressed' if compression else None,
                '--shards {}'.format(shards) if shards else None,
                '--obj-region {}'.format(obj_region) if obj_region else None,
                '--obj-staging-dir {}'.format(obj_staging_dir) if obj_staging_dir else None,
                '--obj-access-key-id {}'.format(obj_access_key_id) if obj_access_key_id else None,
                '--passphrase {}'.format(passphrase) if encrypted else None,
                '--no-progress-bar'
            ]

            cmd = './opt/couchbase/bin/cbbackupmgr backup {}'.format(
                ' '.join(filter(None, flags)))

            logger.info('Running: {}'.format(cmd))
            run(cmd)

    @master_client
    def client_drop_caches(self):
        logger.info('Dropping memory cache')
        run('sync && echo 3 > /proc/sys/vm/drop_caches')

    @master_client
    def restore(self, master_node: str, cluster_spec: ClusterSpec, threads: int, worker_home: str,
                archive: str = '', repo: str = 'default', obj_staging_dir: Optional[str] = None,
                obj_region: Optional[str] = None, obj_access_key_id: Optional[str] = None,
                use_tls: bool = False, map_data: str = None, encrypted: bool = False,
                passphrase: str = 'couchbase'):
        logger.info('Restore from {}'.format(archive or cluster_spec.backup))

        self.cbbackupmgr_restore(master_node, cluster_spec, threads, worker_home,
                                 archive, repo, obj_staging_dir, obj_region, obj_access_key_id,
                                 use_tls, map_data, encrypted, passphrase)

    @master_client
    def cbbackupmgr_restore(self, master_node: str, cluster_spec: ClusterSpec, threads: int,
                            worker_home: str, archive: str = '', repo: str = 'default',
                            obj_staging_dir: Optional[str] = None, obj_region: Optional[str] = None,
                            obj_access_key_id: Optional[str] = None, use_tls: bool = False,
                            map_data: Optional[str] = None, encrypted: bool = False,
                            passphrase: str = 'couchbase'):
        restore_to_capella = cluster_spec.capella_infrastructure

        with cd(worker_home), cd('perfrunner'):
            flags = [
                '--archive {}'.format(archive or cluster_spec.backup),
                '--repo {}'.format(repo),
                '--cluster http{}://{}'.format('s' if use_tls else '', master_node),
                '--cacert root.pem' if use_tls else None,
                '--username {}'.format(cluster_spec.rest_credentials[0]),
                '--password {}'.format(cluster_spec.rest_credentials[1]),
                '--threads {}'.format(threads) if threads else None,
                '--obj-region {}'.format(obj_region) if obj_region else None,
                '--obj-staging-dir {}'.format(obj_staging_dir) if obj_staging_dir else None,
                '--obj-access-key-id {}'.format(obj_access_key_id) if obj_access_key_id else None,
                '--map-data {}'.format(map_data) if map_data else None,
                '--disable-analytics --disable-cluster-analytics' if restore_to_capella else None,
                '--passphrase {}'.format(passphrase) if encrypted else None,
                '--no-progress-bar'
            ]

            cmd = './opt/couchbase/bin/cbbackupmgr restore --force-updates {}'.format(
                ' '.join(filter(None, flags)))

            logger.info('Running: {}'.format(cmd))
            run(cmd)

    @all_servers
    def install_cb_debug_package(self, url):
        logger.info('Installing Couchbase Debug package on all servers')
        if url.endswith('deb'):
            cmd = 'dpkg -i {}'.format(url)
        else:
            cmd = 'rpm -iv {}'.format(url)

        run(cmd, quiet=True)

    @all_servers
    def generate_linux_perf_script(self):

        files_list = 'for i in {}*_perf.data; do echo $i; ' \
                     'done'.format(self.LINUX_PERF_PROFILES_PATH)
        files = run(files_list).replace("\r", "").split("\n")

        for filename in files:
            fname = filename.split('/')[-1]
            if fname != '*_perf.data':
                cmd_perf_script = 'perf script -i {}' \
                    ' --no-inline > {}.txt'.format(filename, filename)

                logger.info('Generating linux script data : {}'.format(cmd_perf_script))
                try:

                    with settings(warn_only=True):
                        run(cmd_perf_script, timeout=600, pty=False)
                        time.sleep(self.LINUX_PERF_DELAY)

                except CommandTimeout:
                    logger.error('linux perf script timed out')

                cmd_zip = 'cd {}; zip -q {}.zip {}.txt'.format(self.LINUX_PERF_PROFILES_PATH,
                                                               fname,
                                                               fname)

                with settings(warn_only=True):
                    run(cmd_zip, pty=False)

    @all_servers
    def get_linuxperf_files(self):

        logger.info('Collecting linux perf files from kv nodes')
        with cd(self.LINUX_PERF_PROFILES_PATH):
            r = run('stat *.zip', quiet=True, warn_only=True)
            if not r.return_code:
                get('*.zip', local_path='.')

    @all_servers
    def txn_query_cleanup(self, timeout):
        logger.info('Running txn cleanup window')

        cmd = \
            "curl -s -u Administrator:password http://localhost:8091/" \
            "settings/querySettings  -d 'queryCleanupWindow={}s'".format(timeout)

        logger.info("Running: {}".format(cmd))
        run(cmd, pty=False)

        cmd2 = "curl -u Administrator:password http://localhost:8093/admin/settings" \
               " -XPOST -d '{\"atrcollection\":\"default._default._default\"}'"

        logger.info("Running: {}".format(cmd2))

        with settings(warn_only=True):
            run(cmd2, pty=False)

    @master_server
    def enable_developer_preview(self):
        logger.info('Enabling developer preview')
        run("curl -X POST -u Administrator:password "
            "localhost:8091/settings/developerPreview -d 'enabled=true'", pty=False)

    @master_server
    def set_magma_quota(self, bucket: str, storage_quota_percentage: int):
        logger.info('Set Magma quota percentage to {}'.format(storage_quota_percentage))
        run("curl -s -u Administrator:password "
            "localhost:8091/pools/default/buckets/{} -d 'storageQuotaPercentage={}'"
            .format(bucket, storage_quota_percentage), pty=False)

    @master_server
    def set_magma_min_memory_quota(self, magma_min_memory_quota: int):
        logger.info('Set Magma min memory quota to {}'.format(magma_min_memory_quota))
        run("curl -s -u Administrator:password "
            "localhost:8091/internalSettings -d 'magmaMinMemoryQuota={}'"
            .format(magma_min_memory_quota), pty=False)

    @all_servers
    def create_data_backup(self, backup_directory: str):
        logger.info('Creating backup file: {}'.format(backup_directory))
        cmd = "tar cf {}/backup.tar.bz2 /data/ --use-compress-program=lbzip2"\
            .format(backup_directory)
        logger.info('cmd: {}'.format(cmd))
        run(cmd, pty=False)
        logger.info('backup create done')

    @all_servers
    def copy_backup(self, backup_directory: str):
        logger.info('Copying data back to /data')
        cmd = "tar xf {}/backup.tar.bz2 --use-compress-program=lbzip2 -C /"\
            .format(backup_directory)
        logger.info('cmd: {}'.format(cmd))
        run(cmd, pty=False)
        logger.info('backup copy done')

    @all_servers
    def remove_data(self):
        logger.info('Clean up /data/')
        run('rm -rf /data/*')

    @master_client
    def ch2_run_task(self, cluster_spec: ClusterSpec, worker_home: str, warehouses: int,
                     aclients: int = 0, tclients: int = 0, duration: int = 0,
                     iterations: int = 0, warmup_iterations: int = 0, warmup_duration: int = 0,
                     query_url: str = None, multi_query_url: str = None,
                     analytics_url: str = None, log_file: str = 'ch2_mixed'):

        flags = ['--warehouses {}'.format(warehouses),
                 '--aclients {}'.format(aclients) if aclients else None,
                 '--tclients {}'.format(tclients) if tclients else None,
                 '--duration {}'.format(duration) if duration else None,
                 '--warmup-duration {}'.format(warmup_duration) if warmup_duration else None,
                 '--query-iterations {}'.format(iterations) if iterations else None,
                 '--warmup-query-iterations {}'.format(warmup_iterations)
                 if warmup_iterations else None,
                 'nestcollections',
                 '--query-url {}'.format(query_url) if tclients else None,
                 '--multi-query-url {}'.format(multi_query_url) if tclients else None,
                 '--analytics-url {}'.format(analytics_url) if aclients else None,
                 '--userid {}'.format(cluster_spec.rest_credentials[0]),
                 '--password {}'.format(cluster_spec.rest_credentials[1]),
                 '--no-load > ../../../{}.log'.format(log_file)]

        cmd = '../../../env/bin/python3 ./tpcc.py {}'.format(
            ' '.join(filter(None, flags)))

        with cd(worker_home), cd('perfrunner'), cd('ch2/ch2driver/pytpcc/'):
            logger.info('Running: {}'.format(cmd))
            run(cmd)

    @master_client
    def ch2_load_task(self, cluster_spec: ClusterSpec, worker_home: str, warehouses: int,
                      load_tclients: int, data_url: str, multi_data_url: str, query_url: str,
                      multi_query_url: str, load_mode: str):
        if load_mode == "qrysvc-load":
            qrysvc_mode = True
        else:
            qrysvc_mode = False

        flags = ['--warehouses {}'.format(warehouses),
                 '--tclients {}'.format(load_tclients),
                 'nestcollections',
                 '--data-url {}'.format(data_url) if not qrysvc_mode else None,
                 '--multi-data-url {}'.format(multi_data_url) if not qrysvc_mode else None,
                 '--query-url {}'.format(query_url) if qrysvc_mode else None,
                 '--multi-query-url {}'.format(multi_query_url) if qrysvc_mode else None,
                 '--userid {}'.format(cluster_spec.rest_credentials[0]),
                 '--password {}'.format(cluster_spec.rest_credentials[1]),
                 '--no-execute',
                 '--{} > ../../../ch2_load.log'.format(load_mode) if not qrysvc_mode else None,
                 '--{} > /dev/null 2>&1'.format(load_mode) if qrysvc_mode else None]

        cmd = '../../../env/bin/python3 ./tpcc.py {}'.format(
            ' '.join(filter(None, flags)))

        with cd(worker_home), cd('perfrunner'), cd('ch2/ch2driver/pytpcc/'):
            logger.info('Running: {}'.format(cmd))
            run(cmd)

    @master_server
    def compress_sg_logs(self):
        logger.info('Compressing Syncgateway log folders')
        cmd = 'tar cvzf /home/sync_gateway/syncgateway_logs.tar.gz /home/sync_gateway/logs'
        run(cmd, warn_only=True)

    @syncgateway_servers
    def compress_sg_logs_new(self):
        logger.info('Compressing Syncgateway log folders')
        run('rm -rf /tmp/sglogs_temp', warn_only=True)
        run('mkdir /tmp/sglogs_temp', warn_only=True)
        r = run('cp -R /var/tmp/sglogs/* /tmp/sglogs_temp', warn_only=True)
        logger.info(str(r))
        run('tar cvzf /var/tmp/sglogs/syncgateway_logs.tar.gz /tmp/sglogs_temp', warn_only=True)
        run('rm -rf /tmp/sglogs_temp', warn_only=True)

    @syncgateway_servers
    def remove_sglogs(self):
        if not self.cluster_spec.capella_infrastructure:
            logger.info('removing old sglogs')
            cmd = 'rm -rf /var/tmp/sglogs/*'
            run(cmd)

    @all_clients
    def download_blockholepuller(self):
        cmd = 'curl -o blackholePuller-linux-x64 https://github.com/couchbaselabs/sg_dev_tools/'
        run(cmd)
        cmd = 'chmod +x blackholePuller-linux-x64'
        run(cmd)

    def nebula_init_ssh(self):
        if not (self.cluster_spec.serverless_infrastructure and
                self.cluster_spec.capella_backend == 'aws'):
            return

        iids = self.cluster_spec.direct_nebula_instance_ids + self.cluster_spec.dapi_instance_ids

        logger.info('Configuring SSH for Nebula nodes.')
        logger.info('Uploading SSH key to {}'.format(', '.join(iids)))

        upload_keys_command = (
            'env/bin/aws ssm send-command '
            '--region $AWS_REGION '
            '--instance-ids "{}" '
            '--document-name \'AWS-RunShellScript\' '
            '--parameters commands="\\"\n'
            'mkdir -p ~root/.ssh\n'
            'cd ~root/.ssh || exit 1\n'
            'grep -q \'$(cat ${{HOME}}/.ssh/id_ed25519_capella.pub)\' authorized_keys '
            '|| echo \'$(cat ${{HOME}}/.ssh/id_ed25519_capella.pub) ssm-session\' >> '
            'authorized_keys\n'
            '\\""'
        ).format('" "'.join(iids))

        _, _, returncode = run_local_shell_command(
            command=upload_keys_command,
            success_msg='Successfully uploaded SSH key to Nebula nodes.',
            err_msg='Failed to upload SSH key to Nebula nodes.'
        )

        if returncode == 0:
            for iid in iids:
                logger.info('Testing SSH to {}'.format(iid))
                test_ssh_command = 'ssh root@{} echo'.format(iid)

                _, _, returncode = run_local_shell_command(
                    command=test_ssh_command,
                    success_msg='SSH connection established with {}'.format(iid),
                    err_msg='Failed to establish SSH connection with {}'.format(iid)
                )

    @all_dapi_nodes
    def set_dapi_log_level(self, level: str = 'debug'):
        logger.info('Setting log level on Data API nodes: {}'.format(level))
        run('curl -ks -X PUT http://localhost:8942/log?level={}'.format(level), warn_only=True)

    @all_dn_nodes
    def set_dn_log_level(self, level: str = 'debug'):
        logger.info('Setting log level on Direct nebula nodes: {}'.format(level))
        run('curl -ks -X PUT http://localhost:8941/log?level={}'.format(level), warn_only=True)

    @all_servers
    def add_system_limit_config(self):
        logger.info("Add system_limits.conf for cgroup")
        run("mkdir -p /etc/systemd/system/couchbase-server.service.d")
        with cd('/etc/systemd/system/couchbase-server.service.d'):
            meminfo = run('cat /proc/meminfo')
            meminfo = dict((i.split()[0].rstrip(':'),
                            int(i.split()[1])) for i in meminfo.splitlines())
            mem_kib = meminfo['MemTotal']
            with open("system_limits.conf", 'w') as f:
                l1 = "[Service]\n"
                l2 = "MemoryLimit={}".format(mem_kib*1024)
                f.writelines([l1, l2])
            put('system_limits.conf', 'system_limits.conf')
        run('systemctl daemon-reload')

    @all_servers
    def clear_system_limit_config(self):
        logger.info("Clear system_limits.conf")
        run("rm -rf /etc/systemd/system/couchbase-server.service.d")
        run('systemctl daemon-reload')

    @master_server
    def configure_analytics_s3_bucket(self, region: str = 'us-east-1',
                                      s3_bucket: str = 'cb-perf-goldfish'):
        logger.info('Configuring Analytics S3 bucket. Bucket name: {}, region: {}'
                    .format(s3_bucket, region))
        command = ("curl --request POST --url http://localhost:8091/settings/analytics " +
                   "--header 'Content-Type: application/x-www-form-urlencoded' " +
                   "--data blobStorageRegion={} " +
                   "--data blobStoragePrefix= " +
                   "--data blobStorageBucket={} " +
                   "--data blobStorageScheme=s3").format(region, s3_bucket)
        run(command)

    @all_servers
    def add_aws_credential(self, access_key_id: str, secret_access_key: str):
        access_key_url = \
            "http://localhost:8091/_metakv/cbas/debug/settings/blob_storage_access_key_id"
        secret_access_key_url = \
            "http://localhost:8091/_metakv/cbas/debug/settings/blob_storage_secret_access_key"

        logger.info('Add AWS access key')
        command = ("curl -v -X PUT {} --data-urlencode value={}"
                   .format(access_key_url, access_key_id))

        retries = 4
        while retries > 0 and not (output := run(command, warn_only=True)).succeeded:
            logger.warn('Error while adding AWS access key: {}'.format(output))
            logger.info('Waiting 5 seconds and retrying ({} retries left)'.format(retries))
            retries -= 1
            time.sleep(5)

        if retries == 0:
            logger.interrupt('Failed to add AWS access key')
            return

        logger.info('Add AWS secret access key')
        command = ("curl -v -X PUT {} --data-urlencode value={}"
                   .format(secret_access_key_url, secret_access_key))
        run(command)

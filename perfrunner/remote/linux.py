import json
import os.path
import time
from random import uniform

from fabric import state
from fabric.api import get, put, run, settings
from fabric.exceptions import CommandTimeout
from logger import logger

from perfrunner.helpers.misc import uhex
from perfrunner.remote import Remote
from perfrunner.remote.context import (
    all_hosts,
    all_kv_nodes,
    index_node,
    kv_node_cbindexperf,
    single_client,
    single_host,
)


class CurrentHostMutex:

    def __init__(self):
        with open("/tmp/current_host.txt", 'w') as f:
            f.write("0")

    @staticmethod
    def next_host_index():
        with open("/tmp/current_host.txt", 'r') as f:
            next_host = f.readline()
        with open("/tmp/current_host.txt", 'w') as f:
            f.write((int(next_host) + 1).__str__())
        return int(next_host)


class RemoteLinux(Remote):

    ARCH = {'i386': 'x86', 'x86_64': 'x86_64'}

    CB_DIR = '/opt/couchbase'

    PROCESSES = ('beam.smp', 'memcached', 'epmd', 'cbq-engine', 'indexer',
                 'cbft', 'goport', 'goxdcr', 'couch_view_index_updater',
                 'moxi', 'spring')

    def __init__(self, cluster_spec, test_config, os):
        super(RemoteLinux, self).__init__(cluster_spec, test_config, os)

        self.current_host = CurrentHostMutex()

    @single_host
    def detect_pkg(self):
        logger.info('Detecting package manager')
        if self.os.upper() in ('UBUNTU', 'DEBIAN'):
            return 'deb'
        else:
            return 'rpm'

    @single_host
    def detect_arch(self):
        logger.info('Detecting platform architecture')
        arch = run('uname -i', pty=True)
        return self.ARCH[arch]

    @single_host
    def detect_os_release(self):
        """Possible values:
            CentOS release 6.x (Final)
            CentOS Linux release 7.2.1511 (Core)
        """
        return run('cat /etc/redhat-release').split()[-2][0]

    def run_cbindex_command(self, command):
        command_path = '/opt/couchbase/bin/cbindex'
        command = "{path} {cmd}".format(path=command_path, cmd=command)
        logger.info('Submitting cbindex command {}'.format(command))

        status = run(command, shell_escape=False, pty=False)
        if status:
            logger.info('Command status {}'.format(status))

    def build_index(self, index_node, bucket_indexes):
        all_indexes = ",".join(bucket_indexes)
        cmd_str = "-auth=Administrator:password -server {index_node} -type build -indexes {all_indexes}" \
            .format(index_node=index_node, all_indexes=all_indexes)

        self.run_cbindex_command(cmd_str)

    def create_index(self, index_nodes, bucket, indexes, storage):
        # Remember what bucket:index was created
        bucket_indexes = []

        for index, field in indexes.items():
            cmd = "-auth=Administrator:password  -server {index_node}  -type create -bucket {bucket}" \
                  "  -fields={field}".format(index_node=index_nodes[0], bucket=bucket, field=field)

            if storage == 'memdb':
                cmd = '{cmd} -using {db}'.format(cmd=cmd, db=storage)

            with_str = r'{\\\"defer_build\\\":true}'
            final_cmd = "{cmd} -index {index} -with=\\\"{with_str}\\\"" \
                .format(cmd=cmd, index=index, with_str=with_str)

            bucket_indexes.append("{}:{}".format(bucket, index))
            self.run_cbindex_command(final_cmd)

        return bucket_indexes

    @single_host
    def build_secondary_index(self, index_nodes, bucket, indexes, storage):
        logger.info('building secondary indexes')

        # Create index but do not build
        bucket_indexes = self.create_index(index_nodes, bucket, indexes, storage)

        # build indexes
        self.build_index(index_nodes[0], bucket_indexes)

    @all_kv_nodes
    def run_spring_on_kv(self, ls, silent=False):

        def calculate_existing_items():
            if ls.creates == 100:
                return operation * host + ls.existing_items
            return ls.existing_items

        throughput = int(ls.throughput) if ls.throughput != float('inf') \
            else ls.throughput
        items_in_working_set = int(ls.working_set)
        operations_to_hit_working_set = ls.working_set_access
        logger.info("running spring on kv nodes")
        number_of_kv_nodes = self.kv_hosts.__len__()
        existing_item = operation = 0
        sleep_time = uniform(1, number_of_kv_nodes)
        time.sleep(sleep_time)
        host = self.current_host.next_host_index()
        logger.info("current_host_index {}".format(host))
        if host != number_of_kv_nodes - 1:
            if (ls.creates != 0 or ls.reads != 0 or ls.updates != 0 or ls.deletes != 0) \
                    and ls.items != float('inf'):
                operation = int(ls.items / (number_of_kv_nodes * 100)) * 100
                existing_item = calculate_existing_items()
        else:
            if (ls.creates != 0 or ls.reads != 0 or ls.updates != 0 or ls.deletes != 0) \
                    and ls.items != float('inf'):
                operation = \
                    ls.items - (int(ls.items / (number_of_kv_nodes * 100)) * 100 * (number_of_kv_nodes - 1))
                existing_item = calculate_existing_items()
                self.current_host = CurrentHostMutex()
        time.sleep(number_of_kv_nodes * 2 - sleep_time)
        cmdstr = "spring -c {} -r {} -u {} -d {} -e {} -s {} -i {} -w {} -W {} -n {}"\
            .format(ls.creates, ls.reads, ls.updates, ls.deletes, ls.expiration, ls.size, existing_item,
                    items_in_working_set, operations_to_hit_working_set, ls.spring_workers, self.hosts[0])
        if operation != 0:
            cmdstr += " -o {}".format(operation)
        if throughput != float('inf'):
            thrput = int(throughput / (number_of_kv_nodes * 100)) * 100
            cmdstr += " -t {}".format(thrput)
        cmdstr += " cb://Administrator:password@{}:8091/bucket-1".format(self.hosts[0])
        pty = True
        if silent:
            cmdstr += " >/tmp/springlog.log 2>&1 &"
            pty = False
        logger.info(cmdstr)
        result = run(cmdstr, pty=pty)
        if silent:
            time.sleep(10)
            res = run(r"ps aux | grep -ie spring | awk '{print $11}'")
            if "python" not in res:
                raise Exception("Spring not run on KV. {}".format(res))
        else:
            if "Current progress: 100.00 %" not in result and "Finished: worker-{}".format(ls.spring_workers - 1) not in result:
                raise Exception("Spring not run on KV")

    @all_kv_nodes
    def check_spring_running(self):
        cmdstr = r"ps aux | grep -ie spring | awk '{print $11}'"
        result = run(cmdstr)
        logger.info(result)
        logger.info(result.stdout)

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

    def shutdown(self, host):
        with settings(host_string=host):
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
            run('yes | gdebi /tmp/{}'.format(filename))
        else:
            run('yes | rpm -i /tmp/{}'.format(filename))

    @all_hosts
    def restart(self):
        logger.info('Restarting server')
        run('service couchbase-server restart', pty=False)

    @all_hosts
    def restart_with_alternative_num_vbuckets(self, num_vbuckets):
        logger.info('Changing number of vbuckets to {}'.format(num_vbuckets))
        run('systemctl set-environment COUCHBASE_NUM_VBUCKETS={}'
            .format(num_vbuckets))
        run('service couchbase-server restart', pty=False)
        run('systemctl unset-environment COUCHBASE_NUM_VBUCKETS')

    @all_hosts
    def stop_server(self):
        logger.info('Stopping Couchbase Server')
        run('/etc/init.d/couchbase-server stop', pty=False)

    @all_hosts
    def start_server(self):
        logger.info('Starting Couchbase Server')
        run('/etc/init.d/couchbase-server start', pty=False)

    def detect_if(self):
        stdout = run("ip route list | grep default")
        return stdout.strip().split()[4]

    def detect_ip(self, _if):
        stdout = run('ifdata -pa {}'.format(_if))
        return stdout.strip()

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

    @all_hosts
    def detect_core_dumps(self):
        # Based on kernel.core_pattern = /data/core-%e-%p
        r = run('ls /data/core*', quiet=True)
        if not r.return_code:
            return r.split()
        else:
            return []

    @all_hosts
    def tune_log_rotation(self):
        logger.info('Tune log rotation so that it happens less frequently')
        run('sed -i "s/num_files, [0-9]*/num_files, 50/" '
            '/opt/couchbase/etc/couchbase/static_config')

    @single_host
    def cbrestorefts(self, archive_path, repo_path):
        cmd = "cd /opt/couchbase/bin && ./cbbackupmgr restore --archive {} --repo {} " \
              " --host http://localhost:8091 --username Administrator " \
              "--password password --disable-ft-indexes --disable-gsi-indexes".\
            format(archive_path, repo_path)
        run(cmd)

    @single_host
    def startelasticsearchplugin(self):
        cmds = [
            'service elasticsearch restart',
            'service elasticsearch stop',
            'sudo chkconfig --add elasticsearch',
            'sudo service elasticsearch restart',
            'sudo service elasticsearch stop',
            'cd /usr/share/elasticsearch',
            'echo "couchbase.password: password" >> /etc/elasticsearch/elasticsearch.yml',
            '/usr/share/elasticsearch/bin/plugin  -install transport-couchbase -url http://packages.couchbase.com.s3.amazonaws.com/releases/elastic-search-adapter" \
                                                      "/2.0.0/elasticsearch-transport-couchbase-2.0.0.zip',
            'echo "couchbase.password: password" >> /etc/elasticsearch/elasticsearch.yml',
            'echo "couchbase.username: Administrator" >> /etc/elasticsearch/elasticsearch.yml'
            'bin/plugin -install mobz/elasticsearch-head',
            'sudo service elasticsearch restart',
        ]

        for cmd in cmds:
            logger.info("Executed command: {}".format(cmd))
            run(cmd)

    @single_client
    def ycsb_load_run(self, path, cmd, log_path=None, mypid=0):
        state.connections.clear()
        if log_path:
            tmpcmd = 'rm -rf ' + log_path
            run(tmpcmd)
            tmpcmd = 'mkdir -p ' + log_path
            run(tmpcmd)
        load_run_cmd = 'cd {}'.format(path) + '_' + str(mypid) + \
            ' && mvn -pl com.yahoo.ycsb:couchbase2-binding -am ' \
            'clean package -Dmaven.test.skip -Dcheckstyle.skip=true && {}'.format(cmd)
        logger.info(" running command {}".format(load_run_cmd))
        return run(load_run_cmd)

    @all_hosts
    def fio(self, config):
        logger.info('Running fio job: {}'.format(config))
        filename = os.path.basename(config)
        remote_path = os.path.join('/tmp', filename)
        put(config, remote_path)
        return run('fio --minimal {}'.format(remote_path))

    @kv_node_cbindexperf
    def kill_process_on_kv_nodes(self, process, **kwargs):
        cmd = "killall -9 {}{}".format(process, kwargs["host_num"])
        logger.info("Running command: {}".format(cmd))
        run(cmd, warn_only=True, quiet=True)

    @kv_node_cbindexperf
    def run_cbindexperf(self, index_node, config_data, concurrency, **kwargs):

        def modify_json_file(per_node_conn):
            config_data["Concurrency"] = per_node_conn
            with open('/tmp/config_template.json', 'w') as config_file:
                json.dump(config_data, config_file)

        host_num = kwargs["host_num"]
        per_node_concurrency = concurrency / len(self.kv_hosts)
        logger.info("Per node concurrency: {}".format(per_node_concurrency))
        modify_json_file(per_node_concurrency)

        executable = "/opt/couchbase/bin/cbindexperf{}".format(host_num)

        copy_cmd = "cp /opt/couchbase/bin/cbindexperf {}".format(executable)
        run(copy_cmd)

        cmdstr = "{} -cluster {} -auth=Administrator:password " \
                 "-configfile /tmp/config_template.json -resultfile result.json" \
                 " -statsfile /root/statsfile &> /tmp/cbindexperf.log &" \
            .format(executable, index_node)

        with open("/tmp/config_template.json") as f:
            file_data = f.read()
        create_file_cmd = "echo '{}' > /tmp/config_template.json".format(file_data)
        logger.info("Calling command: {}".format(create_file_cmd))
        run(create_file_cmd)

        logger.info("Calling command: {}".format(cmdstr))
        result = run(cmdstr, pty=False)
        return result

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
            r = run('grep "Failed over \'" '
                    '/opt/couchbase/var/lib/couchbase/logs/info.log', quiet=True)
            if not r.return_code:
                return r.strip().split(',')[1]

    @property
    def num_vcpu(self):
        return int(run('lscpu -a -e | wc -l')) - 1

    @property
    def num_cores(self):
        return int(run('lscpu | grep socket').strip().split()[-1])

    @all_hosts
    def disable_cpu(self):
        logger.info('Throttling CPU resources')
        reserved_cores = {i for i in range(0, 2 * self.num_cores, 2)}
        all_cores = {i for i in range(self.num_vcpu)}

        for i in all_cores - reserved_cores:
            run('echo 0 > /sys/devices/system/cpu/cpu{}/online'.format(i))

    @all_hosts
    def enable_cpu(self):
        logger.info('Enabling all CPU cores')
        for i in range(self.num_vcpu):
            run('echo 1 > /sys/devices/system/cpu/cpu{}/online'.format(i))

    @index_node
    def kill_indexer_process(self):
        logger.info('Killing indexer process')
        run("killall indexer")

from multiprocessing import Event, Manager, Process

import numpy as np
from logger import logger

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class YCSBException(Exception):
    pass


class YCSBWorker(object):
    def __init__(self, access_settings, remote, run_cmd, ycsb):
        self.workers = access_settings.workers
        self.remote = remote
        self.run_cmd = run_cmd
        self.timer = access_settings.time
        self.ycsb = ycsb
        self.shutdown_event = self.timer and Event() or None
        self.ycsb_result = Manager().dict({key: [] for key in ['Throughput', 'READ_95', 'UPDATE_95']})
        self.ycsb_logfiles = Manager().list()
        self.task = self.ycsb_work

    def time_to_stop(self):
        return (self.shutdown_event is not None and
                self.shutdown_event.is_set())

    def ycsb_work(self, mypid):
        flag = True
        log_file = '{}_{}.txt'.format(self.ycsb.log_path +
                                      self.ycsb.log_file, str(mypid))
        self.ycsb_logfiles.append(log_file)
        self.run_cmd += ' -p exportfile={}'.format(log_file)
        try:
            while flag and not self.time_to_stop():
                self.remote.ycsb_load_run(self.ycsb.path,
                                          self.run_cmd,
                                          log_path=self.ycsb.log_path)
                flag = False
        except Exception as e:
            raise YCSBException(' Error while running YCSB load' + e)

    def pattern(self, line):
        ttype, measure, value = map(str.strip, line.split(','))
        key = ''
        if ttype == "[OVERALL]" and measure == "Throughput(ops/sec)":
            key = 'Throughput'
        elif ttype == "[READ]" and measure == "95thPercentileLatency(us)":
            key = 'READ_95'
        elif ttype == "[UPDATE]" and measure == "95thPercentileLatency(us)":
            key = 'UPDATE_95'
        else:
            return
        self.ycsb_result[key] += [float(value)]

    def parse_work(self, mypid):
        filename = self.ycsb_logfiles[mypid]
        with open(filename, "r") as txt:
            for line in txt:
                self.pattern(line)
        txt.close()

    def run(self):
        processes = [Process(target=self.task, args=(x,)) for x in range(self.workers)]
        for p in processes:
            p.start()

        for p in processes:
            p.join()
            if p.exitcode:
                    logger.interrupt('Worker finished with non-zero exit code')

    def parse(self):
        self.task = self.parse_work
        self.run()
        return np.sum(self.ycsb_result['Throughput']), np.mean(self.ycsb_result['READ_95']), np.mean(self.ycsb_result['UPDATE_95'])


class YCSBdata(PerfTest):

    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        super(YCSBdata, self).__init__(cluster_spec, test_config, verbose, experiment)
        self.ycsb = test_config.ycsb_settings
        self.hosts = [x.rpartition(':')[0] for x in self.cluster_spec.yield_servers()]

    def create_load_cmd(self, action='load', jvm=True):
        """
        The ycsb command looks like
           ./bin/ycsb run couchbase2 -jvm-args=-Dcom.couchbase.connectTimeout=15000
           -jvm-args=-Dcom.couchbase.kvTimeout=60000
           -s -P workloads/workloada  -threads 7 -p couchbase.host=172.23.123.38 -p recordcount=100000 -p
            operationcount=100000
            -p maxexecutiontime=1000 -p couchbase.upsert=true -p couchbase.queryEndpoints=1  -p
            couchbase.epoll=true
            -p couchbase.boost=0 -p exportfile=rundata.json

        ./bin/ycsb load couchbase2 -jvm-args=-Dcom.couchbase.connectTimeout=300000
        -jvm-args=-Dcom.couchbase.kvTimeout=60000 -P workloads/workloada -p
        couchbase.host=172.23.123.38 -threads 6 -p recordcount=100000 -exportfile=loaddata.json
        """
        commandlist = []
        commandlist.append('/' + self.ycsb.path + '/bin/ycsb')
        commandlist.append(action)
        commandlist.append(self.ycsb.sdk)
        commandlist.append('-s -P ' + self.ycsb.workload)
        if jvm:
            cmd = '-jvm-args=-D'
            for c in self.ycsb.jvm.split(','):
                commandlist.append(cmd + c)

        commandlist.append('-p %s' % self.ycsb.bucket)
        commandlist.append('-p couchbase.host=%s' % ','.join(self.hosts))
        commandlist.append('-p threads=%s' % self.ycsb.threads)
        commandlist.append('-p recordcount=%s' % self.ycsb.reccount)
        commandlist.append('-p couchbase.password=%s' % self.rest.rest_password)

        if action == 'run':
            commandlist.append('-p operationcount=%s' % self.ycsb.opcount)
            commandlist.append('-p exportfile=%s' % (self.ycsb.log_path +
                                                     self.ycsb.log_file))

        return ' '.join(commandlist)


class YCSBTest(YCSBdata):

    def __init__(self, cluster_spec, test_config, verbose, experiment=None):
        super(YCSBTest, self).__init__(cluster_spec, test_config,
                                       verbose, experiment)

    def load(self):
        try:
            logger.info('running YCSB for loading data')
            cmd = self.create_load_cmd()
            self.remote.ycsb_load_run(self.ycsb.path, cmd)
        except Exception, e:
            raise YCSBException('YCSB error while loading data' + e.message)

    @with_stats
    def access_bg(self):
        run_cmd = self.create_load_cmd(action="run")
        self.workload = YCSBWorker(self.test_config.access_settings, self.remote, run_cmd, self.ycsb)
        self.workload.run()

    def post_sf(self, thput, readl, writel, query=None):
        self.reporter.post_to_sf(thput, metric='throughput')
        self.reporter.post_to_sf(readl, metric='95_percentile_Read_Latency')
        self.reporter.post_to_sf(writel, metric='95_percentile_update_Latency')

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()
        self.access_bg()
        thput, readl, writel = self.workload.parse()
        self.post_sf(thput, readl, writel)

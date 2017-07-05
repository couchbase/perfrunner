from multiprocessing import Event, Manager, Process

import numpy as np

from logger import logger
from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class YCSBException(Exception):
    pass


class YCSBWorker:
    def __init__(self, access_settings, remote, test, ycsb):
        self.workers = access_settings.workers
        self.remote = remote
        self.test = test
        self.timer = access_settings.time
        self.ycsb = ycsb
        self.shutdown_event = self.timer and Event() or None
        self.ycsb_result = Manager().dict({
            key: [] for key in ['Throughput', 'READ_95', 'UPDATE_95', 'INSERT_95', 'SCAN_95']
        })
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
        self.run_cmd = self.test.create_load_cmd(action="run", mypid=mypid)
        self.run_cmd += ' -p exportfile={}'.format(log_file)

        while flag and not self.time_to_stop():
            self.remote.ycsb_load_run(self.ycsb.path,
                                      self.run_cmd,
                                      log_path=self.ycsb.log_path,
                                      mypid=mypid)
            flag = False

    def pattern(self, line):
        ttype, measure, value = map(str.strip, line.split(','))
        if ttype == "[OVERALL]" and measure == "Throughput(ops/sec)":
            key = 'Throughput'
        elif ttype == "[READ]" and measure == "95thPercentileLatency(us)":
            key = 'READ_95'
        elif ttype == "[UPDATE]" and measure == "95thPercentileLatency(us)":
            key = 'UPDATE_95'
        elif ttype == "[INSERT]" and measure == "95thPercentileLatency(us)":
            key = 'INSERT_95'
        elif ttype == "[SCAN]" and measure == "95thPercentileLatency(us)":
            key = 'SCAN_95'
        else:
            return
        self.ycsb_result[key] += [round(float(value))]

    def parse_work(self, mypid):
        filename = self.ycsb_logfiles[mypid]
        with open(filename, "r") as txt:
            for line in txt:
                self.pattern(line)

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
        return np.sum(self.ycsb_result['Throughput']), \
            np.mean(self.ycsb_result['READ_95']), \
            np.mean(self.ycsb_result['UPDATE_95']), \
            np.mean(self.ycsb_result['INSERT_95']), \
            np.mean(self.ycsb_result['SCAN_95'])


class YCSBdata(PerfTest):

    def __init__(self, cluster_spec, test_config, verbose):
        super().__init__(cluster_spec, test_config, verbose, )
        self.ycsb = test_config.ycsb_settings
        self.hosts = list(self.cluster_spec.servers)

    def create_load_cmd(self, action='load', jvm=True, mypid=0):
        commandlist = [
            '/' + self.ycsb.path + '_' + str(mypid) + '/bin/ycsb',
            action,
            self.ycsb.sdk,
            '-s -P ' + self.ycsb.path + '_' + str(mypid) + self.ycsb.workload,
        ]

        if self.ycsb.workload == '/workloads/workloade':
            PerfTest.COLLECTORS = {
                'n1ql_latency': True,
                'n1ql_stats': True,
                'secondary_stats': True,
            }
        if jvm:
            cmd = '-jvm-args=-D'
            for c in self.ycsb.jvm.split(','):
                commandlist.append(cmd + c)

        commandlist.append('-p %s' % self.ycsb.bucket)
        commandlist.append('-p couchbase.host=%s' % self.hosts[0])
        commandlist.append('-threads %s' % self.ycsb.threads)
        commandlist.append('-p recordcount=%s' % self.ycsb.reccount)
        commandlist.append('-p couchbase.password=%s' % self.rest.rest_password)

        if action == 'run':
            commandlist.append('-p maxexecutiontime=%s' % self.test_config.access_settings.time)
            commandlist.append('-p operationcount=%s' % self.ycsb.opcount)
            commandlist.append('-p exportfile=%s' % (self.ycsb.log_path +
                                                     self.ycsb.log_file))
            if self.ycsb.parameters:
                for param in self.ycsb.parameters.split(', '):
                    commandlist.append('-p %s' % param)

        return ' '.join(commandlist)


class YCSBTest(YCSBdata):

    def create_index(self):
        for server_role in self.cluster_spec.roles.values():
            if 'index' in server_role:
                logger.info('creating indexes')
                host = self.hosts[0]
                length = len(self.hosts)
                for idx in range(0, length):
                    if self.ycsb.index == "primary":
                        statement = "create primary index i" \
                            + str(idx) + " on `" + self.ycsb.bucket.split('=')[1] + "`"
                    else:
                        statement = "create index wle_idx_" \
                            + str(idx) + " on `" + self.ycsb.bucket.split('=')[1] + "`(meta().id)'"
                    self.rest.exec_n1ql_statement(host, statement)
                break

    def load(self, *args):
        logger.info('running YCSB for loading data')
        cmd = self.create_load_cmd()
        self.remote.ycsb_load_run(self.ycsb.path, cmd)

    @with_stats
    def access_bg(self, *args):
        self.workload = YCSBWorker(self.test_config.access_settings, self.remote, self, self.ycsb)
        self.workload.run()

    def post_sf(self, thput, readl, writel, insertl, scanl):
        if not np.isnan(thput):
            self.reporter.post(
                *self.metrics.ycsb_queries(round(thput),
                                           name='Overall_Throughput',
                                           title='Overall Throughput')
            )
        if not np.isnan(readl):
            self.reporter.post(
                *self.metrics.ycsb_queries(round((float(readl) / 1000), 2),
                                           name='Read_Latency_95_p',
                                           title='95th percentile Read latency, ms')
            )
        if not np.isnan(writel):
            self.reporter.post(
                *self.metrics.ycsb_queries(round((float(writel) / 1000), 2),
                                           name='Write_Latency_95_p',
                                           title='95th percentile Write latency, ms')
            )
        if not np.isnan(insertl):
            self.reporter.post(
                *self.metrics.ycsb_queries(round((float(insertl) / 1000), 2),
                                           name='Insert_Latency_95_p',
                                           title='95th percentile Insert latency, ms')
            )
        if not np.isnan(scanl):
            self.reporter.post(
                *self.metrics.ycsb_queries(round((float(scanl) / 1000), 2),
                                           name='Scan_Latency_95_p',
                                           title='95th percentile Scan latency, ms')
            )

    def run(self):
        self.create_index()
        self.load()
        self.wait_for_persistence()
        self.access_bg()
        thput, readl, writel, insertl, scanl = self.workload.parse()
        self.post_sf(thput, readl, writel, insertl, scanl)

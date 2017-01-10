from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.helpers.local import run_ycsb
from perfrunner.tests import PerfTest
from perfrunner.tests.n1ql import N1QLTest


class YCSBTest(PerfTest):

    def load(self, *args):
        load_settings = self.test_config.load_settings
        ycsb_settings = self.test_config.ycsb_settings

        for target in self.target_iterator:
            host = target.node.split(':')[0]

            run_ycsb(host=host,
                     bucket=target.bucket,
                     password=target.password,
                     action='load',
                     workload=ycsb_settings.workload,
                     items=load_settings.items,
                     workers=load_settings.workers)

    @with_stats
    def access(self, *args):
        access_settings = self.test_config.access_settings
        ycsb_settings = self.test_config.ycsb_settings

        for target in self.target_iterator:
            host = target.node.split(':')[0]

            run_ycsb(host=host,
                     bucket=target.bucket,
                     password=target.password,
                     action='run',
                     workload=ycsb_settings.workload,
                     items=access_settings.items,
                     workers=access_settings.workers,
                     ops=int(access_settings.ops),
                     time=access_settings.time)

    def _report_kpi(self):
        self.reporter.post_to_sf(
            self.metric_helper.parse_ycsb_throughput()
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.access()

        self.report_kpi()


class YCSBN1QLTest(YCSBTest, N1QLTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.build_index()

        self.access()

        self.report_kpi()

from perfrunner.tests import PerfTest, TargetIterator
from perfrunner.workloads.n1ql import INDEX_STATEMENTS


class TuqTest(PerfTest):

    def __init__(self, *args, **kwargs):
        super(TuqTest, self).__init__(*args, **kwargs)
        self.ddocs = None  # Compatibility
        self.target_iterator = TargetIterator(self.cluster_spec,
                                              self.test_config,
                                              prefix='')

    def build_index(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for index in self.test_config.index_settings.indexes:
                    for stmnt in INDEX_STATEMENTS[index]:
                        host = master.split(':')[0]
                        self.rest.exec_n1ql_stmnt(host, stmnt.format(bucket))

    def access(self):
        super(TuqTest, self).timer()

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

        self.workload = self.test_config.access_settings
        self.access_bg_with_ddocs()
        self.access()

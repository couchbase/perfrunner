from perfrunner.tests import PerfTest
from perfrunner.workloads.n1ql import INDEX_STATEMENTS


class TuqTest(PerfTest):

    def build_index(self):
        for master in self.cluster_spec.yield_masters():
            for bucket in self.test_config.buckets:
                for index in self.test_config.index_settings.indexes:
                    for stmnt in INDEX_STATEMENTS[index]:
                        host = master.split(':')[0]
                        self.rest.exec_n1ql_stmnt(host, stmnt.format(bucket))

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.build_index()

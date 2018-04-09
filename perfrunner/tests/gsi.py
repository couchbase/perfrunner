from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.tests import PerfTest
from perfrunner.workloads.kvgen import kvgen


class IndexTest(PerfTest):

    COLLECTORS = {
        'secondary_stats': True,
        'secondary_debugstats': True,
        'secondary_debugstats_bucket': True,
        'secondary_debugstats_index': True,
    }

    @with_stats
    @timeit
    def init_index(self):
        self.create_indexes()
        self.wait_for_indexing()

    @with_stats
    @timeit
    def incr_index(self):
        self.wait_for_indexing()

    def _report_kpi(self, indexing_time: float):
        self.reporter.post(
            *self.metrics.indexing_time(indexing_time)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.init_index()

        self.access_bg()
        self.incr_index()


class InitialIndexTest(IndexTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        time_elapsed = self.init_index()

        self.report_kpi(time_elapsed)


class FastIndexTest(PerfTest):

    def load(self, *args):
        kvgen(self.master_node, self.test_config.load_settings.items, wait=True)

    def access_bg(self, *args):
        kvgen(self.master_node, self.test_config.load_settings.items, wait=False)


class FastInitialIndexTest(FastIndexTest, InitialIndexTest):

    pass

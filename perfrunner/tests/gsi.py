from threading import Thread

from perfrunner.helpers.cbmonitor import timeit, with_stats
from perfrunner.helpers.local import run_kvgen
from perfrunner.tests import PerfTest


class IndexTest(PerfTest):

    COLLECTORS = {
        'secondary_stats': True,
        'secondary_debugstats': True,
        'secondary_debugstats_bucket': True,
        'secondary_debugstats_index': True,
    }

    NUM_KVGEN_INSTANCES = 5

    def kvgen(self):
        num_docs = self.test_config.load_settings.items // self.NUM_KVGEN_INSTANCES

        threads = []
        for i in range(self.NUM_KVGEN_INSTANCES):
            prefix = 'prefix-{}'.format(i)
            thread = Thread(target=run_kvgen, args=(self.master_node, num_docs,
                                                    prefix))
            threads.append(thread)

        return threads

    def load(self, *args):
        threads = self.kvgen()
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def bg_load(self):
        threads = self.kvgen()
        for t in threads:
            t.start()

    def create_index(self):
        storage = self.test_config.gsi_settings.storage
        indexes = self.test_config.gsi_settings.indexes

        for server in self.index_nodes:
            for bucket in self.test_config.buckets:
                for name, field in indexes.items():
                    self.rest.create_index(host=server,
                                           bucket=bucket,
                                           name=name,
                                           field=field,
                                           storage=storage)

    @with_stats
    @timeit
    def init_index(self):
        for server in self.index_nodes:
            self.monitor.monitor_indexing(server)

    @with_stats
    @timeit
    def incr_index(self):
        for server in self.index_nodes:
            self.monitor.monitor_indexing(server)

    def _report_kpi(self, indexing_time: float):
        self.reporter.post(
            *self.metrics.indexing_time(indexing_time)
        )

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.create_index()
        self.init_index()

        self.bg_load()
        self.incr_index()


class InitialIndexTest(IndexTest):

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.create_index()
        time_elapsed = self.init_index()

        self.report_kpi(time_elapsed)

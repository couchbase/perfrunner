from threading import Thread

from perfrunner.helpers.cbmonitor import with_stats
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
        master = self.master_node.split(':')[0]
        num_docs = self.test_config.load_settings.items / self.NUM_KVGEN_INSTANCES

        threads = []
        for i in range(self.NUM_KVGEN_INSTANCES):
            prefix = 'prefix-{}'.format(i)
            thread = Thread(target=run_kvgen, args=(master, num_docs, prefix))
            threads.append(thread)

        return threads

    def load(self, *args):
        threads = self.kvgen()
        map(lambda t: t.start(), threads)
        map(lambda t: t.join(), threads)

    def bg_load(self):
        threads = self.kvgen()
        map(lambda t: t.start(), threads)

    def create_index(self):
        storage = self.test_config.gsi_settings.storage
        indexes = self.test_config.gsi_settings.indexes

        for _, servers in self.cluster_spec.yield_servers_by_role('index'):
            for server in servers:
                for bucket in self.test_config.buckets:
                    for name, field in indexes.items():
                        self.rest.create_index(host_port=server,
                                               bucket=bucket,
                                               name=name,
                                               field=field,
                                               storage=storage)

    @with_stats
    def init_index(self):
        for name, servers in self.cluster_spec.yield_servers_by_role('index'):
            for server in servers:
                self.monitor.monitor_indexing(server)

    @with_stats
    def incr_index(self):
        for name, servers in self.cluster_spec.yield_servers_by_role('index'):
            for server in servers:
                self.monitor.monitor_indexing(server)

    def run(self):
        self.load()
        self.wait_for_persistence()

        self.create_index()
        self.init_index()

        self.bg_load()
        self.incr_index()

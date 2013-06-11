from perfrunner.tests.kv import KVTest
from perfrunner.tests.viewgen import ViewGen


class BucketCompactionTest(KVTest):

    def _compact_bucket(self):
        for target in self.target_iterator:
            self.rest.trigger_bucket_compaction(target.node,
                                                target.bucket)
            self.monitor.monitor_bucket_fragmentation(target)

    def run(self):
        self._run_load_phase()  # initial load
        self._run_load_phase()  # extra mutations for bucket fragmentation

        self.reporter.start()
        self._compact_bucket()
        self.reporter.finish('Bucket compaction')


class IndexCompactionTest(BucketCompactionTest):

    def __init__(self, *args):
        super(IndexCompactionTest, self).__init__(*args)

        view_gen = ViewGen()
        views_settings = self.test_config.get_index_settings()
        if views_settings.disabled_updates:
            options = {'updateMinChanges': 0, 'replicaUpdateMinChanges': 0}
        else:
            options = None

        self.ddocs = view_gen.generate_ddocs(views_settings.views, options)

    def _define_ddocs(self):
        for target_settings in self.target_iterator:
            for ddoc_name, ddoc in self.ddocs.iteritems():
                self.rest.create_ddoc(target_settings.node,
                                      target_settings.bucket, ddoc_name, ddoc)

    def _build_index(self):
        for target in self.target_iterator:
            self.monitor.monitor_task(target, 'indexer')

    def _compact_index(self):
        for target in self.target_iterator:
            for ddoc_name in self.ddocs:
                self.rest.trigger_index_compaction(target.node, ddoc_name,
                                                   target.bucket)
            self.monitor.monitor_task(target, 'view_compaction')

    def run(self):
        self._run_load_phase()
        self._compact_bucket()

        self._define_ddocs()
        self._build_index()

        self._run_load_phase()  # extra mutations for index fragmentation
        self._compact_bucket()
        self._build_index()

        self.reporter.start()
        self._compact_index()
        self.reporter.finish('Index compaction')

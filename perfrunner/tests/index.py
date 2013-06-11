from perfrunner.tests.compaction import IndexCompactionTest


class InitialIndexTest(IndexCompactionTest):

    def run(self):
        self._run_load_phase()
        self._compact_bucket()

        self.reporter.start()
        self._define_ddocs()
        self._build_index()
        self.reporter.finish('Initial index')

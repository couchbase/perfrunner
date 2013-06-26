from perfrunner.tests import PerfTest


def with_stats(method):
    def wrapper(self, *args, **kwargs):
        self.cbagent.update_metadata()
        self.cbagent.start()
        method(self, *args, **kwargs)
        self.cbagent.stop()
    return wrapper


class KVTest(PerfTest):

    @with_stats
    def run(self):
        self._run_load_phase()
        self._compact_bucket()
        self._run_access_phase()

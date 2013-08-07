from perfrunner.tests.index import IndexTest

from logger import logger


class ViewsTest(IndexTest):

    def access_bg(self):
        access_settings = self.test_config.get_access_settings()
        logger.info('Running access phase: {0}'.format(access_settings))
        self.worker_manager.run_workload(access_settings, self.target_iterator,
                                         ddocs=self.ddocs)

    def run(self):
        self.load()
        self.wait_for_persistence()
        self.compact_bucket()

        self.define_ddocs()
        self.build_index()

        self.access_bg()
        self.timer()
        self.shutdown_event.set()

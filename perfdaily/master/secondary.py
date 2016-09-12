from perfrunner.tests.secondary import InitialandIncrementalSecondaryIndexTest
import time


class InitialandIncrementalSecondaryIndexTestDaily(InitialandIncrementalSecondaryIndexTest):
    def run(self):
        self.run_load_for_2i()
        self.wait_for_persistence()
        self.compact_bucket()

        metrics = list()
        from_ts, to_ts = self.build_secondaryindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Initial secondary index', time_elapsed)

        metrics.append(self.metric_helper.get_indexing_meta_daily(value=time_elapsed,
                                                                  index_type='Initial'))

        if self.secondaryDB != 'memdb':
            time.sleep(300)
        from_ts, to_ts = self.build_incrindex()
        time_elapsed = (to_ts - from_ts) / 1000.0
        time_elapsed = self.reporter.finish('Incremental secondary index', time_elapsed)
        metrics.append(self.metric_helper.get_indexing_meta_daily(value=time_elapsed,
                                                                  index_type='Incremental'))

        self.reporter.post_to_dailyp(metrics)
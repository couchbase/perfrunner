import concurrent.futures
import time

from perfrunner.helpers.cbmonitor import with_stats
from perfrunner.tests import PerfTest


class TestMultiThreaded(PerfTest):

    """A dummy test that is used for test developer.

    It is for PerfTest that uses multithreading for run
    different test steps (and measure) in parallel during
    the test.
    This approach was not used in other PerfTests before
    try it here.
    Keep the code here in case need to try something later
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.thread1_latency = 0
        self.thread2_latency = 0
        self.raise_exception = self.test_config.unittest_settings.raise_exception
        self.sleep_sec = self.test_config.unittest_settings.sleep_sec

    def execute_tasks(self, *tasks):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)
        results = []
        for c in tasks:
            results.append(executor.submit(c))
        for r in results:
            r.result()

    def method_thr1(self):
        t0 = time.time()
        time.sleep(self.sleep_sec)
        self.thread1_latency = time.time() - t0  # Rebalance time in seconds

    def method_thr2(self):
        t0 = time.time()
        if self.raise_exception:
            raise Exception("Thread2 exception")
        else:
            time.sleep(self.sleep_sec)
        self.thread2_latency = time.time() - t0  # Rebalance time in seconds

    @with_stats
    def access(self, *args, **kwargs):
        self.execute_tasks(self.method_thr1, self.method_thr2)

    def run(self):
        self.access()
        self.report_kpi()

    def _report_kpi(self):
        if self.thread1_latency is not None:
            self.reporter.post(
                *self.metrics.rebalance_time(self.thread1_latency)
            )
        if self.thread2_latency is not None:
            self.reporter.post(
                *self.metrics.rebalance_time(self.thread2_latency)
            )

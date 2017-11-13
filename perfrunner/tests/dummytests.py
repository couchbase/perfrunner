import sys
import threading
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
        self.thr_exceptions = []

    def method_thr1(self):
        try:
            t0 = time.time()
            if self.raise_exception:
                raise Exception("Thread1 exception")
            else:
                time.sleep(self.sleep_sec)
            self.thread1_latency = time.time() - t0  # Rebalance time in seconds
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    def method_thr2(self):
        try:
            t0 = time.time()
            if self.raise_exception:
                raise Exception("Thread1 exception")
            else:
                time.sleep(self.sleep_sec)
            self.thread2_latency = time.time() - t0  # Rebalance time in seconds
        except Exception:
            self.thr_exceptions.append(sys.exc_info())

    @with_stats
    def access(self, *args, **kwargs):
        thr1 = threading.Thread(target=self.method_thr1)
        thr2 = threading.Thread(target=self.method_thr2)
        thr1.start()
        thr2.start()
        thr1.join()
        thr2.join()
        if len(self.thr_exceptions) > 0:
            raise self.thr_exceptions[0][1]

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

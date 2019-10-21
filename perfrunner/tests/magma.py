from perfrunner.tests.kv import ReadLatencyDGMTest, ThroughputDGMCompactedTest


class ReadLatencyDGMMagmaTest(ReadLatencyDGMTest):

    COLLECTORS = {'disk': True, 'latency': True, 'net': False, 'kvstore': True}

    def __init__(self, *args):
        super().__init__(*args)

        self.collect_per_server_stats = self.test_config.magma_settings.collect_per_server_stats


class MixedLatencyDGMTest(ReadLatencyDGMMagmaTest):

    def _report_kpi(self):
        for operation in ('get', 'set'):
            self.reporter.post(
                *self.metrics.kv_latency(operation=operation)
            )


class ThroughputDGMCompactedMagmaTest(ThroughputDGMCompactedTest):

    COLLECTORS = {'disk': True, 'latency': True, 'net': False, 'kvstore': True}

    def __init__(self, *args):
        super().__init__(*args)

        self.collect_per_server_stats = self.test_config.magma_settings.collect_per_server_stats

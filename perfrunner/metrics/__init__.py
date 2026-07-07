from __future__ import annotations

from typing import TYPE_CHECKING

from perfrunner.helpers.metrics import MetricHelper

if TYPE_CHECKING:
    from perfrunner.tests import PerfTest


class PrometheusMetricsHelper(MetricHelper):
    """MetricHelper backed by the Prometheus metrics store.

    Swaps the store to the PrometheusAgent. MetricHelper methods read the cbagent metrics
    pushed to the store (each read resolves to the same ``perf_<name>`` the collectors push).
    """

    def __init__(self, test: PerfTest):
        super().__init__(test)
        self.store = test.collector_agent

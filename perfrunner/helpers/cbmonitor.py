from __future__ import annotations

import importlib.metadata
import time
from typing import TYPE_CHECKING, Callable, Union

if TYPE_CHECKING:
    from perfrunner.tests import PerfTest

from decorator import decorator

from logger import logger
from perfrunner.metrics.cbagent import CbAgent
from perfrunner.metrics.promagent import PrometheusAgent


@decorator
def timeit(method: Callable, *args, **kwargs) -> float:
    t0 = time.time()
    method(*args, **kwargs)
    return time.time() - t0  # Elapsed time in seconds


@decorator
def with_stats(method: Callable, *args, **kwargs) -> Union[float, None]:
    test = args[0]
    phase_name = method.__name__

    # Use PrometheusAgent if configured, otherwise use CbAgent
    agent = test.collector_agent or CbAgent(test=test, phase=phase_name)
    if isinstance(agent, PrometheusAgent):
        agent.set_phase(phase_name)
        # Add custom collectors for metrics not available via Prometheus scraping
        agent.add_custom_collectors(test)

    drain_spring_data_files(test)

    with agent:
        return method(*args, **kwargs)


def drain_spring_data_files(test: PerfTest):
    """Empty the spring latency dump dir so this phase cannot inherit older data files."""
    if not test.test_config.stats_settings.enabled:
        return

    # Only the spring latency collectors read these files, so nothing else has to pay for
    # the round trip to every worker.
    if not (test.COLLECTORS.get("latency") or test.COLLECTORS.get("n1ql_latency")):
        return

    try:
        test.cleanup_spring_data_files()
    except (Exception, SystemExit) as e:
        logger.warning(f"Failed to clean up spring latency data files: {e!r}")


@decorator
def with_cloudwatch(method, *args, **kwargs):
    sdk_major_version = int(importlib.metadata.version("couchbase")[0])

    if sdk_major_version >= 3:
        from perfrunner.helpers.cloudwatch import Cloudwatch
        t0 = time.time()
        method(*args, **kwargs)
        t1 = time.time()
        Cloudwatch(args[0].cluster_spec.servers, t0, t1, method.__name__)
    else:
        logger.info("Cloudwatch unavailable in Python SDK 2 Tests.")

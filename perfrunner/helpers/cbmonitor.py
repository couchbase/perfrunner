import time
from typing import Callable, Union

import pkg_resources
from decorator import decorator

from logger import logger
from perfrunner.metrics.cbagent import CbAgent
from perfrunner.metrics.prometheus_agent import PrometheusAgent


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

    with agent:
        return method(*args, **kwargs)


@decorator
def with_cloudwatch(method, *args, **kwargs):
    sdk_major_version = int(pkg_resources.get_distribution("couchbase").version[0])

    if sdk_major_version >= 3:
        from perfrunner.helpers.cloudwatch import Cloudwatch
        t0 = time.time()
        method(*args, **kwargs)
        t1 = time.time()
        Cloudwatch(args[0].cluster_spec.servers, t0, t1, method.__name__)
    else:
        logger.info("Cloudwatch unavailable in Python SDK 2 Tests.")

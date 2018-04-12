import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from typing import Iterator, List

import numpy

from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.rest import RestHelper
from perfrunner.workloads.bigfun.query_gen import new_statement


def store_metrics(statement: str, metrics: dict):
    with open('bigfun.log', 'a') as fh:
        fh.write(pretty_dict({
            'statement': statement, 'metrics': metrics,
        }))
        fh.write('\n')


def run_query(rest: RestHelper, node: str, statement: str) -> float:
    t0 = time.time()
    response = rest.exec_analytics_statement(node, statement)
    latency = time.time() - t0  # Latency in seconds
    store_metrics(statement, response.json()['metrics'])
    return latency


def run_concurrent_queries(rest: RestHelper,
                           nodes: List[str],
                           query: dict,
                           concurrency: int,
                           num_requests: int) -> List[float]:
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        nodes = cycle(nodes)
        futures = [
            executor.submit(run_query, rest, next(nodes), new_statement(query))
            for _ in range(num_requests)
        ]
        timings = []
        for future in as_completed(futures):
            timings.append(future.result())
        return timings


def bigfun(rest: RestHelper,
           nodes: List[str],
           concurrency: int,
           num_requests: int) -> Iterator:
    with open('tests/analytics/queries.json') as fh:
        queries = json.load(fh)

    for query in queries:
        timings = run_concurrent_queries(rest, nodes, query, concurrency,
                                         num_requests)
        avg_latency = round(1000 * numpy.mean(timings))  # Latency in ms
        yield query, avg_latency

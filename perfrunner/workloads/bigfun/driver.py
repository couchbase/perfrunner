import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from typing import Iterator, List

import numpy

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.rest import RestHelper
from perfrunner.workloads.bigfun.query_gen import Query, new_queries


def store_metrics(statement: str, metrics: dict):
    with open('bigfun.log', 'a') as fh:
        fh.write(pretty_dict({
            'statement': statement, 'metrics': metrics,
        }))
        fh.write('\n')


def run_query(rest: RestHelper, node: str, query: Query) -> float:
    t0 = time.time()
    response = rest.exec_analytics_statement(node, query.statement)
    latency = time.time() - t0  # Latency in seconds
    store_metrics(query.statement, response.json()['metrics'])
    return latency


def run_query_external(rest: RestHelper, node: str, query: Query) -> float:
    t0 = time.time()
    response = rest.exec_analytics_statement_curl(node, query.statement)
    latency = time.time() - t0  # Latency in seconds
    store_metrics(query.statement, json.loads(response)['metrics'])
    return latency


def run_concurrent_queries(rest: RestHelper,
                           nodes: List[str],
                           query: Query,
                           concurrency: int,
                           num_requests: int,
                           external_storage: bool = False) -> List[float]:
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        nodes = cycle(nodes)
        if external_storage:
            futures = [
                executor.submit(run_query_external, rest, next(nodes), query)
                for _ in range(num_requests)
            ]
        else:
            futures = [
                executor.submit(run_query, rest, next(nodes), query)
                for _ in range(num_requests)
            ]
        timings = []
        for future in as_completed(futures):
            timings.append(future.result())
        return timings


def bigfun(rest: RestHelper,
           nodes: List[str],
           concurrency: int,
           num_requests: int,
           query_set: str,
           external_storage: bool = False) -> Iterator:
    logger.info('Running BigFun queries')

    if not num_requests > 0:
        logger.info('No BigFun queries to run')
        return

    for query in new_queries(query_set):
        timings = run_concurrent_queries(rest,
                                         nodes,
                                         query,
                                         concurrency,
                                         num_requests,
                                         external_storage)
        avg_latency = int(1000 * numpy.mean(timings))  # Latency in ms
        yield query, avg_latency

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum, auto
from itertools import cycle
from typing import Iterator, List

import numpy

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.rest import RestHelper
from perfrunner.workloads.bigfun.query_gen import Query, new_queries


class QueryMethod(Enum):
    PYTHON_CBAS = auto()
    CURL_CBAS = auto()
    PYTHON_GOLDFISH_NEBULA = auto()


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


def run_query_goldfish_nebula(rest: RestHelper, endpoint: str, query: Query) -> float:
    t0 = time.time()
    response = rest.exec_analytics_statement_goldfish_nebula(endpoint, query.statement)
    latency = time.time() - t0  # Latency in seconds
    store_metrics(query.statement, response.json()['metrics'])
    return latency


def run_concurrent_queries(rest: RestHelper,
                           nodes: List[str],
                           query: Query,
                           concurrency: int,
                           num_requests: int,
                           query_method: QueryMethod = QueryMethod.PYTHON_CBAS) -> List[float]:
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        nodes = cycle(nodes)

        if query_method == QueryMethod.PYTHON_GOLDFISH_NEBULA:
            run_query_fn = run_query_goldfish_nebula
        elif query_method == QueryMethod.CURL_CBAS:
            run_query_fn = run_query_external
        else:
            run_query_fn = run_query

        futures = [
            executor.submit(run_query_fn, rest, next(nodes), query)
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
           query_method: QueryMethod = QueryMethod.PYTHON_CBAS) -> Iterator:
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
                                         query_method)
        avg_latency = int(1000 * numpy.mean(timings))  # Latency in ms
        yield query, avg_latency

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum, auto
from itertools import cycle
from typing import Any, Callable, Iterator

import numpy
from requests import Response

from logger import logger
from perfrunner.helpers.misc import pretty_dict
from perfrunner.helpers.rest import RestType
from perfrunner.workloads.bigfun.query_gen import Query, new_queries


class QueryMethod(Enum):
    PYTHON_CBAS = auto()
    CURL_CBAS = auto()


def store_metrics(query: Query, response_json: dict):
    with open('bigfun.log', 'a') as fh:
        fh.write(
            pretty_dict(
                {
                    "id": query.id,
                    "description": query.description,
                    "statement": query.statement,
                    "metrics": response_json.get("metrics", {}),
                    "plans": response_json.get("plans", {}),
                    "errors": response_json.get("errors", {}),
                }
            )
        )
        fh.write('\n')


def run_query(
    exec_func: Callable[[str, str, dict], Any],
    convert_func: Callable[[Any], dict],
    node: str,
    query: Query,
    params: dict = {},
) -> float:
    t0 = time.time()
    response = exec_func(node, query.statement, params)
    latency = time.time() - t0  # Latency in seconds
    response_json = convert_func(response)
    store_metrics(query, response_json)
    return latency


def run_concurrent_queries(
    rest: RestType,
    nodes: list[str],
    query: Query,
    concurrency: int,
    num_requests: int,
    query_method: QueryMethod = QueryMethod.PYTHON_CBAS,
    request_params: dict = {},
) -> list[float]:
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        nodes = cycle(nodes)

        if query_method == QueryMethod.CURL_CBAS:
            exec_func = rest.exec_analytics_statement_curl
            convert_func = json.loads
        else:
            exec_func = rest.exec_analytics_statement
            convert_func = Response.json

        futures = [
            executor.submit(run_query, exec_func, convert_func, next(nodes), query, request_params)
            for _ in range(num_requests)
        ]
        timings = []
        for future in as_completed(futures):
            timings.append(future.result())
        return timings


def bigfun(
    rest: RestType,
    nodes: list[str],
    concurrency: int,
    num_requests: int,
    query_set: str,
    query_method: QueryMethod = QueryMethod.PYTHON_CBAS,
    request_params: dict = {},
) -> Iterator:
    logger.info('Running BigFun queries')

    if not num_requests > 0:
        logger.info('No BigFun queries to run')
        return

    for query in new_queries(query_set):
        timings = run_concurrent_queries(
            rest, nodes, query, concurrency, num_requests, query_method, request_params
        )
        avg_latency = int(1000 * numpy.mean(timings))  # Latency in ms
        yield query, avg_latency
